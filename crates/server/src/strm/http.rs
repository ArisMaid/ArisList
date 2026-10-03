//! The single outbound HTTP path for STRM sources.
//!
//! This layer deliberately does not forward caller headers.  It validates
//! every hop, spaces real requests globally, and keeps short-lived failure /
//! cooldown state so a page of media cannot turn into a request storm.

use std::collections::{HashMap, HashSet};
use std::env;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use reqwest::header::{
    ACCEPT_ENCODING, IF_MODIFIED_SINCE, IF_NONE_MATCH, IF_RANGE, RANGE, RETRY_AFTER,
};
use reqwest::{Client, Response, StatusCode};
use tokio::net::lookup_host;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::timeout;
use url::Url;

use crate::error::{AppError, Result};

const DEFAULT_MIN_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_DNS_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const DEFAULT_FAILURE_TTL: Duration = Duration::from_secs(10);
const DEFAULT_MAX_REDIRECTS: usize = 5;
const DEFAULT_429_COOLDOWN: Duration = Duration::from_secs(60);
const DEFAULT_403_COOLDOWN: Duration = Duration::from_secs(5 * 60);
const DEFAULT_503_COOLDOWN: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
struct HttpPolicy {
    min_interval: Duration,
    dns_timeout: Duration,
    request_timeout: Duration,
    max_redirects: usize,
    trusted_origins: Vec<String>,
    trusted_networks: Vec<String>,
}

impl Default for HttpPolicy {
    fn default() -> Self {
        Self {
            min_interval: DEFAULT_MIN_INTERVAL,
            dns_timeout: DEFAULT_DNS_TIMEOUT,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            max_redirects: DEFAULT_MAX_REDIRECTS,
            trusted_origins: Vec::new(),
            trusted_networks: Vec::new(),
        }
    }
}

impl HttpPolicy {
    fn from_env() -> Self {
        let mut policy = Self::default();
        if let Some(value) = env::var("STRM_MIN_REQUEST_INTERVAL_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
        {
            policy.min_interval = Duration::from_millis(value.max(1));
        }
        if let Some(value) = env::var("STRM_DNS_TIMEOUT_SECONDS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
        {
            policy.dns_timeout = Duration::from_secs(value.max(1));
        }
        if let Some(value) = env::var("STRM_REQUEST_TIMEOUT_SECONDS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
        {
            policy.request_timeout = Duration::from_secs(value.max(1));
        }
        if let Some(value) = env::var("STRM_MAX_REDIRECTS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
        {
            policy.max_redirects = value.min(5);
        }
        policy.trusted_origins = csv_env("STRM_TRUSTED_ORIGINS");
        policy.trusted_networks = csv_env("STRM_TRUSTED_NETWORKS");
        policy
    }
}

fn csv_env(name: &str) -> Vec<String> {
    env::var(name)
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

#[derive(Default)]
struct ControllerState {
    last_request_started: Option<Instant>,
    cooldowns: HashMap<String, Cooldown>,
    failures: HashMap<String, Instant>,
}

#[derive(Clone, Copy)]
struct Cooldown {
    until: Instant,
    retry_after_seconds: u64,
}

static CONTROLLER_STATE: LazyLock<Mutex<ControllerState>> =
    LazyLock::new(|| Mutex::new(ControllerState::default()));
static REMOTE_REQUESTS: LazyLock<std::sync::Arc<Semaphore>> =
    LazyLock::new(|| std::sync::Arc::new(Semaphore::new(remote_worker_limit())));
/// Validate a target and return the addresses that the request client may use.
///
/// Public addresses are allowed by default.  Private/local addresses require
/// either an exact trusted origin or a matching `STRM_TRUSTED_NETWORKS` entry.
pub async fn validate_target(raw_url: &str) -> Result<(Url, Vec<SocketAddr>)> {
    let policy = HttpPolicy::from_env();
    validate_target_with_policy(raw_url, &policy).await
}

async fn validate_target_with_policy(
    raw_url: &str,
    policy: &HttpPolicy,
) -> Result<(Url, Vec<SocketAddr>)> {
    let url = Url::parse(raw_url)
        .map_err(|error| AppError::BadRequest(format!("invalid STRM URL: {error}")))?;
    validate_url_shape(&url)?;

    let host = url
        .host_str()
        .ok_or_else(|| AppError::BadRequest("STRM URL has no host".to_string()))?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| AppError::BadRequest("STRM URL has no usable port".to_string()))?;
    let addresses = resolve_addresses(host, port, policy.dns_timeout).await?;
    if addresses.is_empty() {
        return Err(AppError::BadRequest(
            "STRM host resolved to no addresses".to_string(),
        ));
    }

    let exact_origin_trusted = policy
        .trusted_origins
        .iter()
        .any(|origin| exact_origin_matches(&url, origin));
    let all_addresses_allowed = addresses.iter().all(|address| {
        let ip = address.ip();
        is_public_ip(ip)
            || exact_origin_trusted
            || policy
                .trusted_networks
                .iter()
                .any(|network| ip_matches_network(ip, network))
    });
    if !all_addresses_allowed {
        return Err(AppError::BadRequest(
            "STRM target resolves to a non-public address; configure an exact trusted origin or network if this is intentional".to_string(),
        ));
    }

    Ok((url, addresses))
}

fn validate_url_shape(url: &Url) -> Result<()> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(AppError::BadRequest(
            "STRM URL scheme must be http or https".to_string(),
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(AppError::BadRequest(
            "STRM URL credentials are not allowed".to_string(),
        ));
    }
    Ok(())
}

async fn resolve_addresses(
    host: &str,
    port: u16,
    dns_timeout: Duration,
) -> Result<Vec<SocketAddr>> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(vec![SocketAddr::new(ip, port)]);
    }

    let addresses = timeout(dns_timeout, lookup_host((host, port)))
        .await
        .map_err(|_| AppError::BadRequest("STRM DNS lookup timed out".to_string()))?
        .map_err(|error| AppError::BadRequest(format!("STRM DNS lookup failed: {error}")))?;
    Ok(addresses.collect())
}

pub async fn send_get(
    raw_url: &str,
    range: Option<&str>,
    if_range: Option<&str>,
    etag: Option<&str>,
    last_modified: Option<&str>,
) -> Result<Response> {
    let policy = HttpPolicy::from_env();
    let mut current = Url::parse(raw_url)
        .map_err(|error| AppError::BadRequest(format!("invalid STRM URL: {error}")))?;
    let mut visited = HashSet::new();

    for redirect_index in 0..=policy.max_redirects {
        let (validated, addresses) = validate_target_with_policy(current.as_str(), &policy).await?;
        current = validated;
        if !visited.insert(current.to_string()) {
            return Err(AppError::BadRequest(
                "STRM redirect loop detected".to_string(),
            ));
        }

        let source_key = origin_key(&current);
        let _request_permit = wait_for_request_slot(&source_key, &current, &policy).await?;
        let client = build_client(&current, &addresses, &policy)?;
        let mut request = client.get(current.clone());
        request = request.header(ACCEPT_ENCODING, "identity");
        if let Some(value) = range {
            request = request.header(RANGE, value);
        }
        if let Some(value) = if_range {
            request = request.header(IF_RANGE, value);
        }
        if let Some(value) = etag {
            request = request.header(IF_NONE_MATCH, value);
        }
        if let Some(value) = last_modified {
            request = request.header(IF_MODIFIED_SINCE, value);
        }

        let response = match request.send().await {
            Ok(response) => response,
            Err(error) => {
                record_failure(&current);
                return Err(AppError::Http(error));
            }
        };
        let status = response.status();
        record_response(&current, status, &response);

        if status.is_redirection() {
            if redirect_index >= policy.max_redirects {
                return Err(AppError::BadRequest(
                    "STRM redirect limit exceeded".to_string(),
                ));
            }
            let location = response
                .headers()
                .get(reqwest::header::LOCATION)
                .ok_or_else(|| AppError::Other("STRM redirect has no Location header".to_string()))?
                .to_str()
                .map_err(|_| AppError::Other("STRM redirect Location is invalid".to_string()))?;
            current = current
                .join(location)
                .map_err(|error| AppError::BadRequest(format!("invalid STRM redirect: {error}")))?;
            continue;
        }

        return Ok(response);
    }

    Err(AppError::BadRequest(
        "STRM redirect limit exceeded".to_string(),
    ))
}

async fn wait_for_request_slot(
    source_key: &str,
    url: &Url,
    policy: &HttpPolicy,
) -> Result<OwnedSemaphorePermit> {
    let permit = REMOTE_REQUESTS
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| AppError::Other("STRM request controller is unavailable".to_string()))?;
    let failure_key = url.to_string();
    loop {
        let wait = {
            let mut state = CONTROLLER_STATE
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let now = Instant::now();
            state.cooldowns.retain(|_, cooldown| cooldown.until > now);
            state
                .failures
                .retain(|_, failed_at| now.duration_since(*failed_at) < DEFAULT_FAILURE_TTL);

            if let Some(cooldown) = state.cooldowns.get(source_key) {
                return Err(AppError::Overloaded {
                    message: format!("STRM source is cooling down: {source_key}"),
                    retry_after_seconds: cooldown.retry_after_seconds,
                });
            }
            if state.failures.contains_key(&failure_key) {
                return Err(AppError::Overloaded {
                    message: "STRM source recently failed; retry after the short failure window"
                        .to_string(),
                    retry_after_seconds: DEFAULT_FAILURE_TTL.as_secs(),
                });
            }

            let interval_wait = state
                .last_request_started
                .map(|started| {
                    policy
                        .min_interval
                        .saturating_sub(now.duration_since(started))
                })
                .unwrap_or_default();
            if interval_wait.is_zero() {
                state.last_request_started = Some(now);
                None
            } else {
                Some(interval_wait)
            }
        };

        if let Some(wait) = wait {
            tokio::time::sleep(wait).await;
            continue;
        }
        return Ok(permit);
    }
}

fn remote_worker_limit() -> usize {
    env::var("REMOTE_SOURCE_WORKERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1)
        .clamp(1, 32)
}

fn record_response(url: &Url, status: StatusCode, response: &Response) {
    let source_key = origin_key(url);
    let mut state = CONTROLLER_STATE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let request_key = url.to_string();
    if status.is_redirection() {
        return;
    }
    if status.is_success() || status == StatusCode::NOT_MODIFIED {
        state.failures.remove(&request_key);
        return;
    }

    if status == StatusCode::TOO_MANY_REQUESTS
        || status == StatusCode::FORBIDDEN
        || status == StatusCode::SERVICE_UNAVAILABLE
    {
        let default = match status {
            StatusCode::TOO_MANY_REQUESTS => DEFAULT_429_COOLDOWN,
            StatusCode::FORBIDDEN => DEFAULT_403_COOLDOWN,
            _ => DEFAULT_503_COOLDOWN,
        };
        let retry_after = retry_after_seconds(response).unwrap_or(default.as_secs());
        let retry_after = if status == StatusCode::SERVICE_UNAVAILABLE {
            retry_after.max(DEFAULT_503_COOLDOWN.as_secs())
        } else {
            retry_after
        };
        state.cooldowns.insert(
            source_key,
            Cooldown {
                until: Instant::now() + Duration::from_secs(retry_after),
                retry_after_seconds: retry_after,
            },
        );
    }
    state.failures.insert(request_key, Instant::now());
}

fn record_failure(url: &Url) {
    let mut state = CONTROLLER_STATE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    state.failures.insert(url.to_string(), Instant::now());
}

pub fn retry_after_seconds(response: &Response) -> Option<u64> {
    response
        .headers()
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
}

pub fn origin_key(url: &Url) -> String {
    let port = url
        .port_or_known_default()
        .map(|value| value.to_string())
        .unwrap_or_default();
    format!(
        "{}://{}:{port}",
        url.scheme(),
        url.host_str().unwrap_or_default()
    )
}

fn build_client(url: &Url, addresses: &[SocketAddr], policy: &HttpPolicy) -> Result<Client> {
    let host = url
        .host_str()
        .ok_or_else(|| AppError::BadRequest("STRM URL has no host".to_string()))?;
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .connect_timeout(policy.request_timeout.min(Duration::from_secs(15)))
        .timeout(policy.request_timeout)
        .resolve_to_addrs(host, addresses)
        .build()
        .map_err(AppError::Http)
}

fn exact_origin_matches(url: &Url, configured: &str) -> bool {
    let Ok(configured) = Url::parse(configured.trim()) else {
        return false;
    };
    configured.scheme() == url.scheme()
        && configured.host_str() == url.host_str()
        && configured.port_or_known_default() == url.port_or_known_default()
        && configured.username().is_empty()
        && configured.password().is_none()
        && matches!(configured.path(), "" | "/")
        && configured.query().is_none()
        && configured.fragment().is_none()
}

fn ip_matches_network(ip: IpAddr, network: &str) -> bool {
    let Some((base, prefix)) = network.trim().split_once('/') else {
        return network
            .trim()
            .parse::<IpAddr>()
            .map(|value| value == ip)
            .unwrap_or(false);
    };
    let Ok(base) = base.parse::<IpAddr>() else {
        return false;
    };
    let Ok(prefix) = prefix.parse::<u8>() else {
        return false;
    };
    match (base, ip) {
        (IpAddr::V4(base), IpAddr::V4(ip)) if prefix <= 32 => {
            let mask = if prefix == 0 {
                0
            } else {
                u32::MAX << (32 - prefix)
            };
            u32::from(base) & mask == u32::from(ip) & mask
        }
        (IpAddr::V6(base), IpAddr::V6(ip)) if prefix <= 128 => {
            let mask = if prefix == 0 {
                0
            } else {
                u128::MAX << (128 - prefix)
            };
            u128::from(base) & mask == u128::from(ip) & mask
        }
        _ => false,
    }
}

pub(crate) fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            !ip.is_private()
                && !ip.is_loopback()
                && !ip.is_link_local()
                && !ip.is_multicast()
                && !ip.is_unspecified()
                && !is_reserved_ipv4(ip)
        }
        IpAddr::V6(ip) => {
            if let Some(ipv4) = ip.to_ipv4() {
                return is_public_ipv4(ipv4);
            }
            let segments = ip.segments();
            (segments[0] & 0xe000) == 0x2000
                && !ip.is_loopback()
                && !ip.is_unique_local()
                && !ip.is_unicast_link_local()
                && !ip.is_multicast()
                && !ip.is_unspecified()
                && !(segments[0] == 0x2001 && (segments[1] <= 0x01ff || segments[1] == 0x0db8))
                && segments[0] != 0x2002
                && !(segments[0] == 0x3fff && segments[1] < 0x1000)
        }
    }
}

fn is_public_ipv4(ip: Ipv4Addr) -> bool {
    !ip.is_private()
        && !ip.is_loopback()
        && !ip.is_link_local()
        && !ip.is_multicast()
        && !ip.is_unspecified()
        && !ip.is_broadcast()
        && !is_reserved_ipv4(ip)
}

fn is_reserved_ipv4(ip: Ipv4Addr) -> bool {
    let value = u32::from(ip);
    let ranges = [
        (u32::from(Ipv4Addr::new(0, 0, 0, 0)), 8),
        (u32::from(Ipv4Addr::new(100, 64, 0, 0)), 10),
        (u32::from(Ipv4Addr::new(169, 254, 0, 0)), 16),
        (u32::from(Ipv4Addr::new(192, 0, 0, 0)), 24),
        (u32::from(Ipv4Addr::new(192, 0, 2, 0)), 24),
        (u32::from(Ipv4Addr::new(198, 18, 0, 0)), 15),
        (u32::from(Ipv4Addr::new(198, 51, 100, 0)), 24),
        (u32::from(Ipv4Addr::new(203, 0, 113, 0)), 24),
        (u32::from(Ipv4Addr::new(224, 0, 0, 0)), 4),
    ];
    ranges.iter().any(|(base, prefix)| {
        let mask = u32::MAX << (32 - prefix);
        value & mask == *base & mask
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_origin_requires_scheme_host_and_port() {
        let url = Url::parse("http://localhost:8787/file.zip").unwrap();
        assert!(exact_origin_matches(&url, "http://localhost:8787/"));
        assert!(!exact_origin_matches(&url, "https://localhost:8787/"));
        assert!(!exact_origin_matches(&url, "http://localhost:8788/"));
        assert!(!exact_origin_matches(&url, "http://localhost:8787/path"));
    }

    #[test]
    fn network_match_is_exactly_cidr_scoped() {
        assert!(ip_matches_network(
            "198.18.0.32".parse().unwrap(),
            "198.18.0.0/15"
        ));
        assert!(!ip_matches_network(
            "198.20.0.1".parse().unwrap(),
            "198.18.0.0/15"
        ));
        assert!(ip_matches_network(
            "2001:db8::1".parse().unwrap(),
            "2001:db8::/32"
        ));
    }

    #[test]
    fn reserved_ranges_are_not_public() {
        assert!(!is_public_ip("198.18.0.32".parse().unwrap()));
        assert!(!is_public_ip("127.0.0.1".parse().unwrap()));
        assert!(is_public_ip("8.8.8.8".parse().unwrap()));
    }
}
