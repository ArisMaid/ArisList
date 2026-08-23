# N100 performance evidence tools

These scripts implement the G0 evidence contract from
`docs/nas-n100-loading-architecture-refactor-plan.md`. They use Node.js built-in
modules only and do not change the database or media roots. Node.js 22.5 or
newer is required for the built-in read-only SQLite snapshot API.

## 0. Capture repeatable large-scale Inventory/Derivative evidence

The ignored Rust scale tests are intentionally separate from ordinary
regression. Run them through the wrapper below so bounded metrics, command exit
codes, host information, and a sibling raw log are saved as one artifact. The
wrapper refuses to overwrite either output file.

```powershell
node scripts/perf/run-scale-gates.mjs `
  --output perf-results/scale-dev-001/scale-gates.json
```

Use `--skip-inventory`, `--skip-derivative`, or repeat
`--derivative-rows 700000`/`--derivative-rows 1400000` for a targeted run. A
`passed` result proves only the development-scale batch/query shape and SQLite
integrity. It is not an N100/4GiB latency or RSS Gate; keep the raw log and run
the same wrapper inside the target NAS cgroup before changing any feature flag.
The artifact also records the current commit, dirty-state hash, schema version,
platform and the shared allowlisted feature flags so results from different
working trees cannot be silently compared.

## 0.1 Run an isolated Inventory kind simulation

The kind runner authenticates through the normal admin session, enqueues one
bounded `scan-library` job, polls the job/root/health state, and writes a
provenance-labelled artifact without exposing the password. Set the password
only in the process environment:

```powershell
$env:APP_ADMIN_PASSWORD = "..."
node scripts/perf/run-inventory-kind-simulation.mjs `
  --base-url http://127.0.0.1:9048 `
  --kind novel `
  --output perf-results/docker-n100-scale-1cpu/novel-inventory-20260822/run.json
```

Use `--expected-files` only when the mounted root is a deliberate target-size
fixture. Without it, the artifact is explicitly `kind-smoke-evidence` and
cannot be used as a target-scale Gate. The runner never walks or mutates a
second root, does not enable production Compose flags, and records that it
does not measure NAS/HDD latency.

For a long scan that outlives the initial process, poll an already-created job
without enqueueing another scan by passing `--existing-job-id <id>`; this keeps
the final artifact tied to the original job.

### 0.2 Prepare a target-size kind fixture

`prepare-kind-scale-fixture.mjs` supports `audio`, `coser-picture`, and
`gallery`. It creates hardlinks so a scale fixture does not duplicate the
source bytes, and writes its manifest outside the mounted media root. Keep the
manifest outside the root: Inventory counts all discovered entries, so an
in-root manifest would turn a requested 10,000-file fixture into 10,001
entries.

For the Gallery target shape (700,000 images across 600 author directories),
use a large `files-per-work` value so each author remains one image set:

```powershell
node scripts/perf/prepare-kind-scale-fixture.mjs `
  --kind gallery `
  --source .\图库 `
  --output perf-results/docker-n100-scale-1cpu/gallery-target-700k-20260822 `
  --count 700000 `
  --authors 600 `
  --files-per-work 2000 `
  --manifest-output perf-results/docker-n100-scale-1cpu/gallery-target-700k-20260822-manifest.json
```

For multi-track Audio fixtures, pass `--files-per-work 20` to model 20-track
works while retaining the requested total file count. The manifest records
the source count, logical bytes, author count, and grouping parameters; it is
provenance only and is never mounted as media.

## 1. Capture an immutable baseline directory

Run against a database copy or a live SQLite database on its local filesystem:

```powershell
node scripts/perf/capture-baseline.mjs `
  --output perf-results/n100-run-001 `
  --database D:\arislist-data\library.sqlite `
  --health-url http://127.0.0.1:8787/api/health/resources `
  --health-checkpoint `
  --media-root gallery=Z:\图库 `
  --media-root comics=Z:\漫画
```

The output directory must be empty. Existing evidence is never deleted or
silently overwritten. `environment.json` records the commit, dirty-state hash,
safe feature flags and filesystem capacity. `dataset-manifest.json` opens
SQLite read-only and records schema/count/long-tail facts. In addition to total
counts, it records per-kind asset/tag distributions (P50/P95/P99/max),
Inventory root/file/status totals and per-work file maxima, deleted-work count,
Catalog revision and non-secret search-index readiness. Media roots are not
recursively walked by this command; large-directory discovery is a separate N4
scenario so baseline capture cannot accidentally scan 29TB.

Secrets such as passwords, cookies, tokens and API keys are not allowlisted and
are never written.

`--health-checkpoint` is optional. It adds `?checkpoint=true` to the health
request and records one passive WAL checkpoint result (`busy`, log pages,
checkpointed pages and duration). Normal health capture remains read-only so a
one-second sampler does not accidentally checkpoint on every poll.

`--media-root` only records filesystem capacity and never walks the media tree.
To explicitly capture bounded file-count, byte-count, extension and path-length
facts, add one or more `--media-manifest kind=path` arguments:

```powershell
node scripts/perf/capture-baseline.mjs `
  --output perf-results/n100-run-001 `
  --media-manifest gallery=Z:\图库 `
  --media-manifest comic=Z:\漫画 `
  --media-manifest-max-files 2000000 `
  --media-manifest-inspect-limit 256
```

The manifest walk is read-only, keeps aggregate counters rather than file paths,
skips symbolic links, and stops with `complete=false` when a configured file or
directory bound is reached. For up to the configured inspection sample per root,
it also reads image headers for pixel-size buckets and ZIP/CBZ/EPUB central
directories for entry/uncompressed-size summaries; it never reads archive bodies.
It is intentionally separate from the normal baseline capture so a routine
capacity snapshot cannot accidentally scan a multi-terabyte library.

### 1.1 Verify the real N100/4GiB execution boundary

Before treating any media latency matrix as an N100 Gate, run the fail-closed
environment preflight against the same server environment. For a Docker
container, invoke it from the host with `--container`; the helper uses
`docker exec` to read the target container's `/proc` and cgroup files, so the
production image does not need Node or test tooling:

```powershell
node scripts/perf/check-n100-environment.mjs `
  --container arislist-n100-app `
  --output perf-results/n100-run-001/environment-gate.json `
  --health-url http://127.0.0.1:8787/api/health/resources `
  --block-device sda `
  --max-cpu-cores 1
```

For a Docker approximation, add `--max-cpu-cores 1`. This optional check reads
the cgroup CPU quota from inside the same container; it is intentionally not
required for a bare-metal N100 host that has no CPU cgroup quota. A value above
the requested limit, or missing quota evidence when the option is supplied,
fails the preflight instead of treating a Compose setting as proof.

For a bare-metal Linux/N100 host, omit `--container`; for a Docker approximation,
use the actual container name or ID and the host-published health URL. The check
requires Linux, an Intel N100 model string, a finite cgroup memory
limit no greater than 4GiB, `sqlite.config.profile=nas-n100-4g`, an `ok` health
response and the requested `/proc/diskstats` device. When `--max-cpu-cores` is
provided, it also requires a finite cgroup CPU quota no greater than that value.
Missing evidence is
`incomplete` and returns a non-zero exit code; the artifact records only the
health pathname, not credentials or query text. A passing HTTP scenario on
Windows, outside the cgroup, or against a different CPU therefore cannot be
mistaken for the target hardware result.

### 1.2 Verify an old SQLite database migration on a copy

`run-migration-gate.mjs` requires a quiesced database file and never writes to the
source. It copies the file into a new artifact directory, starts the current
server against the copy, waits for `/api/health`, then records startup time,
schema version, `integrity_check`, source hash, and server logs:

```powershell
node scripts/perf/run-migration-gate.mjs `
  --database D:\arislist-data\library.sqlite `
  --output perf-results/n100-run-001/migration-gate
```

The runner rejects a non-empty `-wal` sidecar so the input is reproducible; create
a consistent backup first when the source database is active. A successful result
requires the current schema version and `integrity_check=ok`; it also copies the
migrated database plus any WAL/SHM sidecars to `restore/` and rechecks the restored
copy. This is a migration and startup Gate, not a media latency or RSS Gate; run it
against the target NAS copy before the media matrix and keep the complete artifact.

## 2. Record HTTP scenarios

```powershell
$env:PERF_COOKIE='arislist_session=...'
node scripts/perf/run-http-scenario.mjs `
  --url 'http://127.0.0.1:8787/api/catalog/works?kind=gallery&limit=100' `
  --scenario catalog-gallery-first-page `
  --warm warm `
  --requests 30 `
  --concurrency 1 `
  --output perf-results/n100-run-001/scenario-results.jsonl
```

Credentials are read only from `PERF_COOKIE` or `PERF_AUTHORIZATION` and are not
stored. Record cold and warm samples as separate invocations. Use at least 30
requests per latency scenario; do not mix single-user and two-user concurrency
in one group.

For qmediasync audio startup and disconnect evidence, the same runner can send
bounded byte ranges without buffering an entire track. The runtime file keeps
only cumulative counter snapshots from `/api/health/resources` and a redacted
delta evaluation:

```powershell
node scripts/perf/run-http-scenario.mjs `
  --url http://127.0.0.1:8787/api/assets/123/stream `
  --scenario qms-audio-startup-range `
  --warm cold `
  --requests 30 `
  --concurrency 1 `
  --range 'bytes=0-262143' `
  --max-body-bytes 262144 `
  --require-partial `
  --health-url http://127.0.0.1:8787/api/health/resources `
  --output perf-results/n100-run-001/qms-audio-startup.jsonl `
  --runtime-output perf-results/n100-run-001/qms-audio-startup-runtime.json
```

Each media record includes response `Content-Range`, `Content-Length`,
`Accept-Ranges`, body bytes actually consumed, and whether the client cancelled
after the requested bound. Use `--abort-after-bytes` for disconnect/release
tests. A runtime result is not a pass unless the expected Range count, partial
response count, stream failures, cache quota rejections and downloaded-byte
accounting all satisfy the evaluator.

### 2.1 Run the complete media preview/filter matrix

`run-media-gates.mjs` runs a new, explicit target matrix through the same HTTP
runner and evaluates one fail-closed summary. For a real database, prepare the
matrix from read-only facts instead of hand-picking IDs:

```powershell
node scripts/perf/prepare-media-targets.mjs `
  --database D:\arislist-data\library.sqlite `
  --output perf-results/n100-run-001/media-targets.json `
  --base-url http://127.0.0.1:8787
```

The preparer selects active gallery image, comic/CoserPicture archives with a
positive page count, a sufficiently large audio track for the requested Range,
an EPUB book, and the highest-coverage active tag. It opens SQLite read-only,
does not inspect archive bodies, refuses to overwrite an existing target file,
and fails closed if any required representative is absent. The target file
contains URLs but the Gate artifact stores only URL paths; credentials still
come exclusively from `PERF_COOKIE` or `PERF_AUTHORIZATION`.

```json
{
  "profile": "nas-n100-4g",
  "scope": "media-preview-filter-and-startup",
  "targets": [
    {
      "id": "gallery-thumbnail-warm",
      "scenario": "gallery-thumbnail",
      "warm": "warm",
      "url": "http://127.0.0.1:8787/api/assets/123/thumb?size=256",
      "requests": 30,
      "concurrency": 1
    },
    {
      "id": "audio-startup-range",
      "scenario": "audio-startup-range",
      "warm": "cold",
      "url": "http://127.0.0.1:8787/api/assets/456/stream",
      "range": "bytes=0-262143",
      "max_body_bytes": 262144,
      "health_url": "http://127.0.0.1:8787/api/health/resources",
      "require_partial": true,
      "requests": 30,
      "concurrency": 1
    }
  ]
}
```

The production matrix should include the catalog first page, tag filter,
gallery warm/cold thumbnail, comic page, CoserPicture page, audio Range
startup and novel summary detail. Use the declarative profile below so missing
targets remain `incomplete` instead of being silently omitted:

```powershell
node scripts/perf/run-media-gates.mjs `
  --targets perf-results/n100-run-001/media-targets.json `
  --output perf-results/n100-run-001/media-gates `
  --gates scripts/perf/gates/media-n100-4g.json
```

The runner refuses a non-empty output directory, records one raw JSONL file per
target plus `scenario-results.jsonl`, `summary.json` and `matrix.json`, and
returns a non-zero exit code for missing samples, HTTP failures, or threshold
breaches; child-runner failures are also recorded as a failed `target-runs` check
in `summary.json`. `warm` is a label, not a cache reset: cold runs must be prepared by
restarting/expiring the relevant cache before the matrix starts. The latency
thresholds mirror the plan's N100 budgets; they are not development-machine
evidence and must be run with the 4 GiB cgroup and target HDD/SSD layout.

### 2.2 Run cross-media mixed load

`run-media-mixed-load.mjs` runs every target concurrently and multiplies each
target's request count/concurrency by the logical client count. This is the
recommended preflight for the two-client requirement because a sequential
per-target matrix cannot expose competition between catalog, thumbnail, archive,
EPUB and audio paths. Merged records retain the original target concurrency for
the declarative latency selectors and additionally record `client_id`,
`client_count`, `effective_concurrency`, `target_id` and `round`.

Use one complete round for a short smoke or a duration for the formal window:

```powershell
node scripts/perf/run-media-mixed-load.mjs `
  --targets perf-results/n100-run-001/media-targets.json `
  --output perf-results/n100-run-001/media-mixed-5m `
  --clients 2 `
  --duration-seconds 300 `
  --health-url http://127.0.0.1:8787/api/health/resources `
  --gates scripts/perf/gates/media-n100-4g.json
```

The runner refuses a non-empty output directory, requires complete per-target
coverage for every finished round, records before/after health snapshots without
credentials, and fails closed on any child failure, missing sample, 503, or gate
breach. `--rounds` and `--duration-seconds` are mutually exclusive. Duration mode
stops starting new rounds after the deadline but lets the current round finish, so
`mode.rounds_completed` and every `coverage:*` check must be retained with the
artifact. Run `sample-system.mjs` concurrently to capture cgroup RSS, CPU/iowait,
HDD await, WAL/busy and resource permit time series.

## 3. Sample system and application resources

While a scenario or mixed workload is running, collect one-second system and
application samples. On Linux, set the block-device name shown by
`/proc/diskstats` to capture await and average queue depth:

```powershell
node scripts/perf/sample-system.mjs `
  --output perf-results/n100-run-001/system-samples.csv `
  --health-url http://127.0.0.1:8787/api/health/resources `
  --block-device sda `
  --duration 300 `
  --interval 1000
```

The sampler appends rather than truncating. It reads cgroup memory, CPU/iowait,
frequency, available thermal sensors, optional Linux diskstats, SQLite WAL and
search-outbox pending counts. Missing host metrics remain empty instead of
being replaced with zero. The CSV also records SQLite pool size/idle/active
connections, saturation, checkout/connection-open counters, writer queue
depth/bytes, writer wait and hold accumulators, pool acquire errors and SQLite
busy errors, passive-checkpoint busy/log/checkpointed pages and duration,
background worker and maintenance-permit wait/active counters, outbox revision
lag, current waiters, cumulative resource wait samples/total/max/timeout
counters, processing/inflight bytes, active thumbnail/archive/scan/search
permits, and cumulative qmediasync stream/cloud-cache counters from the
resource-health endpoint. These counters are process-local; compare
before/after deltas and keep the raw health snapshots. Pool checkout idle time
is not pool queue wait time; the distinction is preserved so a Gate cannot
mistake connection reuse evidence for measured saturation latency.

## 3.1 Local Docker N100 approximation

本机可以用 `docker-compose.n100-sim.yml` 做受控近似测试：它把容器内存和
swap 上限固定为 4GiB，默认限制为 1 个 CPU quota，关闭实验性开关，使用独立
8788 端口和 `perf-results/docker-n100-sim/` 数据/缓存目录。该 override 不会
修改生产 `docker-compose.yml`，也不会复用生产数据库或派生缓存：

```powershell
docker compose -f docker-compose.yml -f docker-compose.n100-sim.yml up --build
```

这是资源边界模拟，不是 N100 微架构仿真。正式项目验收固定使用
`mem_limit=4g`、`memswap_limit=4g`、`cpus: 1`、`pids_limit=256` 和应用自身的
`nas-n100-4g` governor；这可以保守地约束 CPU 时间、OOM/暂停/队列/受控 503 等行为，
但仍不会复制 N100 的 IPC、缓存、频率或 NAS HDD 延迟。

因此应按三层使用：

1. Docker 模拟：在上述 1 CPU 边界验证 4GiB hard limit、应用资源 governor、混合请求、
   Range/断流、SQLite busy/WAL 和 fail-closed Gate 编排。
2. 目标规模模拟：用等价合成数据覆盖文件数量、归档长尾、冷/热请求和浏览器 heap；这些
   结果按本项目当前口径作为正式验收证据，并保留宿主/容器 provenance。

如果 Docker daemon 未运行，先启动 Docker Desktop；本机当前 CLI 可执行文件存在，
但 daemon 状态仍需由 `docker info` 成功确认。模拟 Gate 的 URL 应使用
`http://127.0.0.1:8788`，并将其产物放在新的、非空目录之外。

### 3.2 Run the staged N100/NAS Gate wrapper

当需要把预检、基线、媒体矩阵、混合负载和系统采样绑定为同一份证据时，使用：

```powershell
node scripts/perf/run-n100-gate.mjs `
  --output perf-results/n100-gate-001 `
  --database /nas/app-data/library.sqlite `
  --health-url http://127.0.0.1:8787/api/health/resources `
  --base-url http://127.0.0.1:8787 `
  --container arislist-n100-app `
  --block-device sda `
  --media-root gallery=/nas/gallery `
  --media-root comics=/nas/comics `
  --media-root coser-picture=/nas/coser-picture `
  --media-root audio=/nas/audio `
  --media-root novels=/nas/novels `
  --media-manifest gallery=/nas/gallery `
  --media-manifest comics=/nas/comics `
  --media-manifest coser-picture=/nas/coser-picture `
  --media-manifest audio=/nas/audio `
  --media-manifest novels=/nas/novels `
  --mixed-duration-seconds 300 `
  --clients 2 `
  --system-duration-seconds 600
```

该 wrapper 严格按 `preflight -> baseline -> target preparation -> media matrix ->
mixed load + system sampler` 执行；任一正式步骤失败都会保留已产生的子 artifact，
并在根目录写入 `run.json`。输出目录必须为空，凭据仍只从 `PERF_COOKIE`/
`PERF_AUTHORIZATION` 传给子 HTTP runner，绝不写入编排 artifact。

本机 Docker 近似测试可以显式指定 `--mode approximation`。当前项目正式模拟验收固定为
1 CPU、4GiB、256 PID，混合稳定窗口为 300 秒（5 分钟）。只有当唯一的预检失败项是
CPU 型号不是 N100、其余 Linux/4GiB/profile/health/block-device 证据完整时，wrapper
才会继续执行；即使所有媒体和混合负载都通过，根 artifact 的状态仍为
`passed` 只有在正式模拟矩阵、混合窗口和系统采样全部通过时才会写入；CPU/model provenance
仍会保留。该结果用于本项目当前模拟验收，但不能据此开启实验性 ownership 或 reader
开关。`--mixed-duration-seconds` 在正式 Gate 中固定为 300 秒；更长的诊断运行应使用明确的
`--mixed-rounds` 兼容模式，不能把它们标记为稳定窗口通过。
以及其下的原始 JSONL/CSV、`dataset-manifest.json` 和 environment Gate。

## 4. Produce the summary

```powershell
node scripts/perf/summarize-results.mjs `
  --input perf-results/n100-run-001/scenario-results.jsonl `
  --output perf-results/n100-run-001/summary.json
```

The summary uses the conservative nearest-rank definition for P50/P95/P99 and
groups by scenario, cold/warm state and concurrency. Raw JSONL remains the
source of truth.

## Artifact contract

- `run.json`: artifact version, capture time and file inventory.
- `environment.json`: hardware/runtime, source state, safe flags and mounts.
- `dataset-manifest.json`: schema, works/assets/tags/inventory counts and long-tail values.
- `health-snapshot.json`: optional raw resource-health response.
- `scenario-results.jsonl`: one raw HTTP observation per line.
- `*-runtime.json`: optional bounded runtime counter delta, such as qmediasync
  Range/cache evidence.
- `system-samples.csv`: time-series header reserved for N100/cgroup sampling.
- `summary.json`: reproducible grouped percentiles and errors.

An empty `system-samples.csv` or `scenario-results.jsonl` is a prepared
artifact, not a passed Gate.

Run the unit tests with:

```powershell
node --test scripts/perf/perf-lib.test.mjs scripts/perf/synthetic-dataset-lib.test.mjs
```

## 5. Run the derivative ledger development Gate

The ignored Rust acceptance benchmark exercises the real migrations,
trigger-maintained capacity ledger, indexed batch LRU selection, explicit file
eviction and SQLite integrity check. It models a 32 GiB high watermark draining
to 28 GiB. Run both supported ledger sizes separately; the output path must not
already exist. Relative artifact paths are resolved from the workspace root.

```powershell
$env:DERIVATIVE_LEDGER_GATE_ROWS='700000'
$env:DERIVATIVE_LEDGER_GATE_OUTPUT='perf-results/derivative-ledger-700k.json'
cargo test -p media-shelf-server derivative::tests::synthetic_derivative_ledger_scale_gate `
  -- --ignored --exact --nocapture

$env:DERIVATIVE_LEDGER_GATE_ROWS='1400000'
$env:DERIVATIVE_LEDGER_GATE_OUTPUT='perf-results/derivative-ledger-1400k.json'
cargo test -p media-shelf-server derivative::tests::synthetic_derivative_ledger_scale_gate `
  -- --ignored --exact --nocapture
```

Development-machine results prove query shape and regression behavior only.
The release Gate still requires the same commands inside the N100 4 GiB cgroup
while `sample-system.mjs` records memory, CPU, iowait, temperature and SQLite
WAL behavior.

## 6. Run the bounded Facet-cache development Gate

This scenario validates the revision-bound five-second Facet cache, its
single-flight behavior and its advertised capacity. It does not make the
underlying cold Facet query pass. Start a fresh server, or wait longer than the
`facet_cache_ttl_millis` value reported by `/api/health/resources`, before each
run so the concurrent group begins with an empty entry for the selected key.
Do not issue another request with the same kind/tag filter during that wait.

Create a new evidence directory and use output paths that do not already
exist:

```powershell
$env:PERF_COOKIE='arislist_session=...'
node scripts/perf/run-facet-cache-scenario.mjs `
  --base-url http://127.0.0.1:8787 `
  --include-tag 'namespace:key' `
  --kind gallery `
  --concurrency 3 `
  --requests 30 `
  --output perf-results/facet-cache-n100-001/scenario-results.jsonl `
  --runtime-output perf-results/facet-cache-n100-001/facet-runtime.json

node scripts/perf/summarize-results.mjs `
  --input perf-results/facet-cache-n100-001/scenario-results.jsonl `
  --output perf-results/facet-cache-n100-001/summary.json
```

Credentials are used only as request headers. The raw tag is represented by a
SHA-256 digest in the runtime artifact, and neither credentials nor the query
string are written to the JSONL records. A passing runtime artifact requires
one miss for the concurrent group, at least `concurrency - 1` coalesced
requests, no additional warm misses, all expected warm hits, and entry/item
counts within the limits advertised by health.

The 2026-07-31 development-machine evidence is stored in
`perf-results/facet-cache-dev-20260731/`: the 30 warm requests completed at
P50 2.139 ms and P95 2.938 ms, while three concurrent cold requests completed
at about 690 ms with one miss and two coalesced waiters. The earlier uncached
global 50%-coverage tag scenario remained at P95 573.709 ms and failed the
300 ms development Gate. Consequently this cache Gate proves only repeated
browsing and duplicate-request collapse; cold selective Facet still requires
bitmap/incremental materialization work. Neither result replaces the N100,
4 GiB cgroup Gate.

## 7. Run the experimental cold Facet-bitmap Gate

`FACET_BITMAP_ENABLED` is independently disabled by default. When explicitly
enabled, the candidate builds an adaptive in-memory index under the background
Resource Governor. Sparse tags use sorted work slots and high-cardinality tags
upgrade to dense bitsets. The hard limits are 50,000 works, 65,536 tags,
1,000,000 associations and 64 MiB estimated memory; exceeding any limit or
losing a contiguous typed-writer revision chain falls back to the existing
SQLite query.

The selected kind must already have `catalog-v2` ownership. The runner never
changes ownership or the database, so prepare this only in a disposable Gate
fixture. The current production/default ownership remains `legacy` and must
not be changed merely to run a benchmark.

Start a fresh server with `FACET_BITMAP_ENABLED=true`, then run:

```powershell
$env:PERF_COOKIE='arislist_session=...'
node scripts/perf/run-facet-bitmap-scenario.mjs `
  --base-url http://127.0.0.1:8787 `
  --include-tag 'namespace:key' `
  --kind gallery `
  --requests 30 `
  --output perf-results/facet-bitmap-n100-001/scenario-results.jsonl `
  --runtime-output perf-results/facet-bitmap-n100-001/facet-bitmap-runtime.json

node scripts/perf/summarize-results.mjs `
  --input perf-results/facet-bitmap-n100-001/scenario-results.jsonl `
  --output perf-results/facet-bitmap-n100-001/summary.json `
  --gates scripts/perf/gates/facet-bitmap-development.json
```

The warm-up request starts the background build and is excluded from latency
records. Before every measured request, the runner waits longer than the
advertised five-second response-cache TTL. Thirty samples therefore take at
least about 153 seconds. Runtime evidence fails unless the bitmap remains
ready at one stable catalog revision, its query counter advances for every
sample, fallback/scope-rejection/build-failure deltas remain zero, and all
capacity values stay bounded. The raw tag and credentials are not written to
artifacts.

The 2026-07-31 development-machine Gate passed on the complete 40k works,
2,048 tags and 800k associations fixture. The background build took 2.129 s
and estimated 13,283,401 bytes (about 12.67 MiB) of resident index memory.
Internal bitmap evaluation averaged 3.432 ms with a 4.249 ms maximum. All 30
HTTP samples succeeded at P50 15.851 ms, P95 18.256 ms and P99 18.818 ms;
runtime deltas recorded 30 bitmap queries and no fallback, scope rejection or
build failure. Evidence is stored in
`perf-results/facet-bitmap-dev-20260731/`.

This passes only the development Gate; it does not establish N100, 4 GiB,
real-dataset or mixed-load performance. Keep the feature disabled until the
N100 cgroup run has saved raw JSONL/runtime/summary evidence and agrees with
the SQLite fact query.

## 8. Run the explicit shadow-search canary Gate

The production search reader and Catalog search candidates continue to use
`search-index-v2`. Shadow reads require both independently disabled flags:

```text
SEARCH_OUTBOX_SHADOW_ENABLED=true
SEARCH_SHADOW_CANARY_ENABLED=true
```

The outbox worker must first report `search_shadow.status=ready` and
`search_shadow.ready=true` from `/api/health/resources`. A canary request is
always explicit (`reader=shadow`), queries both persisted indexes in parallel,
returns the shadow hits, and reports whether production/shadow work IDs and
ordering agree. It never changes the normal reader.

Prepare a fixed JSON corpus outside the evidence directory. Each item may be a
string or an object with a bounded result limit:

```json
[
  "known title token",
  { "query": "known tag or path token", "limit": 100 }
]
```

Then run:

```powershell
$env:PERF_COOKIE='arislist_session=...'
node scripts/perf/run-search-shadow-canary.mjs `
  --base-url http://127.0.0.1:8787 `
  --corpus D:\gate-inputs\search-corpus.json `
  --output perf-results/search-canary-n100-001/scenario-results.jsonl `
  --runtime-output perf-results/search-canary-n100-001/search-canary-runtime.json

node scripts/perf/summarize-results.mjs `
  --input perf-results/search-canary-n100-001/scenario-results.jsonl `
  --output perf-results/search-canary-n100-001/summary.json
```

Evidence stores only per-query SHA-256 digests, byte lengths, result counts,
latency and comparison booleans; query text, URL query strings and credentials
are excluded. The runtime Gate fails if the shadow index changes revision or
leaves `ready`, any request bypasses the shadow reader, runtime query counts do
not match the corpus, or any ID/order mismatch, failure or rejection occurs.

The Rust integration test proves that the explicit path queries two real
Tantivy directories and detects an injected shadow-document replacement. The
Node evaluator proves the artifact acceptance rules. These tests are not a
passed real-corpus or N100 Gate; keep both flags disabled by default.
