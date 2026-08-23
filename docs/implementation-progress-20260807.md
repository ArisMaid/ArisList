# 实施进度：S1 增量搜索对账与 reader 门禁

更新时间：2026-08-23（Inventory/Search 实验路径门禁收敛）

本批次继续按 `nas-n100-4g` 计划落地；生产默认开关仍保持关闭，实验性开关只在隔离
Docker 验收容器中显式启用。

当前正式验收口径固定为 Docker `1 CPU / 4GiB / 256 PID / 双客户端 / 300 秒（5 分钟）`
混合负载；Docker approximation 结果直接作为本项目正式验收依据。真实 NAS/N100、30 分钟、
24 小时和额外 soak 不再作为本阶段阻塞项；qmediasync 继续 Deferred。

## 当前批次范围（2026-08-22）

本轮只将以下两项纳入实验性功能落地和验收计划：

| 功能 | 当前状态 | 实验性边界 |
| --- | --- | --- |
| Inventory Scanner | 已落地，按 kind 默认关闭、可隔离启用 | 有界 WalkDir 批次、generation/lease fence、失败不删除、root/status 诊断、watcher 定向事件和 Catalog promotion gate；不改变未选中 kind 的 legacy writer |
| Search shadow / incremental reader | 已落地，默认关闭、显式 canary 和 reader gate | `shadow-v3` outbox 增量写入、SQLite/Tantivy 事实 hash 对账、双读 canary、fail-closed、损坏恢复、armed 后增量 reader 和 prewarm |

本轮暂不规划或不作为验收条件：Facet bitmap、Derivative Cache v2、JPEG downscale；
qmediasync 仍为 Deferred。它们保留现有默认关闭代码和回滚边界，不进入本轮实施、性能
稳定窗口或完成度统计。

### 两项实验功能的当前证据

- Inventory：Novel `10,000/10,000`、Comic `10,000/10,000`、CoserPicture `8,000/8,000`、
  Audio `10,000/10,000` 和 Gallery `700,000/700,000` 均已有独立 Docker
  `1 CPU / 4GiB / 256 PID` kind-scale artifact。各 root 的
  `present_files == last_discovered`，`status=idle`，`last_error=null` 才计入通过。
- Search：当前 fixed corpus 的 shadow baseline、事实对账和增量 reader 已在 Docker
  近似环境完成；对账要求 SQLite/Tantivy document count、missing/unexpected/duplicate/
  invalid 和双 revision 全部一致，reader 未 armed 时受控 `503`，armed 后才允许 `200`。

所有正式稳定窗口仍统一使用 Docker `1 CPU / 4GiB / 256 PID / 双客户端 / 300 秒`；真实
NAS/N100 不作为本轮阻塞验收项。

### 2026-08-22：Audio/Gallery reconciliation inventory 短读快照

- reconciliation 的 Audio/Gallery inventory 路径此前在 `file_inventory` 上直接使用连接池
  流式查询，单个作品最多读取 20,000 条路径/资产；现在改为显式
  `TrackedReadTransaction`，仅覆盖 SQL 流读取，成功后立即提交，超限或元数据错误时显式
  回滚，再进入 inspector/file I/O。
- 新增回归确认 Audio 与 Gallery helper 各自产生一次已提交 read snapshot，`active == 0`，
  且 `implicit_rollbacks` 不增加；validator 已增加函数体级契约，防止回退为
  `fetch(db.pool())`。
- 该批只收敛默认关闭的 reconciliation/control-plane 读边界，不改变媒体 HTTP、Catalog
  ownership、Search、qmediasync 或正式稳定窗口。格式化、validator 和两个定向
  reconciliation 回归已通过；正式稳定窗口仍唯一采用 Docker `1 CPU / 4GiB / 256 PID /
  双客户端 / 300 秒（5 分钟）`混合负载。

### 2026-08-22：Search outbox 文档批次短读快照

- shadow outbox consumer 在 claim 后读取作品、标签和搜索文档时，改用一个短
  `TrackedReadTransaction`；查询完成立即提交，再进入 Tantivy `spawn_blocking`，不把 SQLite
  快照跨到索引写入或 merge。
- 新增回归确认该批次产生一次已提交 read snapshot、无活动快照残留且不增加
  `implicit_rollbacks`；项目 validator、fmt 和串行 outbox 定向测试通过。
- 该改动只触及默认关闭的 S1 shadow consumer 读边界，不改变生产搜索、媒体 HTTP、ownership
  或 qmediasync；正式稳定窗口仍唯一采用 Docker `1 CPU / 4GiB / 256 PID / 双客户端 /
  300 秒（5 分钟）`，不追加更长 soak。

- 同批又将 outbox、shadow index 和 reconciliation 三个状态 helper 改为短 tracked read
  wrapper，统一通过对应的 `*_in` 查询读取并立即提交；新增回归确认三次读取均完成且无
  implicit rollback。
- shadow fact reconciliation 的 before/after 状态现在各使用一个短快照，快照在进入和离开
  Tantivy/filesystem I/O 前结束；after 快照同时提供 Catalog/Search revision，保留原有 stale
  fence。新增回归确认 outbox 与 shadow 状态只占用一个 read snapshot。

### 2026-08-22：Inventory coordinator 队列状态短读收敛

- Novel、Comic/CoserPicture、Audio、Gallery coordinator 的待处理 root 列表现在共用一个
  keyset 短 tracked read helper；每个 root drain 后的 terminal count/error 也合并为一个快照，
  不再分别 checkout pool。
- drain 无事件时的 failed/pending phase 读取同样合并为一个短快照；快照只覆盖 SQL，随后才
  更新 coordinator 状态，未改变 lease、generation、retry 或 promotion 语义。
- 新增回归确认三个 helper 各自提交、无活动 snapshot/implicit rollback；validator、fmt 和
  定向回归均已通过。该改动不改变媒体 HTTP 路径、默认 ownership、qmediasync 或正式稳定
  窗口口径。

## 2026-08-22：EPUB fallback 章节 manifest 有界分页

- `GET /api/works/{id}/epub` 新增 `cursor`/`limit` 分页契约，单次最多返回 `500` 章，默认页为
  `200` 章，并返回 `total` 与 `next_cursor`。小于等于 `500` 章的无参数请求继续返回完整目录，
  维持旧客户端兼容；大书的无参数请求自动降级为有界首页，避免误请求一次性传输 10,000 章。
- fallback 阅读器改用 `Map<chapterIndex, chapter>` 保存已加载目录页，以 `total` 维护滚动总高、
  进度分母和恢复位置；目录虚拟窗口进入新页时按 200 章边界请求，AbortController 与 generation
  fence 防止切换作品后旧请求回写。章节 HTML 仍按索引独立请求，因此下一章/恢复到远端章节不需要
  先下载完整目录。
- 本批不改变 Foliate 主阅读路径、EPUB ZIP 标题探测预算、实验性开关或 qmediasync 状态。Rust
  分页边界回归、前端 TypeScript/production build 和项目 validator 已通过；正式稳定窗口仍只
  采用 Docker `1 CPU / 4GiB / 256 PID / 双客户端 / 300 秒（5 分钟）`混合负载。

### 本批正式近似验收复测

- 当前代码构建镜像 `arislist:n100-sim-epub-page-r1`，独立容器 `n100epubpage-app-1`，边界为
  `1 CPU quota / 4GiB memory / 256 PID / RESOURCE_PROFILE=nas-n100-4g`，目标端口 `8999`。
  Artifact：`perf-results/docker-n100-scale-1cpu/unified-scale-epub-page-r1-5m/run.json`。
- 根 Gate、媒体矩阵和混合矩阵均为 `passed`；AMD 宿主的 `n100-model` 仍是唯一 preflight
  差异，按当前 approximation 正式口径不阻断。300 秒双客户端混合负载完成 `82` 轮、
  `39,360` 请求，八个场景均 `4,920/4,920` 成功，失败 `0`、HTTP 503 `0`；媒体矩阵为
  `240/240`。
- 混合 total P95/P99（ms）：Catalog `75.45/79.59`、标签筛选 `102.74/172.50`、图库
  冷/热 `90.67/95.51` / `90.33/94.46`、漫画页 `279.12/294.06`、CoserPicture 页
  `289.43/304.24`、音声 Range `88.36/94.52`、轻小说摘要 `77.15/82.04`。系统样本
  `293` 条，`memory.current` 峰值约 `129.6MiB`，WAL 最大 `86,552B`，SQLite busy、连接池
  获取超时/错误、资源等待超时、writer queue 和 OOM 均为 `0`。

## 2026-08-22：Catalog reconciliation 批量事实比较

- 将五类媒体 reconciliation 的 Legacy Catalog 比较从逐作品多组 SQL 改为每页最多
  `256` 个作品的批量读取：主 `works/scanner_works`、扫描资产流式 multiset、扫描标签、
  扫描 external IDs，以及包含 user-owned assets 的 `work_stats` 聚合均按批次执行；结果
  按 candidate 顺序还原，缺失、字段漂移、scanner ownership/fingerprint、封面和统计语义
  与原路径保持一致。
- 单个作品的资产事实仍只保留计数、模加和 XOR 摘要，不把 20,000 图片/音轨重新收集到
  Rust `Vec`；每批参数数目低于 SQLite 默认限制。该边界覆盖 10,000 音声、图库作者目录、
  轻小说和 10,000+ 漫画对账时的查询放大风险。
- 本批不改变默认 ownership、Inventory/Search/Facet/Derivative 开关或 qmediasync Deferred
  状态；正式稳定窗口仍只采用 Docker `1 CPU / 4GiB / 256 PID / 双客户端 / 300 秒（5 分钟）`
  混合负载。针对性 reconciliation `25 passed / 0 failed`，全量 Rust `345 passed / 0 failed /
  3 ignored`，Node perf `46 passed / 0 failed`，Clippy、前端 production build、validator、
  fmt 和 diff check 均通过。

## 2026-08-22：Inventory coordinator ownership rollback fence

- coordinator 获取 root lease 和领取 `catalog-upsert`/`catalog-delete` 事件时，
  现在都会在同一 SQL 条件中再次确认该 kind 的 `authoritative_writer='catalog-v2'`。
  即使 lease 已建立后发生管理端 rollback，旧 lease 也不能继续领取 v2 事件；pending
  事件保持未处理，随后由 legacy reconcile 接管，避免回滚窗口出现双写。
- 新增回归覆盖 legacy ownership 拒绝 lease，以及模拟 rollback 后 pending v2 事件不被
  claim；Inventory 定向测试为 `40 passed / 1 ignored`。随后全量 Rust 为 `345 passed /
  0 failed / 3 ignored`，Node perf 为 `46 passed / 0 failed`，Clippy、前端 production
  build、validator、fmt 和 diff check 均通过；未改变默认开关、qmediasync Deferred 状态
  或正式稳定窗口口径。
- 当前正式验收仍只采用 Docker `1 CPU / 4GiB / 256 PID / 双客户端 / 300 秒（5 分钟）`
  混合负载；不追加 30 分钟、24 小时或真实 NAS/N100 验收。

## 2026-08-22：增量 production reader 启动预热接入

- 修复启动路径缺口：`SEARCH_READER_PREWARM_ENABLED=true` 且增量 reader 已通过持久化
  reconciliation gate 时，现在会在 readiness 前打开并保留 `shadow-v3` reader；此前该
  预热只覆盖 legacy `search-index-v2`，增量 reader 的首次请求仍可能承担 mmap/segment
  setup 成本。
- 新路径复用 `validate_incremental_reader` 的同一 revision/ownership/对账门禁，最多等待
  30 秒；门禁未通过、索引损坏或超时均只记录 warning，继续保持 fail-closed/lazy 路径，
  不会绕过门禁、不填充 candidate cache，也不改变默认开关或 ownership。
- 新增回归确认 shadow reader 只打开一次、candidate cache 仍为空。`SEARCH_READER_PREWARM_ENABLED`
  和 `SEARCH_INCREMENTAL_READER_ENABLED` 继续默认关闭，qmediasync 仍 Deferred。

## 2026-08-22：稳定窗口口径锁定与增量搜索损坏恢复分流

- 当前阶段唯一正式稳定窗口为 Docker `1 CPU / 4GiB / 256 PID`、双客户端连续
  `300 秒（5 分钟）`混合负载。30 分钟、24 小时、真实 NAS/N100 和额外 soak 不再是本阶段
  验收条件；更长运行只能作为非验收诊断记录。
- 当前正式目标规模 artifact
  `perf-results/docker-n100-scale-1cpu/unified-scale-current-5m-r2/run.json` 已记录
  `stable_window_seconds=300`，根 Gate、媒体矩阵和混合矩阵均通过；AMD 宿主 CPU 只保留在
  provenance，`approximation` 模式不阻断通过判定。
- 修复 S1 恢复分流：增量 reader 的 Tantivy 损坏不再误标记 legacy production index，也不再
  排 legacy rebuild；现在会将 `shadow-v3` 标记为 `degraded`，排队
  `rebuild-shadow-search-index`，并以受控 503 fail-closed。普通 legacy reader 的恢复行为不变。
- 新增回归覆盖 shadow 状态、恢复作业类型和 legacy 作业不误排；本批不打开
  `SEARCH_OUTBOX_SHADOW_ENABLED`、`SEARCH_INCREMENTAL_READER_ENABLED`、Inventory、Facet
  bitmap、Derivative Cache v2 或 qmediasync。

## 2026-08-22：恢复修复后的正式 1 CPU 五分钟复测

- 使用包含本批恢复修复的镜像 `arislist:n100-sim-current-r3`，容器边界为 `1 CPU quota /
  4GiB memory / 4GiB swap / 256 PID / RESOURCE_PROFILE=nas-n100-4g`。目标 SQLite 约
  `393.4MiB`，事实仍为 `40,000 works / 740,000 assets / 800,000 work_tags`，八类媒体
  目标由当前数据库只读生成并绑定端口 `8998`。
- 正式 artifact 为
  `perf-results/docker-n100-scale-1cpu/unified-scale-current-5m-r3/run.json`。根 Gate、
  media Gate 和 mixed Gate 均为 `passed`；AMD 宿主 CPU 只使 `n100-model` preflight 保持
  failed，`approximation` 模式按正式口径允许通过。300 秒双客户端混合负载完成 `82` 轮、
  `39,360` 请求，八个场景均 `4,920/4,920` 成功，失败 `0`、HTTP 503 `0`；媒体矩阵
  `240/240` 成功。
- 混合 total P95/P99（ms）：Catalog `75.38/79.56`、标签筛选 `102.51/169.85`、图库
  冷/热 `90.50/94.85` / `90.49/94.69`、漫画页 `280.18/294.67`、CoserPicture 页
  `288.78/303.07`、音声 Range `88.31/94.28`、轻小说摘要 `76.99/81.95`。
- 系统样本 `292` 条；`memory.current` 峰值 `133,386,240B`（约 `127.2MiB`），WAL 最大
  `70,072B`，SQLite busy、pool acquire timeout/error、resource wait timeout、writer
  queue 和 OOM 均为 `0`。资源等待最大约 `295.6ms`、累计约 `356.2s`；cgroup `cpu.stat`
  记录 `1,884` 次 throttle、累计约 `465.5s`，说明 1 CPU 是余量瓶颈但未造成请求失败。
- 本批最终验证：Rust `343 passed / 0 failed / 3 ignored`，Node perf `46 passed / 0 failed`，
  Clippy、前端 production build、项目 validator、fmt 和 diff check 均通过。该窗口继续只
  作为 5 分钟正式稳定窗口，不追加 30 分钟、24 小时或真实 NAS/N100 验收。

## 2026-08-21：正式验收 CPU 边界收紧为 1 CPU Docker 模拟

- 根据当前项目决策，本阶段取消真实 NAS/N100 设备验收；不再把 N100 实机、NAS HDD、
  温度/降频、HDD await、真实设备 RSS/PSS 和真实设备 24 小时 soak 作为完成条件。
- 4GiB memory、4GiB swap、1 CPU quota、256 PID、`RESOURCE_PROFILE=nas-n100-4g` 的
  受限 Docker 结果改为本项目当前验收口径下的正式性能证据。现有 Windows/AMD 宿主差异
  仍记录在 artifact provenance 中，但不再阻断本阶段完成判定；此前 4 CPU 结果降级为开发
  参考，不能作为正式通过证据。
- 已完成的五类本地媒体单/双客户端矩阵、Catalog reconciliation、迁移恢复和固定 corpus
  近似 Gate 可按该口径计入验收；但小型样本不能自动覆盖目标规模，仍需补齐受限 Docker
  下的 70 万图库、1 万以上漫画、8000 CoserPicture 压缩包、1 万音频文件和 1 万 EPUB
  的规模/长尾模拟，以及浏览器 DOM/heap 和冷启动样本。
- qmediasync 继续冻结并延期，不属于本阶段验收范围；其既有兼容链路不回滚。
- 稳定窗口进一步收紧为 1 CPU 下 5 分钟（300 秒）双客户端混合负载；此前 30 分钟记录
  保留为历史参考，不再作为当前正式验收要求。

本节验收口径优先于后续历史记录中仍保留的“等待真实 N100/NAS”表述；那些表述仅保留为
当时的风险背景，不再作为本阶段未完成项。

## 2026-08-22：媒体 reconciliation 静态契约补齐

- `scripts/validate-project.mjs` 现在同时校验 Audio/Gallery reconciliation API 路由和
  `RECONCILE_AUDIO_JOB_TYPE`、`RECONCILE_GALLERY_JOB_TYPE` promotion 常量，五类媒体
  的当前对账证据不会因路由或门禁回退而静默缺失。
- `scripts/perf/run-n100-gate.mjs` 将正式 `--mixed-duration-seconds` 固定为 `300`；更长
  的诊断只能使用显式 `--mixed-rounds` 兼容模式，并不会被标记为稳定窗口验收。
- 本批不改变业务读写路径、实验性默认开关或 qmediasync；正式稳定窗口仍只执行
  `1 CPU / 4GiB / 256 PID / 双客户端 / 300 秒（5 分钟）`混合负载，不追加 30 分钟、
  24 小时或真实 NAS/N100 验收。
- 验证结果：Rust `341 passed / 0 failed / 3 ignored`，Node perf `45 passed / 0 failed`，
  项目 validator、前端 production build 和 `git diff --check` 均通过。

## 2026-08-22：搜索候选双修订 freshness fence

- Catalog/Tantivy 候选现在同时携带 `catalog_revision` 与 `search_revision`。Catalog、标签、
  关系或搜索源变化只推进其中一个修订时，旧候选也会 fail-closed，不会在 Catalog 修订
  未变化的情况下静默复用过期标签结果；动态 Facet、works/random/facets/counts/collections
  路径沿用同一 tracked snapshot 校验。
- `RevisionSnapshot`、候选缓存和分页查询均使用同一组双修订；新增回归覆盖“仅 search
  revision 变化”和 Catalog revision 变化两种拒绝路径。该修复不打开 Search shadow、
  Facet bitmap、Inventory 或 qmediasync，也不改变正式稳定窗口。
- 本轮验证：Rust `342 passed / 0 failed / 3 ignored`（全量耗时约 `57.7s`）、Clippy、
  Node perf `46 passed / 0 failed`、项目 validator、前端 production build、fmt check
  和 `git diff --check` 均通过。正式验收仍只采用 `1 CPU / 4GiB / 256 PID / 双客户端 /
  300 秒（5 分钟）`混合负载。

## 2026-08-22：双修订后当前 release 目标规模正式模拟复测

- 使用当前代码构建的 `arislist:n100-sim-current`，独立容器 `n100current-scale`，边界为
  `1 CPU quota / 4GiB memory / 4GiB swap / 256 PID / RESOURCE_PROFILE=nas-n100-4g`；
  目标矩阵重新绑定当前端口并由 `prepare-media-targets` 只读生成。Catalog v2、Inventory、
  Search shadow/canary、Facet bitmap、Derivative Cache v2 和 qmediasync 均关闭或未触达。
- `perf-results/docker-n100-scale-1cpu/unified-scale-current-5m-r2/run.json` 根 Gate 为
  `passed`（`execution_mode=approximation`）。正式 300 秒双客户端混合窗口完成 `82` 轮、
  `39,360` 请求；八个场景均 `4,920/4,920` 成功，失败 `0`、HTTP 503 `0`；目标规模
  媒体矩阵 `240/240` 成功。混合 total P95/P99（ms）：Catalog `75.67/80.34`、标签
  筛选 `102.66/171.04`、图库冷/热 `90.81/95.65` / `90.64/94.26`、漫画页
  `282.44/295.68`、CoserPicture 页 `289.76/308.37`、音声 Range `88.67/94.74`、
  轻小说摘要 `77.55/82.48`。
- 294 个系统样本中，cgroup `memory.current` 峰值约 `130.9MiB`（结束 `129.9MiB`，上限
  4GiB），WAL 最大 `61,832B`；SQLite busy、pool acquire timeout/error、resource wait
  timeout、writer queue 和 OOM/OOM-kill 均为 `0`。tracked pool acquire 最大等待约
  `76.9ms`，资源等待最大单次约 `298.7ms`，累计约 `360.0s`；容器 `cpu.stat` 在窗口后
  记录 `1,901` 次节流、约 `463.3s` 节流时间，说明 1 CPU 仍是余量瓶颈但未造成请求失败。
- 预检只因宿主 CPU 为 AMD Ryzen 而非 Intel N100 失败；在 `--mode approximation` 下按当前
  正式口径允许通过，并保留该 provenance。该结果覆盖当前 release 的目标规模数据库、预览、
  标签筛选、Range 起播和 5 分钟混合窗口；不外推真实 TB 物理文件、HDD await、50MP/ZIP
  长尾或浏览器解码行为。正式稳定窗口不追加 30 分钟、24 小时或真实 NAS/N100 验收。

## 2026-08-22：U1 1 万轨浏览器窗口验证完成

- 使用 `n100keysetaudio-app-1` 的 `10k track fixture`，从首轨连续切换至
  `10000/10000`。初始详情只缓存 `128/10000` 条轨道，播放列表只渲染 `25` 个按钮；
  末端仍为 `25` 个按钮、DOM `362` 个节点，说明队列窗口没有随总轨数线性展开。
- 切轨进度为 `1 -> 11 -> 4111 -> 6111 -> 10000`；最后 `3889` 次切轨耗时
  `17.714s`，此前 `2000` 次批次约 `9.2s`。末端 `performance.memory.usedJSHeapSize`
  为 `12.1MiB`（总 heap `20.5MiB`），相对初始约 `9.4MiB` 为小幅绝对增长，未观察到
  与 10,000 轨数量成比例的 DOM/heap 增长。
- 浏览器资源条目 `250`（API `245`、媒体流 `230`），资源耗时 P50/P95/max 为
  `2.6/4.4/26.1ms`。合成夹具没有真实音频正文，因此媒体元素产生约 `200` 条控制台
  错误；这只限制真实解码/起播结论，不影响本次分页、窗口回收和切轨状态验证，也不计入
  服务端正式 Gate 失败。
- 原始证据保存于 `output/playwright/audio-10k-track-window-20260822.json`；本项补齐
  U1 的 1 万轨 Network/DOM/heap/切轨窗口证据，但不替代正式服务端 `1 CPU / 4GiB /
  256 PID / 双客户端 / 300 秒`混合窗口。正式稳定窗口仍只执行 5 分钟，不追加 30 分钟、
  24 小时或真实 NAS/N100 验收，qmediasync 继续 Deferred。

## 2026-08-22：Comic/CoserPicture 阅读器有界自适应预取候选

- 服务端已有 `reader-1280/1920` 派生页和 single-flight，但 Derivative Cache v2 默认关闭时
  `size` 请求会回退原始 ZIP 页；前端因此新增 `readerDerivativesEnabled` 门禁，只有健康状态
  明确报告派生缓存已启用时才创建阅读器预取对象，避免把大原图误当成预取数据。
- 阅读器按 `saveData`、`effectiveType` 和 `deviceMemory` 选择 0、1 或 2 页的相邻范围，最多
  保留 4 个 `HTMLImageElement`，翻页、尺寸变化、切换作品和卸载时删除旧 `src` 并清空引用。
  预取只使用当前标准 `reader-1280/1920` URL，不扩大 manifest、页面数组或服务端并发。
- 前端 production build、项目 validator 和 `git diff --check` 已通过；该候选不打开
  `DERIVATIVE_CACHE_V2_ENABLED`，不改变默认回退路径，也不涉及 qmediasync。
- 派生缓存专项随后在独立隔离容器中完成了代表性 Comic/CoserPicture 冷生成、热读取、
  并发同页响应和主动断流复核；该专项不打开默认开关，也不替代正式稳定窗口。

## 2026-08-22：Derivative Cache v2 隔离阅读页复核

- 容器 `n100p6b-deriv-app` 使用 `4GiB memory`、`4GiB swap`、`1 CPU quota`、`256 PID`、
  `RESOURCE_PROFILE=nas-n100-4g`，仅显式开启 `DERIVATIVE_CACHE_V2_ENABLED` 与
  `JPEG_THUMBNAIL_DOWNSCALE_ENABLED`；Inventory、Search shadow、Facet bitmap、qmediasync
  均未启用或未触达。原始证据位于
  `perf-results/docker-n100-scale-1cpu/derivative-p6b-data/`。
- 冷/热代表页均为 HTTP 200：Comic 冷/热 total P50/P95 为 `8.664/86.400ms`、
  `11.282/27.260ms`；CoserPicture 冷/热为 `57.306/105.578ms`、`13.187/31.571ms`。
  这些是小样本代表页，不外推到 50MP 或全量 TB 物理文件。
- 同页并发 5 的 10 请求复核：Comic `10/10` 成功，响应体均 `407,937B`，total P50/P95/P99
  `17.731/34.901/34.901ms`；CoserPicture `10/10` 成功，响应体均 `2,022,019B`，
  `114.298/203.015/203.015ms`。该样本的 health `coalesced_requests` 没有增量，
  因此只能确认并发响应一致和无错误，不能把它解释为生成阶段 single-flight 命中率证明。
- 主动断流各 5 次（读取 64KiB 后取消）均为 HTTP 200 且 `intentionally_aborted=true`；
  Comic/CoserPicture total P50/P95 分别为 `11.313/27.736ms`、`8.774/22.700ms`，实际读取
  字节因网络 chunk 为约 `65.5–130.9KiB`。断流后 archive/processing/inflight permit
  全部归零，resource wait timeout、SQLite busy/acquire timeout、writer queue 和派生
  generation failure 均为 `0`。
- 最终 `memory.current` 约 `203MiB/4GiB`，`memory.events` 的 `oom`/`oom_kill` 均为 `0`；
  `derivatives` 为 `3` 个 ready 文件、约 `0.72MiB` resident、generation failure `0`。
  容器停止时 Docker 返回 `137` 是停止窗口后的 SIGKILL，`OOMKilled=false`，不计为负载失败。
- 结论：派生缓存路径的代表页冷/热和取消回收在 1 CPU 受限容器下没有出现错误或资源泄漏；
  默认仍保持关闭，正式验收仍只采用上一节的 1 CPU、双客户端、300 秒混合窗口。浏览器
  Network/heap、真实 50MP/ZIP 长尾和盘满/只读故障不在本专项范围内。

## 2026-08-22：P7 音轨队列会话取消与响应代际隔离

- 音频播放栏原先虽然只缓存最多 5 页（640 条）轨道元数据，但 `workAssets` 请求没有
  在切换播放会话或卸载时统一取消；旧请求晚到时可能把上一部作品的页合并到新队列，或
  旧请求的 `finally` 覆盖新会话的 loading 状态。现在为队列增加 `AbortController` 和
  generation fence：新会话先递增 generation、取消旧请求并清空请求引用，响应提交前再次
  校验 signal/generation，旧请求的清理逻辑也不会修改新会话状态。
- `scripts/validate-project.mjs` 增加静态契约，锁定请求 signal、会话 generation、取消和
  响应前 fence；既有 `audio-queue.test.mjs` 两项分页/回收回归继续通过。该改动不改变每页
  128 条、最多 640 条缓存、当前轨道附近 24 条渲染窗口或 qmediasync 范围。
- 本批验证：Rust `340 passed / 0 failed / 3 ignored`，`cargo fmt --all -- --check`、
  前端 `npm run build`、音轨队列 Node 回归、项目 validator 和 `git diff --check` 均通过。
- 该项修复的是客户端会话竞态，不等同于 1 万轨真实浏览器 DOM/heap Gate；正式 1 CPU/5
  分钟混合窗口的服务端证据保持不变。

## 2026-08-22：搜索查询输入边界与 perf runner 契约收敛

- `/api/search` 原先只限制 `limit`，未在进入 Tantivy reader、blocking 查询任务或候选缓存
  前限制 `q` 的字节数；超长查询会把不必要的解析和临时分配带入 1 CPU/4GiB 交互路径。
- 现在服务端统一使用 `MAX_SEARCH_QUERY_BYTES = 512`：直接搜索、Catalog candidate、
  `SearchRuntime` reader/candidate 路径和底层 `query_open_index` 均 fail-closed 返回 400，
  不会先打开 reader 或写入候选缓存 key。搜索 shadow/incremental 两个 perf runner 的
  corpus 校验同步为 512 字节，避免 runner 接受服务端必然拒绝的查询。
- 新增 Rust 边界回归和项目 validator 静态契约；验证结果为 Rust `341 passed / 0 failed /
  3 ignored`、Node perf `45 passed / 0 failed`、前端 production build 和 validator 通过。
  该改动不改变默认 production reader、shadow/canary 开关或 qmediasync 范围。

## 2026-08-22：P6B 改动后的目标规模 1 CPU/5 分钟复核

- 使用当前 release 镜像 `arislist:n100-sim-p6b` 和独立容器 `n100p6b-app`，边界为
  `4GiB memory`、`4GiB swap`、`1 CPU quota`、`256 PID`、`RESOURCE_PROFILE=nas-n100-4g`。
  合成 fixed corpus 保持 `40,000 works`、`740,000 assets`、`800,000 work_tags`、
  逻辑 `29.101TB` 和八项媒体目标；Derivative Cache v2、Inventory、Search shadow、
  Facet bitmap 与 qmediasync 均关闭/未触达。
- 正式 300 秒双客户端混合窗口完成 `83` 轮、`39,840` 请求，八个场景均为 `4,980/4,980`
  成功，失败 `0`、503 `0`。总耗时 P95/P99（ms）：Catalog `75.69/79.84`、标签筛选
  `101.31/149.86`、图库冷/热 `90.35/94.64` / `90.10/94.22`、漫画页
  `281.18/293.23`、CoserPicture 页 `287.84/299.75`、音声 Range `88.00/93.06`、
  轻小说摘要 `77.24/82.71`。目标规模媒体矩阵 `240/240` 成功，根 Gate 在
  `--mode approximation` 下为 `passed`；AMD 宿主 CPU 型号仍仅作为 provenance 记录。
- 采样中 cgroup `memory.current` 峰值约 `124.1MiB`，结束约 `123.4MiB`，WAL 最大
  `53,592B`；SQLite busy、pool acquire timeout/error、resource wait timeout、writer
  queue depth 和 OOM/OOM-kill 均为 `0`。tracked pool acquire 最大等待约 `77.8ms`，资源
  等待最大单次约 `293.5ms`，累计约 `359.7s`。cgroup `cpu.stat` 记录 `3,343` 个周期中
  `1,872` 次节流、累计节流约 `459s`，确认 1 CPU 是当前余量瓶颈，但未导致请求失败。
- 复核只证明本批前端预取候选没有破坏默认关闭路径和目标规模稳定窗口；由于派生缓存仍关闭，
  它没有测量 reader 派生图命中收益。随后已在隔离容器显式开启 Derivative Cache v2，
  对代表性漫画/CoserPicture 页完成冷生成、热读取、同页并发和断流释放复核；该专项结果
  已在上方单独记录。浏览器 Network/heap 尚未执行，不能把小样本派生页结果外推为全量
  50MP/ZIP 长尾验收，也不改变默认关闭或正式 5 分钟稳定窗口口径。

## 2026-08-21：目标规模合成数据 1 CPU/5 分钟正式模拟 Gate

- 在隔离容器 `n100scale1-app`（`http://127.0.0.1:8968`）中使用 `4GiB memory`、
  `4GiB swap`、`1 CPU quota`、`256 PID` 和 `RESOURCE_PROFILE=nas-n100-4g`。
  合成数据库事实为 `40,000 works`、`740,000 assets`、`800,000 work_tags`、
  `2,048 tags`，数据库文件约 `393.4MiB`，逻辑资产容量为 `29.101TB`：图库
  `700,000` 图片、漫画 `10,000`、CoserPicture `8,000`、音频 `10,000`、EPUB
  `10,000`。副本只把每类一个代表资产指向现有合法样本，未复制 TB 级正文。
- 统一 artifact 位于 `perf-results/docker-n100-scale-1cpu/unified-scale-5m/`，
  `run.json` 和媒体 Gate 均为 `passed`。5 分钟双客户端混合负载完成 `82` 轮、
  `39,360` 请求；8 个场景均为 `4,920/4,920` 成功，失败 `0`、HTTP 503 `0`。
- 混合窗口 total P95/P99（毫秒）为：Catalog `76.07/80.14`、标签筛选
  `103.17/172.45`、图库冷/热 `91.16/95.76` / `91.07/95.35`、漫画页
  `282.26/296.84`、CoserPicture 页 `289.76/307.33`、音声 Range 起播
  `88.82/94.79`、轻小说摘要 `77.93/82.91`。目标规模单次矩阵 `240/240` 成功，
  各项也通过同一媒体阈值。
- 293 个系统样本中，cgroup `memory.current` 峰值约 `127.5MiB`（上限 4GiB），
  WAL 最大 `41,232B`，SQLite busy `0`，pool acquire timeout/error `0`，resource
  wait timeout `0`，writer queue 最大深度 `0`，Docker `memory.events` 的 OOM/OOM-kill
  均为 `0`。资源等待累计约 `359.6s`、最大单次约 `300ms`，说明 1 CPU 在混合窗口
  已出现明显节流，但没有产生 503 或超时；该项作为容量余量风险保留。
- 预检唯一失败项是宿主 CPU 型号为 AMD 而非 N100；在 `--mode approximation` 下按
  约束策略允许继续，CPU 型号和 `cpu.stat` 节流证据仍保留。qmediasync、Inventory、
  Search shadow、Facet bitmap 和 Derivative v2 均未开启或未触达。
- 该结果正式覆盖“目标规模数据库 cardinality + 代表性预览/筛选/起播 + 1 CPU 五分钟
  混合窗口”。它不等价于 600/500 作者目录的真实 HDD 冷扫描、50MP JPEG、ZIP/CBZ
  长尾/损坏包、浏览器 DOM/heap 或全量物理文件存在性；这些属于后续专项风险数据，
  不得从本次合成副本的 P95 外推。

## 2026-08-21：本地五类媒体 Docker 4GiB/4CPU 冷预览与双客户端混合复测

- 在隔离容器 `n100nextpreview-app-1`（`http://127.0.0.1:8948`）中重跑本地五类
  媒体矩阵。容器实际边界为 4GiB memory、4GiB swap、4 CPU quota、256 PID；只挂载
  本地 Novel、Comic、CoserPicture、Gallery、Audio，`JPEG_THUMBNAIL_DOWNSCALE_ENABLED=true`，
  Derivative Cache v2、Inventory、Search shadow 和 qmediasync 均未参与。
- 单客户端矩阵输出到 `perf-results/local-next-media-gates-8948/`：`240/240` 请求成功，
  0 失败、0 个 503。total P50/P95/P99（毫秒）为：Catalog 首页
  `2.41/4.29/23.25`，标签筛选 `2.25/3.67/22.62`，图库冷缩略图
  `11.94/13.37/30.90`、热缩略图 `12.80/14.93/71.59`，漫画页
  `3.26/5.33/1530.12`，CoserPicture 页 `320.51/367.25/477.66`，音声 Range 起播
  `12.63/14.77/30.67`，轻小说摘要详情 `2.36/3.57/23.80`。漫画 P99 的约 1.53s
  是首个冷页长尾，不能由 P50 代表。
- 对独立未缓存 JPEG 样本逐个请求：15MP 首次 `534–600ms`（5 个样本），29MP 首次
  `672–687ms`（6 个样本），40.4MP 首次 `1,702ms`（1 个样本）；同一 40.4MP 资源
  再次命中缓存约 `31ms`。重复 30 次组的中位数约 `11–12ms` 主要是缓存命中，不能当作
  冷生成速度；当前没有 50MP 样本。
- 双客户端混合矩阵输出到 `perf-results/local-next-media-mixed-8948/`：`480/480` 成功，
  0 失败、0 个 503。total P95/P99（毫秒）为：Catalog `6.90/40.13`、标签筛选
  `7.95/45.93`、图库冷/热 `27.13/53.71`、漫画 `364.86/531.52`、CoserPicture
  `444.46/593.16`、音声 Range `21.91/82.75`、轻小说 `7.14/44.92`；统一 Gate 判定
  `passed`。
- 混合运行前后 cgroup current memory 约 `19.72MiB → 29.80MiB`（上限 4GiB）；SQLite
  busy、pool acquire timeout/error、resource wait timeout、活动长读快照和 writer queue
  均为 0。qmediasync 计数为 0，但这只是确认本地矩阵未触达 qmediasync，不是 qmediasync
  测试或验收。
- 结论：当前本地请求路径在受限 Docker 下满足本阶段交互 Gate；新增代码没有必要仅为已
  通过的混合窗口强行扩大并发或缓存。首次大 JPEG 和漫画冷页仍是风险项，应由用户在真实
  N100/NAS HDD 上用 15/24/50MP、ZIP 长尾和冷盘矩阵复测；本地小样本不能外推到 7TB/70
  万图库、10TB 漫画、6TB/8000 压缩包、6TB 音声或 1 万本轻小说。
- 本轮继续严格排除 qmediasync；不新增改动、专项测试、性能 Gate 或 ownership promotion，
  既有 STRM/VFS/云缓存兼容链路保留，后续另行规划。

## 2026-08-21：Catalog 书架有界下一页预取与混合 Gate 计数修正

- `frontend/src/catalog/useCatalog.ts` 现在会在当前书架页成功加载后，后台预取紧邻的
  下一页；预取复用同一游标、查询条件和最多 5 页 LRU，不改变服务端分页，也不会把
  全库作品重新放回浏览器。查询条件、刷新或组件卸载时会取消所有预取请求，后台失败
  只丢弃预取结果，前台翻页仍可重试并显示真实错误。
- `scripts/perf/run-media-mixed-load.mjs` 修复 `--rounds N` 被错误提前停止的问题；现在
  固定轮次会确实完成 N 轮，duration 模式仍按时间和安全上限停止。新增回归验证 3 轮、
  覆盖数和 URL 脱敏契约，项目 validator 也锁定该边界。
- 修正后的 Docker approximation `perf-results/docker-n100-r14/media-mixed-c2-r3-fixed/`
  完成双客户端 3 轮、1,440 请求：`1440/1440` 成功、0 失败、0 个 503。total P95/P99
  （ms）为：Catalog 首屏 `7.55/34.06`，标签筛选 `6.95/39.35`，图库冷/热缩略图
  `23.55/54.75`、`22.52/55.03`，漫画页 `420.06/597.61`，CoserPicture 页
  `460.34/621.09`，音声 Range 起播 `25.14/56.41`，轻小说摘要详情 `11.98/46.92`。
  健康证据保持 SQLite busy、pool acquire timeout、resource wait timeout 均为 0；
  cgroup memory 约 `30.1MiB → 34.2MiB`，资源池最终无等待者。该结果仍是 Windows/AMD
  Docker approximation，小型媒体样本不等价于 TB 级 NAS/HDD 或真实 N100。
- 本批验证：前端 production build、Rust `337 passed / 0 failed / 3 ignored`、Clippy、
  Node perf `43 passed / 0 failed`、项目 validator、fmt 和 diff check 均通过。本批不涉及
  qmediasync；新增改动、专项测试、性能 Gate 和 ownership promotion 继续延期，既有兼容
  代码不回滚。

## 2026-08-21：watcher ownership 快照收敛

- watcher 在判断一个事件 burst 是否可以走 Catalog v2 changed-key 路径时，原先会
  对 novel/comic/CoserPicture/audio/gallery 分别 checkout 一次 SQLite。现在由
  `catalog_v2_kinds_snapshot` 在一个短 `TrackedReadTransaction` 中读取完整 ownership
  集合，再在内存中完成五类判断；事务在事件 journal 写入和文件 I/O 前提交。
- `event_requires_legacy_scan_inner` 与 `journal_paths_for_specs_with_kinds` 共用该快照，
  不改变未选中 kind 的 legacy fallback、rollback 触发完整 reconcile 或 changed-key
  容量上限语义。新增回归确认五类 ownership 判断只产生一个 tracked snapshot，并且
  没有 implicit rollback。
- watcher/inventory 定向测试 `38 passed / 0 failed / 1 ignored`；全量 Rust
  `334 passed / 0 failed / 3 ignored`，Clippy、Node perf `43 passed / 0 failed`、
  前端 production build、项目 validator、fmt 和 diff check 均通过。

## 2026-08-21：当前 release r11 Docker 近似复测完成

- 使用当前工作树构建的 `arislist:n100-sim-r11`，独立容器 `n100simr11-app-1`，
  端口 `8838`、独立数据目录 `perf-results/docker-n100-sim/r11-data`；容器边界为
  4GiB memory、4 CPU quota、256 PID，profile 为 `nas-n100-4g`。该环境运行在
  Windows/AMD 宿主上，结果只能标记为 Docker approximation，不能作为真实 N100/NAS
  验收。
- `/api/catalog/reconciliation` 顺序采样 50 次：`50/50` 成功、0 失败、0 个 503，
  total P50/P95/P99 为 `2.664/3.817/24.570ms`。双并发、双客户端采样 100 次：
  `100/100` 成功、0 失败、0 个 503，total P50/P95/P99 为 `2.418/3.219/13.314ms`。
- 两组采样的 health 证据均保持 `sqlite_busy_errors=0`、pool acquire timeout/error 为
  0、`implicit_rollbacks=0`；150 个 endpoint 请求对应 150 个新增 tracked snapshot，
  另有健康探针自身的快照，证明该管理读路径未出现长快照或隐式回滚。采样结束容器内
  `cgroup_memory.current` 约 `17.8MiB`，上限 `4GiB`，WAL `8,272 bytes`。
- 原始证据位于
  `perf-results/docker-n100-sim/reconciliation-baseline-r11/`（顺序/双并发 JSONL、
  summary、前后 health snapshot）。本轮 Docker 近似采样已完成；真实 N100/NAS、TB
  级媒体库、HDD 冷盘长尾和长期稳定窗口仍由用户在目标设备执行。

## 2026-08-21：Catalog reconciliation 启动基线同快照收敛

- `reconcile_target_inner` 的启动基线现在由 `reconciliation_baseline` 在一个短
  `TrackedReadTransaction` 中读取：Catalog revision、当前 kind 的 enabled roots、上一轮
  reconciliation evidence、ownership、library scanner lock 和 kind-scoped pending scan
  events。事务在进入文件/归档 inspector 之前提交，不会把 SQLite 快照延伸到后续 I/O。
- 原先的 `catalog_revision`、`root_snapshot`、`previous_evidence` 与
  `reconciliation_prerequisite_error` 独立 pool checkout 被移除；after revision/root
  stale fence 和 `begin_run`/`persist_result` 的写入闸门保持不变。
- 新增 `reconciliation_baseline_uses_one_tracked_read_snapshot` 回归与 validator 静态契约，
  验证该路径恰好产生一个 tracked snapshot、无 implicit rollback，并保留根就绪和前置条件
  检查。
- Catalog reconciliation 定向测试当前为 `25 passed / 0 failed`；全量 Rust、Clippy、
  Node perf、前端 build、validator、fmt 和 diff check 均已通过。当前 Docker 定向采样
  也已完成（见上节）。该改造不改变默认 ownership、实验性开关或真实 N100/NAS Gate
  状态。

## 2026-08-21：Catalog reconciliation overview 同快照收敛

- `/api/catalog/reconciliation` 原先分别读取 Catalog revision、reconciliation state、
  每个媒体 kind 的 enabled roots 和 recorded diffs；这些 pool 查询可能跨越 inventory
  generation，并在 5 类媒体上重复 checkout。现在统一使用一个短生命周期
  `TrackedReadTransaction`，revision、root digest、状态和 diff 来自同一 SQLite snapshot。
- enabled roots 改为一次有序查询后按 kind 分组生成 digest，避免在事务内按 kind 进行
  N+1 查询；没有 root 的 kind 仍使用空 root snapshot digest，保持原有 stale/current 语义。
- 新增 `reconciliation_overview_uses_one_tracked_read_snapshot` 回归和 validator 静态契约，
  防止该管理读路径退回独立 pool 查询。该事务只覆盖诊断响应，不延伸到 reconciliation
  inspector、文件 I/O 或网络响应，也不改变 ownership 和默认开关。
- 定向 reconciliation 测试 `24 passed / 0 failed`；随后全量 Rust 回归为
  `332 passed / 0 failed / 3 ignored`，Node perf `43 passed / 0 failed`，Clippy、fmt、
  前端 production build、项目 validator 和 `git diff --check` 均通过。真实 N100/NAS
  Gate、TB 级扫描和长期稳定窗口状态不变。
- 使用包含本批改动的 `arislist:n100-sim-r8`（8828，4GiB/4CPU/256 PID）健康容器做
  50 次顺序只读 HTTP 近似采样：全部 HTTP 200，P50/P95/P99 为
  `1.919/2.971/3.828ms`，无错误；health 前后 read-snapshot 计数差为 51，扣除末尾
  health 探针本身后，50 个 endpoint 请求各产生 1 个 tracked snapshot，implicit rollback
  增量为 0。该结果只代表受限 Docker 小型数据库，不代表真实 NAS 管理端延迟。

## 2026-08-21：Docker 近似前置检查增加 CPU quota 证据

- `check-n100-environment.mjs` 现在会读取同一容器 cgroup v2/v1 的 CPU quota，
  并在显式传入 `--max-cpu-cores` 时 fail-closed 校验上限；未传入该选项时，真实
  裸机 N100 不会因为没有 CPU cgroup quota 被误判为不完整。
- 在本机 `arislist:n100-sim` 容器内实测到 `memory.max=4294967296`、
  `cpu.max=400000 100000`（4.0 cores）和 `pids.max=256`；Docker 配置边界与
  容器内 cgroup 证据一致。当前宿主仍是 Windows/AMD，故 preflight 仍按设计失败，
  不能将该结果升级为真实 N100 Gate。
- 生产运行镜像不包含 Node；preflight 现在支持宿主侧 `--container <name|id>`，通过
  `docker exec` 读取目标容器内的 `/proc`、cgroup 和 diskstats，再请求健康端点，避免
  “在镜像内装测试工具”或“只检查 Docker inspect 配置”的弱证据。当前容器证据保存在
  `perf-results/dev-preflight-20260821-container-sdd`，Linux/memory/CPU/profile/health/
  `sdd` 均通过，唯一失败是 CPU 型号为 AMD Ryzen 9950X；因此仍是 approximation。
- 新增 CPU quota 的 passed/failed/incomplete 回归；当前 Node perf 全量为
  `39 passed / 0 failed`，项目 validator 通过。该改动只增强证据契约，不改变任何
  默认开关或 ownership。

## 2026-08-21：Inventory health 读路径合并为 tracked snapshot

- `/api/inventory/status` 原先分别查询 `library_roots`、pending events、pending
  Catalog keys 和 failed Catalog keys；现改为一个短 `begin_tracked_read_transaction()`
  读取并提交，避免健康响应跨扫描 generation，也减少 N100 下的 pool checkout 次数。
- 返回结构、路径脱敏和计数语义保持不变；不会把事务延伸到网络响应或媒体 I/O。
- 定向 Inventory 测试、完整后端回归均通过：`331 passed / 0 failed / 3 ignored`；
  Clippy `-D warnings`、fmt、Node perf `39 passed / 0 failed`、validator、前端构建和
  `git diff --check` 通过。其他 legacy 多查询入口仍按计划继续审计，真实 N100/NAS Gate
  状态不变。

## 2026-08-21：r7 Docker N100 近似 Gate 与连接池诊断口径复核

- 使用当前修正版 `arislist:n100-sim-r7`、独立数据/生成/封面缓存目录和 `8818` 端口，
  施加 4GiB hard memory、4 CPU quota、256 PID、`nas-n100-4g` profile。统一 artifact
  为 `perf-results/docker-n100-sim/unified-gate-20260821-r7/run.json`，根状态为
  `approximation`；唯一预检失败仍是宿主 CPU 为 AMD Ryzen 9 9950X，不是 Intel N100，
  Linux、4GiB、4 CPU、profile、health、`sdd` 和 provenance 均通过。
- 8 个媒体场景短 Gate 共 `240/240` 成功、0 失败、0 个 503；双客户端混合共
  `480/480` 成功、0 失败、0 个 503。混合负载 total P95/P99（毫秒）为：目录首屏
  `5.926/30.697`、标签筛选 `5.259/34.658`、图库冷/热缩略图
  `19.695/44.759`、`18.649/44.620`、漫画页 `352.640/469.235`、CoserPicture 页
  `412.037/498.277`、音声 Range 起播 `16.243/41.028`、轻小说摘要详情
  `5.181/38.198`。Comic 单场景冷请求有一个约 `1.565s` 的长尾，混合窗口 P99
  约 `469ms`，因此不能只看 P50 宣称所有冷页都无感。
- 系统 sampler 产生 11 条样本：cgroup current memory 约 `20.3–30.1MiB`，WAL
  稳定 `1,079,472 bytes`，pool 为 3–5 个连接且全部 idle，`sqlite_pool_saturated=0`；
  SQLite busy、pool acquire timeout/error、resource wait timeout、writer queue 均为 0。
  processing/inflight 观测预算分别为 `256MiB/16MiB`，archive worker 峰值为 2。
- 本轮修正了 `pool_saturated` 的诊断条件：只有 `pool_size >= max_connections` 且
  `idle_connections == 0` 才算饱和；池达到上限但连接空闲时不再误报。该修正不改变请求
  路径，只修正 health/sampler 的解释口径。
- 该 r7 结果仍只证明受限 Docker 下的请求路径、资源闸门和短混合负载稳定性；媒体 target
  仍是小型代表样本，不等价于 7TB/70 万图库、10TB 漫画、6TB/8000 CoserPicture 包、
  6TB 音声或 1 万本轻小说。真实 N100/NAS 冷盘长尾、HDD await、CPU 降频、TB 级扫描和
  24 小时稳定窗口仍由用户在目标设备验收。

## 2026-08-21：当前 release v24 migration/restore Gate

- 发现上一份 `migration-dev-r1g-v23-20260820-r3` artifact 使用的是 08-20 构建的
  旧 release 二进制，结果仍为 schema v23；该旧证据保留，不升级解释。随后重新构建
  当前工作树 release 二进制，并用同一份 quiesced v23 fixed corpus
  `perf-results/r1g-dev-20260820-v23/r1g-v23.sqlite` 重跑，输出到新的
  `perf-results/migration-dev-r1g-v23-20260821-v24-release/`。
- Gate 结果为 `passed`：服务副本启动约 `36,224ms`，迁移后 schema `v24`，主库
  `integrity_check=ok`；恢复副本同为 v24/`ok`。源库无 WAL（0 bytes），前后 SHA-256
  均为 `9904a9e42bccfe20ed7b2c03846dcef5188a1e671a4a4260ef5ba0da050c7c43`，确认
  migration runner 没有修改输入数据库。
- 该 Gate 证明当前 release 的 v23→v24 migration、启动和恢复副本契约在开发机 fixed
  corpus 上成立；36 秒启动时间包含迁移/启动工作，不应解释为媒体浏览延迟，也不替代
  真实旧库备份恢复、4GiB RSS/峰值、N100 CPU 或 NAS 存储 Gate。实验性开关和 ownership
  promotion 继续关闭。

## 2026-08-21：Docker v24 patched Search shadow / incremental reader Gate

- 修复了 shadow outbox worker 与 incremental reader 之间的资源/锁顺序问题：worker
  现在先取得唯一 `SearchWriter` 资源再拿 `SHADOW_OUTBOX_LOCK`，reconciliation worker
  进入轮询休眠前会释放 resource lease；incremental gate 的锁等待也绑定到 N100 profile
  的 interactive deadline，超时会 fail-closed，而不会无限等待后台索引任务。新增回归后，
  outbox 定向测试 `18 passed / 0 failed`。
- 当前 patched release 镜像 `arislist:n100-sim` 在 4GiB memory hard limit、4GiB
  swap 上限、4 CPU quota、256 PID 下重建成功。固定 40,000 works / 740,000 assets
  副本的 shadow canary 6/6 成功，SQLite/Tantivy work-ID hash 均为
  `517db064...aa44e9c`，missing/unexpected/duplicate/invalid 全为 0；请求 total
  P50/P95 约 `14.33/138.19ms`，无 ID/order mismatch、failure 或 rejection。
- 同一最终容器的 incremental reader rerun 6/6 成功，全部走 `reader=production`，
  `rebuilt=false`，total P50/P95 约 `12.22/46.52ms`；起止 pending、revision lag
  均为 0，reconciliation `passed` 且 `cutover_armed=true`。首次运行因在后台 baseline
  尚未稳定时采集 before 快照而按 evaluator 正确 fail-closed，随后 rerun 才计为通过。
- 新增 `run-search-cutover-probe.mjs`，并在本轮继续优化：`search_index_state` 为
  `building`/未 ready 时，gate 先做无锁状态预检再返回受控错误。新的全新隔离副本共有
  265 个 unarmed 样本，其中 264 个为 HTTP 503，P50/P95 约 `1.757/2.210ms`；首次
  building 请求也降至约 `2.396ms`，最终 cutover armed 后 HTTP 200 约 `3.558ms`。
  新证据位于 `perf-results/docker-n100-sim/search-shadow-v24-unarmed-fast/`；上一轮
  `6.813s` 的启动尾延迟 artifact 保留用于回归对比。
- 这批 Docker 证据仍是 Windows/AMD 上的 `approximation`：不证明 N100 IPC/频率/温度、
  NAS HDD await、真实旧库逐 kind 对账或 24 小时 soak；生产 Compose 和实验性默认开关
  仍保持关闭。新增 Docker override 的 `SIM_SEARCH_*` 变量仅允许隔离模拟显式 opt-in。

## 2026-08-20：Docker 4GiB fixed-corpus Facet bitmap 选择性 Gate

- 首轮使用 `genre:perf-tag-0001` 的尝试按设计未进入 bitmap：该固定库 hot tag
  覆盖全部 40,000 个作品，`tag_kind_counts` 预聚合路径可直接返回；服务健康状态为
  `idle/builds=0/queries=0`，因此不能把该失败误判为 bitmap 构建超时或内存不足。
- 改用只覆盖约一半作品的选择性 `genre:perf-tag-0002`，在 4GiB memory hard
  limit、4 CPU quota、256 PID 的 8790 容器中完成 Gate。固定库为 40,000 works、
  800,000 associations、2,048 tags；bitmap 构建 `1,958ms`，估算常驻内存
  `13,283,401 bytes`（约 `12.67MiB`），低于 `64MiB` 上限。
- 30 次逐次过期响应缓存的选择性 Facet 请求全部成功，total P50/P95/P99 为
  `10.947/13.386/18.043ms`；bitmap query 平均约 `0.077ms`、最大约 `0.305ms`，
  增量证据为 30 queries、0 fallback、0 scope rejection、0 build failure。首次
  warmup 的一次 fallback 属于预热路由，不计入 measured delta。
- 原始证据：`perf-results/docker-n100-sim/facet-bitmap-v24-selective/`；此前使用
  universal hot tag 的编排失败保留在 `facet-bitmap-v24/`，不覆盖、不删除。该结果仍
  是 Windows Docker approximation，不构成真实 N100/NAS HDD Gate；`FACET_BITMAP_ENABLED`
  和 ownership promotion 继续默认关闭。

## 2026-08-20：Docker 4GiB fixed-corpus Catalog 近似 Gate

- Docker Desktop Linux engine 在首次构建短暂 `RPC EOF` 后恢复；`arislist:n100-sim`
  镜像成功构建，隔离服务实际核对到 4GiB memory hard limit、4GiB swap 上限、4 CPU
  cgroup quota 和 256 PID 上限。该配置只模拟资源边界，不模拟 N100 IPC、频率、温度或
  NAS HDD seek/await。
- 将既有 fixed corpus 副本（40,000 works、740,000 assets、800,000 work-tag links，
  SQLite 约 392.9MiB）挂入第二个 8789 容器；当前二进制启动后自动迁移到 schema v24，
  容器健康检查通过，启动后观测 cgroup memory 约 186MiB。
- R1G 首轮 30 个并发三元组为 90/90 成功；持续 300 个三元组为 900/900 成功、0 个
  失败/503。持续轮次 total P95：works `23.775ms`、counts `27.367ms`、facets
  `7.042ms`；candidate cache 897 hits、2 misses、4 coalesced、2 evictions，reader
  reuse/single-flight/capacity 检查全部通过。
- 持续负载后的 health：观测内存约 229MiB，OOM/restart 为 0，SQLite busy、pool acquire
  timeout/error、resource wait timeout 均为 0；pool 曾达到 5 连接上限，tracked acquire
  最大等待约 `11.9ms`。Windows sampler 无有效 HDD await、RSS/PSS peak、温度/降频字段，
  因此仍不能把该 artifact 标记为真实 N100/NAS Gate。
- 证据：`perf-results/docker-n100-sim/r1g-v23-catalog/`。实验性开关和 ownership
  promotion 继续保持关闭。

## 2026-08-20：Docker 30 分钟双客户端媒体稳定窗口

- 使用同一 `nas-n100-4g` Docker approximation、8 个媒体场景、双客户端并发，运行
  `1800s` duration Gate；最终完成 144 个完整轮次，共 69,120 条记录，全部成功，0 个
  失败、0 个 503，coverage 对每个场景均为 8,640/8,640。同步 sampler 记录 1,820 个
  每秒样本，health failures 为 0。
- total P95/P99（ms）：Catalog 首屏 `7.85/35.13`，标签筛选 `7.76/39.30`，图库冷/热
  缩略图 `23.80/55.27`、`23.91/52.95`，漫画页 `403.32/552.09`，CoserPicture 页
  `459.03/587.96`，音声 Range 起播 `22.52/55.12`，轻小说详情 `8.40/47.57`。
- 4GiB cgroup memory current 的采样最高约 `41.9MiB`；WAL 全程约 `1.04MiB`；SQLite
  busy、pool acquire timeout/error、resource wait timeout 和 writer queue depth 均为 0。
  pool 曾达到 5 连接上限，tracked acquire 最大等待约 `33.2ms`；窗口结束时 archive、
  processing、inflight permit 均归零。容器结束时仍 healthy，OOMKilled=false，重启次数 0。
- 该结果只加强 Windows Docker approximation 的长期稳定性证据；Windows sampler 没有
  HDD await、RSS/PSS peak、CPU 温度/降频字段，媒体目录也不是目标 TB 级 NAS 根目录，
  因此真实 Linux/N100/NAS HDD Gate、冷盘长尾和 24 小时 soak 仍未完成。原始 artifact：
  `perf-results/docker-n100-sim/media-mixed-2c-1800s/`、
  `perf-results/docker-n100-sim/system-samples-1800s.csv`、
  `perf-results/docker-n100-sim/health-after-1800s.json`。

## 2026-08-20：开发机媒体 HTTP Gate 首轮完成

- 首轮媒体 Gate 发现 target 生成器的 Comic/CoserPicture URL 缺少实际路由要求的
  `/stream` 后缀，导致两类场景各 30 次请求均为 404；该问题属于验收工具路径错误，
  不是媒体读取性能失败。
- 修正 `scripts/perf/prepare-media-targets.mjs` 并增加 URL 回归；修正后的矩阵在隔离
  服务上覆盖 Catalog 首屏、标签筛选、图库冷热缩略图、漫画页、CoserPicture 页、
  音声 Range 起播和轻小说摘要详情，共 8 个场景、240 次请求，全部成功、0 失败、0 个
  503，Gate 状态为 `passed`。证据位于
  `perf-results/dev-media-runtime-20260820-a/media-gates-v2/`。
- Windows 开发机的单并发结果：Catalog 首屏 total P95 `4.47ms`、标签筛选
  `3.15ms`、图库冷/热缩略图分别 `4.41/3.81ms`、漫画首页面 `4.02ms`、CoserPicture
  首页面 `155.94ms`、音声 256KiB Range 起播 `3.89ms`、轻小说摘要详情 `2.59ms`。
  这些数值只代表当前小型本地媒体库和开发机，不代表 4GiB/N100/NAS HDD。
- 修复后 Node perf `36 passed / 0 failed`，项目 validator 和 `git diff --check` 通过。
  当前隔离服务仍使用 `nas-n100-4g` 配置，但运行环境为 Windows，cgroup、RSS/PSS、
  HDD await、冷盘长尾和混合客户端证据仍缺失；下一步是 Linux/N100/NAS 实机 Gate。

## 2026-08-20：双客户端开发机混合预检与 N100 前置拒绝

- 在同一隔离服务上并发运行两套完整媒体矩阵，累计 480 次请求；两个客户端均为
  `240/240` 成功、0 失败、0 个 503，媒体 Gate 均通过。混合运行期间各客户端的
  CoserPicture 首页面 total P95 约 `171–172ms`，音声 Range total P95 约 `4–17ms`，
  其余场景 total P95 约 `2.6–5.1ms`（仍为 Windows 本地库、单并发 target）。
- 运行后健康快照显示 SQLite `busy=0`、pool timeout `0`、writer queue `0`、资源等待
  timeout `0`，观测到的资源等待最大约 `19µs`；这些只是轻负载/开发机证据，不能替代
  30 分钟混合负载或 24 小时 soak。
- 执行 `check-n100-environment.mjs` 后，前置 Gate 按设计拒绝当前环境：平台为 `win32`、
  CPU 为 AMD Ryzen 9950X、cgroup memory limit 未捕获；resource profile/health 本身为
  `nas-n100-4g`/`ok`。该失败产物保留在
  `perf-results/dev-media-runtime-20260820-a/n100-preflight.json`，确认不能把开发机
  结果冒充 N100 证据。

## 2026-08-20：本机 Docker N100 近似模拟入口

- 新增 `docker-compose.n100-sim.yml`，作为生产 Compose 的独立 override：4GiB
  `mem_limit`、同值 `memswap_limit`、默认 4 CPU 配额、256 PID 上限、独立 8788
  端口、独立数据/生成/封面缓存目录，并强制关闭实验性开关。端口和 volume 使用
  `!override`，避免继承生产服务的 8787 或复用生产数据。
- 该入口只能模拟 cgroup 内存/CPU 时间、进程数、应用 governor 和队列行为；不能模拟
  N100 的 IPC/缓存/频率/温度，也不能把 Windows bind mount 变成 NAS HDD seek/await。
  因此不允许将 Docker 结果记作真实 N100 Gate，必须单独保存并标注为 approximation。
- 当前主机检测到 Docker CLI/Compose 二进制，但 Docker daemon 未在可用状态；尚未启动
  Desktop 或执行容器测试。项目 validator 已增加该 override 的隔离与近似声明检查。

## 2026-08-20：Docker 近似容器实测尝试

- Docker Desktop Linux engine 曾短暂恢复并通过 `docker version`，随后使用隔离 override
  执行 `up -d --build`。前端镜像阶段构建成功，服务端 Rust 构建进入依赖编译；Docker
  Desktop 在构建过程中因 backend RPC `EOF` 退出，之后 `dockerDesktopLinuxEngine`
  named pipe 消失，未产生可启动的应用容器。
- 该失败发生在 Docker Desktop 后端，不是 Cargo/项目编译错误；Compose 合并配置仍以
  `config --quiet` 通过，项目 validator、Node perf `38 passed` 和 diff check 通过。
- 当前没有把这次尝试记为媒体性能结果；正式 Docker 模拟 Gate 仍待稳定的 Docker
  daemon/engine 后重跑，真实 N100/NAS Gate 状态不变。

## 2026-08-20：scanner tag 复合读写边界收敛

- 继续审计 `db.rs` 生产区仍保留的 `fetch_*(&self.pool)`。媒体流、缩略图和
  manifest 前置的单语句查询保持为有界热路径，不为单次读取额外打开事务；需要多次
  读取的 Catalog、详情、图库页和媒体 source lookup 已继续使用 tracked snapshot。
- 发现 `link_current_scanner_tag` 原先先从 pool 读取当前 scanner lease，再另开写事务，
  存在租约替换窗口。现在由同一个 tracked writer transaction 读取 lease、校验 lease、
  写入 `work_tags`/`work_tag_sources`、更新 tag count 并发布 Search outbox；普通
  `link_tag`/`link_scanner_tag` 复用同一事务 helper，避免逻辑分叉。
- 新增 `current_scanner_tag_reads_lease_inside_the_tracked_write_snapshot` 回归，并在
  `scripts/validate-project.mjs` 增加静态契约，防止该入口退回跨 pool 读-写。
- 本批验证：后端 `328 passed / 0 failed / 3 ignored`，Node perf `36 passed / 0 failed`，
  Clippy、fmt、项目 validator、前端 production build 与 `git diff --check` 均通过。
- 该批只收敛事务一致性与可观测边界，不打开实验性开关、不切换 ownership，也不构成
  Linux/N100/4GiB/NAS HDD 性能 Gate。真实旧库、媒体根目录、冷/热预览、筛选、翻页、
  音频起播、RSS/PSS、WAL/busy、HDD await 和双客户端稳定窗口仍待目标 NAS 执行。

## 2026-08-20：扫描 revision fence 合并为单一快照

- `scan_all_locked` 的扫描前、扫描后和刷新后的 revision 读取现在使用
  `Db::revision_fence_snapshot`，在一个短 tracked read snapshot 内同时读取 Catalog、
  activity 和 Search source revision，避免两个独立 pool checkout 拼接 fence。
- 保留只读取单一 revision 的轻量 helper 语义；扫描边界才使用三计数器快照，避免把
  普通热路径全部升级为事务。
- 新增 `revision_fence_snapshot_reads_catalog_and_search_from_one_snapshot` 回归，并由
  validator 检查 scanner 不再在该边界调用独立 `search_source_revision()`。
- 本轮针对性测试、Clippy、fmt 和 validator 已通过；完整回归应以本批收尾命令重新留证。

## 2026-08-20：大规模 scale Gate 重新留证与 provenance 收敛

- 在当前工作树、schema v24 下重新执行 `node scripts/perf/run-scale-gates.mjs`，Inventory
  700,000 行与 Derivative ledger 700,000/1,400,000 行均通过，artifact 为
  `perf-results/scale-dev-20260820-v24-provenance`，旁路 raw log 同名 `.log`。
- 当前 Windows 开发机结果：Inventory `15,053ms`，固定批次最大 `1,024` 行、
  `169,985` bytes；Derivative 700k 插入 `8,395ms`、容量查询 `1,124µs`、淘汰
  `22,647ms`；1.4M 插入 `19,523ms`、容量查询 `1,520µs`、淘汰 `47,061ms`；两档
  `integrity_check=ok`，淘汰查询继续只使用 `idx_derivatives_eviction_lru`。
- scale wrapper 现在与 baseline 共用 `scripts/perf/provenance.mjs`，artifact 记录
  commit、branch、dirty 状态/hash、schema v24、平台/Node 和完整安全 feature-flag
  allowlist；不会记录 cookie、密码或任意环境变量。
- 该 artifact 只证明开发机上的批次/SQL 形状、完整性和可审计 provenance，不证明
  N100/4GiB RSS、CPU 降频、HDD await、媒体预览 P95 或混合负载；实验性默认开关仍关闭。

## 2026-08-20：schema 自动一致性与媒体 Gate target 准备

- `scripts/validate-project.mjs` 现在直接解析 `crates/server/src/migrations.rs` 的
  `Migration { version: ... }` 列表，检查版本从 v1 连续递增，并要求最新版本与
  `scripts/perf/schema-version.mjs` 的 `CURRENT_SCHEMA_VERSION` 一致。当前两者均为
  v24；未来追加 migration 后，性能工具契约不再只依赖“文件存在”而静默落后。
- 新增 `scripts/perf/prepare-media-targets.mjs`。它以只读 SQLite 连接选择有效图库图片、
  comic/CoserPicture 正归档、满足 Range 大小的音频轨道、EPUB 轻小说和高覆盖活动标签，
  输出 8 项完整媒体 Gate target；任一必需 kind 或代表性样本缺失即 fail-closed，拒绝
  覆盖已有 target 文件，不读取归档正文。
- 新增两项 target 准备回归，验证完整矩阵、Range/标签 URL 和无 source path 泄漏，以及
  缺少 CoserPicture 时不生成半套文件。`scripts/perf/README.md` 已补充 NAS 使用命令。
- 本轮最终验证：后端 `326 passed / 0 failed / 3 ignored`（329 项），Node perf `36 passed / 0 failed`，
  Clippy、fmt、项目 validator、前端 production build 和 `git diff --check` 均通过。
- 以上只完成验收编排与开发机回归；未执行真实旧库、Linux/N100/4GiB cgroup、目标 NAS
  HDD、实际媒体根目录或冷/热混合负载 Gate，所有实验性默认开关继续关闭。

## 2026-08-20：当前 schema 版本契约收敛（v24）

- 发现 migration 已追加到 v24，但 R1G synthetic fixture、migration Gate、prewarm
  说明和初始化测试仍引用 v23；这会让下一轮固定 corpus 或旧库验收使用过期 schema
  契约，属于验收前置阻断。
- 新增 `scripts/perf/schema-version.mjs` 作为性能工具的显式当前版本常量，并将固定
  corpus profile 更新为 `r1g-40k-740k-800k-v6` / schema v24；migration Gate 的通过
  条件、测试夹具、R1G prewarm 文案及 Rust 初始化测试均同步到 v24。
- 项目 validator 现在要求该版本契约文件存在，避免未来 migration 追加后工具静默滞后。
- 验证：后端 `326 passed / 0 failed / 3 ignored`，Clippy、fmt、validator 和 Node perf
  `36 passed / 0 failed`；前端构建仍保持通过。以上仍不是 N100/4GiB/NAS 实机 Gate。

## 2026-08-20：legacy scanner Search outbox 发布边界补齐

- legacy scanner 的最终作品提交现在统一经过 `finish_scanner_work_in_transaction`；
  小作品的完整 snapshot、音声/图库的大作品 chunk-finalize、scope tombstone 都在其
  最终事务中写入 Search outbox。这样作品、资产清理、标签清理和搜索 upsert/delete
  不会出现“Catalog revision 已变化但 outbox 没有事实”的窗口。
- 轻小说/音声 enrichment、作品 metadata/cover/资产更新、外部标签与 scanner 标签关联
  也会在同一写事务中 coalesce 对应的 `upsert` outbox；标签 metadata 改变时按关联作品
  集合式标记，避免逐作品执行搜索更新。outbox 使用统一 payload version 和幂等的
  `(work_id, catalog_revision)` 覆盖语义。
- 增加 scanner snapshot/enrichment 回归，验证 outbox revision 与最终 Catalog revision
  一致、重复写入不产生重复行，Search gate fixture 也明确确认 baseline 会确认已覆盖的
  初始 outbox。该批没有开启 shadow worker、incremental reader 或 ownership promotion；
  legacy reader 仍保留 full rebuild 回退，待真实逐 kind outbox 对账后再切换。

本批次验证：后端 `324 passed / 0 failed / 3 ignored`、Clippy、fmt、项目 validator、
前端 production build 和 perf 回归均需在本轮收尾重新执行；当前针对性与全量 Rust 回归
已通过。以上仍是开发机证据，不构成 N100/4GiB/NAS Gate。

## 2026-08-20：Search source revision fence 收尾与回归

- 补齐 Search source revision 改造后的 migration、状态结构、snapshot 与测试夹具；
  v16→v24 迁移回归会真实重放新增 schema。
- legacy freshness 同时验证 Catalog revision 与 Search source revision；标签 metadata
  或关联变化即使不推进 Catalog revision，也会使旧索引 fail-closed。无 live work 引用的
  标签维护不会无意义推进全局 Search revision。
- 同一事务的作品/标签 fan-out outbox 行复用一个 Search revision；claim/ack 回归确认旧
  claim 不能确认同一作品后续 Search revision 的新事实。
- 最终开发机验证：后端 `326 passed / 0 failed / 3 ignored`（共 329 项）、Clippy、fmt、项目 validator、
  前端 production build、perf 工具 `33 passed / 0 failed`。这些不构成 Linux/N100/4GiB/NAS
  Gate；实验性 shadow、incremental reader、inventory、derivative cache、facet bitmap
  和 ownership promotion 默认仍关闭。

## 2026-08-20：legacy 搜索索引状态写入与 Outbox 事务边界收敛

- legacy `production-v2` 搜索索引在重建开始时先持久化为 `ready=0/status=building`；
  构建失败会记录 `status=degraded` 和有界错误文本，只有完整 Tantivy snapshot 构建、
  outbox 前缀确认和状态更新均成功后才恢复 `ready/status=ready`。并发查询因此会在重建
  窗口 fail-closed，不会把正在原地更新的索引当作可用索引。
- 启动 prewarm 对 revision-current 但 Tantivy `meta.json` 损坏的目录会先改名为带 PID/
  时间戳的 `*.corrupt-*` quarantine，再创建新索引，不删除旧目录。运行中的 production
  搜索或 Catalog candidate query 若遇到同类损坏，会把状态标记为 degraded、去重排队一次
  `rebuild-search-index` 并返回 retryable 503，而不把底层 Tantivy 错误直接暴露为可继续使用的
  查询路径。
- `record_legacy_search_index_ready`、legacy rebuild outbox acknowledgement、shadow
  baseline/error 状态更新以及 shadow outbox claim/ack/release 均改用
  `Db::begin_tracked_transaction`；状态更新要求恰好命中一行，缺失状态行会显式失败，
  避免迁移异常被静默当成 ready。
- 新增回归覆盖状态行缺失、building/degraded 期间 freshness fence 和 Search Outbox
  tracked transaction 路径。实验性开关、shadow ownership 和生产 reader cutover 继续关闭。

本批次最终验证：后端 `322 passed / 0 failed / 3 ignored`、Clippy `-D warnings`、fmt、
项目 validator、前端 production build 和 `33 passed / 0 failed` perf 工具回归均通过。该批仍
不构成真实 N100/4GiB/NAS Gate；下一步继续执行真实旧库迁移、搜索重建失败恢复和长读
WAL/busy 采集。

## 2026-08-20：图库封面与漫画 manifest 合并为有界 tracked snapshot

- 图库封面路径新增 `work_cover_source`：在一个短的 `TrackedReadTransaction` 内读取作品
  kind、显式图片封面和 Comic/CoserPicture 的归档 fallback，最多 materialize 两行资产，
  不再先读完整详情、再跨连接读取封面/归档。
- Comic/CoserPicture manifest 路径新增 `work_archive_and_meta`：归档源和用于 page-count
  回写的 `kind/meta_json` 在同一短 snapshot 内读取；事务提交后才打开/解析 ZIP，因此
  HDD 或远程归档操作不会持有 SQLite reader。
- 新增 snapshot 生命周期、显式封面/fallback 和 archive+metadata 回归；项目 validator
  增加路由契约，防止路径退回独立 pool 读取。该批没有改变缓存配额、默认实验开关或
  ownership。

本批次验证：后端 `322 passed / 0 failed / 3 ignored`、Clippy `-D warnings`、fmt、
项目 validator 通过；仍仅为开发机证据，真实 N100/4GiB/NAS 的封面冷/热 P95、manifest
首读延迟、SQLite WAL/busy 和 HDD await 尚未执行。

## 2026-08-20：Catalog 资产分页复用维护统计

- `/catalog/works/{id}/assets` 在未指定 role 时不再对每个游标页执行完整
  `COUNT(*) FROM assets`；若 `work_stats.computed_at` 有效，直接复用 trigger-maintained
  `asset_count`。音频 `role=track` 分支同时复用 `track_count`。
- 对迁移尚未回填或统计被标记为 pending 的作品，仍在同一个 tracked read snapshot 内
  精确查询事实表，保持旧库兼容和精确总数语义。
- 增加维护统计命中与 pending 精确回退回归，并把 validator 契约扩展到该计数边界；没有
  改变分页游标、排序或 API 响应形状。

本批次验证：后端 `323 passed / 0 failed / 3 ignored`、Clippy `-D warnings`、fmt、
项目 validator 和 diff check 通过；仍需在 N100/NAS 上测量 1 万章节/大型图库连续翻页的
P95、SQLite page-cache 命中、WAL/busy 与 HDD await。

## 2026-08-20：Catalog 统计回填由相关子查询收敛为批量聚合

- `backfill_stats_for_ids` 不再为每个作品分别执行 asset/tag/image/track/page 五组相关
  `COUNT/SUM` 子查询。现在对调用方已限制的 work ID 集合用 materialized CTE 聚合一次
  `assets`、一次 `work_tags`，再统一回写 `work_stats`。
- 最大 256 条的后台回填批次因此从最多 `256 × 5 = 1,280` 组相关事实聚合，收敛到两次
  按选中 ID 集合的聚合遍历；页面、音轨、图片和标签计数、catalog revision、已删除作品
  及 collection 回填语义保持不变。
- 回归扩大为同时验证 asset/tag/image/track/page 全部统计值；静态 validator 固定
  materialized aggregate 形状，避免未来退回逐作品相关查询。

本批次验证：后端 `323 passed / 0 failed / 3 ignored`、Clippy `-D warnings`、fmt、
项目 validator 和 diff check 通过。此为 SQL 形状与开发机正确性证据；40k/740k fixed
corpus 以及 N100/NAS 上的首屏 prime、后台回填耗时、writer hold、WAL/busy 仍须单独 Gate。

## 2026-08-19：U1 摘要详情纳入 tracked read snapshot

- `asset_mode=summary` 详情路径现在从同一个显式 `TrackedReadTransaction` 读取作品、
  维护统计、首批有界资产、标签和外部 ID；因此不会在扫描提交期间把不同内容修订的
  结果拼成一个详情响应。Legacy 路径仍保持兼容，并继续保留旧的完整资产语义。
- 现有 summary 资产上限、精确 `asset_count`/`track_count` 和 `assets_complete` 语义
  不变；单连接事务内的查询保持有意串行，以保证同一 SQLite snapshot，而不是通过多
  连接并发牺牲一致性。
- 回归新增 snapshot 生命周期断言：summary 请求建立并完成恰好一个 tracked read
  snapshot，成功提交、不产生隐式回滚且不遗留活动快照。定向测试和全量后端回归均通过。

本批次验证：后端 `311 passed、0 failed、3 ignored`，Clippy、fmt、前端 production
build、项目 validator 和 `33 passed、0 failed` perf 测试均通过。该证据仍是开发机
证据；summary 首屏 JSON 大小、1 万轨浏览器 Network/DOM/heap、N100/4GiB/NAS 冷热
延迟与混合负载 Gate 尚未完成。

## 2026-08-19：图库封面请求消除完整详情读放大

- `work_cover` 不再先读取完整 `WorkDetail`。现在先读取 `kind + cover_asset_id`，有
  明确图片封面时只读取该资产；漫画/CoserPicture 没有可用图片封面时只按
  `work_id + role=archive` 读取归档源。图库封面请求因此不会因作品包含数千或数万张
  图片而把全部资产行 materialize 到内存。
- 新增 `work_kind_and_cover_asset_id` 最小查询 helper，并保留已有
  `work_asset_by_role` 的角色/MIME 过滤和覆盖索引；静态 validator 契约禁止封面路径
  重新调用 `work_detail(work_id)`。
- DB 回归覆盖 kind/cover ID 读取、封面资产绑定与角色查询计划；定向封面/媒体测试、
  全量后端回归均通过。

本批次验证：后端 `311 passed、0 failed、3 ignored`，Clippy、fmt、前端 production
build、项目 validator 和 `33 passed、0 failed` perf 测试均通过。该优化只证明读路径
边界和开发机回归，不替代真实图库 70 万图片的冷/热封面 P95、RSS 和 NAS HDD Gate。

## 2026-08-19：轻小说 enrichment 消除章节资产读放大

- 轻小说远程匹配后台任务原先调用完整 `work_detail` 只为取得标题和既有 metadata；
  现在使用 `work_title_and_meta` 单次查询，10,000 章的 EPUB 不再在 enrichment
  请求中 materialize 全部资产、标签和外部 ID。
- 保留 not-found 和成功 enrichment 的 metadata merge、标题回退及 fingerprint
  fence 语义；新增 10,000 章节资产回归和静态契约，防止后台路径重新调用完整详情。

本批次验证：后端 `312 passed、0 failed、3 ignored`，Clippy、fmt、前端 production
build、项目 validator 和 `33 passed、0 failed` perf 测试均通过。该优化仍是本地读路径
证据，不能替代 1 万本轻小说在 N100/NAS 上的扫描、enrichment 队列、SQLite busy/WAL
与混合浏览 Gate。

## 2026-08-19：Catalog works 页与 revision 绑定同一 read snapshot

- `/catalog/works` 的最终页读取现在在一个显式 `TrackedReadTransaction` 内完成；
  `catalog_revision` 与 `activity_revision` 也通过同一 SQLite connection 读取后才提交。
  因此扫描或 typed writer 在请求期间提交时，不会把一页作品与另一 revision 的计数器
  拼接到同一响应。
- stats 尚未计算时仍保持原有行为：先结束读快照，执行一次有界 stats backfill，
  再重新打开快照重读；不会在读事务内等待或嵌套写事务。既有 keyset cursor、搜索候选、
  tag/collection 过滤和最多一次重试语义均不变。
- 新增 `revision_snapshot_with_connection` helper、作品页 snapshot 生命周期回归和
  validator 静态契约，防止该路径退回 pool 分离读取。

本批次验证：后端 `313 passed、0 failed、3 ignored`，Clippy、fmt、前端 production
build、项目 validator 和 `33 passed、0 failed` perf 测试均通过。该证据仍是开发机
证据；40k works 的真实 SQLite P95/P99、冷/热首屏、游标长读快照/WAL busy 和 N100/NAS
混合负载 Gate 尚未完成。

## 2026-08-19：32MiB 搜索合并策略与固定 corpus Gate

- Tantivy 搜索 writer 保持单索引线程、单 merge 线程；当 writer heap 不超过 32MiB 时，
  `LogMergePolicy.min_num_segments` 提高到 32，避免 N100/NAS 文件系统在最终 baseline
  写入期间过早启动大规模 merge；高内存 profile 继续使用 Tantivy 默认策略。该策略不提高
  N100 默认 writer heap，仅改变低内存档的合并时机。
- 在隔离 v22、40,000 works 的固定 corpus 上以 32MiB heap 完成 shadow baseline：
  40,000 documents，SQLite/Tantivy ID hash 一致，missing/unexpected/duplicate/invalid
  均为 0，固定 6 条查询全部成功，无 canary ID/order mismatch、failure 或 rejection。
- 同一副本重启后开启 shadow/canary/incremental reader 进行增量 reader Gate：
  `cutover_armed=true`，persisted reconciliation 为 `passed`，pending/revision lag 为 0，
  6/6 production-reader 查询成功；无 rebuild、503 或 outbox failure，总耗时 P50 约 9.25ms、
  P95 约 42.86ms，TTFB P95 约 42.54ms。
- 证据目录为
  `perf-results/search-shadow-dev-r1g-32m-fixed-fc7fc7034c9e4892a34f203452df2897/`，
  包含 `summary.json`、`incremental-summary.json`、`incremental-reader-runtime.json` 和
  脱敏 scenario JSONL。该证据只代表开发机固定 corpus；不替代 Linux/N100/NAS、真实
  70 万资产、delete/tombstone 稳定窗口或 production ownership cutover。

本批新增低内存 merge policy 阈值回归，随后必须重新执行后端、Clippy、fmt、前端构建和
perf 脚本全量验证；所有实验性开关仍保持关闭。

## 2026-08-19：重复 32MiB Search Gate 与 runner 启动竞态修复

- 首次重复执行暴露两个验收编排问题：服务 `/api/health` 已就绪但 shadow baseline 仍在后台
  构建；以及用于 Gate 的合成副本仍保留五类 `catalog_kind_ownership=legacy`。前者会让
  `before` 快照错误记录 `building`，后者会让 shadow 长期停留在 `shadow`；两种情况均不应
  被当作通过。runner 现对 reconcile 的临时 503 在 30 秒窗口内按 250ms 重试，并在采集
  `before` 前等待 shadow `ready`；degraded、未完成 ownership 或超时仍 fail-closed。
- 在隔离副本中仅为 Gate 前置条件设置五类 ownership 为 `catalog-v2`，源库不变；重新启动
  release binary、32MiB writer、shadow/canary/incremental reader 后，fixed corpus 和
  incremental reader 均通过：40,000 works/documents、SQLite/Tantivy hash 相同、missing/
  unexpected/duplicate/invalid 全为 0、6/6 查询成功、reconciliation `passed`、
  `cutover_armed=true`、pending/revision lag 为 0。
- 重复 Gate 结果：shadow canary 总耗时 P50 `11.392ms`、P95 `43.278ms`；incremental
  reader 总耗时 P50 `11.239ms`、P95 `42.636ms`；无查询失败、重建或 Tantivy `PermissionDenied`。
  原始证据位于
  `perf-results/search-shadow-dev-r1g-32m-repeat-20260819-150943/`。
- 同批保留了两个有意失败的隔离 artifact：一个因 ownership 未切换而在超时后 degraded，
  一个因 baseline 尚未稳定而被 runner fail-closed；它们证明前置条件缺失不会被误报为成功，
  不计入通过证据。

本批新增的 runner 变更需重新执行 perf 全量测试；上述重复 Gate 仍是 Windows 开发机证据，
不能替代 Linux/N100/NAS HDD、4GiB cgroup、真实旧库和目标媒体规模验收。

## 2026-08-19：Search/Catalog Gate runner 失败证据收敛

- `run-search-shadow-canary.mjs`、`run-search-incremental-reader.mjs`、
  `run-r1g-scenario.mjs` 和 `run-facet-cache-scenario.mjs` 现在会在首次写入前创建
  scenario/runtime artifact 的父目录，并拒绝 scenario 与 runtime 使用同一路径；避免从
  仓库外或新的结果目录启动时因目录不存在而丢失证据。
- 四个 runner 的健康探针、事实对账或前置请求抛错时，都会写入
  `status=failed` 的 runtime summary，包含脱敏后的错误和
  `runner-execution` 失败检查，再以非零退出；不会把失败误当作空样本通过，也不会覆盖
  已存在的 artifact。
- 新增共享 `scripts/perf/runner-utils.mjs`，统一目录准备、JSONL/JSON 原子产物写入和
  URL 查询参数/凭据脱敏；新增不可达目标回归，覆盖四个 Search/Catalog runner 的嵌套
  输出路径和失败 artifact；四个 runner 也拒绝在 `--base-url` 中携带 URL credentials，
  强制使用环境变量认证头。

本批次 perf 测试为 `27 passed、0 failed`，项目 validator 通过，`git diff --check`
无实际错误。该修复只提高验收证据可靠性，不改变服务端读路径、实验开关或 ownership；
真实固定 corpus、SQLite 事实对账和 N100/4GiB 性能 Gate 仍未执行。

## 2026-08-19：Inventory 700k 开发机固定批次 Gate

- 通过 `scripts/perf/run-scale-gates.mjs --skip-derivative` 显式执行被 ordinary
  regression 忽略的 700,000 行 Inventory Gate；原始日志和结构化结果保存在
  `perf-results/inventory-700k-dev-20260819/`。
- Gate 结果为 `passed`：耗时 `15,049ms`，固定批次最大 `1,024` 行，最大序列化批次
  `169,985` 字节，最终 present 行数和 inserted 数均为 `700,000`。

这只证明开发机 debug 构建的批次/SQLite 形状和结果完整性；尚未施加 4GiB cgroup、N100
CPU 或 NAS HDD，不能替代真实 Inventory 扫描耗时、RSS、WAL/busy、HDD await 和混合浏览
Gate。Derivative 规模 Gate 的既有两档证据不受本批影响。

## 2026-08-19：R1G v22 固定 corpus 夹具可复现性修复

- 默认 `R1G_40K_PROFILE` 从 schema v21/v3 升级为严格匹配当前 migration v22 的
  `r1g-40k-740k-800k-v4`；未来 schema 变更若未显式更新 profile 会继续 fail-closed。
- 新增契约测试确认默认 profile 的 schema/name；在新的结果目录中完成一次完整生成与校验：
  40,000 works、740,000 assets、2,048 tags、800,000 work_tags，逻辑媒体字节
  `29,101,000,000,000`，`integrity_check=ok`。
- 生成耗时 `103,705ms`，SQLite 文件 `392,847,360` 字节；分阶段结果和 manifest 位于
  `perf-results/r1g-dev-20260819-v22/`。

该结果仍是开发机夹具生成证据，不是 HTTP 延迟或 N100/RSS Gate；下一步可在该 v22 副本上
启动隔离服务，执行 Catalog/Search 固定 corpus 对账，随后再迁移到目标 NAS。既有 v21/v3
历史 artifact 保留，不被覆盖。

## 2026-08-19：R1G 冷启动 prime 延迟单独留证

- `run-r1g-scenario.mjs` 现在把首个 works/counts/facets 并发 triplet（prime）独立写入
  runtime artifact 的 `prime_summary`，包括请求数、失败数和 TTFB/total 的 P50/P95/P99；
  既有 warm records 和 warm Gate 不改变。
- 在 v22 40k/740k 夹具的隔离服务上重新执行：prime 三请求全部成功，TTFB P95
  `4,649.858ms`、total P95 `4,650.161ms`；随后 warm Gate 仍通过，works/counts/facets
  的 warm P95 分别约 `15.852ms`、`21.825ms`、`2.141ms`（30 个 triplet、90 条记录）。
- 新增 runner 回归，使用延迟 mock 服务确认 prime 延迟不会被丢弃；结果位于
  `perf-results/r1g-dev-20260819-v22/evidence-v2/` 和 `evidence-cold/`。

该证据揭示了“warm 浏览顺畅但新进程首屏较慢”的独立问题：约 4.65 秒主要来自首次
Catalog 读/缓存预热，不能仅用 warm P95 判定启动后首屏流畅。该结果仍是开发机、非
N100/4GiB/NAS HDD；下一步应在目标硬件测量 prime 是否可接受，并决定是否需要启动预热、
按 kind 分批初始化或进一步拆分首屏查询。

## 2026-08-18：CBZ/ZIP 归档 manifest 持久化候选

- Comic/CoserPicture 的页名清单现在可以按源归档路径写入 migration v22 的
  `archive_manifest_cache` 表。条目用源文件字节数和 mtime 纳秒值校验，单条最大 8MiB，
  Comic/CoserPicture 共用总预算 128MiB；损坏、过期、超限或包含非图片条目时静默回退到现有 ZIP
  中央目录枚举，不会用不可信清单删除或覆盖业务数据。
- 持久化与最旧条目淘汰在同一 SQLite 写事务内完成，触发器维护 resident bytes/entries；
  首次冷读仍建立现有有界 archive pool，持久化只作为后续重启/缓存淘汰的优化。manifest
  分页接口命中有效缓存行时只解析有界 JSON，不打开 ZIP archive pool；正文页流和归档
  封面仍按需打开 ZIP，不改变源数据和回滚路径。
- 新增 manifest 源 fence、非法页名、大小/总量边界和“manifest 请求不占用
  archive_stream”回归；
  未增加配置开关，也未改变任何默认实验性 flag。

本批次定向回归后，完整 Rust 回归为 `294 passed、0 failed、3 ignored`；Clippy
`-D warnings`、fmt、前端 production build、项目 validator 和 18 项 perf 单测均通过。
这些仍是开发机证据。manifest 首次写入延迟、重启命中率、HDD await、RSS 和 1 万漫画/
8000 COS 包的真实 N100 Gate 尚未测量，不能把该候选写成已通过实机性能。

## 2026-08-18：Inventory 按媒体类型分阶段 rollout 门禁

- 新增 `INVENTORY_SCANNER_KINDS`，支持 `all` 或逗号分隔的五类媒体集合；未知类型在
  启动时拒绝，Inventory health、watcher、shadow reconcile 和 promotion 使用同一组选定
  kind，避免配置与 ownership 判断出现分歧。
- 全量 shadow reconcile、watcher journal/gap、pending event claim 和 Catalog event
  processor 现在按选定 kind 过滤；未选中的事件保持 legacy full-scan 路径，未选中的
  kind 不能被 promotion。Comic/CoserPicture 也纳入 changed-key 容量保护。
- 修正显式 `scan-library { kind }` 的边界：kind-scoped reconcile 只同步该 kind 的
  Inventory root；按集合 rollout 时只停用集合内旧 root，不会把其它媒体 root 误标记为
  disabled。对应回归覆盖了 novel 选中、comic 保留和 unselected comic legacy scan。
- `.env.example` 与 Compose 默认保持 `INVENTORY_SCANNER_KINDS=all`，但 rollout 文档
  明确建议在 N100 上按 `novel → comic → coser-picture → audio → gallery` 逐类运行
  shadow diff、对账和稳定窗口，再切换 ownership；本批没有打开任何实验性默认开关。
- 新增行为级回归：kind-scoped reconcile 不触碰其它根、pending claim 不领取其它 kind、
  未选中路径 fail-closed 到 legacy scan，以及 promotion kind gate 的 fail-closed 约束。

本批次验证：后端 `cargo test -p media-shelf-server --all-targets` 为 293 passed、
0 failed、3 ignored；Clippy `-D warnings`、fmt、前端生产构建、项目 validator 和 14 项
perf 单测均通过。以上仍是开发机证据；Novel 1 万本首批真实 N100/NAS shadow diff、扫描
耗时、RSS、SQLite WAL/busy 和混合浏览 Gate 尚未执行。

## 2026-08-18：legacy scanner 无变化作品写入抑制

- `upsert_scanner_work_in_transaction` 现在只在标题、分类、描述、评分、metadata
  或删除状态确实变化时更新 `works`；相同扫描事实会返回既有 work ID，但不刷新
  `updated_at`，从而不再制造无意义的 Catalog revision 和后续搜索维护失效。
- 该路径保留了恢复已删除作品和真实 metadata 变化的更新语义；SQLite 冲突分支被
  `WHERE` 跳过时通过只读 ID 查询恢复 identity，不额外写入数据库。
- 新增回归覆盖“相同事实 revision/更新时间不变、实际变化仍推进 revision”的契约。

这只降低 legacy scanner 的重复写放大，不改变 scanner ownership、默认功能开关或
搜索 reader；真实 80 万关联的 N100/4GiB/NAS WAL 与扫描尾部耗时仍需 Gate 验证。

## 2026-08-18：按媒体类型作用域扫描与写闸门基线修正

- `ScanRequest` 和 `scan-library` job payload 现在可以携带可验证的 `kind`；worker
  对有 kind 的任务只执行对应媒体模块，旧客户端省略 kind 时仍执行完整扫描。
- Inventory shadow reconcile 新增 kind-scoped 路径。作用域扫描只同步并扫描目标 kind
  的根目录，不会先禁用其它媒体 kind 的 inventory root，适合在 N100 上逐类做
  reconciliation、shadow diff 和 ownership promotion。
- scan job 合并逻辑改为 fail-safe：完整扫描与某类扫描合并时保留完整覆盖范围；不同
  kind 的作用域请求合并为完整扫描，不会静默丢掉某一类媒体；`enqueue_enrichment` 仍
  使用 OR 语义。
- 写入闸门测试在 migration 完成后建立指标 baseline，避免 schema migration 自身的写
  事务改变测试样本数。该修正只改变测试证据，不改变生产限流策略。

本批次新增作用域校验、inventory root 保留、scan job 合并和 worker dispatch 回归；
后端全量回归为 285 passed、0 failed、3 ignored。真实 N100/4GiB/NAS HDD 扫描耗时、RSS、HDD await
和混合浏览 Gate 仍未执行，所有实验性默认开关和 ownership 默认值保持不变。

补充了 worker 级作用域回归：`scan-library` payload 携带 `kind=novel` 时只导入轻小说，
随后独立提交 `kind=comic` 才导入漫画；测试通过同一 `enrich::run_job` dispatch 路径，
而不是直接调用 scanner 函数。该证据覆盖 job payload、扫描锁释放以及跨模块不误扫，
但仍不替代真实 N100/NAS 上的按 kind 扫描耗时与混合负载 Gate。

同批次将 `refresh_tag_counts` 改为事务内的集合式刷新：先清理无关联但仍为非零的
陈旧标签，再对 `work_tags` 做一次 `GROUP BY tag_id`，通过 SQLite `UPDATE ... FROM`
只写入发生变化的标签。这样避免在约 6.5 万标签字典上为每个标签重复执行相关
`COUNT(*)`；现有“只更新脏行、第二次刷新不产生写入”的回归保持通过。该优化尚未在
真实 80 万关联的 N100 数据库上测量耗时/WAL，不能把预期的扫描尾部缩短写成实测收益。

## 2026-08-18：SQLite 单写入闸门与 WAL 证据

- `Db` 新增显式、可观测的单写入闸门。Catalog v2 的 ownership change、typed
  mutation、tombstone、Derivative ledger 的状态/淘汰事务、Facet/统计维护事务以及
  shadow search 的状态提交在 `begin` 前获取闸门；健康快照记录
  `queue_depth`/`queue_bytes`、活动写入、等待 P50/P95 所需的累计/最大微秒和事务持有
  时长。估算字节是 admission 证据，不等同于 SQLite 实际 WAL 字节。
- SQLite runtime snapshot 新增被动 WAL checkpoint 证据：调用次数、耗时、`busy` 页、
  log/checkpointed 页和错误。`/api/health/resources` 默认保持只读；显式加
  `?checkpoint=true` 才执行一次 `PASSIVE` probe，perf baseline 对应增加
  `--health-checkpoint`。
- writer gate 增加 N100 默认的有界队列（32 个等待请求、64MiB 估算字节），超限返回
  可重试的 503，而不是继续堆积 Tokio 等待任务；`SQLITE_WRITER_QUEUE_MAX_DEPTH` 和
  `SQLITE_WRITER_QUEUE_MAX_BYTES` 可在真实 NAS Gate 中调整。
- 该闸门是 DB1 的增量落地，不宣称所有 legacy 单语句写入已经迁移到 actor；仍需把
  legacy scanner、inventory、search outbox 等写路径逐步纳入同一提交队列，并在 N100 上
  测量 SQLite busy、WAL checkpoint 长尾和最老读快照。

本批次新增写闸门串行/等待/取消测试和 WAL probe 测试；后端全量回归为 281 passed、0 failed、
3 ignored。真实 N100/4GiB、混合扫描与浏览负载仍未执行，默认开关和 ownership 均未改变。

## 2026-08-18：G0 媒体根目录清单采集器

- `scripts/perf/capture-baseline.mjs` 新增显式 `--media-manifest kind=path`，与只记录磁盘容量的 `--media-root` 分离；普通基线不会因为配置了媒体根目录而递归扫描大型媒体库。
- 清单采集器只读遍历目录，保留文件/目录数量、总字节、扩展名分布、路径长度和有界错误样本；文件数、目录数均有硬上限，超限会记录 `complete=false` 而不是继续累积内存。
- 每个根目录最多检查固定数量的图片 header 和 ZIP/CBZ/EPUB 中央目录，记录像素桶、entry 数、未压缩大小和中央目录大小；不会读取归档正文，也不会把路径列表写入 artifact。
- 新增 perf 单测覆盖目录聚合、文件/目录上限、PNG 尺寸解析和 ZIP 中央目录统计；默认开关和服务端业务路径未改变。

本批次验证：`node --test scripts/perf/perf-lib.test.mjs` 为 13 passed、0 failed；
三个 perf 测试文件合计 16 passed、0 failed；`node scripts/validate-project.mjs`
通过。该清单能力仍需在真实 NAS/N100 上执行并保存 `dataset-manifest.json`，
不能把开发机采集结果当作 N100 性能 Gate。

## 2026-08-18：U1 摘要详情第一阶段

- `/api/works/{id}` 新增 `asset_mode=legacy|summary`。省略参数或请求
  `legacy` 时保留旧的完整详情语义；`summary` 对非音频/归档作品也设置 16
  条资产上限，并继续返回精确的 `asset_count`、`track_count` 和
  `assets_complete`，剩余资产通过已有 cursor assets API 获取。
- Catalog v2 前端打开作品时请求 `asset_mode=summary`；旧 Catalog/兼容客户端
  仍请求 `legacy`，因此可以独立回退，不改变扫描 ownership、资产数据或阅读历史。
- 新增 64 资产轻小说 fixture，验证 legacy 返回完整资产，summary 只返回 16 条且
  顺序、总数和 `assets_complete=false` 正确。

本批次验证：后端 `cargo test -p media-shelf-server --all-targets` 为 277 passed、
0 failed、3 ignored；`cargo clippy -p media-shelf-server --all-targets -- -D warnings`、
`cargo fmt --all -- --check`、前端 `npm run build`、`node scripts/validate-project.mjs`
和 `git diff --check` 均通过。本实现仍未宣称完成 1 万轨浏览器 heap Gate 或 N100
实机 Gate。

## 2026-08-18：JPEG DCT 缩放解码候选

- 新增可选 `JPEG_THUMBNAIL_DOWNSCALE_ENABLED`（默认 `false`）。启用后，JPEG
  缩略图/封面/漫画与 CoserPicture 阅读派生图在 RGB 输出前使用 MozJPEG 的
  `1/8`、`1/4`、`1/2` 或 `1/1` DCT 比例，选择保证最长边覆盖目标 bucket 的最小
  解码尺寸；PNG、WebP、GIF 和非 JPEG 路径保持原有 `image` 解码器。
- native 路径在读取 scanlines 前按 `MAX_IMAGE_DECODE_ALLOC_BYTES` 检查 RGB
  buffer，MozJPEG 失败时回退原有路径；不会改变原图接口、缓存 bucket 或默认
  开关语义。
- 配置、Docker/.env 示例和 `/health.features.jpeg_thumbnail_downscale` 已同步，
  便于 NAS Gate 记录实际 flag。

本批次验证：服务器 `cargo test -p media-shelf-server --all-targets` 为 272 passed、
0 failed、3 ignored；`cargo clippy -p media-shelf-server --all-targets -- -D warnings`、
`cargo build --release -p media-shelf-server` 和前端 `npm run build` 均通过。新增
缩放比例、native JPEG、非 JPEG 回退和 RGB buffer 上限回归。上述仍是开发机证据；
15MP/24MP/50MP JPEG 的 N100/4GiB 冷缩略图耗时、RSS、温度降频、EXIF orientation
和 Docker 镜像依赖 Gate 尚未完成，因此开关继续保持关闭。

## 2026-08-18：归档 JPEG 派生图改为 entry 流式解码

- 漫画/CoserPicture 的 JPEG 封面和阅读派生图不再先调用
  `cbz_named_page_bytes` 把整个压缩条目解压成 `Vec<u8>`；启用 JPEG flag 时，
  直接把 ZIP entry 作为 MozJPEG 的 `BufRead` 输入，并在 DCT 缩放后才分配 RGB
  buffer。
- PNG/WebP/其它格式以及 JPEG native 失败继续重新读取条目并走原有有界缓冲解码，
  保留兼容性和失败回退；归档池锁在流式解码完成前保持占用，避免句柄失效。
- 增加有效 JPEG entry 和非 JPEG archive fallback 回归；既有 ZIP bounded channel、
  archive pool 和资源释放测试继续保留。

本批次验证：服务器 `cargo test -p media-shelf-server --all-targets` 为 274 passed、
0 failed、3 ignored；Clippy、release build、前端 production build、项目 validator
和 `git diff --check` 均通过。该改动减少有效 JPEG 派生图的页面级 `Vec<u8>`，但当前
资源预约仍按 128 MiB 最大页面安全上限保守计费；N100 实机 RSS、ZIP CRC/损坏包、
长尾页面和混合并发 Gate 仍待执行。

## 2026-08-18：图库/音声分页与本地流资源治理

- 图库 `/works/{id}/gallery` 增加基于 `(position,id)` 的不透明 keyset 游标。旧的
  十进制 offset 仍可读取，作为一次性深度跳转兼容路径；后续页面返回 keyset 游标，
  避免 70 万图片图库连续翻页时由 SQLite 反复丢弃前置行。总数优先读取维护中的
  `work_stats.image_count` 并扣除 image cover，统计尚未完成或旧数据则回退精确 COUNT。
- 前端图库虚拟滚动保存页面前置游标；随机深跳只产生一次 offset 查询，顺序预取使用
  keyset。页面缓存和缩略图缓存上限保持不变。
- 音频轨道分页的 `total` 优先使用 `work_stats.track_count`，仍兼容旧库/统计回填阶段的
  精确事实表回退。
- 本地漫画目录新增 `media_dirs.comic_scan_depth`，默认深度 3（覆盖
  `作者/系列/文件.cbz`），范围 1–64；qmediasync 的独立 `scan_depth` 语义不变。
- 本地音频 Range/整文件流新增独立 `LocalMediaStream` 资源池。`nas-n100-4g` 默认
  2 个并发流，每个流只占一个小型在途缓冲预算，不按整文件大小预约内存；它不再与
  CBZ/ZIP 解压共用 `ArchiveStream` permit。新增环境变量
  `LOCAL_MEDIA_STREAM_WORKERS`。

本批次验证：后端 `cargo test -p media-shelf-server --all-targets` 为 266 passed、0
failed、3 ignored；Clippy `-D warnings`、fmt、前端 `npm run build`、项目 validator、
14 项 perf 单测和 `git diff --check` 均通过。以上仍是开发机证据，尚未替代目标 N100/
4 GiB/NAS HDD 的 RSS、首字节、深页 P95、起播和混合负载 Gate。

## 2026-08-17：Audio/Gallery 定向事件读取的有界化

- Inventory 的 Audio 定向 Catalog event 不再通过 `fetch_all` 先收集整个作品的
  路径；改为数据库流式读取并使用 `LIMIT(MAX_AUDIO_FILES_PER_WORK + 1)`，同时检查
  20,000 文件和 16 MiB 路径预算。
- Gallery 定向事件、rename identity 查询使用同样的 `LIMIT(MAX_GALLERY_FILES_PER_WORK
  + 1)` 和流式行读取；超过 20,000 张图片时在构造 Vec 前 fail-closed，保留旧作品，
  不把异常数据交给 inspector 后才发现。
- 数据库/IO 错误仍向 durable event 重试路径传播；仅数量或路径预算超限转为可诊断的
  degraded/preserve 结果，保持既有错误不删除语义。

新增 20,001 音频路径与 20,001 图库图片回归，验证两个 loader 都在达到上限时停止，
不会无界收集。该回归和现有全量验证仍是开发机证据；真实 N100 上还需测量定向事件
首响应、SQLite busy/WAL 和长路径作品的 RSS。当前后端全量回归为 255 passed、0
failed、3 ignored。

## 2026-08-17：shadow 搜索事实自动对账 worker（默认关闭）

- 新增低频自动事实对账 worker。只有
  `SEARCH_OUTBOX_SHADOW_ENABLED=true` 且
  `SEARCH_SHADOW_CANARY_ENABLED=true` 时才会启动；默认配置完全不启动，生产
  reader 路径和旧的全量 rebuild 路径不变。
- worker 启动后等待 30 秒，之后默认每 60 秒最多检查一次；索引未 ready、仍有
  outbox lag、revision 在检查期间变化或资源不足时只延期，不把竞态误记为失败。
- 自动对账与手动 `POST /api/search/shadow/reconcile` 共享 single-flight 锁，且每次
  使用 `ResourceClass::SearchWriter` 的 16 MiB processing admission；不会和
  shadow writer 无界并发争抢 4 GiB 内存。
- 对账仍受现有 50,000 works/documents 上限、revision fence、ID hash 和
  `degraded`/`passed` 状态机约束；它是可观测性和恢复门禁的补强，不是 production
  reader cutover 证据。

本批次验证：`cargo test -p media-shelf-server --all-targets` 为 254 passed、0
failed、3 ignored；`cargo clippy -p media-shelf-server --all-targets -- -D warnings`、
fmt、12 项 perf 单测、项目 validator 和 `git diff --check` 均通过。worker 的
启动条件和现有 shadow 对账回归均已覆盖，但自动 worker 的实际吞吐、N100 RSS、
HDD 冷页耗时和 24 小时稳定性仍需真实 NAS Gate。

## 2026-08-17：搜索 outbox 重试时间与 N100 可观测性修正

- 统一 search outbox 的新写入时间为毫秒精度 UTC `Z` 字符串。此前失败重试将 `chrono::DateTime<Utc>` 直接绑定为 `+00:00`，而领取条件使用 `Z` 形式做 SQLite 文本比较；失败项可能在未来重试时间到达前被再次领取，造成 N100 上的重试忙循环和无意义 SQLite/Tantivy 唤醒。
- `claim_items`、`release_claims`、acknowledge、shadow baseline 和 legacy/typed tombstone/upsert writer 现在共享同一时间表示；保留旧数据库兼容，不要求重写历史时间字段。
- `/health` 与 `/health/resources` 返回的 `search_outbox` 增加 `retrying`、`max_attempts`、`stale_claims`、`oldest_failed_at`，用于区分正常 lag、退避中的失败和超过 5 分钟的陈旧 claim。
- 新增回归验证：不兼容 payload 失败后，重试项在 backoff 窗口内不能被立即再次 claim；现有 delete/tombstone、幂等 replay 和 stale acknowledgement 测试保持通过。

本批次验证：`cargo test -p media-shelf-server` 为 240 passed / 0 failed / 3 ignored；`cargo clippy -p media-shelf-server --all-targets -- -D warnings`、`cargo fmt --all -- --check`、`node --test scripts/perf/*.test.mjs` 和 `node scripts/validate-project.mjs` 通过；前端在 `frontend` 目录执行 `npm run build` 通过。根目录没有 `package.json`，所以不把根目录 `npm run build` 作为项目失败证据。

## 已落地

- 新增 schema migration v11：`search_reconciliation_state`，持久化 shadow 索引的事实对账状态、revision、ID 摘要、差异计数、连续通过次数和最近检查时间。
- shadow baseline 重建时清空旧对账状态，避免复用过期的通过证据。
- 事实对账区分三种结果：
  - `passed`：revision 稳定且 SQLite/Tantivy work ID 集合一致；
  - `stale`：对账期间 catalog 或 shadow revision 发生变化，不把竞态误报为数据损坏；
  - `failed`：revision 稳定但存在缺失、意外、重复或无效文档。
- `failed` 会将 shadow index 标记为 `degraded`、`ready=0`；后台进度刷新不会自动清除这一状态。
- 修复索引后允许在 `degraded` 状态重新对账；稳定通过后可恢复 `ready`。
- health/resource health 新增 `search_reconciliation`，outbox 状态新增 `shadow_applied_revision` 和 `revision_lag`。
- 新增独立 `SEARCH_INCREMENTAL_READER_ENABLED`：
  - 默认 `false`；
  - 开启时生产搜索与 Catalog 候选读取 shadow index；
  - 必须同时有 shadow worker、当前 revision 对账 `passed`、无 pending/lag、索引 `ready`；
  - Gate 未满足时读请求 fail-closed 为受控 503，不回退到已知可能过期的 legacy index；
  - scanner 在该模式下不再为每次完整扫描自动排队全量 search rebuild，完整重建仍保留为显式恢复操作；
  - 启动先拉起 shadow worker，再记录 Gate 未就绪警告，避免停机期间积累 outbox 时形成恢复死锁。

## 验证证据

- `cargo test -p media-shelf-server`：165 passed，0 failed，3 ignored。
- `cargo clippy -p media-shelf-server --all-targets -- -D warnings`：通过。
- `cargo fmt --all -- --check`：通过。
- `npm run build`：通过。
- `node scripts/validate-project.mjs`：通过。
- 新增测试覆盖：持久化 passed、连续通过计数、revision stale、稳定事实漂移 degraded、修复后恢复 ready、增量 reader 门禁。

## 尚未宣称完成

- 真实固定 corpus 与真实旧库的 SQLite/Tantivy 逐字段对账。
- delete/tombstone 与 reading history/progress 保留契约。
- N100/4 GiB、目标 SSD/HDD 布局的 lag、RSS、冷启动和 24 小时 soak。
- 逐 kind inspector/coordinator 和生产 ownership 切换。
- ZIP channel streaming、音轨分页和图库 70 万文件的 changed-key discovery。

因此 `SEARCH_INCREMENTAL_READER_ENABLED` 仍应保持关闭，直到上述 Gate 证据齐全。

## 2026-08-17：reconciliation 输入与 legacy 单作品边界收敛

- Audio/Gallery Catalog reconciliation 不再使用 `fetch_all` 读取单作品的
  `file_inventory` 输入；查询改为逐行读取，并在数据库游标阶段以
  `LIMIT(max+1)`、单作品文件数和路径字节预算 fail-closed。超大作品不会先
  构造无界结果集再交给 inspector。
- reconciliation 每个 root 的 `work_key` 枚举改为 256 条 keyset 分页；unexpected
  legacy work 查询也改为流式读取，避免一个 root 下的所有作品键一次性进入内存。
- legacy Audio scanner 对单个 work 的路径分组加入 20,000 文件上限；legacy Gallery
  对单个目录加入同等上限。超过上限时保留既有作品并等待下一次可接受的完整扫描，
  不执行不完整快照的删除。
- legacy Audio 根目录的分组路径还受 64 MiB discovery path budget 约束；超过预算时
  整个分组批次保持旧作品，不把根目录路径继续堆入进程内存。
- 生产搜索全量 rebuild 在 shadow worker 关闭时，会只确认快照 revision 覆盖范围内的
  `search_outbox` 项；快照期间竞争写入的更高 revision 仍保留为 pending，不会被误确认。

本批次验证：后端全量 `cargo test -p media-shelf-server --all-targets` 为
249 passed / 0 failed / 3 ignored；`cargo clippy -p media-shelf-server --all-targets
-- -D warnings`、`cargo fmt --all -- --check` 和 12 个 `scripts/perf` 测试通过。
显式 700,000 行 Inventory Gate 通过（18.8 秒、1024 行最大批次、最大序列化批次
169,985 bytes）；700,000 行 Derivative ledger Gate 通过（SQLite integrity ok，
LRU eviction query 使用 `idx_derivatives_eviction_lru`）。这些仍是开发机证据，
不替代真实 N100/4 GiB/HDD Gate。另新增 257-work reconciliation 分页回归，
验证跨越 256 条 keyset 页边界时无漏项或重复。

## 2026-08-17：Inventory present/work 覆盖索引（migration v21）

- 在不改变默认开关和写入语义的前提下追加 `idx_inventory_present_work_cover`：
  `(root_id, work_key, relative_path, size, fast_fingerprint, file_id)`，
  仅包含 `status='present' AND work_key IS NOT NULL` 的行。
- 该索引覆盖 Catalog reconciliation 的 work-key keyset 枚举、Audio/Gallery
  单作品 inventory 读取，以及 Gallery Catalog asset 读取，减少 700,000 行图库
  在索引定位后再回表取元数据的随机访问。
- 内存 SQLite（与生产相同的 `WITHOUT ROWID` 表）700,000 行对照实验：keyset
  首页约 122–124 ms 降至 41–50 ms；单作品资产读取约 35–37 ms 降至
  0.7–0.8 ms；覆盖索引约 47.1 MiB，旧 `(root_id, work_key)` 索引约
  31.9 MiB，开发机建索引约 428 ms。该实验用于判断查询形状，不能替代
  N100/HDD 冷页实测；正式 Gate 仍需记录 migration 时长、RSS、WAL 和冷页 P95。
- 新增 SQLite `EXPLAIN QUERY PLAN` 回归，要求两个读取路径使用覆盖索引。
- 同步性能夹具要求 schema v21，并用临时数据库完成一次完整
  `r1g-40k-740k-800k-v3` 生成与校验：40,000 works、700,000 gallery assets，
  开发机用时 141.8 s、数据库 375,353,344 bytes；该时间包含 Node/SQLite
  合成写入，不代表 NAS 冷扫描或前台浏览延迟。

本轮最终验证：`cargo test -p media-shelf-server --all-targets` 为 252 passed、
0 failed、3 ignored；Clippy `-D warnings`、fmt check、12 项 perf 单测、项目
validator 和 `git diff --check` 均通过。实验性 Inventory/Search/Facet bitmap/
Derivative v2 开关仍未默认开启。

## 2026-08-17：旧缩略图配额目录扫描降频

- 当 Derivative Cache v2 仍关闭时，旧缩略图配额账本不再对大于 4,096 个文件的
  目录响应每次 mtime 变化；这类目录最多按 60 秒间隔重扫。
- 目录重扫期间的估算只会暂时保守地拒绝新 reservation，不会放宽配额，因此
  不牺牲 4 GiB 环境下的磁盘安全边界；小目录仍保留 mtime 变化后的即时校正，
  兼容外部单文件删除的现有契约。
- 新增大目录不重复 `read_dir` 回归，并保留现有小目录删除后立即重同步测试。
  该改动直接针对默认关闭 Derivative v2 时 70 万图库缩略图集中在单目录的
  冷预览路径；真实 NAS 上的目录扫描耗时仍需 N100 Gate 测量。

## S2：音频详情、漫画/COS 阅读流与图库扫描内存边界（本轮）

本轮在不打开实验性默认开关的前提下落地了三项面向 N100/4 GiB 的改动：

- 音频 `WorkDetail` 不再把全部轨道放进一次响应。服务端保留首批 128 条可播放轨道和最多 64 条辅助资产，并返回精确的 `asset_count`、`track_count` 与 `assets_complete`；剩余轨道通过已有 `/works/{id}/assets?role=track` keyset 接口取得。前端播放栏只维护最多 5 页（640 条）元数据，队列只渲染当前轨道附近的窗口，并始终使用一个 `<audio>` 元素。
- 漫画/CoserPicture manifest 支持 `cursor`/`limit` 分段（默认新客户端每次 200 条，最大 500 条），旧的无查询参数调用仍可获得完整 manifest。漫画页正文改为 ZIP 解压 worker + 有界 channel 流式发送：处理预算仍按 128 MiB 页上限计费，但在途响应只保留 4 个 128 KiB 块，客户端断开时 worker 和资源租约会一起释放。
- 图库 legacy scanner 不再先把全部图片路径收集后按父目录分组；先发现父目录，再逐目录读取和提交，单次只保留当前图库文件集。目录发现设置 65,536 个父目录安全上限，遇到不完整读取时不提交部分快照，也不执行缺失 tombstone。

本轮验证：`cargo test -p media-shelf-server` 174 passed、3 ignored；`cargo clippy -p media-shelf-server --all-targets -- -D warnings`、`cargo fmt --all -- --check`、`npm run build` 与 `node scripts/validate-project.mjs` 均通过。新增测试覆盖音频详情有界返回、漫画 ZIP 分块流及资源释放。

这些是查询/内存边界的本地证据，不是 N100 实机证据。仍需在目标 NAS 上测量 4 GiB cgroup 下的 RSS、首字节延迟、冷页 P95、音频分页起播和 70 万图片目录扫描时间；Facet bitmap、Derivative Cache v2、shadow inventory/search reader 仍按 Gate 条件保持默认关闭。

## 2026-08-07：写放大首批落地

- Legacy scanner 的 asset conflict upsert、`scanner_assets` fence 更新和 cover 指针更新现在仅在值实际变化时写入；新增回归测试确认重复两资产批次产生 0 次 asset/cover UPDATE，单个资产变化只产生 1 次 asset UPDATE。
- 扫描结束的标签计数刷新、typed writer 的 dirty-tag 刷新以及外部标签链接计数更新均改为差异条件更新，避免无变化扫描重写整个 `tags` 表。
- 修复 inventory novel/comic coordinator 获取租约时的 `kind` 参数占位符错误；增量 EPUB/CBZ 事件现在可以进入 inspector/writer 路径。
- 本轮验证：Rust 182 passed / 3 ignored；clippy、fmt、前端构建、项目校验和 12 个 perf 脚本测试通过。临时 SQLite 70 万行 inventory 验收为 13.629 秒、1024 行批次、最大序列化批次 169,985 字节；该结果来自开发机，不能替代 N100/HDD Gate。

## 2026-08-17：修复 N100 单 ScanIo 许可的 coordinator 阻塞

- 复现并修复 inventory coordinator 的资源许可嵌套等待：`nas-n100-4g` 只有 1 个 `ScanIo` permit，原先全量 reconcile 和 watcher inventory 批次持有该 permit 时，会调用再次申请 `ScanIo` 的 novel/comic inspector，导致后台任务无限等待。
- 全量 reconcile 改为批次许可接力：walker 持有许可读取最多 1024 个 inventory 项后释放并暂停，coordinator 使用同一个单许可按 64 项子批次清空 novel/comic catalog 队列，再重新授予 walker 下一批许可。目录 I/O 与 archive/EPUB inspector 不会并行争抢 HDD，也不会让 4096 项 changed-key 上限在约第 5 个批次阻断首次建库。
- watcher 路径现在在定向 inventory 事件处理完成后先释放外层许可，再进入 novel/comic coordinator；没有普通 inventory 事件时也不再无意义占用 `ScanIo`。
- 漫画全量 reconcile、小说全量 reconcile 和小说 changed-key watcher 测试均改用 N100 单许可 profile；关键调用增加测试超时。新增 1025 个 inventory 文件/65 本 EPUB 的边界回归，强制执行至少一次 walker 许可交接和至少两轮 catalog 子批次，最终确认 pending event、`scan_io` 与 processing memory 许可全部归零。
- 实验性默认开关保持不变。本轮验证：Rust 191 passed / 3 ignored；新增边界回归在开发机 debug 构建中用时 0.92 秒；Clippy、fmt、前端构建、项目校验和 12 个 perf 脚本测试均通过。

## 2026-08-17：本地 CoserPicture ZIP inspector/coordinator

- 新增纯、数据库无关的 CoserPicture inspector：每个相对 ZIP work key 只生成 1 个 archive asset、3 个 legacy 等价标签和完整 fenced `WorkMutation`；复用现有 ZIP 安全页数统计与 `ScanIo` governor，不把压缩包正文载入 mutation。
- 完整 inventory reconcile 与 watcher changed-key 现在都支持 CoserPicture create/modify/rename/delete/revive；坏 ZIP 连续失败后进入 `degraded`，但保留上一版作品；不完整 root generation 不排队 tombstone。
- 漫画/CoserPicture 共用 archive 入队边界，并显式绑定 `library_roots.kind` 与 ownership，避免某类根目录中的 ZIP 被另一类 coordinator 误领。
- legacy CoserPicture writer 在 inventory 已启用且该 kind 明确切到 `catalog-v2` 时停止写入，保持单 writer；存在 qmediasync comic/CoserPicture root 时，管理端拒绝 promotion，因为该 provider 尚未接入 bounded coordinator。
- 修复 Windows 弱文件身份的重复内容误合并：rename 前身必须已经 missing，或未在当前 generation 出现；1025 文件/65 本相同 EPUB 的 N100 单许可边界测试连续 3 次均保留 65 个独立作品。
- 项目结构校验现在要求 CoserPicture inspector 文件存在。

本轮验证：`cargo test -p media-shelf-server` 196 passed / 3 ignored；CoserPicture 7 项定向测试通过；N100 单许可 65 EPUB 边界测试连续 3 次通过；Clippy `-D warnings`、fmt check、前端生产构建、项目校验和 12 个 perf 脚本测试均通过。

尚未完成：真实 N100/HDD 的 8000 ZIP 冷扫描与混合浏览 Gate、真实 legacy/Catalog v2 逐字段 shadow diff、qmediasync provider coordinator，以及生产 ownership 稳定窗口。因此所有实验性默认开关仍保持关闭，不应据此启用 `INVENTORY_SCANNER_ENABLED` 或切换 CoserPicture ownership。

## 2026-08-17：音声有界 inspector/coordinator

- 新增数据库无关的 Audio inspector，以 256 个资产为一块、深度 2 的有界 channel 输出 partial `WorkMutation`，只有最终空块携带 tags/external IDs 并设置 `complete_snapshot=true`；单作品最多 20,000 个文件和 16MiB 相对路径文本，避免千轨/万轨作品形成无界 mutation。
- 完整 reconcile 和 watcher changed-key 均已接入 Audio coordinator；legacy writer 在 inventory 已启用且 audio ownership 明确切到 `catalog-v2` 时停止写入，保持单 writer。大作品测试在 N100 单 `ScanIo` permit 下跨 writer asset limit 流式提交，最终确认 catalog event、`scan_io` 和 processing memory 许可归零。
- RJ 识别在 legacy 与 Inventory 路径统一为大小写不敏感并规范化为大写；作品根目录推断支持 `作者/rjxxxxxx/作品名/音轨` 和 `[RJxxxxxx] 标题/音轨`，避免错误使用单个音轨子目录作为作品根。
- Inventory、legacy scanner 和 inspector 对 AAC/Opus 的识别保持一致；logical track key 会移除 AAC/Opus 格式噪声，使同轨不同格式共享 position，同时保留 variant 标签、封面和 Lofty metadata fallback。
- 新增故障保留测试：Inventory 仍标记 present 但音频实际消失时，durable catalog event 进入 degraded，上一版资产、作品 ID 和 reading history 不被删除；文件恢复后的新事件复用同一作品 ID 并清除当前失败。
- 项目校验器现在把 Audio inspector 列为必需文件，并验证 256 资产 chunk、depth-two channel、20,000 文件上限、finalize fence、路径逃逸防护和 coordinator 接入；同步修正 typed writer 常量重命名后的陈旧结构断言。

本轮验证：`cargo test -p media-shelf-server` 204 passed / 0 failed / 3 ignored；音声定向 11 项通过；`cargo clippy -p media-shelf-server --all-targets -- -D warnings`、`cargo fmt --all -- --check` 和 `node scripts/validate-project.mjs` 均通过。

尚未完成：显式 1 万轨性能 Gate、真实 N100/HDD 的 1 万文件冷扫描与播放混合负载、legacy/Catalog v2 逐字段 shadow diff、生产 ownership 稳定窗口。因此 `INVENTORY_SCANNER_ENABLED` 继续保持默认关闭，audio ownership 不应在缺少真实库对账时切换。`rj | folder | auto` 配置已在下一批落地并由 v20 root 字段持久化。

下一批优先级调整为：gallery directory inspector、snapshot/chunk-finalize 与 changed-key coordinator，然后执行逐 kind shadow diff 和 ownership cutover；真实 N100/HDD Gate 仍必须单独完成。

## 2026-08-17：图库 ownership 与 legacy 回滚契约

- 当前工作树已包含 Gallery directory inspector、256 资产 chunk、深度 2 channel、完整 reconcile、changed-key coordinator，以及 rename/delete/revive 和失败保留测试；此前进度文档中“图库尚缺 inspector/coordinator”的描述已过期。
- 五类 legacy scanner 现在共用同一 ownership 判断：只有 `INVENTORY_SCANNER_ENABLED=true` 且该 kind 的 `authoritative_writer='catalog-v2'` 时，legacy writer 才停止。关闭 Inventory 会恢复 legacy writer，避免配置回滚后出现无人维护该 kind 的状态。
- Gallery legacy scanner 在 promotion 后会在任何目录遍历前退出，不再重复读取约 70 万图片 metadata，也不会与 Catalog v2 coordinator 同时写入作品、资产和标签。
- 新增回归测试覆盖 Gallery promotion 后不创建 legacy work、关闭 Inventory 后 legacy Gallery 恢复扫描，以及 Novel ownership 同样受 Inventory coordinator 状态约束。
- 实验性默认开关保持关闭。本批次验证：`cargo test -p media-shelf-server` 211 passed / 0 failed / 3 ignored；scanner 定向测试 18 passed / 0 failed；`cargo clippy -p media-shelf-server --all-targets -- -D warnings`、`cargo fmt --all -- --check` 与 `node scripts/validate-project.mjs` 均通过。

下一批应转向逐 kind shadow diff 和 promotion 前置证据，优先补固定真实库副本的 legacy/Catalog v2 逐字段对账；没有真实 N100/HDD Gate 前仍不应开启 Inventory 默认值。

## 2026-08-17：Novel Legacy/Catalog v2 promotion 前置证据

- 新增 schema migration v15：`catalog_reconciliation_state` 按 media kind 保存状态、对账前后 catalog revision、root generation/event 摘要、作品差异计数、连续通过次数和耗时；`catalog_reconciliation_diffs` 每类最多保留 256 条差异，不随 1 万本小说或 70 万资产无界增长。
- 新增单实例 maintenance job `reconcile-catalog-novel`，通过 `POST /api/catalog/reconciliation/novel` 显式排队，`GET /api/catalog/reconciliation` 返回当前性、计数和有界差异。该任务与完整扫描、搜索重建共用 maintenance 串行边界，并继续使用 `nas-n100-4g` 的单 `ScanIo` 许可逐本处理。
- Novel inspector 在不提交 `WorkMutation` 的情况下生成候选事实，再与 Legacy canonical work 比较 scanner-owned 作品字段、资产、标签、外部 ID 和 materialized stats。EPUB 提取封面按 role/variant/content source version 比较，不直接比较含 work ID 或 `epub-cover-v2` 前缀的生成路径。
- 对账证据同时绑定全局 `catalog_state.revision` 与启用 root 的 generation、completed generation、状态、active token、last event seq 和 inventory 计数摘要。运行期间或运行后发生扫描、watcher 事件、enrichment/catalog 写入时，API 将证据报告为 `stale`，promotion 事务也会重新计算摘要并 fail-closed。
- Novel ownership promotion 现在必须具有当前 `passed` 证据且 expected/matched 完全一致、missing/unexpected/mismatch/error 均为 0。没有证据、差异证据或过期证据都会拒绝切换；rollback 行为保持不变。Comic、CoserPicture、Audio、Gallery 尚未接入这一新门禁，仍需后续逐类实现。
- 新增回归覆盖：完全一致、连续通过、Legacy/V2 封面路径语义归一化、作品/标签/资产漂移、missing/unexpected（新增/删除或未完成 rename）、坏 EPUB 只记录 error 且不修改 reading history，以及 root event 变化后的 promotion 拒绝。

本轮验证：`cargo test -p media-shelf-server` 216 passed / 0 failed / 3 ignored；`cargo clippy -p media-shelf-server --all-targets -- -D warnings`、`cargo fmt --all -- --check`、前端 `npm run build` 与 `node scripts/validate-project.mjs` 均通过。

尚未完成：真实旧库副本的 1 万本 EPUB 全量运行时、HDD 读取量、N100 CPU/RSS 和混合浏览延迟 Gate；Comic、CoserPicture、Audio、Gallery 的逐字段对账与 promotion 证据；按 kind revision 降低无关 catalog 写入导致的保守 stale。`INVENTORY_SCANNER_ENABLED`、Search shadow 与其他实验性开关继续保持默认关闭。

## 2026-08-17：统一音声 `rj / folder / auto` 分组策略

- 新增数据库无关的 `AudioGroupingMode` helper，默认 `auto`：路径包含规范 RJ 编号时按 RJ work key 聚合，否则回退到父文件夹；显式 `folder` 始终使用文件夹，显式 `rj` 在无 RJ 标记时仍安全回退文件夹，避免误删或丢失普通目录作品。
- Legacy scanner、Inventory 全量/事件 work-key 推导、watcher 事件、Audio inspector 和 Catalog reconciliation 现在共享同一 helper。Nested RJ、纯文件夹、混合路径、rename、delete/revive 和历史保留测试覆盖同一 work identity；Folder 模式通过真实 watcher/coordinator 测试。
- 新增 schema migration v20，在 `library_roots.audio_grouping` 保存每个 root 的策略。旧 `app-settings.json`、旧 qmediasync source 和旧数据库 root 缺少字段时均回退 `auto`；设置 API 与前端媒体目录设置提供全局音声策略，qmediasync source 也保留 per-source 字段。
- 本轮完整后端验证为 243 passed、0 failed、3 ignored；Audio 定向测试和新增 rename/history 测试均通过。`INVENTORY_SCANNER_ENABLED`、Audio ownership、Facet bitmap、Derivative Cache v2、Search shadow/canary 等实验性生产开关继续关闭。

仍未宣称完成：真实 N100/4GiB/NAS HDD 上约 1 万音频文件的冷扫描、1 万轨单作品、双客户端播放/筛选混合负载、legacy/Catalog v2 逐字段 shadow diff、以及 qmediasync audio/archive bounded coordinator。下一步应先执行真实 Gate，再决定是否推进 Audio/Gallery promotion；不能用开发机回归结果替代实机证据。

## 2026-08-17：Comic Legacy/Catalog v2 promotion 前置证据

- 将 v15 Novel 对账执行器抽象为按 kind 选择 inspector、fingerprint 前缀和 Inventory 文件谓词的共用路径；Novel 回归保持全绿。新增 migration v16，为 `reconcile-catalog-comic` 提供 queued/running 单实例约束。
- 新增 `POST /api/catalog/reconciliation/comic`，与 Novel、完整扫描和搜索重建共用 maintenance 串行边界。Comic 对账逐个读取当前 Inventory 中的 CBZ/ZIP，在 `nas-n100-4g` 单 `ScanIo` 许可下比较 work、scanner-owned archive/cover、tags、external IDs、cover 指针和 materialized stats，差异仍共享每类 256 条上限。
- Comic promotion 现在与 Novel 一样，在 ownership 写事务中重新验证 catalog revision、root generation/event 摘要和零差异证据。没有证据、对账失败或 root event 后证据过期都会拒绝切换；现有 qmediasync provider 阻断继续保留，因为它仍未接入 bounded Inventory coordinator。
- 对账实现暴露并修复三处 Legacy/typed inspector 契约偏差：`AlternateSeries` 恢复写入 Legacy 使用的 `description` 而不是 `subtitle`；自定义 `LanguageIso` tag key 不再额外小写规范化；Legacy 本地 Comic 空标题统一回退为 `Untitled comic`。本地目录封面是实际源文件，因此按完整路径、role、variant、source version 和内容 metadata 比较，不使用 EPUB 提取封面的路径归一化规则。
- 新增测试覆盖 CBZ/ZIP 精确一致、连续通过、无证据拒绝、当前证据 promotion、root event stale、work/asset/tag 漂移、missing/unexpected archive、坏包只记录 error 且 reading history 保留，以及 Novel/Comic reconciliation job 分 kind 单实例。

本轮验证：`cargo test -p media-shelf-server` 222 passed / 0 failed / 3 ignored；`cargo clippy -p media-shelf-server --all-targets -- -D warnings`、`cargo fmt --all -- --check`、前端 `npm run build` 与 `node scripts/validate-project.mjs` 均通过。

尚未完成：真实 N100/4GiB/HDD 上约 1 万 CBZ/ZIP 的全量对账耗时、CPU/RSS、压缩包损坏长尾和浏览混合负载 Gate；qmediasync coordinator；CoserPicture、Audio、Gallery 的同等级 promotion 证据。所有实验性默认开关继续保持关闭。

## 2026-08-17：Catalog 内容/阅读活动修订号分离

- 新增 append-only schema migration v17 `activity-revision-separation`：`activity_state` 单例记录 reading history revision，并由 `reading_history` INSERT/UPDATE/DELETE 触发器推进；`catalog_work_after_update` 不再把仅更新 `works.progress` 视为内容变化。
- 接受的进度保存现在精确产生 `catalog_revision +0`、`activity_revision +1`；过期 update token 两者均 `+0`；标题等内容更新仍产生 `catalog_revision +1`、`activity_revision +0`。`works.updated_at` 继续不因阅读进度变化。
- works、random、counts、collections、history 响应新增 `activity_revision`，前端类型已接收但本批不改变刷新策略。Facet 响应仍只绑定 `catalog_revision`，Search outbox/reconciliation 与 Catalog promotion 门禁也继续只使用内容 revision。
- 量化收益是每次成功进度保存避免 1 次内容 revision 推进、1 次新 revision 下的 Facet 强制 miss，以及一个没有 catalog delta 的 bitmap revision 缺口。直接 SQLite 行写数基本不变，因为原 `catalog_state` 单例更新被 `activity_state` 单例更新替代；真实 N100 提交延迟与 WAL 收益尚未宣称。
- 新增 fresh schema、v16→v17、接受/过期 token、内容更新隔离和 Catalog API 可见性回归。完整验证为 Rust 224 passed / 0 failed / 3 ignored；`cargo clippy -p media-shelf-server --all-targets -- -D warnings`、`cargo fmt --all -- --check`、前端 `npm run build` 与 `node scripts/validate-project.mjs` 均通过。

下一批按风险排序：先补 CoserPicture、Audio、Gallery reconciliation/promotion 证据；再完成 incremental search 生产 reader cutover；随后在真实 N100/4GiB/HDD 上执行 Catalog/Inventory/Search/mixed Gate。Facet bitmap、Derivative Cache v2、Inventory 与 shadow reader 的默认值保持不变。

## 2026-08-17：CoserPicture Legacy/Catalog v2 promotion 前置证据

- 新增 schema migration v18，为 `reconcile-catalog-coser-picture` 提供 queued/running 单实例索引；管理 API 新增 `POST /api/catalog/reconciliation/coser-picture`，并与扫描、搜索重建、Novel/Comic 对账共享 maintenance 串行边界。
- CoserPicture 接入通用 `ReconciliationTarget`：逐个检查当前 Inventory 中的本地 ZIP，复用 N100 单 `ScanIo` 资源约束，比较 work 字段、scanner fingerprint、archive asset、scanner-owned tags、cover 指针和 materialized stats。差异仍按 kind 最多持久化 256 条。
- promotion 事务现在对 CoserPicture 同样要求当前 `passed` 证据、稳定 catalog revision、相同 root generation/event 摘要、expected/matched 完全一致且 missing/unexpected/mismatch/error 全部为 0。qmediasync CoserPicture 根仍由现有路由门禁拒绝 promotion，因为 bounded Inventory coordinator 尚不支持该 provider。
- 新增回归覆盖：无证据拒绝、精确一致与连续通过、成功 promotion、work/tag/archive 漂移、missing/unexpected ZIP、坏包只记录 error 且保留 reading history，以及 root event 后证据 stale；三类 reconciliation job 均保持分 kind 单实例。
- 完整验证为 Rust 229 passed / 0 failed / 3 ignored；`cargo clippy -p media-shelf-server --all-targets -- -D warnings`、`cargo fmt --all -- --check`、前端 `npm run build` 与 `node scripts/validate-project.mjs` 均通过。

动态调整：Audio 与 Gallery inspector 都以 256 assets/chunk、channel depth 2 和 final empty mutation 输出。下一批不能把全部 chunk 聚合回单个 mutation，否则会在 1 万轨/2 万图片作品上恢复线性内存峰值；应先实现共享的有界流式 reconciliation accumulator，再分别接 Audio、Gallery promotion gate。所有实验性默认开关继续保持关闭。

## 2026-08-17：Legacy 扫描有界发现与无变化搜索重建抑制

- 新增 `BatchedFileWalker`，将 `WalkDir` 的匹配路径以 512 条为上限逐批交付，避免漫画、轻小说和 CoserPicture 在首条数据库写入前保留完整路径数组；遍历器状态和后台资源租约跨批次保持，目录不可读、租约失效和 traversal error 仍 fail-closed。
- 漫画、轻小说和 CoserPicture legacy writer 已接入该有界发现器；scope 只在完整遍历结束后完成，部分遍历不会执行缺失 tombstone。音声仍保留完整发现与 work-key 分组，后续单独处理其“流式发现 + 分组”约束。
- 音声也改为 512 条批次发现后再构建 work-key 分组：分组语义保持完整，遍历期间不再同时保留完整发现数组和分组副本；若发现不完整则整个分组阶段丢弃并保留旧作品。
- 完整扫描现在比较扫描前后的 `catalog_revision`：legacy Tantivy 模式只有内容发生变化，或生产索引尚不存在时才排队 `rebuild-search-index`。无变化复扫不再重复触发小时级的全量索引重建；增量 reader 模式行为不变。
- 新增 1,100 文件有界遍历测试、租约失效回归和“已有索引的无变化扫描不重新排队”测试；临时测试辅助函数限定在 test target，严格 Clippy 不再有死代码告警。

本批次当前验证：`cargo test -p media-shelf-server` 为 242 passed、0 failed、3 ignored；`cargo clippy -p media-shelf-server --all-targets -- -D warnings`、`cargo fmt --all -- --check`、前端 `npm run build` 与 `node scripts/validate-project.mjs` 均通过。以上仍是开发机证据，不替代 N100/4GiB/NAS HDD Gate。

下一批优先级：继续评估音声 work-key 分组的首批延迟与单作品长尾，随后完善真实 N100 固定 corpus 的扫描、搜索和混合浏览 Gate；所有实验性默认开关继续关闭。

## 2026-08-17：Comic/Novel legacy snapshot transaction 收敛

- `Db::commit_scanner_work_snapshot` 现在被本地 Comic 与 Novel legacy scanner 共用：作品、最多 512 条资产块、scanner-owned tags、scanner external IDs、旧事实清理和 scanner finish 在同一 fenced lease transaction 内提交。
- Comic 保留 CBZ/普通 ZIP 的 archive variant、目录封面、ComicInfo 标签和原有 `description`/rating/meta 语义；Novel 使用内容寻址的 `epub-cover-v2-*` 封面，避免在事务前必须预先分配 work ID，并扩展安全清理规则以回收无引用的旧 v2 封面。
- 这项变更只减少 SQLite writer transaction 边界，不改变大图库/大音声的 512 条资产分块策略；事务仍可能因单作品标签/资产规模增长而变长，因此不把它等价为 N100 端到端速度提升。
- 修正 snapshot 回归测试中的 SQLite trigger 夹具：`INSERT ... DEFAULT VALUES` 在当前 SQLite trigger 语法路径下不可用，改为显式 `NULL` 主键插入；生产 schema 未改变。

本批次验证：`cargo test -p media-shelf-server` 为 243 passed、0 failed、3 ignored；Comic/Novel 定向测试、`cargo clippy -p media-shelf-server --all-targets -- -D warnings`、`cargo fmt --all -- --check` 均通过。前端构建、项目校验和 perf 工具单测仍需在本批次结束时重新执行。真实 N100/4GiB/NAS HDD 的扫描时长、RSS、SQLite busy/WAL、前台浏览混合负载仍未通过 Gate，所有实验性默认开关继续关闭。

## 2026-08-17：Audio/Gallery 分块扫描的元数据事务收敛

- 新增 `Db::commit_scanner_work_metadata`。Audio 与 Gallery 继续按 512 条资产独立提交，最后将 scanner tags、external IDs、旧 scanner-owned 事实清理和 finish marker 合并为一次 fenced transaction。
- Audio 不再为 RJ/DLsite 两个 external ID、基础分组标签和每种音频格式分别创建事务；Gallery 不再为 folder/artist/filename 标签逐个创建事务。资产分块和当前单 `ScanIo` 许可保持不变，因此不会把 70 万图片或万轨音声聚合到内存或长事务中。
- 这是事务边界和 SQLite writer 争用的改善，不是已测得的 N100 速度倍数；真实 Gate 仍需记录 transaction count、busy/wait、WAL、RSS 与前台请求 P95。

本批次定向验证：Audio 19 passed、Gallery 12 passed；随后应再次运行完整 Rust、Clippy、fmt、前端 build、项目校验和 perf 单测。所有实验性默认开关继续关闭。

## 2026-08-17：qmediasync Comic/CoserPicture bounded Inventory coordinator

- qmediasync Comic/CoserPicture source 现在进入 bounded Inventory discovery，root provider 使用稳定的 `qmediasync:<mount>` key；`.strm` 文件按固定批次写入 `file_inventory`，watcher、重启恢复和 Catalog event 不依赖进程内 mount map。
- Comic/CoserPicture inspector 增加 provider-aware `.strm` 分支：只读取本地 STRM stub、相邻 metadata/封面和有限 fingerprint，验证 URL 格式并把 asset/work identity 写为 `qms-strm://<mount>/<relative>`；不会在扫描阶段下载远程归档或读取远程 ZIP 中央目录。ComicInfo page count 缺失时保持 0。
- 全量/定向 archive queue、missing/delete fence 和本地 provider 扩展名判断已分开：qmediasync 允许 `.strm`，本地仍只允许 CBZ/ZIP；qmediasync audio source 暂不进入 Inventory，避免未实现远程音轨 inspector 的假阳性。
- 新增测试覆盖 provider key round-trip、qmediasync root 配置筛选、Comic/CoserPicture `.strm` inspector、完整 Comic/CoserPicture reconcile、无远程下载以及 N100 单 `ScanIo`/processing-memory 许可归零。

本批次验证：Rust `cargo test -p media-shelf-server --all-targets` 为 249 passed / 0 failed / 3 ignored；`cargo clippy -p media-shelf-server --all-targets -- -D warnings`、`cargo fmt --all -- --check`、前端 `npm run build`、`node scripts/validate-project.mjs` 和 12 项 `node --test scripts/perf/*.test.mjs` 均通过。

动态调整与未完成项：qmediasync Comic/CoserPicture 仍只是 bounded candidate，管理端继续阻止 ownership promotion；尚未执行真实 NAS 上约 1 万 Comic、约 8000 CoserPicture ZIP/STRM 的冷扫描、远程 HTTP/云缓存命中率、N100 RSS/延迟、Legacy/Catalog v2 逐字段对账和稳定窗口。qmediasync audio bounded coordinator 仍留在后续批次；所有实验性默认开关继续关闭。

## 2026-08-17：Legacy library 复合关键集游标与 SQLite 低频 planner 维护

- 旧 `/library` 回退路径的首屏和后续页现在统一使用 `(updated_at DESC, id DESC)` 复合关键集游标；首屏不再按更新时间排序、续页却按 id 排序，避免大库在更新时间变化或 id/更新时间不相关时漏项、重复或乱序。
- 新游标携带 `updated_at` 与 `id` 两个边界；带 `next_updated_at` 的新格式使用复合谓词，旧的 id-only 游标仍可解码并按兼容路径继续，避免迁移期间让已有客户端失效。
- Catalog stats backfill 在扫描器空闲、统计无待处理批次且 catalog revision 发生变化时，最多每 15 分钟在既有 `CatalogWriter` 维护许可内调用一次 SQLite `PRAGMA optimize`；SQLite health snapshot 同时报告调用次数、最近成功时间和最近错误。优化不在交互请求或扫描事务中同步执行，也不改变 SQLite/WAL/ownership 默认值。
- 新增非相关 id/更新时间顺序的分页回归与 planner pragma 回归；当前开发机验证为 Rust 253 passed / 0 failed / 3 ignored、Clippy `-D warnings`、fmt 通过。仍需在 N100 上量测 planner 维护耗时、WAL 和前台 P95；这两项实现不等于 N100 Gate 通过。

## 2026-08-18：qmediasync Audio bounded coordinator 候选

- 配置中的 qmediasync audio source 现在进入稳定的 `qmediasync:<mount>` Inventory
  root；全量 walker、watcher changed-key、missing/revive 和 Audio Catalog event 共享
  同一个 work identity。
- Audio inspector 只读取本地 `.strm` stub 并校验 URL，不下载远程音频或读取远程媒体
  metadata；远程音轨和封面资产使用 `qms-strm://<mount>/<relative>`，大小未知时保留
  `NULL`，播放继续复用现有 Range/cache 代理。
- qms audio 仍受 256 asset chunk、深度 2 channel、20,000 文件/16MiB 路径预算以及
  N100 单 `ScanIo`/processing-memory 约束；本地 audio root 的 `.strm` 不会进入过滤器。
- 管理端对含 qmediasync audio root 的 ownership promotion 新增 fail-closed 门禁，
  因为当前没有 legacy qmediasync audio writer 的逐字段事实证据；默认配置和所有实验
  开关均未改变。

新增开发机回归覆盖远程音频身份、无下载、资源许可归零、Inventory reconcile 和
qmediasync reconciliation 在无 legacy ownership 时的明确 mismatch 结果；本批完整后端
回归为 257 passed / 0 failed / 3 ignored，Clippy、fmt、前端生产构建、项目校验和 12
项 perf 单测均通过。仍不能代替
真实 N100/4GiB/NAS HDD 的冷扫描、远程 HTTP/cache 命中、起播 P95、断流与混合负载 Gate，
也不能据此启用 `INVENTORY_SCANNER_ENABLED` 或切换 audio ownership。

## 2026-08-18：qmediasync Range/cache Gate 观测补齐

- `/api/health/resources` 新增进程内 qmediasync 累计计数：Range/全量请求、成功/206/416/失败、实际流出字节，以及云缓存 hit/miss/304/download bytes、配额拒绝和目录重扫次数；不记录 URL 或媒体正文，不改变路由与默认缓存配额。
- `scripts/perf/run-http-scenario.mjs` 新增 `--range`、`--max-body-bytes`、`--abort-after-bytes`，可记录 `Content-Range`、`Content-Length`、`Accept-Ranges`、实际读取字节和主动断流；配合 `--health-url`/`--runtime-output` 保存 qms 前后计数器 delta。
- Resource Governor 同步暴露累计资源等待样本、总/最大等待微秒和交互超时次数，补足仅有当前 waiter 数量时无法判断的排队长尾。
- `sample-system.mjs` 与 baseline CSV 同步记录资源等待及 qms stream/cache 计数；`evaluateQmsRuntimeEvidence` 对请求数量、Range/206 契约、失败、配额拒绝和下载字节记账执行 fail-closed 判断。

这批只增加可审计证据，不代表 qmediasync 音声已经通过 N100 Gate。仍需在真实 NAS/N100/4GiB 上执行冷/热起播、断流、云缓存命中和双客户端混合负载，并与 RSS、HDD await、permit 归零及 24 小时 soak 结果一起评估。

## 2026-08-18：共享有界 reconciliation accumulator

- `catalog_reconciliation.rs` 将 Audio/Gallery 共用的 chunk 聚合器明确收敛为
  `ReconciliationAccumulator`。它只保留固定大小的资产 multiset 摘要、封面 digest 和
  有界关系列表，不再存在把所有 chunk 重新拼成单个 `WorkMutation` 的路径。
- 新增跨 chunk 总资产闸门 20,001，专门覆盖图库 20,000 文件加独立封面关系；超限会
  返回 fail-closed 错误，避免恶意/异常 inspector 通过无限 chunk 消耗内存。
- Audio inspector 消费 owned inventory paths，并在重复检查完成后释放重复路径集合；
  fingerprint 输入改为 move，减少 20,000 文件作品的路径 clone 常驻量。
- 回归新增/调整：20,000 资产摘要与超限测试、图库 20,000 图片的 256 chunk/深度 2
  channel/finalize 测试；已有 Audio chunk、资源 permit 归零和摘要顺序无关测试继续保留。

本批尚未开启任何实验性默认开关。开发机定向回归已通过；完整 Rust/Clippy/fmt、前端
构建、项目 validator 和 perf 单测仍需在本轮收尾执行。真实 N100/4GiB/NAS HDD 的 RSS
峰值、长尾扫描耗时、SQLite busy/WAL 与混合浏览 Gate 仍是未完成证据。

## 2026-08-18：增量搜索 reader 门禁边界修复

- `SEARCH_INCREMENTAL_READER_ENABLED` 现在显式要求同时开启 shadow outbox 和 shadow
  canary；否则启动阶段直接拒绝配置。原因是 persisted fact reconciliation worker
  依赖 canary flag，避免 reader 开关处于永远无法通过门禁的状态。
- `ensure_incremental_reader_gate` 接受合法的 revision-zero 空 Catalog；schema 已保证
  baseline revision 非负，新增测试覆盖空索引、全 kind ownership、零 pending、零差异
  对账后成功 arm。
- 默认 flag、production reader 回退和正常 revision churn 的 fail-closed 行为不变。

定向 Rust 测试通过；本批仍需全量回归收尾，并且不能替代真实 fixed corpus、N100/4GiB
RSS/WAL/延迟与稳定窗口证据。

## 2026-08-18：production incremental reader fixed-corpus runner

- 新增 `scripts/perf/run-search-incremental-reader.mjs`，对默认 `/api/search` 执行 1–256
  条固定 query，记录 query hash、状态、TTFB/总耗时、`reader` 和 `rebuilt`，不保存明文
  query；起止 health 会捕获 outbox、shadow、reconciliation 门禁。
- 新增 `evaluateSearchIncrementalReaderEvidence`，要求 cutover 已 arm、shadow ready、
  reconciliation passed、pending/revision lag 为零、shadow/catalog revision 一致，且
  所有请求 `reader=production`、`rebuilt=false`。
- 新增 perf 单测与项目 validator 结构检查；脚本不会自动开启任何 flag，失败只生成
  fail-closed 证据。

开发机 perf 单测为 14 passed；真实 NAS/N100/4GiB fixed corpus、P95/P99、RSS、WAL 和
稳定窗口仍待执行。

## 2026-08-18：归档作品 metadata-only 详情

- `Db::work_detail` 对 Comic/CoserPicture 不再直接返回全部 `page`/`image` 资产；详情
  只返回有界的 archive/cover/支持性资产，当前上限为 16 条。漫画页面继续由 manifest
  和 indexed page stream 路由按需读取，完整资产仍可通过 `/works/{id}/assets` 的 keyset
  分页接口获取。
- `asset_count` 仍来自 `work_stats`/事实表，`assets_complete=false` 明确告知客户端
  详情已截断；已有音声和图库边界保持不变。
- 新增 Comic/CoserPicture 300 页资产回归，确认详情响应只保留 archive/cover、总数精确、
  不 materialize 页面集合。该改动不切换任何实验性开关，也不替代 N100 JSON/RSS Gate。

## 2026-08-18：Comic/CoserPicture reader 派生页候选

- `/works/{id}/pages/{page}/stream` 新增可选 `size` 参数，仅接受 1280/1920 两个有界
  bucket；Derivative Cache v2 开启时按 `archive-page + archive asset + page name + source`
  生成 JPEG reader derivative，并复用既有 single-flight、ledger 和资源预算。
- Derivative v2 关闭、生成过载、退避或解码失败时，接口继续返回原始 ZIP 页面流；因此当前
  默认行为和回滚路径不变。前端按视口请求 1280/1920，放大超过 1.25 倍时请求原图。
- 新增 reader size bucket 回归；前端生产构建和 Clippy 通过。仍需在 N100 上验证 24/50MP
  页面、冷/热派生命中、RSS、磁盘配额、断流 permit 归零后，才能开启 Derivative Cache v2
  默认值。

## 2026-08-18：Shadow 搜索 degraded 的显式恢复路径

- 新增受 CSRF 保护的 `POST /api/search/shadow/rebuild`，通过唯一维护 job
  `rebuild-shadow-search-index` 执行，不与生产 `search-index-v2` 重建复用路径。
- 普通 shadow worker 在 `degraded` 状态下不会自动反复重建；显式恢复会在同一 outbox
  串行边界内从 SQLite 一致快照重建 shadow index，清理旧 claim 前缀，并将
  `search_reconciliation_state` 重置为 `unknown`、`cutover_armed=0`，随后必须重新事实对账。
- 恢复成功后丢弃 shadow reader 和对应候选缓存；生产 reader、生产索引和媒体数据不受影响。
  新增 degraded → forced rebuild → 未 armed 回归，避免磁盘/segment 故障在 N100 上形成
  rebuild storm。
- 本批完整后端回归为 268 passed / 0 failed / 3 ignored；Clippy、fmt、前端构建、项目
  validator 和 14 项 perf 单测均通过。仍未开启 shadow/incremental 默认开关，也不能替代
  真实 fixed corpus、N100/4GiB RSS/WAL 与稳定窗口 Gate。

## 2026-08-18：增量搜索提交后的 reader 刷新与按索引候选失效

- `SearchRuntime` 为每个候选缓存键记录所属 Tantivy 索引目录；production rebuild 或 shadow
  增量提交只失效受影响索引的候选，不再无差别清空另一索引的候选结果。
- shadow outbox 批次提交后，若该索引已有常驻 reader，后台显式调用 Tantivy `reload()`；
  baseline 重建则直接丢弃旧 reader，避免 schema/segment 替换时复用旧快照。两条路径均在
  health runtime 中记录 reader reload 与候选失效计数。
- 新增回归覆盖：reader 提交后移除旧词、保留另一个索引的候选缓存，以及 reload/失效计数。
  该改动不改变任何实验性开关默认值，也不代表 production incremental reader 已通过真实
  fixed corpus、N100/4GiB RSS/WAL 或稳定窗口 Gate。

## 2026-08-18：Catalog promotion 搜索门禁统一与 revision-zero 修复

- `routes.rs` 不再复制一份独立的 shadow 搜索 promotion 条件；ownership promotion
  现在调用 `search::outbox::search_promotion_gate_ready`，统一检查 schema、baseline、
  applied revision、事实对账 revision 和 outbox pending 状态。
- 门禁只接受明确的 `shadow`/`ready` 状态，并要求 baseline 不晚于 applied revision；未知
  持久化状态或不可能的 revision 关系会 fail-closed。
- 合法的空 Catalog `baseline_revision=0` 不再被 `/catalog/ownership/{kind}` 错误拒绝；
  首个 kind 仍可以在 shadow index 为 `shadow`、而非最终 `ready` 时按逐 kind 计划推进。
- 新增 revision-zero 正向用例和 schema/revision 不一致负向用例。该修复只收紧门禁一致性，
  不开启 Inventory、Search shadow、incremental reader 或任何默认实验开关。

本轮证据：后端 287 passed、0 failed、3 ignored；Clippy `-D warnings`、fmt/diff check、
前端 production build、项目 validator 和 17 项 perf 单测通过。真实 fixed corpus、N100/4GiB
RSS/WAL/延迟与逐 kind 稳定窗口仍未完成，不能据此切换 production reader 或 ownership。

## 2026-08-18：开发机规模验收补跑（Inventory / Derivative）

- 显式执行 `synthetic_700k_inventory_uses_fixed_batches`：700,000 行耗时 17,616 ms，
  `max_batch_rows=1024`，`max_batch_bytes=169,985`；用例通过，证明当前 Inventory
  pipeline 在合成大表上没有退回无界批次。
- 显式执行 Derivative ledger 700,000 行：插入 9,625 ms、容量查询 1,163 µs、淘汰
  88,704 行 26,683 ms，WAL 21,860,752 bytes；1,400,000 行：插入 21,650 ms、容量查询
  1,875 µs、淘汰 177,408 行 55,146 ms，WAL 24,015,512 bytes。两档均从约 32 GiB 高水位
  收敛到 28 GiB 低水位，`idx_derivatives_eviction_lru` 被使用且 `integrity_check=ok`。
- 以上均为本机 Windows debug 构建的控制台证据，未写入目标 NAS 的结构化 Gate artifact；
  不代表 N100/4GiB 的 CPU、HDD await、RSS、温度或混合负载通过，实验性开关继续关闭。

## 2026-08-18：单选标签 Facet 直接索引路径

- 选择性 Facet 在只包含一个 `include_tag`、且没有 kind 以外的集合/文本/搜索候选约束时，
  现在先解析 tag ID，再从 `(tag_id, work_id)` 覆盖索引读取候选作品并直接做共现聚合；不再
  为单标签执行通用 `matched(work_id)` 的 `GROUP BY + COUNT(DISTINCT)`。多标签、集合、文本
  搜索、bitmap 和 universal kind-level 预聚合继续使用原有路径。
- 新增回归覆盖：kind 隔离、标签文本筛选、游标续页、未知标签空结果和上下文计数；没有切换
  任何实验性生产开关。
- 在现有 40k/800k 开发机 fixture 上用等价 SQL 形状做 8 次样本对比（该历史 fixture 的
  `works` 尚无 `deleted_at` 列，因此只用于查询形状，不是最终 Gate）：20k 覆盖的单标签
  路径从 P50/P95 `553.33/578.18ms` 降到 `471.60/544.67ms`，约为 14.8%/5.8% 改善；
  40k 覆盖标签则从 `931.30/1081.35ms` 降到 `913.55/1077.83ms`，说明高选择性收益更
  明显，全集标签仍受共现聚合本身限制。N100/4GiB 冷 Facet Gate 仍未执行。
- 本批完整后端回归为 `295 passed / 0 failed / 3 ignored`；Clippy `-D warnings`、fmt、
  前端 production build、项目 validator 和 18 项 perf 单测通过。该结果仍不能替代目标
  NAS 上的 SQLite P95、RSS、WAL、HDD await 和混合浏览测量。

## 2026-08-19：Catalog reconciliation / legacy search 写入闸门补齐

- Catalog reconciliation 的运行时错误状态写入现在在短 SQL 更新前获取 SQLite 单写入闸门；
  错误记录仍只更新 `catalog_reconciliation_state`，不会把 Tantivy 构建过程放进 SQLite
  writer 临界区。
- legacy search rebuild 完成后的 `search_outbox` 确认写入现在同样经过单写入闸门，仍只
  确认不晚于一致性快照 revision 的 outbox 行，不改变既有覆盖边界。
- 两条路径均增加 writer completion 计数与持久化状态回归，确认短事务完成后闸门计数推进。

本批验证：后端 `cargo test -p media-shelf-server --all-targets` 为 `297 passed / 0 failed /
3 ignored`；`cargo clippy -p media-shelf-server --all-targets -- -D warnings`、
`cargo fmt --all -- --check`、前端 `npm run build`、`node scripts/validate-project.mjs`、
`git diff --check` 和 18 项 `scripts/perf` 单测均通过。该批仍是开发机证据；没有开启任何
实验性开关，也没有改变 Inventory ownership。

下一批优先处理 Inventory 的短状态写入：`complete_event`、事件失败/重试、事件清理以及
root/coordinator 完成状态。需要先把会嵌套调用的 Novel coordinator 状态更新收敛到同一
事务，避免简单叠加 writer gate 造成自阻塞；`mark_watcher_gap` 等已经在事务闸门内的路径
不重复获取许可。完成后再跑相同完整回归，并进入真实 N100/4GiB/NAS Gate 准备，而不是
据开发机结果切换默认开关。

## 2026-08-19：Inventory 短状态写入闸门与嵌套闸门修正

- Inventory 的事件完成、事件失败/重试、已完成事件清理、Novel coordinator 状态、Novel
  lease 释放、root 完成和 root 异常状态写入现在经过 SQLite 单写入闸门；已有事务闸门的
  `mark_watcher_gap` 不重复获取许可。
- Novel lease 释放在同一个 writer 临界区内更新 root fence 和 coordinator phase；事件 claim
  在已持有 writer 时调用 unlocked coordinator 更新，避免单 writer semaphore 自等待。
  watcher gap 提交后先释放闸门再进行事件清理，同样避免嵌套获取。
- 新增短状态回归，覆盖 completion 计数、失败状态、coordinator checkpoint/error、root
  generation completion 与 idle 状态；首次全量运行发现并修复了单许可 N100 coordinator
  死锁边界，随后 Inventory 定向测试 `37 passed / 1 ignored`。

本批最终验证：后端 `cargo test -p media-shelf-server --all-targets` 为 `298 passed / 0 failed /
3 ignored`；`cargo clippy -p media-shelf-server --all-targets -- -D warnings`、
`cargo fmt --all -- --check`、前端 `npm run build`、项目 validator 和 18 项 perf 单测均通过。
仍然只代表开发机证据；legacy scanner/inventory 其余直接 SQL 写入、真实 N100/4GiB/NAS
RSS、SQLite busy/WAL、HDD await 和混合浏览 Gate 尚未完成，所有实验性开关与 ownership
默认值保持不变。

下一批继续静态盘点 Inventory/legacy scanner 的剩余直接写路径，优先把可独立包成短事务的
状态更新纳入闸门；对已经由更大事务覆盖的调用链保留 unlocked helper，禁止机械叠加。之后
转入真实固定媒体清单的 G0/DB1 基线与逐 kind shadow diff 准备。

## 2026-08-19：Db 直接写入与启动迁移闸门补齐

- `Db::upsert_work`、`Db::upsert_tag` 和迁移期 `coalesce_existing_jobs` 现在统一经过
  SQLite 单写入闸门；Enrichment、生成资源和测试夹具的直接调用不再绕过写入队列。
- 启动迁移的 schema 建表、兼容列变更、scanner 表/索引维护和 pending migration 现在按
  有界估算字节分段获取 writer slot；`migrate_asset_identity`、scanner ownership、审计
  清理仍使用已有 gated helper，避免把整个启动过程错误地包成可嵌套的单一 guard。
- Inventory `begin_root_scan` 的 generation/status fence 写入也补齐短闸门；已有事务内的
  `update_novel_coordinator_state_unlocked` 等路径保持 unlocked，避免单 writer 自等待。
- 新增 `direct_upserts_and_job_coalescing_use_the_single_writer_gate` 回归，确认 upsert、
  job coalesce 的 writer completion 计数和持久化结果；`db` 定向测试 32 passed / 1 ignored。

本批完整验证：`cargo test -p media-shelf-server --all-targets` 为 299 passed / 0 failed /
3 ignored；Clippy `-D warnings`、fmt、diff check、前端 production build、项目 validator
和 18 项 perf 单测均通过。仍然只是开发机证据；真实 N100/4GiB/NAS HDD 的扫描、预览、
筛选、起播、RSS、SQLite busy/WAL、HDD await、混合负载和稳定窗口 Gate 尚未执行，所有实验
性开关及 ownership 默认值继续保持关闭。

## 2026-08-19：Catalog Facet 错误写入与 WAL 探针闸门补齐

- Catalog Facet 统计后台任务在失败时写入 `tag_kind_count_state.last_error` 的独立短 SQL
  现在经过 SQLite 单写入闸门；新增回归确认错误文本持久化且 writer completion 计数只
  增加一次。
- `PRAGMA wal_checkpoint(PASSIVE)` 也纳入短 writer slot。它仍然是非阻塞 checkpoint，
  但不再与 Catalog/Inventory 写事务并发，从而避免健康探针改变 N100 Gate 的 busy/WAL
  观测；回归增加 checkpoint 的 gate completion 断言。
- 本轮生产 SQL 静态复核确认：Inventory coordinator、Catalog reconciliation、Search
  outbox、Derivative ledger、archive manifest 和 DB legacy writer 的独立生产写入口均已
  有 gate，或明确运行在已持有 gate 的事务/unlocked helper 中；没有机械给嵌套事务重复加
  gate。

本轮最终验证：后端 `300 passed / 0 failed / 3 ignored`；Clippy `-D warnings`、fmt、
diff check、前端 production build、项目 validator 和 18 项 perf 单测均通过。仍未执行
真实 N100/4GiB/NAS HDD Gate，实验性开关和 ownership 默认值保持关闭。

补跑的开发机规模 Gate：700,000 行 Inventory 耗时 18,192 ms，固定批次 1,024 行，最大
序列化批次 169,985 bytes；Derivative ledger 700,000 行插入 9,860 ms，容量查询
1,451 µs，淘汰 88,704 行 29,274 ms，WAL 21,860,752 bytes，完整性检查通过且只使用
`idx_derivatives_eviction_lru`。这些结果继续只是开发机批次/查询形状证据，不能替代 N100
CPU/RSS、NAS HDD await、温度降频和混合负载 Gate。

## 2026-08-19：G0 dataset manifest 长尾与 feature flag 完整性

- `scripts/perf/capture-baseline.mjs` 现在把本轮新增的安全开关和资源边界一并写入
  `environment.json`，包括 Facet bitmap、Inventory kind rollout、Search shadow/canary/
  incremental reader、JPEG DCT、本地媒体流、Catalog/Search writer 和派生缓存水位；仍不
  记录凭据、查询明文或媒体路径清单。
- `collectDatasetManifest` 保持 SQLite 只读，并新增按 kind 的资产/标签数量分布
  （P50/P95/P99/max）、Inventory root/file/status/字节和单作品文件长尾、deleted work
  数量、Catalog/activity revision 以及非敏感 Search index readiness。旧库缺少可选表或列时
  显式返回 `null`/空数组，不因兼容副本而伪造 0。
- 新增 fixture 覆盖上述长尾事实和 schema 兼容回退；四组 perf 单测当前为 21 passed、0 failed。
  该批只增强证据采集，不扫描媒体正文、不修改数据库、不改变任何默认开关。

这些仍是本地证据。真实旧库副本和 N100/NAS Gate 仍需保存 `dataset-manifest.json`、原始
JSONL/CSV、RSS/PSS、SQLite busy/WAL、HDD await 和混合负载结果；pool acquire/busy、最老
读快照及 worker/permit 时序字段仍是 G0 的后续观测缺口。

## 2026-08-19：DB1 系统采样补齐 SQLite writer/checkpoint 时间序列

- `scripts/perf/sample-system.mjs` 现在将 `/api/health/resources` 中的 SQLite pool 大小、
  idle connections、writer queue depth/bytes、active/bytes、累计 acquire wait、hold 时间，
  以及 WAL passive checkpoint 的 busy/log/checkpointed pages 和耗时写入同一 CSV 行。
- `capture-baseline.mjs` 的初始 CSV 表头同步更新；采样器会校验既有文件表头，不会把旧格式
  数据错位追加到新格式。性能 artifact 版本提升为 2，明确标识该 schema 变化。
- 采样仍只读取健康快照，不增加 checkpoint；只有显式 `?checkpoint=true` 的基线探针才会
  执行 PASSIVE checkpoint。新增 smoke 验证确认 baseline 与 sampler 均为 55 列且行列一致。

本批仍未伪造 pool acquire/busy 或最老读快照数据；这些字段必须在真实 N100/NAS 服务运行时
采集。当前四组 perf 单测为 21 passed、0 failed，后端/前端全量验证保持通过，默认开关和
ownership 继续关闭。

## 2026-08-19：媒体流/manifest 最小资产查询

- 漫画页清单、页面流以及 EPUB manifest/章节/图片请求不再先读取完整作品详情；新增
  `Db::work_asset_by_role`，按作品、角色和可选 MIME 只取一个有效源资产。
- Comic page count 回写保留原有 kind/meta 语义；EPUB 与漫画的缓存、qmediasync、Range、
  资源租约和失败回退路径不变。新增角色/MIME/缺失资产回归，避免用未验证的源行继续读盘。
- 该批减少高频翻页/预览的 SQLite 读放大，但没有开发机端到端倍数证据；仍需在 N100/
  NAS HDD 上记录连续翻页、EPUB 图片请求的 SQLite P95、WAL/busy、首字节和混合负载。

## 2026-08-19：统一媒体预览/筛选 Gate 编排

- 新增 `scripts/perf/run-media-gates.mjs` 与 `media-n100-4g.json`，统一编排书架首屏、标签
  筛选、图库冷/热缩略图、漫画/CoserPicture 页面、音频 Range 起播和轻小说摘要详情。
- 每个 target 复用现有 HTTP runner，生成脱敏原始 JSONL、nearest-rank P50/P95/P99、资源
  目录和 fail-closed Gate；缺失模块或样本不足会返回 `incomplete`，不会被当作通过。
- 该批只完善真实 N100/NAS 的可重复验收入口，尚无实际硬件结果；仍需在 4GiB cgroup、目标
  HDD/SSD 布局和双客户端混合负载下执行。

## 2026-08-19：媒体 Gate runner 路径与回归收尾

- `run-media-gates.mjs` 现在基于自身 `import.meta.url` 定位仓库根目录；内置的 HTTP 子 runner
  和默认 `media-n100-4g.json` 不再依赖调用者当前工作目录。媒体 Gate 回归改为从仓库外的
  临时工作目录启动，覆盖该路径契约。
- `run-media-gates.mjs` 现在还会把任一子 runner 的非零退出写入 `summary.gate` 的
  `target-runs` 失败检查，避免“进程失败但合并样本仍显示通过”的歧义；新增回归覆盖
  不可达目标、仓库外启动和默认 Gate 配置路径。
- 本轮验证：后端 `301 passed / 0 failed / 3 ignored`；Clippy `-D warnings`、fmt、前端
  production build、项目 validator 和全部 `scripts/perf/*.test.mjs`（23 passed / 0 failed）
  均通过。
- 这仍只是工具和开发机回归证据；真实 N100/4GiB/NAS HDD 的媒体 Gate、旧库 migration、
  RSS/PSS、SQLite busy/WAL、HDD await、混合负载和 24 小时稳定窗口尚未执行，实验性开关与
  ownership promotion 继续保持关闭。

## 2026-08-19：旧库迁移 Gate 与开发机 G0 基线

- 新增 `scripts/perf/run-migration-gate.mjs`：以 quiesced SQLite 文件为输入，拒绝非空 WAL，
  只复制源库到新 artifact，在副本上启动当前服务，等待 `/api/health`，并记录启动耗时、
  schema version、`integrity_check`、源库 SHA-256 和服务日志；源库不会被写入。
- 当前开发库 G0 artifact 位于 `perf-results/g0-dev-current/`：数据库事实为 40 works、
  1,395 assets、42,621 tags；五类媒体目录仅为开发数据，不代表目标 NAS 规模。图库最大单
  作品为 1,166 个资产，说明后续真实 Gate 仍必须保留单作品长尾，而不能只按总量造平均值。
- 使用当前 release 二进制对该旧库副本执行迁移与恢复副本校验：启动约 284ms，schema v22，
  两份数据库均为 `integrity_check=ok`，源库前后 hash 一致，artifact 位于
  `perf-results/migration-dev-current-v22c/`。旧的 v21 release 二进制被同一 Gate 正确判定
  失败，证明版本落后不会被误报为通过。
- 新增迁移 Gate 的 quiesced/WAL/健康启动回归后，完整验证为后端 `301 passed / 0 failed /
  3 ignored`、Clippy、fmt、前端 production build、项目 validator 和 25 项 perf 测试通过。
  该证据仍不是 N100/4GiB/RSS/WAL/HDD await 或媒体延迟 Gate；实验性开关与 ownership
  promotion 继续保持关闭。

## 2026-08-19：G0/DB1 cgroup CPU quota 证据补齐

- `/api/health/resources` 现在同时返回 `cgroup_cpu`：包括 cgroup v2 的
  `cpu.max` 或 v1 的 `cpu.cfs_quota_us/cpu.cfs_period_us`、有效核数以及显式的
  unlimited/null 语义。该字段只观测当前进程所在 cgroup，不改变 Resource Governor
  的 worker/permit 限流策略。
- `scripts/perf/sample-system.mjs` 的 CSV schema 升级为 artifact v3，新增内存上限、
  CPU quota、period 和有效核数列；`capture-baseline.mjs` 的 `environment.json` 也保存
  初始 cgroup v1/v2 形状。缺少 cgroup 文件系统或 quota 为 unlimited 时记录空值，不能
  把“未观测”伪造成 4GB/CPU 限制已生效。
- 新增 v2 quota、unlimited、非法/零 period 和 cgroup v1 内存 sentinel 回归。当前完整验证为后端 `306 passed / 0
  failed / 3 ignored`，Clippy `-D warnings`、fmt、release build、前端 build、项目
  validator 和 30 项 perf 测试均通过。

这批只补强 G0/DB1 的约束证据，不构成真实 N100 Gate。仍必须在目标 Linux/N100/NAS
保存 cgroup limit、RSS/PSS、CPU 温度/降频、SQLite busy/WAL、HDD await、worker/permit
时序和媒体混合负载结果；所有实验性开关与 ownership promotion 继续关闭。

## 2026-08-19：G0/DB1 SQLite pool 与 worker/permit 观测补齐

- SQLite runtime health 现在同时报告 pool 当前 active/saturated、SQLx checkout 次数、
  连接创建次数、复用前 idle 时长累计/最大值，以及显式 tracked transaction 的 acquire
  样本和等待累计/最大值；pool checkout 通过 SQLx `before_acquire` 覆盖隐式 query acquire，
  不把连接复用 idle 时长冒充队列等待。
- Db 内生产事务入口已迁移到 `begin_tracked_transaction`，pool acquire timeout/error 和
  `database is locked/busy` 计数在 health 中独立呈现；未迁移的 legacy 直接 query 仍不被
  误计入 tracked error 计数。
- `/api/health/resources` 新增 jobs runtime：当前 worker limit、active jobs、维护闸门持有数、
  maintenance permit 等待样本/累计/最大时长、成功/失败 job 和 claim error；Resource
  Governor 的每个 pool 也增加 wait samples/total/max/timeout，组合预约重复归因被明确记录。
- `system-samples.csv` 升级为 artifact v4（80 列），保存上述 SQLite pool、busy/error、
  worker/permit 证据；新增 Rust/Node 回归覆盖 pool checkout、tracked transaction 和
  每类资源池等待计数。

本批只增强观测和事务入口，不调整 N100 连接数、资源配额、默认开关或 ownership。开发机
验证仍不能替代真实 Linux/N100/NAS；真实 Gate 仍必须记录 pool saturation/acquire、长读
快照/WAL busy、RSS/PSS、HDD await、worker 时序和媒体混合负载。`before_acquire` 的 idle
时长不是 pool queue wait，真实部署仍需结合系统采样解释。

## 2026-08-19：Search reader prewarm 候选与隔离 A/B 证据

- 新增显式关闭的 `SEARCH_READER_PREWARM_ENABLED`。启动阶段先确保 legacy Tantivy
  production index 存在，再以最多 30 秒超时尝试打开并保留 reader；失败或超时只记录
  warning，继续使用 lazy reader，不阻断服务，也不会执行用户查询或填充无界 candidate
  cache。健康接口同时暴露该开关状态。
- 新增 `scripts/perf/run-r1g-prewarm-ab.mjs`，对同一份 R1G v22 quiesced SQLite 与
  Tantivy index 分别启动 lazy/prewarm 两个隔离副本，记录启动时延、prime triplet、warm
  P50/P95/P99、reader/candidate runtime counters，并要求健康状态与运行时证据一致。
  `scripts/validate-project.mjs` 已将该 runner 纳入必备文件和静态契约检查。
- 当前开发机 release A/B artifact 位于
  `perf-results/r1g-dev-20260819-v22/evidence-prewarm-ab-current-v3/`，源库 SHA-256
  `fc5005e158ee0db8330ee65be9973714fa624f38c6e5b1b5979f5702136586f7`，两组各 30 个并发
  works/counts/facets triplet 均成功。修正启动采样边界后，lazy/prewarm 的健康就绪耗时约为
  `1027/990ms`；首个 prime triplet TTFB P95 为 `119.819/116.078ms`，total P95 为
  `120.167/116.401ms`。reader 在首个请求前的 opens 为 `0/1`，之后均保持 1；两组
  candidate 均为 1 miss、2 coalesced、90 hits，说明 prewarm 没有扩大 candidate cache。
  warm works/counts/facets P95 分别为 `15.702/15.828ms`、`22.100/22.327ms`、
  `2.170/2.143ms`。
- 以上只是一轮 Windows 开发机、已有 index 文件、非 4GiB cgroup 的候选证据；本轮差异
  接近测量噪声，不能外推为 N100 收益，且没有 RSS/PSS、HDD await、CPU 降频或 NAS
  cold-open 证据。历史 R1G cold artifact 的约 4.65s 长尾来自不同的旧启动条件；当前代码
  已在 listener readiness 前确保 production index 存在，因此不能与本轮直接横比。默认
  开关、ownership promotion 和生产 reader cutover 继续关闭；下一 Gate 必须在真实
  N100/NAS 上重复真正冷启动、启动/RSS、SQLite/WAL 与混合负载测量。

### 30.45 DB1 生产事务入口收敛（2026-08-19）

在 read snapshot 观测之后继续审查了事务入口，发现部分生产模块仍直接调用
`db.pool().begin()`，会绕过 `Db::begin_tracked_transaction` 的 pool acquire/error/busy
观测。现已将资产 manifest 写入、Catalog 统计/Facet 维护、Catalog reconciliation、
Catalog writer ownership/mutation/tombstone、Derivative ledger、Inventory coordinator
及 Search outbox 的生产事务统一切换到 `begin_tracked_transaction`。显式 writer gate 的
获取顺序和事务边界保持不变；迁移初始化函数和 `#[cfg(test)]` fixture 仍保留直接 pool
事务，因为它们发生在 `Db` runtime 尚未建立或仅用于测试隔离。

静态复核结果：生产源码中剩余的 `pool.begin()` 仅为 migration 初始化或测试代码；生产
事务入口均可由 tracked acquire 样本覆盖。`cargo check`、`cargo fmt`、全量后端测试
311 passed/0 failed/3 ignored、Clippy `-D warnings` 和 perf 30 passed/0 failed 均通过。
这仍只扩大观测和写路径一致性，不打开实验性开关、不切换 ownership，也不替代真实
N100/4GiB/NAS HDD 的 busy/WAL/RSS/延迟 Gate。

### 30.44 G0/DB1 显式 read snapshot 生命周期观测（2026-08-19）

本轮补齐了此前仅有 pool/WAL 间接证据的一个缺口：`Db::begin_tracked_read_transaction`
现在返回带 RAII 统计的 `TrackedReadTransaction`，记录 active、samples、completed、hold
total/max、当前最老 active 事务时长和 implicit rollback。包装层解引用到底层
`SqliteConnection`，兼容 SQLx 0.8 的 `Executor` 约束；commit、rollback、取消/提前 drop
均会结束观测区间，且不会把普通单语句 pool query 伪装成 read snapshot。

本轮迁移到该 helper 的范围仅包括确实需要一致性多查询快照的 Catalog、Facet bitmap 和
Search 路径；未迁移的 legacy transaction 仍明确不计入这些字段。性能 artifact schema
同步升级为 v5，`system-samples.csv` 增加 7 个 read snapshot 列（共 87 列），采样器和
schema 回归已同步更新，避免旧列布局被继续追加。

验证结果：`cargo check -p media-shelf-server` 通过，Rust 定向 read snapshot 测试 3/3
通过，`cargo fmt --all -- --check` 通过。下一步仍需执行完整 Rust/Clippy/perf/frontend
回归，并在真实 N100/NAS 上验证长读事务、WAL busy、RSS/PSS 与媒体混合负载；本轮不打开
实验性开关、不切换 ownership，也不宣称 legacy 路径已完全覆盖。

### 30.46 N100/4GiB 实机前置 Gate（2026-08-19）

新增 `scripts/perf/check-n100-environment.mjs` 与纯函数 evaluator，在媒体 HTTP
矩阵前先验证运行边界：Linux、Intel N100 CPU、有限且不超过 4GiB 的 cgroup memory、
服务端 `sqlite.config.profile=nas-n100-4g`、健康状态为 `ok`，以及可选的 NAS
`/proc/diskstats` block device。任一证据缺失时结果为 `incomplete`，不允许生成通过
的 N100 artifact；已有文件也不会被覆盖。artifact 只保存健康 URL path，不保存认证或
查询内容。

新增 3 项 evaluator 回归覆盖完整通过、缺失证据和非 N100/超 4GiB 失败。该工具只是
确保后续媒体 Gate 的硬件边界可信，当前 Windows 开发机执行应当失败，不能被当作 N100
结果；实验性开关和 ownership promotion 继续关闭。

## 2026-08-19：DB1 运行时短写 tracked transaction 收敛

- 继续审计 `Db` 运行时入口，发现作品/标签 upsert、任务创建/合并/claim/update、扫描锁
  acquire/heartbeat/release、封面与作品元数据更新、审计写入虽然已经持有单写入闸门，
  但仍直接把 SQL 发给 pool，无法进入显式 tracked acquire 样本，任务合并的读-改-写也
  没有统一事务边界。
- 上述入口现在统一使用 `Db::begin_tracked_transaction`；retry 型任务每次重试都创建
  新的 SQLite snapshot，避免复用可能过期的外部状态；审计的定期保留清理在同一事务
  内执行。迁移初始化、维护 PRAGMA 和测试 fixture 保留原有边界，避免嵌套 writer gate。
- `scripts/validate-project.mjs` 新增函数级静态契约，阻止这些运行时短写重新出现
  `.execute(&self.pool)` 或 `.fetch_*(&self.pool)`。

本批验证：后端 `311 passed / 0 failed / 3 ignored`、Clippy `-D warnings`、fmt、前端
production build、项目 validator 和 `33 passed / 0 failed` perf 测试均通过。该批只扩大
写路径可观测性、任务合并原子性和长期维护边界，不打开实验性开关、不切 ownership，也
不构成真实 N100/4GiB/NAS Gate 通过证据。

## 2026-08-20：Catalog secondary 读路径快照与 Facet revision race 收敛

- `random`、`counts`、`collections`、`history` 以及 `/works/{id}/assets` 现在统一在
  显式 `TrackedReadTransaction` 内读取页面数据、统计、`backfill_pending`、revision 和
  资产计数，避免多个 SQLite pool checkout 之间拼接出跨快照响应。
- works、collections、history 游标携带内容/活动 revision；assets 游标携带
  `source_version` 与 `catalog_revision`。游标对应的 revision 已变化时受控返回 `400`，
  不再允许跨快照继续翻页。
- Facet 动态、单 selected-tag、bitmap 与预聚合路径增加 cursor revision 校验；缓存
  结果 revision 与 key 不一致时丢弃，最多重试两次，持续 revision churn 时返回
  `503` 并附带 `Retry-After: 1`。selected-tag 的 tag ID 解析也移入同一 read snapshot。
- 新增回归覆盖 `catalog_secondary_pages_use_tracked_snapshots_and_expire_cursors` 与
  `facet_cache_discards_a_revision_race_before_retrying`；`scripts/validate-project.mjs`
  增加五条 secondary Catalog 路径的 tracked snapshot 静态契约。
- 本批最近验证：Rust `315 passed / 0 failed / 3 ignored`、Clippy `-D warnings`、
  前端 `npm run build`、项目 validator 与 perf `33 passed / 0 failed`。资产 cursor
  修改后仍需再执行一次显式 `cargo fmt --all -- --check` 留证。
- 这批改动仍是迁移候选，不代表默认 ownership 已切换，也不替代 Linux/N100/4GB/NAS
  实机 Gate。下一阶段继续审计 Facet 外部候选/selected-tag resolver、legacy 读事务、
  详情与音轨分页的有界性，并补充真实旧库和目标硬件证据。

## 2026-08-20：legacy 图库页与 bitmap selected-tag 读边界继续收敛

- 兼容路径 `/works/{id}/gallery` 不再分别 checkout 作品类型、图库总数和当前页；新的
  `Db::gallery_assets_page` 在一个 tracked read snapshot 内完成三者读取，并保留旧的
  数字 offset cursor 兼容行为。新生成的 keyset cursor 增加 `source_version` 与
  `catalog_revision`，作品或目录变化后受控返回 `400`。
- `gallery_assets`、`gallery_assets_after` 与维护计数保留独立 helper，但均通过显式
  tracked transaction 执行；图库页按 `limit + 1` 有界读取，不把 70 万图片物化到响应。
- Facet bitmap 分支的 selected-tag ID 解析改为短 tracked snapshot + expected revision
  fence。该 snapshot 在 bitmap refresh/resource admission 之前提交，避免长事务持有 WAL；
  revision 不一致时直接走动态事实表路径，由外层 Facet cache 再执行 revision race 重试。
- 增加图库页面快照/计数/keyset 回归；本批定向 bitmap、Facet race 测试通过，`cargo
  check -p media-shelf-server` 与格式化检查通过；全量 Rust 基线仍为 `315 passed / 0
  failed / 3 ignored`，Clippy 和项目 validator 已通过。
- 仍未完成：legacy `/library`、旧 history/work-history、默认 legacy detail 的所有读
  路径收敛；真实 N100/NAS Gate、ownership promotion、冷媒体和混合负载证据继续缺失。

## 2026-08-20：legacy Library 分页与上下文快照收敛

- `Db::library()` 不再把 `i64::MAX` 传给查询；旧 helper 现在最多返回 500 条，并通过
  `next_cursor` 继续浏览，避免旧调用方意外把 70 万作品物化到 4GB 进程。
- `/library` 的作品页、tags、jobs、history 现在共享一个 tracked read snapshot；分页
  查询仍保持旧的复合 `(updated_at, id)` keyset 和旧 id-only cursor 兼容语义。基于实际
  扫描并发测试保留了“页间变化不重复/不丢失”的旧行为，因此没有对 legacy cursor 强制
  全局 revision 失效；Catalog v2 的 revision-bound cursor 规则保持不变。
- `history`、`work_history`、`tags`、`jobs` 的独立兼容方法也改为短 tracked snapshot，
  但每次只保留有界结果（history/jobs 最多 500，tags 500）。新增 501 作品测试证明
  `Db::library()` 返回 500 条并提供 continuation cursor。

## 2026-08-20：legacy/summary Work Detail 统一快照边界

- `work_detail_with_mode` 的 legacy 与 summary 分支现在都通过同一个
  `work_detail_with_snapshot` 读取作品、维护计数、资产、标签和 external IDs。legacy
  仍保留原有完整资产返回兼容语义；summary、音频和漫画/COS 分支继续使用各自有界上限。
- 这次只改变事务边界，不改变旧响应字段或资产排序；新增回归确认 legacy 64 章详情也只
  产生一个 tracked read snapshot，summary 仍为 16 条资产上限。

本批验证补充：全量 Rust 回归 `316 passed / 0 failed / 3 ignored`（期间新增图库/legacy
library/detail 测试后最新一次为 `316`）、Clippy `-D warnings`、fmt、前端 production
build 和项目 validator 均通过。以上仍是 Windows 开发机证据，不代表 N100/NAS Gate。

## 2026-08-20：v23 固定 corpus、迁移与 Search shadow Gate 重新留证

- 通过当前真实 append-only migration 初始化并生成新的不可覆盖 v23 固定 corpus：
  `r1g-40k-740k-800k-v5`，包含 40,000 works、740,000 assets、2,048 tags、800,000
  work-tag links、740,000 scanner assets，逻辑媒体大小 `29,101,000,000,000` bytes；
  `integrity_check=ok`，数据库文件约 374.6MiB。生成耗时约 160.5s（Windows 开发机，含
  有界批次写入与校验），manifest 位于
  `perf-results/r1g-dev-20260820-v23/dataset-manifest.json`。
- 使用当前 schema v23 release binary 对该数据库执行隔离迁移/恢复 Gate：源库 SHA-256
  前后一致、迁移副本和恢复副本均 `schema_version=23/integrity_check=ok`，恢复校验通过；
  最新 release 重复 Gate 启动就绪约 `48.5s`，artifact 位于
  `perf-results/migration-dev-r1g-v23-20260820-r3/`。首次自动选择旧 v22 binary 时被正确
  拒绝（`schema version 23 is newer than this application supports (22)`），该失败 artifact
  也保留作为版本闸门证据。
- 在隔离副本中仅为 Search Gate 前置条件设置五类 ownership 为 `catalog-v2`，启动
  `nas-n100-4g` profile、32MiB Search writer、shadow outbox/canary 后，v23 fixed corpus
  对账通过：SQLite/Tantivy 均 40,000 documents，ID SHA-256 一致，missing/unexpected/
  duplicate/invalid 全为 0；固定 6 条查询 6/6 成功，canary ID/order mismatch、failure、
  rejection 均为 0。查询总耗时 P50 `15.665ms`、P95/P99 `64.857ms`；fact reconciliation
  单次约 `112–122ms`。证据位于
  `perf-results/search-shadow-dev-r1g-v23-20260820/evidence/`。
- 同一 v23 副本的 Catalog R1G warm triplet 也已完成：works/counts/facets 各 30 次、
  并发 3、90/90 成功、0 个 503；P95 total 分别为 `43.544ms`、`48.414ms`、
  `22.891ms`，P99 分别为 `43.592ms`、`48.980ms`、`23.140ms`，开发机 profile Gate
  判定通过。首个 prime triplet 的 TTFB P95 `194.012ms`，其中 40,000 candidate
  单飞只产生 1 miss + 2 coalesced，后续 90 次为 cache hit；证据位于
  `perf-results/r1g-dev-v23-20260820/`。
- 本批还补齐 legacy `production-v2` 状态行缺失保护、building/degraded fail-closed、
  损坏 Tantivy `meta.json` quarantine 恢复，以及 Search outbox tracked transaction；
  状态写入必须恰好命中一行，损坏索引不会被静默当作可用。

本批仍是 Windows 开发机与逻辑固定 corpus 证据，不能替代 Linux/N100 实机、4GiB cgroup、
NAS HDD await、RSS/PSS、CPU 降频、媒体预览/翻页/起播、标签筛选、双客户端混合负载和 24 小时
soak。默认实验性开关与 ownership promotion 继续关闭；下一步优先执行真实旧库/N100 Gate，
并在该 Gate 中保留 schema v23、revision fence、WAL/busy 和长读快照证据。

## 2026-08-20：legacy `/library` 页级标签聚合与维护计数复用

- 复核发现旧 `/library` 兼容查询的每个作品行都会执行一次 `GROUP_CONCAT` 标签子查询、
  一次 `work_tags COUNT` 和一次 `assets COUNT`。在 500 行页上，这会重复访问相同的关联表，
  即使 `work_stats` 已经有有效维护计数也没有被使用。
- `Db::library_page` 现在先通过 `work_stats` 复用有效的 `tag_count/asset_count`，只有
  `computed_at IS NULL` 的 pending 行才回退到事实表精确计数；当前页的兼容 `tag_keys` 改为
  一次 `json_each` 选中页内作品、按 namespace/key 排序并 `GROUP BY work_id` 的批量聚合。
  空标签、排序、旧响应字段、复合 keyset 游标和旧 id-only 游标语义保持不变。
- 新增回归覆盖：已回填作品使用维护计数；pending 作品使用精确回退；标签键仍按旧格式和
  稳定顺序返回。`scripts/validate-project.mjs` 增加了页级聚合和维护计数的静态契约。
- 在同一 v23 固定 corpus（40,000 works、740,000 assets、800,000 work-tag links，SQLite
  主库约 392.8MB）上进行只读 SQL 形状对比，500 行页含 501 个 fetch 行：旧相关聚合 P50/P95
  约 `15.742/16.536ms`，当前“列表 + 一次分组标签聚合”P50/P95 约 `13.501/14.241ms`，
  页级 SQL 形状的 P95 约下降 `13.9%`。这是 Windows 开发机的 SQLite 形状证据，不是
  N100/NAS 端到端结论；R1G 隔离 A/B 也以 30 个 triplet、90/90 成功、0 个 503 通过，
  但该 runner 主要覆盖 Catalog v2，因此不把其延迟变化归因于 legacy 页改造。
- 本批最新验证要求：Rust 全量回归、Clippy `-D warnings`、fmt、项目 validator 和
  `git diff --check` 均需重新通过。真实 N100/NAS 的 HDD seek、RSS/PSS、WAL/busy 和浏览器
  fallback 仍未验收，Catalog v2 仍是目标规模的主要读路径，legacy `/library` 只保留兼容用途。

## 2026-08-21：健康诊断同快照与当前 release Docker approximation Gate

- `/api/health/resources` 的 schema、Derivative、Facet、Search outbox/shadow/reconciliation
  和 archive manifest 读取已合并到一个短 `TrackedReadTransaction`；新增的 `*_in` helper
  复用同一 `SqliteConnection`，cgroup/内存观测与可选 WAL checkpoint 保持在事务外。
- 使用当前 release、独立 `r6-data`/生成/封面缓存目录和 4GiB/4CPU/256 PID Docker 边界
  重跑统一 Gate：媒体矩阵 `240/240`、双客户端混合负载 `480/480`、系统采样 11 条，
  各子 Gate 通过；因宿主 CPU 为 AMD Ryzen 9 9950X，根状态正确保持为 `approximation`。
- 同一 Docker 主机上对旧/当前二进制各执行 200 次顺序 health 请求：P95 从 `7.167ms`
  降至 `4.830ms`（约 `32.6%`），pool checkout 从约 `8.055/request` 降至
  `1.005/request`（约 `87.5%`），当前版本每请求产生一个 tracked snapshot，busy 和
  acquire timeout 均为 0。证据位于 `perf-results/health-ab-20260821-old/` 与
  `perf-results/health-ab-20260821-new/`。
- 验证：Rust `331 passed / 0 failed / 3 ignored`、Node perf `43 passed / 0 failed`、
  Clippy、fmt、项目 validator、`git diff --check` 均通过。该批仍不替代真实 N100/NAS
  Gate；下一阶段优先执行真实数据库/媒体清单、legacy 余下多查询读路径和 N100/NAS
  cold preview/filter、WAL/HDD await、RSS/PSS 与稳定窗口。

## 2026-08-21：qmediasync 范围冻结，延期重新规划

- 根据当前范围调整，qmediasync 不再纳入本阶段的新增改动、单独测试、性能 Gate、coordinator
  实施或 ownership promotion。后续工作的媒体规模、冷预览、标签筛选和混合负载结论只覆盖
  本地 Comic、CoserPicture、Gallery、Audio 与 Novel 路径。
- 仓库在本计划开始前已经存在 qmediasync 的 STRM 读取、远端流式访问和云缓存兼容链路；这些
  基础代码暂不做整文件回滚，也不改变其现有默认关闭/失败保护语义，避免误伤共享的 VFS、
  资产路由和资源治理代码。qmediasync 相关新能力不作为本阶段完成条件。
- 现有通用回归套件可能仍覆盖基础兼容契约，但不再单独执行 qmediasync 规模测试或把其结果
  写入 N100 近似 Gate。qmediasync 将作为后续独立规划项，届时重新定义 provider、缓存、
  远端故障和验收矩阵。

## 2026-08-21：音声资产分页的 role/MIME 兼容分支收敛

- 复核 `GET /works/{id}/assets?role=track` 后确认，旧数据可能没有规范化
  `role='track'`，但 `mime` 仍是音频类型；原来的 `role = 'track' OR mime LIKE
  'audio/%'` 会让 SQLite 在大作品上倾向扫描整个 `work_id` 集合再排序。现在保留原有
  语义，把可播放页拆成两个 `UNION ALL` 分支：规范 track 分支与非-track 的
  `lower(mime) LIKE 'audio/%'` 兼容分支；游标条件在两个分支内分别应用，最终仍按
  `(role, position, id)` keyset 顺序合并，避免重复或跳过。
- `Db::migrate` 增加两个有界 partial index：`idx_assets_work_audio_role` 和
  `idx_assets_work_audio_mime_lower`。后者只覆盖非-track 音频 MIME，不给图库/漫画/小说
  的普通资产增加同等规模的全局 MIME 索引；`work_stats.track_count` 的维护计数与 pending
  精确回退保持不变。
- 新增 EXPLAIN QUERY PLAN 回归：512 条兼容音频 + 512 条非音频样本执行 `ANALYZE` 后，
  role 与 MIME 两个分支分别命中对应 partial index；Catalog 定向测试为 `24 passed / 0
  failed`，Node perf 为 `43 passed / 0 failed`，项目 validator、fmt、diff check 与前端
  production build 已通过。Rust 全量回归与 Clippy 将在本批最终验证中再次执行。
- 本批不涉及 qmediasync；qmediasync 新增改动、独立测试、性能 Gate 和 ownership promotion
  继续延期，既有兼容代码不回滚。

## 2026-08-21：本地 Catalog 资产页 NULL-last keyset 索引

- 复核 `GET /works/{id}/assets` 后确认，原有 `ORDER BY role, COALESCE(position,
  9223372036854775807), id` 在大 EPUB、音声和图库作品上不能完整复用普通
  `(work_id, role, position, id)` 索引，可能先扫描候选再建立临时排序。当前为本地五类媒体
  增加 `idx_assets_work_role_position_keyset` 表达式索引，并把游标谓词改为同序行值比较，
  让 continuation 在 `(work_id, role, NULL-last position, id)` 上直接 seek。
- 音声保留旧数据的 MIME 兼容语义：规范 `role='track'` 和非 track 音频 MIME 仍拆成
  两个独立分支，各自命中 partial keyset index；每个分支先限制为 `limit + 1`，再在 Rust
  内存中合并短向量，避免 SQLite `UNION ALL` 外层对整部 1 万轨作品建立临时排序。`-1`
  游标哨兵和 role/position/id 排序语义不变。新增混合 track/MIME 跨页回归、sentinel
  position 分页回归以及两个音频分支的 EXPLAIN 回归。
- 该项只覆盖本地 Catalog 资产分页，不代表 NAS HDD 冷读、压缩包页图解码或音声起播 Gate
  已通过；qmediasync 仍按范围冻结章节延期，不参与实现、测试或验收。

## 2026-08-21：本地资产页 N100 Docker 近似复测

- 使用 `arislist:n100-sim`、4 CPU、4GiB memory、256 PID、`RESOURCE_PROFILE=nas-n100-4g`
  启动隔离容器；固定本地 corpus 为 40,000 works、740,000 assets，SQLite 文件约
  `412,454,912` bytes，schema v24，容器启动迁移和 `/api/health` 均返回 200。该 corpus
  不挂载 qmediasync STRM 目录。
- 30 次串行 warm 请求结果：Catalog 首页 P50/P95 `3.05/7.15ms`，单标签筛选
  `18.58/63.18ms`，图库 350 项资产页 `3.23/7.94ms`；均为 `30/30` HTTP 200。跨页
  continuation 取图库前两页各 100 项，重叠为 0，total `350`。
- 另建 10,000 轨单作品 fixture（8,000 `role=track` + 2,000 MIME 兼容 legacy），100 次
  音声资产页请求均成功，P50/P95 `2.82/3.69ms`；前三页 `100/100/100` 条且唯一 ID
  `300`。两个分支的 EXPLAIN 均命中 `idx_assets_work_audio_*_keyset`，没有旧 UNION 外层
  的 `USE TEMP B-TREE FOR ORDER BY`。
- 这些数字只反映本机 Docker/快盘近似，不能推导 NAS HDD 冷读、压缩包解码、真实 RSS/PSS
  或稳定窗口通过；qmediasync 没有请求、没有专项测试，也不纳入本批结论。

## 2026-08-21：漫画阅读器稀疏页模型（本地媒体）

- 复核大漫画阅读器后发现，前端在收到总页数后会为所有页创建占位对象；100,000 页会
  额外保留约 100,000 个 JS 对象/字符串，并在滚动模式为所有页构造完整 offset 数组。
  这部分元数据不会参与当前虚拟窗口的图片请求，却会增加 4GiB/N100 浏览器端的分配和
  每次尺寸/缩放变化时的 O(N) 重算。
- 现在将 `pages` 降为首批有界 manifest 样本（默认 200 条），另以独立的
  `comicPageCount` 保存总页数；横向和纵向虚拟窗口只根据页计数生成索引，不再创建全量
  placeholder。纵向滚动使用首批页面的中位宽高比和固定估算页高，避免为 10 万页分配
  offset 数组；页图仍由服务端按索引按需读取，分页/阅读进度/恢复位置语义保持不变。
- 该策略把前端 manifest 元数据从 O(total pages) 降为 O(首批 manifest)，在 100,000 页
  上从 100,000 个占位项 + 100,001 个 offset 元素降为约 200 个 manifest 项和少量标量状态。
  这是浏览器内存与布局重算的静态/结构性改善，不等同于 NAS HDD 图片解码或翻页 P95
  已通过；真实浏览器与 N100 近似媒体 Gate 仍需后续执行。
- `frontend/npm run build` 通过；Rust `337 passed / 0 failed / 3 ignored`、Clippy、
  Node perf `43 passed / 0 failed`、项目 validator、fmt 和 diff check 也已通过。本批
  不涉及 qmediasync，既有兼容代码保持冻结。

## 2026-08-21：漫画大 manifest 的无参数请求保护

- 服务端 `GET /works/{id}/pages` 继续兼容小漫画的旧无参数完整响应；当 manifest 超过
  500 页时，即使调用方没有提供 query，也自动返回默认 200 页和 `next_cursor`，避免
  误请求把整张大页表序列化到响应和客户端。显式 `cursor/limit` 仍使用原有上限与顺序。
- 新增超过 500 页的无参数回归，验证 `total` 精确、首页有界且 continuation cursor 正确。
  这与前端稀疏页模型配合，使大漫画预览的首次 manifest 传输和浏览器元数据均保持有界。
- 本批不改变压缩包扫描、页图按索引读取或小规模旧客户端行为；不代表 CBZ 解压、缩略图
  生成或 NAS HDD 翻页 P95 已通过。qmediasync 继续按冻结范围延期。

## 2026-08-21：EPUB fallback 章节目录虚拟化

- 轻小说主阅读器仍优先使用 Foliate；兼容 fallback 路径的章节 manifest 上限为 10,000，
  但原先会把每个章节都渲染为 DOM button。现在目录改为固定行高的窗口化列表，只保留
  可视区加 8 行 overscan，10,000 章不再产生 10,000 个同时挂载的按钮。
- 章节索引、封面行、选中状态、点击跳转和滚动容器语义保持；服务端章节上限、标题探测
  总预算和章节 HTML 按索引读取不变。新增静态契约防止 fallback 恢复为全量 `.map()` 渲染。
- 该改动主要降低极端大章节 EPUB 的浏览器 DOM/RSS 峰值，不代表 10,000 本小说整体扫描、
  EPUB 解压或 NAS HDD 冷启动 Gate 已通过；qmediasync 继续延期。

## 2026-08-21：本地 Comic/CoserPicture 阅读页 r18 近似复测

- 在 `arislist:n100-r16-reader` 镜像、4GiB memory、4 CPU quota 和 256 PID 边界下，先预热
  Comic/CoserPicture manifest（耗时约 `8.6/5.7ms`），随后各执行 30 次阅读页原图请求；两组
  均为 `30/30` HTTP 200，未产生 qmediasync 请求。
- Comic 总耗时 P50/P95/P99 为 `7.86/19.58/1175.62ms`，最大值 `1645.83ms`。长尾集中在
  首次冷请求，后续请求主要落在约 `5–20ms`；因此当前“原图先返回、派生图后台生成”策略已
  消除派生图对大多数重复请求的阻塞，但首个冷页仍需在真实 NAS HDD 上复核。
- CoserPicture 总耗时 P50/P95/P99 为 `10.80/351.89/511.78ms`，最大值 `574.37ms`；请求
  原图约 `11.8MB`，P95 主要反映受限 CPU/网络吞吐下的完整 body 传输，而非 SQLite 查询。
- r18 结束快照显示 cgroup RSS 约 `22.0MiB`、SQLite busy/acquire timeout/resource wait
  timeout 均为 `0`，`archive_stream`、`thumbnail_decode`、`processing_memory` 均已归零。
  该结果仍是本机 Docker approximation，不代表 N100/NAS HDD 验收；qmediasync 按 30.53 节
  保持 Deferred，不参与本批代码、测试或性能 Gate。

### 30.60.5 搜索查询边界变更后的正式 1 CPU 五分钟 Gate（2026-08-22）

为覆盖本轮 `MAX_SEARCH_QUERY_BYTES=512` 变更，使用新镜像
`arislist:n100-sim-query-cap` 在隔离容器 `n100-search-query-cap-app` 中重新执行唯一的
正式稳定窗口。实际 Docker 约束为 `--cpus 1.0`、`--memory 4g`、`--memory-swap 4g`、
`--pids-limit 256`、`RESOURCE_PROFILE=nas-n100-4g`。Docker 的 `memory-swap=4g` 表示
内存与 swap 总上限为 4GiB，本次 `memory.swap.max=0`，没有额外 4GiB swap，属于更严格的
内存边界。宿主 CPU 为 AMD Ryzen 9 9950X，N100 型号预检因此唯一失败，但
`--mode approximation` 按当前正式口径允许该项，Linux、4GiB cgroup、1 CPU quota、健康
状态和 profile 均通过。

目标库为 40,000 works、740,000 assets、800,000 work_tags、2,048 tags，SQLite
`412,459,008` bytes，逻辑资产约 `29.101 TB`；按 kind 为 audio 10,000、comic 10,000、
coser-picture 8,000、gallery 2,000、novel 10,000，图库每作品最多 351 项。该模拟覆盖
数据库规模和代表性冷/热请求，不等同于真实 600/500 作者目录的 HDD 遍历或所有物理 TB
文件。

正式 artifact 为 `perf-results/docker-n100-scale-1cpu/search-query-cap-5m-formal/`，
根状态、媒体矩阵和混合矩阵均为 `passed`。双客户端连续 `300s` 完成 `82` 轮、`39,360`
请求，八个场景均 `4,920/4,920` 成功，失败和 HTTP 503 均为 0。混合负载 total P50/P95/P99
（ms）为：Catalog `5.37/76.05/80.10`；标签筛选 `87.27/102.92/174.64`；图库缩略图
冷/热 `77.50/90.73/95.69` / `75.00/90.54/94.73`；漫画首冷页 `55.88/282.32/295.07`；
CoserPicture 首冷页 `62.33/289.06/302.81`；音声 Range 起播 `18.04/88.55/94.26`；
轻小说摘要 `5.40/77.75/82.92`。

系统采样 292 个样本，`memory.current` 峰值 `215,162,880` bytes（约 `205.2MiB`，4GiB
上限的 `5.01%`）；`memory.events` 的 `oom`、`oom_kill`、`max` 均为 0。SQLite busy、
连接池 acquire timeout/error、资源等待 timeout、writer queue depth/bytes、作业失败均为
0；WAL 为 `82,432` bytes。资源等待累计约 `357.9s`、最大单次约 `297.7ms`；cgroup
`cpu.stat` 记录 `1,935/3,364` 个周期被 throttle、累计 throttle 约 `466.5s`，说明单 CPU
下存在明显排队，但仍未突破当前延迟和错误 Gate。

结论：目标规模合成库在当前 `1 CPU/4GiB/256 PID/双客户端/300s` 正式模拟口径下通过，
现有分页、标签筛选、图库/漫画/CoserPicture 预览、音声 Range 起播和轻小说摘要满足当前
Gate。该窗口没有启用 Derivative Cache v2、Facet bitmap、搜索 shadow/incremental reader
或 qmediasync；`/api/search` 超长查询边界由单元/静态契约覆盖，不在这次媒体混合矩阵中。
稳定窗口不再追加 30 分钟、24 小时或真实 NAS/N100 验收；qmediasync 继续 Deferred。

## 2026-08-22：增量 Search 故障 rollback 状态收敛

- `record_shadow_error` 以及 `record_shadow_reconciliation` 的 `failed` 分支现在与
  `search_index_state.status='degraded'` 共用同一 tracked writer transaction，原子更新
  `search_reconciliation_state` 为 `failed`、清零连续通过次数并清除 `cutover_armed`。这样
  损坏 reader 或事实漂移时，健康状态、持久化 cutover 状态和 fail-closed 路由不会出现
  `degraded + armed` 不一致，也不会在进程重启后误把旧 arm 当成可用 reader。
- 回归覆盖“已通过且 armed -> 注入损坏 -> rollback”的状态转换；恢复仍通过独立的
  `rebuild-shadow-search-index` job 和新一轮事实对账，不复用 legacy `production-v2` 重建。
- 本批验证：Rust `343 passed / 0 failed / 3 ignored`、Clippy、前端 production build、Node
  perf `46 passed / 0 failed`、项目 validator、fmt、diff check 全部通过。稳定窗口口径保持
  为 Docker `1 CPU / 4GiB / 256 PID / 双客户端 / 300 秒（5 分钟）`，不追加更长 soak 或真实
  NAS/N100；qmediasync 继续延期。

### 30.60.7 增量 Search reader prewarm 隔离五分钟复核（2026-08-22）

- 使用 `arislist:n100-sim-prewarm`、容器 `n100incremental-prewarm-r2`，边界固定为
  `1 CPU / 4GiB / 256 PID`、`RESOURCE_PROFILE=nas-n100-4g`、双客户端。夹具为
  40,000 works、740,000 assets、800,000 work_tags、2,048 tags 的目标规模副本；本轮
  仅验证显式 Search shadow/incremental/prewarm 开关，不改变默认关闭状态。
- 启动首个请求前 `reader_opens=1`、`readers=1`、`cutover_armed=true`，shadow index
  为 40,000 文档且 reconciliation `passed`；candidate cache 仍为 0 条。`/api/search`
  返回 `reader=production`，说明预热 reader 可复用且没有填充候选缓存。
- 首次 `r2` 运行的失败原因为合成数据库中的 `/perf/media/...` 路径未挂载，五类媒体
  请求返回 `500 No such file`；Catalog、标签和轻小说请求正常。该 artifact
  `perf-results/docker-n100-scale-1cpu/unified-scale-incremental-prewarm-5m-r2/`
  保留为夹具失败证据，不计入性能结论。修正为既有实际媒体路径后，正式 artifact
  `perf-results/docker-n100-scale-1cpu/unified-scale-incremental-prewarm-5m-r3/` 根、
  媒体矩阵和混合矩阵均 `passed`。
- 300 秒完成 81 轮、`38,880` 请求，8 个场景均 `4,860/4,860` 成功，失败和 HTTP 503
  均为 0。混合负载 total P95/P99（ms）为：Catalog `64.71/76.88`；标签筛选
  `200.03/279.84`；图库缩略图冷/热 `87.33/92.39`、`87.08/91.76`；漫画首冷页
  `269.94/288.43`；CoserPicture 首冷页 `279.53/293.55`；音声 Range 起播
  `84.76/91.44`；轻小说摘要 `74.52/86.25`。
- 系统采样 286 个样本，`memory.current` 峰值 `146,366,464` bytes（约 139.6MiB，
  4GiB 上限的 3.41%）；`memory.events` 的 `oom`、`oom_kill`、`max` 均为 0。SQLite
  busy、连接池 acquire timeout/error、资源等待 timeout、writer queue depth/bytes、
  作业失败均为 0；WAL `737,512` bytes。累计资源等待约 `372.4s`、最大单次约
  `508.2ms`；容器累计 cgroup throttle 为 `2,007/3,478` 个周期、约 `465.0s`，
  说明单 CPU 排队明显但未突破当前 Gate。

该结果证明 prewarm 变体在当前 1 CPU 五分钟混合窗口内不会破坏媒体浏览和资源治理，
但它不等于已批准切换 production 默认 reader；`SEARCH_*_SHADOW`、增量 reader 和
prewarm 仍保持显式关闭，需后续单独决定 promotion。稳定窗口仍只执行
`1 CPU / 4GiB / 256 PID / 双客户端 / 300 秒`，qmediasync 继续 Deferred。

### 30.60.10 封面资产同作品完整性校验（2026-08-22）

`Db::set_work_cover` 现在在同一条 `UPDATE` 中要求封面资产的 `work_id` 与目标作品一致；
跨作品的封面更新会回滚并返回 `404` 语义，不会写入错误的 `cover_asset_id`。图库封面
读取的显式 asset 查询也绑定目标 `work_id`，因此历史脏数据不会把另一作品的图片暴露为
当前作品封面，也不会静默改用归档 fallback。正常路径没有额外 pool checkout；失败路径
才执行一次存在性诊断查询。新增回归覆盖写入拒绝、原封面保持和读取侧 fail-closed。

本项只收紧媒体预览数据完整性，不改变分页、缓存、ownership、实验性开关或 qmediasync
Deferred 状态。全量 Rust `347 passed / 0 failed / 3 ignored`，Clippy、fmt、diff check
通过；正式稳定窗口仍唯一采用 Docker `1 CPU / 4GiB / 256 PID / 双客户端 / 300 秒
（5 分钟）`混合负载，不追加 30 分钟、24 小时或真实 NAS/N100 验收。

### 30.60.11 封面校验后的正式五分钟复测（2026-08-22）

为避免使用封面完整性修复前的旧 artifact，使用当前工作树构建镜像
`arislist:n100-sim-cover-r1`，以 `n100cover-r1` 容器重新执行唯一正式稳定窗口：
`1 CPU / 4GiB / 256 PID / 双客户端 / 300 秒`。目标数据库仍为
`40,000 works / 740,000 assets / 800,000 work_tags`，目标矩阵覆盖 Catalog、标签、
图库冷/热缩略图、漫画、CoserPicture、音声 Range 和轻小说摘要。

正式 artifact：
`perf-results/docker-n100-scale-1cpu/unified-scale-cover-r1-5m/run.json`。根 Gate、媒体
矩阵和混合矩阵均通过；混合窗口完成 `82` 轮、`39,360/39,360` 请求成功，媒体矩阵
`240/240` 成功，失败和 HTTP 503 均为 `0`。混合 total P95/P99（ms）为：Catalog
`75.55/79.48`、标签筛选 `103.03/170.74`、图库冷/热 `91.01/95.66` /
`90.74/94.73`、漫画 `281.08/294.04`、CoserPicture `289.53/301.54`、音声 Range
`88.43/93.24`、轻小说摘要 `77.35/81.62`。

系统采样 `294` 条，`memory.current` 峰值 `135,057,408B`（约 `128.8MiB`），WAL 峰值
`78,312B`；OOM、SQLite busy、连接池 acquire timeout/error、资源 wait timeout、writer
queue、作业失败和 implicit rollback 全为 `0`。资源等待最大约 `298.4ms`；cgroup
`cpu.stat` 为 `1,900` 次 throttle、累计约 `461.1s`。AMD 型号预检失败仍只记录为
approximation provenance，正式模拟 Gate 按当前口径通过；qmediasync、Inventory、Facet
bitmap、Derivative Cache v2 和 Search shadow/incremental/prewarm 仍未打开。

### 30.60.12 客户端详情统一使用有界 summary 与正式五分钟复测（2026-08-22）

- React 客户端打开作品详情时现在始终请求 `asset_mode=summary`，不再因为 Catalog v2
  关闭而回退到可能 materialize 全部资产的 `legacy` 详情形状。服务端 `legacy` API
  仍保留给旧外部客户端；音轨、图库、漫画页和 EPUB 章节继续使用各自已有的 cursor/
  manifest 路由，因此不改变阅读与播放语义。Catalog 状态变化也不再触发同一详情的重复
  请求。项目 validator 增加静态契约，防止客户端回退到无界详情。
- 使用镜像 `arislist:n100-sim-summary-r1`、容器 `n100summary-r1`，边界为 Docker
  `1 CPU / 4GiB / 256 PID`、`RESOURCE_PROFILE=nas-n100-4g`，执行唯一正式双客户端
  `300 秒（5 分钟）`混合窗口。artifact 为
  `perf-results/docker-n100-scale-1cpu/unified-scale-summary-r1-5m/run.json`；目标库仍
  为 `40,000 works / 740,000 assets / 800,000 work_tags`，媒体矩阵 `240/240`，混合
  `39,840/39,840` 成功，失败和 HTTP 503 均为 `0`，根/媒体/混合 Gate 均通过。
- 混合 total P95/P99（ms）：Catalog `75.35/79.15`、标签筛选 `102.28/169.71`、图库
  冷/热 `90.61/94.69` / `90.19/94.26`、漫画页 `281.17/294.27`、CoserPicture 页
  `288.82/305.99`、音声 Range `88.41/94.36`、轻小说摘要 `76.84/82.03`。系统采样
  `298` 条，`memory.current` 峰值 `216,932,352B`（约 `206.9MiB`），WAL 最大
  `61,832B`；SQLite busy、连接池 acquire timeout/error、资源 wait timeout、writer
  queue 和 OOM/OOM-kill 均为 `0`。资源等待最大约 `301.5ms`、累计约 `362.2s`；
  cgroup `cpu.stat` 为 `1,942` 次 throttle、累计约 `465.8s`。AMD CPU 型号预检是唯一
  不通过项，在 `approximation` 模式按正式口径允许；容器停止时的 `137` 是窗口后停止
  超时的 SIGKILL，`OOMKilled=false`，不计为负载失败。
- 本批验证：Rust `347 passed / 0 failed / 3 ignored`、Node perf `46 passed / 0 failed`、
  Clippy、前端 production build、validator、fmt 和 diff check 均通过。稳定窗口仍只
  采用 `1 CPU / 4GiB / 256 PID / 双客户端 / 300 秒`，不追加更长 soak 或真实 NAS/N100
  验收；qmediasync 继续 Deferred。

### 30.60.13 legacy 书架后台分页上限与显式续页（2026-08-22）

- legacy fallback 首次加载后，后台最多继续加载 `5` 页（首屏 `100` 条，后台页最多 `500` 条）。
  达到上限后保留 `next_cursor`，不再把整个 library 拉入浏览器；用户通过显式“加载更多”
  控件逐页续载。AbortController 和 generation fence 保持不变。
- 续页追加使用持久化已见作品 ID 集合，移除每页重建全量 `Set` 以及对完整数组重新排序，
  让后台追加成本随新增页规模增长。Catalog v2 路径、服务端 legacy API 兼容性和 qmediasync
  均未改变。
- 使用镜像 `arislist:n100-sim-legacy-r1`、容器 `n100legacy-r1`，在 Docker
  `1 CPU / 4GiB / 256 PID`、`RESOURCE_PROFILE=nas-n100-4g` 下执行唯一正式双客户端
  `300 秒（5 分钟）`窗口。artifact 为
  `perf-results/docker-n100-scale-1cpu/unified-scale-legacy-r1-5m/run.json`；目标库仍为
  `40,000 works / 740,000 assets / 800,000 work_tags`，媒体矩阵 `240/240`，混合
  `39,360/39,360` 成功，失败和 HTTP 503 均为 `0`，根/媒体/混合 Gate 均通过。
- 混合 total P95/P99（ms）：Catalog `75.66/79.95`、标签筛选 `103.01/173.10`、图库
  冷/热 `91.04/95.98` / `90.81/95.27`、漫画页 `284.03/297.78`、CoserPicture 页
  `289.14/304.67`、音声 Range `88.70/95.29`、轻小说摘要 `77.17/81.68`。媒体矩阵
  单客户端 P95/P99（ms）：图库冷/热 `13.18/32.22` / `14.00/31.93`、漫画 `8.82/91.54`、
  CoserPicture `60.18/108.96`、音声 `16.59/32.50`、轻小说 `9.15/29.52`。
- 窗口期间 `39,360` 条请求全部成功；系统采样未见 OOM、SQLite busy、连接池超时、资源
  wait timeout 或 writer queue 堵塞。cgroup 内存峰值约 `131,694,592B`（约 `125.6MiB`），
  远低于 `4GiB`；WAL 峰值约 `74,192B`。CPU 型号为开发机 AMD，preflight 的 N100 型号
  检查失败按 `approximation` 口径记录，不影响正式通过；窗口后的容器停止超时产生退出码
  `137`，`OOMKilled=false`，不计为负载失败。
- 本批验证：Rust `347 passed / 0 failed / 3 ignored`、Node perf `46 passed / 0 failed`、
  Clippy、前端 production build、validator、fmt 和 diff check 均通过。

### 30.60.14 Catalog 标签 facet 显式续页与前端驻留上限（2026-08-22）

- `useCatalogContext` 现在保留 facet `next_cursor`，侧栏通过显式“加载更多标签”继续请求
  `/catalog/facets/tags`；kind、作品查询、已选标签或标签关键字变化会取消旧续页并重置代际，
  旧响应不能覆盖新筛选上下文。
- 续页请求单飞、按 `namespace:key` 去重，驻留标签最多 `4096` 条；达到上限后停止续页并
  显示受控提示。标签面板不会自动遍历全部标签，常规 2048 标签库仍可通过按钮完整分页，
  超大标签表保持浏览器内存有界。
- 本批只变更前端 facet 状态和 UI，不改变服务端 SQL、媒体路由、资源 governor、默认实验
  开关或 qmediasync Deferred 状态；因此不新增正式 5 分钟负载 artifact，既有
  `1 CPU / 4GiB / 256 PID / 双客户端 / 300 秒` Gate 继续作为服务端基线。
- 验证：`frontend/npm run build`、`node scripts/validate-project.mjs`、`git diff --check` 通过。

### 30.60.15 Catalog ownership 诊断同快照收敛（2026-08-22）

- `/catalog/ownership` 的 root readiness、pending/failed event 和 ownership 字段现在通过
  一个短 `TrackedReadTransaction` 读取，避免切换边界返回混合 generation；事务不跨文件 I/O，
  不改变 promotion/rollback 行为。
- validator 增加 tracked snapshot/commit 静态契约。本批只改变诊断读取边界，不改变媒体、
  搜索、资源 governor 或 qmediasync，故继续沿用既有正式 Docker 五分钟 Gate。
- 验证：Rust `347 passed / 0 failed / 3 ignored`、Clippy、`node scripts/validate-project.mjs`、
  `cargo fmt --check`、`git diff --check`。

### 30.60.16 Derivative Cache v2 受限 Docker A/B（2026-08-22）

- 在同一目标规模数据库上分别执行关闭组和开启组的唯一正式稳定窗口。两组均使用当前镜像、
  `1 CPU / 4GiB / 256 PID / 双客户端 / 300 秒（5 分钟）`；关闭组 artifact 为
  `perf-results/docker-n100-scale-1cpu/unified-scale-derivative-v2-off-r1-5m/run.json`，
  开启组 artifact 为 `perf-results/docker-n100-scale-1cpu/unified-scale-derivative-v2-on-r1-5m/run.json`。
  目标库保持 `40,000 works / 740,000 assets / 800,000 work_tags / 2,048 tags`，媒体矩阵
  两组均 `240/240` 成功。
- 关闭组完成 `39,360/39,360` 混合请求（82 轮），开启组完成 `69,600/69,600`
  混合请求（145 轮）；两组失败、HTTP 503、OOM、SQLite busy、连接池 acquire
  timeout/error、资源 wait timeout、writer queue 堵塞和作业失败均为 `0`，根/媒体/混合
  Gate 均为 `passed`。开启组最终健康计数为 `cache_hits=17,459`、`cache_misses=1`、
  `generated_files=3`、`generation_failures=0`、`resident_bytes=497,474`。
- 混合 total P95/P99（ms）对比：Catalog `75.64/80.04 -> 73.29/77.86`；标签筛选
  `102.01/166.43 -> 99.44/108.12`；图库冷/热 `91.02/95.28 -> 92.11/97.69` /
  `90.73/94.87 -> 91.72/96.64`；漫画页 `281.71/295.19 -> 88.26/93.70`；
  CoserPicture 页 `289.14/303.80 -> 87.04/92.23`；音声 Range `88.57/94.13 ->
  86.69/92.60`；轻小说摘要 `77.63/82.51 -> 75.00/80.73`。漫画和 CoserPicture 冷页
  P95 分别下降约 `68.7%` 和 `69.9%`，图库混合 P95 变化约 `+1ms`。
- 系统采样的 `memory.current` 峰值为关闭组 `215,625,728B`（约 `205.64MiB`）、开启组
  `228,098,048B`（约 `217.53MiB`）；开启组增加约 `11.89MiB`，仍仅为 4GiB 上限的
  `5.31%`。WAL 峰值为 `61,832B -> 222,512B`，两组资源治理错误计数仍为零。
- 结论：Derivative Cache v2 在当前正式 Docker 模拟口径下通过稳定性和交互 Gate，且对
  漫画/CoserPicture 代表性预览有显著收益；但本轮仍只覆盖合成库和少量合法样本，不证明
  50MP JPEG、ZIP/CBZ 长尾、损坏归档或 NAS HDD 冷盘行为。故 `DERIVATIVE_CACHE_V2_ENABLED`
  与 `JPEG_THUMBNAIL_DOWNSCALE_ENABLED` 继续默认关闭，保留为可审计候选；稳定窗口仍只
  执行 1 CPU 五分钟，不追加真实 NAS/N100、30 分钟或更长 soak，qmediasync 继续 Deferred。

### 30.60.17 700k Inventory 固定批次显式基准（2026-08-22）

- 单独运行被普通回归忽略的 `inventory::tests::synthetic_700k_inventory_uses_fixed_batches`
  （binary test target，`--ignored --nocapture`）。合成 `700,000` 行全部写入并完成 root
  fence，结果为 `1 passed / 0 failed`，耗时 `15,097ms`；JSON artifact 为
  `perf-results/inventory-700k-dev-20260822/run-r2.json`。
- 每批固定 `1,024` 行，最大序列化 payload 为 `169,985B`（约 `166KiB`），未出现批次
  膨胀；最终 `file_inventory` present 行数与目标完全一致，`inserted=700,000`。
- 该结果补齐了目标规模 Inventory 的开发机固定批次证据，但不代表真实 NAS/N100 HDD
  遍历吞吐、浏览器或混合负载验收，也不改变 Inventory 默认关闭和逐 kind promotion gate。
  正式稳定窗口仍唯一采用 Docker `1 CPU / 4GiB / 256 PID / 双客户端 / 300 秒（5 分钟）`。

### 30.60.18 Catalog reconciliation generation fence（2026-08-22）

对账基线已经记录每个 Inventory root 的 generation，但 work-key 分页、Audio/Gallery
资产加载和 unexpected legacy 反查原先只按 `root_id/status` 读取，扫描若在对账启动后开始，
中间可能把旧代和新代行混入同一轮检查。现在四类查询均绑定基线
`file_inventory.seen_generation`；最终 root digest/revision fence 仍保留，因此扫描并发时
结果会快速、明确地标记为 stale，而不是继续对新旧混合行做无效归档/媒体解析。

新增回归将现存 Inventory 行标记为旧 generation，确认该行不会进入 expected work，且会被
计入 unexpected；Catalog reconciliation 定向测试 `27 passed / 0 failed`，项目 validator、
fmt 和 diff check 通过。本批不改变 ownership、默认实验开关或媒体 API，也不触及
qmediasync；正式稳定窗口仍唯一采用 Docker `1 CPU / 4GiB / 256 PID / 双客户端 / 300 秒
（5 分钟）`，不重复运行已通过的稳定窗口。

### 30.60.19 Catalog reconciliation page snapshot（2026-08-22）

五类媒体 reconciliation 的每个 work-key 页现在在同一个 bounded tracked read snapshot 中
读取 Legacy Catalog 的作品、scanner assets、tags、external IDs 和 work stats；snapshot
在内存比较前提交，不跨越 inspector、文件或归档 I/O，也不跨越下一页。此前同一页的多组
pool checkout 可能在 1 CPU 下增加连接竞争，并在 revision 变化时混用不同事实代际。

新增回归确认单页只产生一个 tracked snapshot、无 implicit rollback；项目 validator 同步
锁定该契约。该批不改变媒体 API、ownership、默认实验开关或正式稳定窗口，`qmediasync` 仍
Deferred。正式窗口继续只采用 Docker `1 CPU / 4GiB / 256 PID / 双客户端 / 300 秒（5 分钟）`。

### 30.60.20 Catalog reconciliation unexpected-work snapshot（2026-08-22）

`unexpected legacy work` 反查现在把所有 enabled roots 放在一个 bounded tracked read
snapshot 内完成；snapshot 在 SQL 反查结束后立即提交，不跨文件 I/O。多 root 对账不再为每个
root 独立 checkout，也不会在 root 之间混用不同 Catalog/Inventory generation。

新增回归确认单快照、无 implicit rollback，validator 增加静态契约。该批不改变结果语义、
ownership、默认实验开关或正式稳定窗口；`qmediasync` 仍 Deferred。

### 30.60.21 Search 全量重建长快照审计（2026-08-22）

对现有正式 Docker 目标规模 artifact 的 `system-samples.csv` 做了边界审计。
`unified-scale-summary-r1-5m` 启动预热阶段的最长 tracked read snapshot 为 `7,140,288us`
（约 `7.14s`），SQLite writer 最长 hold 为 `5,992,266us`（约 `5.99s`）；Derivative Cache
v2 开启组对应为 `7,295,746us`（约 `7.30s`）和 `5,862,288us`（约 `5.86s`）。这些值出现在
混合窗口开始前的 production Search prewarm，首个交互请求在 listener readiness 之后才放行。
两份 artifact 均未出现 SQLite busy、连接池 acquire timeout/error、活动快照残留或 writer
queue 堵塞，WAL 峰值分别为 `61,832B` 和 `222,512B`。

当前全量重建已经用 bounded channel 限制行驻留，并在构建完成前 fail-closed；把数据库行再写入
临时 spool 再读回会增加一次完整磁盘 I/O、临时文件清理和故障恢复面，而当前正式 5 分钟窗口
没有证据表明这条低频维护路径影响媒体分页、预览或标签筛选。因此本批只完成审计，不改 Search
代码、不新增稳定窗口；“释放 SQLite 快照后再从 bounded spool 构建 Tantivy”保留为后续可选项，
仅在真实重建与在线写入并发造成 WAL/写入等待超预算时再启用评估。正式验收仍唯一采用 Docker
`1 CPU / 4GiB / 256 PID / 双客户端 / 300 秒（5 分钟）`，`qmediasync` 继续 Deferred。

### 30.60.23 Catalog reconciliation runtime-error 短写事务收敛（2026-08-22）

reconciliation 运行时错误状态更新此前虽已申请 writer slot，但仍直接通过 pool 执行单条
`UPDATE`。现在改为显式 `TrackedWriteTransaction`，并在更新后提交；bounded error 文本、
状态语义和 writer slot 保持不变。validator 增加静态契约，避免后续重新引入 pool short write。

本批复用现有 `runtime_error_update_uses_the_single_writer_gate` 回归，未改变媒体 HTTP 路径、
ownership、实验性默认开关或 qmediasync；因此不重复运行正式稳定窗口。正式验收仍唯一采用
Docker `1 CPU / 4GiB / 256 PID / 双客户端 / 300 秒（5 分钟）`。

### 30.60.22 Inventory coordinator 短写 tracked transaction 收敛（2026-08-22）

将 coordinator 的完成、失败、清理、租约释放、root 完成/不完整标记和轻小说状态更新等短写
统一改为显式 `TrackedWriteTransaction`。租约释放时 root 状态与 coordinator 状态在同一事务中提交，
generation/token fence、writer slot 和业务顺序保持不变；短写不再绕过 writer gate 直接调用
连接池执行，降低 1 CPU 下 pool checkout 与 SQLite 写入代际混用风险。

新增 validator 静态契约，Inventory 定向测试 `40 passed / 1 ignored`，全量 Rust 测试
`351 passed / 0 failed / 3 ignored`；Clippy、前端 production build、validator、fmt 和 diff
check 全部通过。该批不改变媒体 HTTP 路径、默认 ownership/实验开关或正式稳定窗口，因此不
重复运行 5 分钟负载；正式验收仍唯一采用 Docker `1 CPU / 4GiB / 256 PID / 双客户端 /
300 秒（5 分钟）`，`qmediasync` 继续 Deferred。

### 30.60.24 Catalog reconciliation 短读 tracked snapshot 收敛（2026-08-22）

reconciliation 的 Inventory work-key 分页、结束时 catalog revision 与 root snapshot 现在都
使用短 `TrackedReadTransaction`，读取完成立即 commit；事务不会跨越 inspector、归档解码、文件
I/O 或下一页。这样在 1 CPU 下每个 bounded read 都进入统一 snapshot 观测，仍保持 keyset 分页
与 generation/root stale fence 语义不变。

validator 增加 work-key 页和短读契约；本批未改变媒体 HTTP 路径、ownership、实验性默认开关或
qmediasync，也不重复运行正式稳定窗口。正式验收继续唯一采用 Docker
`1 CPU / 4GiB / 256 PID / 双客户端 / 300 秒（5 分钟）`。

### 30.60.25 增量 Search reader 快速失败短读收敛（2026-08-22）

`ensure_incremental_reader_gate` 在获取 shadow 锁和 writer slot 前的快速未就绪检查，原先
直接通过连接池读取 `search_index_state`；这条路径虽然只查两列，但会绕过 tracked read
snapshot 统计，并可能与后续 revision-fenced writer transaction 使用不同的读代际。现在由
`incremental_reader_fast_state` 使用短 `TrackedReadTransaction`，读取完成立即提交，再执行
原有 fail-closed 判断；不改变锁顺序、错误文本或增量 reader 默认关闭状态。

新增回归确认 shadow 为 `building` 时仍在锁等待前快速失败，同时产生一个已完成 read snapshot、
无活动快照和 implicit rollback；validator 增加函数体级静态契约。该项只触及显式关闭的
Search incremental gate，不改变媒体 HTTP 路径或正式稳定窗口，qmediasync 继续 Deferred。

### 30.60.26 Catalog ownership promotion 同事务 Search 事实门禁（2026-08-22）

ownership promotion 原先在路由层分别读取 Search shadow、reconciliation 和 outbox 状态，随后
再进入 Catalog ownership writer；在两次 checkout 之间发生 Search revision 或 reconciliation
降级时，预检结果可能与实际写入边界不一致。现在新增 `change_catalog_kind_ownership_checked`，
由控制面在同一个 SQLite writer transaction 内读取三类 Search 事实并执行 promotion gate，
通过后才继续读取 Inventory root facts、当前 reconciliation 和 ownership update；失败会自动回滚，
不会改变 `catalog_kind_ownership`。

旧的低层 `change_catalog_kind_ownership` 保留给隔离 Catalog/Inventory fixture，实际 HTTP
promotion 已固定走 checked 入口；rollback 仍走原有保守的 legacy reconcile 路径。新增回归覆盖
Search 证据缺失时 promotion fail-closed 且 owner 保持 `legacy`，以及三类 Search 状态在一个
tracked read snapshot 中提交一次、无活动快照和 implicit rollback。validator 已锁定 checked
入口与同事务 helper。该批不改变默认 ownership、Search/Inventory 实验开关、媒体 HTTP 或
qmediasync 状态；正式稳定窗口仍唯一采用 Docker `1 CPU / 4GiB / 256 PID / 双客户端 /
300 秒（5 分钟）`，不追加更长 soak。

### 30.60.27 Search degraded 状态清除 cutover arm（2026-08-22）

- `refresh_shadow_progress` 的失败事实分支现在在同一 tracked writer transaction 内清除
  `search_reconciliation_state.cutover_armed`，然后再提交 `search_index_state='degraded'`，
  防止历史 arm 与 degraded 状态组合残留。
- `failed_reconciliation_stays_degraded_across_revision_churn` 回归显式注入
  `status='failed', cutover_armed=1`，验证刷新后 reader 不 ready、状态仍为 `failed/degraded`，
  且 `cutover_armed=false`；validator 增加函数体级静态契约。
- 本批不改变媒体 HTTP、Search/Inventory 默认开关、Catalog ownership、正式 Docker 五分钟
  稳定窗口或 qmediasync Deferred 范围。

### 30.60.28 Docker Inventory kind 模拟开关隔离（2026-08-22）

- `docker-compose.n100-sim.yml` 新增 `SIM_INVENTORY_SCANNER_ENABLED` 与
  `SIM_INVENTORY_SCANNER_KINDS`，允许在独立 1 CPU/4GiB 模拟容器中选择性运行 Inventory
  shadow/coordinator；两项默认仍为 `false`/`all`，生产 Compose 不受影响。
- validator 增加静态契约，确认模拟开关未复用生产环境变量默认值，也不会隐式打开 Inventory。
- 本批仅完善逐 kind 模拟入口，不改变 ownership、Search/Derivative 默认开关或正式五分钟
  稳定窗口；qmediasync 仍 Deferred。

### 30.60.29 Docker 1 CPU Inventory 700k scale evidence（2026-08-22）

- 在 `mcr.microsoft.com/devcontainers/rust:1-bookworm` 容器中以 `1 CPU / 4GiB / 4GiB
  swap / 256 PID` 执行 `synthetic_700k_inventory_uses_fixed_batches`；`700,000/700,000`
  行发现和写入成功，`changed=0`，固定批次 `1,024` 行，最大批次 `169,985B`，耗时
  `15,488ms`。
- artifact：`perf-results/inventory-700k-docker-1cpu-20260822/run.json`，包含 schema、
  dirty-state、容器镜像和资源边界 provenance；明确标记为 scale evidence，不是五分钟媒体
  稳定窗口。
- 首次使用 Rust 1.88 镜像因依赖要求 Rust 1.89 而失败，改用 Rust 1.95 镜像后通过；该
  工具链差异不归因于项目代码。Inventory 默认关闭、ownership 和 qmediasync Deferred
  范围不变。

### 30.60.30 Novel 10k kind scale evidence（2026-08-22）

- 新增 `scripts/perf/run-inventory-kind-simulation.mjs`：正常登录/CSRF 触发单 kind 扫描，
  bounded poll job、Inventory status、health resources；长任务可用
  `--existing-job-id` 只轮询而不重复扫描，artifact fail-closed 且禁止覆盖。
- 独立 Docker `1 CPU / 4GiB / 256 PID` 使用 100 个作者目录、10,000 个有效 EPUB 路径完成
  Novel Inventory 与 legacy scanner。artifact：
  `perf-results/docker-n100-scale-1cpu/novel-10k-inventory-20260822-r3/run.json`。
  Inventory `10,000/10,000`，Catalog Novel `10,000`，0 missing/pending/failed；scan job
  耗时 `1,661,989ms`（约 27 分 42 秒）。
- 最终 RSS 约 `149.6MiB`，数据库约 `47.3MiB`，WAL 约 `16.0MiB`，OOM/restart/SQLite
  busy/pool timeout/resource timeout 均为 0。副本使用 hardlink 复用现有 EPUB 内容，覆盖路径
  和解析规模，不代表 10,000 份独立物理内容的 NAS 吞吐；该结果是 kind scale evidence，
  不扩大正式五分钟窗口，Inventory 默认和 qmediasync Deferred 状态不变。
- 本批验证：项目 validator 全绿，Node perf `46 passed / 0 failed`，runner syntax/help 通过。

### 30.60.31 Comic/CoserPicture Inventory kind scale evidence（2026-08-22）

- Comic 在独立 Docker `1 CPU / 4GiB / 256 PID` 容器完成 `10,000/10,000` 个 CBZ/ZIP
  路径的 Inventory 与 legacy scan；artifact 为
  `perf-results/docker-n100-scale-1cpu/comic-10k-small-inventory-20260822-r2/run.json`。
  root 为 `idle`，`present_files == last_discovered == 10,000`，missing/pending/failed
  均为 `0`；scan job 用时约 `1,211,112ms`（20.2 分钟）。
- CoserPicture 在同一受限边界完成 `8,000/8,000` 个 ZIP 路径；artifact 为
  `perf-results/docker-n100-scale-1cpu/coser-8k-inventory-20260822-r3/run.json`。
  root 为 `idle`，missing/pending/failed 均为 `0`，scan job 用时约 `842,223ms`（14.0 分钟）。
  该 artifact 只按 Inventory root 的 8,000 条目标计数，不把 fixture manifest 或其它孤立
  legacy 记录计入 Inventory 规模。
- 两份 artifact 的 cgroup 都是 `1 CPU / 4GiB / 256 PID`，health 为 `ok`，SQLite busy、
  pool acquire timeout/error、resource wait timeout 和 job failure 均为 `0`。它们属于
  `kind-scale-evidence`，不替代五分钟双客户端稳定窗口；Gallery 目标规模的
  独立 kind artifact仍是可补充项，qmediasync provider 对账和 ownership promotion 继续
  fail-closed，不纳入本轮。

### 30.60.32 Search shadow/incremental reader 实验性落地收敛（2026-08-22）

- 本轮 Search 实验功能已落地并保持生产默认关闭：`shadow-v3` outbox 增量 claim/commit/ack、
  SQLite/Tantivy 文档数与 ID hash 对账、双读 canary、revision/ownership gate、损坏恢复、
  fail-closed、incremental reader arm 与启动 prewarm 均已接入；`SEARCH_OUTBOX_SHADOW_ENABLED`、
  `SEARCH_SHADOW_CANARY_ENABLED`、`SEARCH_INCREMENTAL_READER_ENABLED` 和
  `SEARCH_READER_PREWARM_ENABLED` 只在隔离容器中显式开启。
- Docker 近似固定 corpus reader artifact
  `perf-results/docker-n100-sim/search-shadow-v24-final-rerun/incremental-reader-runtime.json`
  为 `passed`：`40,000` works、6 条固定查询、progress gate 和 production routing 全通过，
  pending/revision lag 为 `0`。正式 `1 CPU / 4GiB / 256 PID / 双客户端 / 300 秒`窗口使用
  `perf-results/docker-n100-scale-1cpu/unified-scale-incremental-prewarm-5m-r3/run.json`，
  media/mixed Gate 通过；AMD 宿主的 N100 型号 preflight 作为 approximation provenance，
  不阻断本项目正式验收。
- 当前 `n100search-r4` 隔离容器的事实对账再次通过：SQLite/Tantivy 均为 `30,015` documents，
  missing/unexpected/duplicate/invalid 均为 `0`，双方 ID SHA-256 为
  `120e5306da946bcefe5a3ee45d1e5397369fbd82b93e790b483dcaf4defd6892`，对账耗时约 `335ms`。
  当前 reader 未 arm 是因为该容器仍保持 legacy ownership；这是默认安全状态，不记为代码失败。

### 30.60.33 本轮实验功能范围最终冻结（2026-08-22）

- 本轮计划与验收只包含 `Inventory Scanner` 和 `Search shadow / incremental reader`。
- `Facet bitmap`、`Derivative Cache v2`、`JPEG downscale` 本轮不实施、不做稳定窗口、不计入
  本轮未完成项；其既有代码/开关保持默认关闭，后续另行立项。
- qmediasync 的改动、专项测试和性能验收继续 Deferred；真实 NAS/N100、30 分钟/24 小时
  soak 也不再是本轮阻塞条件。唯一正式窗口仍为 Docker `1 CPU / 4GiB / 256 PID /
  双客户端 / 300 秒`。

### 30.60.34 Audio 10k Inventory kind scale evidence（2026-08-22）

- 重新校准音声 fixture 为 `500` 个作者目录、每个作者一个 `20` 轨 work，共 `10,000` 个
  音频文件；文件使用 hardlink 复用现有样本，不复制约 `7.7GB` 物理数据。首轮每文件一个
  work 的输入会把 legacy metadata/fingerprint 调度成本夸大，未作为验收证据保留。
- 独立 Docker 容器使用 `1 CPU / 4GiB / 4GiB swap / 256 PID`，Inventory 仅启用 `audio`。
  完整 scan job artifact `perf-results/docker-n100-scale-1cpu/audio-10k-inventory-20260822-grouped/run.json`
  通过：`10,000/10,000` discovered/present、missing/pending/failed 均为 `0`，root 为
  `idle`，job 用时 `126,970ms`（约 `2分07秒`）。最终 cgroup memory 为 `31,821,824B`
  （约 `30.4MiB`），数据库 `13,455,360B`，WAL `16,628,352B`，SQLite busy、pool
  timeout/error、resource timeout 均为 `0`。
- `run-r2.json` 使用 `--existing-job-id 1` 复用已完成 job，不重复扫描，仅补齐
  `arislist:n100-sim-current-r3`、容器名和 `1 CPU / 4GiB / 256 PID` provenance；两份 artifact
  均为 `kind-scale-evidence`。该结果仍只证明 mounted root 与 Inventory coordinator 路径，
  不代表物理 NAS/HDD 延迟，也不扩大正式五分钟双客户端窗口。
- 本批验证：Rust `359 passed / 0 failed / 3 ignored`、Clippy、fmt、Node perf `46 passed`、
  前端 production build、项目 validator 和 `git diff --check` 均通过；Inventory 默认开关、
  ownership promotion 和 qmediasync Deferred 范围不变。

### 30.60.35 Gallery target fixture tool preparation（2026-08-22）

- `scripts/perf/prepare-kind-scale-fixture.mjs` 现在支持 `gallery`，并新增
  `--files-per-work` 与 `--manifest-output`。Gallery/Audio/CoserPicture 目标 fixture 均使用
  hardlink 复用源文件，manifest 默认写在 output 目录外，避免被 Inventory 当作媒体文件计数。
- 新增回归验证 Gallery `12` 文件/`3` 作者/`4` 文件每 work 的目录形状，以及 Audio `6` 文件/
  `2` 作者/`3` 轨分组；`node --test scripts/perf/prepare-kind-scale-fixture.test.mjs` 为
  `2 passed / 0 failed`。
- 该批只改善 Gallery 700k 规模 evidence 的可重复输入，不执行高成本 `700,000` 文件扫描，
  不改变 Inventory/Search 默认开关、ownership、正式五分钟窗口或 qmediasync Deferred 范围。

### 30.60.36 Gallery 700k kind Inventory/legacy scan evidence（2026-08-22）

独立 Docker 模拟容器使用 Gallery 目标 fixture：`600` 个作者目录、`700,000` 个图片文件，
输入通过 hardlink 复用样本，逻辑大小约 `4.69TB`，物理源文件约 `8.6GB`。容器限制为
`1 CPU / 4GiB / 256 PID`，Inventory 仅启用 `gallery`，其它实验开关保持关闭。

正式 artifact 为
`perf-results/docker-n100-scale-1cpu/gallery-700k-inventory-20260822/run.json`，补充了显式
flag provenance 的副本为 `run-r2.json`。Inventory root 在约 `12分41秒` 完成，最终
`700,000/700,000` discovered/present、`0` missing、`0` error，root=`idle`；包含 legacy
Gallery 资产写入和 search rebuild 的完整 scan job 用时约 `40分05秒`。

最终 health：cgroup memory `126,316,544B`（约 `120.4MiB`），SQLite `717,422,592B`
（约 `684.1MiB`），WAL `17,139,232B`（约 `16.3MiB`），SQLite busy/pool timeout/error、
resource timeout、job failure 均为 `0`；Inventory 批次固定为 `1,024` 行。primary artifact
的 write gate 最大等待约 `3.47s`、最大 hold 约 `3.52s`，队列最终深度为 `0`。

该结果完成 Gallery kind-scale 输入与运行证据，但仍只代表当前 Windows Docker 挂载和应用
数据库路径，不代表物理 NAS/HDD 延迟，也不切换 Gallery ownership；生产 Inventory 默认开关、
Search reader 默认开关和本轮其它 Deferred 项保持不变。

### 30.60.37 Inventory/Search 两项实验功能验收收敛（2026-08-23）

本轮只推进 `Inventory Scanner` 与 `Search shadow/incremental reader`；Facet bitmap、
Derivative Cache v2、JPEG downscale 和 qmediasync 不进入本轮落地、稳定窗口或完成度统计。
生产默认开关仍关闭，正式性能口径仍为 Docker `1 CPU / 4GiB / 256 PID / 双客户端 /
300 秒`。

Inventory 五类目标规模 artifact 已齐全：Novel `10,000/10,000`、Comic `10,000/10,000`、
CoserPicture `8,000/8,000`、Audio `10,000/10,000`、Gallery `700,000/700,000`，各自
missing/pending/failed 为 `0`，并保留 hardlink fixture 不代表真实 TB 物理吞吐的限制说明。
Gallery 受限容器又完成一次 ownership promotion fence 复核：promotion job `7` 从
`2026-08-22T16:01:34Z` 到 `17:24:48Z`，约 `4,994s`（83 分 14 秒）；最终 owner 为
`catalog-v2`、root=`idle`、generation/completed_generation=`3/3`、700,000 present、
600 个 Catalog source、Catalog event pending/failed=`0/0`，Search outbox pending=`0`。
promotion 前的 Legacy Catalog reconciliation 事实为 expected/matched `600/600`，
missing/unexpected/mismatch/error 全为 `0`。随后已执行 rollback，owner 返回 `legacy`，
恢复 job `10` 已在 generation 4 完成：job 用时约 `2,074s`（34 分 34 秒），root=
`idle`、generation/completed_generation=`4/4`、700,000 present、pending/failed=`0/0`；
SQLite `quick_check=ok`、`integrity_check=ok`，最终 works/assets 为 `600/700,600`。
整个 promotion/rollback 过程 RSS 约 `150--277MiB`，SQLite busy、pool timeout/error 和
resource timeout 均为 `0`。rollback 后 ownership 和 legacy writer 已恢复，实验开关仍未
默认打开。

Search 固定 corpus evidence 保持通过：40,000 works、6 条脱敏查询，SQLite/Tantivy
document count 与 ID SHA-256 一致，missing/unexpected/duplicate/invalid 全为 `0`，
pending/revision lag 为 `0`；未 armed 时受控 `503`，armed 后 production reader routing、
canary ID/order diff 均通过。当前 Rust 全量回归为 `360 passed / 0 failed / 3 ignored`，
Node perf `48 passed / 0 failed`，clippy、fmt、frontend build、validator 和 diff check
均通过。两项功能仍是隔离实验性路径，不代表已切换生产默认 reader 或 ownership。

### 30.60.38 Gallery rollback 后 reconciliation 前缀归一化复核（2026-08-23）

Gallery rollback 后首次重新排队的 reconciliation job `11` 暴露出 `600/600` 个
`work.meta._scanner_fingerprint` 差异。进一步核对确认：Catalog v2 将带
`gallery-v1:` writer 前缀的 fingerprint 写入 `works.meta_json`，legacy scanner 表保留
无前缀值；两者代表同一文件事实，不能把该格式差异误报为 Catalog 漂移，也不能因此
阻断后续 promotion。`catalog_reconciliation` 的比较路径现在按目标 kind 去除受控
fingerprint 前缀后再比较；新增回归覆盖 prefixed metadata 与 legacy scanner fingerprint
等价。未改变默认 ownership、扫描器开关或 qmediasync 范围。

在重新构建的 `arislist:n100-sim-gallery-fix-r3`、`1 CPU / 4GiB / 256 PID` 容器中，
reconciliation job `13` 于约 `1,224s`（20 分 24 秒）完成：expected/matched 为
`600/600`，missing/unexpected/mismatch/error 全为 `0`，status=`passed`、current=`true`。
最终 Gallery root 为 `idle`、generation/completed_generation=`4/4`、discovered/present
为 `700,000/700,000`；ownership 保持 `legacy`，pending/failed events 为 `0/0`。
最终 cgroup memory 约 `164.4MiB`，SQLite database/WAL 约 `692.3/16.6MiB`，SQLite
busy、pool timeout/error、resource timeout、active job 均为 `0`；宿主只读检查
`quick_check=ok`、`integrity_check=ok`。

修复后验证：Rust 全量 `360 passed / 0 failed / 3 ignored`，Node perf `48 passed / 0 failed`，
Clippy、Rust fmt、frontend production build、项目 validator 和 `git diff --check` 均通过。
本轮仍只落地 Inventory Scanner 与 Search shadow/incremental reader；Facet bitmap、
Derivative Cache v2、JPEG downscale 和 qmediasync 不进入本轮规划或验收，生产默认开关
继续关闭。

### 30.60.39 Search promotion/reader 对账计数门禁（2026-08-23）

Search 的 Catalog ownership promotion 与首次 incremental reader arm 现在不会只信任持久化
`status='passed'`。两条门禁都会显式验证 SQLite work 数、索引 document 数和唯一 work 数一致，
并要求 missing/unexpected/duplicate/invalid 全为 `0`；计数漂移即 fail-closed。新增路由回归覆盖
“状态为 passed 但文档计数不一致”不得 promotion，既有 revision-zero 空库、正常 armed、
degraded 恢复和 revision churn 回归全部保持通过。

本批只强化本轮两项实验功能的事实边界，不打开 `INVENTORY_SCANNER_ENABLED`、Search reader
或任何 ownership 默认值，不涉及 Facet bitmap、Derivative Cache v2、JPEG downscale 或
qmediasync。正式验收口径仍为 Docker `1 CPU / 4GiB / 256 PID / 双客户端 / 300 秒`。

### 30.60.40 Search cutover probe 静态契约修正与全量复验（2026-08-23）

修正 `scripts/validate-project.mjs` 对 Search cutover probe 的静态匹配，使其检查实际
artifact 行记录使用的 `outbox_revision_lag` 与 `reconciliation_status` snake_case 字段，
不改变运行时探针逻辑或生产路由。项目 validator 现已全绿；Rust 全量回归为
`361 passed / 0 failed / 3 ignored`，Node perf 回归为 `50 passed / 0 failed`，并通过
Clippy、Rust fmt、frontend production build 与 `git diff --check`。

本批确认 `Inventory Scanner` 与 `Search shadow/incremental reader` 的实验性落地、规模
证据和 fail-closed 门禁均完成；默认开关、ownership 与正式 Docker
`1 CPU / 4GiB / 256 PID / 双客户端 / 300 秒` 验收口径保持不变。Facet bitmap、Derivative
Cache v2、JPEG downscale 和 qmediasync 继续不实施、不纳入本轮验收。

### 30.60.41 Search promotion/reader hash 事实门禁（2026-08-23）

进一步收紧 Search promotion 与首次 incremental reader arm：除 SQLite work 数、索引 document
数、唯一 work 数及 missing/unexpected/duplicate/invalid 计数外，现要求 shadow state 的
`indexed_documents` 与 reconciliation document 数一致，并要求 SQLite/Tantivy ID SHA-256
均为标准 64 位十六进制、存在且相等。新增 hash 漂移与 indexed-document 漂移回归；不完整或手工修复的
`status='passed'` 行会 fail-closed。默认开关、ownership、Docker 五分钟正式验收口径及
Deferred 范围均不变。

### 30.60.42 Armed reader 快速路径 hash fail-closed（2026-08-23）

armed incremental reader 的快速路径现在也要求保留上次有效的双方 ID SHA-256 证据；若
状态被不完整地手工修复为 `cutover_armed=1` 但 hash 缺失、格式非法或不一致，会拒绝服务。
正常 revision churn 只将 reconciliation 标为 stale、保留有效 hash，因此不影响 shadow
追赶后的正常 reader 恢复。新增 armed 状态 hash 缺失回归；Inventory、Search 默认开关和
正式 Docker 验收口径不变。

### 30.60.43 Search production reader 只读 gate 降低写闸门竞争（2026-08-23）

将 incremental reader gate 拆为“快照读取/纯校验”和“首次 arm 条件更新”两层：已 arm 的
production 请求现在只持有 `SHADOW_OUTBOX_LOCK` 与短 `TrackedReadTransaction`，不再申请
单写入闸门；首次 arm 仍在同一 tracked writer transaction 内完成事实校验和条件更新，避免
Catalog/Search revision 在校验与 arm 之间发生竞态。新增回归确认 armed/unarmed 的只读校验均
不增加 write-gate acquire/completed 计数。

使用新镜像 `arislist:n100-sim-current-r6`，边界为 Docker `1 CPU / 4GiB / 256 PID`，固定
`40,000` works、6 条查询的 cutover probe、shadow canary、incremental reader 均通过：未 arm
请求全部 `503`，arm 后全部走 `production`，canary ID/order mismatch、失败和 rejection 均为
`0`。原始证据位于 `perf-results/docker-n100-sim/search-current-r6-cutover/`、
`search-current-r6-canary/` 和 `search-current-r6-incremental/`。全量 Rust `362 passed /
0 failed / 3 ignored`、Node perf `50 passed`、Clippy、validator、fmt、前端 build 和
`git diff --check` 均通过；Inventory、Search 默认开关以及 Facet/Derivative/JPEG/qmediasync
范围保持不变。

### 30.60.44 本轮计划范围确认（2026-08-23）

根据本轮范围确认，计划中的实验性功能落地仅包含 `Inventory Scanner` 与 `Search
shadow/incremental reader`。两项功能的实现、规模证据、事实对账、fail-closed 门禁和
Docker `1 CPU / 4GiB / 256 PID / 300 秒`模拟验收均计入本轮；生产默认开关和 ownership
仍保持关闭。`Facet bitmap`、`Derivative Cache v2`、`JPEG downscale` 本轮不再安排落地、
稳定窗口或完成度统计，qmediasync 的改动、专项测试和验收继续 Deferred。剩余工作仅是实际
部署库启用前的逐 kind promotion/rollback 复核，不构成上述两项实验功能的实现阻塞。

### 30.60.45 Inventory/Search 实验路径 promotion 矩阵闭环（2026-08-23）

- 修复 Audio Catalog reconciliation 的 unexpected-work 匹配边界：`auto/rj` 分组下，
  legacy scanner 允许将一个 RJ 产品写入 `root/RJ编号/产品子目录`，Inventory 的 work key
  仍为 `RJ编号`。对账现在在保持精确匹配的同时，允许同一 RJ work key 下的合法嵌套 legacy
  source path；非 Audio、非 RJ 目录和 qmediasync 仍走原有精确 source 规则。新增嵌套 RJ
  回归，避免通过放宽全局 unexpected 检查来掩盖真实孤儿作品。
- 在重新构建的 `arislist:n100-sim-current-r8`、Docker `1 CPU / 4GiB / 256 PID` 隔离容器
  `promotion-matrix-r1` 中，五类 Inventory promotion/rollback 均通过：Novel
  `promotion-novel-r2.json`、Comic `promotion-comic.json`、CoserPicture
  `promotion-coser-picture-r2.json`、Audio `promotion-audio-r6.json`、Gallery
  `promotion-gallery-r1.json`。所有 rollback 后 authoritative writer 均回到 `legacy`，
  pending/failed Catalog events 为 `0/0`；Audio 最终 reconciliation 的
  expected/matched 为 `3/3`，missing/unexpected/mismatch/error 为 `0/0/0/0`。
- 旧 Audio 失败产物 `promotion-audio-r3/r4/r5.json` 保留作失败诊断，不计入当前通过证据；
  新产物与现有 kind-scale/Search artifact 一起构成两项实验功能的隔离验收链。生产默认开关、
  ownership、正式 Docker `1 CPU / 4GiB / 256 PID / 双客户端 / 300 秒`口径保持不变。
- 本轮最终复验：Node perf `53 passed / 0 failed`，Rust `365 passed / 0 failed / 3 ignored`，
  Clippy、项目 validator、Rust fmt、frontend production build 和 `git diff --check` 均通过。
  Facet bitmap、Derivative Cache v2、JPEG downscale 和 qmediasync 继续不实施、不纳入本轮
  规划或验收。

### 30.60.46 当前工作树 promotion/rollback 复验（2026-08-23）

为刷新当前代码快照证据，在 `arislist:n100-sim-current-r8`、容器
`promotion-matrix-r1`（实际限制 `1 CPU / 4GiB / 4GiB swap / 256 PID`）中重新执行五类
promotion/rollback。当前 dirty-state SHA-256 为
`1f2dc23caa759b2bc4c77849b7e0938cd493729ce012e894f494c8f757bd61f2`。

新增 artifact：
`perf-results/docker-n100-sim/promotion-matrix-r1/promotion-novel-current-r2.json`、
`promotion-comic-current-r2.json`、`promotion-coser-picture-current-r2.json`、
`promotion-audio-current-r2.json`、`promotion-gallery-current-r2.json`。五份均为
`passed`，且每类都完成 `legacy -> catalog-v2 -> legacy`，promotion/rollback 检查均为
`passed`，root `present == discovered`，health `ok`，failed jobs、SQLite busy、pool
timeout、resource timeout 和 Catalog pending/failed events 均为 `0`。

这组 artifact 只刷新当前实现的 promotion/rollback 控制面证据；目标规模扫描证据仍引用
Novel `10,000`、Comic `10,000`、CoserPicture `8,000`、Audio `10,000` 和 Gallery
`700,000` 的既有 artifact。两项实验功能的默认开关、ownership 和 production reader
仍保持关闭/fail-closed，其他 Deferred 项与 qmediasync 不变。
