# ArisList

ArisList 是一个自托管的本地媒体书架，用于管理漫画、轻小说、音声和图库。它支持媒体扫描、标签、搜索、阅读进度、漫画/EPUB/音频/图库浏览器，并可通过 qmediasync STRM 接入 115 云端漫画资源。

## 功能

- 扫描本地 CBZ 漫画、EPUB 轻小说、音声文件夹和图库文件夹。
- 图库支持缩略图、虚拟网格、浏览历史和进度恢复。
- 内置全屏漫画阅读器、EPUB 阅读器、音频播放器和图库浏览器。
- 支持 qmediasync STRM 漫画源，避免项目本身递归请求网盘目录。
- 适合 Docker/NAS 部署，媒体目录只读挂载，应用数据单独保存。

## Docker

复制 `.env.example` 为 `.env`，然后修改管理员密码、会话密钥和可选的 qmediasync 配置。

```bash
docker compose pull
docker compose up -d
```

打开：

```text
http://localhost:8787
```

NAS 部署时，修改 `docker-compose.yml` 里的 `volumes`。左侧是宿主机/NAS 路径，右侧是 ArisList 容器内使用的路径：

```yaml
- /volume1/media/comics:/library/comics:ro
- /volume1/media/novels:/library/novels:ro
- /volume1/media/audio:/library/audio:ro
- /volume1/media/gallery:/library/gallery:ro
```

建议保持媒体目录只读挂载。`data/` 和 `generated/` 需要可写，用于保存数据库、索引、缩略图和缓存。

默认 Compose 配置会拉取：

```text
ghcr.io/arismaid/arislist:latest
```

如需固定版本或使用自己的镜像仓库，在 `.env` 中修改：

```env
ARISLIST_IMAGE=ghcr.io/arismaid/arislist:v1.0.0
```

如果要从当前源码本地构建：

```bash
docker compose -f docker-compose.yml -f docker-compose.build.yml up --build -d
```

镜像发布由 `.github/workflows/docker-image.yml` 完成。推送到 `main`/`master` 会发布 `latest` 和 `sha-...` 标签；推送 `v1.0.0` 这类 Git tag 会发布对应版本标签。

如果 `docker compose pull` 返回 `denied`，通常是 GHCR 镜像还没有发布，或 GitHub Packages 中该镜像包仍是私有。公开部署时请在仓库的 Packages 设置中把镜像设为 Public；私有部署时，需要先在部署机器上执行 `docker login ghcr.io`。

## 开发

后端：

```bash
cargo run -p media-shelf-server
```

前端：

```bash
npm install --prefix frontend
npm run dev --prefix frontend
```

检查：

```bash
cargo test -p media-shelf-server
npm run build --prefix frontend
node scripts/validate-project.mjs
```
