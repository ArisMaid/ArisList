# ArisList

ArisList 是一个自托管的本地媒体书架，用于管理漫画、轻小说、音声、图库和 CoserPicture。它支持媒体扫描、标签、搜索、阅读进度、漫画/EPUB/音频/图库浏览器，并可通过 qmediasync STRM 接入 115 云端漫画资源。

## 功能

当前版本：**v0.5.0**。升级注意事项与镜像用法见 [发布说明](docs/releases/v0.5.0.md)。

- 扫描本地 CBZ 漫画、EPUB 轻小说、音声文件夹、图库文件夹和 CoserPicture zip 图片包。
- 图库支持缩略图、虚拟网格、浏览历史和进度恢复。
- 内置全屏漫画/CoserPicture 阅读器、EPUB 阅读器、音频播放器和图库浏览器。
- 支持 qmediasync STRM 漫画源，避免项目本身递归请求网盘目录。
- 适合 Docker/NAS 部署，媒体目录只读挂载，应用数据单独保存。

## Docker

复制 `.env.example` 为 `.env`，然后按需修改 qmediasync 等运行配置。

本项目不再提供管理员密码机制。请通过 Docker 端口暴露范围、反向代理或局域网访问控制保护服务；不要将未额外保护的端口直接暴露到公网。

```bash
docker compose pull
docker compose up -d
```

打开：

```text
http://localhost:8787
```

NAS 部署时，修改 `docker-compose.yml` 里的 `volumes`。左侧是宿主机/NAS 路径，右侧是 ArisList 容器内使用的路径。设置页中的资源目录为只读展示，修改资源位置需要改动 volume 映射和对应的容器环境变量后重启：

```yaml
- /volume1/media/comics:/library/comics:ro
- /volume1/media/novels:/library/novels:ro
- /volume1/media/audio:/library/audio:ro
- /volume1/media/gallery:/library/gallery:ro
- /volume1/media/COS图:/library/coser-picture:ro
```

建议保持媒体目录只读挂载。`data/`、`generated/` 和 `cover-cache/` 需要可写，用于保存数据库、索引、缩略图、生成资源和按媒体库拆分的封面缓存。

封面缓存默认写入容器内 `/app/cover-cache`，并按媒体库分为 `comic`、`novel`、`audio`、`gallery`、`coser-picture` 子目录。需要单独指定时，可在 `.env` 中设置：

```env
COVER_CACHE_DIR=/app/cover-cache
COMIC_COVER_CACHE_DIR=/app/cover-cache/comic
NOVEL_COVER_CACHE_DIR=/app/cover-cache/novel
AUDIO_COVER_CACHE_DIR=/app/cover-cache/audio
GALLERY_COVER_CACHE_DIR=/app/cover-cache/gallery
COSER_PICTURE_COVER_CACHE_DIR=/app/cover-cache/coser-picture
```

默认 Compose 配置会拉取：

```text
ghcr.io/arismaid/arislist:latest
```

如需固定版本或使用自己的镜像仓库，在 `.env` 中修改：

```env
ARISLIST_IMAGE=ghcr.io/arismaid/arislist:v0.5.0
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

Windows 本地 STRM 来源有意使用 Fake-IP/内网解析时，可明确指定已确认的来源启动（不自动信任整个内网）：

```powershell
.\scripts\start-local-strm.ps1 -TrustedOrigin 'http://your-media-host:12366'
```

该配置仅用于此次后端启动；普通 `cargo run` 不会自动读取 `.env`。公网来源继续使用普通启动方式。


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
