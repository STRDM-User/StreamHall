<div align="center"><a name="readme-top"></a>

# StreamHall

**自托管直播站点 - 公开列表、播放器、管理后台、观看统计与推送通知**

[English](./README.md) · **简体中文**

[![][license-shield]][license-link]
[![][python-shield]][python-link]
[![][docker-shield]][docker-link]
[![][postgres-shield]][postgres-link]

</div>

---

- [✨ 功能特性](#-功能特性)
- [🚀 快速开始](#-快速开始)
- [💻 本地开发](#-本地开发)
- [⚙️ 配置项](#️-配置项)
- [🐳 服务说明](#-服务说明)
- [🔌 推流配置](#-推流配置)
- [📡 API 文档](#-api-文档)
- [🤝 参与贡献](#-参与贡献)
- [📝 许可证](#-许可证)

## ✨ 功能特性

- **公开直播列表** - 直播 / 存档双 Tab，支持密码保护、自定义站点品牌，内置中英双语界面（含分语言站点简介）
- **播放器** - 基于 ArtPlayer，支持 HLS、FLV、MPEG-DASH 播放；支持 AES-128 密钥覆盖及 DASH ClearKey
- **管理后台** - 直播的增删改查、启用/禁用、拖拽排序；多播放源管理
- **观看统计** - 会话追踪、独立访客数、峰值并发、平均时长、设备 / 浏览器 / 操作系统 / 地理分布实时看板，支持 CSV 导出
- **Telegram 推送** - 可按直播单独配置，开播 / 关播自动发送通知
- **推流配置** - SRS RTMP 推流辅助工具，隐藏 HLS 路由代理（公开地址不暴露真实推流码）
- **HLS 代理** - 带签名验证的 `/proxy/hls/` 路由，解决跨域 HLS 播放问题
- **API 密钥鉴权** - 在后台生成 Token，可通过 API 密钥对所有管理及统计接口进行程序化访问

<div align="right">

[![][back-to-top]](#readme-top)

</div>

## 🚀 快速开始

**1. 下载并配置**

```bash
curl -LO https://git.stdm.moe/Stardream/StreamHall/raw/branch/main/docker-compose.yml
curl -LO https://git.stdm.moe/Stardream/StreamHall/raw/branch/main/nginx-hls.conf
```

> [!IMPORTANT]
> 启动前请打开 `docker-compose.yml`，将 `SECRET_KEY` 和 `POSTGRES_PASSWORD` 改为强随机值。

**2. 启动**

```bash
docker compose up -d
```

**3. 获取初始管理员密码**

```bash
docker logs streamhall
```

在日志中找到：

```
StreamHall initial admin password: <random-password>
```

**4. 访问地址**

| 地址 | 说明 |
|---|---|
| `http://HOST:8085/` | 公开直播列表 |
| `http://HOST:8085/admin` | 管理后台 |
| `http://HOST:18088/` | SRS HTTP 播放 |
| `http://HOST:8889/` | Nginx HLS 代理 |

> [!TIP]
> 首次登录后，请在**网站设置 → 安全设置**中修改密码。初始密码仅打印一次，不会以明文形式存储。

**更新到新版本**

```bash
docker compose pull && docker compose up -d
```

<div align="right">

[![][back-to-top]](#readme-top)

</div>

## 💻 本地开发

**前置要求：** Python 3.12+、PostgreSQL 14+

```bash
# 1. 克隆仓库
git clone https://git.stdm.moe/Stardream/StreamHall.git
cd StreamHall

# 2. 安装唯一的运行时依赖
pip install -r requirements.txt

# 3. 配置必要的环境变量
export SECRET_KEY=dev-secret
export DATABASE_URL=postgresql://user:pass@localhost:5432/streamhall

# 4. 启动服务
python server.py
```

服务默认监听 `http://localhost:8080`。首次启动时会自动创建数据库结构，并在终端打印一次性管理员密码。

<div align="right">

[![][back-to-top]](#readme-top)

</div>

## ⚙️ 配置项

在 `docker-compose.yml` 中设置以下环境变量：

| 变量 | 默认值 | 是否必填 | 说明 |
|---|---|---|---|
| `SECRET_KEY` | `change-this-secret` | **必填** | 会话与推流路由的 HMAC 签名密钥 |
| `DATABASE_URL` | 见 compose 文件 | **必填** | PostgreSQL 连接字符串 |
| `POSTGRES_PASSWORD` | 见 compose 文件 | **必填** | PostgreSQL 数据库密码 |
| `TZ` | `UTC` | 否 | 容器时区，如 `Asia/Shanghai` |
| `SRS_HTTP_ORIGIN` | `http://srs:8080` | 否 | SRS HTTP 播放基础地址 |
| `STREAM_PROBE_TIMEOUT` | `4` | 否 | 流地址探测超时秒数 |
| `STREAM_MONITOR_INTERVAL` | `10` | 否 | 流存活检测间隔秒数 |
| `TELEGRAM_TIMEOUT` | `6` | 否 | Telegram API 请求超时秒数 |

> [!WARNING]
> 在将 StreamHall 暴露到网络前，务必修改 `SECRET_KEY` 和 `POSTGRES_PASSWORD`。默认值仅为占位符，安全性极低。

<div align="right">

[![][back-to-top]](#readme-top)

</div>

## 🐳 服务说明

默认 compose 配置启动四个容器：

| 容器名 | 镜像 | 端口 | 用途 |
|---|---|---|---|
| `streamhall` | 本地构建 | `8085→8080` | Python 应用 + 静态前端 |
| `streamhall-postgres` | `postgres:16-alpine` | - | PostgreSQL 持久化数据库 |
| `streamhall-srs` | `ossrs/srs:6` | `1935`、`18088→8080` | RTMP 推流 + HTTP 播放 |
| `streamhall-nginx-hls` | `nginx:alpine` | `8889→80` | HLS 公开路由代理 |

持久化数据存储在 `./postgres-data/` 目录下。

<div align="right">

[![][back-to-top]](#readme-top)

</div>

## 🔌 推流配置

SRS 在 `1935` 端口接收 RTMP 推流，推流端配置示例：

```
RTMP 服务器：  rtmp://HOST:1935/live
推流码：       your-stream-key
```

管理后台的**推流配置**页面可生成隐藏公开 HLS 地址（`/h/<slug>/...`），通过 Nginx 在 `8889` 端口对外提供访问，真实推流码不会出现在公开 URL 中。

> [!NOTE]
> RTMP 主机字段支持填写自定义端口，如 `live.example.com:1935`。若使用非标准端口，请勿将 `:1935` 硬编码到地址中。

<div align="right">

[![][back-to-top]](#readme-top)

</div>

## 📡 API 文档

### 鉴权方式

- **浏览器会话** - 通过 `POST /api?action=login` 登录后设置的 Cookie
- **API 密钥** - 在请求头携带 `Authorization: Bearer <token>`，或在 URL 追加 `?api_key=<token>`。密钥在后台**网站设置 → API 密钥**处管理。

### 公开接口

| 方法 | 接口 | 说明 |
|---|---|---|
| `GET` | `/api?action=public_list` | 首页直播列表 |
| `GET` | `/api?action=site_settings` | 站点标题、简介、品牌设置 |
| `GET` | `/api?action=get_player_data&id=<public_id>` | 播放器播放源及元数据 |
| `POST` | `/api?action=verify_password` | 验证直播访问密码 |

### 管理接口（需鉴权）

| 方法 | 接口 | 说明 |
|---|---|---|
| `POST` | `/api?action=login` | 登录并创建会话 |
| `GET` | `/api?action=logout` | 退出当前会话 |
| `GET` | `/api?action=list_admin` | 完整直播列表 |
| `POST` | `/api?action=add` | 添加直播 |
| `POST` | `/api?action=update` | 修改直播 |
| `POST` | `/api?action=delete` | 删除直播 |
| `POST` | `/api?action=set_stream_enabled` | 切换直播可见状态 |
| `POST` | `/api?action=reorder_streams` | 调整同分组内排序 |
| `POST` | `/api?action=update_site_settings` | 保存网站设置 |
| `GET` | `/api?action=telegram_settings` | 读取 Telegram Bot 配置 |
| `POST` | `/api?action=update_telegram_settings` | 保存 Telegram Bot 配置 |
| `POST` | `/api?action=update_admin_password` | 修改管理员密码 |
| `GET` | `/api?action=list_api_keys` | 列出所有 API 密钥（不返回 Token 明文） |
| `POST` | `/api?action=create_api_key` | 创建密钥（Token 仅返回一次） |
| `POST` | `/api?action=delete_api_key` | 按 `id` 撤销密钥 |

### 统计接口（需鉴权）

| 方法 | 接口 | 说明 |
|---|---|---|
| `GET` | `/api?action=stats_overview` | 汇总数据 |
| `GET` | `/api?action=stats_streams` | 各直播统计明细 |
| `GET` | `/api?action=stats_timeseries` | 分桶观看趋势 |
| `GET` | `/api?action=stats_geo` | 地理分布（国家级） |
| `GET` | `/api?action=stats_stream_detail&id=<id>` | 单场直播详情 |
| `GET` | `/api?action=stats_sessions_page&id=<id>` | 分页会话列表 |
| `GET` | `/api?action=stats_export_csv` | 导出会话 CSV |

<div align="right">

[![][back-to-top]](#readme-top)

</div>

## 💡 致谢

StreamHall 参照 [AniLive](https://anilive.nekoss.cn/) 网站进行重构，该网站提供直播列表功能但未开源。本项目为独立重写实现，与原网站不共享任何代码。

<div align="right">

[![][back-to-top]](#readme-top)

</div>

## 🤝 参与贡献

欢迎提交 Issue 和 Pull Request。较大的改动请先开 Issue 讨论方向。

<div align="right">

[![][back-to-top]](#readme-top)

</div>

## 📝 许可证

版权所有 © 2026 [Stardream](https://git.stdm.moe/Stardream)。本项目基于 [GNU Affero General Public License v3.0](./LICENSE) 协议开源。

**授权权限**

| | |
|---|---|
| ✅ | 商业使用 |
| ✅ | 修改源代码 |
| ✅ | 分发 |
| ✅ | 私人使用 |

**使用条件**

| | |
|---|---|
| ⚠️ | 开放源码 - 修改后的版本同样必须开源 |
| ⚠️ | 相同许可证 - 衍生作品须采用 AGPL-3.0 协议 |
| ⚠️ | 网络使用视同分发 - 若将修改版作为网络服务运行，须向用户提供完整源码 |
| ⚠️ | 说明修改内容 - 需记录对源码的修改 |

简而言之：你可以自由使用、修改和自部署 StreamHall。但若将修改版对外分发或作为托管服务提供，须以 AGPL-3.0 协议公开完整源码。

---

<!-- LINK GROUP -->
[back-to-top]: https://img.shields.io/badge/-回到顶部-black?style=flat-square
[license-shield]: https://img.shields.io/badge/许可证-AGPL--3.0-white?labelColor=black&style=flat-square
[license-link]: ./LICENSE
[python-shield]: https://img.shields.io/badge/Python-3.12-blue?labelColor=black&logo=python&logoColor=white&style=flat-square
[python-link]: https://www.python.org/
[docker-shield]: https://img.shields.io/badge/Docker-Compose-2496ED?labelColor=black&logo=docker&logoColor=white&style=flat-square
[docker-link]: https://docs.docker.com/compose/
[postgres-shield]: https://img.shields.io/badge/PostgreSQL-16-336791?labelColor=black&logo=postgresql&logoColor=white&style=flat-square
[postgres-link]: https://www.postgresql.org/
