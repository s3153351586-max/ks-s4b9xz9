# IPTV 源自动巡检看板

云端 GitHub Actions 日更体检，广西移动内网源由手机侧巡检，公网看板脱敏发布。

- **看板**：https://s3153351586-max.github.io/ks-s4b9xz9/
- **巡检**：每日北京时间 08:00 自动运行（也可手动触发）

---

## 这套东西在做什么

```
┌──────────── GitHub Actions（每天 UTC 00:00 / 北京 08:00）────────────┐
│  tools/probe_v4.py      探测公网源 A1-A5 / B2                        │
│         ↓ report.json                                                │
│  tools/publish_cloud.py   R12b 脱敏闸门 → 推 gh-pages 孤儿分支        │
│         ↓                                                            │
│  gh-pages: index.html + cloud/latest.json + status/latest.json       │
└──────────────────────────────────────────────────────────────────────┘
                    ↓ GitHub Pages 渲染        ↓ 手机拉摘要
┌──────────────── 手机（Termux，每 2 小时）────────────────┐
│  probe_local.py   动态抽取内网直链 → 探 B1/B1b/B2i + 基线 │
│  merge_on_phone.py  本地合并                             │
│       ├─→ merge.html   本地轨（含直链，仅限局域网）        │
│       └─→ 告警文本      有告警才推送                      │
└──────────────────────────────────────────────────────────┘
```

**核心取向**：宁可漏报也不误报。家里路由器挂了时所有源都会"失败"，但那不是源的问题——网络基线与告警抑制就是为此而设。

---

## 一、部署（GitHub 网页端上传，不用命令行）

### 1. 上传文件

浏览器打开 <https://github.com/s3153351586-max/ks-s4b9xz9>，
点 **Add file → Upload files**，把 `DEPLOY-README.md` 末尾清单里的文件
**按目录结构**拖进去（网页支持拖拽文件夹）。

> ⚠️ `.github/workflows/` 目录**必须存在**，否则 Actions 不会注册。
> 网页上传时如果拖入整个 `.github` 文件夹即可；若只能传单文件，
> 请用 **Add file → Create new file**，文件名填
> `.github/workflows/publish.yml`（斜杠会自动建目录）。

提交到 **main** 分支。

### 2. 开启 GitHub Actions 写权限

**Settings → Actions → General → Workflow permissions**：

- 选 **Read and write permissions**
- 保存

> 这一步不能省。workflow 里的 `permissions: contents: write` 是**申请**，
> 但仓库级设置若为只读，申请会被拒——表现为 push 时报 403。

### 3. 配置 Pages

**Settings → Pages**：

| 项 | 值 |
|---|---|
| Source | **Deploy from a branch** |
| Branch | **`gh-pages`** |
| 目录 | **`/ (root)`** |

> `gh-pages` 分支在第一次 workflow 成功运行后才会出现。
> 所以顺序是：先跑一次 Actions → 再回来配 Pages。

### 4. 手动跑一次验证

**Actions → publish-dashboard → Run workflow → Run workflow**

跑完后检查：

- ✅ 绿色对勾
- ✅ 仓库出现 `gh-pages` 分支，含 `index.html` / `cloud/` / `status/`
- ✅ 打开 Pages 地址能看到看板

---

## 二、防自我触发循环（三重保险）

产物 push 到 gh-pages 会不会又触发一次 workflow？不会：

| # | 保险 | 位置 |
|---|---|---|
| 1 | `on:` **只写** schedule + workflow_dispatch，**没有 `on: push`** | `.github/workflows/publish.yml` |
| 2 | 推送用内置 `secrets.GITHUB_TOKEN`（其 push 默认不触发 workflow） | workflow 的 env |
| 3 | commit message 带 `[skip ci]` | `publish_cloud.py` 的 `SKIP_CI_SUFFIX` |

外加 workflow 只 `checkout main`，**从不 checkout gh-pages**——产物分支完全由
`publish_cloud.py` 内部 fetch/创建，职责分离。

---

## 三、关于 cron 的 5-15 分钟延迟（正常现象）

GitHub Actions 的 `schedule` 是**尽力而为**，不是精确定时：

- 高峰期（UTC 00:00 是负载高峰）通常延迟 **5-15 分钟**
- 极端情况下可能**跳过**某次运行
- 这是官方已知行为，**不是配置错误**

所以别指望 08:00:00 整点看到新数据，08:15 前属于正常范围。
真要精确，只能用自建 cron（见 §六）。

---

## 四、看板两条轨道

| | 本地轨 `merge.html` | 公网轨 `index.html` |
|---|---|---|
| 生成者 | `merge_on_phone.py` | `publish_cloud.py` |
| 字段 | **全字段**，含 `url`/`final_url`/`segment_url` | 仅脱敏白名单 |
| 直链 | ✅ 含 | ❌ 零直链 |
| 暴露面 | 仅本机/局域网 | 公网 |

公网轨保留字段（白名单，**新字段默认被剥离**而非默认泄漏）：

```
id, name, kind, scope, status, ok,
latency_ms, ttfb_ms, speed_kbps, change, error, hint
```

本地轨顶部有红色警告条。**别提交到任何仓库、别发到公网。**

---

## 五、手机侧（可选，但内网源必需）

内网源（广西移动 CDN 域）只有广西移动网络内才能测，云端在外省必然是假阴性
——所以这块能力整体下放到了手机。

```bash
# Termux
pkg install python
pip install -r tools/requirements-phone.txt
```

> **手机侧不需要任何 GitHub 凭据**：它只读公开的 raw 文件 + 本地合并告警，
> 保持零凭据是刻意设计（手机上放长期 token 风险高于收益）。

### 云端摘要的三级回退

手机拉 `status/latest.json` 按序尝试，任一成功即用：

| 级别 | 通道 | 特点 |
|---|---|---|
| 1 | **Pages URL** | 最快（CDN 缓存），但可能有构建延迟 |
| 2 | **raw(gh-pages 分支)** | 直读仓库，比 Pages 更新更及时 |
| 3 | **gh-proxy(raw)** | 绕过 raw 在部分运营商网络下的 TLS 阻断/污染 |

全部失败则降级为"云端视角未知"，**不影响本地告警**（本地判断自成一体）。

配置（`run_trial.sh` 内置默认值）：

```bash
export IPTV_STATUS_URL=https://s3153351586-max.github.io/ks-s4b9xz9/status/latest.json
```

### 定时（crontab，每 2 小时）

```cron
0 */2 * * * cd ~/iptv-src && ./run_trial.sh phone
```

手机轨**只在有告警时推送**，无告警静默——避免每 2 小时一次噪声把真告警淹掉。

---

## 六、自建 cron（可选，替代 Actions）

Actions 的 5-15 分钟延迟不能接受时，可在自己机器上跑：

```bash
export GITHUB_PAT=github_pat_xxxxxxxx    # 需要 Contents: Read and write
0 8 * * * cd ~/iptv-src && ./run_trial.sh cloud
```

> ⚠️ cron **不加载** `~/.bashrc`，环境变量必须写在 cron 行内或用
> `run_trial.sh` 自动 source `~/.iptv_env`。

---

## 七、观察期指标（7 天）

看 `status/latest.json` 的 `counters`：

```json
{
  "run_id": 12,
  "counters": {
    "probe_runs": 12, "alarm_runs": 3, "alarms_total": 5,
    "suppressed_total": 1, "pending_total": 1,
    "first_run_at": "2026-09-21T..."
  }
}
```

| 指标 | 看什么 |
|---|---|
| `alarm_runs / probe_runs` | **告警率**。过高 = 误报太多，要调阈值 |
| `suppressed_total` | 被抑制次数。**漏报风险来源**，需人工抽查是否合理 |
| `pending_total` | 积压待复核（网络恢复后要回头看） |
| `probe_runs` | 连续性。不增长说明 Actions 没在跑 |

---

## 八、常见问题

**Q: Actions 报 403 `Resource not accessible`**
§一.2 的 Workflow permissions 没设成 Read and write。

**Q: `gh-pages` 分支不存在**
第一次 workflow 还没跑成功。去 Actions 页看失败原因。

**Q: 看板 404**
Pages 的 Source 指错分支了，应该是 `gh-pages` / root。

**Q: 数据几天没更新**
① Actions 被自动禁用（仓库 60 天无活动）→ 去 Actions 页 Enable；
② 看 `probe_runs` 是否在涨。

**Q: 手机总说"家里视角缺报"**
Actions 24 小时没收到手机摘要。检查手机端 cron 在不在跑。

**Q: 内网源显示"云端免测"**
正常。内网源在云端必然不可达，显式标记 skip 而非静默消失，是为了让人
一眼看懂不是漏配。

---

## 九、本地跑测试

```bash
python3 test_probe_v4.py      # 214 项断言
```

覆盖：脱敏白名单、fail-closed 闸门、双轨渲染、计数器单调性、动态抽取与回退、
head 判定放宽、告警抑制链、位置感知跳过、参数兼容垫片、R14 workflow 结构。

---

## 十、目录结构

```
.
├── .github/workflows/publish.yml   # Actions 定义
├── requirements.txt                # 云端依赖（锁 aiohttp 版本）
├── index.html                      # 公网看板（单文件零依赖）
├── tools/
│   ├── probe_v4.py                 # 云端探针（仅 public 源）
│   ├── publish_cloud.py            # 脱敏 + 发布 gh-pages
│   ├── summarize_probe.py          # workflow 用：体检结果摘要
│   ├── requirements-phone.txt      # 手机侧依赖
│   ├── probe_local.py              # 手机探针（内网动态抽取）
│   ├── merge_on_phone.py           # 手机合并 + 本地轨看板
│   └── iptv_daily_report.py        # 日报（可选）
└── README.md
```

`gh-pages` 分支（自动生成，勿手工编辑）：

```
gh-pages/
├── index.html
├── cloud/latest.json
├── cloud/history/YYYYMMDD.json     # 保留最近 30 天
└── status/latest.json
```
