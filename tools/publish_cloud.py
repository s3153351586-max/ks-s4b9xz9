#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
云端报告发布器（R9：链路方向反转）
====================================
设计动机
--------
旧链路是「本地 → 云端」：手机把 local.json 上传到云端，云端合并。
问题：手机在内网、云端在公网，**上传方向要打通入站网络**（端口/NAT/鉴权），
而手机侧恰恰是最不方便开入站的一端。

R9 把方向反过来：**云端发布、手机拉取合并**。
  1. 云端探测 public 源 → 写 `cloud/latest.json` + `cloud/history/YYYYMMDD.json`
     + `cloud/status/latest.json`（供手机拉取的轻量摘要）
  2. 手机 `probe_local` 出 `local.json` → GET 云端 `status/latest.json`
     → 本地合并 → 出结论 → rikkahub 推送
  3. 云端日报**降级**：不再承担「合并定性」职责，只做公开视角归档 + 网页数据源
  4. 本地摘要若超过 24h 未更新，云端日报标注「家里视角缺报」——
     **这条标注本身就是告警：手机侧探针挂了**

为什么用 git 分支而不是 API 上传
--------------------------------
GitHub Pages 只能从仓库分支发布，所以「推送」和「发布」共用一次 git push 即可，
零服务器、零成本、零额外凭据（复用已有的 gh 登录态）。

敏感度
------
本仓库只含**公网源探测结果**（公开源的可用性/延迟/码率），不含任何内网地址、
账号、密钥。因此可安全使用公开仓库。唯一要求：**仓库名不可被轻易猜中**，
否则等于公开你的源清单（见部署说明）。

用法：
    python3 publish_cloud.py --report /tmp/iptv_20250101.json \\
        --repo ~/iptv-dashboard --branch dashboard --push
    python3 publish_cloud.py --report r.json --dry-run      # 只打印不落盘
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

CST = timezone(timedelta(hours=8))  # 中国标准时间，避免云端 UTC 造成日期错位
STATUS_TTL_HOURS = 24               # 本地摘要超过此时长视为「家里视角缺报」

# ============================================================ 部署配置（R12/R14）
# 【安全红线】凭据 **绝不硬编码**。密钥一旦进代码就随 git 历史永久留存，
# 即使之后删除也仍可从历史中检出。这里只读环境变量，仓库里零密钥。
#
# 凭据来源优先级（R14）：
#   1. GITHUB_TOKEN —— GitHub Actions 内置凭据，**首选**。
#      它由 Actions 运行时自动注入，无需任何长期 PAT；权限由 workflow 的
#      `permissions: contents: write` 授予。用它 push 默认**不会**触发新
#      workflow，天然规避自我触发循环。
#   2. GITHUB_PAT   —— 本地/云端手动运行时的回退（如 cron 跑在自建机器上）。
#
#   export GITHUB_TOKEN=xxx   （Actions 自动注入）
#   export GITHUB_PAT=github_pat_xxx   （手动运行时才需要）
DEFAULT_PAGES_URL = "https://s3153351586-max.github.io/ks-s4b9xz9/"

REPO_URL: str = os.environ.get(
    "IPTV_REPO_URL",
    os.environ.get("GITHUB_REPO", "https://github.com/s3153351586-max/ks-s4b9xz9"),
).rstrip("/")
# R14：发布分支从 dashboard 改为 gh-pages（孤儿分支，与代码分支隔离）
BRANCH_DEFAULT: str = os.environ.get("IPTV_BRANCH", "gh-pages")

# history 保留天数（R14）：超过此天数的 cloud/history/*.json 在发布时删除，
# 避免孤儿分支随日积月累无限膨胀。
HISTORY_KEEP_DAYS: int = int(os.environ.get("IPTV_HISTORY_KEEP_DAYS", "30"))

# commit message 后缀：阻止这次 push 触发新的 workflow（三重保险之一）
SKIP_CI_SUFFIX = "[skip ci]"


def get_credential() -> Optional[str]:
    """
    按 R14 优先级取凭据：GITHUB_TOKEN > GITHUB_PAT。

    【为什么 GITHUB_TOKEN 优先】
      Actions 环境里 GITHUB_TOKEN 是自动注入的短期凭据（默认 1 小时有效），
      用完即弃，不存在长期密钥的泄漏面。GITHUB_PAT 只在本地/自建 cron 上
      手动运行时才需要。

    Returns:
        token 字符串；两者都未设置时返回 None。
    """
    return os.environ.get("GITHUB_TOKEN") or os.environ.get("GITHUB_PAT")


def build_auth_remote(repo_url: Optional[str] = None) -> Optional[str]:
    """
    构造带鉴权的 HTTPS 远端 URL（x-access-token 形式）。

    【为什么用 x-access-token 而非 user:pass】
      GitHub 推荐 `https://x-access-token:<token>@host/...`，用户名固定为
      x-access-token，token 作密码。Actions 的内置 GITHUB_TOKEN 同样适用
      这个形式（它本质也是一个 installation token）。固定写法避免用户名
      写错导致的 403。

    【安全】返回值含明文 token，**只应传给 git 命令行，绝不可打印或落盘**。
    _run() 会在回显时做脱敏（见 _redact）。

    Returns:
        带 token 的远端 URL；GITHUB_TOKEN/GITHUB_PAT 均未设置时返回 None。
    """
    cred = get_credential()
    url = (repo_url or REPO_URL or "").rstrip("/")
    if not cred or not url:
        return None
    if url.startswith("https://"):
        host_path = url[len("https://"):]
    elif url.startswith("http://"):
        host_path = url[len("http://"):]
    else:
        # ssh://git@github.com/... 或 git@github.com:... → 转成 https 形态
        host_path = url.split("://", 1)[-1].split("@")[-1].replace(":", "/", 1)
    return f"https://x-access-token:{cred}@{host_path}"


# 覆盖两种可能出现在回显里的凭据形态：
#   1. URL 内嵌：x-access-token:xxx@host
#   2. 裸 token：ghs_/ghp_/github_pat_ 前缀（git 报错有时会直接回显 token）
_TOKEN_RE = re.compile(
    r"x-access-token:[^@/\s]+|(?:ghs_|ghp_|gho_|github_pat_)[A-Za-z0-9_]+"
)


def _redact(text: str) -> str:
    """把回显文本里的 token 打码，防止泄漏到日志/CI 输出。"""
    out = _TOKEN_RE.sub("x-access-token:***", text or "")
    return out


# 发布到仓库的目录结构
P_CLOUD_LATEST = "cloud/latest.json"
P_CLOUD_HISTORY = "cloud/history/{date}.json"
P_STATUS_LATEST = "status/latest.json"      # 手机拉取的轻量摘要
P_INDEX = "index.html"                      # 看板（另由 dashboard 生成）


def _now() -> datetime:
    return datetime.now(CST)


def _log(msg: str) -> None:
    print(f"[publish] {msg}")


def _run(cmd: List[str], cwd: Optional[str] = None,
         check: bool = True) -> subprocess.CompletedProcess:
    """
    执行命令并回显。失败时抛出并带上 stderr，便于定位（如鉴权过期）。

    【安全】命令与输出都会经 _redact 打码，防止 PAT 随报错泄漏到 CI 日志。
    """
    safe_cmd = [_redact(c) for c in cmd]
    proc = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if check and proc.returncode != 0:
        raise RuntimeError(f"命令失败 {' '.join(safe_cmd)}: "
                           f"{_redact(proc.stderr or proc.stdout or '')[:500]}")
    return proc


# ================================================================ R12b 脱敏
# 白名单：**只保留这些字段**发布到公网。白名单比黑名单安全——
# 新增字段默认被剥离，而不是默认泄漏。
PUBLIC_TARGET_FIELDS = (
    "id", "name", "kind", "scope", "status", "ok",
    "latency_ms", "ttfb_ms", "speed_kbps", "change", "error", "hint",
)

# 兜底黑名单：即使白名单被误改，这些含直链/主机信息的键也必须被剔除。
# 覆盖 R12b 点名的 url/final_url/segment_url 及所有分片候选字段。
SENSITIVE_KEYS = frozenset({
    "url", "final_url", "segment_url",
    "segment_urls", "segments", "candidates", "candidate_urls",
    "variant_url", "variant_urls", "variants", "playlist_url",
    "master_url", "base_url", "redirect_url", "source_url",
    "raw_url", "original_url", "cdn_urls", "urls",
})

# 直链特征串：脱敏后全文 grep 必须 0 命中（R12b 验收标准）
LEAK_MARKERS = ("cdnrrs", "PLTV", "chinamobile", "m3u8?servicetype")


def strip_target(t: Dict[str, Any]) -> Dict[str, Any]:
    """
    对单个目标做白名单脱敏。

    保留判断是「**键在不在白名单**」而非「键敏不敏感」：新增字段默认被剥离。

    Args:
        t: 原始目标 dict。

    Returns:
        仅含 PUBLIC_TARGET_FIELDS 的新 dict（原对象不被修改）。
    """
    out: Dict[str, Any] = {}
    for k in PUBLIC_TARGET_FIELDS:
        if k in t:
            v = t[k]
            # 兜底：即便白名单里的键，值若是 URL 形态也剔除（防上游把直链塞进 name）
            if isinstance(v, str) and ("://" in v or v.startswith("//")):
                continue
            out[k] = v
    return out


def deep_strip(obj: Any, _depth: int = 0) -> Any:
    """
    递归脱敏任意嵌套结构。

    处理顺序：
      1. 命中 SENSITIVE_KEYS 的键 → 整键删除；
      2. 字符串值命中 LEAK_MARKERS → 替换为 "«stripped»"（保留键以维持结构可读）；
      3. list/dict → 递归。

    Args:
        obj: 任意 JSON 可序列化对象。
        _depth: 递归深度保护（防畸形数据导致栈溢出）。

    Returns:
        脱敏后的新对象。
    """
    if _depth > 12:
        return "«depth-limited»"
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if k in SENSITIVE_KEYS:
                continue
            out[k] = deep_strip(v, _depth + 1)
        return out
    if isinstance(obj, (list, tuple)):
        return [deep_strip(v, _depth + 1) for v in obj]
    if isinstance(obj, str):
        low = obj.lower()
        if any(mk.lower() in low for mk in LEAK_MARKERS):
            return "«stripped»"
        return obj
    return obj


def sanitize_report(report: Dict[str, Any]) -> Dict[str, Any]:
    """
    报告脱敏总入口：clonable、幂等、不修改入参。

    对 targets 走白名单（更严），其余结构走 deep_strip（清 meta/dynamic 等
    可能携带直链的角落）。

    Args:
        report: 原始报告 dict。

    Returns:
        可安全发布到公网仓库的 dict。
    """
    out = deep_strip(report)
    if isinstance(out, dict) and isinstance(out.get("targets"), list):
        out["targets"] = [strip_target(t) for t in out["targets"]
                          if isinstance(t, dict)]
    # meta.dynamic.cdn_urls 可能藏在别处，显式再清一遍
    if isinstance(out.get("meta"), dict):
        dyn = out["meta"].get("dynamic")
        if isinstance(dyn, dict) and "cdn_urls" in dyn:
            dyn.pop("cdn_urls", None)
    return out


def assert_no_leak(payload: Any, label: str = "payload") -> None:
    """
    脱敏自检：序列化后 grep 直链特征串，命中即抛异常**阻断发布**。

    这是 R12b 的验收动作——脱敏不是"尽力而为"，而是发布前的硬闸门。
    宁可发布失败，也不泄漏直链。

    Args:
        payload: 待发布对象。
        label: 用于报错定位的名称。

    Raises:
        RuntimeError: 发现残留直链特征。
    """
    blob = json.dumps(payload, ensure_ascii=False)
    hits = [mk for mk in LEAK_MARKERS if mk.lower() in blob.lower()]
    if hits:
        raise RuntimeError(
            f"[脱敏失败] {label} 仍含直链特征 {hits}，已阻断发布。"
            "请检查 sanitize_report 白名单与 SENSITIVE_KEYS。")


# 扫描工作区时跳过的路径：.git 内部对象是压缩的，且本身就是历史，不该重复报警
_SCAN_SKIP_DIRS = {".git", "__pycache__", ".pytest_cache", "node_modules"}
_SCAN_TEXT_EXT = {".json", ".html", ".htm", ".js", ".css", ".txt", ".md", ".m3u", ".m3u8"}
_SCAN_MAX_BYTES = 4 * 1024 * 1024   # 超过 4MB 的文件跳过（避免读大二进制）


def scan_worktree_for_leaks(root: str) -> List[Any]:
    """
    递归扫描工作区文件，返回 [(相对路径, 命中的特征串), ...]。

    【为什么要做文件级扫描，而不只是校验 payload】
      assert_no_leak 只保证"这次写的内容"干净，但发布分支上可能残留：
        - 手工拷贝进来调试的未脱敏报告；
        - 早期版本遗留的 cloud/*.json；
        - 看板 HTML 里内联的样例数据。
      git 一旦提交，直链就永久留在历史里，事后删除也救不回来。因此提交前
      对整个工作区做一次全盘扫描，是最便宜也最可靠的防线。

    Args:
        root: 仓库工作区根目录。

    Returns:
        命中列表；空列表表示干净。
    """
    found: List[Any] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in _SCAN_SKIP_DIRS]
        for fn in filenames:
            ext = os.path.splitext(fn)[1].lower()
            if ext not in _SCAN_TEXT_EXT:
                continue
            full = os.path.join(dirpath, fn)
            try:
                if os.path.getsize(full) > _SCAN_MAX_BYTES:
                    continue
                with open(full, encoding="utf-8", errors="replace") as f:
                    text = f.read()
            except OSError:
                continue
            low = text.lower()
            for mk in LEAK_MARKERS:
                if mk.lower() in low:
                    rel = os.path.relpath(full, root)
                    found.append((rel, mk))
                    break   # 同一文件只报一次，够定位了
    return found


def build_status_summary(merged: Optional[Dict[str, Any]],
                         cloud_report: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    构造手机侧拉取用的**轻量摘要**（数十字节量级）。

    刻意只放「结论」不放明细：
      - 矩阵定性数量（all_pass / source_dead / local_channel_blocked ...）
      - 告警 id 列表
      - 网络状态与内网状态
      - 时间戳
    手机合并时需要的「变化对比」由手机自己的历史完成，云端摘要只提供对照坐标。

    Args:
        merged: 双位置合并结果（可能为 None，如云端未收到本地摘要）。
        cloud_report: 云端单侧报告。

    Returns:
        可直接写入 status/latest.json 的 dict。
    """
    out: Dict[str, Any] = {
        "version": "v4.2",
        "published_at": _now().isoformat(),
        "cloud_timestamp": (cloud_report or {}).get("timestamp"),
        "cloud_ok_stream": ((cloud_report or {}).get("summary") or {}).get("ok_stream"),
        "cloud_total_stream": ((cloud_report or {}).get("summary") or {}).get("total_stream"),
        "cloud_ok_list": ((cloud_report or {}).get("summary") or {}).get("ok_list"),
        "cloud_total_list": ((cloud_report or {}).get("summary") or {}).get("total_list"),
        "verdicts": {},
        "alarms": [],
        "suppressed": [],
        "network_state": None,
        "intranet_status": None,
    }
    if merged:
        s = merged.get("summary") or {}
        out["verdicts"] = s.get("verdicts") or {}
        out["alarms"] = s.get("alarms") or []
        out["suppressed"] = s.get("suppressed") or []
        out["network_state"] = s.get("network_state")
        out["intranet_status"] = ((merged.get("intranet") or {}).get("status"))
    elif cloud_report:
        # 未收到本地摘要：只给云端视角，手机侧自行补齐本地半格
        net = ((cloud_report.get("summary") or {}).get("network") or {})
        out["network_state"] = net.get("state")
    return out


# ================================================================ R12d 试运行仪表
COUNTER_KEYS = ("probe_runs", "alarm_runs", "alarms_total",
                "suppressed_total", "pending_total", "first_run_at")


def load_counters(status_path: str) -> Dict[str, Any]:
    """
    读取上一版累计计数器。缺失或损坏时返回全零初始值。

    计数器只增不减，用于 7 天观察期统计误报/漏报率（分母是 probe_runs）。
    """
    init = {"run_id": 0, "probe_runs": 0, "alarm_runs": 0, "alarms_total": 0,
            "suppressed_total": 0, "pending_total": 0, "first_run_at": None}
    if not os.path.isfile(status_path):
        return init
    try:
        with open(status_path, encoding="utf-8") as f:
            data = json.load(f)
        c = data.get("counters") or {}
        for k in init:
            if k in c and isinstance(c[k], (int, float)):
                init[k] = c[k]
    except Exception as e:
        _log(f"[warn] 计数器读取失败，从零开始：{e}")
    return init


def bump_counters(counters: Dict[str, Any],
                  cloud_report: Optional[Dict[str, Any]],
                  merged: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    本次发布对计数器的增量。

    统计口径（供 7 天观察期算指标）：
      probe_runs      —— 探测轮次（每次发布 +1），所有比率的**分母**
      alarm_runs      —— 有告警的轮次（算误报率时分子）
      alarms_total    —— 告警源累计次数
      suppressed_total—— 被网络基线抑制的次数（**漏报风险来源**，重点观察）
      pending_total   —— 待复核项累计（抑制产生的 pending）

    Args:
        counters: 上一版计数器（load_counters 产出）。
        cloud_report: 已脱敏的云端报告。
        merged: 已脱敏的合并报告，可为 None。

    Returns:
        新的计数器 dict（不修改入参）。
    """
    c = dict(counters)
    c["run_id"] = int(c.get("run_id") or 0) + 1
    c["probe_runs"] = int(c.get("probe_runs") or 0) + 1
    if c.get("first_run_at") is None:
        c["first_run_at"] = _now().isoformat()

    ms = (merged or {}).get("summary") or {}
    alarms = ms.get("alarms") or []
    suppressed = ms.get("suppressed") or []

    if alarms:
        c["alarm_runs"] = int(c.get("alarm_runs") or 0) + 1
    c["alarms_total"] = int(c.get("alarms_total") or 0) + len(alarms)
    c["suppressed_total"] = int(c.get("suppressed_total") or 0) + len(suppressed)
    # pending：被抑制后降级为待复核的项，数量上等同 suppressed
    c["pending_total"] = int(c.get("pending_total") or 0) + len(suppressed)

    # 云端视角的告警也计入（merged 缺失时至少不丢信息）
    if merged is None and cloud_report is not None:
        cs = (cloud_report.get("summary") or {})
        cl_alarms = (cs.get("changes") or {}).get("broken") or []
        if cl_alarms:
            c["alarm_runs"] = int(c.get("alarm_runs") or 0) + 1
        c["alarms_total"] = int(c.get("alarms_total") or 0) + len(cl_alarms)
    return c


def make_daily_summary(report: Dict[str, Any]) -> Dict[str, Any]:
    """
    生成日报用的「本地视角是否缺报」判定（R9 第 3 条）。

    规则：本地摘要文件缺失或时间戳早于 24h → stale=True。
    这条本身就是告警：手机侧探针已停止上报。

    Args:
        report: 云端报告（含 summary.network 等可选字段）。

    Returns:
        {"stale": bool, "last_seen": str|None, "age_hours": float|None, "note": str}
    """
    return {"stale": False, "last_seen": None, "age_hours": None,
            "note": "本地摘要新鲜度需在发布时注入（见 publish 主流程）"}


def check_local_freshness(status_path: str,
                          ttl_hours: int = STATUS_TTL_HOURS) -> Dict[str, Any]:
    """
    检查已发布的本地摘要是否过期。

    Args:
        status_path: 仓库内 status/latest.json 的路径。
        ttl_hours: 过期阈值（小时）。

    Returns:
        {"stale": bool, "last_seen": str|None, "age_hours": float|None, "note": str}
    """
    if not os.path.isfile(status_path):
        return {"stale": True, "last_seen": None, "age_hours": None,
                "note": "从未收到本地摘要：家里视角完全缺报"}
    try:
        with open(status_path, encoding="utf-8") as f:
            data = json.load(f)
        ts = data.get("local_timestamp") or data.get("published_at")
        if not ts:
            return {"stale": True, "last_seen": None, "age_hours": None,
                    "note": "摘要无时间戳，视为缺报"}
        seen = datetime.fromisoformat(ts)
        if seen.tzinfo is None:
            seen = seen.replace(tzinfo=CST)
        age = (_now() - seen).total_seconds() / 3600.0
        stale = age > ttl_hours
        note = (f"家里视角已 {age:.1f}h 未更新（阈值 {ttl_hours}h），"
                f"疑似手机侧探针停摆" if stale else
                f"家里视角 {age:.1f}h 前更新，新鲜")
        return {"stale": stale, "last_seen": ts,
                "age_hours": round(age, 1), "note": note}
    except Exception as e:
        return {"stale": True, "last_seen": None, "age_hours": None,
                "note": f"摘要解析失败：{e}"}


def ensure_orphan_branch(repo_dir: str, remote: Optional[str],
                         branch: str) -> None:
    """
    确保 repo_dir 是一个已切到 `branch`（孤儿分支）的可用仓库。

    【R14 为什么用孤儿分支 gh-pages】
      产物分支只该装产物（index.html + JSON），不该带上代码历史。孤儿分支
      没有与 main 的公共祖先，checkout 后工作区是干净的，天然满足"网页上传
      的代码留在 main、Actions 只往 gh-pages 写产物"的分工。

    【首次创建 vs 已存在】
      - 远端已有该分支 → fetch 后 checkout（保留历史，避免每次重建）
      - 远端没有       → git checkout --orphan，清空工作区后首建

    Args:
        repo_dir: 本地仓库路径（可为空目录；不存在则创建并 init）。
        remote: 远端 URL（带鉴权注入）。
        branch: 目标分支名（gh-pages）。

    Raises:
        RuntimeError: git 操作失败。
    """
    auth_remote = build_auth_remote(remote) or remote

    if not os.path.isdir(os.path.join(repo_dir, ".git")):
        os.makedirs(repo_dir, exist_ok=True)
        _run(["git", "init"], cwd=repo_dir)
        _log(f"已 git init {repo_dir}")
        if auth_remote:
            _run(["git", "remote", "add", "origin", auth_remote], cwd=repo_dir)
            _log(f"已设置 origin = {_redact(auth_remote)}")
    elif auth_remote:
        # 已有仓库：origin 可能是旧的（dashboard 时代的无鉴权地址）→ 重设
        existing = _run(["git", "remote"], cwd=repo_dir, check=False).stdout
        if "origin" in existing.split():
            _run(["git", "remote", "set-url", "origin", auth_remote], cwd=repo_dir)
        else:
            _run(["git", "remote", "add", "origin", auth_remote], cwd=repo_dir)
        _log(f"origin 已更新为 {_redact(auth_remote)}")

    # 身份兜底（CI 里通常没有全局 git config）
    if not _run(["git", "config", "user.email"], cwd=repo_dir,
                check=False).stdout.strip():
        _run(["git", "config", "user.email", "iptv-bot@users.noreply.github.com"],
             cwd=repo_dir)
    if not _run(["git", "config", "user.name"], cwd=repo_dir,
                check=False).stdout.strip():
        _run(["git", "config", "user.name", "iptv-publisher"], cwd=repo_dir)

    # 远端是否已有该分支？有则 fetch 复用，无则建孤儿。
    remote_has_branch = False
    if auth_remote:
        ls = _run(["git", "ls-remote", "--heads", "origin", branch],
                  cwd=repo_dir, check=False)
        remote_has_branch = branch in (ls.stdout or "")

    local_has_branch = branch in _run(
        ["git", "branch", "--list", branch], cwd=repo_dir).stdout

    if remote_has_branch:
        _run(["git", "fetch", "origin", branch], cwd=repo_dir)
        _run(["git", "checkout", "-B", branch, f"origin/{branch}"], cwd=repo_dir)
        _log(f"已从远端检出 {branch}（保留历史）")
    elif local_has_branch:
        _run(["git", "checkout", branch], cwd=repo_dir)
        _log(f"已切换到本地 {branch}")
    else:
        # 孤儿分支：清空 index 与工作区，确保不残留 main 的文件
        _run(["git", "checkout", "--orphan", branch], cwd=repo_dir)
        _run(["git", "rm", "-rf", "--cached", "."], cwd=repo_dir, check=False)
        _log(f"已创建孤儿分支 {branch}（首建）")


def prune_old_history(repo_dir: str, keep_days: int = HISTORY_KEEP_DAYS) -> List[str]:
    """
    删除 cloud/history/ 下超过 keep_days 的归档文件。

    【为什么要删】
      gh-pages 是长期存在的分支，每天一个 JSON 会无限累积。30 天足够画出
      月度趋势，再老的归档价值低、仓库体积代价高。

    Args:
        repo_dir: 仓库根目录。
        keep_days: 保留天数。

    Returns:
        被删除的相对路径列表。
    """
    hist_dir = os.path.join(repo_dir, "cloud", "history")
    if not os.path.isdir(hist_dir):
        return []
    cutoff = (_now().date() - timedelta(days=keep_days))
    removed: List[str] = []
    for fn in sorted(os.listdir(hist_dir)):
        m = re.match(r"^(\d{8})\.json$", fn)
        if not m:
            continue
        try:
            d = datetime.strptime(m.group(1), "%Y%m%d").date()
        except ValueError:
            continue
        if d < cutoff:
            full = os.path.join(hist_dir, fn)
            os.remove(full)
            removed.append(f"cloud/history/{fn}")
    if removed:
        _log(f"已清理 {len(removed)} 个过期归档（保留 {keep_days} 天）："
             f"{', '.join(os.path.basename(p) for p in removed[:5])}"
             + ("…" if len(removed) > 5 else ""))
    return removed


def publish(report_path: str, repo_dir: str, branch: str = BRANCH_DEFAULT,
            remote: Optional[str] = None, do_push: bool = False,
            dry_run: bool = False, merged_path: Optional[str] = None,
            dashboard_html: Optional[str] = None, date: Optional[str] = None) -> Dict[str, Any]:
    """
    发布云端报告到仓库。

    Args:
        report_path: 云端报告 JSON 路径。
        repo_dir: 目标仓库本地路径。
        branch: 发布分支。
        remote: 远端 URL（首次 clone 用）。
        do_push: 是否真正 git push。
        dry_run: 只打印将要写的内容，不落盘不提交。
        merged_path: 可选的合并报告路径（用于生成 status 摘要的结论部分）。
        dashboard_html: 可选的看板 HTML 路径，将覆盖仓库 index.html。
        date: 覆盖日期（YYYYMMDD），默认取 CST 当天。

    Returns:
        {"written": [...], "committed": bool, "pushed": bool, "status": {...}}

    Raises:
        RuntimeError: 仓库操作或 push 失败。
    """
    with open(report_path, encoding="utf-8") as f:
        cloud_report = json.load(f)

    merged = None
    if merged_path and os.path.isfile(merged_path):
        with open(merged_path, encoding="utf-8") as f:
            merged = json.load(f)

    # ---- R12b：发布前强制脱敏 + 自检 ----
    # 顺序很关键：先脱敏、再自检、最后才落盘。自检不过直接抛异常中断发布。
    cloud_public = sanitize_report(cloud_report)
    assert_no_leak(cloud_public, "cloud report")
    if merged is not None:
        merged_public = sanitize_report(merged)
        assert_no_leak(merged_public, "merged report")
    else:
        merged_public = None

    day = date or _now().strftime("%Y%m%d")
    written: List[str] = []

    latest_abs = os.path.join(repo_dir, P_CLOUD_LATEST)
    history_abs = os.path.join(repo_dir, P_CLOUD_HISTORY.format(date=day))
    status_abs = os.path.join(repo_dir, P_STATUS_LATEST)

    # 缺报判定：读**上一版**摘要（写之前），这样本次发布就能带出「上次是什么时候」
    freshness = check_local_freshness(status_abs)

    # 累计计数器：读上一版 → 累加 → 写回（R12d 试运行仪表）
    counters = load_counters(status_abs)
    counters = bump_counters(counters, cloud_public, merged_public)

    status = build_status_summary(merged_public, cloud_public)
    status["local_freshness"] = freshness
    status["counters"] = counters
    status["run_id"] = counters["run_id"]
    # 若本次发布了合并结果，说明本地摘要刚被消费 → 刷新新鲜度坐标
    if merged_public:
        status["local_timestamp"] = merged_public.get("timestamp") or _now().isoformat()
    assert_no_leak(status, "status summary")

    if dry_run:
        _log(f"[dry-run] would write {P_CLOUD_LATEST} "
             f"({len(json.dumps(cloud_public))}B, 已脱敏)")
        _log(f"[dry-run] would write {P_CLOUD_HISTORY.format(date=day)}")
        _log(f"[dry-run] would write {P_STATUS_LATEST}: "
             f"{json.dumps(status, ensure_ascii=False)}")
        _log(f"[dry-run] local freshness: {freshness['note']}")
        _log(f"[dry-run] run_id={counters['run_id']} counters={counters}")
        return {"written": [], "committed": False, "pushed": False,
                "status": status, "freshness": freshness, "counters": counters}

    # R14：切到 gh-pages 孤儿分支（远端已有则复用，无则首建）
    ensure_orphan_branch(repo_dir, remote, branch)

    for path, payload, label in (
        (latest_abs, cloud_public, P_CLOUD_LATEST),
        (history_abs, cloud_public, P_CLOUD_HISTORY.format(date=day)),
        (status_abs, status, P_STATUS_LATEST),
    ):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        written.append(label)
        _log(f"写入 {label}")

    if dashboard_html and os.path.isfile(dashboard_html):
        shutil.copyfile(dashboard_html, os.path.join(repo_dir, P_INDEX))
        written.append(P_INDEX)
        _log(f"写入 {P_INDEX}")

    # R14：清理超期归档，控制孤儿分支体积
    prune_old_history(repo_dir)

    # ---- R12b 最后一道闸门：对整个工作区做文件级泄漏扫描 ----
    # 上面 assert_no_leak 校验的是「将要写入的 payload」，但磁盘上可能残留
    # 历史产物（如手工拷贝的 debug json、旧版未脱敏报告）。提交前全盘扫一遍，
    # 命中即中止 —— 宁可发布失败，也不让直链进 git 历史。
    # R14 起这道闸门位于 push gh-pages **之前**，语义不变。
    leaks = scan_worktree_for_leaks(repo_dir)
    if leaks:
        detail = "\n".join(f"  {p}: 命中 {mk}" for p, mk in leaks[:20])
        raise RuntimeError(
            f"R12b 脱敏闸门拦截：工作区存在 {len(leaks)} 处直链特征串，已中止提交。\n"
            f"{detail}\n"
            "请清理上述文件后重试（它们不应出现在发布分支）。")

    _run(["git", "add", "-A"], cwd=repo_dir)
    diff = _run(["git", "diff", "--cached", "--quiet"], cwd=repo_dir, check=False)
    committed = False
    if diff.returncode != 0:
        # R14：message 带 [skip ci]（三重保险之一，防止产物 push 触发新 workflow）
        msg = (f"cloud report {day} "
               f"(local_freshness={'stale' if freshness['stale'] else 'fresh'}, "
               f"run_id={counters['run_id']}) {SKIP_CI_SUFFIX}")
        _run(["git", "commit", "-m", msg], cwd=repo_dir)
        committed = True
        _log(f"已提交：{msg}")
    else:
        _log("无变更，跳过提交")

    pushed = False
    if do_push and committed:
        _run(["git", "push", "origin", f"HEAD:refs/heads/{branch}"], cwd=repo_dir)
        pushed = True
        _log(f"已推送 origin/{branch}")
    elif do_push:
        _log("无新提交，跳过推送")

    return {"written": written, "committed": committed, "pushed": pushed,
            "status": status, "freshness": freshness, "counters": counters,
            "branch": branch}


def main() -> int:
    ap = argparse.ArgumentParser(
        description="云端报告发布器（R14：Actions 日更，产物推 gh-pages；"
                    "R12b 强制脱敏；R12d 仪表）")
    ap.add_argument("--report", required=True, help="云端报告 JSON 路径")
    ap.add_argument("--merged", default=None, help="合并报告 JSON（可选，用于结论摘要）")
    ap.add_argument("--repo", default=os.path.expanduser("~/iptv-dashboard"),
                    help="目标仓库本地路径")
    ap.add_argument("--remote", default=REPO_URL,
                    help=f"远端 URL（默认取 $IPTV_REPO_URL，当前 {REPO_URL}）")
    ap.add_argument("--branch", default=BRANCH_DEFAULT,
                    help=f"发布分支（默认取 $IPTV_BRANCH，当前 {BRANCH_DEFAULT}）")
    ap.add_argument("--dashboard-html", default=None, help="看板 HTML 路径（覆盖 index.html）")
    ap.add_argument("--date", default=None, help="覆盖日期 YYYYMMDD")
    ap.add_argument("--push", action="store_true",
                    help="真正推送到远端（需 GITHUB_TOKEN 或 GITHUB_PAT）")
    ap.add_argument("--dry-run", action="store_true", help="只打印计划，不落盘")
    args = ap.parse_args()

    if not os.path.isfile(args.report):
        print(f"错误：报告不存在 {args.report}", file=sys.stderr)
        return 2

    # push 前置检查：凭据缺失时**早失败**，而不是等到 git push 报 403
    if args.push and not get_credential():
        print("[!] 需要推送但未设置凭据。\n"
              "    GitHub Actions 环境：由 workflow 的 permissions 自动注入 GITHUB_TOKEN，"
              "无需手动设置。\n"
              "    本地/自建 cron   ：export GITHUB_PAT=github_pat_xxx\n"
              "    （PAT 需要对该仓库有 Contents: Read and write 权限）",
              file=sys.stderr)
        return 2

    try:
        res = publish(args.report, args.repo, args.branch, args.remote,
                      args.push, args.dry_run, args.merged,
                      args.dashboard_html, args.date)
    except RuntimeError as e:
        print(f"[!] 发布失败：{e}", file=sys.stderr)
        return 1

    print(f"[*] 完成：写入 {len(res['written'])} 个文件，"
          f"提交={res['committed']}，推送={res['pushed']}")
    print(f"[*] run_id={res['counters']['run_id']} "
          f"probe_runs={res['counters']['probe_runs']} "
          f"alarm_runs={res['counters']['alarm_runs']} "
          f"suppressed_total={res['counters']['suppressed_total']} "
          f"pending_total={res['counters']['pending_total']}")
    if res["freshness"]["stale"]:
        print(f"[!] 家里视角缺报：{res['freshness']['note']}")
    print(f"[*] 看板地址：{DEFAULT_PAGES_URL}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
