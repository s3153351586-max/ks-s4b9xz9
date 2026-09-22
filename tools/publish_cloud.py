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
from typing import Any, Dict, Iterator, List, Optional

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


# ============================================================ R16 通知状态机
#
# 【为什么需要「上次通知状态」】
#   云端每天 08:00 跑一次。若每次「当前有告警」就推一条，一个持续 3 天的故障
#   会推 3 条一模一样的消息 —— 这正是「狼来了」噪声的来源，推得多了真告警
#   也没人看。正确的语义是**只在状态翻转时通知**：
#
#     无告警 → 有告警   推「🔴 告警」（故障开始，此刻才知道）
#     有告警 → 无告警   推「✅ 恢复」（恢复也是重要信息，否则不知道该不该看电视）
#     有告警 → 有告警   **静默**（还没修好，重复推送只会让人麻木）
#     无告警 → 无告警   静默
#
#   注意：静默的是「推送」，不是「记录」——看板与 status/latest.json 每天都更新，
#   想看当前状态随时可查。推送只负责「变化」。
NOTIFY_OK = "ok"
NOTIFY_ALARM = "alarm"
VALID_NOTIFY_STATES = (NOTIFY_OK, NOTIFY_ALARM)

# 网络状态取值（与 probe_v4 保持一致；此处只用到 UNKNOWN 作缺省）
#
# 【为什么 publish_cloud 需要这个常量】
#   R18 收口修正了一个语义误报：云端**无权**判定"家里网络状态"。
#   `status/latest.json` 的 network_state 唯一合法来源是**本地摘要**；
#   没有本地摘要（缺报）时必须是 `net_unknown`，而不是回落到云端自己
#   baseline 组的结论（那必然是 lan_down —— 云端网关 192.168.1.1 打不通）。
NET_UNKNOWN = "net_unknown"

# 通知状态存放位置：孤儿分支上的一个单行文本文件。
#
# 【为什么不塞进 status/latest.json】
#   status/latest.json 是**手机侧**读的摘要，往里面塞云端自己的推送簿记属于
#   职责污染；而且它每次都整体重写，一旦被别的流程覆盖就丢标记 → 重复推送。
#   独立文件只有 2 字节，每天最多写一次，语义清晰、互不干扰。
P_NOTIFY_STATE = "status/notify_state.txt"


def read_notify_state(repo_dir: str) -> Optional[str]:
    """
    读取上一轮的通知状态（ok / alarm），`None` 表示从未运行过。

    Args:
        repo_dir: 仓库工作区根目录（应为已 checkout 出来的产物分支）。

    Returns:
        "ok" | "alarm" | None。
    """
    p = os.path.join(repo_dir, P_NOTIFY_STATE)
    if not os.path.isfile(p):
        return None
    try:
        with open(p, encoding="utf-8") as f:
            v = f.read().strip()
        return v if v in VALID_NOTIFY_STATES else None
    except OSError:
        return None


def write_notify_state(repo_dir: str, state: str) -> None:
    """
    落盘本轮通知状态。

    Args:
        repo_dir: 仓库工作区根目录。
        state: "ok" | "alarm"。

    Raises:
        ValueError: state 不在合法取值内（防止把 None 写成 "None" 造成状态漂移）。
    """
    if state not in VALID_NOTIFY_STATES:
        raise ValueError(f"非法通知状态：{state!r}")
    p = os.path.join(repo_dir, P_NOTIFY_STATE)
    os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        f.write(state + "\n")


# 状态文件回读失败的哨兵：与「首次运行」语义不同，需要保守处理
def decide_notify(prev_state: Optional[str], cur_alarm: bool,
                  forced: bool = False) -> Dict[str, Any]:
    """
    通知状态机：决定本轮要不要推、推什么语义。

    Args:
        prev_state: 上轮状态（read_notify_state 返回值）；None 表示首次运行。
        cur_alarm: 本轮是否存在告警。
        forced: 早报模式（R16-5）或 --force-notify，绕过状态机直接推。

    Returns:
        {"send": bool, "kind": "first_alarm"|"recover"|"daily"|"none",
         "state": "ok"|"alarm", "note": str}
    """
    cur = NOTIFY_ALARM if cur_alarm else NOTIFY_OK

    if forced:
        return {"send": True, "kind": "daily", "state": cur,
                "note": "强制推送（早报 / --force-notify），绕过状态机"}

    if prev_state is None:
        # 首次运行且当前无告警 → 不推。否则每一次重新部署都会先来一条"正常"，
        # 属于纯噪声（正常是默认状态，不需要被通知）。
        if cur_alarm:
            return {"send": True, "kind": "first_alarm", "state": cur,
                    "note": "首次运行即发现告警"}
        return {"send": False, "kind": "none", "state": cur,
                "note": "首次运行且无告警，静默（正常是默认状态，不推）"}

    if prev_state == NOTIFY_ALARM and cur == NOTIFY_OK:
        return {"send": True, "kind": "recover", "state": cur,
                "note": "告警已恢复"}
    if prev_state == NOTIFY_OK and cur == NOTIFY_ALARM:
        return {"send": True, "kind": "first_alarm", "state": cur,
                "note": "新出现告警（上轮正常）"}
    if cur == NOTIFY_ALARM:
        return {"send": False, "kind": "none", "state": cur,
                "note": "告警持续中，上轮已推过，静默以免狼来了"}
    return {"send": False, "kind": "none", "state": cur,
            "note": "持续正常，静默"}


def extract_push_content(status: Dict[str, Any],
                         local_rep: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    从已脱敏的 status 摘要里提炼推送所需的原始结论。

    【为什么在这里提炼而不是在原库里】
      家侧（probe_local / merge_on_phone）在**手机上**，云端 Actions 里根本没有
      这两个模块。云端能拿到的只有自己发布的 status/latest.json —— 所以只能
      在这里把「结论」还原出来。文案生成逻辑本身复用 merge_on_phone 里那套措辞
      风格（大白话定性 + 处置建议），见 notify_push.py。

    Args:
        status: build_status_summary 的产物（已脱敏）。
        local_rep: 家侧本地报告；云端恒为 None（家侧已降级为人肉确认，R16-3）。

    Returns:
        {"alarms": [...], "verdicts": {...}, "network_state": str|None,
         "freshness": {...}, "targets": [{"id","name","ok","error"}...],
         "counters": {...}, "run_id": int, "cloud": {...}}
    """
    alarms = list(status.get("alarms") or [])
    net_state = status.get("network_state")

    # 网络基线故障时的告警已在 merge 侧归并为 NETWORK，这里对齐一次，
    # 避免云端 status 来自旧版数据时漏掉网络类告警。
    if net_state in ("lan_down", "wan_down") and "NETWORK" not in alarms:
        alarms.insert(0, "NETWORK")

    return {
        "alarms": alarms,
        "verdicts": dict(status.get("verdicts") or {}),
        "suppressed": list(status.get("suppressed") or []),
        "network_state": net_state,
        "intranet_status": status.get("intranet_status"),
        "freshness": dict(status.get("local_freshness") or {}),
        "counters": dict(status.get("counters") or {}),
        "run_id": status.get("run_id"),
        "cloud": {
            "ok_stream": status.get("cloud_ok_stream"),
            "total_stream": status.get("cloud_total_stream"),
            "ok_list": status.get("cloud_ok_list"),
            "total_list": status.get("cloud_total_list"),
            "timestamp": status.get("cloud_timestamp"),
        },
    }


def record_push_result(status: Dict[str, Any], ok: bool,
                       error: Optional[str] = None) -> Dict[str, Any]:
    """
    把推送结果写进 status 的累计计数器（R16-4：失败计入 counters）。

    设计上**只增不减**，与 R12d 其余计数器口径一致，便于 7 天观察期统计
    「推送成功率」。失败时额外写 push_last_error，供人工排查。

    Args:
        status: 待写入 status/latest.json 的 dict（原地修改 counters 字段）。
        ok: 推送是否成功。
        error: 失败原因（已脱敏）。

    Returns:
        同一个 status 对象（便于链式调用）。
    """
    c = status.setdefault("counters", {})
    if ok:
        c["push_successes"] = int(c.get("push_successes") or 0) + 1
        c["push_last_error"] = None
    else:
        c["push_failures"] = int(c.get("push_failures") or 0) + 1
        c["push_last_error"] = (error or "unknown")[:200]
    return status


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
# R18：EPG 对比产物（masked，可公开）
P_EPG_DIFF_LATEST = "epg/diff_latest.json"
P_EPG_DIFF_HISTORY = "epg/history/diff_{date}.json"

# EPG 对比产物的保留天数（与 cloud/history 同策略，独立配置便于单独调整）
EPG_HISTORY_KEEP_DAYS = 30


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


# ================================================================ R18 EPG 脱敏
# 【为什么需要一整套新闸门】
#   EPG 端点的域名（gxtvepg.taipan.jda.bcs.ottcn.com）**不含任何 LEAK_MARKERS**：
#   现有三道闸门（deep_strip / assert_no_leak / scan_worktree_for_leaks）**全都拦不住它**。
#   若不加防护，一旦有人把域名或直链写进 EPG 产物，会**静默进 git 历史**。
#
# 【为什么不干脆把 ottcn 加进 LEAK_MARKERS】
#   因为 `EPG-API-NOTES.md` 是**故意公开**的文档 —— 它就该写这个域名。
#   加进 LEAK_MARKERS 会让文档扫描直接红，与本项目"公开文档可写形态"的设计冲突。
#   所以本套闸门**只作用于产物**（payload 级），不参与源码/文档扫描。

# 产物中禁止出现的主机/路径特征串（只用于校验产物，不加入 LEAK_MARKERS）
EPG_FORBIDDEN_MARKERS = (
    "ottcn", "taipan", "ysten", "bcs.",          # 新 EPG 端点族特征
    "gxtvepg", "looktvepg", "jda",               # 具体端点/节点名
    ".shtml",                                     # 接口路径特征
)

# 产物中禁止出现的键（出现即视为有人把原始数据塞进来了）
EPG_FORBIDDEN_KEYS = frozenset({
    "url", "urls", "cdn_urls", "epg_urls", "healer_urls",
    "sample_names", "names", "name_by_url", "names_by_url",
    "channel_names",
})

# EPG 对比产物的**白名单**键：只保留这些。
# 与 targets 的 PUBLIC_TARGET_FIELDS 同理 —— 白名单比黑名单安全，
# 新增字段默认被剥离，而不是"记得排除"。
EPG_PUBLIC_KEYS = frozenset({
    "version", "generated_at", "normalization_rule",
    "epg_stage", "epg_ok", "healer_fetch_ok",
    "epg_count", "healer_count", "intersect",
    "epg_only_count", "healer_only_count", "union_count", "jaccard",
    "placeholder_count", "multi_name_groups", "raw_channel_count",
    "channel_names_top", "baseline", "drift", "meta",
})


def _walk_keys(obj: Any) -> Iterator[str]:
    """递归产出所有 dict 的键名（用于禁用键检查）。"""
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield str(k)
            yield from _walk_keys(v)
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            yield from _walk_keys(v)


def sanitize_epg_diff(diff: Any) -> Dict[str, Any]:
    """
    EPG 对比产物的脱敏总入口：**白名单**裁剪 + 现有 deep_strip 兜底。

    Args:
        diff: 原始 diff 报告（可能来自 epg_diff.build_epg_diff）。

    Returns:
        可安全发布的 dict；非 dict 输入返回空 dict。
    """
    if not isinstance(diff, dict):
        return {}
    # 第一层：白名单
    out = {k: v for k, v in diff.items() if k in EPG_PUBLIC_KEYS}
    # 第二层：现有黑名单兜底（清 url/cdn_urls 等键 + 特征串）
    out = deep_strip(out)
    # channel_names_top 默认应为空 —— 万一上游开了开关，这里强制清掉。
    # （保守优先：漂移预警不需要频道名，能不给就不给。）
    if isinstance(out.get("channel_names_top"), list):
        out["channel_names_top"] = []
    return out


def assert_epg_masked(payload: Any, label: str = "epg diff") -> None:
    """
    EPG 产物脱敏自检（**硬闸门**）：命中禁用键或主机特征串即抛异常阻断发布。

    这是 R18 的核心防线 —— 现有 LEAK_MARKERS 对 EPG 域名无效，
    必须有一道专门针对产区端点的检查。

    Args:
        payload: 待发布的 EPG 产物。
        label: 报错定位用名称。

    Raises:
        RuntimeError: 发现禁用键或主机特征串。
    """
    bad_keys = sorted({k for k in _walk_keys(payload) if k in EPG_FORBIDDEN_KEYS})
    if bad_keys:
        raise RuntimeError(
            f"[EPG 脱敏失败] {label} 含禁用键 {bad_keys}，已阻断发布。"
            "对比产物只应含计数与比率，不含任何 URL/名单。")

    blob = json.dumps(payload, ensure_ascii=False)
    low = blob.lower()
    hits = [mk for mk in EPG_FORBIDDEN_MARKERS if mk.lower() in low]
    if hits:
        raise RuntimeError(
            f"[EPG 脱敏失败] {label} 含端点特征串 {hits}，已阻断发布。"
            "检查 sanitize_epg_diff 白名单是否被绕过。")



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
                         cloud_report: Optional[Dict[str, Any]],
                         epg_diff: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    构造手机侧拉取用的**轻量摘要**（数十字节量级）。

    刻意只放「结论」不放明细：
      - 矩阵定性数量（all_pass / source_dead / local_channel_blocked ...）
      - 告警 id 列表
      - 网络状态与内网状态
      - R18：EPG 漂移状态（上游死亡预警）
      - 时间戳
    手机合并时需要的「变化对比」由手机自己的历史完成，云端摘要只提供对照坐标。

    【R18 兼容性】只**加键**不删键 —— 手机侧的解析是宽容的（按需要取键），
    新增字段不会让它崩，但删键会。

    Args:
        merged: 双位置合并结果（可能为 None，如云端未收到本地摘要）。
        cloud_report: 云端单侧报告。
        epg_diff: R18 可选的 EPG 对比产物（**已脱敏**）。

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
        # R18：EPG 漂移摘要（只放结论与计数，明细在 epg/diff_latest.json）
        "epg_drift": None,
    }
    if merged:
        s = merged.get("summary") or {}
        out["verdicts"] = s.get("verdicts") or {}
        out["alarms"] = s.get("alarms") or []
        out["suppressed"] = s.get("suppressed") or []
        out["network_state"] = s.get("network_state") or NET_UNKNOWN
        out["intranet_status"] = ((merged.get("intranet") or {}).get("status"))
    else:
        # 未收到本地摘要：network_state **只能是 net_unknown**。
        # 【为什么不从 cloud_report 取（曾是 bug）】
        #   旧代码在这里写 `net.get("state")`，把云端报告里 baseline 组的
        #   判定当成"家里网络状态"。但云端 runner 在 GitHub 机房，N1 打的是
        #   `192.168.1.1` —— 必然 timeout，必然 lan_down，于是看板渲染出
        #   "家里网络断了"。云端根本没有"家里的网络"，这个字段它无权置喙。
        #   现在 cloud 已不探测 baseline 组，但这里仍显式写死 net_unknown，
        #   防止将来有人把 baseline 加回去时**又**悄悄复活这个误报。
        out["network_state"] = NET_UNKNOWN

    # R18：EPG 漂移结论。
    # 【为什么把 R-a/R-b 推进 alarms】它们是"上游可能已死"的实质性告警，
    #   应走与源告警同一条通知链路（R16 状态机已在上面消费 out["alarms"] 前
    #   计算过 cur_alarm，所以这里必须在那个位置之前完成 —— 见 publish() 的调用顺序）。
    if isinstance(epg_diff, dict):
        drift = epg_diff.get("drift") or {}
        base = epg_diff.get("baseline") or {}
        out["epg_drift"] = {
            "epg_count": epg_diff.get("epg_count"),
            "healer_count": epg_diff.get("healer_count"),
            "intersect": epg_diff.get("intersect"),
            "jaccard": epg_diff.get("jaccard"),
            "worst": drift.get("worst"),
            "warmup": base.get("warmup"),
            "rules": {k: bool(drift.get(k)) for k in ("R-a", "R-b", "R-c", "R-d")},
        }
        if drift.get("R-b"):
            out["alarms"].append("EPG-R-b")
        if drift.get("R-a"):
            out["alarms"].append("EPG-R-a")
    return out


# ================================================================ R12d 试运行仪表
#
# R16 追加两个推送相关计数器：
#   push_successes —— 成功推送次数（含告警/恢复/早报）
#   push_failures  —— 推送失败次数（R16-4：失败不阻断发布，但必须留痕）
#   push_last_error—— 最近一次失败原因（成功时清空），便于事后定位
COUNTER_KEYS = ("probe_runs", "alarm_runs", "alarms_total",
                "suppressed_total", "pending_total", "first_run_at",
                "push_successes", "push_failures", "push_last_error")


def load_counters(status_path: str) -> Dict[str, Any]:
    """
    读取上一版累计计数器。缺失或损坏时返回全零初始值。

    计数器只增不减，用于 7 天观察期统计误报/漏报率（分母是 probe_runs）。
    """
    init = {"run_id": 0, "probe_runs": 0, "alarm_runs": 0, "alarms_total": 0,
            "suppressed_total": 0, "pending_total": 0, "first_run_at": None,
            "push_successes": 0, "push_failures": 0, "push_last_error": None}
    if not os.path.isfile(status_path):
        return init
    try:
        with open(status_path, encoding="utf-8") as f:
            data = json.load(f)
        c = data.get("counters") or {}
        for k in init:
            if k not in c:
                continue
            # push_last_error 是字符串，其余是数值 —— 分开判定，否则字符串
            # 计数会被静默丢弃（isinstance(v, (int,float)) 对 str 为 False）。
            if k == "push_last_error":
                if isinstance(c[k], str) or c[k] is None:
                    init[k] = c[k]
            elif isinstance(c[k], (int, float)):
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
      push_*          —— R16 推送计数由 record_push_result 单独维护，此处只透传

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


def _prune_history_dir(hist_dir: str, keep_days: int,
                       pattern: str = r"^(?:diff_)?(\d{8})\.json$") -> List[str]:
    """
    通用归档清理：删除 hist_dir 下超过 keep_days 的 {date}.json / diff_{date}.json。

    Args:
        hist_dir: 归档目录。
        keep_days: 保留天数。
        pattern: 文件名正则，需含一个 8 位日期捕获组。

    Returns:
        被删除的文件名列表。
    """
    if not os.path.isdir(hist_dir):
        return []
    cutoff = (_now().date() - timedelta(days=keep_days))
    removed: List[str] = []
    for fn in sorted(os.listdir(hist_dir)):
        m = re.match(pattern, fn)
        if not m:
            continue
        try:
            d = datetime.strptime(m.group(1), "%Y%m%d").date()
        except ValueError:
            continue
        if d < cutoff:
            try:
                os.remove(os.path.join(hist_dir, fn))
            except OSError:
                continue
            removed.append(fn)
    return removed


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
            dashboard_html: Optional[str] = None, date: Optional[str] = None,
            force_notify: bool = False,
            emit_notify_marker: Optional[str] = None,
            epg_diff_path: Optional[str] = None) -> Dict[str, Any]:
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
        force_notify: R16-5 早报开关。True 时绕过通知状态机，无条件推一条
            （内容 = 昨日/当前看板摘要）。
        emit_notify_marker: R16。给定时，若通知状态机判定「本轮要推」，就把
            状态机结论写成该路径的 JSON 标记文件。workflow 用这个文件判断
            要不要执行推送步骤 —— 让「判断」留在 Python 里（可测试），
            而不是散落到 YAML 的 shell 条件里。
        epg_diff_path: R18。可选的 EPG 对比报告路径（`epg_diff.py` 的产物）。
            **注意：该参数必须放在参数表末尾** —— 调用方（main）是全位置传参，
            插在中间会让后续参数错位，属静默数据损坏级事故。

    Returns:
        {"written": [...], "committed": bool, "pushed": bool, "status": {...},
         "notify": {...}, "notify_state": str}

    Raises:
        RuntimeError: 仓库操作或 push 失败；或脱敏自检失败。
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

    # ---- R18：EPG 对比产物脱敏（独立闸门）----
    # 现有 LEAK_MARKERS 对 EPG 端点域名无效，所以这里走一套专门的白名单 + 特征串检查。
    # 顺序同报告：先脱敏 → 再自检 → 最后落盘；自检不过抛异常中断发布。
    epg_raw = None
    if epg_diff_path and os.path.isfile(epg_diff_path):
        try:
            with open(epg_diff_path, encoding="utf-8") as f:
                epg_raw = json.load(f)
        except (OSError, ValueError) as e:
            _log(f"[!] EPG 对比报告读取失败，跳过：{e}")
            epg_raw = None
    epg_public = sanitize_epg_diff(epg_raw) if isinstance(epg_raw, dict) else None
    if epg_public is not None:
        assert_no_leak(epg_public, "epg diff")
        assert_epg_masked(epg_public, "epg diff")

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

    status = build_status_summary(merged_public, cloud_public,
                                  epg_diff=epg_public)
    status["local_freshness"] = freshness
    status["counters"] = counters
    status["run_id"] = counters["run_id"]
    # 若本次发布了合并结果，说明本地摘要刚被消费 → 刷新新鲜度坐标
    if merged_public:
        status["local_timestamp"] = merged_public.get("timestamp") or _now().isoformat()
    assert_no_leak(status, "status summary")

    # ---- R16：通知状态机 ----
    # 必须在 ensure_orphan_branch **之后**才能读上轮状态（状态文件在产物分支上）。
    # dry-run 分支不读也无所谓，那里只是预演。
    prev_notify = read_notify_state(repo_dir) if not dry_run else None
    cur_alarm = bool(status.get("alarms"))
    notify = decide_notify(prev_notify, cur_alarm, forced=force_notify)
    status["notify"] = {
        "prev_state": prev_notify,
        "state": notify["state"],
        "sent": notify["send"],
        "kind": notify["kind"],
    }

    if dry_run:
        _log(f"[dry-run] would write {P_CLOUD_LATEST} "
             f"({len(json.dumps(cloud_public))}B, 已脱敏)")
        _log(f"[dry-run] would write {P_CLOUD_HISTORY.format(date=day)}")
        _log(f"[dry-run] would write {P_STATUS_LATEST}: "
             f"{json.dumps(status, ensure_ascii=False)}")
        _log(f"[dry-run] local freshness: {freshness['note']}")
        _log(f"[dry-run] run_id={counters['run_id']} counters={counters}")
        _log(f"[dry-run] 通知决策：send={notify['send']} kind={notify['kind']} "
             f"— {notify['note']}（上轮 {prev_notify}）")
        return {"written": [], "committed": False, "pushed": False,
                "status": status, "freshness": freshness, "counters": counters,
                "notify": notify, "notify_state": prev_notify}

    # R14：切到 gh-pages 孤儿分支（远端已有则复用，无则首建）
    ensure_orphan_branch(repo_dir, remote, branch)

    # 切分支后再读一次：上面的 dry-run 预演不能代表真分支上的状态
    prev_notify = read_notify_state(repo_dir)
    notify = decide_notify(prev_notify, cur_alarm, forced=force_notify)
    status["notify"] = {
        "prev_state": prev_notify,
        "state": notify["state"],
        "sent": notify["send"],
        "kind": notify["kind"],
    }
    _log(f"通知决策：send={notify['send']} kind={notify['kind']} "
         f"— {notify['note']}（上轮 {prev_notify}）")

    # R18：EPG 对比产物（可能为 None —— 例如 workflow 未提供该步骤）。
    # 【必须过滤 None】否则 json.dump(None) 会产出内容为 "null" 的文件，
    # 而且它会**进 git 历史**，让看板与手机侧读到 null 后解析失败。
    _write_items = [
        (latest_abs, cloud_public, P_CLOUD_LATEST),
        (history_abs, cloud_public, P_CLOUD_HISTORY.format(date=day)),
        (status_abs, status, P_STATUS_LATEST),
    ]
    if epg_public is not None:
        _write_items.append((
            os.path.join(repo_dir, P_EPG_DIFF_LATEST),
            epg_public, P_EPG_DIFF_LATEST))
        _write_items.append((
            os.path.join(repo_dir, P_EPG_DIFF_HISTORY.format(date=day)),
            epg_public, P_EPG_DIFF_HISTORY.format(date=day)))

    for path, payload, label in _write_items:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        written.append(label)
        _log(f"写入 {label}")

    # R18：清理过期的 EPG 历史（与 cloud/history 同策略，独立保留天数）
    #   显式传 pattern（而非依赖默认值）：EPG 归档名是 diff_YYYYMMDD.json，
    #   与 cloud/history 的 YYYYMMDD.json 不同。写清楚可防将来有人把两个
    #   目录搞混 —— 默认 pattern 是宽松的 `(?:diff_)?`，只靠目录隔离不够稳。
    _epg_hist_dir = os.path.join(repo_dir, os.path.dirname(
        P_EPG_DIFF_HISTORY.format(date=day)))
    _epg_removed = _prune_history_dir(_epg_hist_dir, EPG_HISTORY_KEEP_DAYS,
                                      pattern=r"^diff_(\d{8})\.json$")
    if _epg_removed:
        _log(f"已清理 {len(_epg_removed)} 个过期 EPG 归档"
             f"（保留 {EPG_HISTORY_KEEP_DAYS} 天）")

    # R16：把本轮告警状态落盘，下一轮据此判断「是否翻转」。
    # 只推送给定的状态而不是「有告警就推」，是整个降噪设计的关键一步。
    write_notify_state(repo_dir, notify["state"])
    written.append(P_NOTIFY_STATE)
    _log(f"写入 {P_NOTIFY_STATE}（{notify['state']}）")

    # R16：给 workflow 落一个「本轮该推送」的标记文件。
    # 写**主机文件系统**而不是仓库工作区 —— 它是一次运行的临时信号，
    # 绝不该进 git（否则会污染产物分支，还会被下一次运行误读）。
    if emit_notify_marker and notify["send"]:
        os.makedirs(os.path.dirname(emit_notify_marker) or ".", exist_ok=True)
        with open(emit_notify_marker, "w", encoding="utf-8") as f:
            json.dump({"send": True, "kind": notify["kind"],
                       "state": notify["state"], "run_id": counters["run_id"]},
                      f, ensure_ascii=False)
        _log(f"已落推送标记 {emit_notify_marker}（kind={notify['kind']}）")

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
            "branch": branch, "notify": notify, "notify_state": prev_notify}


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
    ap.add_argument("--force-notify", action="store_true",
                    help="R16：绕过通知状态机无条件推一条（早报模式）")
    ap.add_argument("--emit-notify-marker", default=None, metavar="PATH",
                    help="R16：本轮判定需要推送时，把结论写到该路径（供 workflow 判断）")
    ap.add_argument("--epg-diff", default=None, metavar="PATH",
                    help="R18：EPG 对比报告路径（epg_diff.py 产物，masked）。"
                         "给定时会脱敏后发布到 epg/ 目录；缺省则跳过该产物")
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
                      args.dashboard_html, args.date, args.force_notify,
                      args.emit_notify_marker, args.epg_diff)
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
    _nt = res.get("notify") or {}
    print(f"[*] 通知决策：send={_nt.get('send')} kind={_nt.get('kind')} "
          f"— {_nt.get('note')}")
    if res["freshness"]["stale"]:
        print(f"[!] 家里视角缺报：{res['freshness']['note']}")
    print(f"[*] 看板地址：{DEFAULT_PAGES_URL}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
