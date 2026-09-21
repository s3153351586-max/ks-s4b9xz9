#！/usr/bin/env python3
#-*-编码：utf-8-*-
"""
云端报告发布器（R9：链路方向反转）
====================================
设计动机
--------
旧链路是“本地→云端”：手机把local.json上传到云端，云端合并。
问题：手机在内网、云端在公网，**上传方向要打通入站网络**（端口/NAT/鉴权），
而手机侧恰恰是最不方便开入站的一端。

R9 把方向反过来：**云端发布、手机拉取合并**。
1.云端探测publicource→写'cloud/latest.JSON'+'cloud/history/YYYYMMDD.约翰逊
+'cloud/status/latest.json'（供手机拉取的轻量摘要）
2.手机'probe_local'出'local.json'→GET云端'status/latest.json'
→本地合并→出结论→里卡胡卜推送
3. 云端日报**降级**：不再承担「合并定性」职责，只做公开视角归档 + 网页数据源
  4. 本地摘要若超过 24h 未更新，云端日报标注「家里视角缺报」——
**这条标注本身就是告警：手机侧探针挂了**

为什么用 git 分支而不是 API 上传
--------------------------------
GitHub页面只能从仓库分支发布，所以"推送"和"发布"共用一次git推送即可，
零服务器、零成本、零额外凭据（复用已有的 gh 登录态）。

敏感度
------
本仓库只含**公网源探测结果**（公开源的可用性/延迟/码率），不含任何内网地址、
账号、密钥。因此可安全使用公开仓库。唯一要求：**仓库名不可被轻易猜中**，
否则等于公开你的源清单（见部署说明）。

用法：
python3publish_cloud.py-report/tmp/iptv_20250101。JSON\\
--repo~/iptv-仪表板--分行仪表盘--推送
python3publish_cloud.py--报告r.json----------------------------------------------------------------------------------------------只打印不落盘
"""

进口argparse
进口JSON
进口操作系统
进口再
进口shutil
进口子流程
进口sys
从……起datetime进口datetime，timedelta，timezone
从……起打字进口任意、口述、列表、可选

CST=时区(定时δ(小时=8))  #中国标准时间，避免云端UTC造成日期错位
status_TTL_HOURS=24               # 本地摘要超过此时长视为「家里视角缺报」

#============================================================部署配置(R12/R14)
# 【安全红线】凭据 **绝不硬编码**。密钥一旦进代码就随 git 历史永久留存，
# 即使之后删除也仍可从历史中检出。 这里只读环境变量，仓库里零密钥。
#
#凭据来源优先级(R14)：
#1.GitHub_TOKEN-GitHub操作内置凭据，**首选**。
#它由行动运行时自动注入，无需任何长期PAT；权限由工作流程的
#'权限：内容：写入'授予。用它push默认**不会**触发新
#workflow，天然规避自我触发循环。
#2.GitHub_Pat--本地/云端手动运行时的回退(如cron跑在自建机器上).
#
#export GITHUB_TOKEN=xxx(操作自动注入）
#export GITHUB_PAT=github_pat_xxx（手动运行时才需要）
default_PAGES_URL="https://s3153351586-max.github.io/ks-s4b9xz9/"

repo_URL:str=os。环境.得到(
    "IPTV_REPO_URL",
操作系统。环境.得到("GITHUB_REPO", "https://github.com/s3153351586-max/ks-s4b9xz9"),
).rstrip("/")
#R14：发布分支从仪表板改为gh-pages（孤儿分支，与代码分支隔离）
branch_DEFAULT:str=os。环境.得到("IPTV_BRANCH", "gh-pages")

#history保留天数（R14）：超过此天数的云/历史记录/*.json在发布时删除，
# 避免孤儿分支随日积月累无限膨胀。
history_KEEP_DAYS:int=int(操作系统。环境.得到("IPTV_HISTORY_KEEP_DAYS", "30"))

#commit message后缀：阻止这次push触发新的工作流程(三重保险之一)
skip_CI_SUFFIX="[跳过ci]"


#============================================================ R16通知状态机
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
#注意：静默的是“推送”，不是“记录”--看板与status/latest.json每天都更新，
#   想看当前状态随时可查。推送只负责「变化」。
notify_OK="确定"
notify_ALARM="报警"
有效通知状态=(notify_OK，NOTIFY_ALARM)

# 通知状态存放位置：孤儿分支上的一个单行文本文件。
#
#【为什么不塞进status/latest.json]
#status/latest.json是**手机侧**读的摘要，往里面塞云端自己的推送簿记属于
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
    如果 不操作系统。路径.ISDIR(历程目录(_D)):
        返回 []
切断=(_now().日期() - 定时δ(天数=保留天数))
已删除：列表[str]=[]
    为FN在……内 已排序(操作系统。listdir(历程目录(_D))):
M=re.匹配(R"^(\d{8})\.json$"，fn)
        如果 不米：
            继续
        尝试:
D=日期时间。strptime(米。组(1), "%Y%m%d").日期()
        除……之外ValueError：
            继续
        如果D<截止：
full=os.路径.参加(历史目录，fn(_D))
操作系统。移除(满的)
已删除。追加(F”云/history/{FN}")
    如果已删除：
        _log(f"已清理 {Len(已删除)} 个过期归档（保留 {保留天数(_D)}天）："
             f"{', '.参加(操作系统。路径.basename(p) 为p在……内已删除[:5])}"
+("…" 如果 Len(已删除)>5 其他 ""))
    返回已删除


定义 出版(report_path:str，repo_dir:str，branch:str=BRANCH_DEFAULT，
远程：可选[str]=没有一个，do_push:bool=假的,
dry_run:bool=假的，merged_path：可选[str]=没有一个,
dashboard_html：可选[str]=没有一个，日期：可选[str]=没有一个,
force_notify:bool=假的,
emit_notify_marker：可选[str]=没有一个)->口述[str，任意]:
    """
发布云端报告到仓库。

args：
report_path：云端报告JSON路径.
repo目录(_D)：目标仓库本地路径。
分支：发布分支。
遥远的：远端URL(首次克隆用）。
执行推送(_P)：是否真正git推.
        dry_run: 只打印将要写的内容，不落盘不提交。
合并路径(_P)：可选的合并报告路径（用于生成状态摘要的结论部分）。
仪表板HTML(_H)：可选的看板HTML路径，将覆盖仓库index.html。
日期：覆盖日期(YYYYMMDD)，默认取CST当天.
force_notify:r16-5早报告打开.true时间过通知函态度，无条件推一条
(内容=昨天/当前招牌摘要)。
emit_notify_marker:R16。给定时，若通知状态机判定“本轮要推”，就把
            状态机结论写成该路径的 JSON 标记文件。workflow 用这个文件判断
            要不要执行推送步骤 —— 让「判断」留在 Python 里（可测试），
而不是散落到YAML的shell条件里。

退货：
{"已写入"：[...]，"已提交"：布尔，"已推送"：布尔，"状态"：{...}，
"notify"：{...}，"notify_state"：str}

加薪：
RuntimeError：仓库操作或push失败.
"""
和……一起打开(报告路径，编码="utf-8")作为f：
cloud_report=JSON。负载(f)

合并=没有一个
    如果合并路径(_P)和操作系统。路径.isFile(合并路径(_P)):
和……一起打开(合并路径(_P)，编码="utf-8")作为f：
合并=JSON.负载(f)

    #----R12b：发布前强制脱敏+自查---
    # 顺序很关键：先脱敏、再自检、最后才落盘。自检不过直接抛异常中断发布。
cloud_public=清理报表(_R)(cloud_report)
    assert_no_leak(cloud_public，"云报告")
    如果合并的是 不 没有一个:
merged_public=清理报表(_R)(合并的)
        assert_no_leak(合并公用(_P)，"合并报表")
其他:
merged_public=没有一个

日=日期或_now().strftime("%Y%m%d")
书面：列表[str]=[]

latest_abs=os。路径.参与(repo_dir，P_CLOUD_LATEST)
history_abs=os。路径.参与(repo_dir，P_CLOUD_HISTORY。格式(日期=天))
status_abs=os。路径.参与(repo目录，P_STATUS_LATEST)

    # 缺报判定：读**上一版**摘要（写之前），这样本次发布就能带出「上次是什么时候」
新鲜度=check_local_新鲜度(status_abs)

    # 累计计数器：读上一版 → 累加 → 写回（R12d 试运行仪表）
计数器=load_counters(status_abs)
计数器=bump_counters(counters、cloud_public、merged_public)

状态=build_status_summary(合并公共、云公共)
状态["本地_新鲜度"]=新鲜度
状态["计数器"]=计数器
状态["run_id"]=计数器["run_id"]
    # 若本次发布了合并结果，说明本地摘要刚被消费 → 刷新新鲜度坐标
如果合并公用(_P)：
状态["local_timestamp"]=merged_public。得到("时间戳")或现在(_n)().isiformat()
assert_no_leak(状态，“状态摘要”)

#----R16：通知状态机----
#必须在确保_孤立_分支**之后**才能读上轮状态（状态文件在产物分支上）。
    # dry-run 分支不读也无所谓，那里只是预演。
上一个通知(_N)=read_notify_state(repo目录(_D))(如果不是dry_run其他)无
cur_alarm=布尔(状态。得到("闹钟"))
通知=决定通知(prev_notify，cur_alarm，formed=force_notify)
状态["通知"]={
"prev_state"：prev_notify，
"state"：通知["state"]，
"已发送"：通知["发送"]，
“种类”：通知["kind"]，
    }

如果执行(_R)：
_log(f"[模拟运行]将写入{P_CLOUD_LATEST}"
F"({len(JSON.dump(cloud_public))}B，已脱敏)")
_log(f"[模拟运行]将写入{P_CLOUD_HISTORY.format(日期=天)}")
_log(f"[预演]将写入{P_STATUS_LATEST}："
F"{json.dumps(状态，确保_ascii=False)}")
_log(f"[干式运行]局部新鲜度：{新鲜度['注释']}")
_log
_log(f"[空转]通知决策：send={notify['send']}kind={notify['kind']}"
F"-{notify['note']}(上轮{prev_notify})")
return{"written"：[]，"committed"：False，"pushed"：False，
“状态”：状态，“新鲜度”：新鲜度，“计数器”：计数器，
"notify"：notify，"notify_state"：prev_notify}

#R14：切到gh-pages孤儿分支（远端已有则复用，无则首建）
确保孤立分支(repo目录、远程、分支)

    # 切分支后再读一次：上面的 dry-run 预演不能代表真分支上的状态
上一个通知(_N)=读取通知状态(repo目录(_D))
通知=决定通知(prev_notify，cur_alarm，formed=force_notify)
状态["通知"]={
"prev_state"：prev_notify，
"state"：通知["state"]，
"已发送"：通知["发送"]，
“种类”：通知["kind"]，
    }
_log(f"通知决策：send={notify['send']}kind={notify['kind']}"
F"-{notify['note']}(上轮{prev_notify})")

(中的路径、有效负载、标签)
(
(history_abs、cloud_public、P_CLOUD_HISTORY。格式(日期=天))，
(
    ):
os.makedirs(os.path.dirname(路径)或"."，exist_ok=True)
open(路径，"w"，编码="utf-8")为f：
JSON.dump(有效负载，f，确保_ASCII=False，缩进=2)
written.append(标签)
_log(f"写入{label}")

    # R16：把本轮告警状态落盘，下一轮据此判断「是否翻转」。
    # 只推送给定的状态而不是「有告警就推」，是整个降噪设计的关键一步。
write_notify_state(repo_dir，notify["state"])
written.append(P_NOTIFY_STATE)
_log(f"写入{P_NOTIFY_STATE}({notify['state']})")

#R16：给工作流落一个“本轮该推送”的标记文件。
    # 写**主机文件系统**而不是仓库工作区 —— 它是一次运行的临时信号，
    # 绝不该进 git（否则会污染产物分支，还会被下一次运行误读）。
如果emit_notify_marker和notify["send"]：
操作系统。makedirs(os.路径。目录名(emit_notify_marker)或"."，exist_ok=True)
将(emit_notify_marker，"w"，encoding="utf-8")作为f打开：
JSON.dump({"send"：True，"kind"：notify["kind"]，
"state"：notify["state"]，"run_id"：counters["run_id"]}，
F，确保_ascii=False)
_log(f"已落推送标{emit_notify_marker}(kind={notify['kind']})")

如果dashboard_html和os.路径。isFile(仪表板HTML)(_H)：
shutil.copyfile(dashboard_html，os.路径。联接(repo_dir，P_INDEX)
written.append(P_INDEX)
_log(f"写入{P_INDEX}")

    # R14：清理超期归档，控制孤儿分支体积
删除旧历史(repo目录)

    # ---- R12b 最后一道闸门：对整个工作区做文件级泄漏扫描 ----
    # 上面 asse
