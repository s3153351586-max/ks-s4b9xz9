#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
广西（及可参数化省份）移动 OTT EPG 取数脚本 gx_epg_fetch.py
=============================================================
主动查询 EPG 接口取频道表 → 抽直播直链 → 产出 m3u / 探针目标。

【本脚本的由来 —— 为什么不是"抓包路线"】
  原计划：手机侧 Reqable 抓 IPTV App 的 HAR。
  实测作废：和家亲 / 移动高清是**绑定型 App**，直播流由**机顶盒**发起，
  手机侧不产生 m3u8/EPG 请求 → 抓不到。
  转向：直接查 EPG 元数据接口。**该接口公网可达**（实测 HTTP 200），
  于是"抓包"这一步整个被跳过了。

【实测证据（2026-09-22，从公网沙箱）】
  端点：http://gxtvepg.taipan.jda.bcs.ottcn.com:8080
        /ysten-lvoms-epg/epg/getChannels.shtml?deviceGroupId=4747&districtCode=450000
  → HTTP 200，119KB，0.23s，**305 个频道**，每条含 livePlayUrl
  → 其中 **303 条命中本项目现有的 CDN_URL_RE**（cdnrrs.gx.chinamobile.com/PLTV/...）

【一个关键的不对称（决定了管道怎么接）】
  ┌──────────────┬──────────────┬─────────────────────────────┐
  │ 资源          │ 公网可达？    │ 含义                        │
  ├──────────────┼──────────────┼─────────────────────────────┤
  │ EPG 元数据接口 │ ✅ 可达       │ 云端就能拉频道表              │
  │ CDN 直链(m3u8)│ ❌ 不可达     │ 必须在内网侧探测（手机/机顶盒）  │
  └──────────────┴──────────────┴─────────────────────────────┘
  所以正确分工是：**云端拉列表 → 手机侧探可达**，两件事不可互相替代。
  本脚本只做前半段；后半段交给 probe_local.py。

【为什么独立于 har_extract.py】
  两者互补而非替代：
    har_extract  处理"你抓到了什么"（离线 HAR 文件）
    gx_epg_fetch 处理"接口能给我什么"（在线查询）
  URL 识别规则是**同一份**（都 import probe_local.CDN_URL_RE），不重写。

用法：
    # 常规：拉列表 → 写 m3u + 探针目标
    python3 gx_epg_fetch.py --m3u gx.m3u --targets targets.json

    # 先看接口通不通，不做别的（排错第一步）
    python3 gx_epg_fetch.py --check

    # discovery：遍历所有候选端点，三态记录（通/不通/接口变了）
    python3 gx_epg_fetch.py --discover

    # 换省份/节点
    python3 gx_epg_fetch.py --host gxtvepg.taipan.jda.bcs.ottcn.com \
                            --group-id 4747 --district 450000

退出码：0 成功；1 无可用直链；2 参数错误；3 接口不可达；4 接口可达但形态变了。
"""

import argparse
import json
import os
import re
import socket
import sys
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

# ---------------------------------------------------------------- 复用现有资产
# 【设计原则：不许有第二份正则】
#   直链识别只有 probe_local.CDN_URL_RE 一处。本脚本 import 它，不复制。
from probe_local import (  # noqa: E402
    CDN_HOST_MARKER, CDN_URL_RE, extract_cdn_urls,
)

# ---------------------------------------------------------------- 默认端点
# 广西移动（实测可用，2026-09-22）
DEFAULT_HOST = "gxtvepg.taipan.jda.bcs.ottcn.com"
DEFAULT_PORT = 8080
DEFAULT_GROUP_ID = "4747"
DEFAULT_DISTRICT = "450000"

# 已知路径模板（按命中概率排序；{gid}=deviceGroupId, {dc}=districtCode）
PATH_TEMPLATES = [
    "/ysten-lvoms-epg/epg/getChannels.shtml?deviceGroupId={gid}&districtCode={dc}",
    "/ysten-lvoms-epg/epg/getChannelIndexs.shtml?deviceGroupId={gid}",
    "/ysten-lvoms-epg/epg/getChannels.shtml?deviceGroupId={gid}",
]

HTTP_TIMEOUT = 15.0
CHECK_TIMEOUT = 1.5   # 可达性预检用的短超时

# 响应形态三态
SHAPE_JSON_LIST = "json-list"      # 期望：频道数组
SHAPE_JSON_MAP = "json-map"        # channel617: {...} 这种映射式
SHAPE_HTML = "html"                # 403/404 页面
SHAPE_UNKNOWN = "unknown"

# 发现模式的候选端点（来自公开资料，按"像广西"的概率排序，**不代表都能用**）
# 命名规律：<前缀>.<地区码>.bcs.ottcn.com 或 <前缀>.<运营商>.ysten.com
CANDIDATE_HOSTS = [
    # 广西移动（实测可用）
    "gxtvepg.taipan.jda.bcs.ottcn.com",
    # 广西联通（解析到同一 IP，可能是共享基础设施）
    "looktvepg.gxc.bcs.ottcn.com",
    "looktvepg.cugx.ysten.com",
    # 同族参考：上海移动（同 taipan.jda 段）
    "looktvepg.jda.bcs.ottcn.com",
    # 广东移动（对照）
    "looktvepg.gda.bcs.ottcn.com",
]

CANDIDATE_PORTS = [8080, 80, 10001, 8081]


# 直链形态校验：路径里必须有 6 位以上的数字 ID 段。
# 【为什么需要这条】
#   实测发现 24 个频道（"新看点测试"/"广西-动作电影" 等）共用一条
#   `.../PLTV/77777777/224//index.m3u8` —— 注意 `224//` 是**双斜杠**，
#   即频道号段为空。这是接口里的**占位符**，不是真直链。
#   若不过滤，它会进探针 → 永远失败 → 变成一条**永久假告警**。
#   经验：这类"非空但无效"的值，比"空值"危险得多 —— 空值会被跳过，
#   而有值会被当成真数据。
VALID_URL_RE = re.compile(r"/\d{6,}/index\.m3u8", re.I)


def is_valid_channel_url(url: str) -> bool:
    """
    判断直链是否形态有效（非占位符）。

    Args:
        url: 待判定的直链。

    Returns:
        形态有效返回 True。
    """
    return bool(VALID_URL_RE.search(url or ""))


# ================================================================ 可达性预检
def probe_tcp(host: str, port: int, timeout: float = CHECK_TIMEOUT
              ) -> Tuple[bool, str]:
    """
    极短超时的 TCP 连通性预检。

    【为什么要单独做这一步】
      EPG 接口和 CDN 都在"可能不通"的网段上。若不预检就直接拉，
      失败表现统一是"超时"，你分不清三种完全不同的情况：
        ① 网/路由不通      ② 域名解析失败      ③ 接口改了但网络没问题
      分开预检后，三种情况给出三种不同的输出，排错成本从"猜"变成"看"。

    Args:
        host: 主机名或 IP。
        port: 端口。
        timeout: 超时秒数（默认 1.5，够局域网也够公网首包）。

    Returns:
        (是否连通, 说明)。
    """
    try:
        ip = socket.gethostbyname(host)
    except socket.gaierror as e:
        return False, f"DNS 解析失败（{e}）"
    try:
        with socket.create_connection((ip, port), timeout=timeout):
            return True, f"TCP 可达（{host} → {ip}:{port}）"
    except socket.timeout:
        return False, f"TCP 超时（{ip}:{port}）—— 网络/路由不通或端口未开放"
    except OSError as e:
        return False, f"TCP 拒绝（{ip}:{port}，{e}）"


# ================================================================ HTTP 取数
def http_get(url: str, timeout: float = HTTP_TIMEOUT) -> Tuple[int, bytes, str]:
    """
    极简 HTTP GET（标准库，不依赖 requests —— 手机侧也零依赖）。

    Args:
        url: 完整 URL。
        timeout: 超时秒数。

    Returns:
        (状态码, 响应体字节, 错误说明)；网络异常时状态码为 0。
    """
    import urllib.error
    import urllib.request

    req = urllib.request.Request(url, headers={
        # 与 probe_local 保持同一 UA：口径一致，便于交叉比对
        "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 14; SM-S9280 Build/UP1A.231005.007)",
        "Accept": "*/*",
    })
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read(), ""
    except urllib.error.HTTPError as e:
        return e.code, (e.read() or b"")[:2000], f"HTTP {e.code}"
    except urllib.error.URLError as e:
        return 0, b"", f"{type(e.reason).__name__}: {e.reason}"
    except Exception as e:
        return 0, b"", f"{type(e).__name__}: {e}"


def shape_of_body(status: int, body: bytes) -> Tuple[str, Any]:
    """
    判断响应形态。**只判断，不假设** —— 苏州是 `.shtml` 却返回 JSON，
    广西也是 `.shtml` 返回 JSON。后缀名完全不可信。

    【为什么非 200 也要判形态】
      403/404 的错误页往往是 HTML，里面常带"为什么被拒"的线索
      （如"请从内网访问"/"token 无效"）。若直接返回 unknown，
      这些线索就丢了，你只能看到"403"三个字。
      判形态与判成败是两件事，不该混在一起。

    Args:
        status: HTTP 状态码。
        body: 响应体字节。

    Returns:
        (形态标记, 解析后的对象或 None)。非 200 时对象恒为 None ——
        **形态是诊断信息，不代表数据可用**。
    """
    if not body:
        return SHAPE_UNKNOWN, None
    text = ""
    for codec in ("utf-8", "gbk", "latin-1"):
        try:
            text = body.decode(codec)
            break
        except UnicodeDecodeError:
            continue
    s = text.lstrip()
    if s.startswith("<"):
        return SHAPE_HTML, None
    if status != 200:
        # 非 200 的正文即使是 JSON 也不当数据用（可能是错误对象）
        return SHAPE_UNKNOWN, None
    if s.startswith("["):
        try:
            obj = json.loads(text)
            return SHAPE_JSON_LIST, obj
        except json.JSONDecodeError:
            return SHAPE_UNKNOWN, None
    if s.startswith("{"):
        try:
            obj = json.loads(text)
            return SHAPE_JSON_MAP, obj
        except json.JSONDecodeError:
            return SHAPE_UNKNOWN, None
    return SHAPE_UNKNOWN, None


# ================================================================ 频道解析
def extract_channels(obj: Any) -> List[Dict[str, Any]]:
    """
    从各种已知响应形态里统一抽出频道列表。

    【为什么要"统一"】
      广西返回 `[{...}]`（数组），其他节点可能返回 `{"channel617":{...}}`
      （映射式）。调用方不该关心这个差异，所以在这里归一化成列表。

    Args:
        obj: shape_of_body 解析出的对象。

    Returns:
        频道 dict 列表。
    """
    if isinstance(obj, list):
        return [c for c in obj if isinstance(c, dict)]
    if isinstance(obj, dict):
        # 映射式：{"channel617": {...}, "channel616": {...}}
        # 也可能是 {"channels": [...]} 这种包了一层
        for key in ("channels", "data", "list", "result"):
            v = obj.get(key)
            if isinstance(v, list):
                return [c for c in v if isinstance(c, dict)]
        vals = [v for v in obj.values() if isinstance(v, dict)]
        if vals:
            return vals
    return []


def channel_urls(channels: List[Dict[str, Any]]) -> List[Tuple[str, str]]:
    """
    从频道列表里抽 (名称, 直链)。字段名多形态兜底。

    【已知字段名】
      广西移动：livePlayUrl / channelName / no / uuid
      广东：    zteurl / hwurl / title / channelnum
      苏州：    结构未完全确认，故一并兜底

    Args:
        channels: 频道 dict 列表。

    Returns:
        (名称, url) 列表，保序，只含非空 url。
    """
    # 直链字段的候选名，按"实测见过"的顺序
    url_keys = ("livePlayUrl", "zteurl", "hwurl", "url", "playUrl",
                "playurl", "liveUrl", "channelUrl")
    name_keys = ("channelName", "title", "name", "subTitle", "channelname")

    out: List[Tuple[str, str]] = []
    for c in channels:
        u = ""
        for k in url_keys:
            v = c.get(k)
            if isinstance(v, str) and v.strip():
                u = v.strip()
                break
        if not u:
            continue
        n = ""
        for k in name_keys:
            v = c.get(k)
            if isinstance(v, str) and v.strip():
                n = v.strip()
                break
        out.append((n or "未命名", u))
    return out


# ================================================================ 构建 URL
def build_url(host: str, port: int, path_tmpl: str,
              group_id: str, district: str) -> str:
    """
    按模板拼出 EPG 请求 URL。

    Args:
        host: 主机。
        port: 端口；80 时省略（保持 URL 干净，也便于肉眼核对）。
        path_tmpl: 路径模板（含 {gid}/{dc} 占位）。
        group_id: deviceGroupId。
        district: districtCode。

    Returns:
        完整 URL。
    """
    path = path_tmpl.format(gid=group_id, dc=district)
    port_part = "" if port == 80 else f":{port}"
    return f"http://{host}{port_part}{path}"


# ================================================================ discovery
def discover(hosts: Optional[List[str]] = None,
             ports: Optional[List[int]] = None,
             group_id: str = DEFAULT_GROUP_ID,
             district: str = DEFAULT_DISTRICT,
             verbose: bool = True
             ) -> List[Dict[str, Any]]:
    """
    遍历候选端点，对每个组合做**三态**记录。

    【为什么是三态而不是成败】
      "接口不可达"和"接口可达但响应形态变了"是**两种完全不同的故障**，
      排查方向相反（前者查网络，后者查接口版本）。
      合在一起报"失败"会让你在错误的路上找很久。

    Args:
        hosts: 候选 host 列表，默认 CANDIDATE_HOSTS。
        ports: 候选端口列表，默认 CANDIDATE_PORTS。
        group_id: deviceGroupId。
        district: districtCode。
        verbose: 是否打印过程。

    Returns:
        结果列表，每项含 host/port/url/tcp_reachable/http_status/shape/
        channel_count/note。
    """
    hosts = hosts or CANDIDATE_HOSTS
    ports = ports or CANDIDATE_PORTS
    results: List[Dict[str, Any]] = []
    path_tmpl = PATH_TEMPLATES[0]

    for host in hosts:
        dns_ok = True
        try:
            socket.gethostbyname(host)
        except socket.gaierror:
            dns_ok = False

        # DNS 不通就不必逐个端口试了 —— 省掉 N 次无意义的超时等待
        _ports = ports if dns_ok else [ports[0]]

        for port in _ports:
            rec: Dict[str, Any] = {"host": host, "port": port,
                                   "url": build_url(host, port, path_tmpl,
                                                    group_id, district)}
            if not dns_ok:
                rec.update(tcp_reachable=False, dns_ok=False, http_status=None,
                           shape="dns-fail", channel_count=0,
                           note="DNS 解析失败")
                results.append(rec)
                if verbose:
                    print(f"  ✗ {host}:{port}  DNS 解析失败")
                continue

            reach, why = probe_tcp(host, port)
            rec.update(tcp_reachable=reach, dns_ok=True, note=why)
            if not reach:
                rec.update(http_status=None, shape="tcp-fail", channel_count=0)
                results.append(rec)
                if verbose:
                    print(f"  ✗ {host}:{port}  {why}")
                continue

            st, body, err = http_get(rec["url"])
            shape, obj = shape_of_body(st, body)
            chans = extract_channels(obj)
            rec.update(http_status=st, shape=shape,
                       channel_count=len(chans),
                       note=err or f"{len(body)}B")
            results.append(rec)
            if verbose:
                mark = "✓" if chans else "△"
                print(f"  {mark} {host}:{port}  HTTP {st} | {shape} "
                      f"| {len(chans)} 频道 | {len(body)}B")
    return results


# ================================================================ 主流程
def fetch(host: str = DEFAULT_HOST, port: int = DEFAULT_PORT,
          group_id: str = DEFAULT_GROUP_ID,
          district: str = DEFAULT_DISTRICT,
          do_check: bool = False,
          verbose: bool = True) -> Dict[str, Any]:
    """
    拉取一次 EPG 并解析。

    Args:
        host: EPG 主机。
        port: 端口。
        group_id: deviceGroupId。
        district: districtCode。
        do_check: 只做可达性预检，不拉数据。
        verbose: 是否打印过程。

    Returns:
        {"ok": bool, "stage": str, "url": str, "channels": [...],
         "urls": [...], "note": str, "raw_count": int}
        stage 取值：dns / tcp / http / parse / ok
    """
    url = build_url(host, port, PATH_TEMPLATES[0], group_id, district)
    res: Dict[str, Any] = {"ok": False, "stage": "", "url": url,
                           "channels": [], "urls": [], "note": "",
                           "raw_count": 0}

    reach, why = probe_tcp(host, port)
    if verbose:
        print(f"[*] 可达性预检：{why}", file=sys.stderr)
    if not reach:
        res.update(stage="dns" if "DNS" in why else "tcp", note=why)
        return res
    if do_check:
        res.update(stage="tcp", note=why)
        return res

    st, body, err = http_get(url)
    if st == 0:
        res.update(stage="http", note=f"请求失败：{err}")
        return res

    shape, obj = shape_of_body(st, body)
    if st != 200:
        # 把错误页里的线索带出来 —— "HTTP 403" 三个字没有诊断价值，
        # 但 403 页正文里的 "请从内网访问" 有。
        hint = ""
        if shape == SHAPE_HTML and body:
            _t = body.decode("utf-8", "replace")
            _m = re.sub(r"<[^>]+>", " ", _t)
            _m = " ".join(_m.split())[:160]
            hint = f" | 正文明文：{_m}" if _m else ""
        res.update(stage="http",
                   note=f"HTTP {st}（响应 {len(body)}B，形态 {shape}）{hint}")
        return res
    if shape == SHAPE_UNKNOWN:
        res.update(stage="parse",
                   note=f"HTTP 200 但响应形态无法识别（{len(body)}B）"
                        f"—— 接口可能改版，请人工查看")
        return res

    chans = extract_channels(obj)
    res["raw_count"] = len(chans)
    if not chans:
        res.update(stage="parse", note="响应可解析但没有频道项")
        return res

    pairs = channel_urls(chans)
    # 用**现有正则**过滤：只留本项目认得的直链形态。
    # 不自己写匹配 —— 那样两边规则会漂移。
    blob = "\n".join(u for _, u in pairs)
    cdn_urls = extract_cdn_urls(blob, limit=1000)

    # 再过滤掉占位符（形如 /224//index.m3u8，见 is_valid_channel_url 的说明）。
    # 顺序很重要：先按正则收窄到"本项目认得"，再排掉"认得但无效"。
    _before = len(cdn_urls)
    cdn_urls = [u for u in cdn_urls if is_valid_channel_url(u)]
    _dropped = _before - len(cdn_urls)

    # 名称映射：url → 名称（供 m3u 用；extract_cdn_urls 只返回 URL）
    # 【注意】频道名与直链**不是一对一**：实测 303 条里有 27 条是重复 URL
    #   （同一路流挂了多个频道名）。这里保留"首个名字"，
    #   并另存全部名字供查证，避免信息丢失。
    name_by_url: Dict[str, str] = {}
    names_by_url: Dict[str, List[str]] = {}
    for n, u in pairs:
        if not is_valid_channel_url(u):
            continue
        names_by_url.setdefault(u, []).append(n)
        name_by_url.setdefault(u, n)

    _multi = sum(1 for v in names_by_url.values() if len(v) > 1)

    res.update(ok=bool(cdn_urls), stage="ok", channels=chans, urls=cdn_urls,
               name_by_url=name_by_url, names_by_url=names_by_url,
               dropped_placeholder=_dropped, multi_name_groups=_multi,
               note=f"共 {len(chans)} 频道 → {len(cdn_urls)} 条唯一有效直链"
                    + (f"（排除 {_dropped} 条占位符）" if _dropped else "")
                    + (f"，{_multi} 组多频道共链" if _multi else ""))
    return res


def build_m3u(urls: List[str], name_by_url: Optional[Dict[str, str]] = None,
              tag: str = "GX-EPG") -> str:
    """
    生成 m3u 播放列表。

    Args:
        urls: 直链列表。
        name_by_url: url → 频道名映射。
        tag: 分组标签。

    Returns:
        m3u 文本。
    """
    name_by_url = name_by_url or {}
    lines = ["#EXTM3U"]
    for i, u in enumerate(urls, 1):
        n = name_by_url.get(u) or f"{tag}-{i}"
        lines.append(f'#EXTINF:-1 group-title="{tag}",{n}')
        lines.append(u)
    return "\n".join(lines) + "\n"


def build_targets(urls: List[str], name_by_url: Optional[Dict[str, str]] = None,
                  limit: int = 8) -> List[Dict[str, Any]]:
    """
    生成 `probe_local.py --targets` 可用的 JSON。

    【为什么默认只取前 limit 条】
      探针是逐条实测的，305 条全测会很慢。抽样能回答"这批源整体通不通"，
      不需要每一条都验。要全量用 --all。

    【name 里为什么带频道名但不带 url】
      name 在 publish_cloud.PUBLIC_TARGET_FIELDS 白名单里会被发布。
      频道名（如"广西卫视"）不含直链信息，是安全的；
      而 url 必须留空 —— 它会被脱敏闸门整体剥离。

    Args:
        urls: 直链列表。
        name_by_url: url → 频道名映射。
        limit: 取样条数。

    Returns:
        目标列表。
    """
    name_by_url = name_by_url or {}
    out = []
    # 均匀取样：从头取会全是同一类频道（列表按内部顺序排的），
    # 等间隔抽样能覆盖到不同分组。
    step = max(1, len(urls) // limit) if limit and len(urls) > limit else 1
    picked = urls[::step][:limit] if limit else urls
    for i, u in enumerate(picked, 1):
        nm = name_by_url.get(u) or f"频道{i}"
        out.append({
            "id": f"E{i}",
            "name": f"EPG源{i}·{nm}",
            "url": u,
            "type": "stream",
            "scope": "intranet",
        })
    return out


# ================================================================ CLI
def main() -> int:
    ap = argparse.ArgumentParser(
        description="广西移动 OTT EPG 取数（主动查询，替代抓包路线）")
    ap.add_argument("--host", default=DEFAULT_HOST, help=f"EPG 主机（默认 {DEFAULT_HOST}）")
    ap.add_argument("--port", type=int, default=DEFAULT_PORT, help="端口（默认 8080）")
    ap.add_argument("--group-id", default=DEFAULT_GROUP_ID,
                    help=f"deviceGroupId（默认 {DEFAULT_GROUP_ID}）")
    ap.add_argument("--district", default=DEFAULT_DISTRICT,
                    help=f"districtCode（默认 {DEFAULT_DISTRICT}）")
    ap.add_argument("--check", action="store_true",
                    help="只做可达性预检（排错第一步）")
    ap.add_argument("--discover", action="store_true",
                    help="遍历候选端点做三态探测（不写文件）")
    ap.add_argument("--m3u", metavar="PATH", help="输出 m3u 播放列表")
    ap.add_argument("--targets", metavar="PATH",
                    help="输出 probe_local.py --targets 可用 JSON")
    ap.add_argument("--json", metavar="PATH",
                    help="输出**脱敏**结果 JSON（白名单，不含直链，可安全引用）")
    ap.add_argument("--raw-urls", metavar="PATH",
                    help="输出**原始直链**列表 JSON（供 CI 内部对比用）。"
                         "含内网直链，**绝不入库/上传**，仅存活于 runner 临时目录")
    ap.add_argument("--limit", type=int, default=8,
                    help="targets 取样条数（默认 8；0 = 全量）")
    ap.add_argument("--verbose", action="store_true", help="打印过程")
    args = ap.parse_args()

    if args.discover:
        print("[*] discovery：遍历候选端点（三态记录）\n", file=sys.stderr)
        rs = discover(group_id=args.group_id, district=args.district)
        ok = [r for r in rs if r.get("channel_count")]
        print(f"\n[*] 可用端点 {len(ok)}/{len(rs)}", file=sys.stderr)
        for r in ok:
            print(f"    ✓ {r['host']}:{r['port']}  {r['channel_count']} 频道"
                  f"  shape={r['shape']}", file=sys.stderr)
        if args.json:
            os.makedirs(os.path.dirname(os.path.abspath(args.json)) or ".",
                        exist_ok=True)
            with open(args.json, "w", encoding="utf-8") as f:
                json.dump(rs, f, ensure_ascii=False, indent=2)
            print(f"[*] 结果已写入 {args.json}", file=sys.stderr)
        return 0 if ok else 3

    res = fetch(args.host, args.port, args.group_id, args.district,
                do_check=args.check, verbose=True)

    if args.check:
        if res["stage"] == "tcp":
            print(f"✓ 可达：{res['note']}")
            return 0
        print(f"✗ 不可达（stage={res['stage']}）：{res['note']}", file=sys.stderr)
        return 3

    if not res["ok"]:
        print(f"\n[!] 失败（stage={res['stage']}）：{res['note']}", file=sys.stderr)
        if res["stage"] == "dns":
            print("    → DNS 解析不了，域名可能已废弃或需要内网 DNS。", file=sys.stderr)
        elif res["stage"] == "tcp":
            print("    → TCP 不通，检查网络/端口。注意 CDN 直链在内网，"
                  "EPG 接口通常公网可达。", file=sys.stderr)
        elif res["stage"] == "parse":
            print("    → 接口可达但形态变了。这是**接口改版**，不是网络问题。",
                  file=sys.stderr)
        print(f"    原始频道的数量：{res['raw_count']}", file=sys.stderr)
        return 4 if res["stage"] == "parse" else 3

    print(f"\n[✓] {res['note']}", file=sys.stderr)
    print(f"    端点：{res['url']}", file=sys.stderr)

    name_by_url = res.get("name_by_url") or {}

    if args.json:
        os.makedirs(os.path.dirname(os.path.abspath(args.json)) or ".",
                    exist_ok=True)
        # JSON 里**不含直链**（报告层不打码会泄漏；要直链用 --m3u/--targets）。
        # 用白名单而非黑名单：新增字段默认不外泄，比"记得排除"可靠。
        _safe_keys = ("ok", "stage", "note", "raw_count",
                      "dropped_placeholder", "multi_name_groups")
        pub = {k: res.get(k) for k in _safe_keys}
        # 【为什么不输出端点 host】
        #   曾经这里输出 res["url"].split("?")[0]（无 query 的端点路径），
        #   当时假设"壳域可公开"（EPG-API-NOTES 就写着端点规律）。但 R18
        #   收口重估后：--json 是**唯一**被允许进公开产物的出口，而
        #   publish_cloud.assert_epg_masked() 会拦 ottcn/gxtvepg/bcs./.shtml
        #   —— 保留端点会直接撞红闸门。**产物里连端点壳都不留**。
        #   （技术备忘里保留端点规律是另一回事：那是**源码**，只为让协作者
        #     看懂接口，不对产物负责。）
        pub["url_count"] = len(res["urls"])
        # sample_names 只含频道名（无直链），但名单可反推源，**默认也不给**；
        # 需要时由 epg_diff 的 channel_names_top 显式开启（默认关）。
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump(pub, f, ensure_ascii=False, indent=2)
        print(f"[*] 结果已写入 {args.json}（不含直链）", file=sys.stderr)

    if args.raw_urls:
        # 【为什么需要这个出口】
        #   --json 走白名单，故意不含 urls（那是直链）。但 EPG 对比功能
        #   （epg_diff.py）**必须**拿到直链才能算交集。所以单开一路：
        #   脱敏的给产物，原始的只给 CI 内部消费。
        #   使用方有义务保证这个文件不出 runner 临时目录。
        _raw = {
            "urls": res["urls"],
            "name_by_url": {u: res.get("name_by_url", {}).get(u, "")
                            for u in res["urls"]},
            "raw_count": res.get("raw_count"),
            "dropped_placeholder": res.get("dropped_placeholder"),
            "multi_name_groups": res.get("multi_name_groups"),
            "stage": res.get("stage"),
            "ok": res.get("ok"),
        }
        with open(args.raw_urls, "w", encoding="utf-8") as f:
            json.dump(_raw, f, ensure_ascii=False, indent=2)
        print(f"[!] 原始直链已写入 {args.raw_urls}（{len(res['urls'])} 条）"
              f"—— 含内网直链，仅供 CI 内部对比，**禁止入库/上传**。",
              file=sys.stderr)

    if args.m3u:
        os.makedirs(os.path.dirname(os.path.abspath(args.m3u)) or ".",
                    exist_ok=True)
        with open(args.m3u, "w", encoding="utf-8") as f:
            f.write(build_m3u(res["urls"], name_by_url))
        print(f"[*] m3u 已写入 {args.m3u}（{len(res['urls'])} 条）"
              f"—— 含内网直链，请勿提交仓库。", file=sys.stderr)

    if args.targets:
        os.makedirs(os.path.dirname(os.path.abspath(args.targets)) or ".",
                    exist_ok=True)
        lim = args.limit if args.limit > 0 else len(res["urls"])
        tg = build_targets(res["urls"], name_by_url, limit=lim)
        with open(args.targets, "w", encoding="utf-8") as f:
            json.dump(tg, f, ensure_ascii=False, indent=2)
        print(f"[*] 探针目标已写入 {args.targets}（{len(tg)} 个，"
              f"从 {len(res['urls'])} 条均匀取样）", file=sys.stderr)
        print("[!] 该文件含内网直链，请勿提交仓库。", file=sys.stderr)
        print(f"    下一步：python3 probe_local.py --targets {args.targets} "
              f"-o local.json", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
