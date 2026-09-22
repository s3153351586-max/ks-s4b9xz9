#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IPTV 本地探针 probe_local.py
=============================
在**广西移动内网**（Android / rikkahub）运行的探针，专职测 intranet 组。
输出 schema 与 probe_v4.py **完全一致**，可与云端报告一起喂给
`probe_v4.py --merge cloud.json local.json` 做双位置鉴别。

为什么要独立脚本而不是复用 probe_v4：
  1. 依赖降级 —— 手机/路由器上常常没有 aiohttp，本脚本 aiohttp 缺失时
     自动回退到 requests + 线程池实现，保证任何环境可跑；
  2. 默认参数正确 —— 默认 --location local --scope intranet，
     避免误用云端语义；
  3. 体积与可审计性 —— 单文件、零项目依赖，便于拷进 Android。

依赖差异（重要）：
  ┌──────────────┬──────────────────────┬────────────────────────────┐
  │ 能力          │ aiohttp 路径（首选）    │ requests 路径（回退）        │
  ├──────────────┼──────────────────────┼────────────────────────────┤
  │ 并发模型      │ asyncio 单线程         │ ThreadPoolExecutor 多线程   │
  │ 连接复用      │ TCPConnector 池        │ urllib3 池（session 复用）  │
  │ 分片限速读取   │ iter_chunked 精确封顶   │ stream + 手动封顶           │
  │ 重定向计数     │ TraceConfig           │ 手动 allow_redirects=False  │
  │ 建连/TTFB 拆分 │ 原生支持               │ 近似（请求→首字节）          │
  │ 字段完备性     │ 全量                  │ 全量（个别精度略降）         │
  └──────────────┴──────────────────────┴────────────────────────────┘
  requests 路径的 connect_ms 为近似值（无法单独剥离 TLS 握手），
  因此**选源排序请以 speed_kbps 为准**，不要依赖 connect_ms 做精细比较。

用法：
    python3 probe_local.py                          # 测默认 intranet 组
    python3 probe_local.py --targets local.json     # 自定义内网源
    python3 probe_local.py -o local.json --html     # 输出报告
    python3 probe_local.py --force-requests         # 强制走 requests 路径

退出码：0 有可用源；1 全失败；2 参数错误；130 手动取消。
"""

import argparse
import asyncio
import html
import json
import os
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

# ---------------------------------------------------------------- 依赖探测
_HAS_AIOHTTP = False
try:
    import aiohttp
    _HAS_AIOHTTP = True
except ImportError:
    aiohttp = None

try:
    import requests
    _HAS_REQUESTS = True
except ImportError:
    requests = None

if not _HAS_AIOHTTP and not _HAS_REQUESTS:
    print("错误：需要 aiohttp 或 requests 之一。pip install aiohttp 或 pip install requests",
          file=sys.stderr)
    sys.exit(2)

# 与 probe_v4 保持一致的常量
UA = "Dalvik/2.1.0 (Linux; U; Android 14; SM-S9280 Build/UP1A.231005.007)"
SCOPE_PUBLIC = "public"
SCOPE_INTRANET = "intranet"
SCOPE_BASELINE = "baseline"
# R18 新增：运营商公网生态域（诊断上下文，不参与源告警）
SCOPE_ECOSYSTEM = "ecosystem"
VALID_SCOPES = (SCOPE_PUBLIC, SCOPE_INTRANET, SCOPE_BASELINE, SCOPE_ECOSYSTEM)
ALERT_EXEMPT_SCOPES = (SCOPE_BASELINE,)
LOC_CLOUD = "cloud"
LOC_LOCAL = "local"
VALID_LOCATIONS = (LOC_CLOUD, LOC_LOCAL)
SKIP_CLOUD = "skip_cloud"
VALID_KINDS = ("list", "stream", "head")

# 网络健康判定（R10）
NET_OK = "net_ok"
NET_LAN_DOWN = "lan_down"
NET_WAN_DOWN = "wan_down"
NET_UNKNOWN = "net_unknown"

# 内网冗余判定（R10）
INTRA_ALIVE = "intranet_alive"
INTRA_NODE_DOWN = "single_node_down"
INTRA_DOMAIN_DOWN = "domain_down"
INTRA_DOWN = "intranet_down"
INTRA_UNKNOWN = "intranet_unknown"

# --------------------------------------------------------------- 目标源
# 内网冗余组（R10）：三个探针共同区分「单节点故障 / 整域故障 / 内网全断」
#   B1  主链：日常选源对象，可播放流
#   B1b 副链：同域另一频道 → 与 B1 同时挂 = 域级故障，而非单节点
#   B2i EPG 域：只做 HEAD，探「网络层是否通」而非「业务层是否有流」
#
# R12a：B1/B1b 改为**启动时动态抽取**（见 resolve_intranet_targets）。
# 下面的硬编码仅作回退兜底（抽取失败时使用，并标注 stale）。
FALLBACK_INTRANET_TARGETS: List[Dict[str, Any]] = [
    {
        "id": "B1",
        "name": "移动CDN主链",
        "url": "http://cdnrrs.gx.chinamobile.com/PLTV/77777777/224/3221226407/index.m3u8?servicetype=1",
        "type": "stream",
        "scope": "intranet",
    },
    {
        "id": "B1b",
        "name": "移动CDN副链(同域另一频道)",
        "url": "http://cdnrrs.gx.chinamobile.com/PLTV/77777777/224/3221226408/index.m3u8?servicetype=1",
        "type": "stream",
        "scope": "intranet",
    },
]

# 兼容旧引用名：默认即回退组（动态解析在 parse_targets 内进行）
INTRANET_TARGETS: List[Dict[str, Any]] = FALLBACK_INTRANET_TARGETS

# B2i 固定：运营商**公网**生态域 head 探测（R12a 要求固定，不参与动态抽取）。
#
# 【R18 换靶】旧靶 `epg.gx.chinamobile.com` **实测 NXDOMAIN** —— 它从未解析成功过，
#   也就是说这个探针一直在"永久失败"，只是没人注意到（因为它的失败被当成
#   "内网本来就不可达"）。换成本轮实测可用的官方 EPG 端点。
#
# 【R18 语义】scope 标为 ecosystem 而非 intranet：新靶公网可达，
#   它可达只证明"运营商生态域通"，**不等于**"IPTV 内网通"。
#   判定链文案已同步（见 assess_intranet）。
EPG_TARGET: Dict[str, Any] = {
    "id": "B2i",
    "name": "移动生态域(公网EPG)",
    "url": ("http://gxtvepg.taipan.jda.bcs.ottcn.com:8080"
            "/ysten-lvoms-epg/epg/getChannels.shtml"
            "?deviceGroupId=4747&districtCode=450000"),
    "type": "head",
    "scope": "ecosystem",
}

# --------------------------------------------------------------- R12a 动态抽取
# gx.m3u 公开播放列表的获取通道（按序尝试，任一成功即止）。
#
# 【为什么必须走 gh-proxy】
#   1. raw.githubusercontent.com 在多数运营商网络下被 TLS 层阻断或污染
#      （实测直连报 "TLS/SSL connection has been closed (EOF)"）；
#   2. 云端拉取曾遭遇**错误上游**导致 cdnrrs 0 命中 —— 这是 R12a 必须下放到
#      手机侧的根本原因：只有广西移动内网才能验证直链真实可达。
#
# 【上游选择】必须锁定广西移动专版列表。iptv-org 的 cn.m3u 是**全国**聚合，
#   不含 cdnrrs.gx.chinamobile.com（实测 148 条 0 命中），拉它等于必然回退。
GX_M3U_SOURCES: List[str] = [
    # 主通道：广西移动专版（Healer-sys/Home），实测 149 条 cdnrrs 直链
    "https://gh-proxy.com/raw.githubusercontent.com/Healer-sys/Home/refs/heads/main/iptv/gx.m3u",
    # 兼容 gh-proxy 的 /https:/// 完整形式（部分节点只认这种拼法）
    "https://gh-proxy.com/https://raw.githubusercontent.com/Healer-sys/Home/refs/heads/main/iptv/gx.m3u",
    # 备用镜像通道
    "https://ghproxy.net/https://raw.githubusercontent.com/Healer-sys/Home/refs/heads/main/iptv/gx.m3u",
]

# 抽取出的直链必须匹配此特征，避免把公网源误当内网源
CDN_HOST_MARKER = "cdnrrs.gx.chinamobile.com"
CDN_URL_RE = re.compile(
    r"https?://" + re.escape(CDN_HOST_MARKER) + r"/[^\s\"'<>]+?\.m3u8[^\s\"'<>]*",
    re.IGNORECASE,
)


def extract_cdn_urls(text: str, limit: int = 2) -> List[str]:
    """
    从 m3u 文本中抽取前 limit 条 cdnrrs.gx.chinamobile.com 直链。

    抽取时做**去重**（同一直链在列表中可能重复出现多次），保持出现顺序。

    Args:
        text: m3u 文件全文。
        limit: 需要的条数。

    Returns:
        直链列表，可能少于 limit 条（不足时由调用方决定是否回退）。
    """
    seen: List[str] = []
    for m in CDN_URL_RE.finditer(text or ""):
        u = m.group(0)
        if u not in seen:
            seen.append(u)
        if len(seen) >= limit:
            break
    return seen


def fetch_gx_m3u_text(sources: Optional[List[str]] = None,
                      timeout: float = 8.0) -> Tuple[Optional[str], str]:
    """
    拉取 gx.m3u 文本。

    Returns:
        (text_or_None, note)：note 说明用了哪个通道，或失败原因。
    """
    try:
        import requests
    except ImportError:
        return None, "requests 不可用"

    for url in (sources or GX_M3U_SOURCES):
        try:
            r = requests.get(url, timeout=timeout, headers={"User-Agent": UA})
            if r.status_code == 200 and r.text:
                return r.text, f"OK({url.split('/')[2]})"
        except Exception as e:
            last = f"{type(e).__name__}@ {url.split('/')[2]}"
            continue
    return None, f"全部通道失败（最后：{locals().get('last', 'n/a')}）"


def _try_epg_urls(limit: int,
                  verbose: bool = False) -> Tuple[List[str], str, bool]:
    """
    尝试从官方 EPG 接口拿直链（R18：新增的**首选**来源）。

    为什么 EPG 优先于 gx.m3u：
      - EPG 是**一手数据**（运营商自己的频道表），gx.m3u 是第三方聚合；
      - 一手数据不会因为"作者删库"而消失；
      - gx.m3u 一旦停更，源就断了，而 EPG 不会。
    为什么还要回退 gx.m3u：
      - EPG 是**公网**接口，手机在 IPTV 网段内时反而可能被策略拦；
      - EPG 只在广西这套端点上验证过，别处未必有；
      - 两条路互为备份，比单点强。

    【延迟 import】`gx_epg_fetch` 可能没被分发到手机（它是新文件）。
    放函数内 import，缺文件时返回空列表走回退，而不是让整个脚本 import 失败。

    Args:
        limit: 需要的条数。
        verbose: 是否打印过程。

    Returns:
        (urls, note, epg_ok)。epg_ok 表示 EPG 这条路本身是否成功
        （即使抽到的条数不够），用于 meta 留痕。
    """
    try:
        import gx_epg_fetch as _epg
    except ImportError:
        return [], "gx_epg_fetch 未分发到本机", False

    try:
        res = _epg.fetch(verbose=False)
    except Exception as e:  # noqa: BLE001 - 手机侧网络环境复杂，任何异常都走回退
        return [], f"EPG 调用异常（{type(e).__name__}）", False

    stage = res.get("stage") or "unknown"
    if not res.get("ok"):
        return [], f"EPG 不可用（stage={stage}）", False

    # 占位符过滤：EPG 里有 24 个频道共用一条 /224//index.m3u8 这类占位链。
    # 不排掉的话会进探针 → 永久失败 → **永久假告警**。
    urls = [u for u in (res.get("urls") or []) if _epg.is_valid_channel_url(u)]
    if verbose:
        print(f"[*] EPG 抽取：{len(urls)} 条有效直链（stage={stage}）",
              file=sys.stderr)
    return urls[:limit], f"EPG 抽到 {len(urls)} 条有效直链", True


def resolve_intranet_targets(limit_per_attempt: int = 2,
                             verbose: bool = False,
                             use_epg: bool = True) -> Tuple[List[Dict[str, Any]],
                                                            Dict[str, Any]]:
    """
    动态解析内网目标（R12a 建立，R18 改为 **EPG 优先、gx.m3u 回退**）。

    流程：
      1. **EPG 优先**：查官方接口拿直链（`gx_epg_fetch`）
      2. EPG 不足/失败 → **gx.m3u 回退**（第三方列表）
      3. 两级都失败 → 回退 FALLBACK_INTRANET_TARGETS 并置 stale=True
      4. B2i 固定为生态域 head 探测（不参与动态抽取）

    【为什么"EPG 抽到 1 条"仍要回退】
      我们要 2 条（B1 + B1b）。只抽到 1 条时，若就此罢手，
      B1b 会缺位 → 单节点/全域故障的区分能力丧失。所以**条数不足也算失败**，
      继续试 gx.m3u 补足。

    Args:
        limit_per_attempt: 需要的直链条数（B1/B1b 共 2 条）。
        verbose: 是否打印解析过程。
        use_epg: 是否启用 EPG 优先（False 则直接走 gx.m3u，测试/排查用）。

    Returns:
        (targets, meta)；meta 含
        {"source": "epg"|"m3u"|"fallback", "stale": bool,
         "epg_ok": bool, "epg_stage": str|None, "note": str, "cdn_urls": [...]}
        （cdn_urls 仅用于本地日志，**不可入库/公网**）
    """
    epg_note = ""
    epg_ok = False
    epg_urls: List[str] = []

    # ---- 第一级：EPG ----
    if use_epg:
        epg_urls, epg_note, epg_ok = _try_epg_urls(limit_per_attempt, verbose)
        if len(epg_urls) >= limit_per_attempt:
            tg = [
                {"id": "B1", "name": "移动CDN主链(EPG)", "url": epg_urls[0],
                 "type": "stream", "scope": "intranet"},
                {"id": "B1b", "name": "移动CDN副链(EPG)", "url": epg_urls[1],
                 "type": "stream", "scope": "intranet"},
                dict(EPG_TARGET),
            ]
            meta = {"source": "epg", "stale": False, "epg_ok": True,
                    "epg_stage": "ok",
                    "note": f"EPG 直连抽取成功：{epg_note}",
                    "cdn_urls": epg_urls}
            if verbose:
                print(f"[*] 内网目标：EPG 优先命中（{len(epg_urls)} 条）",
                      file=sys.stderr)
            return tg, meta

    # ---- 第二级：gx.m3u 回退 ----
    text, note = fetch_gx_m3u_text()
    if text:
        urls = extract_cdn_urls(text, limit_per_attempt)
        if len(urls) >= limit_per_attempt:
            tg = [
                {"id": "B1", "name": "移动CDN主链(gx.m3u)", "url": urls[0],
                 "type": "stream", "scope": "intranet"},
                {"id": "B1b", "name": "移动CDN副链(gx.m3u)", "url": urls[1],
                 "type": "stream", "scope": "intranet"},
                dict(EPG_TARGET),
            ]
            _why = f"（EPG 未命中：{epg_note}）" if use_epg else ""
            meta = {"source": "m3u", "stale": False, "epg_ok": epg_ok,
                    "epg_stage": None,
                    "note": f"gx.m3u 抽取成功：{note}{_why}",
                    "cdn_urls": urls}
            if verbose:
                print(f"[*] 内网目标：gx.m3u 回退命中{_why}（{len(urls)} 条）",
                      file=sys.stderr)
            return tg, meta

    # ---- 第三级：硬编码兜底 ----
    tg = [dict(t) for t in FALLBACK_INTRANET_TARGETS] + [dict(EPG_TARGET)]
    got = len(extract_cdn_urls(text, limit_per_attempt)) if text else 0
    meta = {"source": "fallback", "stale": True, "epg_ok": epg_ok,
            "epg_stage": None,
            "note": f"抽取失败（EPG: {epg_note or '未启用'}；gx.m3u: {note}），"
                    f"已回退硬编码频道号",
            "cdn_urls": extract_cdn_urls(text, limit_per_attempt) if text else []}
    if got:
        meta["note"] = f"仅抽到 {got}/{limit_per_attempt} 条直链，已回退硬编码"
    if verbose:
        print(f"[!] 内网目标回退硬编码（stale）：{meta['note']}", file=sys.stderr)
    return tg, meta


# 基线组（R10）：**不参与源告警**，只回答「本机网络是否健康」
#   N1 默认网关：LAN / 路由器层
#   N2 公网基线：宽带 / 运营商层（挑一个纯 TCP 可达、无策略干扰的 DNS IP）
# 基线误报是噪声，因此基线失败只做抑制上下文，绝不进源告警集合。
BASELINE_TARGETS: List[Dict[str, Any]] = [
    {
        "id": "N1",
        "name": "默认网关",
        "url": "http://192.168.1.1/",
        "type": "head",
        "scope": "baseline",
    },
    {
        "id": "N2",
        "name": "公网基线(DNS)",
        "url": "http://119.29.29.29/",
        "type": "head",
        "scope": "baseline",
    },
]

# 默认内网源（回退组 + 基线组）。动态解析由 parse_targets 负责替换前两项。
DEFAULT_LOCAL_TARGETS: List[Dict[str, Any]] = (
    [dict(t) for t in FALLBACK_INTRANET_TARGETS] + [dict(EPG_TARGET)]
    + [dict(t) for t in BASELINE_TARGETS]
)


CHANGE_LABELS = {
    "recovered": "恢复",
    "broken": "中断",
    "new": "新增",
    "still_ok": "持续可用",
    "still_fail": "持续失败",
}

_TS_SYNC = 0x47
_FLV_MAGIC = b"FLV"

# R12a：head 探针视为"明确可达"的状态码。405 = 服务端回绝 HEAD 但网络栈健康。
HEAD_REACHABLE_CODES = (200, 301, 302, 303, 307, 308, 405)


def _head_hint(status: Optional[int]) -> Optional[str]:
    """
    为 head 探测结果生成人性化提示。

    区分三类，避免把"可达"笼统读成"服务正常"：
      - 2xx/3xx：正常可达
      - 405    ：服务端回绝 HEAD，但网络层确证可达
      - 其他   ：可达但服务端拒绝裸请求
    """
    if status is None:
        return None
    if 200 <= status < 400:
        return None
    if status == 405:
        return "网络层可达（HTTP 405 回绝 HEAD，但 TCP/HTTP 栈通）"
    return f"网络层可达（HTTP {status}，服务端拒绝裸请求属正常）"


# ================================================================ 数据模型
# 字段顺序与 probe_v4.Result 完全一致，保证两份 JSON 可无缝合并
@dataclass
class Result:
    id: str
    name: str
    kind: str
    scope: str = SCOPE_INTRANET
    # 目标原始 URL（探测发起地址）。**与 final_url 的区别**：
    #   url       始终有值，是「我们打算测什么」——即使探测失败/跳过也在，
    #             本地轨看板靠它展示直链；
    #   final_url 仅在收到响应后才赋值，是「实际落到了哪」。
    # 早期版本只有 final_url，导致失败/跳过的目标在本地轨看板里直链丢失。
    url: Optional[str] = None
    probe_location: Optional[str] = None
    status: Optional[int] = None
    error: Optional[str] = None
    connect_ms: Optional[int] = None
    ttfb_ms: Optional[int] = None
    redirects: Optional[int] = None
    final_url: Optional[str] = None
    latency_ms: Optional[int] = None
    has_extm3u: Optional[bool] = None
    channel_count: Optional[int] = None
    bytes_total: Optional[int] = None
    encoding: Optional[str] = None
    is_hls: Optional[bool] = None
    container: Optional[str] = None
    variant_bandwidth: Optional[int] = None
    variant_count: Optional[int] = None
    segment_url: Optional[str] = None
    segment_bytes: Optional[int] = None
    segment_ttfb_ms: Optional[int] = None
    speed_kbps: Optional[float] = None
    speed_avg_kbps: Optional[float] = None
    cloud_bandwidth_kbps: Optional[float] = None
    segments_tried: Optional[int] = None
    duration_s: Optional[float] = None
    hint: Optional[str] = None
    change: Optional[str] = None
    ok: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @property
    def skipped(self) -> bool:
        return self.error == SKIP_CLOUD


@dataclass
class Target:
    id: str
    name: str
    url: str
    kind: str
    scope: str = SCOPE_INTRANET
    headers: Dict[str, str] = field(default_factory=dict)


# ================================================================ 共用工具
def decode_text(raw: bytes, content_type: str = "") -> Tuple[str, str]:
    """多级编码回退，与 probe_v4 一致。"""
    m = re.search(r"charset=([\w\-]+)", content_type or "", re.I)
    if m:
        enc = m.group(1).lower()
        try:
            return raw.decode(enc), enc
        except (LookupError, UnicodeDecodeError):
            pass
    if raw.startswith(b"\xef\xbb\xbf"):
        return raw[3:].decode("utf-8", errors="replace"), "utf-8-sig"
    if raw.startswith(b"\xff\xfe"):
        return raw[2:].decode("utf-16-le", errors="replace"), "utf-16-le"
    if raw.startswith(b"\xfe\xff"):
        return raw[2:].decode("utf-16-be", errors="replace"), "utf-16-be"
    try:
        return raw.decode("utf-8"), "utf-8"
    except UnicodeDecodeError:
        pass
    for enc in ("gb18030", "gbk", "big5"):
        try:
            return raw.decode(enc), enc
        except (LookupError, UnicodeDecodeError):
            continue
    return raw.decode("latin-1", errors="replace"), "latin-1"


def sniff_container(chunk: bytes) -> Optional[str]:
    """容器嗅探，与 probe_v4 一致。"""
    if not chunk:
        return None
    if chunk[:3] == _FLV_MAGIC:
        return "flv"
    if len(chunk) >= 12 and chunk[4:8] == b"ftyp":
        return "mp4"
    if chunk[0] == _TS_SYNC:
        step = 188
        if len(chunk) >= step * 3:
            if chunk[step] == _TS_SYNC and chunk[step * 2] == _TS_SYNC:
                return "ts"
        else:
            return "ts"
    idx = chunk.find(bytes([_TS_SYNC]))
    if 0 < idx < 512 and len(chunk) > idx + 376:
        if chunk[idx + 188] == _TS_SYNC:
            return "ts"
    return None


def parse_segments(text: str, base_url: str) -> List[str]:
    out = []
    for ln in text.splitlines():
        ln = ln.strip()
        if not ln or ln.startswith("#"):
            continue
        out.append(urljoin(base_url, ln))
    return out


def parse_variants(text: str, base_url: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    pending: Optional[Dict[str, Any]] = None
    for ln in text.splitlines():
        s = ln.strip()
        if s.startswith("#EXT-X-STREAM-INF:"):
            attrs = s[len("#EXT-X-STREAM-INF:"):]
            bw = None
            m = re.search(r"BANDWIDTH=(\d+)", attrs)
            if m:
                bw = int(m.group(1))
            else:
                m = re.search(r"AVERAGE-BANDWIDTH=(\d+)", attrs)
                if m:
                    bw = int(m.group(1))
            res_attr = None
            m = re.search(r"RESOLUTION=(\d+x\d+)", attrs)
            if m:
                res_attr = m.group(1)
            pending = {"bandwidth": bw, "resolution": res_attr}
        elif s and not s.startswith("#"):
            if pending is not None:
                out.append({"url": urljoin(base_url, s),
                            "bandwidth": pending["bandwidth"],
                            "resolution": pending["resolution"]})
                pending = None
    return out


def pick_variant(variants: List[Dict[str, Any]], strategy: str) -> Tuple[str, Optional[int]]:
    with_bw = [v for v in variants if v.get("bandwidth")]
    if not with_bw:
        return variants[0]["url"], variants[0].get("bandwidth")
    if strategy == "min":
        v = min(with_bw, key=lambda x: x["bandwidth"])
    elif strategy == "first":
        v = variants[0]
    else:
        v = max(with_bw, key=lambda x: x["bandwidth"])
    return v["url"], v.get("bandwidth")


def order_segments(segs: List[str], strategy: str) -> List[str]:
    if strategy == "first":
        return segs[:3]
    if strategy == "middle":
        mid = len(segs) // 2
        return [segs[mid]] + segs[:mid] + segs[mid + 1:]
    return list(reversed(segs))[:3]


# ================================================================ requests 实现
class RequestsProbe:
    """
    回退实现：ThreadPoolExecutor 并发。
    语义与 aiohttp 版对齐，个别指标为近似值（见模块 docstring 的依赖差异表）。
    """

    def __init__(self, timeout=10.0, concurrency=4, total_timeout=60.0,
                 max_bytes=512 * 1024, max_seconds=3.0, retries=1,
                 verbose=False, variant_strategy="max", segment_strategy="last",
                 per_host=2, location=LOC_LOCAL):
        self.timeout = timeout
        self.concurrency = max(1, concurrency)
        self.total_timeout = total_timeout
        self.max_bytes = max_bytes
        self.max_seconds = max_seconds
        self.retries = retries
        self.verbose = verbose
        self.variant_strategy = variant_strategy
        self.segment_strategy = segment_strategy
        self.per_host = max(1, per_host)
        self.location = location
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": UA})

    def _log(self, msg):
        if self.verbose:
            print(msg, file=sys.stderr)

    def _base(self, t: Target) -> Result:
        # 务必带上 t.url：本地轨看板要展示直链，而失败/跳过的目标没有 final_url。
        return Result(id=t.id, name=t.name, kind=t.kind, scope=t.scope,
                      url=t.url, probe_location=self.location)

    @staticmethod
    def _classify(e: Exception) -> str:
        if isinstance(e, (requests.Timeout,)):
            return "timeout"
        if isinstance(e, requests.ConnectionError):
            # requests 不区分 DNS/连接，做个启发式判断
            s = str(e).lower()
            if "name or service not known" in s or "nodename nor servname" in s \
                    or "getaddrinfo" in s or "temporary failure in name resolution" in s:
                return "dns"
            return "conn"
        if isinstance(e, requests.exceptions.SSLError):
            return "tls"
        if isinstance(e, ValueError):
            return "other"
        return "other"

    def _get(self, t: Target, url: str):
        """返回 (resp, redirects, elapsed_ms) 或抛异常。"""
        last = None
        for attempt in range(self.retries + 1):
            try:
                t0 = time.monotonic()
                resp = self._session.get(
                    url, headers=t.headers or None, timeout=self.timeout,
                    stream=True, allow_redirects=True,
                )
                ms = int((time.monotonic() - t0) * 1000)
                return resp, len(resp.history), ms
            except Exception as e:
                last = e
                if attempt < self.retries and self._classify(e) in (
                        "timeout", "conn", "dns", "tls", "other"):
                    time.sleep(min(0.5 * (2 ** attempt), 3.0) + random.uniform(0, 0.3))
                    continue
                raise
        if last:
            raise last

    def _read_capped(self, resp):
        """流式读取并精确封顶；返回 (data, ttfb_ms, container, timed_out)。"""
        buf = bytearray()
        t0 = time.monotonic()
        ttfb = None
        deadline = t0 + self.max_seconds
        container = None
        try:
            for chunk in resp.iter_content(chunk_size=64 * 1024):
                if not chunk:
                    continue
                if ttfb is None:
                    ttfb = int((time.monotonic() - t0) * 1000)
                if container is None:
                    container = sniff_container(chunk)
                take = min(len(chunk), self.max_bytes - len(buf))
                if take <= 0:
                    break
                buf.extend(chunk[:take])
                if len(buf) >= self.max_bytes or time.monotonic() >= deadline:
                    break
        except Exception as e:
            if not buf:
                raise
            self._log(f"[warn] 读取中断但已有 {len(buf)}B：{e}")
        return bytes(buf), ttfb, container

    def probe_list(self, t: Target) -> Result:
        r = self._base(t)
        t0 = time.monotonic()
        try:
            resp, redirs, _ = self._get(t, t.url)
        except Exception as e:
            r.error = self._classify(e)
            r.latency_ms = int((time.monotonic() - t0) * 1000)
            return r
        try:
            r.status = resp.status_code
            r.redirects = redirs
            r.final_url = resp.url
            if r.status >= 400:
                r.error = f"http_{r.status // 100}xx"
                r.latency_ms = int((time.monotonic() - t0) * 1000)
                return r
            data, ttfb, _ = self._read_capped(resp)
            r.latency_ms = int((time.monotonic() - t0) * 1000)
            r.ttfb_ms = ttfb
            r.bytes_total = len(data)
            text, enc = decode_text(data, resp.headers.get("Content-Type", ""))
            r.encoding = enc
            r.has_extm3u = "#EXTM3U" in text[:8192]
            r.channel_count = sum(1 for ln in text.splitlines()
                                  if ln.lstrip().startswith("#EXTINF:"))
            if r.has_extm3u and (r.channel_count or 0) > 0:
                r.ok = True
            elif r.has_extm3u:
                r.error = "empty_list"
                r.hint = "含 #EXTM3U 但无任何 #EXTINF 频道条目"
            else:
                r.error = "not_m3u"
            return r
        finally:
            resp.close()

    def _fetch(self, t: Target, url: str):
        resp, redirs, _ = self._get(t, url)
        try:
            if resp.status_code >= 400:
                return None, None, resp.status_code, redirs, str(resp.url), \
                    f"http_{resp.status_code // 100}xx"
            data, ttfb, _ = self._read_capped(resp)
            return data, ttfb, resp.status_code, redirs, str(resp.url), None
        finally:
            resp.close()

    def probe_head(self, t: Target) -> Result:
        """
        网络层可达性探针（R10 / R12a）。

        判定放宽为 **2xx / 3xx / 405 均记 reachable**：
          - 2xx/3xx：正常可达；
          - 405 Method Not Allowed：服务端明确回绝 HEAD 但**TCP 与 HTTP 栈都是通的**，
            这恰恰证明网络层健康，是最有信息量的"可达"信号之一；
          - 其他 4xx/5xx 仍记 reachable（服务端拒绝裸请求属业务层行为），
            但会在 hint 里标注，供人工区分。

        为什么不判 status >= 400 为失败：EPG 域/网关对裸请求回 403/404 是常态，
        把服务端拒绝当网络故障，会把「路由通但业务拒绝」误报成「网络断」，
        而这恰恰是我们要区分的两件事。语义：**TCP/TLS 握手成功 = 网络层健康**。
        """
        r = self._base(t)
        try:
            resp, redirs, ms = self._get(t, t.url)
        except Exception as e:
            r.error = self._classify(e)
            return r
        try:
            r.status = resp.status_code
            r.connect_ms = ms
            r.ttfb_ms = ms
            r.redirects = redirs
            r.final_url = str(resp.url)
            r.ok = True
            r.hint = _head_hint(resp.status_code)
            return r
        finally:
            resp.close()

    def probe_stream(self, t: Target) -> Result:
        r = self._base(t)
        url = t.url
        candidates: List[str] = []
        is_hls = False
        extinf = 0
        for _ in range(3):
            try:
                data, ttfb, status, redirs, final, err = self._fetch(t, url)
            except Exception as e:
                r.error = self._classify(e)
                return r
            if err or data is None:
                r.status = status
                r.error = err or "empty"
                return r
            if r.ttfb_ms is None:
                r.ttfb_ms = ttfb
            r.status, r.redirects, r.final_url = status, redirs, final

            text, _ = decode_text(data, "")
            if "#EXTM3U" in text[:512]:
                is_hls = True
            extinf = sum(1 for ln in text.splitlines()
                         if ln.lstrip().startswith("#EXTINF:"))

            if is_hls:
                variants = parse_variants(text, url)
                if variants:
                    url, bw = pick_variant(variants, self.variant_strategy)
                    r.variant_count = len(variants)
                    r.variant_bandwidth = bw
                    continue
                candidates = parse_segments(text, url)
                if candidates:
                    break
                r.is_hls = True
                r.error = "empty_playlist"
                return r

            container = sniff_container(data)
            if container:
                r.container = container
                r.is_hls = False
                return self._measure_direct(t, url, r, data, ttfb)
            r.is_hls = False
            r.error = "not_hls"
            return r
        else:
            r.is_hls = is_hls
            r.error = "too_many_redirects"
            return r

        r.is_hls = True
        if extinf >= 10:
            r.hint = f"该地址含{extinf}个#EXTINF，疑似频道列表(m3u)，建议改用 type=list"
        if not candidates:
            r.error = "empty_playlist"
            return r

        last_err = "empty_segment"
        tried = 0
        for seg in order_segments(candidates, self.segment_strategy):
            tried += 1
            try:
                data, ttfb, status, redirs, final, err = self._fetch(t, seg)
            except Exception as e:
                last_err = f"segment_{self._classify(e)}"
                continue
            if err or not data:
                last_err = f"segment_{err}" if err else "empty_segment"
                continue
            r.segment_url = seg
            r.segment_bytes = len(data)
            r.container = "hls"
            r.segment_ttfb_ms = ttfb
            r.segments_tried = tried
            r.ok = True
            # requests 路径无法精确剥离建连，speed_avg 与 speed 同源（近似）
            r.speed_avg_kbps = round(len(data) * 8 / 1000 / max(self.max_seconds, 1e-6), 1)
            r.speed_kbps = r.speed_avg_kbps
            return r
        r.segments_tried = tried
        r.error = last_err
        return r

    def _measure_direct(self, t: Target, url: str, r: Result,
                        data: bytes, ttfb: Optional[int]) -> Result:
        r.segment_url = url
        r.segment_bytes = len(data)
        r.segment_ttfb_ms = ttfb
        r.ok = len(data) > 0
        if not r.ok:
            r.error = "empty_stream"
            return r
        r.speed_avg_kbps = round(len(data) * 8 / 1000 / max(self.max_seconds, 1e-6), 1)
        r.speed_kbps = r.speed_avg_kbps
        if r.container == "ts":
            r.hint = r.hint or "直连 TS 流（非 HLS），播放器需支持 MPEG-TS over HTTP"
        return r

    def run(self, targets: List[Target]) -> List[Result]:
        def work(t: Target) -> Result:
            try:
                if t.kind == "head":
                    r = self.probe_head(t)
                elif t.kind == "list":
                    r = self.probe_list(t)
                else:
                    r = self.probe_stream(t)
            except Exception as e:
                r = self._base(t)
                r.error = self._classify(e)
            self._log(f"[done] {t.id} {t.name} ok={r.ok} err={r.error} "
                      f"speed={r.speed_kbps}")
            return r

        out: List[Result] = []
        with ThreadPoolExecutor(max_workers=self.concurrency) as ex:
            futs = {ex.submit(work, t): t for t in targets}
            for fut in as_completed(futs):
                t = futs[fut]
                try:
                    out.append(fut.result())
                except Exception as e:
                    r = self._base(t)
                    r.error = self._classify(e)
                    out.append(r)
        by_id = {r.id: r for r in out}
        return [by_id[t.id] for t in targets if t.id in by_id]


# ================================================================ aiohttp 实现
class AioProbe(RequestsProbe):
    """aiohttp 实现：连接池复用 + 精确 TTFB 拆分。行为对齐 probe_v4。"""

    def __init__(self, **kw):
        super().__init__(**kw)
        self._redirects = 0
        trace = aiohttp.TraceConfig()

        async def _on_redirect(_s, _c, _p):
            self._redirects += 1

        trace.on_request_redirect.append(_on_redirect)
        self._trace = trace

    def _client_timeout(self):
        return aiohttp.ClientTimeout(total=self.timeout,
                                     connect=min(self.timeout, 5.0),
                                     sock_connect=min(self.timeout, 5.0),
                                     sock_read=self.timeout)

    async def _req(self, session, t: Target, url: str):
        attempt = 0
        while True:
            attempt += 1
            self._redirects = 0
            try:
                t0 = time.monotonic()
                resp = await session.get(url, headers=t.headers or None,
                                         timeout=self._client_timeout(),
                                         allow_redirects=True, max_redirects=10)
                return resp, int((time.monotonic() - t0) * 1000)
            except Exception as e:
                err = self._classify(e)
                if attempt <= self.retries and err in ("timeout", "conn", "dns", "tls", "other"):
                    await asyncio.sleep(min(0.5 * (2 ** (attempt - 1)), 3.0)
                                        + random.uniform(0, 0.3))
                    continue
                raise

    @staticmethod
    def _classify(e: Exception) -> str:
        if isinstance(e, (asyncio.TimeoutError, TimeoutError)):
            return "timeout"
        dns = getattr(aiohttp, "ClientConnectorDNSError", None)
        if dns is not None and isinstance(e, dns):
            return "dns"
        if isinstance(e, aiohttp.ClientSSLError):
            return "tls"
        if isinstance(e, aiohttp.ClientConnectorError):
            return "conn"
        if isinstance(e, aiohttp.ClientPayloadError):
            return "payload"
        if isinstance(e, aiohttp.ClientResponseError):
            return "http"
        if isinstance(e, aiohttp.ClientError):
            return "client"
        return "other"

    async def _read_capped(self, resp):
        buf = bytearray()
        t0 = time.monotonic()
        ttfb = None
        container = None
        deadline = t0 + self.max_seconds
        try:
            async for chunk in resp.content.iter_chunked(64 * 1024):
                if ttfb is None:
                    ttfb = int((time.monotonic() - t0) * 1000)
                if container is None:
                    container = sniff_container(chunk)
                take = min(len(chunk), self.max_bytes - len(buf))
                if take <= 0:
                    break
                buf.extend(chunk[:take])
                if len(buf) >= self.max_bytes or time.monotonic() >= deadline:
                    break
        except Exception as e:
            if not buf:
                raise
            self._log(f"[warn] 读取中断但已有 {len(buf)}B：{e}")
        return bytes(buf), ttfb, container

    async def _probe_head(self, session, t: Target) -> Result:
        """
        网络层可达性探针（aiohttp 版）。语义与 RequestsProbe.probe_head 一致：
        2xx/3xx/405 均记 reachable，仅连接层异常才算断。
        """
        r = self._base(t)
        try:
            resp, cms = await self._req(session, t, t.url)
        except Exception as e:
            r.error = self._classify(e)
            return r
        try:
            r.status = resp.status
            r.connect_ms = cms
            r.ttfb_ms = cms
            r.redirects = self._redirects
            r.final_url = str(resp.url)
            r.ok = True
            r.hint = _head_hint(resp.status)
            return r
        finally:
            resp.release()

    async def _probe_list(self, session, t: Target) -> Result:
        r = self._base(t)
        t0 = time.monotonic()
        try:
            resp, cms = await self._req(session, t, t.url)
        except Exception as e:
            r.error = self._classify(e)
            r.latency_ms = int((time.monotonic() - t0) * 1000)
            return r
        try:
            r.status = resp.status
            r.connect_ms = cms
            r.redirects = self._redirects
            r.final_url = str(resp.url)
            if resp.status >= 400:
                r.error = f"http_{resp.status // 100}xx"
                r.latency_ms = int((time.monotonic() - t0) * 1000)
                return r
            data, ttfb, _ = await self._read_capped(resp)
            r.latency_ms = int((time.monotonic() - t0) * 1000)
            r.ttfb_ms = ttfb
            r.bytes_total = len(data)
            text, enc = decode_text(data, resp.headers.get("Content-Type", ""))
            r.encoding = enc
            r.has_extm3u = "#EXTM3U" in text[:8192]
            r.channel_count = sum(1 for ln in text.splitlines()
                                  if ln.lstrip().startswith("#EXTINF:"))
            if r.has_extm3u and (r.channel_count or 0) > 0:
                r.ok = True
            elif r.has_extm3u:
                r.error = "empty_list"
                r.hint = "含 #EXTM3U 但无任何 #EXTINF 频道条目"
            else:
                r.error = "not_m3u"
            return r
        finally:
            resp.release()

    async def _fetch(self, session, t: Target, url: str):
        resp, cms = await self._req(session, t, url)
        try:
            if resp.status >= 400:
                return None, None, resp.status, self._redirects, str(resp.url), \
                    f"http_{resp.status // 100}xx"
            data, ttfb, _ = await self._read_capped(resp)
            return data, ttfb, resp.status, self._redirects, str(resp.url), None
        finally:
            resp.release()

    async def _probe_stream(self, session, t: Target) -> Result:
        r = self._base(t)
        url = t.url
        candidates: List[str] = []
        is_hls = False
        extinf = 0

        for _ in range(3):
            try:
                data, ttfb, status, redirs, final, err = await self._fetch(session, t, url)
            except Exception as e:
                r.error = self._classify(e)
                return r
            if err or data is None:
                r.status = status
                r.error = err or "empty"
                return r
            if r.ttfb_ms is None:
                r.ttfb_ms = ttfb
            r.status, r.redirects, r.final_url = status, redirs, final

            text, _ = decode_text(data, "")
            if "#EXTM3U" in text[:512]:
                is_hls = True
            extinf = sum(1 for ln in text.splitlines()
                         if ln.lstrip().startswith("#EXTINF:"))

            if is_hls:
                variants = parse_variants(text, url)
                if variants:
                    url, bw = pick_variant(variants, self.variant_strategy)
                    r.variant_count = len(variants)
                    r.variant_bandwidth = bw
                    continue
                candidates = parse_segments(text, url)
                if candidates:
                    break
                r.is_hls = True
                r.error = "empty_playlist"
                return r

            container = sniff_container(data)
            if container:
                r.container = container
                r.is_hls = False
                r.segment_url = url
                r.segment_bytes = len(data)
                r.ok = len(data) > 0
                r.speed_avg_kbps = round(len(data) * 8 / 1000 / max(self.max_seconds, 1e-6), 1)
                r.speed_kbps = r.speed_avg_kbps
                if not r.ok:
                    r.error = "empty_stream"
                return r
            r.is_hls = False
            r.error = "not_hls"
            return r
        else:
            r.is_hls = is_hls
            r.error = "too_many_redirects"
            return r

        r.is_hls = True
        if extinf >= 10:
            r.hint = f"该地址含{extinf}个#EXTINF，疑似频道列表(m3u)，建议改用 type=list"
        if not candidates:
            r.error = "empty_playlist"
            return r

        last_err = "empty_segment"
        tried = 0
        for seg in order_segments(candidates, self.segment_strategy):
            tried += 1
            try:
                data, ttfb, status, redirs, final, err = await self._fetch(session, t, seg)
            except Exception as e:
                last_err = f"segment_{self._classify(e)}"
                continue
            if err or not data:
                last_err = f"segment_{err}" if err else "empty_segment"
                continue
            r.segment_url = seg
            r.segment_bytes = len(data)
            r.container = "hls"
            r.segment_ttfb_ms = ttfb
            r.segments_tried = tried
            r.latency_ms = None
            r.ok = True
            # 纯传输速率：这里 TTFB 已单独测得，用它剥离建连影响
            if ttfb is not None and ttfb >= 0:
                trans = max(self.max_seconds - ttfb / 1000.0, 1e-6)
                r.speed_kbps = round(len(data) * 8 / 1000 / trans, 1)
            r.speed_avg_kbps = r.speed_kbps
            return r
        r.segments_tried = tried
        r.error = last_err
        return r

    async def _run_async(self, targets: List[Target]) -> List[Result]:
        connector = aiohttp.TCPConnector(
            limit=self.concurrency * 2, limit_per_host=self.per_host + 1,
            enable_cleanup_closed=True, ttl_dns_cache=300)
        sem = asyncio.Semaphore(self.concurrency)

        async with aiohttp.ClientSession(
                connector=connector, headers={"User-Agent": UA},
                trace_configs=[self._trace]) as session:

            async def guarded(t: Target) -> Result:
                async with sem:
                    try:
                        if t.kind == "head":
                            r = await self._probe_head(session, t)
                        elif t.kind == "list":
                            r = await self._probe_list(session, t)
                        else:
                            r = await self._probe_stream(session, t)
                    except Exception as e:
                        r = self._base(t)
                        r.error = self._classify(e)
                    self._log(f"[done] {t.id} {t.name} ok={r.ok} err={r.error} "
                              f"speed={r.speed_kbps}")
                    return r

            import asyncio as _a
            tasks = [_a.create_task(guarded(t)) for t in targets]
            try:
                results = await _a.wait_for(_a.gather(*tasks, return_exceptions=True),
                                            timeout=self.total_timeout)
            except _a.TimeoutError:
                results = []
                for t, task in zip(targets, tasks):
                    if task.done() and not task.cancelled():
                        try:
                            results.append(task.result())
                        except Exception as e:
                            rr = self._base(t)
                            rr.error = self._classify(e)
                            results.append(rr)
                    else:
                        task.cancel()
                        rr = self._base(t)
                        rr.error = "global_timeout"
                        rr.hint = f"超出整体超时 {self.total_timeout}s，未完成"
                        results.append(rr)
                await _a.sleep(0)

            final: List[Result] = []
            for t, item in zip(targets, results):
                if isinstance(item, Result):
                    final.append(item)
                else:
                    rr = self._base(t)
                    rr.error = self._classify(item) if isinstance(item, BaseException) else "unknown"
                    final.append(rr)
            return final

    def run(self, targets: List[Target]) -> List[Result]:
        import asyncio as _a
        return _a.run(self._run_async(targets))


# ================================================================ 判定（R10）
def assess_network(results: List[Result]) -> Dict[str, Any]:
    """
    网络健康评估：用基线组（N1 网关 / N2 公网）回答「本机网络是否健康」。

    返回 dict：
      {"state": net_ok|lan_down|wan_down|net_unknown,
       "lan": bool|None, "wan": bool|None,
       "suppress": bool,        # True 时源告警应被抑制（故障在本机网络，不在源）
       "semantic": str,         # 中文一句话定性
       "action": str}           # 处置建议

    判定表：
      N1✗                → lan_down   本机 LAN/路由器故障，**抑制所有源告警**
      N1✓ + N2✗          → wan_down   宽带/运营商故障，抑制源告警（源无责）
      N1✓ + N2✓          → net_ok     网络健康，源告警可信
      基线缺失            → net_unknown 不抑制（无基线时不能拿它当挡箭牌）
    """
    by_id = {r.id: r for r in results if r.scope == SCOPE_BASELINE}
    n1, n2 = by_id.get("N1"), by_id.get("N2")
    lan = n1.ok if n1 else None
    wan = n2.ok if n2 else None

    if lan is False:
        return {"state": NET_LAN_DOWN, "lan": lan, "wan": wan, "suppress": True,
                "semantic": "家里 LAN / 路由器故障（默认网关不可达）",
                "action": "先查路由器与网线，源侧结论在本次探测中无效"}
    if lan is True and wan is False:
        return {"state": NET_WAN_DOWN, "lan": lan, "wan": wan, "suppress": True,
                "semantic": "宽带 / 运营商故障（网关通、公网不通）",
                "action": "查宽带线路，源侧结论在本次探测中无效"}
    if lan is True and wan is True:
        return {"state": NET_OK, "lan": lan, "wan": wan, "suppress": False,
                "semantic": "本机网络健康（网关与公网均可达）",
                "action": "源告警可信"}
    return {"state": NET_UNKNOWN, "lan": lan, "wan": wan, "suppress": False,
            "semantic": "网络基线缺失或不足，无法判定本机网络健康",
            "action": "不抑制源告警（无基线时不拿它当挡箭牌）"}


def assess_intranet(results: List[Result]) -> Dict[str, Any]:
    """
    内网 + 生态域冗余评估（R10 建立，R18 语义重命名）。

    【R18 变更】B2i 的靶从**内网 EPG 域**换成**公网生态域**，证明力随之变化：
      旧："B2i 可达 ⇒ 内网网络层可达"
      新："B2i 可达 ⇒ 运营商生态域公网可达"（**不再等价于内网可达**）
    所以状态语义与动作建议同步重命名，避免在真内网断线时给出错误处置。

      B1✗ B1b✓            → single_node_down 同域另一频道还活着 → 单节点问题
      B1✗ B1b✗ B2i✓       → cdn_down   生态域通、CDN 整域无流 → 域级/鉴权故障
      B1✗ B1b✗ B2i✗       → eco_down   生态域也不可达 → DNS/路由/污染，非源问题
      B1✓                 → intranet_alive 主链正常
      数据不足             → intranet_unknown
    """
    # 【必须同时收 ecosystem】B2i 的 scope 已改为 ecosystem（公网域）。
    # 只按 SCOPE_INTRANET 过滤会把 B2i 排除掉 → b2i 恒为 None →
    # 判定链永远走不到 cdn_down/eco_down，退化成"数据不足"。
    by = {r.id: r for r in results
          if r.scope in (SCOPE_INTRANET, SCOPE_ECOSYSTEM)}
    b1, b1b, b2i = by.get("B1"), by.get("B1b"), by.get("B2i")
    if b1 is None:
        return {"state": INTRA_UNKNOWN, "semantic": "内网主链未探测",
                "action": "检查目标配置"}
    if b1.ok:
        spd = f"，{b1.speed_kbps} kbps" if b1.speed_kbps else ""
        return {"state": INTRA_ALIVE, "semantic": f"内网主链正常{spd}",
                "action": "无需动作"}
    b1b_ok = b1b.ok if b1b else None
    b2i_ok = b2i.ok if b2i else None
    if b1b_ok is True:
        return {"state": INTRA_NODE_DOWN,
                "semantic": "主链单节点异常，同域副链仍通",
                "action": "切 B1b 频道播放；主链抖动通常在数小时内自愈"}
    if b1b_ok is False and b2i_ok is True:
        return {"state": INTRA_DOMAIN_DOWN,
                "semantic": "CDN 整域无流，但运营商生态域公网可达",
                "action": "域级故障或鉴权失效（查 UA/Referer）；"
                          "等运营商修复，可用公网源兜底"}
    if b1b_ok is False and b2i_ok is False:
        return {"state": INTRA_DOWN,
                "semantic": "运营商生态域不可达（含 DNS/路由层）",
                "action": "查 DNS/路由与内网接入（专线/CPE），非源问题"}
    if b2i_ok is False:
        return {"state": INTRA_DOWN,
                "semantic": "运营商生态域不可达",
                "action": "查 DNS/路由与内网接入（专线/CPE）"}
    return {"state": INTRA_UNKNOWN, "semantic": "内网数据不足以定性",
            "action": "补测 B1b / B2i"}


# ================================================================ 报告
def build_report(results: List[Result], meta=None) -> Dict[str, Any]:
    def _c(pred):
        return sum(1 for r in results if pred(r))

    counted = [r for r in results if not r.skipped]
    ok_list = _c(lambda r: r.kind == "list" and r.ok)
    ok_stream = _c(lambda r: r.kind == "stream" and r.ok)
    total_list = _c(lambda r: r.kind == "list" and not r.skipped)
    total_stream = _c(lambda r: r.kind == "stream" and not r.skipped)

    scope_stats = {}
    for scope in VALID_SCOPES:
        grp = [r for r in counted if r.scope == scope]
        scope_stats[scope] = {
            "total": len(grp),
            "ok": sum(1 for r in grp if r.ok),
            "fail": sum(1 for r in grp if not r.ok),
            "ok_list": sum(1 for r in grp if r.kind == "list" and r.ok),
            "total_list": sum(1 for r in grp if r.kind == "list"),
            "ok_stream": sum(1 for r in grp if r.kind == "stream" and r.ok),
            "total_stream": sum(1 for r in grp if r.kind == "stream"),
        }

    rep: Dict[str, Any] = {
        "version": "v4.2-local",
        "timestamp": datetime.now().isoformat(),
        "probe_location": (meta or {}).get("location", LOC_LOCAL),
        "summary": {
            "total": len(results),
            "counted": len(counted),
            "skipped": len(results) - len(counted),
            "ok_list": ok_list,
            "fail_list": total_list - ok_list,
            "total_list": total_list,
            "ok_stream": ok_stream,
            "fail_stream": total_stream - ok_stream,
            "total_stream": total_stream,
            "scopes": scope_stats,
            "network": assess_network(results),
            "intranet": assess_intranet(results),
            # alertable：真正参与源告警的目标数（基线组豁免，R10）
            "alertable": sum(1 for r in counted if r.scope not in ALERT_EXEMPT_SCOPES),
            "changes": {
                "recovered": [r.id for r in results if r.change == "recovered"],
                "broken": [r.id for r in results if r.change == "broken"],
                "new": [r.id for r in results if r.change == "new"],
            },
        },
        "targets": [r.to_dict() for r in results],
    }
    # R12a：动态解析元信息（**剔除 cdn_urls**，那是敏感直链，不落盘进报告）
    dmeta = (meta or {}).get("dynamic") or {}
    if dmeta:
        rep["dynamic"] = {k: v for k, v in dmeta.items() if k != "cdn_urls"}
    if meta:
        # meta 里的 dynamic 同样要清干净——否则直链会以 meta.dynamic.cdn_urls
        # 形式残留在报告里（发布脱敏能拦，但本地文件也不该存副本）
        clean_meta = dict(meta)
        if isinstance(clean_meta.get("dynamic"), dict):
            clean_meta["dynamic"] = {k: v for k, v in clean_meta["dynamic"].items()
                                     if k != "cdn_urls"}
        rep["meta"] = clean_meta
    return rep


def render_html(report: Dict[str, Any]) -> str:
    s = report["summary"]
    net = s.get("network") or {}
    intra = s.get("intranet") or {}
    rows = []
    for r in report["targets"]:
        skipped = r.get("error") == SKIP_CLOUD
        cls = "ok" if r["ok"] else ("skip" if skipped else "fail")
        if r["kind"] == "list":
            detail = (f"频道数 {r['channel_count']} / {r.get('encoding') or '-'}"
                      if r["channel_count"] is not None else "-")
            lat = f"{r['latency_ms']}ms" if r["latency_ms"] is not None else "-"
        elif r["kind"] == "head":
            detail = "网络层探针"
            lat = f"{r['ttfb_ms']}ms" if r.get("ttfb_ms") is not None else "-"
        else:
            cont = r.get("container") or "-"
            spd = r.get("speed_kbps")
            detail = (f"{cont} · {spd} kbps / {r['segment_bytes']}B"
                      if spd is not None else f"{cont} · -")
            lat = f"{r['ttfb_ms']}ms" if r.get("ttfb_ms") is not None else "-"
        if skipped:
            state = "<span class='badge badge-skip'>云端免测</span>"
        elif r["ok"]:
            state = "<span class='badge badge-ok'>可用</span>"
        else:
            state = "<span class='badge badge-fail'>失败</span>"
        hint = html.escape(str(r.get("hint") or ""))
        err = html.escape(str(r.get("error") or ""))
        if hint:
            err += f"<div class='hint'>{hint}</div>"
        rows.append(
            f"<tr class='{cls}'><td>{html.escape(str(r['id']))}</td>"
            f"<td>{html.escape(str(r['name']))}</td>"
            f"<td>{html.escape(str(r.get('scope')))}</td>"
            f"<td>{html.escape(str(r.get('container') or '-'))}</td>"
            f"<td>{html.escape(str(r['status'] or '-'))}</td>"
            f"<td>{html.escape(lat)}</td><td>{html.escape(detail)}</td>"
            f"<td class='state'>{state}</td>"
            f"<td>{err}</td></tr>")
    css = ("body{font-family:-apple-system,Segoe UI,Arial,sans-serif;margin:20px;color:#222}"
           "table{border-collapse:collapse;width:100%;margin-top:10px}"
           "th,td{border:1px solid #ddd;padding:6px 8px;font-size:13px}"
           "th{background:#f5f5f5}.ok td{background:#f0faf0}.fail td{background:#fdf2f2}"
           ".skip td{background:#eef3f8;color:#5b7a99}"
           ".badge{display:inline-block;padding:2px 8px;border-radius:10px;"
           "font-size:12px;font-weight:600;white-space:nowrap}"
           ".badge-ok{background:#e3f4e6;color:#1a7f37;border:1px solid #a8d5b2}"
           ".badge-fail{background:#fbe6e6;color:#c62828;border:1px solid #e8a9a9}"
           ".badge-skip{background:#e4ecf5;color:#4a6b8a;border:1px solid #b3c8dd}"
           ".hint{color:#b26a00;font-size:12px;margin-top:4px}"
           ".panel{border:1px solid #e0e0e0;border-radius:8px;padding:10px 14px;"
           "margin:10px 0;background:#fafbfc;font-size:13px}"
           ".panel b{color:#333}")
    net_badge = ("badge-ok" if net.get("state") == NET_OK
                 else "badge-fail" if net.get("suppress") else "badge-skip")
    panels = (
        f"<div class='panel'><b>网络基线：</b>"
        f"<span class='badge {net_badge}'>{html.escape(str(net.get('state') or '-'))}</span> "
        f"{html.escape(str(net.get('semantic') or ''))}"
        f"<div class='hint'>N1网关={net.get('lan')} / N2公网={net.get('wan')} · "
        f"{html.escape(str(net.get('action') or ''))}</div></div>"
        f"<div class='panel'><b>内网冗余：</b>"
        f"<span class='badge badge-skip'>{html.escape(str(intra.get('state') or '-'))}</span> "
        f"{html.escape(str(intra.get('semantic') or ''))}"
        f"<div class='hint'>{html.escape(str(intra.get('action') or ''))}</div></div>"
    )
    return ("<!doctype html><html><head><meta charset='utf-8'><title>本地探针报告</title>"
            f"<style>{css}</style></head><body><h1>IPTV 本地探针报告（家里视角）</h1>"
            f"<p>时间：{html.escape(str(report['timestamp']))}　"
            f"目标 {s['total']}　可用流 {s['ok_stream']}/{s['total_stream']}</p>"
            + panels +
            "<table><tr><th>ID</th><th>名称</th><th>域</th><th>容器</th><th>HTTP</th>"
            "<th>延迟</th><th>明细</th><th>状态</th><th>错误</th></tr>"
            + "".join(rows) + "</table></body></html>")


# ================================================================ CLI
def parse_targets(path: Optional[str], scope_filter: Optional[str],
                  include_baseline: bool = False,
                  dynamic: bool = True,
                  verbose: bool = False,
                  use_epg: bool = True) -> List[Target]:
    """
    解析目标。

    Args:
        path: 自定义源文件；None 时用内置源（动态或回退）。
        scope_filter: 非空时只保留该 scope。
        include_baseline: 使用内置源且为 True 时保留基线组（N1/N2）；
            默认 False 可让只想测内网源的通路不被基线污染。
        dynamic: R12a，使用内置源时是否启动时动态抽取 B1/B1b 直链。
            自定义 --targets 文件时不生效（显式覆盖优先）。
        verbose: 打印动态解析过程。
        use_epg: R18，动态抽取时是否启用 EPG 优先（False 只用 gx.m3u）。

    Note:
        动态解析结果（含直链）**只存在于内存与本地报告**，
        绝不进入发布到公网的 JSON——脱敏在 publish_cloud 侧强制完成。
    """
    if not path:
        if dynamic:
            raw, _meta = resolve_intranet_targets(verbose=verbose,
                                                  use_epg=use_epg)
            parse_targets.last_meta = _meta
            # 动态组已含 B2i；基线按需追加
            if include_baseline:
                raw = raw + [dict(t) for t in BASELINE_TARGETS]
        else:
            raw = [dict(t) for t in FALLBACK_INTRANET_TARGETS] + [dict(EPG_TARGET)]
            parse_targets.last_meta = {"source": "fallback", "stale": True,
                                       "note": "dynamic=False，使用硬编码", "cdn_urls": []}
            if include_baseline:
                raw += [dict(t) for t in BASELINE_TARGETS]
    else:
        raw = json.load(open(path, encoding="utf-8"))
        parse_targets.last_meta = {"source": "file", "stale": False,
                                   "note": f"自定义源文件 {path}", "cdn_urls": []}
    out = []
    for i, item in enumerate(raw):
        kind = str(item.get("type", "stream")).lower()
        if kind not in VALID_KINDS:
            kind = "stream"
        scope = str(item.get("scope") or SCOPE_INTRANET).lower()
        if scope not in VALID_SCOPES:
            scope = SCOPE_INTRANET
        # baseline 不受 scope_filter 约束：它是判定「故障在本机还是源」的必需上下文，
        # 若被 scope=intranet 顺手滤掉，assess_network 就永远拿不到基线 → 判定退化成 unknown。
        # R18：ecosystem 同理豁免 —— 它是评断"CDN 挂了还是网络挂了"的必需上下文，
        # 默认跑 --scope intranet 时必须一起带上，否则判定链永远走不到 cdn_down/eco_down。
        if (scope_filter and scope != scope_filter
                and scope not in (SCOPE_BASELINE, SCOPE_ECOSYSTEM)):
            continue
        out.append(Target(id=str(item.get("id") or f"L{i + 1}"),
                          name=str(item.get("name") or ""),
                          url=str(item["url"]), kind=kind, scope=scope,
                          headers=dict(item.get("headers") or {})))
    return out


parse_targets.last_meta: Dict[str, Any] = {}  # type: ignore[attr-defined]


def main() -> int:
    ap = argparse.ArgumentParser(
        description="IPTV 本地探针 v4.2（家里视角，R12a 内网目标动态化）")
    ap.add_argument("--timeout", type=float, default=10.0, help="单请求超时秒数（默认 10）")
    ap.add_argument("--concurrency", type=int, default=4, help="并发数（默认 4）")
    ap.add_argument("--per-host", type=int, default=2, help="单主机并发上限（默认 2）")
    ap.add_argument("--total-timeout", type=float, default=60.0, help="整体超时秒数")
    ap.add_argument("--max-bytes", type=int, default=512 * 1024, help="测速最大读取字节数")
    ap.add_argument("--max-sec", type=float, default=3.0, help="测速最大读取秒数")
    ap.add_argument("--retries", type=int, default=1, help="重试次数")
    ap.add_argument("--targets", type=str, default=None, help="自定义源文件(JSON)")
    ap.add_argument("--scope", choices=list(VALID_SCOPES), default=SCOPE_INTRANET,
                    help="只测该 scope（默认 intranet）")
    ap.add_argument("--location", choices=list(VALID_LOCATIONS), default=LOC_LOCAL,
                    help="探针位置标记（默认 local，通常不需改）")
    ap.add_argument("--variant-strategy", choices=["max", "min", "first"], default="max")
    ap.add_argument("--segment-strategy", choices=["last", "middle", "first"], default="last")
    ap.add_argument("--force-requests", action="store_true",
                    help="强制使用 requests 实现（即使 aiohttp 可用）")
    ap.add_argument("--force-aiohttp", action="store_true",
                    help="强制使用 aiohttp 实现（不可用时直接报错）")
    ap.add_argument("--no-baseline", action="store_true",
                    help="不测基线组(N1/N2)；默认测，基线用于判断故障在本机网络还是源")
    ap.add_argument("--no-dynamic", action="store_true",
                    help="R12a：不动态抽取内网直链，直接用硬编码频道号（排错用）")
    ap.add_argument("--no-epg", action="store_true",
                    help="R18：关闭 EPG 优先，只用 gx.m3u 抽取（EPG 被墙时排错用）")
    ap.add_argument("-o", "--output", type=str, default=None)
    ap.add_argument("--html", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    use_aio = _HAS_AIOHTTP and not args.force_requests
    if args.force_aiohttp and not _HAS_AIOHTTP:
        print("错误：--force-aiohttp 但未安装 aiohttp", file=sys.stderr)
        return 2
    backend = "aiohttp" if use_aio else "requests"

    try:
        targets = parse_targets(args.targets, args.scope,
                                include_baseline=not args.no_baseline,
                                dynamic=not args.no_dynamic,
                                verbose=args.verbose,
                                use_epg=not args.no_epg)
    except (OSError, json.JSONDecodeError) as e:
        print(f"错误：源文件读取失败：{e}", file=sys.stderr)
        return 2
    if not targets:
        print("错误：没有可用的探测目标", file=sys.stderr)
        return 2

    dmeta = getattr(parse_targets, "last_meta", {}) or {}
    print(f"[*] 本地探针：backend={backend}，location={args.location}，"
          f"scope={args.scope}，共 {len(targets)} 个目标", file=sys.stderr)
    if dmeta.get("source") == "dynamic":
        print(f"[*] 内网目标：动态抽取（{dmeta.get('note')}）", file=sys.stderr)
    elif dmeta.get("source") == "fallback" and dmeta.get("stale"):
        print(f"[!] 内网目标：硬编码回退 STALE（{dmeta.get('note')}）", file=sys.stderr)

    kw = dict(timeout=args.timeout, concurrency=args.concurrency,
              total_timeout=args.total_timeout, max_bytes=args.max_bytes,
              max_seconds=args.max_sec, retries=args.retries, verbose=args.verbose,
              variant_strategy=args.variant_strategy,
              segment_strategy=args.segment_strategy,
              per_host=args.per_host, location=args.location)
    probe = AioProbe(**kw) if use_aio else RequestsProbe(**kw)

    try:
        results = probe.run(targets)
    except KeyboardInterrupt:
        print("已取消", file=sys.stderr)
        return 130

    meta = {"location": args.location, "scope_filter": args.scope,
            "backend": backend, "timeout": args.timeout,
            "variant_strategy": args.variant_strategy,
            "segment_strategy": args.segment_strategy,
            "dynamic": dmeta}
    report = build_report(results, meta)
    payload = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        os.makedirs(os.path.dirname(os.path.abspath(args.output)) or ".", exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(payload)
        print(f"本地报告已写入：{args.output}")
        if args.html:
            hp = os.path.splitext(args.output)[0] + ".html"
            with open(hp, "w", encoding="utf-8") as f:
                f.write(render_html(report))
            print(f"本地 HTML 已写入：{hp}")
    else:
        print(payload)

    s = report["summary"]
    net = s.get("network") or {}
    intra = s.get("intranet") or {}
    print(f"[*] 内网流源 {s['ok_stream']}/{s['total_stream']} 可用（backend={backend}）",
          file=sys.stderr)
    print(f"[*] 网络基线：{net.get('state')} — {net.get('semantic')}", file=sys.stderr)
    print(f"[*] 内网冗余：{intra.get('state')} — {intra.get('semantic')}", file=sys.stderr)
    if net.get("suppress"):
        print("[!] 本机网络异常，源侧结论本次无效（告警已被判定为抑制）", file=sys.stderr)
    # 退出码只看非基线目标：基线是上下文，不是被测源
    alertable = [r for r in results if r.scope not in ALERT_EXEMPT_SCOPES]
    return 0 if any(r.ok for r in alertable) else 1


if __name__ == "__main__":
    sys.exit(main())
