#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IPTV 源探测工具 v4.0
=====================
相对 v3.1 的修复与增强（每条都对应实测复现的缺陷）：

[P0 正确性]
F1  结果集完整性：v3.1 用 asyncio.wait + cancel(pending) 后直接丢弃 pending，
    慢目标在报告里凭空消失（既不 ok 也不 fail），summary 统计随之撒谎。
    v4 改为 asyncio.gather(return_exceptions=True) + 整体超时兜底，
    所有目标必然产出 Result；超时者标记 error=global_timeout。
F2  列表源判据过松：v3.1 `res.ok = has_extm3u`，仅含 "#EXTM3U" 一行的空列表
    也算可用。v4 要求 has_extm3u AND channel_count>0 才算 ok，空列表记
    error=empty_list。
F3  裸 TS 直连流被误杀：v3.1 非 #EXTM3U 一律 not_hls。v4 增加容器嗅探
    （TS 0x47 同步字节 / FLV "FLV" magic / fMP4 "ftyp" box），命中即按
    直连流测速，标记 container=ts|flv|mp4。
F4  GBK 列表解码丢字符：v3.1 硬编码 utf-8+ignore，中文频道名被吞。
    v4 用 Content-Type charset → BOM → utf-8 → gbk/gb18030 → latin-1 逐级回退。
F5  gzip 解压后超 max_bytes 抛 ClientPayloadError 被误判为网络错误。
    v4 用 iter_chunked 手动累积封顶，不再依赖 content.read(N) 的解压语义。
F6  HTML 报告 XSS/结构破坏：v3.1 未转义 id/hint/status。v4 全字段 html.escape。

[P1 测速质量]
F7  master playlist 盲取第一个 variant：v3.1 会选中低码率音频轨，
    速度排序系统性失真。v4 解析 EXT-X-STREAM-INF 的 BANDWIDTH/RESOLUTION，
    支持 --variant-strategy max|min|first（默认 max）。
F8  测速把建连+TLS+TTFB 全算进 elapsed，高 RTT 源被低估。
    v4 记录 ttfb_ms，speed 基于纯传输时长（ttfb 后第一个 chunk 起算），
    同时输出 speed_avg_kbps（含建连）供交叉参考。
F9  直播流盲取第一个分片：滚动窗口首个分片常已过期(404)或正被清理。
    v4 支持 --segment-strategy last|middle|first（默认 last），
    且对失败分片自动降级到其他候选。
F10 分片阈值判定在累加后，最大溢出 1 个 chunk。v4 先判后加，精确封顶。

[P2 并发与网络]
F11 退避 sleep 期间仍占用并发槽 → 重试风暴时并发度腰斩。
    v4 把信号量收窄到「单次 HTTP 请求」粒度，sleep 在槽外。
F12 缺少 per-host 限流：同 CDN 的多个源同时打过去触发网关限速自伤。
    v4 增加 --per-host 每主机并发上限（默认 4，host 级 Semaphore）。
F13 重定向链不可见。v4 记录 redirects 次数与最终 URL。
F14 --proxy 与 per-host 限流交互：按代理主机而非目标主机限流。
F15 缺少 connect/read 耗时拆分。v4 输出 connect_ms / ttfb_ms / segments 详情。

[P3 CLI 与集成]
F16 退出码语义过粗：v3.1 只要有 1 个源活着就返回 0，直播流全断也报成功。
    v4 增加 --fail-threshold 与 --fail-on-stream-down：可要求「列表源与流源
    各自至少 N 个可用」，否则返回 1，供调度系统正确告警。
F17 历史对比与 --only-ok 交互：裁剪后再对比，被裁掉的失败项下次全变 new。
    v4 先对比全量结果、再裁剪输出。
F18 历史对比只认 iptv_YYYYMMDD.json 且按文件名当天排除，重跑覆盖会自我对比。
    v4 用 --compare-file 显式指定 + 时间戳去重，且支持跨目录。
F19 报告 file:// 链接在部分前端不可点。v4 保留计算机路径并同时打印相对路径。

退出码语义（v4）：
  0 满足全部可用性要求；1 可用性要求未满足；2 参数/源文件错误；130 手动取消。

用法示例：
    python3 probe_v4.py -o report.json --html
    python3 probe_v4.py --variant-strategy max --segment-strategy last --per-host 4
    python3 probe_v4.py --fail-on-stream-down --verbose
"""

import argparse
import asyncio
import html
import json
import os
import random
import re
import signal
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

try:
    import aiohttp
    HAS_AIOHTTP = True
except ImportError:  # pragma: no cover
    HAS_AIOHTTP = False

UA = "Dalvik/2.1.0 (Linux; U; Android 14; SM-S9280 Build/UP1A.231005.007)"

# scope 语义：
#   public   —— 公网可达源，云端与本地都应能测（A1-A5、B2）
#   intranet —— 运营商内网源，**仅手机侧（广西移动）可测**（B1、B1b、B2i）
#   baseline —— 网络基线探针，**不参与源告警**，只用于判定本地网络健康
#              （N1 默认网关、N2 公网基线）
#
# 【R12a 架构下放】内网源的**动态抽取与真实探测全部下放到手机侧**：
#   云端曾试图拉 gx.m3u 抽 cdnrrs 直链，但云端遭遇网络污染/拉错上游（iptv-org
#   cn.m3u 是**全国**聚合，不含广西移动源，实测 0 命中），且云端本就在外省、
#   测内网直链必为假阴性。因此：
#     - 云端**不再**自己做内网动态抽取；
#     - 内网目标仍保留在列表里（看板显示「云端免测」），但 _should_skip 会跳过；
#     - 真实抽取逻辑见 probe_local.py 的 resolve_intranet_targets()。
#
# 新增源默认 public；内网源必须显式标 intranet，否则会在云端产生系统性误报。
DEFAULT_TARGETS: List[Dict[str, Any]] = [
    {"id": "A1", "name": "raw", "url": "https://raw.githubusercontent.com/Healer-sys/Home/refs/heads/main/iptv/gx.m3u", "type": "list", "scope": "public"},
    {"id": "A2", "name": "gh-proxy", "url": "https://gh-proxy.com/raw.githubusercontent.com/Healer-sys/Home/refs/heads/main/iptv/gx.m3u", "type": "list", "scope": "public"},
    {"id": "A3", "name": "kkgithub", "url": "https://raw.kkgithub.com/Healer-sys/Home/refs/heads/main/iptv/gx.m3u", "type": "list", "scope": "public"},
    {"id": "A4", "name": "jsdelivr", "url": "https://fastly.jsdelivr.net/gh/Healer-sys/Home@main/iptv/gx.m3u", "type": "list", "scope": "public"},
    {"id": "A5", "name": "gitee", "url": "https://gitee.com/Hello_skylar/Home/raw/main/iptv/gx.m3u", "type": "list", "scope": "public"},
    {"id": "B2", "name": "全国备用(代理)", "url": "https://gh-proxy.com/raw.githubusercontent.com/vbskycn/iptv/refs/heads/master/tv/iptv4.m3u", "type": "stream", "scope": "public"},
]

# 内网组（scope=intranet）：**云端只登记、不探测**（_should_skip 会跳过）。
# 保留在此是为了让云端看板能显示「云端免测」行，而不是静默消失 —— 缺行会让人
# 误以为漏配，显式 skip 才是诚实的。
#   B1   主直链        —— 手机侧动态抽取（EPG 优先 / gx.m3u 回退）
#   B1b  同域另一频道   —— 手机侧动态抽取（同上）
#
# 这里的 url 是**占位**，云端永不请求，仅用于看板展示来源域名。真值在手机侧。
INTRANET_TARGETS: List[Dict[str, Any]] = [
    {"id": "B1", "name": "移动CDN主链（手机侧动态）", "url": "http://cdnrrs.gx.chinamobile.com/", "type": "stream", "scope": "intranet"},
    {"id": "B1b", "name": "移动CDN副链（手机侧动态）", "url": "http://cdnrrs.gx.chinamobile.com/", "type": "stream", "scope": "intranet"},
]

# 生态组（scope=ecosystem）：运营商公网生态域，作为**诊断上下文**。
#
# 【R18 换靶】B2i 原本指向 `epg.gx.chinamobile.com` —— 该域名**实测 NXDOMAIN**，
#   是历史遗留的废假设（它从未解析成功过，所以这个探针一直在"永久失败"）。
#   换成本轮实测可用的官方 EPG 端点。
#
# 【R18 语义变更 —— 重要】
#   旧注释说："B2i 可达 ⇒ 移动内网可达"。**新语义下这不再成立**：
#   新靶是**公网**域，它可达只能证明"移动生态域公网可达"，
#   **不能**推出"IPTV 内网可达"。因此判定链的结论文案必须同步改（见 assess_intranet）。
#
# 【为什么云端也跳过（而不是让云端测）】
#   云端已经在跑 `gx_epg_fetch.py`（Actions 日更步骤）—— 那次请求的成败
#   就是云端侧"EPG 健康"的信号。这里再探一次是**重复探测**，
#   只会引入"两个地方对同一个域给出不同结论"的困惑。
ECOSYSTEM_TARGETS: List[Dict[str, Any]] = [
    {
        "id": "B2i",
        "name": "移动生态域(公网EPG)",
        "url": ("http://gxtvepg.taipan.jda.bcs.ottcn.com:8080"
                "/ysten-lvoms-epg/epg/getChannels.shtml"
                "?deviceGroupId=4747&districtCode=450000"),
        "type": "head",
        "scope": "ecosystem",
    },
]

BASELINE_TARGETS: List[Dict[str, Any]] = [
    {"id": "N1", "name": "默认网关", "url": "http://192.168.1.1/", "type": "head", "scope": "baseline"},
    {"id": "N2", "name": "公网基线(DNS)", "url": "http://119.29.29.29/", "type": "head", "scope": "baseline"},
]

# scope 取值
SCOPE_PUBLIC = "public"
SCOPE_INTRANET = "intranet"
SCOPE_BASELINE = "baseline"
# R18 新增：ecosystem —— 运营商的**公网**生态域（EPG 元数据接口）。
#   与 intranet 的区别：它公网可解析可达，不受"必须接 IPTV 网段"约束；
#   与 public 的区别：它是运营商自有基础设施，语义上属于"生态"而非"公网源"，
#   且**不参与源告警**（它挂了意味着"生态域不可达"，不是"某个源死了"）。
SCOPE_ECOSYSTEM = "ecosystem"
VALID_SCOPES = (SCOPE_PUBLIC, SCOPE_INTRANET, SCOPE_BASELINE, SCOPE_ECOSYSTEM)

# 不参与源告警的 scope
#   baseline  —— 只做网络健康判定
#   ecosystem —— 只做"生态域可达性"判定，是**诊断上下文**而非被监控的源。
#                它进告警会产生"EPG 域抖了一下 → 报某频道源挂了"的误导。
ALERT_EXEMPT_SCOPES = (SCOPE_BASELINE, SCOPE_ECOSYSTEM)

# 目标类型：list（m3u 列表）/ stream（直播流）/ head（仅探可达性，不测速）
VALID_KINDS = ("list", "stream", "head")

# location 取值：探针所处网络位置
LOC_CLOUD = "cloud"
LOC_LOCAL = "local"
VALID_LOCATIONS = (LOC_CLOUD, LOC_LOCAL)

# 云端跳过内网源时使用的 error 值（F16/统计/对比均需识别）
SKIP_CLOUD = "skip_cloud"

# 网络健康判定（R10）
NET_OK = "net_ok"              # N1 N2 都通
NET_LAN_DOWN = "lan_down"      # N1 fail —— 局域网/路由器问题，抑制全部源告警
NET_WAN_DOWN = "wan_down"      # N1 ok + N2 fail —— 宽带断网/运营商故障
NET_UNKNOWN = "net_unknown"    # 基线数据缺失

_DNS_ERR_CLS = getattr(aiohttp, "ClientConnectorDNSError", None) if HAS_AIOHTTP else None

CHANGE_LABELS = {
    "recovered": "恢复",
    "broken": "中断",
    "new": "新增",
    "still_ok": "持续可用",
    "still_fail": "持续失败",
}

# 分片内容的容器魔数（用于直连流嗅探）
_TS_SYNC = 0x47
_FLV_MAGIC = b"FLV"


# ================================================================ 数据模型
@dataclass
class Target:
    id: str
    name: str
    url: str
    kind: str  # "list" | "stream"
    scope: str = SCOPE_PUBLIC   # "public" | "intranet"
    headers: Dict[str, str] = field(default_factory=dict)


@dataclass
class Result:
    id: str
    name: str
    kind: str
    scope: str = SCOPE_PUBLIC            # "public" | "intranet"
    probe_location: Optional[str] = None  # "cloud" | "local"，产出该结果的位置
    # ---- 网络层 ----
    status: Optional[int] = None
    error: Optional[str] = None
    connect_ms: Optional[int] = None      # TCP+TLS 建连耗时（F15）
    ttfb_ms: Optional[int] = None         # 首字节耗时
    redirects: Optional[int] = None       # 重定向次数（F13）
    final_url: Optional[str] = None       # 重定向后最终 URL（F13）
    # ---- 列表源 ----
    latency_ms: Optional[int] = None
    has_extm3u: Optional[bool] = None
    channel_count: Optional[int] = None
    bytes_total: Optional[int] = None
    encoding: Optional[str] = None        # 实际解码所用编码（F4）
    # ---- 流源 ----
    is_hls: Optional[bool] = None
    container: Optional[str] = None       # hls / ts / flv / mp4（F3）
    variant_bandwidth: Optional[int] = None   # 选中 variant 的码率（F7）
    variant_count: Optional[int] = None       # master 内 variant 数量（F7）
    segment_url: Optional[str] = None
    segment_bytes: Optional[int] = None
    segment_ttfb_ms: Optional[int] = None
    speed_kbps: Optional[float] = None        # 纯传输速率（F8）；选源排序以此为准
    speed_avg_kbps: Optional[float] = None    # 含建连+TTFB 的平均速率（F8）
    cloud_bandwidth_kbps: Optional[float] = None  # 仅云端测得，公网参考值，不用于内网选源
    segments_tried: Optional[int] = None      # 尝试过的分片数（F9）
    duration_s: Optional[float] = None
    hint: Optional[str] = None
    change: Optional[str] = None
    ok: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @property
    def skipped(self) -> bool:
        """是否因位置不匹配被跳过（不计入失败、不参与对比）。"""
        return self.error == SKIP_CLOUD


# ================================================================ 解码
def decode_text(raw: bytes, content_type: str = "") -> Tuple[str, str]:
    """多级编码回退解码（F4）。返回 (text, encoding)。"""
    # 1) 显式 charset
    m = re.search(r"charset=([\w\-]+)", content_type or "", re.I)
    if m:
        enc = m.group(1).lower()
        try:
            return raw.decode(enc), enc
        except (LookupError, UnicodeDecodeError):
            pass
    # 2) BOM
    if raw.startswith(b"\xef\xbb\xbf"):
        return raw[3:].decode("utf-8", errors="replace"), "utf-8-sig"
    if raw.startswith(b"\xff\xfe"):
        return raw[2:].decode("utf-16-le", errors="replace"), "utf-16-le"
    if raw.startswith(b"\xfe\xff"):
        return raw[2:].decode("utf-16-be", errors="replace"), "utf-16-be"
    # 3) 严格 utf-8
    try:
        return raw.decode("utf-8"), "utf-8"
    except UnicodeDecodeError:
        pass
    # 4) 中文常见编码
    for enc in ("gb18030", "gbk", "big5"):
        try:
            return raw.decode(enc), enc
        except (LookupError, UnicodeDecodeError):
            continue
    # 5) 兜底
    return raw.decode("latin-1", errors="replace"), "latin-1"


# ================================================================ 容器嗅探
def sniff_container(chunk: bytes) -> Optional[str]:
    """从分片前缀嗅探容器类型（F3）。"""
    if not chunk:
        return None
    if chunk[:3] == _FLV_MAGIC:
        return "flv"
    if len(chunk) >= 12 and chunk[4:8] == b"ftyp":
        return "mp4"
    # TS：188 字节包，同步字节应周期性出现在 0x47
    if chunk[0] == _TS_SYNC:
        step = 188
        if len(chunk) >= step * 3:
            if chunk[step] == _TS_SYNC and chunk[step * 2] == _TS_SYNC:
                return "ts"
        else:
            return "ts"  # 数据太短，按首字节乐观判定
    # 部分源前置了少量垃圾字节，扫描前 512B 找同步字节
    idx = chunk.find(bytes([_TS_SYNC]))
    if 0 < idx < 512 and len(chunk) > idx + 376:
        if chunk[idx + 188] == _TS_SYNC:
            return "ts"
    return None


# ================================================================ 核心探测
class IPTVProbe:
    def __init__(self, targets: List[Target], timeout: float = 5.0,
                 concurrency: int = 10, total_timeout: float = 60.0,
                 max_bytes: int = 512 * 1024, max_seconds: float = 3.0,
                 retries: int = 1, max_depth: int = 3,
                 proxy: Optional[str] = None, insecure: bool = False,
                 verbose: bool = False,
                 variant_strategy: str = "max",
                 segment_strategy: str = "last",
                 per_host: int = 4,
                 location: str = LOC_CLOUD,
                 scope_filter: Optional[str] = None):
        if not HAS_AIOHTTP:  # pragma: no cover
            raise RuntimeError("需要安装 aiohttp：pip install aiohttp")
        # F13: 用 TraceConfig 统计重定向链（aiohttp 无 on_redirect 参数）
        self._redirects = 0
        _trace = aiohttp.TraceConfig()

        async def _on_redirect(_session, _ctx, _params):
            self._redirects += 1

        _trace.on_request_redirect.append(_on_redirect)
        self._trace = _trace
        self.targets = targets
        self.timeout = timeout
        self.concurrency = max(1, concurrency)
        self.total_timeout = total_timeout
        self.max_bytes = max_bytes
        self.max_seconds = max_seconds
        self.retries = retries
        self.max_depth = max_depth
        self.proxy = proxy
        self.insecure = insecure
        self.verbose = verbose
        self.variant_strategy = variant_strategy
        self.segment_strategy = segment_strategy
        self.per_host = max(1, per_host)
        # 位置感知：cloud 下 intranet 源直接跳过，避免系统性误报
        if location not in VALID_LOCATIONS:
            raise ValueError(f"location 必须是 {VALID_LOCATIONS}，得到 {location!r}")
        self.location = location
        if scope_filter is not None and scope_filter not in VALID_SCOPES:
            raise ValueError(f"scope_filter 必须是 {VALID_SCOPES}，得到 {scope_filter!r}")
        self.scope_filter = scope_filter
        # F11: 请求级信号量（sleep 在槽外）
        self._req_sem = asyncio.Semaphore(self.concurrency)
        # F12: per-host 信号量
        self._host_sems: Dict[str, asyncio.Semaphore] = {}

    def _should_skip(self, t: Target) -> bool:
        """
        云端不探测的源。

        两类：
          intranet  —— 位置不可达，测了也是假阴性（本就只在手机侧测）；
          ecosystem —— **不是位置问题**，是**职责划分**：云端已在 Actions 里
                       跑 gx_epg_fetch，那次的成败即 EPG 健康信号。
                       这里再探一遍属重复探测，且会造成两处结论打架。
        """
        if self.location != LOC_CLOUD:
            return False
        return t.scope in (SCOPE_INTRANET, SCOPE_ECOSYSTEM)

    # ------------------------------------------------------------ 工具
    @staticmethod
    def _classify_error(e: Exception) -> str:
        if isinstance(e, (asyncio.TimeoutError, TimeoutError)):
            return "timeout"
        if _DNS_ERR_CLS is not None and isinstance(e, _DNS_ERR_CLS):
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

    @staticmethod
    def _is_network_error(err: str) -> bool:
        return err in ("timeout", "dns", "conn", "tls", "client", "other", "payload")

    def _timeout(self) -> aiohttp.ClientTimeout:
        return aiohttp.ClientTimeout(
            total=self.timeout,
            connect=min(self.timeout, 5.0),
            sock_connect=min(self.timeout, 5.0),
            sock_read=self.timeout,
        )

    @staticmethod
    def _backoff(attempt: int) -> float:
        return min(0.5 * (2 ** (attempt - 1)), 3.0) + random.uniform(0, 0.3)

    def _log(self, msg: str) -> None:
        if self.verbose:
            print(msg, file=sys.stderr)

    def _host_sem(self, url: str) -> asyncio.Semaphore:
        """F12/F14: 按目标主机（有代理时按目标 host 仍成立）限流。"""
        host = urlparse(url).netloc or "-"
        sem = self._host_sems.get(host)
        if sem is None:
            sem = asyncio.Semaphore(self.per_host)
            self._host_sems[host] = sem
        return sem

    def _base_result(self, t: Target) -> Result:
        return Result(id=t.id, name=t.name, kind=t.kind,
                      scope=t.scope, probe_location=self.location)

    def _skip_result(self, t: Target) -> Result:
        """云端遇到内网源：产出占位结果，明确标记而非伪造成失败。"""
        r = self._base_result(t)
        r.error = SKIP_CLOUD
        r.ok = False
        r.hint = "内网源，云端不可测，交由本地探针"
        return r

    # ------------------------------------------------------------ 统一请求
    async def _request(self, session: aiohttp.ClientSession, t: Target,
                       url: str, *, stream: bool = False):
        """
        发一次请求，返回 (resp_or_None, Result_patch, exc_or_None)。
        F11 信号量只包住请求建立阶段；F12 叠加 per-host；F13 记录重定向。
        调用方负责 release()。
        """
        attempt = 0
        while True:
            attempt += 1
            await self._req_sem.acquire()
            try:
                async with self._host_sem(url):
                    self._redirects = 0
                    t_conn = time.monotonic()
                    resp = await session.get(
                        url,
                        headers=t.headers or None,
                        proxy=self.proxy,
                        timeout=self._timeout(),
                        allow_redirects=True,
                        max_redirects=10,
                    )
                    connect_ms = int((time.monotonic() - t_conn) * 1000)
            except Exception as e:
                err = self._classify_error(e)
                if attempt <= self.retries and self._is_network_error(err):
                    self._log(f"[retry] {t.id} 第{attempt}次失败({err})，退避后重试")
                    await asyncio.sleep(self._backoff(attempt))
                    continue
                return None, {"error": err}, e
            finally:
                self._req_sem.release()

            patch = {
                "status": resp.status,
                "connect_ms": connect_ms,
                "redirects": self._redirects,
                "final_url": str(resp.url),
            }
            return resp, patch, None

    # ------------------------------------------------------------ 可达性探针
    async def _probe_head(self, session, t: Target) -> Result:
        """
        head 类型：只探可达性，不测速、不校验内容。
        用于网络基线（N1/N2）与 EPG 域（B2i）判定。

        判定约定：
          - 任何 HTTP 响应（含 4xx/5xx）都算「网络层可达」→ ok=True。
            因为网关/DNS 服务器对裸 GET 返回 403/404 是常态，
            那证明 TCP 与 HTTP 往返正常。
          - 连接类错误（dns/conn/timeout/tls）→ ok=False，即网络层不可达。
        """
        res = self._base_result(t)
        resp, patch, _ = await self._request(session, t, t.url)
        if resp is None:
            res.error = patch.get("error", "other")
            res.ok = False
            return res
        try:
            res.status = resp.status
            res.connect_ms = patch.get("connect_ms")
            res.ttfb_ms = patch.get("connect_ms")
            res.redirects = patch.get("redirects")
            res.final_url = patch.get("final_url")
            # 有响应即视为网络层可达
            res.ok = True
            if resp.status >= 400:
                res.hint = f"网络层可达（HTTP {resp.status}，服务端拒绝裸请求属正常）"
            return res
        finally:
            resp.release()

    # ------------------------------------------------------------ 列表源
    async def _probe_list(self, session, t: Target) -> Result:
        res = self._base_result(t)
        t0 = time.monotonic()
        resp, patch, exc = await self._request(session, t, t.url)
        if resp is None:
            res.error = patch.get("error", "other")
            res.latency_ms = int((time.monotonic() - t0) * 1000)
            return res
        try:
            for k, v in patch.items():
                setattr(res, k, v)
            if resp.status >= 400:
                res.error = f"http_{resp.status // 100}xx"
                res.latency_ms = int((time.monotonic() - t0) * 1000)
                return res

            # F5: 手动累积封顶，兼容 gzip 解压流
            body = bytearray()
            try:
                async for chunk in resp.content.iter_chunked(64 * 1024):
                    body.extend(chunk)
                    if len(body) >= self.max_bytes:
                        break
            except Exception as e:
                res.error = self._classify_error(e)
                res.latency_ms = int((time.monotonic() - t0) * 1000)
                return res
            res.latency_ms = int((time.monotonic() - t0) * 1000)
            res.bytes_total = len(body)
            text, enc = decode_text(bytes(body), resp.headers.get("Content-Type", ""))
            res.encoding = enc
            res.has_extm3u = "#EXTM3U" in text[:8192]
            res.channel_count = sum(
                1 for ln in text.splitlines() if ln.lstrip().startswith("#EXTINF:"))
            # F2: 空列表不再算可用
            if res.has_extm3u and (res.channel_count or 0) > 0:
                res.ok = True
            elif res.has_extm3u:
                res.error = "empty_list"
                res.hint = "含 #EXTM3U 但无任何 #EXTINF 频道条目"
            else:
                res.error = "not_m3u"
            return res
        finally:
            resp.release()

    # ------------------------------------------------------------ 流源
    async def _fetch_playlist(self, session, t: Target, url: str) -> Tuple[Optional[bytes], Result, Optional[str]]:
        """取一份 m3u8 文本，返回 (body, res, next_url_or_None)。next_url 非空表示层级已推进。"""
        res = self._base_result(t)
        t0 = time.monotonic()
        resp, patch, _ = await self._request(session, t, url)
        if resp is None:
            res.error = patch.get("error", "other")
            return None, res, None
        try:
            if res.ttfb_ms is None:
                res.ttfb_ms = patch.get("connect_ms")
            for k, v in patch.items():
                setattr(res, k, v)
            if resp.status >= 400:
                res.error = f"http_{resp.status // 100}xx"
                return None, res, None
            body = bytearray()
            try:
                async for chunk in resp.content.iter_chunked(32 * 1024):
                    body.extend(chunk)
                    if len(body) >= self.max_bytes:
                        break
            except Exception as e:
                res.error = self._classify_error(e)
                return None, res, None
            return bytes(body), res, None
        finally:
            resp.release()

    async def _probe_stream(self, session, t: Target) -> Result:
        res = self._base_result(t)
        url = t.url
        text = ""
        is_hls = False
        extinf_count = 0
        candidates: List[str] = []
        variant_bw: Optional[int] = None
        variant_n: Optional[int] = None

        # ---- 层级推进：master -> media ----
        for _depth in range(self.max_depth):
            body, r1, _ = await self._fetch_playlist(session, t, url)
            # 网络层信息回填（首次有效值优先）
            for f in ("status", "connect_ms", "ttfb_ms", "redirects", "final_url"):
                if getattr(res, f) is None and getattr(r1, f) is not None:
                    setattr(res, f, getattr(r1, f))
            if body is None:
                res.error = r1.error
                return res

            text, _enc = decode_text(body, "")
            if "#EXTM3U" in text[:512]:
                is_hls = True
            extinf_count = sum(
                1 for ln in text.splitlines() if ln.lstrip().startswith("#EXTINF:"))

            if is_hls:
                # F7: master playlist 按策略选 variant
                variants = self._parse_variants(text, url)
                if variants:
                    variant_n = len(variants)
                    chosen_url, variant_bw = self._pick_variant(variants)
                    url = chosen_url
                    candidates = []
                    res.variant_count = variant_n
                    res.variant_bandwidth = variant_bw
                    self._log(f"[variant] {t.id} 共{variant_n}路，选中 BANDWIDTH={variant_bw} -> {chosen_url}")
                    continue  # 继续拉 media playlist
                candidates = self._parse_segments(text, url)
                if candidates:
                    break
                res.is_hls = is_hls
                res.error = "empty_playlist"
                return res

            # 非 HLS：可能是裸 TS / FLV 直连流（F3）
            container = sniff_container(body)
            if container:
                res.container = container
                res.is_hls = False
                break
            res.is_hls = False
            res.error = "not_hls"
            return res
        else:
            res.is_hls = is_hls
            res.error = "too_many_redirects"
            return res

        # ---- 裸直连流：直接对整段响应测速 ----
        if not is_hls and res.container:
            return await self._measure_direct(session, t, url, res)

        res.is_hls = True
        if extinf_count >= 10:
            res.hint = f"该地址含{extinf_count}个#EXTINF，疑似频道列表(m3u)，建议改用 type=list"
        if not candidates:
            res.error = "empty_playlist"
            return res

        # F9: 分片选取策略 + 失败降级
        ordered = self._order_segments(candidates)
        last_err = "empty_segment"
        tried = 0
        for seg in ordered:
            tried += 1
            r = await self._measure_segment(session, t, seg, res)
            if r.ok:
                res.segments_tried = tried
                return r
            last_err = r.error or last_err
            if tried >= 3:  # 最多降级尝试 3 个候选
                break
        res.segments_tried = tried
        res.error = last_err
        return res

    def _order_segments(self, segs: List[str]) -> List[str]:
        if self.segment_strategy == "first":
            return segs[:3]
        if self.segment_strategy == "middle":
            mid = len(segs) // 2
            return [segs[mid]] + segs[:mid] + segs[mid + 1:]
        # last：直播滚动窗口末端最新
        return list(reversed(segs))[:3]

    async def _measure_direct(self, session, t: Target, url: str, res: Result) -> Result:
        """裸 TS/FLV/MP4 直连流测速。"""
        resp, patch, _ = await self._request(session, t, url)
        if resp is None:
            res.error = f"stream_{patch.get('error', 'other')}"
            return res
        try:
            if resp.status >= 400:
                res.error = f"stream_http_{resp.status // 100}xx"
                return res
            total, first_byte_at, deadline = 0, None, time.monotonic() + self.max_seconds
            t0 = time.monotonic()
            try:
                async for chunk in resp.content.iter_chunked(64 * 1024):
                    if first_byte_at is None:
                        first_byte_at = time.monotonic()
                    # F10: 先判后加，精确封顶
                    take = min(len(chunk), self.max_bytes - total)
                    if take <= 0:
                        break
                    total += take
                    if total >= self.max_bytes or time.monotonic() >= deadline:
                        break
            except Exception as e:
                res.error = f"stream_{self._classify_error(e)}"
                return res
            now = time.monotonic()
            res.segment_url = str(resp.url)
            res.segment_bytes = total
            res.segment_ttfb_ms = (
                int((first_byte_at - t0) * 1000) if first_byte_at else None)
            res.duration_s = round(now - t0, 3)
            res.speed_avg_kbps = round(total * 8 / 1000 / max(now - t0, 1e-6), 1)
            # F8: 纯传输速率
            if first_byte_at and total > 0:
                trans = max(now - first_byte_at, 1e-6)
                res.speed_kbps = round(total * 8 / 1000 / trans, 1)
            else:
                res.speed_kbps = res.speed_avg_kbps
            res.ok = total > 0
            if not res.ok:
                res.error = "empty_stream"
            if res.container == "ts":
                res.hint = (res.hint or "") or "直连 TS 流（非 HLS），播放器需支持 MPEG-TS over HTTP"
            return res
        finally:
            resp.release()

    async def _measure_segment(self, session, t: Target, seg_url: str, base: Result) -> Result:
        """测一个 HLS 分片。返回新的 Result（成功时携带完整信息）。"""
        res = base
        resp, patch, _ = await self._request(session, t, seg_url)
        if resp is None:
            res.error = f"segment_{patch.get('error', 'other')}"
            return res
        try:
            if resp.status >= 400:
                res.error = f"segment_http_{resp.status // 100}xx"
                return res
            total, first_byte_at, deadline = 0, None, time.monotonic() + self.max_seconds
            t0 = time.monotonic()
            try:
                async for chunk in resp.content.iter_chunked(64 * 1024):
                    if first_byte_at is None:
                        first_byte_at = time.monotonic()
                    take = min(len(chunk), self.max_bytes - total)
                    if take <= 0:
                        break
                    total += take
                    if total >= self.max_bytes or time.monotonic() >= deadline:
                        break
            except Exception as e:
                res.error = f"segment_{self._classify_error(e)}"
                return res
            now = time.monotonic()
            res.segment_url = seg_url
            res.segment_bytes = total
            # container 的语义：仅当整个探测链路是直连流时才用嗅探结果；
            # HLS 场景下分片是 TS 属于正常现象，不应因此改写 container。
            res.container = "hls"
            res.segment_ttfb_ms = (
                int((first_byte_at - t0) * 1000) if first_byte_at else None)
            res.duration_s = round(now - t0, 3)
            res.speed_avg_kbps = round(total * 8 / 1000 / max(now - t0, 1e-6), 1)
            if first_byte_at and total > 0:
                trans = max(now - first_byte_at, 1e-6)
                res.speed_kbps = round(total * 8 / 1000 / trans, 1)
            else:
                res.speed_kbps = res.speed_avg_kbps
            res.ok = total > 0
            if not res.ok:
                res.error = "empty_segment"
            return res
        finally:
            resp.release()

    # ------------------------------------------------------------ 解析
    @staticmethod
    def _parse_segments(text: str, base_url: str) -> List[str]:
        out = []
        for ln in text.splitlines():
            ln = ln.strip()
            if not ln or ln.startswith("#"):
                continue
            out.append(urljoin(base_url, ln))
        return out

    @staticmethod
    def _parse_variants(text: str, base_url: str) -> List[Dict[str, Any]]:
        """解析 EXT-X-STREAM-INF，返回 [{url, bandwidth, resolution}]（F7）。"""
        out: List[Dict[str, Any]] = []
        lines = text.splitlines()
        pending: Optional[Dict[str, Any]] = None
        for ln in lines:
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
                    out.append({
                        "url": urljoin(base_url, s),
                        "bandwidth": pending["bandwidth"],
                        "resolution": pending["resolution"],
                    })
                    pending = None
        return out

    def _pick_variant(self, variants: List[Dict[str, Any]]) -> Tuple[str, Optional[int]]:
        with_bw = [v for v in variants if v.get("bandwidth")]
        if not with_bw:
            v = variants[0]
            return v["url"], v.get("bandwidth")
        if self.variant_strategy == "min":
            v = min(with_bw, key=lambda x: x["bandwidth"])
        elif self.variant_strategy == "first":
            v = variants[0]
        else:  # max
            v = max(with_bw, key=lambda x: x["bandwidth"])
        return v["url"], v.get("bandwidth")

    # ------------------------------------------------------------ 编排
    async def run(self) -> List[Result]:
        # 位置跳过：云端不探内网源，避免"运营商封外省"被误报为源故障
        skipped: Dict[str, Result] = {}
        active: List[Target] = []
        for t in self.targets:
            if self._should_skip(t):
                skipped[t.id] = self._skip_result(t)
                self._log(f"[skip] {t.id} {t.name} 内网源，云端跳过")
            else:
                active.append(t)

        if not active:
            return [skipped[t.id] for t in self.targets if t.id in skipped]

        connector = aiohttp.TCPConnector(
            limit=self.concurrency * 2,
            limit_per_host=self.per_host + 1,
            enable_cleanup_closed=True,
            ttl_dns_cache=300,
            ssl=False if self.insecure else None,
        )
        async with aiohttp.ClientSession(
            connector=connector, headers={"User-Agent": UA},
            trace_configs=[self._trace],
        ) as session:

            async def guarded(t: Target) -> Result:
                try:
                    if t.kind == "list":
                        r = await self._probe_list(session, t)
                    elif t.kind == "head":
                        r = await self._probe_head(session, t)
                    else:
                        r = await self._probe_stream(session, t)
                except asyncio.CancelledError:
                    r = self._base_result(t)
                    r.error = "cancelled"
                    raise
                except Exception as e:
                    r = self._base_result(t)
                    r.error = self._classify_error(e)
                self._log(f"[done] {t.id} {t.name} ok={r.ok} err={r.error} "
                          f"speed={r.speed_kbps}")
                return r

            # F1: 保证每个目标都有结果，绝不让慢目标从报告里消失
            tasks = [asyncio.create_task(guarded(t)) for t in active]
            try:
                results = await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True),
                    timeout=self.total_timeout,
                )
            except asyncio.TimeoutError:
                results = []
                for t, task in zip(active, tasks):
                    if task.done() and not task.cancelled():
                        try:
                            results.append(task.result())
                        except Exception as e:
                            r = self._base_result(t)
                            r.error = self._classify_error(e)
                            results.append(r)
                    else:
                        task.cancel()
                        r = self._base_result(t)
                        r.error = "global_timeout"
                        r.hint = f"超出整体超时 {self.total_timeout}s，未完成"
                        results.append(r)
                # 给被取消的任务一个事件循环回合做清理
                await asyncio.sleep(0)

            final: List[Result] = []
            for t, item in zip(active, results):
                if isinstance(item, Result):
                    final.append(item)
                elif isinstance(item, BaseException):
                    r = self._base_result(t)
                    r.error = self._classify_error(item)
                    final.append(r)
                else:
                    r = self._base_result(t)
                    r.error = "unknown"
                    final.append(r)

        # 按原始 targets 顺序合入跳过项，保证目标数守恒且顺序稳定
        by_id = {r.id: r for r in final}
        by_id.update(skipped)
        return [by_id[t.id] for t in self.targets if t.id in by_id]


# ================================================================ 历史对比
def find_latest_report(report_dir: Optional[str], exclude_date: str) -> Optional[str]:
    if not report_dir or not os.path.isdir(report_dir):
        return None
    best: Optional[tuple] = None
    for fn in os.listdir(report_dir):
        m = re.fullmatch(r"iptv_(\d{8})\.json", fn)
        if not m:
            continue
        d = m.group(1)
        if d >= exclude_date:
            continue
        if best is None or d > best[0]:
            best = (d, os.path.join(report_dir, fn))
    return best[1] if best else None


def apply_changes(results: List[Result], prev_path: str,
                  location: Optional[str] = None) -> None:
    """
    按 (id, location) 与上一份报告对比，写入 change 字段。

    - 对比键含 location：云端报告与本地报告若有相同 id，互不污染判定。
    - 跳过项（skip_cloud）不参与对比，change 保持 None。
    - 历史报告中 location 缺失时视为同 location（兼容 v4.0 旧报告）。
    """
    try:
        with open(prev_path, encoding="utf-8") as f:
            prev = json.load(f)
    except Exception:
        return

    loc = location or prev.get("probe_location")
    prev_map = {}
    for t in prev.get("targets", []):
        tid = t.get("id")
        if not tid:
            continue
        prev_loc = t.get("probe_location")
        # 旧报告无 probe_location：按当前 location 归并，保持向后兼容
        if prev_loc is None:
            prev_loc = loc
        if loc is not None and prev_loc != loc:
            continue  # 位置不同，不可比
        prev_map[tid] = t

    for r in results:
        if r.skipped:
            r.change = None
            continue
        p = prev_map.get(r.id)
        if p is None:
            r.change = "new"
        elif p.get("error") == SKIP_CLOUD or p.get("ok") is None:
            r.change = "new"  # 上次被跳过，本次首次真实探测
        elif p.get("ok") and r.ok:
            r.change = "still_ok"
        elif not p.get("ok") and r.ok:
            r.change = "recovered"
        elif p.get("ok") and not r.ok:
            r.change = "broken"
        else:
            r.change = "still_fail"


# ================================================================ 双位置合并
# 双位置鉴别矩阵（核心增值）：把"不通过"翻译成"该做什么"
MATRIX = {
    ("ok", "ok"): {
        "verdict": "all_pass",
        "label": "全通",
        "level": "normal",
        "action": "无需动作",
        "alarm": False,
    },
    ("ok", "fail"): {
        "verdict": "local_channel_blocked",
        "label": "本地通道被墙/DNS 异常",
        "level": "level2",
        "action": "二级战备：切换 gh-proxy / kkgithub 通道",
        "alarm": True,
    },
    ("fail", "ok"): {
        "verdict": "source_geo_blocked",
        "label": "源对海外封锁，内网存活",
        "level": "normal",
        "action": "移动源健康常态，禁止告警",
        "alarm": False,
    },
    ("fail", "fail"): {
        "verdict": "source_dead",
        "label": "源真死",
        "level": "level1",
        "action": "一级战备：切个人 Fork",
        "alarm": True,
    },
}

# 出现以下状态说明该位置数据不可用，不做定性
_MATRIX_UNKNOWN = {
    "verdict": "unknown",
    "label": "数据不足",
    "level": "unknown",
    "action": "补齐两端探针数据后再判定",
    "alarm": False,
}

# intranet 源的特例：云端必然 skip（运营商封外省），本地可达即为「正常」。
# 这一格**绝不能告警** —— 它是移动源的健康常态，不是故障。
_MATRIX_INTRANET_OK = {
    "verdict": "intranet_alive",
    "label": "内网源存活（云端不可测属正常）",
    "level": "normal",
    "action": "移动源健康常态，禁止告警",
    "alarm": False,
}

_MATRIX_INTRANET_DEAD = {
    "verdict": "intranet_dead",
    "label": "内网源真死",
    "level": "level1",
    "action": "一级战备：内网源失效，切个人 Fork / 备用线路",
    "alarm": True,
}


def _state_of(_id: str, results: Dict[tuple, Dict[str, Any]]) -> str:
    """
    把某位置的结果归一化为 ok / fail / skip / unknown。

    skip 与 unknown 语义不同，必须区分：
      skip    —— 该位置**主动**跳过（云端遇 intranet 源），预期行为；
      unknown —— 该位置压根没数据（报告缺失/目标遗漏），需补齐。
    """
    item = results.get(_id)
    if item is None:
        return "unknown"
    if item.get("error") == SKIP_CLOUD:
        return "skip"
    return "ok" if item.get("ok") else "fail"


def _resolve_matrix(tid: str, scope: str, cs: str, ls: str) -> Dict[str, Any]:
    """把 (cloud_state, local_state) 翻译成定性结论。"""
    # intranet 源特例优先：云端 skip 是设计使然，不能按「数据不足」处理
    if scope == SCOPE_INTRANET:
        if ls == "ok":
            return dict(_MATRIX_INTRANET_OK)
        if ls == "fail":
            return dict(_MATRIX_INTRANET_DEAD)
        return dict(_MATRIX_UNKNOWN)

    # public 源：skip 与 unknown 都按「该位置无数据」处理
    cs_n = "unknown" if cs == "skip" else cs
    ls_n = "unknown" if ls == "skip" else ls
    if cs_n == "unknown" or ls_n == "unknown":
        return dict(_MATRIX_UNKNOWN)
    return dict(MATRIX[(cs_n, ls_n)])


def merge_reports(cloud_path: Optional[str], local_path: Optional[str]) -> Dict[str, Any]:
    """
    合并云端与本地两份报告，按双位置矩阵定性。

    返回结构：
      {
        "version": "v4.1",
        "matrices": [ {id, name, kind, scope, cloud, local,
                       verdict, label, level, action, alarm}, ... ],
        "summary": { counts per verdict, alarms[], level1[], level2[] },
        "cloud": <原报告或 None>, "local": <原报告或 None>,
        "targets": [...]  # 与 matrices 同构
      }
    """
    def _load(path):
        if not path or not os.path.isfile(path):
            return None
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None

    cloud_rep = _load(cloud_path)
    local_rep = _load(local_path)

    def _index(rep):
        out = {}
        if not rep:
            return out
        for t in rep.get("targets", []):
            if t.get("id"):
                out[t["id"]] = t
        return out

    cloud_idx, local_idx = _index(cloud_rep), _index(local_rep)

    # 目标全集：两边并集，保持稳定顺序
    ids: List[str] = []
    for rep in (cloud_rep, local_rep):
        if not rep:
            continue
        for t in rep.get("targets", []):
            if t.get("id") and t["id"] not in ids:
                ids.append(t["id"])

    rows: List[Dict[str, Any]] = []
    verdict_count: Dict[str, int] = {}
    alarms, level1, level2 = [], [], []
    suppressed: List[str] = []

    # R10：网络基线压制。基线失败说明故障在本机网络/宽带，此时任何源告警都是
    # **归因错误**（把「家里断网」读成「源挂了」）。故先算网络结论，再决定是否压制。
    # 注意：基线组自身永远不进告警集合，只做上下文。
    local_net = ((local_rep or {}).get("summary") or {}).get("network") or {}
    # 本地报告缺失时，从本地 targets 现算一次，避免旧版报告无 network 字段就失去压制能力
    if not local_net and local_rep:
        _lt = [Result(**{k: v for k, v in t.items()
                         if k in Result.__dataclass_fields__})
               for t in local_rep.get("targets", [])]
        if _lt:
            local_net = assess_network(_lt)
    suppress_all = bool(local_net.get("suppress"))

    for tid in ids:
        c, l = cloud_idx.get(tid), local_idx.get(tid)
        base = c or l or {}
        cs, ls = _state_of(tid, cloud_idx), _state_of(tid, local_idx)

        scope = base.get("scope", SCOPE_PUBLIC)
        # 基线组不是「源」，单列上下文，不参与矩阵定性/告警
        if scope == SCOPE_BASELINE:
            continue
        m = _resolve_matrix(tid, scope, cs, ls)

        verdict_count[m["verdict"]] = verdict_count.get(m["verdict"], 0) + 1

        row = {
            "id": tid,
            "name": base.get("name", ""),
            "kind": base.get("kind"),
            "scope": scope,
            "cloud": {
                "state": cs,
                "ok": (c or {}).get("ok"),
                "error": (c or {}).get("error"),
                "speed_kbps": (c or {}).get("speed_kbps"),
                "ttfb_ms": (c or {}).get("ttfb_ms"),
                "probe_location": (c or {}).get("probe_location"),
            },
            "local": {
                "state": ls,
                "ok": (l or {}).get("ok"),
                "error": (l or {}).get("error"),
                "speed_kbps": (l or {}).get("speed_kbps"),
                "ttfb_ms": (l or {}).get("ttfb_ms"),
                "probe_location": (l or {}).get("probe_location"),
            },
            "cloud_local": f"{cs}/{ls}",
            **{k: m[k] for k in ("verdict", "label", "level", "action", "alarm")},
        }
        # 网络故障期：告警降级为「待复核」，并记录原因，避免误指源
        if suppress_all and m["alarm"]:
            row["alarm_original"] = True
            row["alarm"] = False
            row["suppressed_reason"] = local_net.get("semantic")
            row["level"] = "pending"
            row["action"] = f"{local_net.get('action')}；源状态待网络恢复后复核"
            suppressed.append(tid)
        rows.append(row)

        if row["alarm"]:
            alarms.append(tid)
        if row["level"] == "level1":
            level1.append(tid)
        if row["level"] == "level2":
            level2.append(tid)

    _lt_results = ([Result(**{k: v for k, v in t.items()
                              if k in Result.__dataclass_fields__})
                    for t in (local_rep or {}).get("targets", [])]
                   if local_rep else [])
    _lt_net_state = local_net.get("state")
    _lt_net_ok: Optional[bool] = (
        True if _lt_net_state == NET_OK
        else False if _lt_net_state in (NET_LAN_DOWN, NET_WAN_DOWN)
        else None)

    return {
        "version": "v4.2",
        "timestamp": datetime.now().isoformat(),
        "matrices": rows,
        "network": local_net,
        "intranet": (assess_intranet(_lt_results, network_ok=_lt_net_ok)
                     if local_rep else {}),
        "summary": {
            "total": len(rows),
            "verdicts": verdict_count,
            "alarms": alarms,
            "level1": level1,
            "level2": level2,
            "alarm_count": len(alarms),
            "suppressed": suppressed,
            "suppressed_count": len(suppressed),
            "network_state": local_net.get("state") or NET_UNKNOWN,
        },
        "cloud": cloud_rep,
        "local": local_rep,
        # 与 v4 单位置报告兼容的 targets 视图
        "targets": rows,
    }


def render_merge_html(merged: Dict[str, Any]) -> str:
    """双位置对照 HTML：左云端视角、右家里视角。"""
    s = merged["summary"]
    css = (
        "body{font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;"
        "margin:24px;color:#222}"
        "table{border-collapse:collapse;width:100%;margin-top:12px}"
        "th,td{border:1px solid #ddd;padding:8px 10px;text-align:left;font-size:14px}"
        "th{background:#f5f5f5}"
        ".lvl-normal td{background:#f0faf0}"
        ".lvl-level2 td{background:#fff8e1}"
        ".lvl-level1 td{background:#fdf2f2}"
        ".lvl-pending td{background:#eef3f8}"
        ".lvl-unknown td{background:#f7f7f7;color:#888}"
        ".badge{padding:2px 8px;border-radius:10px;font-size:12px;font-weight:600}"
        ".b-normal{background:#e6f4ea;color:#1a7f37}"
        ".b-level2{background:#fff3cd;color:#8a6100}"
        ".b-level1{background:#fdecea;color:#c62828}"
        ".b-pending{background:#e4ecf5;color:#4a6b8a}"
        ".b-unknown{background:#eee;color:#666}"
        ".skipnote{color:#4a6b8a;font-size:12px}"
        ".col{display:inline-block;width:48%;vertical-align:top;margin-right:2%}"
        ".hint{color:#666;font-size:12px;margin-top:4px}"
        ".panel{border:1px solid #e0e0e0;border-radius:8px;padding:10px 14px;"
        "margin:10px 0;background:#fafbfc;font-size:13px}"
    )
    rows = []
    for r in merged["matrices"]:
        cv, lv = r["cloud"], r["local"]

        def cell(v):
            if v["state"] == "unknown":
                return "<span style='color:#999'>未探测</span>"
            if v["state"] == "skip":
                return ("<span class='badge b-pending'>云端免测</span>"
                        "<div class='skipnote'>内网源云端不可测，非故障</div>")
            mark = "✅" if v["state"] == "ok" else "❌"
            spd = (f"{v['speed_kbps']} kbps" if v.get("speed_kbps") is not None else "-")
            err = html.escape(str(v.get("error") or ""))
            return f"{mark} {spd}<div class='hint'>{err}</div>"

        sup = ("<div class='skipnote'>⚠ 已被抑制："
               + html.escape(str(r.get("suppressed_reason") or "")) + "</div>"
               if r.get("suppressed_reason") else "")
        rows.append(
            f"<tr class='lvl-{html.escape(r['level'])}'>"
            f"<td>{html.escape(str(r['id']))}</td>"
            f"<td>{html.escape(str(r['name']))}</td>"
            f"<td>{html.escape(str(r['scope']))}</td>"
            f"<td>{cell(cv)}</td><td>{cell(lv)}</td>"
            f"<td><span class='badge b-{html.escape(r['level'])}'>"
            f"{html.escape(r['label'])}</span>{sup}</td>"
            f"<td>{html.escape(r['action'])}</td></tr>"
        )

    # R10 上下文面板：网络基线 + 内网冗余
    net = merged.get("network") or {}
    intra = merged.get("intranet") or {}
    net_cls = ("b-normal" if net.get("state") == NET_OK
               else "b-level1" if net.get("suppress") else "b-pending")
    intra_cls = ("b-normal" if intra.get("status") in ("intranet_ok", "intranet_alive")
                 else "b-pending" if intra.get("status") in ("partial", "no_data", None)
                 else "b-level2")
    panels = ""
    if net:
        panels += (f"<div class='panel'><b>网络基线（本机网络是否健康）：</b> "
                   f"<span class='badge {net_cls}'>{html.escape(str(net.get('state') or '-'))}</span> "
                   f"{html.escape(str(net.get('semantic') or net.get('label') or ''))}"
                   f"<div class='hint'>{html.escape(str(net.get('action') or net.get('detail') or ''))}</div></div>")
    if intra:
        panels += (f"<div class='panel'><b>内网冗余（区分单节点/整域/内网层）：</b> "
                   f"<span class='badge {intra_cls}'>{html.escape(str(intra.get('status') or '-'))}</span> "
                   f"{html.escape(str(intra.get('semantic') or intra.get('label') or ''))}"
                   f"<div class='hint'>{html.escape(str(intra.get('action') or intra.get('detail') or ''))}</div></div>")

    alarm_line = ""
    if s.get("suppressed"):
        alarm_line += (f"<p class='skipnote'>⏸ {len(s['suppressed'])} 个源告警因本机网络异常被"
                       f"<b>抑制</b>（{html.escape(', '.join(s['suppressed']))}）："
                       f"故障在{html.escape(str(s.get('network_state') or ''))}，不是源，"
                       f"网络恢复后需复核</p>")
    if s["level1"]:
        alarm_line += (f"<p style='color:#c62828;font-weight:600'>"
                       f"🔴 一级战备：{html.escape(', '.join(s['level1']))}</p>")
    if s["level2"]:
        alarm_line += (f"<p style='color:#8a6100;font-weight:600'>"
                       f"🟡 二级战备：{html.escape(', '.join(s['level2']))}</p>")
    if not alarm_line:
        alarm_line = "<p style='color:#1a7f37'>🟢 无告警</p>"

    return (
        "<!doctype html><html><head><meta charset='utf-8'>"
        "<title>IPTV 双位置鉴别报告</title><style>" + css + "</style></head><body>"
        "<h1>IPTV 双位置鉴别报告</h1>"
        f"<p>时间：{html.escape(str(merged['timestamp']))}　目标 {s['total']} 个　"
        f"告警 {s['alarm_count']} 个</p>"
        + panels + alarm_line +
        "<table><tr><th>ID</th><th>名称</th><th>域</th>"
        "<th>云端视角</th><th>家里视角</th><th>定性</th><th>处置</th></tr>"
        + "".join(rows) + "</table>"
        "<p class='hint'>矩阵规则：cloud✓+local✓=全通；cloud✓+local✗=本地通道被墙（二级战备）；"
        "cloud✗+local✓=源对海外封锁但内网存活（正常，禁止告警）；cloud✗+local✗=源真死（一级战备）。"
        "「云端免测」=内网源云端主动跳过（非故障）；「已被抑制」=本机网络异常导致的告警降级，非源结论。</p>"
        "</body></html>"
    )
# ================================================================ 报告
# ================================================================ 网络健康（R10）
def assess_network(results: List[Result]) -> Dict[str, Any]:
    """
    依据 baseline 组（N1 默认网关 / N2 公网基线）判定本地网络健康。

    判定规则：
      N1 fail                        → lan_down  「家里局域网/路由器问题」→ 抑制全部源告警
      N1 ok + N2 fail                → wan_down  「宽带断网/运营商故障」
      N1 ok + N2 ok                  → net_ok    网络正常，源失败才是真源问题
      基线缺失                        → net_unknown

    返回 {"status", "label", "suppress_alarms", "detail"}
    """
    by_id = {r.id: r for r in results}
    n1, n2 = by_id.get("N1"), by_id.get("N2")

    if n1 is None and n2 is None:
        return {"status": NET_UNKNOWN, "state": NET_UNKNOWN, "label": "无网络基线数据",
                "semantic": "网络基线缺失或不足，无法判定本机网络健康",
                "action": "不抑制源告警（无基线时不拿它当挡箭牌）",
                "suppress_alarms": False, "suppress": False, "detail": "未探测 baseline 组"}

    n1_ok = bool(n1 and n1.ok)
    n2_ok = bool(n2 and n2.ok)

    if n1 is not None and not n1_ok:
        return {
            "status": NET_LAN_DOWN, "state": NET_LAN_DOWN,
            "label": "家里局域网/路由器问题",
            "semantic": "家里 LAN / 路由器故障（默认网关不可达）",
            "action": "先查路由器与网线，源侧结论本次无效",
            "suppress_alarms": True, "suppress": True,
            "detail": f"N1 默认网关不可达（{n1.error or 'unknown'}）——"
                      f"此时所有源失败都可能是本地网络导致，已抑制源告警",
        }
    if n2 is not None and not n2_ok:
        return {
            "status": NET_WAN_DOWN, "state": NET_WAN_DOWN,
            "label": "宽带断网/运营商故障",
            "semantic": "宽带 / 运营商故障（网关通、公网不通）",
            "action": "查宽带线路，源侧结论本次无效",
            "suppress_alarms": True, "suppress": True,
            "detail": f"N1 网关可达但 N2 公网基线不可达（{n2.error or 'unknown'}）——"
                      f"宽带出口异常，源失败非源侧问题",
        }
    if n1_ok and n2_ok:
        return {"status": NET_OK, "state": NET_OK, "label": "本地网络正常",
                "semantic": "本机网络健康（网关与公网均可达）",
                "action": "源告警可信",
                "suppress_alarms": False, "suppress": False,
                "detail": "N1/N2 均可达，源失败可判定为真源问题"}
    return {"status": NET_UNKNOWN, "state": NET_UNKNOWN, "label": "网络基线不完整",
            "semantic": "网络基线缺失或不足，无法判定本机网络健康",
            "action": "不抑制源告警（无基线时不拿它当挡箭牌）",
            "suppress_alarms": False, "suppress": False,
            "detail": "部分基线缺失，按不抑制处理"}


def assess_intranet(results: List[Result],
                    network_ok: Optional[bool] = None) -> Dict[str, Any]:
    """
    内网 + 生态域判定（R10 → R18 语义重命名）：区分「单节点」「CDN 层」「生态层」故障。

    R18 变更：B2i 的靶从**内网 EPG 域**换成**公网生态域**，因此"B2i 可达"
    的证明力变了 —— 旧注释说它证明"移动内网可达"，**新语义下它只证明
    "运营商生态域公网可达"**。所以状态名与文案同步重命名，让"结论即指路"。

    判定链（N = 网络基线，可选传入用于 eco_all_down）：

      B1 ok                                  → intranet_ok    源正常
      B1 fail, B1b ok                        → single_node    单节点故障，切 B1b
      B1/B1b fail, B2i ok                    → cdn_down       CDN 流死但生态域可达
                                                              → 查 UA/Referer（三/四级战备）
      B1/B1b fail, B2i fail, N ok            → eco_down       生态域不可达
                                                              → 查 DNS/路由（二级战备）
      B1/B1b fail, B2i fail, N fail          → eco_all_down   连网络基线都挂
                                                              → 一级战备 + 报障运营商

    兼容别名：`domain_down` 对应 cdn_down、`intranet_down` 对应 eco_down/eco_all_down，
    保留一版以免看板/手机侧的旧解析崩掉（见 label 文案已换新语义）。

    Args:
        results: 全部探测结果（含 ecosystem 组的 B2i —— 由调用方保证传入全量）。
        network_ok: 网络基线（N1/N2）是否健康；None 表示未知。

    Returns:
        含 status / label / detail 的 dict。
    """
    by_id = {r.id: r for r in results}
    b1, b1b, b2i = by_id.get("B1"), by_id.get("B1b"), by_id.get("B2i")
    if b1 is None and b1b is None:
        return {"status": "no_data", "label": "无内网探测数据", "detail": ""}

    b1_ok = bool(b1 and b1.ok)
    b1b_ok = bool(b1b and b1b.ok)
    # B2i 可能不在本组结果里（云端会 skip 它）。None 表示"未测"，
    # 与"测了但失败"（False）必须区分 —— 否则云端会把"没测"误报成"生态域挂了"。
    b2i_ok = bool(b2i and b2i.ok) if b2i is not None else None

    if b1_ok and (b1b is None or b1b_ok):
        return {"status": "intranet_ok", "label": "内网源正常", "detail": ""}
    if not b1_ok and b1b is not None and b1b_ok:
        return {"status": "single_node", "label": "单节点故障（B1 挂、B1b 通）",
                "detail": "同域另一频道可达 → 切 B1b 副链，非全域故障"}
    if not b1_ok and b1b is not None and not b1b_ok:
        if b2i_ok:
            return {"status": "cdn_down",
                    "label": "CDN 层故障（生态域可达）",
                    "detail": "B1/B1b 全挂但生态域可达 → 网络层通、CDN 业务层故障；"
                              "查 UA/Referer 或切备用域（三/四级战备）",
                    "alias": "domain_down"}
        if b2i_ok is False:
            if network_ok is False:
                return {"status": "eco_all_down",
                        "label": "移动生态整体不可达（含网络基线）",
                        "detail": "B2i/B1/B1b 全挂且网络基线也失败 → "
                                  "一级战备：核查本机网络并报障运营商",
                        "alias": "intranet_down"}
            return {"status": "eco_down",
                    "label": "移动生态域不可达",
                    "detail": "连运营商公网生态域都不可达（网络基线尚可）→ "
                              "疑似 DNS/路由/污染，二级战备：换 DNS 或切通道",
                    "alias": "intranet_down"}
    return {"status": "partial", "label": "内网状态不完整", "detail": ""}


def build_report(results: List[Result], meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    构建报告。关键规则：
      - error=skip_cloud 的结果（云端跳过内网源）不计入 fail 计数；
      - 统计同时给出 public / intranet 分组，便于双位置合并；
      - F16 退出码只看 scope=public 的流源（见 main）。
    """
    def _count(pred) -> int:
        return sum(1 for r in results if pred(r))

    # 参与"有效统计"的结果：跳过项不算失败；baseline 组不计入源统计
    counted = [r for r in results if not r.skipped]
    alertable = [r for r in counted if r.scope not in ALERT_EXEMPT_SCOPES]

    ok_list = _count(lambda r: r.kind == "list" and r.ok and r.scope not in ALERT_EXEMPT_SCOPES)
    ok_stream = _count(lambda r: r.kind == "stream" and r.ok and r.scope not in ALERT_EXEMPT_SCOPES)
    total_list = _count(lambda r: r.kind == "list" and not r.skipped
                        and r.scope not in ALERT_EXEMPT_SCOPES)
    total_stream = _count(lambda r: r.kind == "stream" and not r.skipped
                          and r.scope not in ALERT_EXEMPT_SCOPES)

    # 分组统计（public / intranet / baseline）
    scope_stats: Dict[str, Dict[str, int]] = {}
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

    # R10→R18: 网络健康 + 内网/生态冗余判定
    net = assess_network(results)
    # 把网络基线的结论喂给内网判定 —— 用来区分 eco_down（网络还行、生态域挂）
    # 与 eco_all_down（连网络基线都挂）。少了这个参数，两者会被混为一谈。
    net_state = net.get("state")
    if net_state == NET_OK:
        net_ok: Optional[bool] = True
    elif net_state in (NET_LAN_DOWN, NET_WAN_DOWN):
        net_ok = False
    else:
        net_ok = None      # 基线数据缺失 → 未知，不强行归类
    intra = assess_intranet(results, network_ok=net_ok)

    rep: Dict[str, Any] = {
        "version": "v4.2",
        "timestamp": datetime.now().isoformat(),
        "probe_location": (meta or {}).get("location"),
        "summary": {
            "total": len(results),
            "counted": len(counted),
            "skipped": len(results) - len(counted),
            "alertable": len(alertable),
            "ok_list": ok_list,
            "fail_list": total_list - ok_list,
            "total_list": total_list,
            "ok_stream": ok_stream,
            "fail_stream": total_stream - ok_stream,
            "total_stream": total_stream,
            "scopes": scope_stats,
            "network": net,
            "intranet": intra,
            "changes": {
                "recovered": [r.id for r in results if r.change == "recovered"],
                "broken": [r.id for r in results if r.change == "broken"],
                "new": [r.id for r in results if r.change == "new"],
            },
        },
        "targets": [r.to_dict() for r in results],
    }
    if meta:
        rep["meta"] = meta
    return rep


def sort_results(results: List[Result], key: str) -> List[Result]:
    def k(r: Result):
        if key == "speed":
            return (r.speed_kbps is None, -(r.speed_kbps or 0))
        if key == "latency":
            v = r.latency_ms if r.kind == "list" else r.ttfb_ms
            return (v is None, v if v is not None else 1 << 30)
        return (r.id,)
    return sorted(results, key=k)


def render_html(report: Dict[str, Any]) -> str:
    s = report["summary"]
    loc = report.get("probe_location") or (report.get("meta") or {}).get("location") or "-"
    rows = []
    for r in report["targets"]:
        skipped = r.get("error") == SKIP_CLOUD
        if skipped:
            status_cls = "skip"
        else:
            status_cls = "ok" if r["ok"] else "fail"
        if r["kind"] == "list":
            detail = (f"频道数 {r['channel_count']} / {r.get('encoding') or '-'}"
                      if r["channel_count"] is not None else "-")
            lat = f"{r['latency_ms']}ms" if r["latency_ms"] is not None else "-"
        else:
            cont = r.get("container") or "-"
            spd = r.get("speed_kbps")
            detail = (f"{cont} · {spd} kbps / {r['segment_bytes']}B"
                      if spd is not None else f"{cont} · -")
            lat = f"{r['ttfb_ms']}ms" if r.get("ttfb_ms") is not None else "-"
        err = html.escape(str(r.get("error") or ""))
        if skipped:
            # R11：skip_cloud 必须与 ok(绿)/fail(红) 三色可辨，独立灰蓝徽章「云端免测」。
            # 用文字徽章而非仅改行背景色——人眼扫表时背景色差异弱于彩色 pill。
            state = "<span class='badge badge-skip'>云端免测</span>"
        elif r["ok"]:
            state = "<span class='badge badge-ok'>可用</span>"
        else:
            state = "<span class='badge badge-fail'>失败</span>"
        chg = CHANGE_LABELS.get(r.get("change") or "", "-")
        hint = html.escape(str(r.get("hint") or ""))
        if hint:
            err += f'<div class="hint">{hint}</div>'
        red = r.get("redirects")
        red_txt = f" ↻{red}" if red else ""
        rows.append(
            f"<tr class='{status_cls}'><td>{html.escape(str(r['id']))}</td>"
            f"<td>{html.escape(str(r['name']))}</td>"
            f"<td>{html.escape(str(r.get('scope') or SCOPE_PUBLIC))}</td>"
            f"<td>{html.escape(str(r['kind']))}</td>"
            f"<td>{html.escape(str(r['status'] or '-'))}{red_txt}</td>"
            f"<td>{html.escape(lat)}</td>"
            f"<td>{html.escape(detail)}</td>"
            f"<td class='state'>{state}</td><td>{chg}</td>"
            f"<td>{err}</td></tr>"
        )
    chg_line = ""
    c = s.get("changes") or {}
    if c.get("recovered") or c.get("broken") or c.get("new"):
        chg_line = (f"<p>较上次：恢复 {len(c.get('recovered', []))}、"
                    f"中断 {len(c.get('broken', []))}、新增 {len(c.get('new', []))}</p>")
    if loc == LOC_CLOUD:
        chg_line += ("<p class='hint'>⚠ 本报告为<strong>云端视角</strong>：intranet 源已跳过"
                     "（运营商封外省，云端探测无意义）。内网选源请以本地探针 speed_kbps 为准。</p>")
    # 分组统计行
    sc = s.get("scopes") or {}
    pub = sc.get(SCOPE_PUBLIC) or {}
    intra = sc.get(SCOPE_INTRANET) or {}
    scope_line = (
        f"<p>public：列表 {pub.get('ok_list', 0)}/{pub.get('total_list', 0)}　"
        f"流 {pub.get('ok_stream', 0)}/{pub.get('total_stream', 0)}"
        + (f"　|　intranet：列表 {intra.get('ok_list', 0)}/{intra.get('total_list', 0)}　"
           f"流 {intra.get('ok_stream', 0)}/{intra.get('total_stream', 0)}"
           if intra.get("total") else "")
        + "</p>"
    )
    css = (
        "body{font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;"
        "margin:24px;color:#222}"
        "table{border-collapse:collapse;width:100%;margin-top:12px}"
        "th,td{border:1px solid #ddd;padding:8px 10px;text-align:left;font-size:14px}"
        "th{background:#f5f5f5}.ok td{background:#f0faf0}.fail td{background:#fdf2f2}"
        ".skip td{background:#eef3f8;color:#5b7a99}"
        ".badge{display:inline-block;padding:2px 8px;border-radius:10px;"
        "font-size:12px;font-weight:600;white-space:nowrap}"
        ".badge-ok{background:#e3f4e6;color:#1a7f37;border:1px solid #a8d5b2}"
        ".badge-fail{background:#fbe6e6;color:#c62828;border:1px solid #e8a9a9}"
        ".badge-skip{background:#e4ecf5;color:#4a6b8a;border:1px solid #b3c8dd}"
        ".ok .state{color:#1a7f37;font-weight:600}.fail .state{color:#c62828;font-weight:600}"
        ".skip .state{color:#4a6b8a}"
        ".hint{color:#b26a00;font-size:12px;margin-top:4px}"
        ".legend{margin:8px 0;font-size:13px;color:#555}"
        ".legend .badge{margin-right:6px}"
    )
    return (
        "<!doctype html><html><head><meta charset='utf-8'>"
        "<title>IPTV 源探测报告</title><style>" + css + "</style></head><body>"
        f"<h1>IPTV 源探测报告 <small style='font-weight:400;color:#888'>"
        f"{html.escape(str(report.get('version', '')))} · "
        f"{'云端视角' if loc == LOC_CLOUD else '家里视角' if loc == LOC_LOCAL else ''}"
        f"</small></h1>"
        f"<p>时间：{html.escape(str(report['timestamp']))}　"
        f"总量 {s['total']}（有效 {s.get('counted', s['total'])}"
        + (f" / 跳过 {s['skipped']}" if s.get("skipped") else "")
        + f"）　列表可用 {s['ok_list']}/{s.get('total_list', 0)}　"
        f"流可用 {s['ok_stream']}/{s.get('total_stream', 0)}</p>"
        + scope_line + chg_line +
        "<div class='legend'>图例："
        "<span class='badge badge-ok'>可用</span> 探测成功　"
        "<span class='badge badge-fail'>失败</span> 探测失败　"
        "<span class='badge badge-skip'>云端免测</span> "
        "云端主动跳过（内网源，非故障）</div>"
        "<table><tr><th>ID</th><th>名称</th><th>域</th><th>类型</th><th>HTTP</th>"
        "<th>延迟</th><th>明细</th><th>状态</th><th>较上次</th><th>错误</th></tr>"
        + "".join(rows) + "</table></body></html>"
    )


# ================================================================ CLI
def parse_targets(path: Optional[str], scope_filter: Optional[str] = None,
                  include_baseline: bool = False) -> List[Target]:
    """
    解析目标，scope 缺省为 public（新增源默认 public，内网源须显式标 intranet）。
    scope_filter 非空时只保留该 scope 的目标（baseline 例外，见下）。
    include_baseline=True 且使用内置源时，自动追加 intranet 组与 baseline 组（N1/N2）。

    【为什么 baseline 不受 scope_filter 约束】
      baseline 是判定「网络是否健康」的上下文，不是被探测的「源」。若被
      scope_filter 过滤掉，网络故障时就无从判定该抑制告警了。intranet 组则
      照常受约束（云端默认不带，避免无意义的 skip 行）。

    Args:
        path: 目标 JSON 路径；None 表示用内置 DEFAULT_TARGETS。
        scope_filter: 只保留该 scope；None 表示不过滤。
        include_baseline: 是否追加 intranet 组 + ecosystem 组 + baseline 组。

    Returns:
        去重后的 Target 列表。
    """
    if not path:
        raw = list(DEFAULT_TARGETS)
        if include_baseline:
            raw += [d for d in INTRANET_TARGETS if d.get("scope") == SCOPE_INTRANET]
            raw += [d for d in ECOSYSTEM_TARGETS
                    if d.get("scope") == SCOPE_ECOSYSTEM]
            raw += BASELINE_TARGETS
    else:
        raw = json.load(open(path, encoding="utf-8"))
    targets = []
    seen_ids: set = set()
    for i, item in enumerate(raw):
        kind = str(item.get("type", "list")).lower()
        if kind not in VALID_KINDS:
            kind = "list"
        scope = str(item.get("scope") or SCOPE_PUBLIC).lower()
        if scope not in VALID_SCOPES:
            scope = SCOPE_PUBLIC
        # baseline 永远保留（它是对照上下文，不是"源"）
        if (scope_filter is not None and scope != scope_filter
                and scope != SCOPE_BASELINE):
            continue
        tid = str(item.get("id") or f"T{i + 1}")
        if tid in seen_ids:      # 防 B1 在 DEFAULT/INTRANET 两处重复登记
            continue
        seen_ids.add(tid)
        targets.append(Target(
            id=tid,
            name=str(item.get("name") or ""),
            url=str(item["url"]),
            kind=kind,
            scope=scope,
            headers=dict(item.get("headers") or {}),
        ))
    return targets


def append_baseline(targets: List[Target]) -> List[Target]:
    """给目标列表追加 baseline 组（若尚未包含）。"""
    have = {t.id for t in targets}
    out = list(targets)
    for d in BASELINE_TARGETS:
        if d["id"] not in have:
            out.append(Target(id=d["id"], name=d["name"], url=d["url"],
                              kind=d["type"], scope=d["scope"]))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(
        description="IPTV 源探测工具 v4.1（位置感知 + 双位置鉴别 + HLS/直连流真实验证）")
    ap.add_argument("--timeout", type=float, default=5.0, help="单请求超时秒数（默认 5）")
    ap.add_argument("--concurrency", type=int, default=10, help="总并发请求数（默认 10）")
    ap.add_argument("--per-host", type=int, default=4, help="单主机并发上限（默认 4）")
    ap.add_argument("--total-timeout", type=float, default=60.0, help="整体超时秒数（默认 60）")
    ap.add_argument("--max-bytes", type=int, default=512 * 1024, help="测速/列表最大读取字节数")
    ap.add_argument("--max-sec", type=float, default=3.0, help="分片测速最大读取秒数（默认 3）")
    ap.add_argument("--retries", type=int, default=1, help="网络类错误重试次数（默认 1）")
    ap.add_argument("--proxy", type=str, default=None, help="HTTP(S) 代理")
    ap.add_argument("--insecure", action="store_true", help="跳过 TLS 证书校验")
    ap.add_argument("--targets", type=str, default=None, help="外部源文件(JSON)")
    ap.add_argument("--location", choices=list(VALID_LOCATIONS), default=LOC_CLOUD,
                    help="探针所处位置：cloud=云端（跳过 intranet 源）；local=本地内网（默认 cloud）")
    ap.add_argument("--scope", choices=list(VALID_SCOPES), default=None,
                    help="只探测该 scope 的目标（cloud 下测 intranet 无意义，会被跳过）")
    ap.add_argument("--variant-strategy", choices=["max", "min", "first"], default="max",
                    help="master playlist 选流策略（默认 max=BANDWIDTH 最高）")
    ap.add_argument("--segment-strategy", choices=["last", "middle", "first"], default="last",
                    help="HLS 分片选取策略（默认 last=直播窗口末端）")
    ap.add_argument("-o", "--output", type=str, default=None, help="JSON 报告输出路径")
    ap.add_argument("--html", action="store_true", help="同时生成 HTML 报告")
    ap.add_argument("--no-compare", action="store_true", help="关闭历史对比")
    ap.add_argument("--compare-file", type=str, default=None, help="显式指定历史报告路径")
    ap.add_argument("--sort", choices=["id", "latency", "speed"], default="id")
    ap.add_argument("--only-ok", action="store_true", help="只输出可用项（跳过项会被保留）")
    ap.add_argument("--fail-on-stream-down", action="store_true",
                    help="public 流源全部不可用即返回退出码 1（不含 intranet）")
    ap.add_argument("--min-ok-list", type=int, default=None,
                    help="要求 public 列表源至少 N 个可用，否则退出码 1")
    ap.add_argument("--min-ok-stream", type=int, default=None,
                    help="要求 public 流源至少 N 个可用，否则退出码 1")
    ap.add_argument("--merge", nargs="+", metavar="JSON",
                    help="合并模式：传入 cloud.json local.json（顺序不限，按 probe_location 自动识别），"
                         "输出双位置鉴别报告，不执行探测")
    ap.add_argument("--verbose", action="store_true", help="打印进度日志到 stderr")
    args = ap.parse_args()

    # ---- 合并模式：只做双位置鉴别，不探测 ----
    if args.merge:
        return _run_merge_mode(args)

    try:
        # include_baseline=True 让云端也带上 intranet 组与基线组：
        #   - intranet 组会被 _should_skip 跳过，看板显示「云端免测」（R12a 下放）；
        #   - baseline 组用于 emit 网络健康上下文。
        targets = parse_targets(args.targets, scope_filter=args.scope,
                                include_baseline=True)
    except (OSError, json.JSONDecodeError) as e:
        print(f"错误：源文件读取失败：{e}", file=sys.stderr)
        return 2
    if not targets:
        print("错误：没有可用的探测目标", file=sys.stderr)
        return 2

    if args.verbose:
        n_pub = sum(1 for t in targets if t.scope == SCOPE_PUBLIC)
        n_intra = sum(1 for t in targets if t.scope == SCOPE_INTRANET)
        print(f"[*] location={args.location}，共 {len(targets)} 个目标"
              f"（public {n_pub} / intranet {n_intra}），总并发 {args.concurrency}，"
              f"单主机并发 {args.per_host}，单请求超时 {args.timeout}s，"
              f"整体超时 {args.total_timeout}s"
              + (f"，代理 {args.proxy}" if args.proxy else ""), file=sys.stderr)

    probe = IPTVProbe(
        targets=targets,
        timeout=args.timeout,
        concurrency=args.concurrency,
        total_timeout=args.total_timeout,
        max_bytes=args.max_bytes,
        max_seconds=args.max_sec,
        retries=args.retries,
        proxy=args.proxy,
        insecure=args.insecure,
        verbose=args.verbose,
        variant_strategy=args.variant_strategy,
        segment_strategy=args.segment_strategy,
        per_host=args.per_host,
        location=args.location,
        scope_filter=args.scope,
    )

    try:
        results = asyncio.run(probe.run())
    except KeyboardInterrupt:
        print("已取消", file=sys.stderr)
        return 130

    # 云端测得的公网带宽另存 cloud_bandwidth_kbps：仅作公网参考，
    # 内网选源排序必须以 local 探针的 speed_kbps 为准。
    if args.location == LOC_CLOUD:
        for r in results:
            if r.speed_kbps is not None:
                r.cloud_bandwidth_kbps = r.speed_kbps

    # F17: 先对比全量结果，再裁剪输出
    prev_path = args.compare_file
    if prev_path is None and args.output and not args.no_compare:
        report_dir = os.path.dirname(os.path.abspath(args.output))
        today_key = datetime.now().strftime("%Y%m%d")
        prev_path = find_latest_report(report_dir, today_key)
    if prev_path and not args.no_compare:
        apply_changes(results, prev_path, location=args.location)
        if args.verbose:
            print(f"[*] 已与历史报告对比：{prev_path}", file=sys.stderr)

    meta = {
        "location": args.location,
        "scope_filter": args.scope,
        "proxy": args.proxy,
        "insecure": args.insecure,
        "variant_strategy": args.variant_strategy,
        "segment_strategy": args.segment_strategy,
        "per_host": args.per_host,
        "timeout": args.timeout,
        "max_bytes": args.max_bytes,
        "max_sec": args.max_sec,
    }

    results = sort_results(results, args.sort)
    if args.only_ok:
        # 跳过项保留：它们不是失败，只是不在本位置测
        results = [r for r in results if r.ok or r.skipped]

    report = build_report(results, meta)
    payload = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        os.makedirs(os.path.dirname(os.path.abspath(args.output)) or ".", exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(payload)
        print(f"JSON 报告已写入：{args.output}")
        if args.html:
            html_path = os.path.splitext(args.output)[0] + ".html"
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(render_html(report))
            print(f"HTML 报告已写入：{html_path}")
    else:
        print(payload)

    # F16: 精细化退出码 —— 只统计 scope=public（intranet 在云端不可测，不该影响判定）
    s = report["summary"]
    pub = s.get("scopes", {}).get(SCOPE_PUBLIC, {})
    pub_ok_stream = pub.get("ok_stream", 0)
    pub_total_stream = pub.get("total_stream", 0)
    pub_ok_list = pub.get("ok_list", 0)
    reasons = []
    if args.min_ok_list is not None and pub_ok_list < args.min_ok_list:
        reasons.append(f"public 列表源可用 {pub_ok_list} < 要求 {args.min_ok_list}")
    if args.min_ok_stream is not None and pub_ok_stream < args.min_ok_stream:
        reasons.append(f"public 流源可用 {pub_ok_stream} < 要求 {args.min_ok_stream}")
    if args.fail_on_stream_down and pub_total_stream > 0 and pub_ok_stream == 0:
        reasons.append("public 流源全部不可用")
    if reasons:
        print("可用性要求未满足：" + "；".join(reasons), file=sys.stderr)
        return 1

    # 只要有一个真实探测过的目标可用即算成功（跳过项不算可用也不算失败）
    ok_count = sum(1 for r in results if r.ok)
    return 0 if ok_count > 0 else 1


def _run_merge_mode(args) -> int:
    """--merge cloud.json local.json：按 probe_location 识别并鉴别。"""
    cloud_path = local_path = None
    for p in args.merge:
        if not os.path.isfile(p):
            print(f"错误：合并输入不存在：{p}", file=sys.stderr)
            return 2
        if "local" in os.path.basename(p).lower():
            local_path = p
        elif "cloud" in os.path.basename(p).lower():
            cloud_path = p
        else:
            # 按文件内容里的 probe_location 判断
            try:
                with open(p, encoding="utf-8") as f:
                    loc = json.load(f).get("probe_location")
            except Exception:
                loc = None
            if loc == LOC_LOCAL:
                local_path = p
            elif loc == LOC_CLOUD:
                cloud_path = p
            elif cloud_path is None:
                cloud_path = p
            else:
                local_path = p

    merged = merge_reports(cloud_path, local_path)
    out = json.dumps(merged, ensure_ascii=False, indent=2)
    if args.output:
        os.makedirs(os.path.dirname(os.path.abspath(args.output)) or ".", exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(out)
        print(f"合并报告已写入：{args.output}")
        if args.html:
            html_path = os.path.splitext(args.output)[0] + ".html"
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(render_merge_html(merged))
            print(f"合并 HTML 已写入：{html_path}")
    else:
        print(out)

    s = merged["summary"]
    if s["level1"]:
        print(f"🔴 一级战备（源真死）：{', '.join(s['level1'])}", file=sys.stderr)
    if s["level2"]:
        print(f"🟡 二级战备（本地通道被墙）：{', '.join(s['level2'])}", file=sys.stderr)
    return 1 if s["alarm_count"] else 0


if __name__ == "__main__":
    sys.exit(main())
