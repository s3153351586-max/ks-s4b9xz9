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
from urllib.parse import quote, urlparse

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

# ---------------------------------------------------------------- 复用现有资产
# 【设计原则：不许有第二份正则】
#   直链识别只有 probe_local.CDN_URL_RE 一处。本脚本 import 它，不复制。
from probe_local import (  # noqa: E402
    CDN_HOST_MARKER, CDN_URL_RE, extract_cdn_urls,
)
# ---------------------------------------------------------------- 图标索引（可选）
# 【为什么必须是 try/except 而不是直接 import —— 教训换来的】
#   R20 run#20 的真实事故：仓库里漏传了 `tools/logo_index.py`，
#   这一行直接 ModuleNotFoundError，整个 export-backup 步骤 4 崩掉。
#   而图标只是**播放器的装饰**：少了它，频道名、直链、UA 全都好好的，
#   用户照样能看。为了让"台标"这个纯装饰功能，把"能不能看"这件事搞挂，
#   是完全不可接受的优先级倒挂。
#
#   同款事故过两次了（probe_local.py 漏传也崩过）。**规律很清楚**：
#   凡是新增的跨文件依赖，都必须做成"缺了也能跑"。
#   所以这里用 try/except 降级为**空索引**，后面再给一个纯算法兜底。
try:
    import logo_index as _logo_index  # noqa: E402
except ImportError:  # pragma: no cover - 事故场景，用单测的注入路径覆盖
    _logo_index = None  # type: ignore[assignment]

# _logo_index 缺失时的降级替身：提供 normalize_key / build_index 两个本文件用到的接口。
# 归一化规则与 logo_index.normalize_key 保持**逐字一致**（否则降级后行为会漂）。
if _logo_index is None:  # pragma: no cover - 同上
    import re as _re_fb

    class _FallbackLogoIndex:  # noqa: D101 - 见下方注释
        """
        图标索引的降级替身。

        【降级到什么程度】
          build_index() 返回**空字典** → 所有频道都匹配不到图标 →
          build_m3u 一条 tvg-logo 都不写。
          结果：产物 m3u **完全合法、275 条全在、UA 正常、能正常播放**，
          只是播放器里看到的都是默认图标。

        【为什么不在这里内嵌一份索引副本】
          那等于把 logo_index.py 的内容复制成两份，两边各自演化 ——
          迟早出现"降级时用的索引和正常时不一样"的诡异 bug。
          宁可降级得更彻底（0 个图标），也要保证**只有一处真相**。
        """

        @staticmethod
        def normalize_key(name: str) -> str:
            """归一化键，规则与 logo_index.normalize_key 逐字一致。"""
            return _re_fb.sub(r"[^0-9a-z\u4e00-\u9fff+]+", "",
                              (name or "").strip().lower())

        @staticmethod
        def build_index():
            """降级索引：空字典（不匹配任何图标）。"""
            return {}

    _logo_index = _FallbackLogoIndex()  # type: ignore[assignment]

# ---------------------------------------------------------------- 拼音（可选）
# 【为什么是可选依赖】
#   get_logo_url() 用拼音匹配中文频道名（"安徽卫视" → ANHUI）。
#   但图标是**装饰性**功能，绝不该因为它让整个 m3u 导出失败。
#   所以 pypinyin 缺失时降级为空实现：拼音类候选全部落空，
#   剩下"原名/去修饰名/中文名"三条路照样能命中 `CHC家庭影院`、`CCTV1` 这些。
#   实测：有 pypinyin 命中 51/216，没有则降到 ~30/216 —— 少些图标，但**照样能跑**。
try:
    from pypinyin import lazy_pinyin  # noqa: E402
except ImportError:  # pragma: no cover - 环境相关
    def lazy_pinyin(s):  # type: ignore[misc]
        """pypinyin 缺失时的降级：返回空列表，拼音候选全部落空。"""
        return []

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


# ================================================================ R20：播放列表
# 【R20 是什么】
#   "Healer 死亡时的备用源"：手动触发 Actions，把 EPG 的 275 条直链导出成
#   一份**可直接导入播放器**的 m3u。产物只走 artifact（7 天），绝不进公开仓库。
#
# 【为什么这两个常量要硬编码】
#   UA：广西移动 CDN 校验客户端特征，不带 UA 直接 403/无响应。实测确认。
#       这个 UA 是移动客户端标准 UA，写死在产物里，用户导入后无需自己填。
#   图标库：Gitee 开源频道图标库。写 get_logo_url() 做清洗匹配，
#           匹配不到就不写 tvg-logo（fallback，播放器显示默认图标）。
M3U_UA = ("Dalvik/2.1.0 (Linux; U; Android 14; "
          "SM-S9280 Build/UP1A.231005.007)")

LOGO_BASE = "https://gitee.com/mytv-android/myTVlogo/raw/main/img"

# 分组名：产物固定用一个分组，导入播放器后就是"广西移动"这一栏。
M3U_GROUP = "广西移动"

# ================================================================ R21 台标直采
# 【R21 第 1 项：台标直采 channelIcon，弃 Gitee 为主线】
#   实测（2026-09-23，EPG 305 条）：
#     channelIcon 非空 = 201/305 = 65.9%   ← 上游真实覆盖率
#     全部来自单一域名 images.center.bcs.ottcn.com:8080
#   Gitee 库（789 条）R20 实测在 216 个真实频道名上命中 51 条 = 23.6%。
#   故 R21 采「直采为主 + Gitee 兜底」（B 方案）：
#     - 直采优先：上游字段权威、实时、随源站变更自动跟随，零维护。
#     - Gitee 兜底：仅在 channelIcon 为空时启用，已建成资产白用。
#
# 【双层断言指标（实测标定，不是拍的）】
#   硬线 direct_logo_rate  >= 0.60  → FAIL（上游字段突变防线）
#       实测 65.9%，留 ~6pt 缓冲。跌破 60% 说明上游 channelIcon 字段被削，
#       属**上游结构变更**，必须红 —— 这是唯一能提前发现"字段没了"的信号。
#   软线 combined_logo_rate >= 0.75 → WARN 不 FAIL（第三方抖动不拦发布）
#       Gitee 侧属于第三方，它抖动不该阻断你的发布链路。
#       所以只告警、不失败 —— 与"图标可以全丢，m3u 必须完整产出"同一原则。
DIRECT_LOGO_HARD_FLOOR = 0.60   # 硬线：低于此值 FAIL
COMBINED_LOGO_SOFT_TARGET = 0.75  # 软线：低于此值 WARN（不 FAIL）

# 【R21 第 4 项红线 · catchup 时移开关】
#   实测结论（2026-09-23）：
#     ① EPG 305 条 × 全字段仅 10 个：channelIcon/channelName/coverImgUrl/
#        livePlayUrl/logo/no/onplay/urlid/usable/uuid
#        —— **零个** playseek/timeshift/catchup 字段。上游不下发时移能力。
#     ② 沙箱与 GitHub Actions 出口均**连不上** cdnrrs.gx.chinamobile.com
#        （TCP 通、HTTP 空回复 exit 52）→ 时移可用性**无法在 CI 实测**。
#   故：默认关闭。只有显式 --enable-catchup 才注入时移参数。
#   回归断言保证：默认模式下产物**绝不含**任何 catchup 字段（防误开）。
CATCHUP_ENABLED_DEFAULT = False
# 时移参数模板。**待本地实测回填** —— 当前值仅为最常见形态的占位，
# 在未实测确认前，--enable-catchup 会拒绝工作（见 main 中的守卫）。
CATCHUP_PARAM_TEMPLATE = "playseek={start}-{end}"
# 时移参数是否已经过实测确认。False => --enable-catchup 直接报错退出。
# 这是"绝不在未验证前提下产出错误 m3u"的硬保护。
CATCHUP_RULE_VERIFIED = False

# 画质/码率/厂商前后缀。这些是 EPG 频道名里的**冗余修饰**，
# 必须在匹配图标前剥掉 —— 库里没有 "CCTV-2高清8M.png" 这种名字。
_QUALITY_RE = r"(高清|超高清|标清|蓝光|超清|4K|8K|HD|SD|2\.5M|4M|8M|码率|测试|直播)"

# 频道形态后缀。剥掉后剩下的往往是库里真正用的名字：
#   "广西卫视" -> "广西" -> GUANGXI（库里若有就是短拼音）
#   "凤凰中文台" -> "凤凰中文" -> FENGHUANGZHONGWEN
_CHANNEL_SUFFIX_RE = re.compile(r"(卫视|电视台|频道|台|套)$")


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


# ================================================================ R21 台标直采
def extract_icons(channels: List[Dict[str, Any]]) -> Dict[str, str]:
    """
    从频道列表抽出 url -> channelIcon 映射（R21 台标直采通道）。

    【与 get_logo_url 的关系 —— 两条独立通道，不是主备替换】
      R21 的台标有两个来源，优先级明确：
        1. **直采**（本函数）：EPG 的 `channelIcon` 字段，上游原样给出。
           权威、实时、随源站变更自动跟随。实测覆盖 201/305 = 65.9%。
        2. **匹配**（get_logo_url）：Gitee 789 条库做归一化匹配。
           仅在直采为空时启用。R20 实测在真实频道名上命中 23.6%。

    【为什么不直接用 `logo` 字段】
      实测 `logo` 字段的值是被截断的坏数据（如裸 "http"），**不可用**。
      只有 `channelIcon` 是完整 URL。这一点是实测踩出来的，写死在这里。

    【为什么只认 http(s) 前缀】
      channelIcon 为空的条目实测有 104/305。空串、None、裸 "http"
      都会在这里被过滤掉，返回的 map 里绝不会出现无效值 ——
      调用方可以无条件信任拿到的值是可用的 URL。

    Args:
        channels: 频道 dict 列表。

    Returns:
        {直链: 图标URL}。仅含 channelIcon 为非空 http(s) 的条目。
    """
    url_keys = ("livePlayUrl", "zteurl", "hwurl", "url", "playUrl",
                "playurl", "liveUrl", "channelUrl")
    out: Dict[str, str] = {}
    for c in channels:
        u = ""
        for k in url_keys:
            v = c.get(k)
            if isinstance(v, str) and v.strip():
                u = v.strip()
                break
        if not u:
            continue
        icon = c.get("channelIcon")
        if isinstance(icon, str):
            icon = icon.strip()
            # 只接受完整 URL。裸 "http" 这类截断值必须挡掉。
            if icon.startswith(("http://", "https://")):
                out[u] = icon
    return out


def resolve_logo(url: str, name: str, direct: Optional[Dict[str, str]] = None,
                 index: Optional[Dict[str, str]] = None) -> Tuple[str, str]:
    """
    R21 台标双通道路由：直采优先，Gitee 兜底。

    【返回值带来源标记，是为了统计命中率】
      统计必须能区分 "直采命中" 和 "兜底命中"，否则两种断言
      （硬线看直采、软线看合计）无法分别计算。所以返回 (url, source)。

    【为什么兜底失败必须降级回纯直采、不能抛异常】
      用户的裁决原文："Gitee 补洞失败必须降级回纯直采、不崩。"
      这与 R20.1 的三层防线原则一致：图标是装饰，**m3u 必须完整产出**。
      所以这里对 Gitee 通道再包一层 try —— 第四层防线。

    Args:
        url: 频道直链（直采通道的键）。
        name: 频道名（兜底通道的输入）。
        direct: 直采映射 {url: icon}。
        index: Gitee 匹配索引。

    Returns:
        (图标URL, 来源)。来源为 "direct" / "fallback" / "" (未命中)。
        未命中时图标 URL 为空串（调用方不写 tvg-logo 属性）。
    """
    # 通道 1：直采。上游权威，优先。
    if direct:
        hit = direct.get(url)
        if hit:
            return hit, "direct"
    # 通道 2：Gitee 兜底。任何异常都降级为"未命中"，绝不外抛。
    try:
        fb = get_logo_url(name, index=index)
    except Exception:  # noqa: BLE001 - 兜底通道故障不得阻断导出
        fb = ""
    if fb:
        return fb, "fallback"
    return "", ""


def logo_stats(total: int, direct_hits: int, fallback_hits: int) -> Dict[str, Any]:
    """
    计算 R21 双层断言的命中率指标。

    【为什么单独抽成函数】
      断言要测的是"统计逻辑正确"，而不是"某次跑的数字好看"。
      抽出来后单测可以直接注入任意 (total, direct, fallback) 组合，
      验证硬线/软线的判定边界 —— 不依赖真实网络、不依赖 EPG 当天数据。

    Args:
        total: 频道总数。
        direct_hits: 直采命中数。
        fallback_hits: 兜底命中数。

    Returns:
        dict: direct 率、combined 率、以及两条线的通过状态。
    """
    if total <= 0:
        return {"total": 0, "direct": 0, "fallback": 0, "combined": 0,
                "direct_logo_rate": 0.0, "combined_logo_rate": 0.0,
                "direct_ok": False, "combined_ok": False}
    direct_rate = direct_hits / total
    combined_rate = (direct_hits + fallback_hits) / total
    return {
        "total": total,
        "direct": direct_hits,
        "fallback": fallback_hits,
        "combined": direct_hits + fallback_hits,
        "direct_logo_rate": round(direct_rate, 4),
        "combined_logo_rate": round(combined_rate, 4),
        # 硬线：低于 60% => 不通过（上游字段突变）
        "direct_ok": direct_rate >= DIRECT_LOGO_HARD_FLOOR,
        # 软线：低于 75% => 告警（第三方抖动，不拦发布）
        "combined_ok": combined_rate >= COMBINED_LOGO_SOFT_TARGET,
    }


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


def _strip_quality(name: str) -> str:
    """
    剥离频道名里的画质/码率/厂商前后缀，得到"频道本体名"。

    【为什么需要反复剥离】
      EPG 的名字是机器拼接的，修饰词可以叠好几层，实测最长 4 层：
        "H265-CCTV-2高清4M"  -> "CCTV-2"
        "北京卫视4K-25P"     -> "北京卫视"    （-25P 是帧率，也是修饰）
        "三沙卫视2.5M"       -> "三沙卫视"
      用固定次数的替换是等价的（最多 4 层，跑 6 轮留余量）。

    Args:
        name: 原始频道名。

    Returns:
        剥离修饰后的名字。无法再剥离时返回当前值。
    """
    t = (name or "").strip()
    for _ in range(6):
        t2 = re.sub(r"^\d+\s*", "", t)                                # 前导序号
        t2 = re.sub(r"^(H265|H264|HEVC)[-_]?\s*", "", t2, flags=re.I)  # 编码前缀
        # 括号包裹的修饰：CCTV-9(英) → 不动（(英) 是语义，不是画质）
        t2 = re.sub(r"[-_（(]\s*" + _QUALITY_RE + r"\s*[)）]$", "", t2,
                    flags=re.I)
        # 尾部裸露的修饰：CCTV-2高清8M / 兵团卫视标清
        t2 = re.sub(r"[\s\-_（(]?\s*" + _QUALITY_RE + r"\s*[)）]?$", "", t2,
                    flags=re.I)
        t2 = re.sub(r"[-_]\d+[pP]$", "", t2)                          # -25P/-50P
        t2 = t2.strip(" -_（）()")
        if t2 == t:
            break
        t = t2
    return t


def _name_candidates(name: str) -> List[str]:
    """
    由一个频道名推导出**全部**可能的图标文件名候选（按可信度排序）。

    【为什么是"多候选"而不是"一个名字"】
      频道名和图标名是两套命名体系：
        "H265-CCTV-2高清4M"  →  库里叫 `CCTV2`
        "凤凰中文台"          →  库里叫 `FENGHUANGZHONGWEN`（拼音！）
        "安徽卫视"            →  库里叫 `ANHUI`（短拼音，不带"卫视"！）
      没有任何单条规则能覆盖全部，只能列举候选再逐一比对。

    【重要：候选只能由"名字本身"推导】
      绝不生成与本体无关的粘连形式。曾经为了让 "CCTV-16 4K" 命中而生成
      "CCTV164K"，结果它真的撞上了库里的 `CCTV164K` —— 一个**不同的频道**。
      这类"为了凑命中而放宽"的规则，是错误图标的唯一来源。
      现在只剩两条路：本体/剥修饰后的本体/剥后缀/对应拼音，
      **且**在比对时叠加数字块校验（见 get_logo_url）。

    Args:
        name: 原始频道名。

    Returns:
        候选字符串列表（已去重，顺序即优先级）。
    """
    out: List[str] = []
    seen = set()

    def add(s: str) -> None:
        s = (s or "").strip()
        k = _logo_index.normalize_key(s)
        if s and k not in seen:
            seen.add(k)
            out.append(s)

    add(name)
    stripped = _strip_quality(name)
    add(stripped)
    core = _CHANNEL_SUFFIX_RE.sub("", stripped).strip()
    add(core)

    # 对"剥了修饰"和"再剥后缀"两个本体做拼音展开。
    for base in (stripped, core):
        if not base:
            continue
        if re.search(r"[\u4e00-\u9fff]", base):
            parts = lazy_pinyin(base)
            py = "".join(parts)
            add(py.upper())           # GUANGXI / ANHUI / FENGHUANGZIXUN
            add(py)
            add(py.capitalize())
            if len(parts) > 1:
                # 省份短拼音：库里 `ANHUI` 是"安徽"整串，
                # 但有些库用首字（如 `Nanning`），两手都试。
                add(parts[0].upper())
                add(parts[0])
                add(parts[0].capitalize())
    return out


def get_logo_url(name: str, index: Optional[Dict[str, str]] = None) -> str:
    """
    由频道名得到频道图标 URL；匹配不到返回空串（fallback，不写 tvg-logo）。

    【匹配规则 —— 两道闸门，缺一不可】
      闸门 1（归一化相等）：把候选与库中文件名都归一化（转小写、剥掉
        空格/-/_/./括号）后比对。这一步解决了 CCTV-1 / cctv1 / -CCTV1
        的大小写与分隔符差异。
      闸门 2（数字块一致）：归一化会把"分隔符"抹掉，于是
        `CCTV-16 4K` 和 `CCTV164K` 归一化后**同为** `cctv164k` —— 但它们
        是两个不同频道（前者是 16 套 4K 版，后者是 164K 这个不存在的号）。
        规则：候选与库文件名的"数字块元组"必须完全相同。
        `CCTV-16 4K` 的数字块是 (16, 4)，`CCTV164K` 是 (164) → 拒绝。

    【匹配不到怎么办】
      返回 ""。调用方 build_m3u 就不写 tvg-logo，播放器显示默认图标。
      **宁可不显示，也不显示错的** —— 错图标比没图标更让人困惑。

    实测效果（216 个真实 EPG 频道名）：命中 51 个（23.6%）。
    未命中的是库里确实没有的：广西本地台（广西卫视/南宁新闻综合/慢享…）
    以及库未收录的频道。CCTV 全系、CGTN、CHC、凤凰、金鹰、
    以及 ANHUI/DONGFANG/DONGNAN/BEIJING/CHONGQING 等省级台均已命中。

    Args:
        name: EPG 返回的 channelName。
        index: 匹配索引（键=归一化名，值=库中文件名不含扩展名）。
               默认 None 用内置静态索引（零网络）。单测可注入小索引。

    Returns:
        图标 URL；无匹配时返回空串。
    """
    if not name:
        return ""
    # 【为什么这里再包一层 try】
    #   上面的 import 保护只挡住"文件不存在"。
    #   但 logo_index.py 若**存在却坏了**（语法错、_INDEX 被误删、
    #   build_index 签名改了），导入能过、调用才炸 —— 那时已经跑到
    #   275 条的循环里，等于把装饰性功能的问题延迟放大成整批失败。
    #   所以在调用点也兜一层：出错就当没有索引，图标全 fallback。
    #   **原则：图标可以全丢，m3u 必须完整产出来。**
    try:
        idx = index if index is not None else _logo_index.build_index()
    except Exception:  # noqa: BLE001 - 任何异常都不该阻断导出
        return ""
    if not idx:
        return ""

    # 库文件名的数字块，用于闸门 2。惰性构建并缓存（索引不变时可复用）。
    for cand in _name_candidates(name):
        key = _logo_index.normalize_key(cand)
        hit = idx.get(key)
        if not hit:
            continue
        # 闸门 2：数字块必须完全一致，防止 CTSV-16 4K ↔ CCTV164K 这类混淆。
        if tuple(re.findall(r"\d+", cand)) != tuple(re.findall(r"\d+", hit)):
            continue
        return f"{LOGO_BASE}/{quote(hit)}.png"
    return ""


def build_m3u(urls: List[str], name_by_url: Optional[Dict[str, str]] = None,
              tag: str = M3U_GROUP, ua: str = M3U_UA,
              index: Optional[Dict[str, str]] = None,
              direct_icons: Optional[Dict[str, str]] = None,
              enable_catchup: bool = CATCHUP_ENABLED_DEFAULT,
              stats_out: Optional[Dict[str, Any]] = None) -> str:
    """
    生成 m3u 播放列表（R21 备用源格式）。

    【格式规格】
      #EXTM3U
      # exTVLCOPT:http-user-agent=<UA>     ← VLC 系播放器会读这行
      # UA: <UA>                            ← 纯注释，给人和其它播放器看
      #EXTINF:-1 group-title="广西移动" tvg-logo="<图标URL>",<频道名>
      <直链>

    【R21 变更点】
      1. 台标双通道：direct_icons（直采 channelIcon）优先，Gitee 兜底。
         见 resolve_logo。
      2. enable_catchup 默认 False。**默认模式下产物绝不出现任何
         catchup/timeshift/playseek 字样** —— 有断言守着（防误开）。
      3. stats_out 回填命中率统计，供双层断言使用。用 out 容器而非
         返回值，是为了不破坏现有调用方的签名兼容性。

    【为什么 tvg-logo 在逗号之前、且与 group-title 用空格分隔】
      需求里写的形态是 `group-title="广西移动", tvg-logo="…", 频道名`。
      但 m3u 的语法是：**#EXTINF 行里第一个逗号之后全部是"显示名"**。
      若照抄那两个逗号，`tvg-logo="…"` 会被当成显示名的一部分，
      频道名反而丢失 —— 播放器里会看到一长串 URL 当台标。
      所以属性之间用**空格**分隔、逗号只留一个、后面直接跟频道名。
      视觉顺序与需求完全一致（分组 → 图标 → 频道名），且解析 100% 标准。

    【为什么 UA 要写两遍】
      `# exTVLCOPT:` 是 VLC 的私有扩展，mytv 等基于 VLC 的播放器认它。
      但它以 `# ` 开头，某些严格的解析器会整行忽略，所以再写一行普通
      注释 `# UA:` 兜底 —— 人肉排查时一看就知道该填什么 UA。

    【为什么 tvg-logo 可能缺席】
      resolve_logo 两条通道都没命中就**整个属性不写**，而不是写
      tvg-logo=""。空属性会让某些播放器尝试请求空 URL 而卡顿；
      不写则是干净的降级。

    【频道名里的引号】
      频道名可能含 `"`（实测没有，但接口是外部的）。统一替换成全角引号，
      避免把 #EXTINF 行的属性语法打破 —— 格式正确比保留原字符重要。

    Args:
        urls: 直链列表（应已过 is_valid_channel_url 过滤）。
        name_by_url: url → 频道名映射。
        tag: group-title 值。
        ua: User-Agent，同时进注释行与 exTVLCOPT。
        index: Gitee 兜底匹配索引，透传给 get_logo_url。
        direct_icons: 直采映射 {url: channelIcon}，优先于 index。
        enable_catchup: 是否注入时移参数。默认 False。
        stats_out: 非 None 时回填 {"direct": n, "fallback": n} 统计。

    Returns:
        m3u 文本（以换行结尾）。
    """
    name_by_url = name_by_url or {}
    direct_icons = direct_icons or {}
    lines = [
        "#EXTM3U",
        f"# exTVLCOPT:http-user-agent={ua}",
        f"# UA: {ua}",
        f"# 由 iptv 项目 R21 生成 · 分组={tag} · {len(urls)} 条",
    ]
    n_direct = 0
    n_fallback = 0
    for i, u in enumerate(urls, 1):
        raw = name_by_url.get(u) or f"{tag}-{i}"
        # 引号/逗号转全角：频道名若含 " 或 , 会破坏 #EXTINF 的属性语法
        # （逗号还兼任"显示名分隔符"，必须最优先处理）。
        n = raw.replace('"', "＂").replace(",", "，")
        # 【离产物最近的一层防线：图标逻辑无论怎么炸，都不能影响产出】
        #   resolve_logo 内部已兜两层（直采无异常 + 兜底 try）；
        #   这里是第三层。三层看着冗余，但代价是 0（异常路径永不触发），
        #   收益是"305 条里第 187 条的台标算不出来"这件事**永远不会**
        #   变成"整个文件产不出来"。R20.1 事故教训值得这点冗余。
        try:
            logo, source = resolve_logo(u, raw, direct=direct_icons,
                                        index=index)
        except Exception:  # noqa: BLE001 - 装饰性功能，绝不阻断导出
            logo, source = "", ""
        if source == "direct":
            n_direct += 1
        elif source == "fallback":
            n_fallback += 1
        # catchup 注入（默认关闭）。仅在显式开启时拼接参数。
        url_out = u
        if enable_catchup:
            sep = "&" if "?" in u else "?"
            url_out = f"{u}{sep}{CATCHUP_PARAM_TEMPLATE}"
        # 【逗号只有一个】：属性区（group-title / tvg-logo）用空格分隔，
        # 逗号之后是显示名。这是 m3u 规范，任何播放器都能正确取到频道名。
        logo_part = f' tvg-logo="{logo}"' if logo else ""
        lines.append(f'#EXTINF:-1 group-title="{tag}"{logo_part},{n}')
        lines.append(url_out)
    if stats_out is not None:
        stats_out["direct"] = n_direct
        stats_out["fallback"] = n_fallback
    return "\n".join(lines) + "\n"


# ================================================================ R21 双轨输出
def split_tracks(urls: List[str], name_by_url: Optional[Dict[str, str]] = None
                 ) -> Tuple[List[str], List[str]]:
    """
    把频道列表切成"精简轨(HD)"与"全集轨(全量)"两条。

    【R21 第 2 项：为什么是双轨而不是单轨】
      单轨要么全给（305 条里混着标清/冷门台，播放器噪音大），要么挑给
      （手动维护白名单，源一变就得跟着改，必腐化）。双轨把两种需求分开，
      且**切分规则由数据决定、不由人决定**。

        - 精简轨 gx_clean_hd.m3u：只留"高清及以上"且名字干净的台。
          剔除：标清/2.5M/4M/8M/测试/直播 等低码率或非正式条目。
          保留：命中 高清/超清/蓝光/4K/8K/HD，或名字完全不带画质标记
                （视为默认高清）。
        - 全集轨 gx_full_305.m3u：**原样全量**，一条不删。它是"数据完整性"
          的基准 —— 精简轨漏掉的台，这里永远找得到。

    【为什么规则要写死在代码里、且可被断言覆盖】
      规则一旦藏在人脑里，下次源变更就没人记得当初为什么剔了某个台。
      写死 + 断言 = 规则可审计、可回归。

    Args:
        urls: 直链列表。
        name_by_url: url → 频道名映射。

    Returns:
        (精简轨 urls, 全集轨 urls)。全集轨与入参等长且同序。
    """
    name_by_url = name_by_url or {}
    # 低码率/非正式特征：出现即踢出精简轨
    low_re = re.compile(r"(标清|2\.5M|4M|8M|测试|直播)", re.I)
    # 高清特征：出现即保留
    hd_re = re.compile(r"(高清|超高清|超清|蓝光|4K|8K|HD)", re.I)
    # 任何画质标记（用于判定"名字完全不带画质标记"）
    any_q_re = re.compile(r"(标清|高清|超清|蓝光|4K|8K|HD|SD|[0-9.]+M)", re.I)
    clean: List[str] = []
    for u in urls:
        name = name_by_url.get(u, "")
        if low_re.search(name):
            continue
        if hd_re.search(name) or not any_q_re.search(name):
            clean.append(u)
    return clean, list(urls)


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
    # ---- R21 新增 ----
    ap.add_argument("--tracks", metavar="DIR",
                    help="R21 双轨输出目录：写 gx_clean_hd.m3u（精简高清轨）"
                         "与 gx_full_305.m3u（全集轨）。同一次抓取、同一快照")
    ap.add_argument("--logo-stats", metavar="PATH",
                    help="R21 把台标命中率统计写成 JSON（供 CI 断言消费）")
    ap.add_argument("--enable-catchup", action="store_true",
                    help="R21 注入时移(catchup)参数。**默认关闭**；"
                         "且时移规则未经本地实测确认前会被守卫拒绝")
    ap.add_argument("--verbose", action="store_true", help="打印过程")
    args = ap.parse_args()

    # 【R21 catchup 守卫：绝不在未验证前提下产出错误 m3u】
    #   实测（2026-09-23）：
    #     ① EPG 全字段仅 10 个，**零** playseek/timeshift 字段 → 上游不告诉
    #        你哪台支持时移、参数怎么拼。
    #     ② 沙箱与 GHA 出口都连不上移动 CDN（空回复）→ 无法在 CI 实测。
    #   所以 CATCHUP_RULE_VERIFIED 保持 False 时，显式开启也直接拒绝。
    #   这不是"功能没做完"，而是**拒绝在猜测的基础上产出错误产物** ——
    #   一个带错误 playseek 的 m3u 会让播放器点开就报错，比没有更糟。
    if args.enable_catchup and not CATCHUP_RULE_VERIFIED:
        print("[!] 拒绝执行 --enable-catchup：时移规则尚未经实测确认。",
              file=sys.stderr)
        print("    原因：EPG 无任何时移字段，且 CI 出口连不上移动 CDN，",
              file=sys.stderr)
        print("          无法验证 playseek 拼接方式与是否需额外 token。",
              file=sys.stderr)
        print("    步骤：在本地网络（手机 Termux / 连手机热点）跑 "
              "gx_catchup_probe.py，", file=sys.stderr)
        print("          按实测结论回填 CATCHUP_PARAM_TEMPLATE，并把 "
              "CATCHUP_RULE_VERIFIED 置 True。", file=sys.stderr)
        return 5

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

    # ============================================================ R21 双轨输出
    if args.tracks:
        # 【为什么双轨要一次算出两个文件、而不是两个独立开关】
        #   两个文件必须来自**同一次抓取**。若分两次跑，EPG 若在两次之间
        #   变了（哪怕一条），两份产物就不再是同一快照 —— 你无法回答
        #   "精简轨比全集轨少了哪几条、为什么少"。同快照是双轨可用性的前提。
        direct_icons = extract_icons(res.get("channels") or [])
        clean_urls, full_urls = split_tracks(res["urls"], name_by_url)
        stats: Dict[str, Any] = {}

        out_dir = os.path.dirname(os.path.abspath(args.tracks)) or "."
        os.makedirs(out_dir, exist_ok=True)
        clean_path = os.path.join(out_dir, "gx_clean_hd.m3u")
        full_path = os.path.join(out_dir, "gx_full_305.m3u")

        clean_txt = build_m3u(clean_urls, name_by_url,
                              direct_icons=direct_icons,
                              enable_catchup=args.enable_catchup,
                              stats_out=stats)
        with open(clean_path, "w", encoding="utf-8") as f:
            f.write(clean_txt)
        # 全集轨单独统计（命中率断言以全集轨为分母，它才是全体本）
        full_stats: Dict[str, Any] = {}
        full_txt = build_m3u(full_urls, name_by_url,
                             direct_icons=direct_icons,
                             enable_catchup=args.enable_catchup,
                             stats_out=full_stats)
        with open(full_path, "w", encoding="utf-8") as f:
            f.write(full_txt)

        # 命中率以**全集轨**为分母 —— 它是全体频道，不受精简规则影响，
        # 因而能真实反映"上游给了多少台标"。用精简轨做分母会因剔台而虚高。
        st = logo_stats(len(full_urls), full_stats.get("direct", 0),
                        full_stats.get("fallback", 0))
        print(f"[*] 双轨已写入：", file=sys.stderr)
        print(f"    {clean_path}  精简轨 {len(clean_urls)} 条", file=sys.stderr)
        print(f"    {full_path}  全集轨 {len(full_urls)} 条", file=sys.stderr)
        print(f"[*] 台标命中率（分母=全集轨 {st['total']}）：", file=sys.stderr)
        print(f"    直采 {st['direct']}  兜底 {st['fallback']}  "
              f"合计 {st['combined']}", file=sys.stderr)
        print(f"    direct_logo_rate   = {st['direct_logo_rate']:.4f}  "
              f"（硬线 ≥{DIRECT_LOGO_HARD_FLOOR} → "
              f"{'通过' if st['direct_ok'] else '失败'}）", file=sys.stderr)
        print(f"    combined_logo_rate = {st['combined_logo_rate']:.4f}  "
              f"（软线 ≥{COMBINED_LOGO_SOFT_TARGET} → "
              f"{'通过' if st['combined_ok'] else '告警(不拦发布)'}）",
              file=sys.stderr)
        if not st["direct_ok"]:
            # 硬线失败 = 上游 channelIcon 字段被削，属结构突变。
            print("[!] 硬线未达标：上游 channelIcon 覆盖率跌破 "
                  f"{DIRECT_LOGO_HARD_FLOOR}，疑似**上游字段变更**，"
                  "请核查 EPG 返回结构。", file=sys.stderr)
        if not st["combined_ok"]:
            # 软线失败只告警 —— 第三方（Gitee）抖动不该阻断发布链路。
            print(f"[~] 软线告警：合计命中率低于 {COMBINED_LOGO_SOFT_TARGET}，"
                  "多为 Gitee 兜底未命中，不影响发布。", file=sys.stderr)
        print("[!] 以上两个文件含内网直链，请勿提交仓库。", file=sys.stderr)

        # 命中率统计另存 JSON，供 CI 断言直接消费（避免解析 stderr 文本）。
        if args.logo_stats:
            os.makedirs(os.path.dirname(
                os.path.abspath(args.logo_stats)) or ".", exist_ok=True)
            with open(args.logo_stats, "w", encoding="utf-8") as f:
                json.dump(st, f, ensure_ascii=False, indent=2)
            print(f"[*] 命中率统计已写入 {args.logo_stats}", file=sys.stderr)

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
