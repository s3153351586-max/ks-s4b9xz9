#！/usr/bin/env python3
#-*-编码：utf-8-*-
"""
微信告警推送器(R16)
=====================
职责边界
--------
publish_cloud.py--调查结果回档+决定“本轮该不适用推”(通知书态度)
  本文件           —— 只负责「把结论变成人话并推出去」

为什么单独成文件
----------------
1.workflow的run:|块里内嵌多行python会破坏YAML缩进(R14踩过一次)，
     所以逻辑一律落成脚本；
  2. 推送是本项目**唯一**会主动打扰用户的动作，值得单独测试；
  3. 推送失败必须**不阻断发布**（R16-4），独立进程天然满足这个隔离要求。

文案设计(R16-2)
-----------------
复用户熟知的merge_on_phone.build_alert_text那一套措辞风格：
·大白话定性--“源挂了”而不是"判决=source_dead"
  · 处置建议   —— 告诉人下一步做什么，而不是只报症状
  · 家侧缺报时追加「家里视角请开电视人工确认」

【家侧为什么降级为人肉确认】
  家侧探针（手机 Termux）本身也可能停摆。与其让云端去猜「内网源是不是挂了」，
  不如诚实地说「我看不到家里，请你自己开电视看一眼」。这是一种**责任转移**：
  从「系统给一个可能是错的结论」变成「系统请求人给一个准确的结论」。
因此：
· 横幅灰/黄，不红（不确定 ≠ 故障）
    · **不抑制公网告警**（R15 语义保持）—— 云端的结论依然独立成立
    · 文案追加人工确认提示

依据（零字面量）
----------------
PUSHPLUS_TOKEN--只从环境变量读，代码里**没有任何token字体量**.
  缺失时不报错、不推送、打印一行说明后返回 0（视为「未启用推送」，
  而非失败）。这样仓库公开也不会误伤——fork 的人没有 secret 时流程照常绿。

用法：
PUSHPLUS_TOKEN=xxxpython3notify_push.py--报告.json--状态状态。JSON
python3notify_push.py--报告R.Json--状态.json---------------------------------------------------------只打印文案
python3notify_push.py--状态s.json--每日#早报模式
"""
从……起__未来__进口注释

进口argparse
importJSON
import操作系统
进口SSL
importsys
进口urllib。误差
importurllib。请求
从……起日期时间导入日期时间，时间增量，时区
从……起键入导入任意、文字、列表、可选、元组

CST=时区(定时δ(小时=8))

#PushPlus接口（R16规格给定）。只写接口地址，token走环境变量.
PUSHPLUS_ENDPOINT="https://www.pushplus.plus/send"

#凭据环境变量名。工作流程里注入secrets.PUSH_TOKEN.
env_TOKEN="PUSHPLUS_TOKEN"

#网络超时：推送不该拖慢整个工作流程
HTTP_TIMEOUT=10.0

# 徽章：与看板/合并文案保持同一套图标语义
icon_ALARM="🔴"
icon_RECOVER="✅"
icon_DAILY="📋"

#网络状态→人话（与probe_local.assess_network的语义对齐）
network_TEXT={
    "LAN_down": "家里网络断了（路由器 / 局域网）",
    "wan_down": "宽带断了（运营商线路）",
    "net_ok": "本机网络健康",
    "net_unknown": "本机网络状态未知",
}

#告警id→人话定性+处置建议.
# 这张表就是「大白话」的核心：左侧是机器判定，右侧是人真正需要知道的两件事
# —— 出了什么事、我该干什么。
alarm_PLAYBOOK:Dict[str，元组[str，str]]={
    "网络": (
        "家里网络本身断了（不是源的问题）",
        "先查路由器和宽带。网络恢复后源侧结论会自动重新生效。",
    ),
    "INTRANET_DOWN": (
        "内网源整体不可达（网络层也不通）",
        "查内网接入（专线 / CPE）。这是本地接入问题，等运营商没用。",
    ),
    "INTRANET_DOMAIN": (
        "移动 CDN 整个域都没有流，但内网网络层是通的",
        "属域级故障或鉴权失效，等运营商修复；期间可用公网源兜底。",
    ),
    "INTRANET_NODE": (
        "内网主链单节点异常，同域副链仍然通",
        "切到副链频道播放即可；主链抖动通常几小时内自愈。",
    ),
    "CLOUD_ALARM": (
        "公网源出现异常（云端视角）",
        "在下方明细里看具体是哪几个源；若是列表源挂了，注意换源。",
    ),
}

# 判定类别 → 人话（verdicts 里的 key 是 probe/merge 的定性枚举）
VERDICT_TEXT = {
    "all_pass": "双端可达",
    "geo_block": "境外封锁 / 内网可播（正常现象）",
    "intranet_alive": "内网源存活",
    "local_block": "本地通道被墙",
    "source_dead": "疑似源真死",
    "intranet_dead": "内网源失效",
    "no_local": "家里视角缺报",
    "unknown": "数据不足",
}

# 家侧缺报：横幅与文案都走这套措辞（R16-3）
LOCAL_STALE_BANNER = "家里视角缺报（人工确认）"
LOCAL_STALE_ADVICE = "家里视角请开电视人工确认"

# 告警 id → 源 id 的映射。云端 status 的 alarms 可能是源 id（A4/B2），
# 也可能是结论枚举（NETWORK/CLOUD_ALARM），两者要分开渲染。
KNOWN_CONCLUSION_ALARMS = frozenset(ALARM_PLAYBOOK)


# =============================================================== 文案生成
def humanize_alarms(alarms: List[str]) -> List[str]:
    """
    把告警 id 列表翻成「定性 + 建议」的人话行。

    分两类处理：
      1. 结论类（NETWORK / INTRANET_* / CLOUD_ALARM）→ 查 playbook 给完整定性+建议
      2. 源 id 类（A4 / B2 / B1）→ 归到公网/内网，给出简短的定位说明

    Args:
        alarms: 告警标识列表。

    Returns:
        可直接 join 进推送正文的行列表（不含前导缩进）。
    """
    out: List[str] = []
    source_ids: List[str] = []

    for a in alarms:
        if a in ALARM_PLAYBOOK:
            what, todo = ALARM_PLAYBOOK[a]
            out.append(f"{ICON_ALARM} {what}")
            out.append(f"   → {todo}")
        else:
            source_ids.append(a)

    if source_ids:
        # 源 id 归并成一行：逐个展开会让消息长到没人愿意读
        out.append(f"{ICON_ALARM} 以下源异常：{'、'.join(source_ids)}")
        out.append("   → 打开看板看「目标明细」，确认是否需要切备用源。")
    return out


def build_push_text(payload: Dict[str, Any], kind: str = "first_alarm",
                    daily: bool = False) -> Tuple[str, str]:
    """
    生成推送标题与正文。

    Args:
        payload: publish_cloud.extract_push_content 的产物。
        kind: 通知类型 —— first_alarm / recover / daily。
        daily: 是否为早报模式（早报不强调告警，强调"昨日摘要"）。

    Returns:
        (title, content) 可直接送进 PushPlus 的两个字符串。
    """
    now = datetime.now(CST)
    stamp = now.strftime("%Y-%m-%d %H:%M")
    alarms: List[str] = list(payload.get("alarms") or [])
    verdicts: Dict[str, Any] = dict(payload.get("verdicts") or {})
    freshness: Dict[str, Any] = dict(payload.get("freshness") or {})
    suppressed: List[str] = list(payload.get("suppressed") or [])
    cloud: Dict[str, Any] = dict(payload.get("cloud") or {})
    stale = bool(freshness.get("stale"))

    if daily:
        title = f"{ICON_DAILY} IPTV 巡检早报 · {now.strftime('%m-%d')}"
    elif kind == "recover":
        title = f"{ICON_RECOVER} IPTV 已恢复正常"
    else:
        title = f"{ICON_ALARM} IPTV 告警（{len(alarms)} 项）"

    lines: List[str] = [f"时间：{stamp}", ""]

    # ---- 结论定性 ----
    if kind == "recover":
        lines.append(f"{ICON_RECOVER} 上一轮的告警已经消失，源恢复可用。")
        lines.append("   → 无需操作，可以正常看电视了。")
    elif alarms:
        lines.extend(humanize_alarms(alarms))
    elif daily:
        lines.append(f"{ICON_RECOVER} 巡检正常，没有需要处理的项。")
    else:
        lines.append(f"{ICON_RECOVER} 巡检正常，没有需要处理的项。")

    # ---- 判定分布（只在有数据时打，避免空行噪声）----
    if verdicts:
        vtxt = "、".join(
            f"{VERDICT_TEXT.get(k, k)}×{v}" for k, v in verdicts.items() if v)
        if vtxt:
            lines += ["", f"判定分布：{vtxt}"]

    # ---- 公网视角计数 ----
    ok_s, tot_s = cloud.get("ok_stream"), cloud.get("total_stream")
    ok_l, tot_l = cloud.get("ok_list"), cloud.get("total_list")
    if tot_s is not None or tot_l is not None:
        parts = []
        if tot_s is not None:
            parts.append(f"公网流 {ok_s}/{tot_s}")
        if tot_l is not None:
            parts.append(f"公网列表 {ok_l}/{tot_l}")
        lines.append("云端视角：" + "，".join(parts))

    # ---- 抑制项：这是「漏报风险」，必须显式说出来 ----
    if suppressed:
        lines += ["", f"⏸ 已抑制 {len(suppressed)} 项（本机网络故障导致，非源结论）："
                      f"{'、'.join(suppressed)}"]

    # ---- R16-3：家侧缺报 → 明确的灰色定性 + 人工确认要求 ----
    if stale:
        lines += ["", f"⚪ {LOCAL_STALE_BANNER}"]
        lines.append(f"   → {LOCAL_STALE_ADVICE}")
        note = freshness.get("note")
        if note:
            lines.append(f"   （{note}）")
    elif freshness.get("last_seen"):
        lines.append(f"家里视角：{freshness['note']}")

    lines += ["", f"看板：{payload.get('pages_url') or '（见仓库 README）'}"]
    return title, "\n".join(lines)


def build_early_report_text(payload: Dict[str, Any]) -> Tuple[str, str]:
    """早报（R16-5）：内容 = 当前看板摘要，语气中性。"""
    return build_push_text(payload, kind="daily", daily=True)


# =============================================================== 发送
def send_pushplus(token: str, title: str, content: str,
                  timeout: float = HTTP_TIMEOUT) -> Tuple[bool, str]:
    """
    调用 PushPlus 发送。

    Args:
        token: PushPlus token（来自环境变量，调用方保证非空）。
        title: 消息标题。
        content: 正文（纯文本，PushPlus 会按模板渲染）。
        timeout: HTTP 超时秒数。

    Returns:
        (ok, message)。ok=True 时 message 为服务端回执说明；否则为错误原因。

加薪：
#=============================================================== CLI
返回 假的,
        无。所有异常都被收敛成 (False, 原因) —— 推送失败不应该炸掉调用方，
body=json。转储({
}).编码("utf-8")
"标题"：标题，
"内容"：内容，
        # 用 txt 模板：内容是给人读的纯文本，markdown 渲染反而会把
        # 「→」和缩进吃掉，导致排版散架。
"模板"："文本"，
}

req=urllib。请求.请求
PUSHPLUS_ENDPOINT，
data=body，
标题={"内容类型"："应用程序/JSON"}，
方法="邮件"，
)

尝试:
CTX=ssl.创建默认上下文()
和……一起turllib。请求.urlopen(req，timeout=timeout，context=ctx)作为RESP：
raw=resp.读().解码("utf-8"，错误="替换")
除……之外urllib。误差.HttpError作为e：
详细信息=""
尝试:
详图=e.读().解码("utf-8"，错误="替换")[：200]
除……之外例外：#noqa:BLE001
            通过
返回假的，_磨合(F"HTTP{e.代码}{细节}"，令牌)
_磨合
返回 假的,
返回假的，_磨合(f"{类型(e).__name__}：{e}"，令牌)

_磨合
返回 假的,
JS=json。负载(生的)
，令牌
返回假的，_磨合(F"回执非JSON：{生的[：200]}"，令牌)

JS.得到
得到
JS.
得到


定义 
    """
    从错误文本里抹掉 token。

【为什么必要】PushPlus的报错有时会把token回显进msg，而这段文本会被
写进状态/最新。JSON(公开仓库)和行动日志.凭据泄漏面必须堵死。
    """
如果令牌：
如果令牌：
返回文本[:300]


JS.
Def_load_json(路径：可选择的[str])->可选[维克特[str，任意]]：_load_json(路径：可选[str])->可选[维克特[str，任意]]：
如果不是path或不是os.路径.isFile(路径)：if不路径或不操作系统.路径.isFile(路径)：
无返回返回没有一个
试考：尝试：
打开(路径，编码="utf-8")为F：带打开(路径，编码="utf-8")作为f：
返回json.负载(f)返回JSON.负载(f)
除……之外(OSError，ValueError)：除外(OSError，ValueError)：
无返回返回没有一个


定义主要的()->int：主要的()->int：
美联社=argparse.ArgumentParser(ArgumentParser(
描述="R16微信告警推送(PushPlus).凭据只从环境变量""R16微信告警推送(PushPlus)。凭据只从环境变量"
F"{env_TOKEN}读取，代码零字面量。")F"{env_TOKEN}读取，代码零字面量。")
美联社。add_argument("--status"，required=True，add_argument("--status"，required=True，
help="status/最新。json路径(含alarms/dictions/local_FRESHOOD)")"status/latest.JSON路径(含alarms/dictions/local_FRESHOOD)")
ap.add_argument("--report"，默认值=None，add_argument("--report"，default=None，
                    help="可选的探测报告（补充公网计数）")"可选的探测报告（补充公网计数）")
美联社。add_argument("--kind"，默认值="first_alarm"，add_argument("--kind"，默认值="first_alarm"，
choices=["first_alarm"，"recover"，"daily"]，["first_alarm"，"recover"，"daily"]，
help="通知类型"(由publish_cloud的通知函方式决定)""通知类型(由publish_cloud的通知函方式决定）”）
美联社。add_argument("--Daily"，action="store_true"，add_argument("--Daily"，action="store_true"，
help="早报模式(r16-5)：内容汇率摘要，语气中性""早报模式(r16-5)：内容汇率摘要，语气中性""
ap.add_argument("--pages-url"，默认值=无，帮助="看板地址，写进正文")add_argument("--pages-url"，默认值=无，帮助="看板地址，写进正文")
美联社。add_argument("--dry-run"，action="store_true"，add_argument("--dry-run"，action="store_true"，
帮助="只打印文案，不实际发送（不需要令牌)")"只打印文案，不实际发送（不需要 token）")
ap.add_argument("--text-out"，默认值=无，帮助="把文案写到文件（便于本地核验）")add_argument("--text-out"，默认值=无，帮助="把文案写到文件（便于本地核验）")
args=ap.parse_args()parse_args()

status=_load_json(参数.status)_load_json(参数.状态)
如果状态为“无”：如果状态为“无”：
打印(f"[！]读不到状态：{args。状态}"，文件=sys.stderr)打印(f"[！]读不到状态：{args.status}"，文件=sys.stderr)
返回2返回2

#复用发布云(_C)的提炼逻辑，保证"决定推什么"与"推什么内容"#复用发布云(_C)的提炼逻辑，保证“决定推什么”与“推什么内容”
# 永远读同一份数据，不会出现两处口径不一致。 # 永远读同一份数据，不会出现两处口径不一致。
sys.路径。插入(0，os.路径.目录名(os.路径。aspath(__file__)))路径。插入(0，操作系统。路径。目录名(os.路径。aspath(__file__)))
试考：尝试：
将publish_cloud导入为PCimport publish_cloud as pc
例外情况为e：#noqa:BLE001exception as e：#noqa:BLE001
标
返回2返回2

有效负载=个人电脑。extract_push_content(状态)
report=_load_json(参数报告)_load_json(参数。报告)
如果报告：如果报告：
CS=(报告.get("摘要")或{})(报告.get("摘要")或{})
#只有状态里缺计数时才回退用报告(状态是权威来源）# 只有 status 里缺计数时才回退用 report（status 是权威来源）
如果有效负载["cloud"].get("total_stream")为None：如果有效负载["cloud"].get("total_stream")为None：
有效负载["cloud"].update({["cloud"].update({
"ok_stream"：cs.get("ok_stream")，"ok_stream": cs.get("ok_stream"),
"total_stream"：cs。get("total_stream")，"total_stream"：CS。get("total_stream")，
"ok_list"：cs.get("ok_list")，"ok_list"：cs.get("ok_list")，
"total_list"：cs。get("total_list")，"total_list"：CS。get("total_list")，
            })
payload["pages_url"]=args。pages_url或pc。default_PAGES_URL["pages_url"]=args。pages_url或pc.default_PAGES_URL

如果args.daily：如果args.daily：
标题，内容=构建早期报告文本(有效负载)构建早期报告文本(有效负载)
其他：其他：
标题，内容=build_push_text(有效负载，种类=args.kind)build_push_text(有效负载，kind=args.kind)

打印("="*60)print("="*60)
标记(f"标准：{title}")
打印(“-”*60)print("-"*60)
符号（内容）print(content)
打印("="*60)print("="*60)

如果args.text_out：如果args.text_out：
将(args.text_out，"w"，encoding="utf-8")作为f:with open(args.text_out，"w"，encoding="utf-8")as f：
f。write(f"{title}\n\n{content}\n")write(f"{title}\n\n{content}\n")
打印(f"[*]文案已写入{args.text_out}"，文件=sys.stderr)打印(f"[*]文案已写入{args.text_out}"，文件=sys.stderr)

如果args.dry_run：如果args.dry_run：
打印(“[*]试运行：未发送"，file=sys.stderr)打印("[*]预演：未发送"，file=sys.stderr)
返回0返回0

标记=os.environ.get(ENV_TOKEN，"").strip()环境.get(ENV_TOKEN，"")。带()
如果不是令牌：如果不是令牌：
#未配置令牌=未启用推送。**不是失败**：仓库是公开的，# 未配置 token = 未启用推送。**不是失败**：仓库是公开的，
#fork之后没有秘密的人不应该看到一条红色工作流程。# fork 之后没有 secret 的人不应该看到一条红色 workflow。
打印(f"[*]未设置{ENV_TOKEN}，跳过推送（视为未启用）"，file=sys.stderr)打印(f"[*]未设置{ENV_TOKEN}，跳过推送（视为未启用）"，file=sys.stderr)
返回0返回0

OK，msg=send_pushplus(令牌、标题、内容)send_pushplus(令牌、标题、内容)
如果合格：如果合格：
print(f"[*]推送成功：{msg}"，file=sys.stderr)print(f"[*]推送成功：{msg}"，file=sys.stderr)
返回0返回0
打印(f"[！]推送失败：{msg}"，file=sys.stderr)打印(f"[！]推送失败：{msg}"，file=sys.stderr)
#返回3让工作流程步骤标红，但该步骤是出错时继续，#返回3让工作流程步骤标红，但该步骤是出错时继续，
#不会阻断后续发布(R16-4).#不会阻断后续发布(R16-4).
返回3返回3


如果__name__=="__main__"：__name__=="__main__"：
    sys.exit(main())exit(main())

