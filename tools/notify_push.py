#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
微信告警推送器（R16）
=====================
职责边界
--------
  publish_cloud.py —— 探测结果归档 + 决定「本轮该不该推」（通知状态机）
  本文件           —— 只负责「把结论变成人话并推出去」

为什么单独成文件
----------------
  1. workflow 的 run: | 块里内嵌多行 Python 会破坏 YAML 缩进（R14 踩过一次），
     所以逻辑一律落成脚本；
  2. 推送是本项目**唯一**会主动打扰用户的动作，值得单独测试；
  3. 推送失败必须**不阻断发布**（R16-4），独立进程天然满足这个隔离要求。

文案设计（R16-2）
-----------------
复用户熟知的 merge_on_phone.build_alert_text 那一套措辞风格：
  · 大白话定性 —— 「源挂了」而不是「verdict=source_dead」
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

凭据（零字面量）
----------------
  PUSHPLUS_TOKEN  —— 只从环境变量读，代码里**没有任何 token 字面量**。
  缺失时不报错、不推送、打印一行说明后返回 0（视为「未启用推送」，
  而非失败）。这样仓库公开也不会误伤——fork 的人没有 secret 时流程照常绿。

用法：
    PUSHPLUS_TOKEN=xxx python3 notify_push.py --report report.json --status status.json
    python3 notify_push.py --report r.json --status s.json --dry-run   # 只打印文案
    python3 notify_push.py --status s.json --daily                     # 早报模式
"""
from __future__ import annotations

import argparse
import json
import os
import ssl
import sys
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

CST = timezone(timedelta(hours=8))

# PushPlus 接口（R16 规格给定）。只写接口地址，token 走环境变量。
PUSHPLUS_ENDPOINT = "https://www.pushplus.plus/send"

# 凭据环境变量名。workflow 里注入 secrets.PUSH_TOKEN。
ENV_TOKEN = "PUSHPLUS_TOKEN"

# 网络超时：推送不该拖慢整个 workflow
HTTP_TIMEOUT = 10.0

# 徽章：与看板/合并文案保持同一套图标语义
ICON_ALARM = "🔴"
ICON_RECOVER = "✅"
ICON_DAILY = "📋"

# 网络状态 → 人话（与 probe_local.assess_network 的语义对齐）
NETWORK_TEXT = {
    "lan_down": "家里网络断了（路由器 / 局域网）",
    "wan_down": "宽带断了（运营商线路）",
    "net_ok": "本机网络健康",
    "net_unknown": "本机网络状态未知",
}

# 告警 id → 人话定性 + 处置建议。
# 这张表就是「大白话」的核心：左侧是机器判定，右侧是人真正需要知道的两件事
# —— 出了什么事、我该干什么。
ALARM_PLAYBOOK: Dict[str, Tuple[str, str]] = {
    "NETWORK": (
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

    Raises:
        无。所有异常都被收敛成 (False, 原因) —— 推送失败不应该炸掉调用方，
        调用方需要的是「知道失败了」而不是「拿到一个异常」。
    """
    body = json.dumps({
        "token": token,
        "title": title,
        "content": content,
        # 用 txt 模板：内容是给人读的纯文本，markdown 渲染反而会把
        # 「→」和缩进吃掉，导致排版散架。
        "template": "txt",
    }).encode("utf-8")

    req = urllib.request.Request(
        PUSHPLUS_ENDPOINT,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        detail = ""
        try:
            detail = e.read().decode("utf-8", errors="replace")[:200]
        except Exception:                       # noqa: BLE001
            pass
        return False, _scrub(f"HTTP {e.code} {detail}", token)
    except Exception as e:                      # noqa: BLE001
        # 网络层问题（DNS/TLS/超时）统一收敛
        return False, _scrub(f"{type(e).__name__}: {e}", token)

    # PushPlus 的成功回执是 {"code":200,"msg":"请求成功",...}
    try:
        js = json.loads(raw)
    except ValueError:
        return False, _scrub(f"回执非 JSON：{raw[:200]}", token)

    code = js.get("code")
    if code == 200:
        return True, str(js.get("msg") or "ok")
    return False, _scrub(f"code={code} msg={js.get('msg')}", token)


def _scrub(text: str, token: str) -> str:
    """
    从错误文本里抹掉 token。

    【为什么必要】PushPlus 的报错有时会把 token 回显进 msg，而这段文本会被
    写进 status/latest.json（公开仓库）和 Actions 日志。凭据泄漏面必须堵死。
    """
    if token:
        text = text.replace(token, "***")
    return text[:300]


# =============================================================== CLI
def _load_json(path: Optional[str]) -> Optional[Dict[str, Any]]:
    if not path or not os.path.isfile(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def main() -> int:
    ap = argparse.ArgumentParser(
        description="R16 微信告警推送（PushPlus）。凭据只从环境变量 "
                    f"{ENV_TOKEN} 读取，代码零字面量。")
    ap.add_argument("--status", required=True,
                    help="status/latest.json 路径（含 alarms/verdicts/local_freshness）")
    ap.add_argument("--report", default=None,
                    help="可选的探测报告（补充公网计数）")
    ap.add_argument("--kind", default="first_alarm",
                    choices=["first_alarm", "recover", "daily"],
                    help="通知类型（由 publish_cloud 的通知状态机决定）")
    ap.add_argument("--daily", action="store_true",
                    help="早报模式（R16-5）：内容为看板摘要，语气中性")
    ap.add_argument("--pages-url", default=None, help="看板地址，写进正文")
    ap.add_argument("--dry-run", action="store_true",
                    help="只打印文案，不实际发送（不需要 token）")
    ap.add_argument("--text-out", default=None, help="把文案写到文件（便于本地核验）")
    args = ap.parse_args()

    status = _load_json(args.status)
    if status is None:
        print(f"[!] 读不到 status：{args.status}", file=sys.stderr)
        return 2

    # 复用 publish_cloud 的提炼逻辑，保证「决定推什么」与「推什么内容」
    # 永远读同一份数据，不会出现两处口径不一致。
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    try:
        import publish_cloud as pc
    except Exception as e:                      # noqa: BLE001
        print(f"[!] 无法导入 publish_cloud：{e}", file=sys.stderr)
        return 2

    payload = pc.extract_push_content(status)
    report = _load_json(args.report)
    if report:
        cs = (report.get("summary") or {})
        # 只有 status 里缺计数时才回退用 report（status 是权威来源）
        if payload["cloud"].get("total_stream") is None:
            payload["cloud"].update({
                "ok_stream": cs.get("ok_stream"),
                "total_stream": cs.get("total_stream"),
                "ok_list": cs.get("ok_list"),
                "total_list": cs.get("total_list"),
            })
    payload["pages_url"] = args.pages_url or pc.DEFAULT_PAGES_URL

    if args.daily:
        title, content = build_early_report_text(payload)
    else:
        title, content = build_push_text(payload, kind=args.kind)

    print("=" * 60)
    print(f"标题：{title}")
    print("-" * 60)
    print(content)
    print("=" * 60)

    if args.text_out:
        with open(args.text_out, "w", encoding="utf-8") as f:
            f.write(f"{title}\n\n{content}\n")
        print(f"[*] 文案已写入 {args.text_out}", file=sys.stderr)

    if args.dry_run:
        print("[*] dry-run：未发送", file=sys.stderr)
        return 0

    token = os.environ.get(ENV_TOKEN, "").strip()
    if not token:
        # 未配置 token = 未启用推送。**不是失败**：仓库是公开的，
        # fork 之后没有 secret 的人不应该看到一条红色 workflow。
        print(f"[*] 未设置 {ENV_TOKEN}，跳过推送（视为未启用）", file=sys.stderr)
        return 0

    ok, msg = send_pushplus(token, title, content)
    if ok:
        print(f"[*] 推送成功：{msg}", file=sys.stderr)
        return 0
    print(f"[!] 推送失败：{msg}", file=sys.stderr)
    # 返回 3 让 workflow 步骤标红，但该步骤是 continue-on-error，
    # 不会阻断后续发布（R16-4）。
    return 3


if __name__ == "__main__":
    sys.exit(main())
