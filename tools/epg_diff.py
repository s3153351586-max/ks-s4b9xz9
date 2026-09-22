#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EPG 对比模块（R18）：官方 EPG 列表 vs Healer gx.m3u 的**上游死亡预警**。

===============================================================================
为什么需要它
===============================================================================
我们的源来自 Healer-sys 的公开 gx.m3u 列表 —— 那是一个**第三方聚合**，
它可能删库、大改、或长期不更新。而官方 EPG 接口（`gx_epg_fetch.py`）是
**一手数据**。两者对比就能回答一个关键问题：

    Healer 列表是否还跟得上官方？偏离到某个程度 = 上游可能已经死了。

===============================================================================
实测基线（2026-09-22，决定了阈值怎么定）
===============================================================================
    EPG 唯一有效直链   275
    Healer 直链        148
    交集               119
    Jaccard            0.391   ← 两者部分重叠、各有独有

    ⇒ **Jaccard 绝对值不可作阈值**（0.391 是常态，不是异常）。
      告警必须用「相对自身历史基线」的漂移判定，而不是绝对水位。

===============================================================================
告警规则（R-a/R-b/R-c/R-d）
===============================================================================
    R-b  critical  Healer 拉取失败 / 无 #EXTM3U / count==0
                   → 立即告警，**不看基线**（一级战备前兆）
    R-a  warning   healer_count 较 7 天中位数下降 >40%
                   → "Healer 上游异常（疑似删库/大改）"
    R-c  info      epg_count 偏离自身中位数 >30% → 仅记录不推送
    R-d  info      jaccard 较自身中位数相对下降 >50% → 仅记录不推送

    warmup 期（history < 7 天）：仅 R-b 生效。

===============================================================================
安全边界（本模块**必须**只产 masked 数据）
===============================================================================
本模块的输出会进公开仓库，因此：

  - **禁止**输出任何 URL / UUID / host —— 只输出计数与比率；
  - 直链抽取**必须** import `probe_local.extract_cdn_urls`，绝不新写正则
    （铁律：全项目只有一份直链识别正则，复制必然漂移）；
  - `epg_raw`（`gx_epg_fetch.py --json` 的产物）**含直链，绝不入库/上传**，
    只作为本模块的**内存输入**。

最终防线在 `publish_cloud.assert_epg_masked()` —— 但那不该是唯一防线，
本模块的 schema 设计是白名单式的：只挑计数键，新增字段默认不外泄。
"""

from __future__ import annotations

import argparse
import json
import os
import re
import statistics
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlsplit, urlunsplit

# 复用现有的直链识别正则与抽取函数 —— 铁律：不许有第二份正则。
# 复制一份必然与 probe_local 漂移，而漂移后"同一个 URL 一个认一个不认"极难排查。
from probe_local import CDN_URL_RE, extract_cdn_urls  # noqa: F401

CST = timezone(timedelta(hours=8))

VERSION = "r18"

# ---------------------------------------------------------------- 阈值常量
# 全部集中在这里，方便 7 天 warmup 后人工复核调参（见审计报告遗留项）。
BASELINE_WINDOW_DAYS = 7          # 滚动窗口长度
R_A_HEALER_DROP = 0.40            # Healer 计数下降比例阈值
R_C_EPG_DEVIATION = 0.30          # EPG 计数偏离比例阈值
R_D_JACCARD_DROP = 0.50           # Jaccard 相对下降比例阈值

# 归一化规则标识（写进报告 meta，便于事后追溯"这批数据是怎么算的"）
NORMALIZATION_RULE = "strip-query+strip-trailing-slash"


def _now() -> datetime:
    return datetime.now(CST)


# ================================================================ 归一化
def normalize_url(u: str) -> str:
    """
    归一化 URL，供交集计算使用。

    为什么必须归一化：同一条流在不同来源里可能写成：
        http://host/path/index.m3u8?servicetype=1
        http://host/path/index.m3u8
        http://host/path/index.m3u8/
    不归一化的话，这三条会被算成"三条不同的流"，
    Jaccard 会被**格式差异**污染 —— 那是假漂移，不是真问题。

    【关键决策：去 query 是否安全？】
      对"计数对比"这个用途是安全的 —— 我们关心的是"是不是同一路流",
      而 query 里的 servicetype 等是访问参数，不改变资源的身份。
      反过来，**保留** query 会让 CDN 加个参数就触发假告警。

    Args:
        u: 原始 URL。

    Returns:
        归一化后的 URL；空输入返回空串。
    """
    if not u:
        return ""
    s = u.strip()
    try:
        parts = urlsplit(s)
    except ValueError:
        return s
    # 去 query 与 fragment
    path = parts.path
    # 去尾斜杠（但保留根路径的单斜杠，虽然直链不会是根路径）
    while len(path) > 1 and path.endswith("/"):
        path = path[:-1]
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), path, "", ""))


def normalize_set(urls: Sequence[str]) -> set:
    """
    ���一组 URL 归一化并去重。

    Args:
        urls: 原始 URL 序列。

    Returns:
        归一化后的集合（空值被丢弃）。
    """
    out = set()
    for u in urls or ():
        n = normalize_url(u)
        if n:
            out.add(n)
    return out


# ================================================================ 计数对比
def diff_counts(epg_urls: Sequence[str],
                healer_urls: Sequence[str]) -> Dict[str, Any]:
    """
    纯计数级对比。**只返回数字**，不含任何 URL。

    Args:
        epg_urls: 官方 EPG 抽出的直链。
        healer_urls: Healer gx.m3u 抽出的直链。

    Returns:
        dict，含 epg_count / healer_count / intersect / jaccard /
        epg_only_count / healer_only_count / union_count。
        全部是计数或比率，可安全发布。
    """
    epg_set = normalize_set(epg_urls)
    healer_set = normalize_set(healer_urls)

    inter = epg_set & healer_set
    union = epg_set | healer_set

    return {
        "epg_count": len(epg_set),
        "healer_count": len(healer_set),
        "intersect": len(inter),
        "epg_only_count": len(epg_set - healer_set),
        "healer_only_count": len(healer_set - epg_set),
        "union_count": len(union),
        # 空集时定义为 0.0，而不是除零崩掉 —— 上游全挂时这个值会被 R-b 盖过，
        # 但报告本身必须能生成出来（能生成才能留痕）。
        "jaccard": round(len(inter) / len(union), 4) if union else 0.0,
    }


def _healer_urls_from_text(text: Optional[str]) -> Tuple[List[str], bool, str]:
    """
    从 Healer m3u 文本抽取直链。

    Returns:
        (urls, ok, note)。ok=False 表示上游形态不对（空 / 缺 #EXTM3U）。
    """
    if not text:
        return [], False, "Healer 列表拉取失败或为空"
    if "#EXTM3U" not in text:
        return [], False, "Healer 响应不含 #EXTM3U，可能不是 m3u（被劫持/换内容）"
    urls = extract_cdn_urls(text, limit=10000)
    if not urls:
        return [], False, "Healer m3u 里 0 条本项目认得的直链"
    return urls, True, f"Healer m3u 抽到 {len(urls)} 条直链"


# ================================================================ 报告构造
def build_epg_diff(epg_result: Optional[Dict[str, Any]],
                   healer_text: Optional[str],
                   healer_fetch_note: str = "",
                   history: Optional[Sequence[Dict[str, Any]]] = None,
                   channel_names_top: int = 0) -> Dict[str, Any]:
    """
    构造 masked 对比报告。

    Args:
        epg_result: `gx_epg_fetch.fetch()` 的返回 dict（**内存输入，不入库**）。
        healer_text: Healer m3u 全文（**内存输入，不入库**）。
        healer_fetch_note: Healer 拉取通道说明。
        history: 历史 diff 报告列表（用于算 baseline）。
        channel_names_top: 保留多少个频道名（**0 = 不保留**，默认关）。

    Returns:
        masked 报告 dict，可安全发布到公开仓库。
    """
    epg_result = epg_result or {}
    epg_urls: List[str] = list(epg_result.get("urls") or [])
    epg_ok = bool(epg_result.get("ok"))
    epg_stage = epg_result.get("stage") or "unknown"

    healer_urls, healer_ok, healer_note = _healer_urls_from_text(healer_text)

    counts = diff_counts(epg_urls, healer_urls)

    report: Dict[str, Any] = {
        "version": VERSION,
        "generated_at": _now().isoformat(),
        "normalization_rule": NORMALIZATION_RULE,
        "epg_stage": epg_stage,
        "epg_ok": epg_ok,
        "healer_fetch_ok": healer_ok,
        # 计数（§ diff_counts 全数字，安全）
        **counts,
        # 占位符与共链：证明"过滤真的生效了"，而非静默丢数据
        "placeholder_count": int(epg_result.get("dropped_placeholder") or 0),
        "multi_name_groups": int(epg_result.get("multi_name_groups") or 0),
        "raw_channel_count": int(epg_result.get("raw_count") or 0),
        # 频道名 Top-N：**默认不保留**。
        # 理论上频道名不算敏感（LEAK_MARKERS 里没有它，EPG-API-NOTES 也公开讨论过），
        # 但本报告的用途（漂移预警）**不需要**逐条名字 —— 计数足够。
        # 最小化原则：能不给就不给。
        "channel_names_top": [],
        "baseline": {},
        "drift": {},
        "meta": {"healer_fetch_note": healer_fetch_note, "note": healer_note},
    }

    if channel_names_top and epg_result.get("name_by_url"):
        # 仅当显式开启时才有值（默认关，故正常路径不会有名字）
        names = sorted({n for n in (epg_result.get("name_by_url") or {}).values() if n})
        report["channel_names_top"] = names[:channel_names_top]

    report["baseline"], report["drift"] = assess_drift(report, history or [])
    return report


# ================================================================ 漂移判定
def _median(values: Sequence[float]) -> Optional[float]:
    """中位数；空序列返回 None。

    为什么用中位数而不是均值：上游偶发抽风会拉偏均值，
    而中位数对离群值稳健 —— 告警阈值必须建在稳健统计量上。
    """
    vals = [v for v in values if isinstance(v, (int, float))]
    if not vals:
        return None
    return float(statistics.median(vals))


def assess_drift(cur: Dict[str, Any],
                 history: Sequence[Dict[str, Any]]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    用滚动窗口历史算 baseline，给出 R-a/R-b/R-c/R-d 判定。

    Args:
        cur: 当天的 diff 报告（需含 healer_count / epg_count / jaccard）。
        history: 历史 diff 报告列表（不含当天）。

    Returns:
        (baseline, drift) 二元组，均为 masked dict。
    """
    window = list(history or [])[-BASELINE_WINDOW_DAYS:]

    healer_series = [h.get("healer_count") for h in window
                     if isinstance(h.get("healer_count"), (int, float))]
    epg_series = [h.get("epg_count") for h in window
                  if isinstance(h.get("epg_count"), (int, float))]
    jac_series = [h.get("jaccard") for h in window
                  if isinstance(h.get("jaccard"), (int, float))]

    # warmup：样本不足一整个窗口时，相对基线判定没有统计意义。
    # 这期间只让 R-b（硬底线）生效 —— 它不依赖历史。
    warmup = len(window) < BASELINE_WINDOW_DAYS

    med_healer = _median(healer_series)
    med_epg = _median(epg_series)
    med_jac = _median(jac_series)

    baseline: Dict[str, Any] = {
        "warmup": warmup,
        "window_days": BASELINE_WINDOW_DAYS,
        "samples": len(window),
        "medians": {
            "healer_count": med_healer,
            "epg_count": med_epg,
            "jaccard": med_jac,
        },
    }

    drift: Dict[str, Any] = {
        "R-a": False, "R-a_detail": "",
        "R-b": False, "R-b_detail": "",
        "R-c": False, "R-c_detail": "",
        "R-d": False, "R-d_detail": "",
        "worst": "none",
    }

    # ---- R-b 硬底线：不依赖基线，任何时候都判 ----
    if not cur.get("healer_fetch_ok"):
        drift["R-b"] = True
        drift["R-b_detail"] = "Healer 上游不可用或形态异常（拉取失败/无 #EXTM3U/0 直链）"
    elif not cur.get("healer_count"):
        drift["R-b"] = True
        drift["R-b_detail"] = "Healer 直链计数为 0"

    # ---- R-a/R-c/R-d：相对基线，warmup 期不判 ----
    if not warmup:
        hc = cur.get("healer_count")
        if (isinstance(hc, (int, float)) and med_healer
                and med_healer > 0):
            drop = (med_healer - hc) / med_healer
            if drop > R_A_HEALER_DROP:
                drift["R-a"] = True
                drift["R-a_detail"] = (
                    f"Healer 计数 {hc} 较 7 天中位数 {med_healer:.0f} 下降 "
                    f"{drop * 100:.0f}%（>{R_A_HEALER_DROP * 100:.0f}%）")

        ec = cur.get("epg_count")
        if (isinstance(ec, (int, float)) and med_epg
                and med_epg > 0):
            dev = abs(ec - med_epg) / med_epg
            if dev > R_C_EPG_DEVIATION:
                drift["R-c"] = True
                drift["R-c_detail"] = (
                    f"官方 EPG 计数 {ec} 偏离中位数 {med_epg:.0f} "
                    f"{dev * 100:.0f}%（>{R_C_EPG_DEVIATION * 100:.0f}%）")

        jc = cur.get("jaccard")
        if (isinstance(jc, (int, float)) and med_jac and med_jac > 0):
            jdrop = (med_jac - jc) / med_jac
            if jdrop > R_D_JACCARD_DROP:
                drift["R-d"] = True
                drift["R-d_detail"] = (
                    f"Jaccard {jc} 较中位数 {med_jac} 相对下降 "
                    f"{jdrop * 100:.0f}%（>{R_D_JACCARD_DROP * 100:.0f}%）")

    # worst 用于让上层一眼看出严重度
    if drift["R-b"]:
        drift["worst"] = "critical"
    elif drift["R-a"]:
        drift["worst"] = "warning"
    elif drift["R-c"] or drift["R-d"]:
        drift["worst"] = "info"

    return baseline, drift


# ================================================================ 历史读写
def load_history(history_dir: Optional[str]) -> List[Dict[str, Any]]:
    """
    读 history 目录下所有 diff_YYYYMMDD.json，按日期升序返回。

    容错：单个文件坏了只跳过它，不影响整体（一份坏历史不该让整个对比停摆）。

    Args:
        history_dir: 历史目录；None 或不存在则返回空列表。

    Returns:
        历史报告列表（按文件名排序 = 按日期排序）。
    """
    if not history_dir or not os.path.isdir(history_dir):
        return []
    out: List[Dict[str, Any]] = []
    try:
        names = sorted(fn for fn in os.listdir(history_dir)
                       if fn.startswith("diff_") and fn.endswith(".json"))
    except OSError:
        return []
    for fn in names:
        full = os.path.join(history_dir, fn)
        try:
            with open(full, encoding="utf-8") as f:
                obj = json.load(f)
            if isinstance(obj, dict):
                out.append(obj)
        except (OSError, ValueError):
            continue
    return out


# ================================================================ CLI
def _fetch_healer(url: str, timeout: float = 10.0) -> Tuple[Optional[str], str]:
    """
    拉取 Healer m3u 文本。

    有意不走 gh-proxy 三级回退 —— 对比场景下"拉不到"本身就是 R-b 告警信号，
    静默换通道反而会掩盖"这个通道死了"的事实。通道选择留给 workflow 传参。

    Args:
        url: m3u 地址。
        timeout: 超时秒数。

    Returns:
        (text_or_None, note)。
    """
    try:
        import urllib.request
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (compatible; iptv-monitor/1.0)"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
        return raw.decode("utf-8", "replace"), f"OK {len(raw)}B"
    except Exception as e:  # noqa: BLE001 - 网络层任何异常都算拉取失败
        return None, f"{type(e).__name__}: {e}"


def summarize_file(path: str) -> int:
    """
    打印一份已生成的 diff 报告的摘要（供 CI 日志用）。

    Args:
        path: diff 报告 JSON 路径。

    Returns:
        退出码：0 正常；3 报告缺失或 R-b 触发（让 CI 日志标红但不阻断）。
    """
    if not path or not os.path.isfile(path):
        print(f"[epg] 报告不存在：{path}")
        return 3
    try:
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
    except (OSError, ValueError) as e:
        print(f"[epg] 报告读取失败：{e}")
        return 3

    print(f"[epg] EPG={d.get('epg_count')} Healer={d.get('healer_count')} "
          f"交集={d.get('intersect')} Jaccard={d.get('jaccard')} "
          f"占位符={d.get('placeholder_count')}")
    b = d.get("baseline") or {}
    dr = d.get("drift") or {}
    print(f"[epg] baseline warmup={b.get('warmup')} samples={b.get('samples')} "
          f"medians={b.get('medians')}")
    print(f"[epg] drift worst={dr.get('worst')} "
          f"(R-a={dr.get('R-a')} R-b={dr.get('R-b')} "
          f"R-c={dr.get('R-c')} R-d={dr.get('R-d')})")
    for k in ("R-a", "R-b", "R-c", "R-d"):
        if dr.get(k) and dr.get(f"{k}_detail"):
            print(f"[epg]   {k}: {dr[f'{k}_detail']}")
    return 3 if dr.get("R-b") else 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        description="EPG 对比（官方 vs Healer gx.m3u）—— 只产 masked 报告")
    ap.add_argument("--epg-raw", default=None,
                    help="gx_epg_fetch.py --raw-urls 的产物路径"
                         "（含直链，仅内存使用，不入库/不上传）")
    ap.add_argument("--healer-url", default=None, help="Healer m3u 地址")
    ap.add_argument("--healer-file", default=None, help="本地 m3u 文件（替代 --healer-url）")
    ap.add_argument("--history-dir", default=None, help="历史 diff 目录（算 baseline 用）")
    ap.add_argument("--out", default=None, help="输出 masked 报告路径")
    ap.add_argument("--summary-of", default=None, metavar="PATH",
                    help="打印已有报告的摘要（供 CI 日志用），不重新取数")
    ap.add_argument("--channel-names-top", type=int, default=0,
                    help="保留频道名 Top-N（默认 0 = 不保留）")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args(argv)

    # 摘要模式：只读已生成的报告，不触网
    if args.summary_of:
        return summarize_file(args.summary_of)

    # --- 读 EPG 原始结果 ---
    epg_result: Dict[str, Any] = {}
    if args.epg_raw and os.path.isfile(args.epg_raw):
        try:
            with open(args.epg_raw, encoding="utf-8") as f:
                epg_result = json.load(f) or {}
        except (OSError, ValueError) as e:
            print(f"[!] EPG 原始结果读取失败：{e}", file=sys.stderr)

    # --- 拉 Healer ---
    healer_text: Optional[str] = None
    fetch_note = ""
    if args.healer_file and os.path.isfile(args.healer_file):
        try:
            with open(args.healer_file, encoding="utf-8") as f:
                healer_text = f.read()
            fetch_note = "file"
        except OSError as e:
            fetch_note = f"file-read-fail: {e}"
    elif args.healer_url:
        healer_text, fetch_note = _fetch_healer(args.healer_url)
    else:
        fetch_note = "未提供 Healer 来源"

    history = load_history(args.history_dir)
    report = build_epg_diff(epg_result, healer_text,
                            healer_fetch_note=fetch_note,
                            history=history,
                            channel_names_top=args.channel_names_top)

    text = json.dumps(report, ensure_ascii=False, indent=2)
    if args.out:
        os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(text + "\n")
        print(f"[*] masked 对比报告已写入 {args.out}")
    else:
        print(text)

    if args.verbose:
        d = report["drift"]
        b = report["baseline"]
        print(f"[*] EPG {report['epg_count']} / Healer {report['healer_count']} "
              f"/ 交集 {report['intersect']} / Jaccard {report['jaccard']}",
              file=sys.stderr)
        print(f"[*] baseline warmup={b['warmup']} samples={b['samples']} "
              f"medians={b['medians']}", file=sys.stderr)
        print(f"[*] drift worst={d['worst']} "
              f"(R-a={d['R-a']} R-b={d['R-b']} R-c={d['R-c']} R-d={d['R-d']})",
              file=sys.stderr)

    # 退出码：R-b 为 critical（值得让 CI 标红），其余为 0
    return 3 if report["drift"]["R-b"] else 0


if __name__ == "__main__":
    sys.exit(main())
