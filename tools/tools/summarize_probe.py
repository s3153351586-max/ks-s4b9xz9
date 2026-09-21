#!/usr/bin/env python3
"""
体检结果摘要打印（R14 workflow 用）。

【为什么单独成文件而不内嵌在 YAML 里】
  YAML 的 run: | 块要求内容整体缩进一致。内嵌多行 Python 一旦顶格写，就会
  破坏 YAML 结构（实测报 "could not find expected ':'"）。拆成独立脚本既
  避免这个坑，也便于本地单独运行排查。

用法：
    python tools/summarize_probe.py report.json
"""
from __future__ import annotations

import json
import os
import sys


def main() -> int:
    path = sys.argv[1] if len(sys.argv) > 1 else "report.json"

    if not os.path.isfile(path):
        # 用 Actions 注解语法产生一条 warning，便于在 UI 上直接看到
        print(f"::warning::{path} 未生成，发布步骤将失败")
        return 1

    size = os.path.getsize(path)
    print(f"{path} 已生成（{size} 字节）")

    try:
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
    except Exception as e:                      # noqa: BLE001
        print(f"::warning::解析 {path} 失败: {e}")
        return 1

    s = d.get("summary") or {}
    loc = d.get("probe_location") or "?"
    print(f"  探测位置      : {loc}")
    print(f"  公网列表源可用: {s.get('ok_list')}/{s.get('total_list')}")
    print(f"  公网流源可用  : {s.get('ok_stream')}/{s.get('total_stream')}")

    # 顺手把每行写进 Actions 的 job summary，跑完能在页面上直接看
    step_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if step_summary:
        with open(step_summary, "a", encoding="utf-8") as f:
            f.write("## 体检结果\n\n")
            f.write(f"- 探测位置：`{loc}`\n")
            f.write(f"- 公网列表源可用：**{s.get('ok_list')}/{s.get('total_list')}**\n")
            f.write(f"- 公网流源可用：**{s.get('ok_stream')}/{s.get('total_stream')}**\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
