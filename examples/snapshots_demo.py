#!/usr/bin/env python3
"""操作后状态快照回看示例（仅依赖标准库）。

用法：
    python3 examples/snapshots_demo.py [base_url]

默认 base_url 为 http://localhost:8000。

调度员需要按操作顺序回看供水状态。本脚本演示：
  1. 整网导入（版本 1）-> 同事务生成快照 1（operation_type=import）
  2. 提交隔离（关 P4，版本 2）-> 快照 2，关联隔离事件 1
  3. 水源切换（演示网无第二水源，改用隔离/复供全流程）
  4. 复供确认（重开 P4，版本 3）-> 快照 3，关联事件 1
  5. 按快照编号回看当时完整状态（节点、水源运行、阀位、待修复故障、可达用水点）
  6. 相邻快照净变化对照（节点/管段/阀位/水源状态/待修复故障/供水点）
  7. 整网重新导入：版本重置为 1，但只追加快照 4，不覆盖此前快照
  8. 失败/版本冲突/整批目标已满足 -> 不留新快照；未知编号 -> 明确 404
"""
import json
import os
import sys
import urllib.error
import urllib.request

BASE_URL = sys.argv[1] if len(sys.argv) > 1 else os.environ.get(
    "BASE_URL", "http://localhost:8000"
)
SAMPLE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "sample_network.json"
)
TWO_SOURCE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "sources_network.json"
)


def call(method: str, path: str, payload: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        BASE_URL + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def show(title: str, status: int, body: dict) -> None:
    print(f"\n=== {title} -> HTTP {status} ===")
    print(json.dumps(body, ensure_ascii=False, indent=2))


def main() -> None:
    with open(SAMPLE, encoding="utf-8") as f:
        network = json.load(f)

    # 1. 整网导入：版本 1，冻结快照 1
    status, body = call("POST", "/api/network/import", network)
    show("1. 整网导入（预期版本 1、快照 1）", status, body)
    assert status == 200 and body["version"] == 1, body
    assert body["snapshot_id"] == 1, body
    import_snapshot = body["snapshot_id"]

    # 2. 提交隔离：故障 P6，关 P4 -> 版本 2，事件 1，快照 2
    status, body = call(
        "POST",
        "/api/isolation/submit",
        {"fault_pipe_id": "P6", "close_pipe_ids": ["P4"], "version": 1},
    )
    show("2. 提交隔离（预期版本 2、事件 1、快照 2）", status, body)
    assert status == 200, body
    assert body["event_id"] == 1 and body["snapshot_id"] == 2, body
    isolation_snapshot = body["snapshot_id"]

    # 3a. 版本冲突的提交不留快照
    status, body = call(
        "POST",
        "/api/isolation/submit",
        {"fault_pipe_id": "P5", "close_pipe_ids": ["P2"], "version": 1},
    )
    show("3a. 过期版本提交（预期 409 VERSION_CONFLICT，不留快照）", status, body)
    assert status == 409 and body["error"]["code"] == "VERSION_CONFLICT", body

    # 3b. 复供确认（演示网无叠加故障，重开 P4 安全）-> 版本 3，快照 3
    status, body = call(
        "POST", "/api/isolation/events/1/restore", {"version": 2}
    )
    show("3b. 复供确认（预期版本 3、快照 3，U3 恢复供水）", status, body)
    assert status == 200 and body["version"] == 3, body
    assert body["snapshot_id"] == 3, body
    restore_snapshot = body["snapshot_id"]

    # 4. 水源切换：导入双水源网后停运 S1（版本 2，快照 5）
    with open(TWO_SOURCE, encoding="utf-8") as f:
        two_source = json.load(f)
    status, body = call("POST", "/api/network/import", two_source)
    assert status == 200 and body["snapshot_id"] == 4, body
    status, body = call(
        "POST",
        "/api/sources/switch",
        {"targets": [{"source_id": "S1", "running": False}], "version": 1},
    )
    show("4. 停运水源 S1（预期版本 2、快照 5）", status, body)
    assert status == 200 and body["snapshot_id"] == 5, body
    switch_snapshot = body["snapshot_id"]

    # 4b. 整批目标状态已满足：changed=false，版本不变，snapshot_id=null
    status, body = call(
        "POST",
        "/api/sources/switch",
        {"targets": [{"source_id": "S1", "running": False}], "version": 2},
    )
    show("4b. 停运已停运的 S1（预期 changed=false，无快照）", status, body)
    assert status == 200 and body["changed"] is False, body
    assert body["snapshot_id"] is None and body["version"] == 2, body

    # 5. 按快照编号回看：快照 2 = 隔离提交后的状态
    status, body = call("GET", f"/api/snapshots/{isolation_snapshot}")
    show("5. 回看快照 2（隔离提交后：P4 关闭、事件 1 待修复、U3 失供）", status, body)
    assert status == 200, body
    assert body["operation_type"] == "isolation_submit"
    assert body["version"] == 2 and body["event_id"] == 1
    assert next(p for p in body["pipes"] if p["id"] == "P4")["valve_closed"] is True
    assert body["open_faults"] == [
        {"event_id": 1, "fault_pipe_id": "P6", "closed_pipe_ids": ["P4"]}
    ]
    assert body["supplied_sink_ids"] == ["U1", "U2"]

    # 6. 相邻快照净变化对照
    status, body = call("GET", f"/api/snapshots/{isolation_snapshot}/diff")
    show("6. 快照 1 -> 2 净变化（P4 关闭、故障新增、U3 失供）", status, body)
    assert status == 200, body
    assert body["valve_positions"]["closed"] == ["P4"]
    assert body["open_faults"]["added"][0]["event_id"] == 1
    assert body["supplied_sinks"]["newly_lost"] == ["U3"]

    status, body = call("GET", f"/api/snapshots/{restore_snapshot}/diff")
    show("6b. 快照 2 -> 3 净变化（P4 重开、故障解除、U3 恢复）", status, body)
    assert body["valve_positions"]["reopened"] == ["P4"]
    assert body["open_faults"]["resolved"][0]["event_id"] == 1
    assert body["supplied_sinks"]["newly_supplied"] == ["U3"]

    # 6c. 快照 4（双水源导入）-> 5（停运 S1）：水源状态净变化
    status, body = call("GET", f"/api/snapshots/{switch_snapshot}/diff")
    show("6c. 快照 4 -> 5 净变化（S1 运行 true -> false）", status, body)
    assert body["source_states"] == [
        {"id": "S1", "running_before": True, "running_after": False}
    ], body

    # 7. 整网重新导入：版本重置为 1，旧快照不被覆盖
    status, body = call("POST", "/api/network/import", network)
    show("7. 再次整网导入（版本重置 1，追加快照 6，不覆盖旧快照）", status, body)
    assert body["version"] == 1 and body["snapshot_id"] == 6, body
    status, old = call("GET", f"/api/snapshots/{isolation_snapshot}")
    assert status == 200 and old["version"] == 2 and old["event_id"] == 1, old
    print("\n[旧快照 2 内容仍为当时版本 2 的状态，未被当前管网回填]")

    # 8. 快照列表（按操作顺序）与未知编号报错
    status, body = call("GET", "/api/snapshots?limit=100")
    ids = [(i["id"], i["operation_type"], i["version"]) for i in body]
    print(f"\n8. 快照列表（按操作顺序）：{ids}")
    assert ids == [
        (1, "import", 1),
        (2, "isolation_submit", 2),
        (3, "restore", 3),
        (4, "import", 1),
        (5, "source_switch", 2),
        (6, "import", 1),
    ], ids

    status, body = call("GET", "/api/snapshots/999")
    show("8b. 读取未知快照编号（预期 404 SNAPSHOT_NOT_FOUND）", status, body)
    assert status == 404 and body["error"]["code"] == "SNAPSHOT_NOT_FOUND", body

    status, body = call("GET", f"/api/snapshots/{import_snapshot}/diff")
    show("8c. 首条快照无相邻基线（预期 409 SNAPSHOT_NO_BASELINE）", status, body)
    assert status == 409 and body["error"]["code"] == "SNAPSHOT_NO_BASELINE", body

    print("\n全部状态快照示例执行完成。")


if __name__ == "__main__":
    main()
