"""领域对象 -> 响应 dict 的转换。"""
from .planning import (
    Node,
    Pipe,
    PlanResult,
    RecommendResult,
    RestoreResult,
    SourceSwitchResult,
)

def serialize_node(node: Node) -> dict:
    return {"id": node.id, "type": node.type, "name": node.name, "running": node.running}


def serialize_pipe(pipe: Pipe) -> dict:
    return {
        "id": pipe.id,
        "from_": pipe.from_node,
        "to_": pipe.to_node,
        "has_valve": pipe.has_valve,
        "valve_closed": pipe.valve_closed if pipe.has_valve else None,
        "name": pipe.name,
    }


def serialize_snapshot(version: int, updated_at, nodes, pipes) -> dict:
    return {
        "version": version,
        "updated_at": updated_at,
        "nodes": [serialize_node(nodes[nid]) for nid in sorted(nodes)],
        "pipes": [serialize_pipe(pipes[pid]) for pid in sorted(pipes)],
    }


def serialize_preview(
    version: int,
    expected_version: int,
    fault_pipe_id: str,
    close_pipe_ids: list[str],
    result: PlanResult,
) -> dict:
    return {
        "version": version,
        "version_match": version == expected_version,
        "fault_pipe_id": fault_pipe_id,
        "close_pipe_ids": list(close_pipe_ids),
        "isolatable": result.isolatable,
        "affected_sink_ids": result.affected_sink_ids,
        "issues": [i.as_dict() for i in result.issues],
    }


def serialize_restore_preview(
    version: int,
    expected_version: int,
    event: dict,
    result: RestoreResult,
) -> dict:
    return {
        "version": version,
        "version_match": version == expected_version,
        "event_id": event["id"],
        "status": event["status"],
        "fault_pipe_id": event["fault_pipe_id"],
        "closed_pipe_ids": list(event["closed_pipe_ids"]),
        "safe": result.safe,
        "restored_sink_ids": result.restored_sink_ids,
        "block_reasons": [b.as_dict() for b in result.blocks],
    }


def serialize_recommend(
    version: int,
    expected_version: int,
    fault_pipe_id: str,
    result: RecommendResult,
) -> dict:
    return {
        "version": version,
        "version_match": version == expected_version,
        "fault_pipe_id": fault_pipe_id,
        "feasible": result.feasible,
        "recommended_close_pipe_ids": list(result.recommended_close_pipe_ids),
        "affected_sink_ids": list(result.affected_sink_ids),
        "reason": result.reason.as_dict() if result.reason else None,
    }


def serialize_source_switch_preview(
    version: int,
    expected_version: int,
    result: SourceSwitchResult,
) -> dict:
    return {
        "version": version,
        "version_match": version == expected_version,
        "changed": result.changed,
        "safe": result.safe,
        "targets": [
            {"source_id": source_id, "running": running}
            for source_id, running in result.targets
        ],
        "affected_sink_ids": list(result.affected_sink_ids),
        "restored_sink_ids": list(result.restored_sink_ids),
        "block_reasons": [b.as_dict() for b in result.blocks],
        "issues": [i.as_dict() for i in result.issues],
    }


# ---------------------------------------------------------------- 状态快照


def serialize_frozen_state(state: dict) -> dict:
    """冻结载荷 -> 响应结构（管段键名 from/to；与 GET /api/network 口径一致）。"""
    nodes = [
        {
            "id": n["id"],
            "type": n["type"],
            "name": n.get("name"),
            "running": n["running"],
        }
        for n in state.get("nodes", [])
    ]
    pipes = [
        {
            "id": p["id"],
            "from_": p["from"],
            "to_": p["to"],
            "has_valve": p["has_valve"],
            "valve_closed": p["valve_closed"],
            "name": p.get("name"),
        }
        for p in state.get("pipes", [])
    ]
    return {
        "nodes": nodes,
        "pipes": pipes,
        "open_faults": [
            {"event_id": f["event_id"], "fault_pipe_id": f["fault_pipe_id"]}
            for f in state.get("open_faults", [])
        ],
        "supplied_sink_ids": list(state.get("supplied_sink_ids", [])),
    }


def serialize_snapshot_row(row: dict) -> dict:
    return {
        "id": row["id"],
        "op_type": row["op_type"],
        "network_version": row["network_version"],
        "event_id": row["event_id"],
        "state": serialize_frozen_state(row["state"]),
        "created_at": row["created_at"],
    }


def serialize_snapshot_diff(
    base_row: dict | None, target_row: dict, diff: dict
) -> dict:
    def _pipe(p: dict) -> dict:
        return {
            "id": p["id"],
            "from_": p["from"],
            "to_": p["to"],
            "has_valve": p["has_valve"],
            "valve_closed": p["valve_closed"],
            "name": p.get("name"),
        }

    def _node(n: dict) -> dict:
        return {
            "id": n["id"],
            "type": n["type"],
            "name": n.get("name"),
            "running": n["running"],
        }

    return {
        "base_snapshot_id": base_row["id"] if base_row else None,
        "target_snapshot_id": target_row["id"],
        "base_network_version": (
            base_row["network_version"] if base_row else None
        ),
        "target_network_version": target_row["network_version"],
        "nodes": {
            "added": [_node(n) for n in diff["nodes"]["added"]],
            "removed": [_node(n) for n in diff["nodes"]["removed"]],
        },
        "pipes": {
            "added": [_pipe(p) for p in diff["pipes"]["added"]],
            "removed": [_pipe(p) for p in diff["pipes"]["removed"]],
        },
        "valve_positions": {"changed": list(diff["valve_positions"]["changed"])},
        "source_states": {"changed": list(diff["source_states"]["changed"])},
        "open_faults": {
            "added": [
                {"event_id": f["event_id"], "fault_pipe_id": f["fault_pipe_id"]}
                for f in diff["open_faults"]["added"]
            ],
            "removed": [
                {"event_id": f["event_id"], "fault_pipe_id": f["fault_pipe_id"]}
                for f in diff["open_faults"]["removed"]
            ],
        },
        "supplied_sinks": {
            "added": list(diff["supplied_sinks"]["added"]),
            "removed": list(diff["supplied_sinks"]["removed"]),
        },
    }
