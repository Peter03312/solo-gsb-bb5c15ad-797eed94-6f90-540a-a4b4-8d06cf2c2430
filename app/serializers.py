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


def serialize_state_snapshot(snapshot: dict) -> dict:
    """冻结载荷 -> GET /api/snapshots/{id} 响应（结构与读网接口一致）。"""
    return {
        "id": snapshot["id"],
        "operation_type": snapshot["operation_type"],
        "version": snapshot["version"],
        "event_id": snapshot["event_id"],
        "created_at": snapshot["created_at"],
        "nodes": [
            {"id": n["id"], "type": n["type"], "name": n.get("name"),
             "running": n.get("running", True)}
            for n in snapshot["nodes"]
        ],
        "pipes": [
            {
                "id": p["id"],
                "from_": p["from"],
                "to_": p["to"],
                "has_valve": p["has_valve"],
                "valve_closed": p.get("valve_closed"),
                "name": p.get("name"),
            }
            for p in snapshot["pipes"]
        ],
        "open_faults": [
            {
                "event_id": f["event_id"],
                "fault_pipe_id": f["fault_pipe_id"],
                "closed_pipe_ids": list(f["closed_pipe_ids"]),
            }
            for f in snapshot["open_faults"]
        ],
        "supplied_sink_ids": list(snapshot["supplied_sink_ids"]),
    }
