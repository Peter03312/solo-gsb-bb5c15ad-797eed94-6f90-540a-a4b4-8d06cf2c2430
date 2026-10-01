"""供水管网故障隔离服务 —— FastAPI 入口。

接口前缀 /api：
- GET  /health                                         健康检查
- POST /api/network/import                             整网导入（替换式，版本重置为 1）
- GET  /api/network                                    读取当前管网与版本
- POST /api/isolation/recommend                        关阀建议（只读，按当前管网计算可提交方案）
- POST /api/isolation/preview                          隔离方案预演（只读）
- POST /api/isolation/submit                           隔离方案提交（原子关阀 + 版本 +1）
- GET  /api/isolation/events                           隔离/复供操作历史
- POST /api/isolation/events/{event_id}/restore/preview 复供预演（只读）
- POST /api/isolation/events/{event_id}/restore        复供确认（单事务复核 + 重开阀门）
- POST /api/sources/switch/preview                     水源停运/恢复预演（只读）
- POST /api/sources/switch                             水源停运/恢复确认（单事务复核 + 原子改状态）
"""
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from . import db
from .config import get_settings
from .planning import (
    Node,
    Pipe,
    evaluate_plan,
    evaluate_restore,
    evaluate_source_switch,
    recommend_plan,
    validate_import,
    BLOCKING_ISSUE_CODES,
)
from .schemas import (
    ImportIn,
    PlanIn,
    RecommendIn,
    RestoreIn,
    SourceSwitchIn,
    ImportOut,
    NetworkOut,
    PreviewOut,
    RecommendOut,
    SubmitOut,
    RestorePreviewOut,
    RestoreOut,
    SourceSwitchPreviewOut,
    SourceSwitchOut,
    EventOut,
)
from .serializers import (
    serialize_preview,
    serialize_recommend,
    serialize_restore_preview,
    serialize_source_switch_preview,
    serialize_snapshot,
)


class ApiError(Exception):
    def __init__(self, status_code: int, code: str, message: str, issues: list | None = None):
        self.status_code = status_code
        self.code = code
        self.message = message
        self.issues = issues or []
        super().__init__(message)


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    yield


app = FastAPI(
    title="供水管网故障隔离服务",
    version="1.0.0",
    lifespan=lifespan,
)


@app.exception_handler(ApiError)
async def api_error_handler(request, exc: ApiError):
    body: dict = {"error": {"code": exc.code, "message": exc.message}}
    if exc.issues:
        body["error"]["issues"] = [i.as_dict() for i in exc.issues]
    return JSONResponse(status_code=exc.status_code, content=body)


@app.exception_handler(db.VersionConflict)
async def version_conflict_handler(request, exc: db.VersionConflict):
    return JSONResponse(
        status_code=409,
        content={
            "error": {
                "code": "VERSION_CONFLICT",
                "message": f"版本过期：当前版本为 {exc.current}，提交基于版本 {exc.expected}，请重新读取后再试",
                "current_version": exc.current,
                "expected_version": exc.expected,
            }
        },
    )


@app.get("/health", tags=["系统"])
def health():
    return {"status": "ok"}


# ---------------------------------------------------------------- 管网


def _to_domain_nodes(payload: ImportIn) -> list[Node]:
    return [Node(id=n.id, type=n.type, name=n.name) for n in payload.nodes]


def _to_domain_pipes(payload: ImportIn) -> list[Pipe]:
    return [
        Pipe(
            id=p.id,
            from_node=p.from_node,
            to_node=p.to_node,
            has_valve=p.has_valve,
            valve_closed=p.valve_closed,
            name=p.name,
        )
        for p in payload.pipes
    ]


@app.post(
    "/api/network/import",
    response_model=ImportOut,
    tags=["管网"],
    summary="整网导入（替换当前管网，版本重置为 1）",
)
def import_network(payload: ImportIn):
    nodes = _to_domain_nodes(payload)
    pipes = _to_domain_pipes(payload)

    issues = validate_import(nodes, pipes)
    if issues:
        raise ApiError(
            422,
            "IMPORT_INVALID",
            "管网数据校验失败：存在悬空端点、重号、自环或无阀管段被标记关闭",
            issues=issues,
        )

    with db.transaction() as conn:
        version = db.replace_network(conn, nodes, pipes)

    return {
        "version": version,
        "node_count": len(nodes),
        "pipe_count": len(pipes),
    }


@app.get("/api/network", response_model=NetworkOut, tags=["管网"], summary="读取当前管网与版本")
def get_network():
    with db.get_conn() as conn:
        version, updated_at, nodes, pipes = db.load_snapshot(conn)
    return serialize_snapshot(version, updated_at, nodes, pipes)


# ---------------------------------------------------------------- 隔离


@app.post(
    "/api/isolation/recommend",
    response_model=RecommendOut,
    tags=["故障隔离"],
    summary="关阀建议：只知道故障管段时，按当前管网计算一套可提交的关阀方案（只读）",
)
def recommend_isolation(payload: RecommendIn):
    with db.get_conn() as conn:
        version, _updated_at, nodes, pipes = db.load_snapshot(conn)
        pending_fault_ids = frozenset(
            e["fault_pipe_id"] for e in db.list_open_events(conn)
        )

    result = recommend_plan(
        nodes,
        pipes,
        payload.fault_pipe_id,
        pending_fault_pipe_ids=pending_fault_ids,
    )
    return serialize_recommend(version, payload.version, payload.fault_pipe_id, result)


@app.post(
    "/api/isolation/preview",
    response_model=PreviewOut,
    tags=["故障隔离"],
    summary="预演：判断能否隔离并计算失供用水点（只读，不改状态）",
)
def preview_isolation(payload: PlanIn):
    with db.get_conn() as conn:
        version, _updated_at, nodes, pipes = db.load_snapshot(conn)
        pending_fault_ids = frozenset(
            e["fault_pipe_id"] for e in db.list_open_events(conn)
        )

    result = evaluate_plan(
        nodes,
        pipes,
        payload.fault_pipe_id,
        list(payload.close_pipe_ids),
        for_submit=False,
        pending_fault_pipe_ids=pending_fault_ids,
    )
    return serialize_preview(
        version, payload.version, payload.fault_pipe_id, payload.close_pipe_ids, result
    )


@app.post(
    "/api/isolation/submit",
    response_model=SubmitOut,
    tags=["故障隔离"],
    summary="提交隔离：基于当前版本重新校验，原子更新阀位与版本",
)
def submit_isolation(payload: PlanIn):
    close_ids = list(dict.fromkeys(payload.close_pipe_ids))  # 去重但保序（重复仍会被拒）

    with db.transaction() as conn:
        # 锁定当前版本后基于最新数据重新校验
        version, _updated_at, nodes, pipes = db.load_snapshot(conn, for_update=True)

        if version != payload.version:
            raise db.VersionConflict(version, payload.version)

        # 其他待修复事件的故障管段仍阻断通水
        pending_fault_ids = frozenset(
            e["fault_pipe_id"] for e in db.list_open_events(conn)
        )

        result = evaluate_plan(
            nodes,
            pipes,
            payload.fault_pipe_id,
            list(payload.close_pipe_ids),
            for_submit=True,
            pending_fault_pipe_ids=pending_fault_ids,
        )

        blocking = [i for i in result.issues if i.code in BLOCKING_ISSUE_CODES]
        if blocking:
            raise ApiError(
                422,
                "PLAN_INVALID",
                "隔离方案非法（管段不存在、无阀管段、重复关闭或把故障管段当作阀门）",
                issues=blocking,
            )
        if not result.isolatable:
            raise ApiError(
                422,
                "NOT_ISOLATABLE",
                "该方案无法隔离故障管段：故障管段至少有一端在方案图中仍可从水源到达",
                issues=result.issues,
            )

        outcome = db.submit_isolation(
            conn,
            expected_version=payload.version,
            fault_pipe_id=payload.fault_pipe_id,
            close_pipe_ids=close_ids,
            result=result,
        )

    return {
        "version": outcome["version"],
        "fault_pipe_id": payload.fault_pipe_id,
        "closed_pipe_ids": close_ids,
        "affected_sink_ids": result.affected_sink_ids,
        "event_id": outcome["event_id"],
    }


@app.get(
    "/api/isolation/events",
    response_model=list[EventOut],
    tags=["故障隔离"],
    summary="隔离/复供操作历史（按时间倒序，含待修复与已复供状态）",
)
def get_events():
    settings = get_settings()
    with db.get_conn() as conn:
        return db.list_events(conn, settings.history_limit)


# ---------------------------------------------------------------- 复供


def _load_restore_context(conn, event_id: int, *, for_update: bool = False):
    """读取事件、快照与其他待修复事件。调用方负责事件存在性/状态/版本判定。"""
    event = db.get_event(conn, event_id, for_update=for_update)
    if event is None:
        raise ApiError(404, "EVENT_NOT_FOUND", f"隔离事件不存在：{event_id}")
    if event["status"] != db.EVENT_OPEN:
        raise ApiError(
            409,
            "EVENT_ALREADY_RESTORED",
            f"隔离事件 {event_id} 已复供，不能重复操作",
        )
    version, _updated_at, nodes, pipes = db.load_snapshot(
        conn, for_update=for_update
    )
    other_open = db.list_open_events(conn, exclude_event_id=event_id,
                                     for_update=for_update)
    other_fault_ids = frozenset(e["fault_pipe_id"] for e in other_open)
    result = evaluate_restore(
        nodes,
        pipes,
        reopen_pipe_ids=frozenset(event["closed_pipe_ids"]),
        other_fault_pipe_ids=other_fault_ids,
    )
    return version, event, result


@app.post(
    "/api/isolation/events/{event_id}/restore/preview",
    response_model=RestorePreviewOut,
    tags=["故障复供"],
    summary="复供预演：重开该事件阀门后的安全性与预计恢复用水点（只读）",
)
def restore_preview(event_id: int, payload: RestoreIn):
    with db.get_conn() as conn:
        version, event, result = _load_restore_context(conn, event_id)

    return serialize_restore_preview(version, payload.version, event, result)


@app.post(
    "/api/isolation/events/{event_id}/restore",
    response_model=RestoreOut,
    tags=["故障复供"],
    summary="复供确认：同一事务内复核版本与安全性，原子重开阀门并更新事件状态",
)
def restore_confirm(event_id: int, payload: RestoreIn):
    with db.transaction() as conn:
        # 锁定事件行/版本行后基于最新数据复核，保证不产生部分变更
        version, event, result = _load_restore_context(conn, event_id, for_update=True)

        if version != payload.version:
            raise db.VersionConflict(version, payload.version)
        if not result.safe:
            raise ApiError(
                422,
                "RESTORE_UNSAFE",
                "复供存在重新进水风险：其他待修复故障管段的端点将重新通水",
                issues=result.blocks,
            )

        reopen_ids = list(event["closed_pipe_ids"])
        outcome = db.confirm_restore(
            conn,
            event_id=event_id,
            expected_version=payload.version,
            reopened_pipe_ids=reopen_ids,
            restored_sink_ids=result.restored_sink_ids,
        )

    return {
        "version": outcome["version"],
        "event_id": event_id,
        "status": db.EVENT_RESTORED,
        "fault_pipe_id": event["fault_pipe_id"],
        "reopened_pipe_ids": reopen_ids,
        "restored_sink_ids": result.restored_sink_ids,
    }


# ---------------------------------------------------------------- 水源状态切换


def _load_source_switch_context(conn, payload: SourceSwitchIn, *, for_update: bool = False):
    """读取快照、待修复故障并评估水源切换。返回 (version, result, targets)。"""
    version, _updated_at, nodes, pipes = db.load_snapshot(conn, for_update=for_update)
    pending_fault_ids = frozenset(
        e["fault_pipe_id"] for e in db.list_open_events(conn)
    )
    targets = [(t.source_id, t.running) for t in payload.targets]
    result = evaluate_source_switch(
        nodes,
        pipes,
        targets,
        pending_fault_pipe_ids=pending_fault_ids,
    )
    return version, result, targets


def _switch_response_body(version: int, targets, result) -> dict:
    return {
        "version": version,
        "changed": result.changed,
        "targets": [
            {"source_id": source_id, "running": running}
            for source_id, running in targets
        ],
        "affected_sink_ids": result.affected_sink_ids,
        "restored_sink_ids": result.restored_sink_ids,
    }


@app.post(
    "/api/sources/switch/preview",
    response_model=SourceSwitchPreviewOut,
    tags=["水源调度"],
    summary="水源停运/恢复预演：失供与恢复用水点、待修复故障重新进水风险（只读）",
)
def source_switch_preview(payload: SourceSwitchIn):
    with db.get_conn() as conn:
        version, result, targets = _load_source_switch_context(conn, payload)

    return serialize_source_switch_preview(version, payload.version, result)


@app.post(
    "/api/sources/switch",
    response_model=SourceSwitchOut,
    tags=["水源调度"],
    summary="水源停运/恢复确认：锁版本复核安全性，原子更新水源状态与版本",
)
def source_switch_confirm(payload: SourceSwitchIn):
    with db.transaction() as conn:
        # 锁定版本行后基于最新状态复核，保证不产生部分变更
        version, result, targets = _load_source_switch_context(
            conn, payload, for_update=True
        )

        if version != payload.version:
            raise db.VersionConflict(version, payload.version)
        if result.issues:
            raise ApiError(
                422,
                "SOURCE_SWITCH_INVALID",
                "水源切换请求非法（编号不存在、节点不是水源或同批编号重复）",
                issues=result.issues,
            )

        # 全部目标状态已满足：不推进版本，直接回显当前状态
        if not result.changed:
            return _switch_response_body(version, targets, result)

        if not result.safe:
            raise ApiError(
                422,
                "SOURCE_SWITCH_UNSAFE",
                "水源切换存在重新进水风险：恢复水源后待修复故障管段的端点将重新通水",
                issues=result.blocks,
            )

        outcome = db.switch_sources(
            conn, expected_version=payload.version, targets=targets
        )

    return _switch_response_body(outcome["version"], targets, result)
