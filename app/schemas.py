"""HTTP 请求 / 响应模型。

节点类型：
- source  水源
- sink    用水点
- junction 连接点

编号（id）为字符串，便于保留 "N01" 这类业务编号；排序时按字符串字典序。
"""
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

NodeType = Literal["source", "sink", "junction"]


class NodeIn(BaseModel):
    id: str = Field(min_length=1, description="节点编号，唯一")
    type: NodeType
    name: str | None = Field(default=None, description="可选名称")


class PipeIn(BaseModel):
    id: str = Field(min_length=1, description="管段编号，唯一")
    # JSON 字段为 from/to（from 是 Python 关键字），Python 属性名为 from_node/to_node
    from_node: str = Field(min_length=1, validation_alias="from")
    to_node: str = Field(min_length=1, validation_alias="to")
    has_valve: bool = Field(default=False, description="该管段是否设有阀门")
    # 阀门初始状态：默认开启（未关闭）。非阀门管段传 true 会在导入时被拒绝。
    valve_closed: bool = False
    name: str | None = None

    model_config = {"populate_by_name": True, "extra": "forbid"}


class ImportIn(BaseModel):
    nodes: list[NodeIn]
    pipes: list[PipeIn]


class PlanIn(BaseModel):
    """故障隔离预演 / 提交入参。"""

    fault_pipe_id: str = Field(min_length=1, description="故障管段编号")
    close_pipe_ids: list[str] = Field(
        default_factory=list, description="拟关闭的阀门管段编号列表"
    )
    version: int = Field(ge=0, description="读取管网时的版本号，空网为 0")


class RecommendIn(BaseModel):
    """关阀建议入参：只知道故障管段编号与已读版本。"""

    fault_pipe_id: str = Field(min_length=1, description="故障管段编号")
    version: int = Field(ge=0, description="读取管网时的版本号，空网为 0")


class SourceTargetIn(BaseModel):
    """单个水源的目标运行状态。"""

    source_id: str = Field(min_length=1, description="水源节点编号")
    running: bool = Field(description="目标运行状态：true 恢复运行，false 停运")

    model_config = {"extra": "forbid"}


class SourceSwitchIn(BaseModel):
    """水源状态切换预演 / 确认入参：一批水源及各自目标运行状态 + 已读版本。"""

    targets: list[SourceTargetIn] = Field(
        min_length=1, description="水源编号与目标运行状态列表（同批编号不得重复）"
    )
    version: int = Field(ge=0, description="读取管网时的版本号，空网为 0")


# ---------------------------------------------------------------- 响应模型


class NodeOut(BaseModel):
    id: str
    type: NodeType
    name: str | None = None
    # 仅水源节点有意义：true=运行中，false=已停运；非水源节点恒为 true
    running: bool = True


class PipeOut(BaseModel):
    id: str
    # 出参 JSON 键名为 from/to；Python 属性使用 from_/to_
    from_: str = Field(serialization_alias="from")
    to_: str = Field(serialization_alias="to")
    has_valve: bool
    valve_closed: bool | None = None
    name: str | None = None


class ImportOut(BaseModel):
    version: int
    node_count: int
    pipe_count: int


class NetworkOut(BaseModel):
    version: int
    updated_at: datetime | None = None
    nodes: list[NodeOut]
    pipes: list[PipeOut]


class IssueOut(BaseModel):
    code: str
    message: str
    pipe_id: str | None = None


class PreviewOut(BaseModel):
    version: int
    version_match: bool
    fault_pipe_id: str
    close_pipe_ids: list[str]
    isolatable: bool
    affected_sink_ids: list[str]
    issues: list[IssueOut]


class RecommendReasonOut(BaseModel):
    code: str
    message: str
    pipe_id: str | None = None


class RecommendOut(BaseModel):
    version: int
    version_match: bool
    fault_pipe_id: str
    feasible: bool
    recommended_close_pipe_ids: list[str]
    affected_sink_ids: list[str]
    reason: RecommendReasonOut | None = None


class SubmitOut(BaseModel):
    version: int
    fault_pipe_id: str
    closed_pipe_ids: list[str]
    affected_sink_ids: list[str]
    event_id: int


class RestoreIn(BaseModel):
    """复供预演 / 确认入参：按隔离事件编号与当前管网版本操作。"""

    version: int = Field(ge=0, description="读取管网时的版本号，空网为 0")


class ReopenBlockOut(BaseModel):
    code: str
    message: str
    pipe_id: str | None = None
    reachable_endpoints: list[str] = []


class RestorePreviewOut(BaseModel):
    version: int
    version_match: bool
    event_id: int
    status: str
    fault_pipe_id: str
    closed_pipe_ids: list[str]
    safe: bool
    restored_sink_ids: list[str]
    block_reasons: list[ReopenBlockOut]


class RestoreOut(BaseModel):
    version: int
    event_id: int
    status: str
    fault_pipe_id: str
    reopened_pipe_ids: list[str]
    restored_sink_ids: list[str]


class SourceTargetOut(BaseModel):
    source_id: str
    running: bool


class SourceSwitchPreviewOut(BaseModel):
    version: int
    version_match: bool
    changed: bool
    safe: bool
    targets: list[SourceTargetOut]
    affected_sink_ids: list[str]
    restored_sink_ids: list[str]
    block_reasons: list[ReopenBlockOut]
    issues: list[IssueOut]


class SourceSwitchOut(BaseModel):
    version: int
    changed: bool
    targets: list[SourceTargetOut]
    affected_sink_ids: list[str]
    restored_sink_ids: list[str]


class EventOut(BaseModel):
    id: int
    version_before: int
    version_after: int
    fault_pipe_id: str
    closed_pipe_ids: list[str]
    affected_sink_ids: list[str]
    status: str = "open"
    restored_at: datetime | None = None
    created_at: datetime
