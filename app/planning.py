"""管网故障隔离与复供的纯领域逻辑（不依赖数据库 / Web 框架）。

规则概述：
- 管网为无向图，节点分为水源(source)/用水点(sink)/连接点(junction)。
- 已关闭阀门的管段视为不可通行。
- 其他“待修复(open)”隔离事件的故障管段同样视为不可通行（故障仍在，不能通水）。
- 方案图 = 当前管网移除故障管段，再移除/关闭所有“拟关闭管段”，
  同时已关闭阀门、其他待修复故障管段继续保持阻断。
- 只有故障管段**两端**在方案图中都无法从任一水源到达，方案才可隔离(isolatable)。
- 失供用水点 = 方案前由任一水源可达、方案后不可达的 sink，按编号排序。

复供（restore）规则：
- 复供图 = 当前管网重新打开本事件关闭的阀门（从“仍关闭阀门”集合中剔除），
  再继续移除“其他待修复事件”的故障管段。
- 仅当重开后，其余每条待修复故障管段的**两端**都仍无法从水源到达，才允许复供。
- 预计恢复供水用水点 = 复供前不可达、复供图中可达的 sink，按编号排序。

关阀建议（recommend）规则：
- 只读计算：为故障管段给出一套可直接提交的关阀方案（拟关阀门 + 预计失供用水点）。
- 只能从尚未关闭的有阀管段中选取（不含故障管段本身），方案须使故障管段两端
  都与所有水源断开；优选顺序：新增失供用水点数最少 → 本次关阀数最少 →
  关阀编号序列字典序最小。

水源状态（source running）规则：
- 每个水源节点有运行(running)/停运(stopped)状态；整网导入时水源初始视为运行。
- 可达性计算只从“运行中”的水源出发（停运水源不再向管网供水）；
  读网、隔离预演/提交、关阀建议、复供预演/确认全部按运行水源计算。
- 水源切换预演为只读：返回当前版本、版本是否匹配、切换后新增失供与恢复供水的
  用水点，以及每条待修复故障管段的重新进水风险；仅水源节点可切换，同批编号
  不得重复；全部目标状态已满足（无实际变化）时不推进版本；停运可导致全网无
  运行水源（此时所有用水点失供，不额外报错）。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from collections import deque
from itertools import combinations

# 问题代码 ---------------------------------------------------------------

# 阻断类问题：出现任一即不能隔离、提交时拒绝（422）
FAULT_PIPE_NOT_FOUND = "FAULT_PIPE_NOT_FOUND"
CLOSE_PIPE_NOT_FOUND = "CLOSE_PIPE_NOT_FOUND"
CLOSE_PIPE_NO_VALVE = "CLOSE_PIPE_NO_VALVE"
CLOSE_PIPE_DUPLICATE = "CLOSE_PIPE_DUPLICATE"
CLOSE_PIPE_IS_FAULT = "CLOSE_PIPE_IS_FAULT"
# 复供阻断：重开阀门后其他待修复故障管段有一端重新进水
FAULT_REWATER_RISK = "FAULT_REWATER_RISK"
# 关阀建议：关闭全部可用阀门后故障管段仍有端点与水源相通
NO_FEASIBLE_PLAN = "NO_FEASIBLE_PLAN"
# 导入期问题
DUPLICATE_NODE_ID = "DUPLICATE_NODE_ID"
DUPLICATE_PIPE_ID = "DUPLICATE_PIPE_ID"
PIPE_SELF_LOOP = "PIPE_SELF_LOOP"
PIPE_ENDPOINT_UNKNOWN = "PIPE_ENDPOINT_UNKNOWN"
NON_VALVE_PIPE_CLOSED = "NON_VALVE_PIPE_CLOSED"

# 水源切换问题
SOURCE_NOT_FOUND = "SOURCE_NOT_FOUND"          # 编号不存在
SOURCE_NOT_SWITCHABLE = "SOURCE_NOT_SWITCHABLE"  # 节点存在但不是水源，不可切换
SOURCE_TARGET_DUPLICATE = "SOURCE_TARGET_DUPLICATE"  # 同批水源编号重复

# 结构性阻断问题（预演会给出，提交据此拒绝）
BLOCKING_ISSUE_CODES = frozenset(
    {
        FAULT_PIPE_NOT_FOUND,
        CLOSE_PIPE_NOT_FOUND,
        CLOSE_PIPE_NO_VALVE,
        CLOSE_PIPE_DUPLICATE,
        CLOSE_PIPE_IS_FAULT,
    }
)


# 快照操作类型
OP_NETWORK_IMPORT = "network_import"    # 整网导入
OP_ISOLATION_SUBMIT = "isolation_submit"  # 隔离提交
OP_RESTORE_CONFIRM = "restore_confirm"  # 复供确认
OP_SOURCE_SWITCH = "source_switch"      # 水源切换
OP_BASELINE = "baseline"                # 旧库上线基线（仅当下状态，不伪造历史）

SNAPSHOT_OP_TYPES = frozenset(
    {
        OP_NETWORK_IMPORT,
        OP_ISOLATION_SUBMIT,
        OP_RESTORE_CONFIRM,
        OP_SOURCE_SWITCH,
        OP_BASELINE,
    }
)


@dataclass(frozen=True)
class Node:
    id: str
    type: str  # source / sink / junction
    name: str | None = None
    # 仅水源节点有意义：True=运行（供水），False=停运。导入的水源初始为运行。
    running: bool = True


@dataclass(frozen=True)
class Pipe:
    id: str
    from_node: str
    to_node: str
    has_valve: bool = False
    valve_closed: bool = False
    name: str | None = None


@dataclass
class Issue:
    code: str
    message: str
    pipe_id: str | None = None

    def as_dict(self) -> dict:
        return {"code": self.code, "message": self.message, "pipe_id": self.pipe_id}


@dataclass
class PlanResult:
    isolatable: bool
    affected_sink_ids: list[str]
    issues: list[Issue] = field(default_factory=list)
    # 方案中实际被阻断的管段（已关 ∪ 拟关 ∪ 待修复故障，故障管段单独移除）
    blocked_pipe_ids: frozenset[str] = field(default_factory=frozenset)


@dataclass
class ReopenBlock:
    """复供阻断原因：某条其他待修复故障管段在重开阀门后有一端重新进水。"""

    fault_pipe_id: str
    reachable_endpoints: list[str]

    def as_dict(self) -> dict:
        return {
            "code": FAULT_REWATER_RISK,
            "message": (
                f"重开阀门后，待修复故障管段 {self.fault_pipe_id} 的 "
                f"{ '、'.join(self.reachable_endpoints) } 端将重新进水，禁止复供"
            ),
            "pipe_id": self.fault_pipe_id,
            "reachable_endpoints": list(self.reachable_endpoints),
        }


@dataclass
class RestoreResult:
    safe: bool
    restored_sink_ids: list[str]
    blocks: list[ReopenBlock] = field(default_factory=list)
    # 复供评估中仍不可通行的管段（其他待修复故障管段 ∪ 仍关闭阀门）
    blocked_pipe_ids: frozenset[str] = field(default_factory=frozenset)


@dataclass
class SourceSwitchBlock:
    """水源切换阻断：切换后某条待修复故障管段有一端将重新进水。"""

    fault_pipe_id: str
    reachable_endpoints: list[str]

    def as_dict(self) -> dict:
        return {
            "code": FAULT_REWATER_RISK,
            "message": (
                f"切换后，待修复故障管段 {self.fault_pipe_id} 的 "
                f"{ '、'.join(self.reachable_endpoints) } 端将重新进水，禁止切换水源状态"
            ),
            "pipe_id": self.fault_pipe_id,
            "reachable_endpoints": list(self.reachable_endpoints),
        }


@dataclass
class SourceSwitchResult:
    """水源状态切换预演结果（只读计算，不改任何状态）。

    - changed：是否存在实际状态变化；全部目标状态已满足时为 False，确认不推进版本；
    - affected_sink_ids：切换后“新增失供”的用水点（切换前可达、切换后不可达）；
    - restored_sink_ids：切换后“恢复供水”的用水点（切换前不可达、切换后可达）；
    - blocks：每条待修复故障管段在切换后的重新进水风险（任一端可达即列入）；
    存在风险时 safe=False，确认接口据此拒绝（422 SOURCE_SWITCH_UNSAFE）。
    """

    changed: bool
    safe: bool
    targets: list[tuple[str, bool]]
    affected_sink_ids: list[str]
    restored_sink_ids: list[str]
    blocks: list[SourceSwitchBlock] = field(default_factory=list)
    issues: list[Issue] = field(default_factory=list)


@dataclass
class RecommendResult:
    """关阀建议结果（只读计算，不改任何状态）。

    feasible=False 时 reason 给出原因：
    - FAULT_PIPE_NOT_FOUND：故障管段不存在；
    - NO_FEASIBLE_PLAN：关闭全部可用阀门后故障管段仍有端点与水源相通。
    """

    feasible: bool
    recommended_close_pipe_ids: list[str]
    affected_sink_ids: list[str]
    reason: Issue | None = None


# ---------------------------------------------------------------- 导入校验


def validate_import(
    nodes: list[Node], pipes: list[Pipe]
) -> list[Issue]:
    """校验整网导入数据：重号、自环、悬空端点、无阀管段初始关闭。"""
    issues: list[Issue] = []

    node_ids: set[str] = set()
    for n in nodes:
        if n.id in node_ids:
            issues.append(Issue(DUPLICATE_NODE_ID, f"节点编号重复：{n.id}", n.id))
        node_ids.add(n.id)

    pipe_ids: set[str] = set()
    for p in pipes:
        if p.id in pipe_ids:
            issues.append(Issue(DUPLICATE_PIPE_ID, f"管段编号重复：{p.id}", p.id))
        pipe_ids.add(p.id)

        if p.from_node == p.to_node:
            issues.append(Issue(PIPE_SELF_LOOP, f"管段自环（两端为同一节点）：{p.id}", p.id))
        for endpoint in (p.from_node, p.to_node):
            if endpoint not in node_ids:
                issues.append(
                    Issue(
                        PIPE_ENDPOINT_UNKNOWN,
                        f"管段 {p.id} 的端点 {endpoint} 未在节点列表中定义（悬空端点）",
                        p.id,
                    )
                )
        if p.valve_closed and not p.has_valve:
            issues.append(
                Issue(
                    NON_VALVE_PIPE_CLOSED,
                    f"管段 {p.id} 没有阀门，不能设置为关闭状态",
                    p.id,
                )
            )
    return issues


# ---------------------------------------------------------------- 可达性


def _reachable_from(
    nodes: dict[str, Node],
    pipes: list[Pipe],
    removed_pipe_ids: frozenset[str],
    seeds,
) -> set[str]:
    """无向 BFS：从 seeds 出发，返回可达节点集合。

    removed_pipe_ids 中的管段视为不存在/不可通行。
    """
    adjacency: dict[str, list[str]] = {nid: [] for nid in nodes}
    for p in pipes:
        if p.id in removed_pipe_ids:
            continue
        if p.from_node not in adjacency or p.to_node not in adjacency:
            continue
        adjacency[p.from_node].append(p.to_node)
        adjacency[p.to_node].append(p.from_node)

    reachable: set[str] = set()
    queue: deque[str] = deque()
    for nid in seeds:
        if nid in nodes and nid not in reachable:
            reachable.add(nid)
            queue.append(nid)

    while queue:
        current = queue.popleft()
        for nxt in adjacency.get(current, ()):
            if nxt not in reachable:
                reachable.add(nxt)
                queue.append(nxt)
    return reachable


def _reachable(
    nodes: dict[str, Node],
    pipes: list[Pipe],
    removed_pipe_ids: frozenset[str],
) -> set[str]:
    """无向 BFS：从全部“运行中”的水源出发，返回可达节点集合。

    停运(running=False)的水源不供水，不作为通水起点，因此读网、隔离、
    关阀建议与复供的通水判断均按运行水源计算。
    """
    return _reachable_from(
        nodes,
        pipes,
        removed_pipe_ids,
        [
            nid
            for nid, node in nodes.items()
            if node.type == "source" and node.running
        ],
    )


# ---------------------------------------------------------------- 方案评估


def validate_plan_request(
    pipes: dict[str, Pipe],
    fault_pipe_id: str,
    close_pipe_ids: list[str],
    *,
    for_submit: bool,
) -> list[Issue]:
    """校验故障/拟关列表本身的合法性。

    for_submit=True 时额外校验“已关闭阀门不得重复关闭”。
    """
    issues: list[Issue] = []

    if fault_pipe_id not in pipes:
        issues.append(
            Issue(FAULT_PIPE_NOT_FOUND, f"故障管段不存在：{fault_pipe_id}", fault_pipe_id)
        )

    seen: set[str] = set()
    for pid in close_pipe_ids:
        if pid in seen:
            issues.append(Issue(CLOSE_PIPE_DUPLICATE, f"拟关闭管段重复：{pid}", pid))
            continue
        seen.add(pid)

        if pid == fault_pipe_id:
            issues.append(
                Issue(CLOSE_PIPE_IS_FAULT, "不能把故障管段本身作为关闭阀门", pid)
            )
            continue
        pipe = pipes.get(pid)
        if pipe is None:
            issues.append(Issue(CLOSE_PIPE_NOT_FOUND, f"拟关闭管段不存在：{pid}", pid))
        elif not pipe.has_valve:
            issues.append(Issue(CLOSE_PIPE_NO_VALVE, f"管段没有阀门，无法关闭：{pid}", pid))
        elif for_submit and pipe.valve_closed:
            issues.append(
                Issue(
                    CLOSE_PIPE_DUPLICATE,
                    f"阀门已处于关闭状态，不能重复关闭：{pid}",
                    pid,
                )
            )
    return issues


def evaluate_plan(
    nodes: dict[str, Node],
    pipes: dict[str, Pipe],
    fault_pipe_id: str,
    close_pipe_ids: list[str],
    *,
    for_submit: bool = False,
    pending_fault_pipe_ids: frozenset[str] = frozenset(),
) -> PlanResult:
    """评估隔离方案。

    预演(for_submit=False)：只做结构校验，已关闭阀门不会产生“重复关闭”问题；
    提交(for_submit=True)：对已关闭阀门再次关闭视为 CLOSE_PIPE_DUPLICATE。

    pending_fault_pipe_ids：其他“待修复”隔离事件的故障管段编号集合，
    无论方案前/方案后都视为不可通行（故障未修复，不能通水）。
    """
    issues = validate_plan_request(
        pipes, fault_pipe_id, close_pipe_ids, for_submit=for_submit
    )
    blocking = [i for i in issues if i.code in BLOCKING_ISSUE_CODES]

    fault = pipes.get(fault_pipe_id)
    if fault is None or blocking:
        # 结构不合法：无法进行可达性评估
        return PlanResult(
            isolatable=False, affected_sink_ids=[], issues=issues
        )

    pipe_list = list(pipes.values())
    pending_faults = frozenset(pid for pid in pending_fault_pipe_ids if pid in pipes)

    # 方案前（当前态）：已关闭阀门、其他待修复故障管段不可通行，本次故障尚未移除
    already_closed = frozenset(p.id for p in pipe_list if p.has_valve and p.valve_closed)
    base_removed = already_closed | pending_faults
    before_reachable = _reachable(nodes, pipe_list, base_removed)

    # 方案后：故障管段移除；已关阀门、其他待修复故障继续生效；拟关阀门全部关闭
    blocked = base_removed | frozenset(close_pipe_ids)
    after_removed = blocked | frozenset({fault_pipe_id})
    after_reachable = _reachable(nodes, pipe_list, after_removed)

    both_ends_isolated = (
        fault.from_node not in after_reachable
        and fault.to_node not in after_reachable
    )

    affected = sorted(
        nid
        for nid, node in nodes.items()
        if node.type == "sink" and nid in before_reachable and nid not in after_reachable
    )

    return PlanResult(
        isolatable=both_ends_isolated,
        affected_sink_ids=affected,
        issues=issues,
        blocked_pipe_ids=frozenset(blocked),
    )


# ---------------------------------------------------------------- 复供评估


def evaluate_restore(
    nodes: dict[str, Node],
    pipes: dict[str, Pipe],
    reopen_pipe_ids: frozenset[str],
    other_fault_pipe_ids: frozenset[str],
) -> RestoreResult:
    """评估复供安全性（只读，不改任何状态）。

    - reopen_pipe_ids：本事件提交时关闭的阀门管段，复供后重新通水；
    - other_fault_pipe_ids：其他“待修复”事件的故障管段，继续视为不可通行。

    仅当其余待修复故障管段的两端在复供图中仍均不可从水源到达，safe=True。
    预计恢复供水用水点 = 复供前不可达、复供后可达的 sink。
    """
    pipe_list = list(pipes.values())
    already_closed = frozenset(p.id for p in pipe_list if p.has_valve and p.valve_closed)
    other_faults = frozenset(
        pid for pid in other_fault_pipe_ids if pid in pipes
    )

    # 复供前（当前态）：仍关闭阀门 + 其他待修复故障管段不可通行
    before_removed = already_closed | other_faults
    before_reachable = _reachable(nodes, pipe_list, before_removed)

    # 复供后：本事件阀门重开（从阻断集合剔除）；其他待修复故障仍然阻断
    after_removed = (already_closed - frozenset(reopen_pipe_ids)) | other_faults
    after_reachable = _reachable(nodes, pipe_list, after_removed)

    blocks: list[ReopenBlock] = []
    for fid in sorted(other_faults):
        fault = pipes[fid]
        reachable_ends = [
            end
            for end in (fault.from_node, fault.to_node)
            if end in after_reachable
        ]
        if reachable_ends:
            blocks.append(
                ReopenBlock(fault_pipe_id=fid, reachable_endpoints=reachable_ends)
            )

    restored = sorted(
        nid
        for nid, node in nodes.items()
        if node.type == "sink" and nid not in before_reachable and nid in after_reachable
    )

    return RestoreResult(
        safe=not blocks,
        restored_sink_ids=restored,
        blocks=blocks,
        blocked_pipe_ids=frozenset(after_removed),
    )


# ---------------------------------------------------------------- 关阀建议


def recommend_plan(
    nodes: dict[str, Node],
    pipes: dict[str, Pipe],
    fault_pipe_id: str,
    *,
    pending_fault_pipe_ids: frozenset[str] = frozenset(),
) -> RecommendResult:
    """按当前管网为故障管段计算一套可提交的关阀建议（只读，不改任何状态）。

    规则：
    - 已关闭阀门与其他待修复事件的故障管段均不可通行；
    - 只能从“尚未关闭的有阀管段”中选取，且不含故障管段本身
      （与提交接口 CLOSE_PIPE_IS_FAULT / CLOSE_PIPE_DUPLICATE 约束一致，
      保证建议可直接提交）；
    - 方案须使故障管段两端都与所有水源断开；
    - 优选顺序：新增失供用水点数最少 → 本次关阀数最少 →
      关阀编号序列（按字典序排序后）字典序最小。

    实现：候选阀门为“位于某条水源→故障端路径上”的未关闭有阀管段
    （不在任何此类路径上的阀门无助于隔离，只会增加失供与关阀数，
    任何包含它的方案都被去掉它后的方案严格占优）；
    按关阀数升序、同数按编号字典序枚举候选子集，用 evaluate_plan 评估。
    新增失供数以“仅移除故障管段、不关任何阀”时的失供数为理论下界，
    枚举中首个达到下界的方案即全局最优，可提前结束。
    """
    fault = pipes.get(fault_pipe_id)
    if fault is None:
        return RecommendResult(
            feasible=False,
            recommended_close_pipe_ids=[],
            affected_sink_ids=[],
            reason=Issue(
                FAULT_PIPE_NOT_FOUND, f"故障管段不存在：{fault_pipe_id}", fault_pipe_id
            ),
        )

    pipe_list = list(pipes.values())
    pending_faults = frozenset(pid for pid in pending_fault_pipe_ids if pid in pipes)
    already_closed = frozenset(p.id for p in pipe_list if p.has_valve and p.valve_closed)
    base_removed = already_closed | pending_faults

    # 空方案（只移除故障管段）：若已可隔离即最优（失供最少且关阀数为 0）；
    # 其失供数同时是任何方案新增失供的理论下界（关阀只会移除更多管段）。
    empty = evaluate_plan(
        nodes, pipes, fault_pipe_id, [], pending_fault_pipe_ids=pending_faults
    )
    if empty.isolatable:
        return RecommendResult(
            feasible=True,
            recommended_close_pipe_ids=[],
            affected_sink_ids=empty.affected_sink_ids,
        )
    min_affected_count = len(empty.affected_sink_ids)

    # 方案图（移除故障管段后）中：水源侧与故障端侧各自的可达范围
    fault_removed = base_removed | frozenset({fault_pipe_id})
    source_side = _reachable(nodes, pipe_list, fault_removed)
    fault_side = _reachable_from(
        nodes, pipe_list, fault_removed, (fault.from_node, fault.to_node)
    )

    # 候选阀门：未关闭、非故障管段本身，且一端在水源侧、一端在故障端侧
    candidates = sorted(
        p.id
        for p in pipe_list
        if p.has_valve
        and not p.valve_closed
        and p.id != fault_pipe_id
        and (
            (p.from_node in source_side and p.to_node in fault_side)
            or (p.to_node in source_side and p.from_node in fault_side)
        )
    )

    best: tuple[tuple, list[str], list[str]] | None = None
    for size in range(1, len(candidates) + 1):
        for combo in combinations(candidates, size):
            result = evaluate_plan(
                nodes,
                pipes,
                fault_pipe_id,
                list(combo),
                pending_fault_pipe_ids=pending_faults,
            )
            if not result.isolatable:
                continue
            key = (len(result.affected_sink_ids), size, combo)
            if best is None or key < best[0]:
                best = (key, list(combo), result.affected_sink_ids)
            if len(result.affected_sink_ids) == min_affected_count:
                # 达到理论下界：size 升序 + 组合字典序，首个即全局最优
                return RecommendResult(
                    feasible=True,
                    recommended_close_pipe_ids=list(combo),
                    affected_sink_ids=result.affected_sink_ids,
                )

    if best is None:
        return RecommendResult(
            feasible=False,
            recommended_close_pipe_ids=[],
            affected_sink_ids=[],
            reason=Issue(
                NO_FEASIBLE_PLAN,
                f"关闭全部可用阀门后，故障管段 {fault_pipe_id} 仍至少有一端"
                "可从水源到达，无可行关阀方案",
                fault_pipe_id,
            ),
        )
    return RecommendResult(
        feasible=True,
        recommended_close_pipe_ids=best[1],
        affected_sink_ids=best[2],
    )


# ---------------------------------------------------------------- 水源状态切换


def validate_source_targets(
    nodes: dict[str, Node],
    targets: list[tuple[str, bool]],
) -> list[Issue]:
    """校验水源切换请求本身：编号存在、节点是水源、同批不重复。"""
    issues: list[Issue] = []
    seen: set[str] = set()
    for source_id, _running in targets:
        if source_id in seen:
            issues.append(
                Issue(
                    SOURCE_TARGET_DUPLICATE,
                    f"同批水源编号不得重复：{source_id}",
                    source_id,
                )
            )
            continue
        seen.add(source_id)

        node = nodes.get(source_id)
        if node is None:
            issues.append(
                Issue(SOURCE_NOT_FOUND, f"水源节点不存在：{source_id}", source_id)
            )
        elif node.type != "source":
            issues.append(
                Issue(
                    SOURCE_NOT_SWITCHABLE,
                    f"节点 {source_id} 不是水源，仅水源节点可切换运行状态",
                    source_id,
                )
            )
    return issues


def evaluate_source_switch(
    nodes: dict[str, Node],
    pipes: dict[str, Pipe],
    targets: list[tuple[str, bool]],
    *,
    pending_fault_pipe_ids: frozenset[str] = frozenset(),
) -> SourceSwitchResult:
    """评估水源运行状态切换（只读，不改任何状态）。

    targets：(水源编号, 目标运行状态) 列表，True=恢复运行、False=停运。
    pending_fault_pipe_ids：待修复(open)隔离事件的故障管段，通水时仍视为不可通行
    （已关闭阀门同样阻断）。

    返回：
    - changed：目标状态与当前是否有差异（无差异确认时不推进版本）；
    - affected_sink_ids：切换后新增失供用水点（切换前可达 → 切换后不可达）；
    - restored_sink_ids：切换后恢复供水用水点（切换前不可达 → 切换后可达）；
    - blocks：切换后每条待修复故障管段的重新进水风险（任一端从运行水源可达）。
      停运只会收缩可达范围，不会引入风险；风险只可能由恢复水源引发。
    """
    issues = validate_source_targets(nodes, targets)
    if issues:
        return SourceSwitchResult(
            changed=False,
            safe=False,
            targets=list(targets),
            affected_sink_ids=[],
            restored_sink_ids=[],
            issues=issues,
        )

    pipe_list = list(pipes.values())
    already_closed = frozenset(p.id for p in pipe_list if p.has_valve and p.valve_closed)
    pending_faults = frozenset(pid for pid in pending_fault_pipe_ids if pid in pipes)
    blocked = already_closed | pending_faults

    target_map = dict(targets)
    changed = any(nodes[sid].running != running for sid, running in targets)

    # 切换前（当前态）：按当前运行水源计算可达
    before_reachable = _reachable(nodes, pipe_list, blocked)

    # 切换后：仅水源运行状态改变，管网结构/阀位/故障管段不变
    switched_nodes = {
        nid: Node(
            id=n.id,
            type=n.type,
            name=n.name,
            running=target_map.get(nid, n.running),
        )
        for nid, n in nodes.items()
    }
    after_reachable = _reachable(switched_nodes, pipe_list, blocked)

    affected = sorted(
        nid
        for nid, node in nodes.items()
        if node.type == "sink" and nid in before_reachable and nid not in after_reachable
    )
    restored = sorted(
        nid
        for nid, node in nodes.items()
        if node.type == "sink" and nid not in before_reachable and nid in after_reachable
    )

    # 每条待修复故障：切换后任一端可从运行水源到达即有重新进水风险
    blocks: list[SourceSwitchBlock] = []
    for fid in sorted(pending_faults):
        fault = pipes[fid]
        reachable_ends = [
            end
            for end in (fault.from_node, fault.to_node)
            if end in after_reachable
        ]
        if reachable_ends:
            blocks.append(
                SourceSwitchBlock(
                    fault_pipe_id=fid, reachable_endpoints=reachable_ends
                )
            )

    return SourceSwitchResult(
        changed=changed,
        safe=not blocks,
        targets=list(targets),
        affected_sink_ids=affected,
        restored_sink_ids=restored,
        blocks=blocks,
        issues=[],
    )


# ---------------------------------------------------------------- 状态快照


def reachable_sink_ids(
    nodes: dict[str, Node],
    pipes: dict[str, Pipe] | list[Pipe],
    removed_pipe_ids: frozenset[str] = frozenset(),
) -> list[str]:
    """返回由运行中水源可达的用水点编号（按编号排序）。

    快照冻结“供水点”时统一使用该结果：只从 running=True 的水源出发，
    已关闭阀门与待修复故障管段视为不可通行（由调用方计入 removed_pipe_ids）。
    """
    pipe_list = list(pipes.values()) if isinstance(pipes, dict) else list(pipes)
    reachable = _reachable(nodes, pipe_list, removed_pipe_ids)
    return sorted(
        nid for nid, node in nodes.items() if node.type == "sink" and nid in reachable
    )


def freeze_state(
    nodes: dict[str, Node],
    pipes: dict[str, Pipe],
    open_faults: list[tuple[int, str]] | None = None,
) -> dict:
    """把当前领域状态冻结为不可变（JSON 可序列化）的快照载荷。

    open_faults：(事件编号, 故障管段编号) 列表，即全部待修复(open)故障，
    按事件编号排序；快照据此记录“当时待修复故障”，不引用后续事件回填。

    载荷中的 supplied_sink_ids 为“由运行水源可达的用水点”，计算时排除
    已关闭阀门与待修复故障管段，与隔离/复供/切换的通水口径完全一致。
    """
    frozen_nodes = [
        {
            "id": node.id,
            "type": node.type,
            "name": node.name,
            "running": node.running,
        }
        for node in (nodes[nid] for nid in sorted(nodes))
    ]
    frozen_pipes = [
        {
            "id": pipe.id,
            "from": pipe.from_node,
            "to": pipe.to_node,
            "has_valve": pipe.has_valve,
            "valve_closed": pipe.valve_closed,
            "name": pipe.name,
        }
        for pipe in (pipes[pid] for pid in sorted(pipes))
    ]
    blocked = frozenset(
        p.id for p in pipes.values() if p.has_valve and p.valve_closed
    ) | frozenset(fault_pid for _eid, fault_pid in (open_faults or []))
    frozen_faults = [
        {"event_id": event_id, "fault_pipe_id": fault_pipe_id}
        for event_id, fault_pipe_id in sorted(open_faults or [])
    ]
    return {
        "nodes": frozen_nodes,
        "pipes": frozen_pipes,
        "open_faults": frozen_faults,
        "supplied_sink_ids": reachable_sink_ids(nodes, list(pipes.values()), blocked),
    }


def _node_index(state: dict) -> dict[str, dict]:
    return {n["id"]: n for n in state.get("nodes", [])}


def _pipe_index(state: dict) -> dict[str, dict]:
    return {p["id"]: p for p in state.get("pipes", [])}


def _fault_index(state: dict) -> dict[int, dict]:
    return {f["event_id"]: f for f in state.get("open_faults", [])}


def diff_states(base: dict | None, target: dict) -> dict:
    """对照两份冻结状态，返回六类净变化。

    - nodes：节点新增/移除（整网导入为替换式，通常整批增删）；
    - pipes：管段新增/移除；
    - valve_positions：共有管段中阀门位置（开/关）变化；
    - source_states：共有水源节点运行(running)状态变化；
    - open_faults：待修复故障（按事件编号）新增/移除；
    - supplied_sinks：由运行水源可达用水点集合的新增/移除（净变化）。

    base 为 None（首条快照没有更早基线）时，目标状态全部计为新增。
    历史结果只取自两份冻结载荷，不读取当前管网或后续事件。
    """
    base = base or {}

    base_nodes, target_nodes = _node_index(base), _node_index(target)
    base_pipes, target_pipes = _pipe_index(base), _pipe_index(target)
    base_faults, target_faults = _fault_index(base), _fault_index(target)

    node_added = [target_nodes[nid] for nid in sorted(target_nodes.keys() - base_nodes.keys())]
    node_removed = [base_nodes[nid] for nid in sorted(base_nodes.keys() - target_nodes.keys())]

    pipe_added = [target_pipes[pid] for pid in sorted(target_pipes.keys() - base_pipes.keys())]
    pipe_removed = [base_pipes[pid] for pid in sorted(base_pipes.keys() - target_pipes.keys())]

    valve_changes: list[dict] = []
    for pid in sorted(base_pipes.keys() & target_pipes.keys()):
        before_p, after_p = base_pipes[pid], target_pipes[pid]
        if (
            before_p.get("has_valve")
            and after_p.get("has_valve")
            and bool(before_p.get("valve_closed")) != bool(after_p.get("valve_closed"))
        ):
            valve_changes.append(
                {
                    "pipe_id": pid,
                    "valve_closed_before": bool(before_p.get("valve_closed")),
                    "valve_closed_after": bool(after_p.get("valve_closed")),
                }
            )

    source_changes: list[dict] = []
    for nid in sorted(base_nodes.keys() & target_nodes.keys()):
        before_n, after_n = base_nodes[nid], target_nodes[nid]
        if (
            before_n.get("type") == "source"
            and after_n.get("type") == "source"
            and bool(before_n.get("running")) != bool(after_n.get("running"))
        ):
            source_changes.append(
                {
                    "source_id": nid,
                    "running_before": bool(before_n.get("running")),
                    "running_after": bool(after_n.get("running")),
                }
            )

    fault_added = [
        target_faults[eid] for eid in sorted(target_faults.keys() - base_faults.keys())
    ]
    fault_removed = [
        base_faults[eid] for eid in sorted(base_faults.keys() - target_faults.keys())
    ]

    base_sinks = set(base.get("supplied_sink_ids", []))
    target_sinks = set(target.get("supplied_sink_ids", []))
    sink_added = sorted(target_sinks - base_sinks)
    sink_removed = sorted(base_sinks - target_sinks)

    return {
        "nodes": {"added": node_added, "removed": node_removed},
        "pipes": {"added": pipe_added, "removed": pipe_removed},
        "valve_positions": {"changed": valve_changes},
        "source_states": {"changed": source_changes},
        "open_faults": {"added": fault_added, "removed": fault_removed},
        "supplied_sinks": {"added": sink_added, "removed": sink_removed},
    }
