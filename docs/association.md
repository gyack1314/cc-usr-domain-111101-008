# 双时间维度关联

`datapack.association` 在不可变的资料快照上完成双时间连接。每个维度记录同时
携带两条左闭右开 UTC 区间：

| 区间 | 字段 | 含义 |
| --- | --- | --- |
| 业务有效 | `valid_from`, `valid_to` | 该归属在业务上生效的时间 |
| 知识可见 | `known_from`, `known_to` | 这条记录可被系统用于计算的时间 |

终点字段（`valid_to`、`known_to`）为空（`null`、缺失或 CSV 空单元格）表示
正无穷；起始字段必须给出。所有时刻必须带明确 UTC 偏移；纯日期、无偏移的
本地时间会被拒绝，不会用运行环境时区补全。事实只有 `event_at`，引擎从不读取
`ingested_at` 或任何入库时间代替业务发生时点。

## 连接顺序

对给定的 `knowledge_cutoff`：

1. 先筛出 `known_from <= cutoff < known_to` 的维度版本；
2. 再按事实 `event_at` 筛出 `valid_from <= event_at < valid_to` 的记录；
3. 每一段链路必须恰好命中一条记录。零条是 `gap`，多条（包括同版本有效区间
   重叠）是 `conflict`；
4. 链路为 `vehicle -> operator -> region`，软件发布作为 `vehicle -> software`
   的并行链。任一环失败都只写错误旁路，并标明 `stage`、被查实体和候选
   `row_id`，不会生成多条成功事实。

`latest` 不是“边跑边取最新”。它在计算开始前解析为资料中已出现的最晚
`known_from`，随后整次连接只使用这一个固定边界。历史显式 cutoff 的查询结果
不受之后导入的新知识影响；新知识应作为带新 `known_from` 的新记录加入，并把
旧记录的 `known_to` 收口，而不是覆盖旧行。

## Python API

```python
from datapack.association import load_dataset

dataset = load_dataset("samples")          # 读取 bundle.json 与四个记录文件
result = dataset.join("2026-11-10T12:00:00Z")
result = dataset.join("latest")            # 解析为一个固定 cutoff

report = dataset.impact(
    "2026-11-07T12:00:00Z",
    "2026-11-10T12:00:00Z",
)
```

也可以直接构造快照，便于测试或接入其他读取方式：

```python
from datapack.association import BitemporalDataset

dataset = BitemporalDataset(
    vehicle_rows, operator_rows, software_rows, fact_rows,
)
```

`join()` 返回：

* `knowledge_cutoff`: `{"mode": "explicit"|"latest", "cutoff": "..."}`；
* `facts`: 成功连接，每个事实最多一条，含 `attribution`、`references` 和
  完整 `chain`（每段引用的 `row_id`、有效/可见区间）；
* `errors`: 错误旁路，含 `fact_id`、`partition`、`kind`
  (`gap`/`conflict`/`fact_conflict`/`fact_invalid`)、`stage`、`entity_id`、
  `candidates`、已解析的 `resolved` 前缀和 `detail`；
* `counts` / `duplicate_fact_ids` / `conflict_fact_ids` / `invalid_fact_ids`。

事实主键完全相同的重复输入会幂等去重并记入 `duplicate_fact_ids`；主键相同
但内容不同会进入 `fact_conflict`，不会静默覆盖或任选一行成功。维度主键内容
冲突则在构造快照时直接报错。

## 修订影响分析

`impact(from_cutoff, to_cutoff)` 固定两个截止后逐事实重算，返回：

* `changed`: 语义连接结果确实改变的事实，含 `partition`、改前/改后的
  `status`、`attribution`/`errors` 与引用链。比较的是成功归属或错误类别、
  段、候选，而不是“该车辆出现的所有月份”。
* `reference_only_changes`: 归属未变、仅引用行变化的记录，单独列出，避免把
  知识重排误算成统计影响。
* `partitions` / `changed_by_partition`: 精确到受影响事实的分区。
* `unchanged_fact_ids` 与 `excluded_fact_ids`（无法连接的主键冲突/非法事实）。

样本中 `2026-11-07T12:00:00Z -> 2026-11-10T12:00:00Z` 只改变 `f2`
（O1/east 变成 O2/west）和 `f4`（软件版本冲突），`f1` 仅换引用行、
归属不变，`f3` 仍为车辆断档。受影响分区只有 `nov`，不会扩张成车辆 V1 的
所有月份。

## 命令行

```bash
# 成功事实写入连接文件，断档/冲突写入独立错误旁路
python -m datapack.cli join samples \
  --cutoff 2026-11-10T12:00:00Z \
  --output state/fact-joins.jsonl \
  --errors state/fact-errors.jsonl

# 固定 latest 边界
python -m datapack.cli join samples --cutoff latest

# 两个固定 cutoff 的修订影响
python -m datapack.cli impact samples \
  --from-cutoff 2026-11-07T12:00:00Z \
  --to-cutoff 2026-11-10T12:00:00Z
```

`join` 摘要输出到 stdout，`--output` 与 `--errors` 均为 JSONL（每行一个对象，
原子写入）。`impact` 将报告输出到 stdout，可用 `--output` 另存 JSON。输入资料
非法或 cutoff 无偏移时命令返回 2；事实级 gap/conflict 属于正常旁路，命令仍
返回 0，便于批量重算。
