# Iceberg Merge 分发设计（中文）

本文档给出一个 Trino 风格的分发策略设计：INSERT 行按分区列（或 RR），DELETE/UPDATE 行按 row_id（更理想是 row_id.file_path）。
目标是减少插入侧的小文件，同时保持语义正确与原子性。

范围：设计 + 实现跟踪。

## 0. TODO（实现跟踪）

- [x] Step 1：补齐实现 TODO 列表（本节）
- [x] Step 2：FE 分发规格与计划翻译（DistributionSpecMerge + DataPartition + translator）
- [x] Step 3：Thrift 扩展与生成代码同步（MERGE_PARTITIONED + TMergePartitionInfo）
- [x] Step 4：BE Exchange 分发实现（MergePartitioner + MERGE_PARTITIONED）
- [x] Step 5：UT/回归测试与 Explain 校验更新
- [x] Step 6：修复分区列匹配对输出列名的依赖（方案 2：按目标列顺序/ExprId 映射）
- [x] Step 7：补充分区列更新表达式的单测覆盖
- [x] Step 8：支持非 identity 分区的 insert 分支 shuffle（传递 partition spec + BE 计算 transform）
- [x] Step 9：MERGE insert 分支支持 scale write / skew rebalancer（分区表 + 非分区表）
- [x] Step 10：Iceberg MERGE INTO 语法支持（复用 parser，路由到 IcebergMergeCommand）

## 1. 背景

当前实现：
- 进入 `IcebergMergeSink` 之前做一次 exchange，按 row_id hash。
- `IcebergMergeSink` 内部分裂为 delete_block 与 insert_block。
- insert_block 不携带 row_id，但已经按 row_id shuffle 过。

问题：
- insert 行没有按分区列聚合，分区表容易产生小文件。

目标：
- 保持正确性与原子性。
- insert 行按分区列聚合（或 RR），delete/update 行按 row_id 聚合。

## 2. 分发规则（与 Trino 对齐）

分发规则：
- INSERT / UPDATE_INSERT：按分区列 hash；无分区则 round-robin。
- DELETE / UPDATE_DELETE / UPDATE：按 row_id（建议用 row_id.file_path）。
- operation 列仅用于分支选择，不参与 hash key。

## 3. 数据结构设计

### 3.1 新分发规格（FE）

```
class DistributionSpecMerge extends DistributionSpec {
  ExprId operationExprId;
  List<ExprId> insertPartitionExprIds;  // 空表示 RR
  List<ExprId> deletePartitionExprIds;  // 通常是 row_id 或 row_id.file_path
  boolean insertRandom;
}
```

说明：
- deletePartitionExprIds 可直接用 row_id ExprId，由 BE 提取 file_path 参与 hash。
- 如果 FE 侧可以做 `struct_element(row_id, 'file_path')`，可直接传该 ExprId。

### 3.2 Thrift 扩展（FE <-> BE）

```
enum TPartitionType {
  ...
  MERGE_PARTITIONED = <new_id>
}

struct TMergePartitionInfo {
  1: required Exprs.TExpr operation_expr
  2: optional list<Exprs.TExpr> insert_partition_exprs
  3: optional list<Exprs.TExpr> delete_partition_exprs
  4: required bool insert_random
}

struct TDataPartition {
  1: required TPartitionType type
  2: optional list<Exprs.TExpr> partition_exprs
  ...
  N: optional TMergePartitionInfo merge_partition_info
}
```

## 4. FE 侧改动清单（类/函数级）

### 4.1 物理属性
- `PhysicalIcebergMergeSink.getRequirePhysicalProperties()`
  - 计算 operationExprId。
  - delete 分支：row_id ExprId 或 row_id.file_path ExprId。
  - insert 分支：从 Iceberg 分区列映射到子节点输出 slot。
  - 若无分区列，设 `insertRandom = true`。
  - 返回 `DistributionSpecMerge`。

### 4.2 属性派生与 Enforcer
- `RequestPropertyDeriver.visitPhysicalIcebergMergeSink(...)`
  - 允许 `DistributionSpecMerge` 作为 required。
- `DistributionEnforcer`（或等价模块）
  - 根据 `DistributionSpecMerge` 插入 `PhysicalDistribute`。

### 4.3 物理计划翻译
`PhysicalPlanTranslator` 中：
- 将 `DistributionSpecMerge` 翻译为 `TPartitionType::MERGE_PARTITIONED`。
- 填充 `TMergePartitionInfo`：
  - operation_expr
  - insert_partition_exprs
  - delete_partition_exprs
  - insert_random

### 4.4 表达式/Slot 处理
若选择 row_id.file_path：
- 方案 A（BE 处理）：FE 仅传 row_id，BE 内部提取 file_path。
- 方案 B（FE 投影）：FE 新增隐藏投影 `struct_element(row_id, 'file_path')`。

### 4.5 分区列匹配的别名问题（新增）
当前实现若用“输出列名 == 分区列名”来做匹配，遇到 UPDATE/MERGE 的投影改写就会失败：

示例 1（UPDATE 改分区列为表达式）：
```
UPDATE t
SET country = upper(country)
WHERE country = 'US';
```
`country` 输出列会变成 `upper(country)` 或 `expr$0` 之类的名字，导致匹配失败，
insert 分支退化为 RR。

示例 2（UPDATE 改分区列为常量）：
```
UPDATE t
SET country = 'CN'
WHERE country = 'US';
```
输出列名为常量/表达式，同样无法按名字匹配。

因此需要方案 2：按目标表列顺序（ExprId 对齐）来映射分区列对应的输出 ExprId，
避免依赖输出列名。

### 4.6 非 identity 分区的 shuffle 支持（规划）
目标：insert 分支在非 identity 分区（如 days/months/bucket/truncate）时也能按分区聚合写，
避免退化为 RR。

核心思路：
1) FE 获取 Iceberg partition spec（field 列表：source_id、transform、param、name）。
2) FE 生成 insert 分支分发元数据：将每个 partition field 映射到“源列输出 ExprId”。
3) Thrift 透传到 BE（新结构体）：包含 transform 类型与参数 + 对应的 source expr。
4) BE MergePartitioner 在 insert 分支计算 transform 后的 partition value，再做 hash 分发。
5) 若 transform 不支持或映射失败，回退 RR。

#### 4.6.1 支持的 transform（以 BE 现有实现为准）
当前 BE 已支持以下 Iceberg transform（来自 `partition_transformers`）：
- identity
- year / month / day / hour（datev2、datetimev2）
- bucket[N]（int/bigint/string/datev2/datetimev2/decimal）
- truncate[N]（int/bigint/string/decimal）
- void

非上述 transform 或不匹配的列类型，直接回退 RR。

#### 4.6.2 FE 侧分区 spec 解析与映射
1) 从 Iceberg table 获取当前 spec（与 writer 保持一致），读取字段：
   - source_id
   - transform (string)
   - name
2) 通过 Iceberg schema 建立 `source_id -> column_name` 映射，再基于目标表列顺序/ExprId
   映射到 “源列输出 ExprId”。
3) 按 spec 中字段顺序生成 insert_partition_fields（顺序用于 composite hash）。

若任一 field 无法映射到输出 ExprId，回退 RR。

#### 4.6.3 Thrift 扩展（建议）
```
struct TIcebergPartitionField {
  1: required string transform   // identity/bucket/truncate/year/month/day/hour
  2: optional i32 param          // truncate/bucket width, identity 无参数
  3: required Exprs.TExpr source_expr
  4: optional string name
  5: optional i32 source_id
}

struct TMergePartitionInfo {
  ...
  5: optional list<TIcebergPartitionField> insert_partition_fields
  6: optional i32 partition_spec_id
}
```

FE 侧生成逻辑（建议位置：PhysicalIcebergMergeSink.getRequirePhysicalProperties）：
- 使用目标表列顺序/ExprId 映射到“源列 ExprId”。
- 将 Iceberg partition spec 的 transform + source_id 绑定到对应 ExprId。
- 如果存在非 identity transform，填充 insert_partition_fields；
  同时 insert_partition_exprs 保持为源列 expr（用于兼容旧 BE）。

#### 4.6.4 BE 侧实现（MergePartitioner）
- 如果 insert_partition_fields 不为空：
  - 为每个 field 构造 `PartitionColumnTransform`（复用 writer）。
  - 先对 source_expr 列执行 transform，得到 “partition value 列”。
  - 使用现有 hash 逻辑（update_crc32c_batch / update_crcs_with_value）按 spec 顺序组合 hash，
    保证相同 partition value 进入同一 channel。
  - bucket/truncate/year/month/day/hour 的语义严格复用 writer：
    - bucket 使用 murmur_hash3_32(seed=0) 与 INT32_MAX mask 后取模；
    - date/datetime 的 bucket 使用 epoch day / unix_timestamp*1_000_000；
    - year/month/day/hour 使用 epoch-based datetime_diff。
- 如果 insert_partition_fields 为空：沿用当前 insert_partition_exprs 或 RR。
- void transform：产出全 null 列，所有行落同一 partition（等价于单通道）。

#### 4.6.5 回退与兼容策略
- transform 不支持、参数非法、source_expr 缺失 -> insertRandom = true（RR）。
- 老版本 BE 不认识新字段：忽略 insert_partition_fields，使用 insert_partition_exprs 或 RR。
- 指定 partition_spec_id 仅用于调试/追踪，不影响分发。

#### 4.6.6 测试建议（新增）
- UT：构造不同 transform（bucket/truncate/day/month）确认 insert 分支使用 transform 后分发。
- 回归：EXPLAIN 展示 MERGE_PARTITIONED + transform 字段（如可打印）。

#### 4.6.7 备选方案：exchange 复用 partition_spec_json
思路：不在 FE 解析 spec，直接把 `partition_specs_json + partition_spec_id` 透传到
`TMergePartitionInfo`，在 BE 的 MergePartitioner 内部解析 JSON 并构造
`PartitionColumnTransform`。

优点：
- FE 逻辑更轻，避免在 FE 维护 transform 解析。
- BE 与 writer 复用同一套 JSON 解析与 transform 实现。

缺点/挑战：
- 需要在 Exchange 侧额外拿到 schema_json（否则无法完成 source_id -> column_idx 映射）。
- MergePartitioner 目前只拿 `TMergePartitionInfo`，需新增字段并初始化 schema/spec（性能开销）。
- 需要保证 output 表达式的列顺序与 schema 列顺序一致，否则 source_id 映射不可靠。

建议形态：
```
struct TMergePartitionInfo {
  ...
  7: optional string schema_json
  8: optional map<i32, string> partition_specs_json
  9: optional i32 partition_spec_id
}
```

BE 侧流程：
1) 解析 schema_json + partition_specs_json[partition_spec_id] -> PartitionSpec。
2) 建立 source_id -> column_idx，再映射到 output ExprId。
3) 用 PartitionColumnTransform 计算 partition value 并 hash。

结论：
该方案可行但侵入更大，推荐优先使用 4.6.3 的 `insert_partition_fields` 方案。

## 5. BE 侧改动清单（类/函数级）

### 5.1 Exchange 分发
新增 `MergePartitioner`（或扩展现有 HashPartitioner）：
- 评估 operation_expr。
- INSERT / UPDATE_INSERT：
  - 若 insert_random=true -> RR
  - 否则 hash(insert_partition_exprs)
- DELETE / UPDATE_DELETE / UPDATE：
  - hash(delete_partition_exprs)
  - 若 expr 为 row_id，取 row_id.file_path 参与 hash

### 5.2 row_id 哈希
row_id 为 STRUCT：
- 取 field `file_path` 作为 hash key。
- 与 Trino 的 IcebergUpdateBucketFunction 行为一致。

### 5.3 Merge Sink
`VIcebergMergeSink` 无需修改：
- 仍按 operation 分裂 block 并执行 delete/insert 写入。

### 5.4 MERGE insert 的 scale write / skew rebalancer
当前 `MERGE_PARTITIONED` 仅使用 `MergePartitioner` 直接 hash 到 channel，
不会像 `HIVE_TABLE_SINK_HASH_PARTITIONED` 一样启用 `SkewedPartitionRebalancer`，
也不会像 `HIVE_TABLE_SINK_UNPARTITIONED` 一样做非分区 scale write。

新增目标：只对 insert 分支引入“分区写 skew 纠偏 / 非分区 scale write”，
delete 分支仍保持 row_id hash 语义不变。

**分区表 insert（按分区键 hash）**
- hash -> 虚拟 partition id（count = channels * max_partition_nums_per_writer）
- `SkewedPartitionRebalancer` 将 partition id -> writer channel
- 仅对 insert 分支生效；delete 分支继续按 row_id 直达 channel
- 参数复用：
  - `table_sink_partition_write_max_partition_nums_per_writer`
  - `table_sink_partition_write_min_partition_data_processed_rebalance_threshold`
  - `table_sink_partition_write_min_data_processed_rebalance_threshold`
  - task_num 缩放逻辑保持与 exchange sink 一致

**非分区 insert（insertRandom）**
- 使用 `table_sink_non_partition_write_scaling_data_processed_threshold`
  控制 active writer 的逐步扩张
- insert 分支 RR 仅在 [0, active_writers) 内分发
- delete 分支不受影响

## 6. 执行流程图（ASCII）

```
Scan -> Project (operation, row_id, data cols)
        |
        v
  MergePartitionExchange
  - if op in INSERT/UPDATE_INSERT:
      hash(partition_cols) or RR
  - else (DELETE/UPDATE_DELETE/UPDATE):
      hash(row_id.file_path)
        |
        v
  IcebergMergeSink
    - filter -> delete_block -> IcebergDeleteWriter
    - filter -> insert_block -> IcebergTableWriter
    - commit RowDelta (atomic)
```

## 7. 测试清单

### FE 单测
- 断言 `DistributionSpecMerge` 生成。
- exchange 使用 merge 分发且 exprs 正确。

### BE 单测
- MergePartitioner 路由正确：
  - INSERT -> insert 分支（hash/rr）
  - DELETE -> delete 分支（hash row_id.file_path）

### 回归
- EXPLAIN 输出包含 merge 分发与 sink。
- 分区表 update 后小文件改善（可选指标）。

## 8. 发布与开关

建议引入 session 开关：
- `enable_iceberg_merge_partitioning`
- 默认关闭，验证稳定后再默认开启。

## 9. Iceberg MERGE INTO 语法支持设计

### 9.1 目标
- 复用已有 MERGE INTO 语法与 Nereids parser，不新增语法分支。
- 在语义分析/执行阶段根据目标表类型路由：
  - Olap(MOW) 继续走现有 `MergeIntoCommand` 流程；
  - Iceberg 外表走新的 `IcebergMergeCommand`（或 `IcebergMergeIntoCommand`）。
- 复用现有 Iceberg UPDATE/DELETE 的 merge sink 能力。

### 9.2 入口复用（Parser）
当前 parser 已支持 MERGE INTO（`LogicalPlanBuilder.visitMergeInto`），直接产生
`MergeIntoCommand`，携带：
- targetNameParts / targetAlias
- source plan
- onClause
- matched / notMatched clauses

设计原则：**parser 不依赖 catalog 类型**，仍产出通用命令节点，后续阶段再分流。

### 9.3 路由策略（Command/Analyzer）
方案建议：
1) 在 `MergeIntoCommand.run()` / `getExplainPlan()` 中判断目标表类型；
2) 若为 IcebergExternalTable，则构建 `IcebergMergeCommand` 并委派执行/Explain；
3) 若为 OlapTable 且 MOW 开启，保持现有逻辑；
4) 其他表类型报错。

优点：不改 parser；保持语法兼容；Iceberg 逻辑独立。

### 9.4 IcebergMergeCommand 逻辑框架
核心输出与现有 Iceberg UPDATE 对齐：`operation` + `row_id` + `data columns`，
最终进入 `LogicalIcebergMergeSink` / `PhysicalIcebergMergeSink`。

主要步骤：
1) 生成基础 join：
   - 复用 MergeIntoCommand 的 join 模式（`source LEFT JOIN target ON onClause`），
     target 侧需注入 `row_id` slot；
2) 解析 matched / not matched clause：
   - DELETE -> `operation = DELETE`
   - UPDATE -> `operation = UPDATE` + 输出新值列
   - INSERT -> `operation = INSERT` + 输出插入列
3) 生成投影：
   - `operation`（TinyInt）
   - `row_id`（target row_id；对 insert-only 行可为 NULL）
   - 目标表数据列（按表列顺序输出）
4) 类型对齐：
   - 与 update 一致，使用绑定阶段 cast/alias 对齐到表列类型；
5) 生成 `LogicalIcebergMergeSink`：
   - delete file type = position delete
   - sink outputExprs = 上述投影

### 9.5 clause 语义与列映射
- `WHEN MATCHED THEN UPDATE SET ...`
  - 对未更新列填充目标表原值。
- `WHEN MATCHED THEN DELETE`
  - data 列可输出原值或默认值，实际写入只影响 delete sink。
- `WHEN NOT MATCHED THEN INSERT (...) VALUES (...)`
  - 若未指定列列表，按目标表列顺序对齐。
  - 未赋值列：可允许默认值（若 Iceberg 支持），否则报错（需定义策略）。

### 9.6 Explain 输出
- Explain 仍展示 MERGE INTO；
- 对 Iceberg 路径应展示 `IcebergMergeSink` 与 `MERGE_PARTITIONED` 分发。

### 9.7 兼容性与限制
- 首版仅支持 Iceberg v2（position delete + data write）。
- 不支持复杂子查询/多分支回写（若现有 MergeIntoCommand 已限制，可沿用）。
- 对 default value / generated column 的处理需明确（Iceberg 侧可能不支持）。

### 9.8 测试计划（高层）
- FE 单测：MERGE INTO 命令对 Iceberg 表路由到 IcebergMergeCommand。
- Plan 单测：operation/row_id/data 列顺序正确；Explain 含 merge sink 与 exchange。
- 端到端：简单 MERGE（UPDATE/DELETE/INSERT）验证结果与文件变更。
