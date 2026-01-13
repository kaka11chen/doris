# Iceberg UPDATE 单次扫描 + Merge Sink 设计

## 1. 背景

当前 `IcebergUpdateCommand` 使用“先 DELETE 再 INSERT”的两次执行方式：
- 两次扫描（重复 IO）
- 两次事务（非原子）

根据 `Trino_Iceberg_Update_Delete_原理与实现详解.md` 的思路，UPDATE 应该等价于
**DELETE + INSERT**，但需要 **单次扫描 + RowDelta 一次提交**，确保原子性并减少 IO。

## 2. 目标

- UPDATE 只扫描一次数据文件。
- 在同一个 Iceberg 事务里一次 `RowDelta.commit()`，原子提交 delete + insert。
- 复用现有 position delete 和 Iceberg table writer。
- 维持 explain 中 `operation` + `row_id` 的分布属性。

## 3. 非目标

- 不实现 equality delete。
- 不实现 MERGE SQL 语法。
- 不实现冲突检测/隔离级别增强。
- 不支持 Iceberg format version < 2。

## 4. 关键概念

- `operation` 列：`IcebergMergeOperation` 中的常量。
- `__DORIS_ICEBERG_ROWID_COL__`：struct，包含
  `(file_path, row_position, partition_spec_id, partition_data_json)`。
- UPDATE 行输出统一为 **单条**，由 merge sink 同时处理 delete + insert。

### 4.1 操作类型映射

| operation | 含义 | Merge Sink 行为 |
| --- | --- | --- |
| 1 (INSERT) | 插入 | 只写 data file |
| 2 (DELETE) | 删除 | 只写 delete file |
| 3 (UPDATE) | 更新 | 同时写 delete + insert |
| 4 (UPDATE_INSERT) | 更新插入部分 | 只写 data file |
| 5 (UPDATE_DELETE) | 更新删除部分 | 只写 delete file |

当前 UPDATE 计划优先使用 `UPDATE`（值 3），避免生成两条记录。

## 5. 总体流程

```
Scan -> Project(operation, row_id, updated_cols)
    -> Exchange(hash(operation, row_id))
    -> IcebergMergeSink
        -> delete writer (position delete)
        -> data writer (insert)
    -> FE RowDelta.addDeletes + addRows -> commit
```

执行步骤：
1) FE 构造单条更新计划，输出 `operation + row_id + updated columns`。
2) BE 侧 merge sink 根据 `operation` 分流：
   - UPDATE: 同时生成 delete + insert。
3) BE 回传 `TIcebergCommitData` 列表（DATA + POSITION_DELETES）。
4) FE `finishMerge()` 统一提交 RowDelta。

## 6. FE 设计

### 6.1 逻辑计划

- 在 `IcebergUpdateCommand` 中生成单条计划：
  - 注入 row_id（复用 `IcebergRowIdInjector`）。
  - `LogicalProject` 输出：
    - `operation = UPDATE_OPERATION_NUMBER`
    - `__DORIS_ICEBERG_ROWID_COL__`
    - 更新后的列（复用 `buildUpdateSelectItems`）。

### 6.2 Sink 节点

- 新增 `LogicalIcebergMergeSink` / `PhysicalIcebergMergeSink`。
- 输出物理分布要求：
  - `hash(operation, __DORIS_ICEBERG_ROWID_COL__)`，强制 remote exchange。
  - 与 `PhysicalIcebergDeleteSink` 现有行为保持一致。

### 6.3 Thrift & Translator

新增 `TDataSinkType::ICEBERG_MERGE_SINK`，并定义 `TIcebergMergeSink`：

- Insert 侧字段（等同 Iceberg table sink）：
  - `schema_json`
  - `partition_specs_json`
  - `partition_spec_id`
  - `file_format`
  - `compression_type`
  - `output_path`
  - `original_output_path`
  - `hadoop_config`
  - `file_type`
  - `broker_addresses`
- Delete 侧字段：
  - `delete_type`（POSITION_DELETES）
  - `table_location`
  - `partition_spec_id`
  - `partition_data_json`

### 6.4 事务与提交

新增 `IcebergTransaction.beginMerge()` / `finishMerge()`：

- `finishMerge()` 内部：
  - 从 `commitDataList` 拆分 DATA / POSITION_DELETES。
  - DATA -> `IcebergWriterHelper.convertToWriterResult(...)`
  - DELETE -> `IcebergWriterHelper.convertToDeleteFiles(...)`
  - `RowDelta.addRows(dataFiles)` + `RowDelta.addDeletes(deleteFiles)`
  - `RowDelta.commit()`

## 7. BE 设计

### 7.1 Merge Sink

新增 `VIcebergMergeSink`（pipeline 侧新增 `IcebergMergeSinkOperatorX`）：

- 复用：
  - `VIcebergTableWriter` 写 data files
  - `VIcebergDeleteSink` 生成 position delete
- 输入 block：
  - `operation`（tinyint）
  - `__DORIS_ICEBERG_ROWID_COL__`（struct）
  - data columns

### 7.2 行分流逻辑

对每个 block：
- `INSERT/UPDATE_INSERT`：抽取 data 列，写入 table writer。
- `DELETE/UPDATE_DELETE`：抽取 row_id 列，收集 row positions。
- `UPDATE`：同一行分别进入 delete 与 insert。

避免全量拷贝：建议按行构建两个轻量 block（delete-only 和 insert-only），只复制必要列。

### 7.3 Commit Data 汇总

`VIcebergMergeSink::close()`：
- 关闭 data writer，回传 `TIcebergCommitData`（DATA）。
- 写 position delete 文件并回传 `TIcebergCommitData`（POSITION_DELETES）。
- 使用 `RuntimeState::add_iceberg_commit_datas` 汇总。

## 8. RowId 与分区信息

- `row_id` 由 Parquet/ORC reader 生成（已实现）。
- 非分区表：spec id = 0，`partition_data_json` 为空。
- 分区表：`partition_spec_id`、`partition_data_json` 由 scan 传入 row_id。

## 9. 失败与回滚

- BE 执行失败：事务回滚，未提交文件视为垃圾文件。
- FE `RowDelta.commit()` 失败：回滚事务并返回错误。
- 提交成功后触发表级刷新（现有逻辑）。

## 10. 性能与资源

- 单次扫描减少 IO 和 CPU。
- 删除文件采用批量写（沿用现有 position delete 逻辑）。
- UPDATE 行不会额外复制为两条记录，减少内存占用。

## 11. 测试计划

### 11.1 FE
- explain update 验证：
  - `ICEBERG MERGE SINK`
  - `HASH(operation, row_id)` 的 remote exchange

### 11.2 BE
- Merge sink unit test：
  - UPDATE -> 同时生成 DATA + POSITION_DELETES
  - DELETE -> 仅 delete
  - INSERT -> 仅 data

### 11.3 端到端
- UPDATE with where（非分区表/分区表）：
  - 更新前后查询结果正确
  - snapshot 中 delete manifests 可见

## 12. 迁移与开关

可选增加 session var（例如 `enable_iceberg_merge_update`）进行灰度切换：
- ON：单次扫描 + merge sink
- OFF：保留旧路径（两次 scan）

## 13. Open Questions

- 是否需要进一步把 `hash(operation, row_id)` 换成按 `file_path` 聚合以减少 delete files？
- `UPDATE` 是否需要拆分为 `UPDATE_INSERT/UPDATE_DELETE` 两条逻辑行（目前不需要）？

## 14. 变更清单（文件级）

### 14.1 FE

- `fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/commands/IcebergUpdateCommand.java`
  - 替换为单条执行计划（project + merge sink）。
  - 调用 `beginMerge()/finishMerge()`，不再走 delete + insert 两次执行器。
- `fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/logical/LogicalIcebergMergeSink.java`
  - 新增逻辑 sink。
- `fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/physical/PhysicalIcebergMergeSink.java`
  - 新增物理 sink，包含 `hash(operation,row_id)` 分布要求。
- `fe/fe-core/src/main/java/org/apache/doris/nereids/rules/implementation/LogicalIcebergMergeSinkToPhysicalIcebergMergeSink.java`
  - 逻辑到物理规则。
- `fe/fe-core/src/main/java/org/apache/doris/nereids/rules/analysis/BindExpression.java`
  - 添加 merge sink output 绑定规则（类似 `BINDING_ICEBERG_DELETE_SINK_OUTPUT`）。
- `fe/fe-core/src/main/java/org/apache/doris/nereids/rules/RuleType.java`
  - 新增规则枚举（binding + implementation）。
- `fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/PlanType.java`
  - 新增 `LOGICAL_ICEBERG_MERGE_SINK` / `PHYSICAL_ICEBERG_MERGE_SINK`。
- `fe/fe-core/src/main/java/org/apache/doris/planner/IcebergMergeSink.java`
  - 生成 `TIcebergMergeSink`（类似 `IcebergDeleteSink` + `IcebergTableSink`）。
- `fe/fe-core/src/main/java/org/apache/doris/nereids/glue/translator/PhysicalPlanTranslator.java`
  - 新增 merge sink 翻译到 thrift。
- `fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/IcebergTransaction.java`
  - 增加 `beginMerge()/finishMerge()`。
  - `finishMerge()` 中对 commit data 拆分并 `RowDelta.addDeletes/addRows`。
- `fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/helper/IcebergWriterHelper.java`
  - 新增 `filterCommitDataByContent(...)` 或 `splitCommitDataByContent(...)` 工具方法。
- 可选：`fe/fe-core/src/main/java/org/apache/doris/qe/SessionVariable.java`
  - 增加开关 `enable_iceberg_merge_update` 便于灰度。

### 14.2 BE

- `be/src/vec/sink/viceberg_merge_sink.h/.cpp`
  - 新增 merge sink，内部复用 table writer + delete writer。
- `be/src/pipeline/exec/iceberg_merge_sink_operator.h/.cpp`
  - pipeline 侧 operator。
- `be/src/pipeline/pipeline_fragment_context.cpp`
  - 识别 `ICEBERG_MERGE_SINK` 并创建 operator。
- `be/src/pipeline/exec/operator.cpp`
  - 注册 operator。
- `be/src/vec/sink/writer/iceberg/viceberg_table_writer.*`
  - 复用，不改接口（只在 merge sink 中调用）。
- `be/src/vec/sink/viceberg_delete_sink.*`
  - 复用其 position delete 逻辑（可抽取公共函数或组合使用）。

### 14.3 Thrift/Gen

- `gensrc/thrift/DataSinks.thrift`
  - `TDataSinkType` 增加 `ICEBERG_MERGE_SINK = 18`。
  - 新增 `struct TIcebergMergeSink`。
  - `struct TDataSink` 增加 `iceberg_merge_sink` 字段。
- 重新生成 thrift：
  - `gensrc/build/gen_cpp/*`
  - `gensrc/build/gen_java/*`
  - `fe-common` / `be` 侧引用更新。

### 14.4 测试

- FE：`fe/fe-core/src/test/java/org/apache/doris/datasource/iceberg/IcebergDDLAndDMLPlanTest.java`
  - 新增 `explain update` 断言 merge sink + exchange。
- BE：`be/test/vec/sink/viceberg_merge_sink_test.cpp`
  - 验证 UPDATE 同时生成 DATA + POSITION_DELETES。

## 15. Thrift/Plan 接口草案

### 15.1 DataSinks.thrift

```thrift
enum TDataSinkType {
    ...
    ICEBERG_DELETE_SINK = 17,
    ICEBERG_MERGE_SINK = 18,
}

struct TIcebergMergeSink {
    // table write side (same as Iceberg table sink)
    1: optional string db_name
    2: optional string tb_name
    3: optional string schema_json
    4: optional map<i32, string> partition_specs_json
    5: optional i32 partition_spec_id
    6: optional list<TSortField> sort_fields
    7: optional PlanNodes.TFileFormatType file_format
    8: optional PlanNodes.TFileCompressType compression_type
    9: optional string output_path
    10: optional string original_output_path
    11: optional map<string, string> hadoop_config
    12: optional Types.TFileType file_type
    13: optional list<Types.TNetworkAddress> broker_addresses

    // delete side (position delete only)
    20: optional TFileContent delete_type
    21: optional string table_location
    22: optional i32 partition_spec_id_for_delete
    23: optional string partition_data_json_for_delete
}

struct TDataSink {
    1: optional TDataSinkType type
    ...
    28: optional TIcebergDeleteSink iceberg_delete_sink
    29: optional TIcebergMergeSink iceberg_merge_sink
}
```

说明：
- delete 侧 `partition_spec_id_for_delete` / `partition_data_json_for_delete` 用于兜底。
  实际 delete 仍以 row_id 中的 spec_id / partition_data 为准。

### 15.2 物理分布属性

- `PhysicalIcebergMergeSink#getRequirePhysicalProperties()`：
  - `hash(operation, row_id)`，`ShuffleType.REQUIRE`。

## 16. Merge Sink 伪代码

### 16.1 FE 计划生成（简化）

```java
// IcebergUpdateCommand
LogicalPlan scan = injectRowIdColumn(logicalQuery);
List<NamedExpression> updateCols = buildUpdateSelectItems(...);
NamedExpression op = new UnboundAlias(
    new TinyIntLiteral(IcebergMergeOperation.UPDATE_OPERATION_NUMBER), "operation");
NamedExpression rowId = findRowIdSlot(scan.getOutput()).orElse(new UnboundSlot("__DORIS_ICEBERG_ROWID_COL__"));
LogicalPlan project = new LogicalProject<>(ImmutableList.of(op, rowId, ...updateCols), scan);
return new LogicalIcebergMergeSink(..., project);
```

### 16.2 BE Merge Sink（核心逻辑）

```cpp
Status VIcebergMergeSink::write(RuntimeState* state, Block& block) {
    // 1. 找到 operation / row_id / data columns index
    auto op_col = block.get_by_name("operation");
    int row_id_idx = block.get_position_by_name("__DORIS_ICEBERG_ROWID_COL__");
    std::vector<int> data_idx = _data_column_indices;

    // 2. 构造两个 filter：delete_filter, insert_filter
    IColumn::Filter delete_filter(block.rows(), 0);
    IColumn::Filter insert_filter(block.rows(), 0);
    for (size_t i = 0; i < block.rows(); ++i) {
        auto op = op_col->get_element(i);
        if (op == DELETE || op == UPDATE_DELETE || op == UPDATE) {
            delete_filter[i] = 1;
        }
        if (op == INSERT || op == UPDATE_INSERT || op == UPDATE) {
            insert_filter[i] = 1;
        }
    }

    // 3. delete 分支：仅保留 row_id（可选保留 operation）
    if (has_any(delete_filter)) {
        Block delete_block = block.select_columns({row_id_idx});
        delete_block.filter(delete_filter);
        RETURN_IF_ERROR(_delete_sink.collect(delete_block));
    }

    // 4. insert 分支：仅保留 data 列
    if (has_any(insert_filter)) {
        Block insert_block = block.select_columns(data_idx);
        insert_block.filter(insert_filter);
        RETURN_IF_ERROR(_table_writer.write(state, insert_block));
    }
    return Status::OK();
}

Status VIcebergMergeSink::close(Status st) {
    // close table writer -> DATA commit data
    // write delete files -> POSITION_DELETES commit data
    // add to RuntimeState
}
```

### 16.3 FE 提交（RowDelta）

```java
// IcebergTransaction.finishMerge()
List<TIcebergCommitData> data = filterByContent(DATA);
List<TIcebergCommitData> deletes = filterByContent(POSITION_DELETES);
List<DataFile> dataFiles = IcebergWriterHelper.convertToWriterResult(..., data).dataFiles();
List<DeleteFile> deleteFiles = IcebergWriterHelper.convertToDeleteFiles(..., deletes);
RowDelta rowDelta = transaction.newRowDelta();
rowDelta.addRows(dataFiles);
rowDelta.addDeletes(deleteFiles);
rowDelta.commit();
```

## 17. 实际落地修改清单（按落地顺序）

1) Thrift 变更 + 代码生成
   - `gensrc/thrift/DataSinks.thrift`：
     - `TDataSinkType` 增加 `ICEBERG_MERGE_SINK`
     - 新增 `TIcebergMergeSink`
     - `TDataSink` 增加 `iceberg_merge_sink`
   - 重新生成 thrift 并同步到 `gensrc/build/gen_cpp/*`、`gensrc/build/gen_java/*`
2) FE 计划层与 Translator
   - `PlanType` 增加 `LOGICAL_ICEBERG_MERGE_SINK` / `PHYSICAL_ICEBERG_MERGE_SINK`
   - 新增 `LogicalIcebergMergeSink` / `PhysicalIcebergMergeSink`
   - 新增绑定与实现规则（BindExpression + RuleType + impl rule）
   - `PhysicalPlanTranslator` 生成 `TIcebergMergeSink` + `TDataSinkType`
3) FE Update 改造为单次计划
   - `IcebergUpdateCommand`：
     - 输出 `operation + row_id + updated columns` 的单条计划
     - 走 merge sink 执行路径
4) BE Merge Sink 落地
   - 新增 `VIcebergMergeSink`（复用 table writer + delete writer）
   - 新增 `IcebergMergeSinkOperatorX`
   - pipeline 注册与创建（`pipeline_fragment_context.cpp` / `operator.cpp`）
5) FE 事务提交（RowDelta 合并）
   - `IcebergTransaction.beginMerge()/finishMerge()`：
     - commit data 拆分 DATA/DELETE
     - `RowDelta.addRows/addDeletes` 一次提交
6) 测试与验证
   - FE explain 测试（验证 merge sink + exchange）
   - BE sink 单测（UPDATE 同时产出 DATA + DELETE）
   - E2E update 验证（结果正确、delete manifest 可见）

## 18. TODO 列表（动态更新）

说明：该列表会在每项完成后更新为 `[x]`。

- [x] T01 Thrift 增加 `ICEBERG_MERGE_SINK` 与 `TIcebergMergeSink` 并生成代码
- [x] T02 新增 FE Logical/Physical merge sink + 规则 + 翻译
- [x] T03 `IcebergUpdateCommand` 单次计划改造（operation+row_id+updated cols）
- [x] T04 BE `VIcebergMergeSink` + `IcebergMergeSinkOperatorX`
- [x] T05 FE `IcebergTransaction.beginMerge/finishMerge`
- [x] T06 FE explain UT 覆盖 merge sink + exchange
- [x] T07 BE merge sink UT（UPDATE 同时产出 DATA + DELETE）
- [ ] T08 E2E update 验证（非分区/分区表）
