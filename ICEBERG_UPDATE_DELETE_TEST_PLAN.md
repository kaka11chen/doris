# Iceberg Update/Delete 单元测试与回归测试方案

本文档基于以下两个提交的功能改动，整理需要补充的单元测试与回归测试覆盖面：

- 3152708c9cfe14bb0872f12c202c34e938c84284：iceberg_update_delete_poc
- b40c8eb6de8b3339aff1cf277526ebace7133031：position delete

## 1. 代码改动要点梳理

### 1.1 Update/Delete 逻辑（Nereids/FE）
- UPDATE 使用单扫描 + merge sink：扫描满足条件行，输出 `operation` + `row_id` + 更新后的数据列，
  通过 `IcebergMergeSink` 写入 data files 与 position delete files。
- DELETE 使用 position delete：扫描满足条件行，输出 `operation` + `row_id`，
  通过 `IcebergDeleteSink` 写入 delete files。
- `needIcebergRowId` 仅在 explain/执行时通过 `ConnectContext` 打开，确保 row_id 注入到 Scan 输出。
- explain 侧对 `LogicalIcebergDeleteSink`/`LogicalIcebergMergeSink` 进行计划生成。
- 物理阶段要求 exchange（hash shuffle）仅基于 `row_id`，不再依赖 `operation`。

### 1.2 BE 写入侧（Vec）
- `VIcebergDeleteSink` 依赖 row_id 列生成 position delete。
- `VIcebergMergeSink` 依赖 `operation` 列判定 delete/insert 分支，
  并依赖 row_id 列生成 delete 分支的 position delete。
- merge sink 的输出列布局需要能够识别 `operation` 与 `__DORIS_ICEBERG_ROWID_COL__`。

## 2. 单元测试方案（FE/BE）

### 2.1 FE 侧（Nereids 计划）
文件建议集中在：
`fe/fe-core/src/test/java/org/apache/doris/datasource/iceberg/IcebergDDLAndDMLPlanTest.java`

#### 2.1.1 逻辑计划层（Explain Plan）
- **Update 逻辑 project 输出校验**
  - 断言 `LogicalProject` 中包含 `operation` 与 `__DORIS_ICEBERG_ROWID_COL__`。
  - 断言 `LogicalIcebergMergeSink` 作为 explain plan root。
- **Delete 逻辑 project 输出校验**
  - 断言 `LogicalProject` 中包含 `operation` 与 `__DORIS_ICEBERG_ROWID_COL__`。
  - 断言 `LogicalIcebergDeleteSink` 作为 explain plan root。

#### 2.1.2 物理计划层（Physical Plan）
- **Update 物理 sink 类型**
  - 断言 `PhysicalIcebergMergeSink` 存在。
- **Delete 物理 sink 类型**
  - 断言 `PhysicalIcebergDeleteSink` 存在。
- **Exchange/Distribute 仅基于 row_id**
  - 断言物理计划根节点是 `PhysicalDistribute`。
  - 断言 `DistributionSpecHash.getOrderedShuffledColumns()` 仅包含 row_id 的 ExprId。
  - 断言不包含 `operation` 的 ExprId。

#### 2.1.3 Explain 文本校验（稳定性：contains）
- **Update explain**
  - `ExplainLevel.DISTRIBUTED_PLAN` 或 `OPTIMIZED_PLAN`。
  - 断言包含 `EXCHANGE` 与 `ICEBERG MERGE SINK`。
  - 可选：断言包含 `__DORIS_ICEBERG_ROWID_COL__`。
- **Delete explain**
  - 断言包含 `EXCHANGE` 与 `ICEBERG DELETE SINK`。

#### 2.1.4 约束与异常场景
- **非 Iceberg 表 update/delete**
  - 断言抛错（只能用于 Iceberg）。

### 2.2 BE 侧（Vec）
若已有 gtest 框架，建议新增（或补齐）：

1) `VIcebergMergeSink::_prepare_output_layout`
- 输入表达式列表缺少 `operation` 时应报错。
- 缺少 `row_id` 时应报错。
- 输出列数量与 schema 列数不匹配时应报错。

2) `VIcebergDeleteSink` row_id 处理
- 校验 row_id 列缺失时的报错路径。

注：若 BE 不便新增 UT，可在回归测试侧用 explain/执行覆盖。

## 3. 回归测试方案（regression-test）

目录要求：新增用例放到
`regression-test/suites/external_table_p0/iceberg/ddl/`

### 3.1 测试准备与公共前置
- 参照 `regression-test/suites/external_table_p0/iceberg/` 现有用例写法。
- 建议复用 REST catalog（localhost:8181）与 `enableIcebergTest` 开关。
- 表属性使用 `format-version = 2`，保证 update 可用。

### 3.2 用例组织建议
新增文件示例：
- `test_iceberg_update_delete_explain.groovy`
- `test_iceberg_update_delete_exchange.groovy`
- `test_iceberg_update_delete_errors.groovy`

### 3.3 具体用例建议

#### A. Explain / Exchange 回归
目标：确保 exchange 存在且基于 row_id。

步骤：
1. 创建 catalog、db、table（format-version=2）。
2. 插入少量数据。
3. `EXPLAIN UPDATE ...` 与 `EXPLAIN DELETE ...`。
4. 断言 explain 文本包含：
   - `EXCHANGE`
   - `ICEBERG MERGE SINK` / `ICEBERG DELETE SINK`
   - `__DORIS_ICEBERG_ROWID_COL__`（可选）

#### B. Update 基本功能回归
目标：验证 UPDATE 语义、数据正确性、数据可见性。

用例：
- 单列更新（带 where）
- 多列更新（带 where）
- 表达式更新（`score = score + 1`）
- 全表更新（无 where）

校验：
- `SELECT` 校验行值变化。
- `SELECT COUNT(*)` 保持一致（更新不改变行数）。

#### C. Delete Position Delete 回归
目标：验证 position delete 生效。

用例：
- `DELETE ... WHERE ...` 删除部分数据。
- 再次查询应返回 0 行（或行数减少）。
- 可选：查询 metadata table 验证 delete files 生成。
  - 如 `SELECT * FROM ${catalog}.${db}.${table}.delete_files` 或 `snapshots`。

#### D. 错误与边界回归
用例：
- 非 Iceberg 表执行 UPDATE/DELETE -> 预期报错。
- format-version=1 表执行 UPDATE -> 预期报错（若已实现校验）。
- 更新不可见列 / 非法列 -> 预期报错。

### 3.4 覆盖映射（回归 -> 代码路径）
- Update explain -> `UpdateCommand` + `IcebergUpdateCommand` + `ExplainCommand`
- Delete explain -> `DeleteFromCommand` + `ExplainCommand`
- Exchange -> `PhysicalIceberg*Sink.getRequirePhysicalProperties` + `RequestPropertyDeriver`
- 执行 -> `IcebergMergeSink` / `IcebergDeleteSink` + BE `VIcebergMergeSink` / `VIcebergDeleteSink`

## 4. 执行建议

### 4.1 单元测试
- `fe/fe-core`：执行 `IcebergDDLAndDMLPlanTest`
- 若 BE 有 gtest：执行对应 test binary

### 4.2 回归测试
- 在 `regression-test` 目录运行 p0 套件
- 单独跑 `external_table_p0/iceberg/ddl` 新增用例

## 5. 风险与注意事项
- explain 输出格式可能变化，断言尽量用 `contains`。
- update/delete 依赖 row_id 注入开关，测试中需确保 `ConnectContext.needIcebergRowId` 正确切换。
- position delete 依赖 Iceberg v2，表属性必须正确设置。
