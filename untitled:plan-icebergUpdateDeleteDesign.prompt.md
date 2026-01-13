# Iceberg UPDATE / DELETE 设计与实现计划 (Doris)

本文档为 Iceberg 表在 Doris 中支持 SQL 层的 UPDATE 与 DELETE 的详细设计与实现方案（中文）。目标是在保留现有读取端对 delete files 的支持基础上，扩展写入端以生成并提交 Iceberg Delete 文件（并在必要时提供 rewrite 支持）。

---

## 1. 背景与目标

- 当前仓库现状（要点）：
  - 读取层：已支持 Iceberg 的 delete manifest、position delete 与 equality delete 的应用。关键类：`DeleteFileIndex`（`fe-core`）。
  - 写入/提交：已有 insert/overwrite/replace、以及部分 rewrite scaffold（`finishInsert` / `finishRewrite`）。
  - 缺口：尚无完整把 SQL DELETE/UPDATE 转换成 Iceberg delete-file 写入并 atomically commit 的端到端实现。

- 目标：实现 DELETE/UPDATE 的写入支持。首要目标是 **实现 equality delete**，随后实现 **position delete**（必要时）及基于成本的 **rewrite**。保证提交的原子性、并发冲突检测、并提供回归测试。

---

## 2. 关键文件与控制流（简要）

- 读取相关：
  - `fe-core/src/main/java/org/apache/iceberg/DeleteFileIndex.java` —— 管理 delete files 索引并提供按 data-file 匹配 delete-file 的能力。
  - `.../IcebergScan` / `IcebergSnapshot`（扫描构建 delete manifests）——读取阶段应用 delete 文件。

- 写入相关（现状）：
  - `IcebergInsertExecutor`（Nereids 路径）——处理 INSERT / INSERT OVERWRITE
  - `finishInsert` / `finishRewrite` —— 提交事务的关键位置（需扩展以纳入 delete files）。

- Planner：
  - Nereids parser -> bind -> 生成 sink（Insert/UnboundSink） -> 物理 plan -> `IcebergInsertExecutor`（需扩展支持 DELETE/UPDATE）。

---

## 3. Iceberg 语义要点（必须遵守）

- Equality deletes：基于列值写入 delete 文件，读取时对 data file 进行 equality match。优点：实现相对简单，不需 data file 内位置。
- Position deletes：基于 data-file path + position 精确删除行；需要写入端能够获取 row positions（写入时由 BE 任务掌握）。
- 原子提交：使用 Iceberg Transaction/Append/Overwrite/Rewrite API 原子性提交。并发场景需处理冲突检测与重试。
- 合并/compaction：大量 delete files 会影响读取性能，需后续实现合并或 rewrite 策略。

参考：Iceberg Spec（https://iceberg.apache.org/spec/），Position/Eq Deletes 部分。

---

## 4. 设计方案（优选与备选）

方案 A（推荐）—— 写 Delete Files（Equality 首先，后支持 Position）
- 思路：将 SQL 的 DELETE/UPDATE（需要删除的行）**输出为 Iceberg delete 文件**（equality 或 position），由事务将其附加到表的 manifest 中并提交。对于 UPDATE：可以实现为 delete + insert 的组合。
- 优点：符合 Iceberg 原生语义，IO 负担较小，原子性可靠事务保证。
- 风险：position delete 需要 BE 协同；delete files 增长需 compaction。
- 核心改动点：新增 delete writer（FE 或 BE 写），FE 在 commit 阶段把 delete files 注册到 Iceberg transaction 并 commit；planner 扩展 DELETE/UPDATE 到 Iceberg sink。

方案 B（备选）—— Rewrite Files（文件重写）
- 思路：对需要更新/删除的 data files，读取并写出新 data files，使用 RewriteFiles API 替换旧文件。
- 优点：无需产生 delete files，数据更“干净”。
- 缺点：成本高、实现复杂（需要较大的数据复制与协调）。

方案 C（混合）—— 小规模使用 delete-files，大规模使用 rewrite
- 思路：基于估算阈值（删除行数 / 影响数据大小）选择路径。实现更复杂但更高效。

---

## 5. 推荐实施步骤（分阶段，含估时）

阶段 0 — 决策与范围确认（1 day）
- 确认优先支持 equality / position / hybrid；确认 UPDATE 的语义（delete+insert 或 rewrite）。

阶段 1 — 设计 & API（1–2 days）
- 设计 FE 端 commit API 扩展（例如 `appendDeleteFiles(...)` 或通用 `appendDelete(...)`），并确定 T/Thrift 级别数据结构是否需要扩展（`TIcebergCommitData`）。

阶段 2 — Delete File 写入实现（3–6 days）
- Equality delete: 先实现 FE 指挥 BE 写（或由 FE 直接写到 object storage）的 delete file writer；定义 delete file 的 schema（字段 ids、columns、partition info）。
- Position delete（后期）：添加 BE 端位置记录逻辑，生成 (file, pos) 对应的 position delete 文件。

阶段 3 — Planner/Executor 改造（2–4 days）
- 在 Nereids / planner 中新增或扩展对 DELETE/UPDATE 的绑定，若目标为 Iceberg 则生成对应的 DML Plan（由 `IcebergDmlExecutor` 处理），下发写任务并收集 delete-files 元数据。

阶段 4 — FE 汇总 & 提交（1–2 days）
- 在 `finishInsert` / commit 的阶段，若存在 delete-files 列表，则将其以事务 API 注册到 Iceberg（确保 commit 原子性、冲突检测）。

阶段 5 — 测试与回归（1–3 weeks）
- 单元、集成、并发、性能测试（见测试矩阵）。

阶段 6 — 性能优化（后续，视情况）
- Delete files 合并/compaction、rewrite 策略、并发提交优化。

---

## 6. 受影响文件清单（初步）

- FE（必须改动）
  - `fe/fe-core/.../DeleteFileIndex.java`（验证/兼容性检查，通常无需改动主体）
  - `fe/fe-core/.../IcebergInsertExecutor.java`（新增或重构以支持 DELETE/UPDATE）
  - `fe/fe-core/.../finishInsert` / `finishRewrite` 所在类（扩展 commit 逻辑，纳入 delete files）
  - Planner / Nereids bind 类：生成适用于 Iceberg DML 的 PlanNode（`Insert`/`DML` binding 扩展）
  - 可能需新增 `IcebergDeleteWriterHelper` 或扩展现有 writer helper。

- BE（可选，若 BE 写 delete file）
  - 增加 delete-file writer（parquet/orc writer 能写 delete 格式），并在任务结束时回报 delete-file metadata 给 FE（扩展 `TIcebergCommitData`）。

- Tests
  - 新增单元测试、集成测试（regression-test/**）覆盖 equality delete / position delete / update 重写 / 并发冲突 等场景。

---

## 7. 风险与注意事项

- 并发场景：必须在 FE 侧处理 Iceberg transaction 的 conflict detection 和重试；在并发删除/插入场景下保证幂等与正确的冲突暴露。
- Dangling delete files：delete files 可能与 data files 无法匹配导致 dangling（已有 session var `ignore_iceberg_dangling_delete`，需文档化）。
- Position delete 的实现复杂性（需要 BE 输出 row position）。
- Schema evolution：保证 equality delete 字段使用 field ids，处理 schema 演化。

---

## 8. 建议的测试矩阵（最小）

- 基础功能
  - `DELETE WHERE id = x`（single-row equality delete）
  - `UPDATE ... SET col = ... WHERE ...`（实现为 delete + insert, 或 rewrite）
- Edge Cases
  - 并发 delete + insert（冲突重试）
  - Dangling delete（delete file 指向无对应 data file）
  - Position delete correctness（若实现）
- Performance
  - 大量 delete 文件对 scan 性能影响测试
  - 大规模 delete 的 rewrite vs delete-file 性能比较

---

## 9. 关键决策问题（需要你确认）

1. 优先实现哪种 delete？
   - A) **Equality**（建议）
   - B) **Position**（复杂，需要 BE 协作）
   - C) 两者同时

2. UPDATE 的首选实现语义：
   - A) **delete + insert**（建议）
   - B) **rewrite files（替换数据文件）**

3. 是否在首期实现中包含 delete-files 合并（compaction）？
   - A) 是（一起做）
   - B) 否（后续优化）


---

## 10. 后续工作（我可以继续完成的事项）

- 基于你对上面 3 个问题的回答：生成详细的变更清单（逐文件、逐方法），并给出每项改动的代码片段与 PR 切分建议。
- 编写关键单元测试模板与集成测试脚本示例。
- 如果需要，我可以直接着手实现第一个 PR（优先实现 equality delete + delete+insert update）。

---

如需我现在开始生成 PR 补丁草案（包括单元测试），请先确认上面的 3 个关键决策。