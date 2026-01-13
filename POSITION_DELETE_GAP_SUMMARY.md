# Position Delete 缺失分析 - 简明版

## 🎯 核心问题

**当前实现了前 60% 的流程，但缺少关键的 BE 端写入部分（后 40%）**

## ✅ 已完成部分

### FE 端（Frontend）
1. ✅ `DeleteFromCommand` → 路由到 `IcebergDeleteCommand`
2. ✅ `IcebergDeleteCommand.completeQueryPlan()` → 添加 `$row_id` 列到查询
3. ✅ `IcebergMetadataColumn` → 定义 `$row_id` 结构
4. ✅ `IcebergDeleteExecutor` → 管理 DELETE 执行流程
5. ✅ `IcebergTransaction.beginDelete()` / `finishDelete()` → 事务管理
6. ✅ `IcebergWriterHelper.convertToDeleteFiles()` → 转换为 Iceberg DeleteFile

### BE 端（Backend）
1. ✅ `IcebergTableReader` → 读取 Iceberg 数据
2. ✅ `set_current_file_info()` → 设置当前文件信息
3. ✅ `_append_row_id_column()` → 生成 `$row_id` 列

## ❌ 核心缺失 - BE 端写入链路

### 问题 1: 没有 Delete File 写入器

```
当前流程:
IcebergTableReader → 生成 $row_id 列 → ❓❓❓ → 发送回 FE

缺失:
IcebergTableReader → 生成 $row_id 列 
                            ↓
                     ❌ VIcebergDeleteSink
                            ↓
                     ❌ VIcebergDeleteFileWriter
                            ↓
                     写入 position_delete.parquet
                            ↓
                     生成 TIcebergCommitData
                            ↓
                     发送回 FE
```

**需要实现的文件**:
- `be/src/vec/sink/viceberg_delete_sink.h`
- `be/src/vec/sink/viceberg_delete_sink.cpp`
- `be/src/vec/sink/writer/viceberg_delete_file_writer.h`
- `be/src/vec/sink/writer/viceberg_delete_file_writer.cpp`

### 问题 2: FE 端没有 DeleteSink 计划节点

```
当前流程:
IcebergDeleteCommand → InsertIntoTableCommand → ❓ 如何知道是 DELETE？

应该的流程:
IcebergDeleteCommand → completeQueryPlan() 
                            ↓
                       添加 UnboundIcebergDeleteSink
                            ↓
                       转换为 PhysicalIcebergDeleteSink
                            ↓
                       BE 端生成 VIcebergDeleteSink
```

**需要实现的文件**:
- `fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/physical/PhysicalIcebergDeleteSink.java`
- `fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/logical/UnboundIcebergDeleteSink.java`

### 问题 3: Thrift 定义可能不完整

需要检查 `TIcebergCommitData` 是否有 `positions` 字段:

```thrift
struct TIcebergCommitData {
    // ... 已有字段 ...
    
    // ❓ 需要添加：
    12: optional list<i64> positions;  // 行位置列表
}
```

## 🔍 详细调用链对比

### 当前实现（60%）

```
┌─── FE 端 ───┐
│ DELETE SQL  │
│      ↓      │
│ IcebergDeleteCommand
│      ↓      │
│ 添加 $row_id 到查询
│      ↓      │
│ InsertIntoTableCommand  ← ⚠️  这里有问题
└─────────────┘
       ↓
┌─── BE 端 ───┐
│ IcebergTableReader
│      ↓      │
│ 生成 $row_id 列
│      ↓      │
│     ❌      │  ← 没有 Sink 来接收和处理
└─────────────┘
```

### 应该的实现（100%）

```
┌─── FE 端 ───┐
│ DELETE SQL  │
│      ↓      │
│ IcebergDeleteCommand
│      ↓      │
│ 添加 $row_id 到查询
│      ↓      │
│ 添加 UnboundIcebergDeleteSink  ← 新增
│      ↓      │
│ 转换为 PhysicalIcebergDeleteSink ← 新增
└─────────────┘
       ↓
┌─── BE 端 ───┐
│ IcebergTableReader
│      ↓      │
│ 生成 $row_id 列
│      ↓      │
│ VIcebergDeleteSink  ← 新增
│      ↓      │
│ VIcebergDeleteFileWriter ← 新增
│      ↓      │
│ 写入 .parquet
│      ↓      │
│ TIcebergCommitData
│      ↓      │
│ 发送回 FE    │
└─────────────┘
       ↓
┌─── FE 端 ───┐
│ IcebergTransaction
│      ↓      │
│ convertToDeleteFiles()
│      ↓      │
│ RowDelta.addDeletes()
│      ↓      │
│ transaction.commit()
└─────────────┘
```

## 📋 最小实现清单

要让 Position Delete 真正工作，必须实现以下组件：

### 优先级 P0（阻塞）

1. **VIcebergDeleteFileWriter** (BE)
   - 接收包含 `$row_id` 的 Block
   - 按 `file_path` 分组
   - 写入 Parquet 格式：`(file_path: STRING, pos: BIGINT)`
   - 生成 `TIcebergCommitData`

2. **VIcebergDeleteSink** (BE)
   - 作为 Sink 节点接收数据
   - 调用 `VIcebergDeleteFileWriter`
   - 发送 commit data 回 FE

3. **PhysicalIcebergDeleteSink** (FE)
   - 物理计划节点
   - 转换为 BE 端的 `VIcebergDeleteSink`

4. **修改 IcebergDeleteCommand**
   - 在 `completeQueryPlan()` 末尾添加 `UnboundIcebergDeleteSink`
   - 传递 `DeleteCommandContext`

### 优先级 P1（高）

5. **TIcebergCommitData 扩展**
   - 添加 `list<i64> positions` 字段
   - 确保所有必要的元数据都能传递

6. **集成测试**
   - 端到端测试整个流程
   - 验证 delete file 正确生成
   - 验证事务正确提交

## 🚀 实现策略

### 方案 A：完整实现（推荐）

按照上面的清单，完整实现所有缺失组件。

**优点**：架构清晰，易于维护
**缺点**：工作量较大

### 方案 B：简化实现（快速 POC）

在 FE 端直接写 delete file，跳过 BE 端的 Sink。

```java
// 在 IcebergDeleteExecutor.doBeforeCommit() 中：
// 1. 从查询结果中获取 $row_id 数据
// 2. 在 FE 端用 Java 写 Parquet 文件
// 3. 直接生成 DeleteFile 对象
// 4. 提交事务
```

**优点**：快速验证，无需修改 BE
**缺点**：
- 性能差（所有数据都要传到 FE）
- 不可扩展
- 不符合 Doris 架构

## 📊 当前状态总结

| 组件 | 状态 | 完成度 |
|-----|------|--------|
| FE - 命令路由 | ✅ 完成 | 100% |
| FE - 查询计划 | ✅ 完成 | 100% |
| FE - 元数据列定义 | ✅ 完成 | 100% |
| BE - 数据扫描 | ✅ 完成 | 100% |
| BE - $row_id 生成 | ✅ 完成 | 100% |
| **BE - Delete Sink** | ❌ **未实现** | **0%** |
| **BE - Delete Writer** | ❌ **未实现** | **0%** |
| **FE - Delete Sink 节点** | ❌ **未实现** | **0%** |
| FE - 事务管理 | ✅ 完成 | 100% |
| FE - Commit 处理 | ✅ 完成 | 100% |

**总体完成度：约 60%**

**阻塞项：BE 端的 Sink 和 Writer（0%）**

## 🔧 快速诊断

运行以下 SQL 并观察日志：

```sql
SET enable_nereids_planner = true;
SET enable_profile = true;

-- 查看执行计划
EXPLAIN DELETE FROM iceberg_table WHERE id = 1;

-- 实际执行
DELETE FROM iceberg_table WHERE id = 1;
```

**应该看到但当前看不到的**：
- ❌ PhysicalIcebergDeleteSink 节点
- ❌ BE 日志中的 "Writing position delete file"
- ❌ 生成的 delete file 路径

**当前能看到的**：
- ✅ IcebergDeleteCommand 被调用
- ✅ $row_id 列在查询计划中
- ✅ IcebergTableReader 生成 $row_id

## 📝 结论

**Position Delete 的"骨架"已经搭好，但缺少"肌肉"（BE 端写入器）**。

要完成实现，核心是：
1. 实现 BE 端的 `VIcebergDeleteFileWriter`
2. 实现 BE 端的 `VIcebergDeleteSink`  
3. 实现 FE 端的 Sink 计划节点
4. 将它们连接起来

这些组件实现后，整个 Position Delete 流程就能端到端运行了。
