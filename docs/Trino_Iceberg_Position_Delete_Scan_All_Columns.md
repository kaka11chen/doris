# Trino Iceberg Position Delete 为什么需要扫描所有字段

## 1. 问题现象

从执行计划可以看到，即使 DELETE 语句只需要 `country = 'US'` 这一个字段做过滤，但 ScanFilterProject 却读取了所有字段：

```sql
DELETE FROM iceberg.cqtest.user_actions_iceberg WHERE country = 'US';
```

**执行计划片段**：
```
Fragment 2 [SOURCE]
    ScanFilterProject[table = iceberg:cqtest.user_actions_iceberg$data, 
                      filterPredicate = ("country" = VARCHAR 'US')]
        Layout: [user_id:bigint, action_type:varchar, action_date:date, 
                 operation:tinyint, 
                 field:row(_file varchar, _pos bigint, partition_spec_id integer, partition_data varchar), 
                 insert_from_update:tinyint]
        
        -- 所有字段都被读取，但大部分在 Project 阶段被设置为 null
        user_id := CAST(null AS bigint)
        action_type := CAST(null AS varchar)
        action_date := CAST(null AS date)
        operation := TINYINT '2'  -- DELETE 操作
        insert_from_update := TINYINT '0'
        country := 4:country:varchar  -- 过滤条件
        field := -2147483647:$row_id:row(...)  -- Position delete 需要的行位置信息
```

**关键观察**：
- ✅ 扫描阶段读取了所有字段：`user_id`, `action_type`, `action_date`, `country`
- ✅ 但 Project 阶段大部分字段被设置为 `null`
- ✅ 只有 `operation` 和 `field`（row_id）有实际值
- ❓ **为什么不能只扫描 `country` 字段？**

## 2. Position Delete 机制回顾

### 2.1 Position Delete 的工作原理

Position Delete 通过**文件路径 + 行位置**来标记要删除的行：

```
Position Delete File 结构：
┌─────────────────────────────────────────┐
│ file_path (varchar)                      │  ← 数据文件路径
│ pos (bigint)                            │  ← 行在文件中的位置（从0开始）
│ partition_spec_id (integer)             │  ← 分区规范ID
│ partition_data (varchar)                 │  ← 分区数据
└─────────────────────────────────────────┘
```

### 2.2 DELETE 操作的执行流程

```
DELETE FROM table WHERE country = 'US'
    ↓
1. 扫描数据文件，找到所有 country = 'US' 的行
    ↓
2. 对于每一行，需要获取：
   - file_path: 数据文件路径
   - pos: 行在文件中的位置（行号）
   - partition_spec_id: 分区规范ID
   - partition_data: 分区数据
    ↓
3. 将这些信息写入 Position Delete File
    ↓
4. 提交时，将 Delete File 添加到表的元数据中
```

## 3. 为什么必须扫描所有字段？

### 3.1 根本原因：Parquet 列式存储的限制

**Parquet 文件是列式存储格式**：

```
Parquet 文件结构：
┌─────────────────────────────────────────┐
│ Row Group 0                             │
│   Column Chunk: user_id [0, 1, 2, ...]  │
│   Column Chunk: action_type [a, b, c]    │
│   Column Chunk: action_date [2024-01-01]│
│   Column Chunk: country [US, CN, ...]   │
│   ...                                    │
└─────────────────────────────────────────┘
```

**关键问题**：
- Parquet 中**行位置（row position）不是显式存储的**
- 行位置是通过**读取所有列并计算行数**来确定的
- 要确定第 N 行的位置，需要：
  1. 读取所有列的 Column Chunk
  2. 解析每一列的数据
  3. 通过行数计数来确定位置

### 3.2 行位置计算的复杂性

在 Parquet 中，行位置的计算方式：

```java
// 伪代码：Parquet 行位置计算
int rowPosition = 0;
for (RowGroup rowGroup : file.getRowGroups()) {
    for (int i = 0; i < rowGroup.getRowCount(); i++) {
        // 需要读取所有列才能确定这是完整的一行
        if (matchesFilter(row)) {
            // 找到匹配的行，rowPosition 就是该行的位置
            return rowPosition;
        }
        rowPosition++;
    }
}
```

**问题**：
- 即使只需要 `country` 字段做过滤，Parquet 读取器仍然需要：
  1. 读取所有列的 Column Chunk（因为行是跨列的）
  2. 解析每一行来确定行位置
  3. 应用过滤条件

### 3.3 Trino 的实现细节

从执行计划可以看到，Trino 的实现流程：

```
ScanFilterProject
    ↓
1. Scan: 读取 Parquet 文件的所有列
   - 必须读取所有列才能确定行位置
   - Parquet 的列式存储使得行位置是隐式的
    ↓
2. Filter: 应用 WHERE 条件 (country = 'US')
   - 过滤掉不匹配的行
    ↓
3. Project: 只保留需要的字段
   - user_id, action_type, action_date → null（不需要）
   - country → 保留（用于过滤）
   - field ($row_id) → 保留（Position delete 需要）
   - operation → 设置为 2（DELETE 操作）
```

**为什么 Project 阶段设置为 null？**
- 虽然扫描时读取了所有列，但 Position Delete 只需要 `$row_id`（包含 file_path 和 pos）
- 其他列的数据在写入 Delete File 时不需要，所以设置为 null
- **但这不能避免扫描阶段的 I/O 开销**

## 4. 与 Equality Delete 的对比

### 4.1 Equality Delete 的优势

Equality Delete 通过**列值**来标识要删除的行，而不是位置：

```
Equality Delete File 结构：
┌─────────────────────────────────────────┐
│ equality_column_1 (varchar)             │  ← 用于匹配的列值
│ equality_column_2 (bigint)              │
│ ...                                     │
└─────────────────────────────────────────┘
```

**优势**：
- ✅ 只需要读取用于匹配的列（equality columns）
- ✅ 可以做列裁剪优化
- ✅ I/O 开销更小

**示例**：
```sql
-- 如果使用 Equality Delete（基于 user_id）
DELETE FROM table WHERE country = 'US';
-- 只需要读取 country 列（如果 country 是 equality column）
```

### 4.2 Position Delete 的劣势

**劣势**：
- ❌ 必须读取所有列来确定行位置
- ❌ 无法做列裁剪
- ❌ I/O 开销大

**为什么 Iceberg 仍然使用 Position Delete？**
1. **通用性**：Position Delete 适用于所有表结构，不需要指定 equality columns
2. **精确性**：通过位置删除，不会因为列值重复导致误删
3. **简单性**：不需要维护 equality columns 的索引

## 5. 为什么不能优化？

### 5.1 列裁剪的限制

**理论上**：
- 如果只需要 `country` 字段，应该可以只读取 `country` 列的 Column Chunk

**实际上**：
- Position Delete 需要 `$row_id`，而 `$row_id` 包含 `pos`（行位置）
- 行位置需要读取所有列才能确定
- **因此列裁剪无法应用**

### 5.2 Parquet 格式的限制

Parquet 的列式存储特性：

```
Parquet 文件：
Row 0: [user_id=1, action_type='click', country='US']
Row 1: [user_id=2, action_type='view', country='CN']
Row 2: [user_id=3, action_type='click', country='US']

存储方式：
Column Chunk (user_id):     [1, 2, 3]
Column Chunk (action_type): ['click', 'view', 'click']
Column Chunk (country):     ['US', 'CN', 'US']
```

**问题**：
- 行位置（0, 1, 2）不是显式存储的
- 需要通过读取所有列并计数来确定
- 即使只关心 `country` 列，也需要读取其他列来确定行位置

### 5.3 Trino 的优化尝试

从执行计划可以看到，Trino 已经做了一些优化：

1. **Project 阶段设置为 null**：
   - 虽然扫描时读取了所有列，但 Project 阶段将不需要的列设置为 null
   - 减少了后续处理的数据量

2. **Filter 下推**：
   - `filterPredicate = ("country" = VARCHAR 'US')` 在 Scan 阶段就应用
   - 可以减少需要处理的行数

**但无法避免**：
- ❌ 扫描阶段的 I/O 开销（必须读取所有列）
- ❌ 解析所有列的开销（需要确定行位置）

## 6. 与 Doris Unique Key MOW 的对比

### 6.1 Doris 的优势

Doris Unique Key MOW 使用**隐藏列**机制：

```
Doris DELETE 流程：
DELETE FROM table WHERE country = 'US'
    ↓
1. 查询计划阶段：只选择需要的列
   - country (用于过滤)
   - __DORIS_DELETE_SIGN__ (设置为 1)
   - Key 列（用于定位）
    ↓
2. 扫描阶段：只读取需要的列
   - 可以做列裁剪优化
   - I/O 开销小
    ↓
3. 写入阶段：写入包含 DELETE_SIGN=1 的新行
```

**关键差异**：
- ✅ Doris 的 `__DORIS_ROWID_COL__` 是**显式存储的列**
- ✅ 不需要通过读取所有列来确定行位置
- ✅ 可以做列裁剪优化

### 6.2 Iceberg Position Delete 的限制

```
Iceberg DELETE 流程：
DELETE FROM table WHERE country = 'US'
    ↓
1. 查询计划阶段：需要 $row_id（包含 pos）
    ↓
2. 扫描阶段：必须读取所有列
   - 因为 pos 需要通过读取所有列来确定
   - 无法做列裁剪
    ↓
3. 写入阶段：写入 Position Delete File
```

**关键差异**：
- ❌ Iceberg 的 `pos` 是**隐式的**（通过行计数确定）
- ❌ 必须读取所有列才能确定行位置
- ❌ 无法做列裁剪优化

## 7. 优化建议

### 7.1 使用 Equality Delete（如果可能）

如果表有明确的 equality columns，使用 Equality Delete：

```sql
-- 创建表时指定 equality columns
CREATE TABLE ... WITH (
    'delete.mode' = 'equality',
    'equality.delete.columns' = 'user_id,country'
);
```

**优势**：
- ✅ 只需要读取 equality columns
- ✅ 可以做列裁剪
- ✅ I/O 开销小

### 7.2 使用分区删除（如果可能）

如果 WHERE 条件只涉及分区列，使用分区删除：

```sql
-- 如果 country 是分区列
DELETE FROM table WHERE country = 'US';
-- 可以删除整个分区，不需要扫描数据文件
```

**优势**：
- ✅ 不需要扫描数据文件
- ✅ 直接删除分区
- ✅ 性能最优

### 7.3 定期 Compaction

定期运行 Compaction 来合并 Delete Files：

```sql
-- 合并 Delete Files 到数据文件
ALTER TABLE table EXECUTE OPTIMIZE;
```

**优势**：
- ✅ 减少 Delete Files 的数量
- ✅ 提高查询性能
- ✅ 减少元数据开销

### 7.4 批量删除

对于大量删除，考虑批量操作：

```sql
-- 批量删除，减少 Delete Files 数量
DELETE FROM table WHERE country = 'US' AND date < '2024-01-01';
```

**优势**：
- ✅ 减少 Delete Files 数量
- ✅ 提高后续查询性能

## 8. 总结

### 8.1 核心原因

Trino Iceberg Position Delete 需要扫描所有字段的根本原因：

1. **Parquet 列式存储**：行位置是隐式的，需要通过读取所有列来确定
2. **Position Delete 机制**：需要 `pos`（行位置）来标识要删除的行
3. **格式限制**：Parquet 格式不支持只读取部分列来确定行位置

### 8.2 与 Doris 的对比

| 特性 | Doris Unique Key MOW | Iceberg Position Delete |
|------|---------------------|-------------------------|
| 行标识 | 显式存储 `__DORIS_ROWID_COL__` | 隐式通过行计数确定 `pos` |
| 列裁剪 | ✅ 支持 | ❌ 不支持 |
| I/O 开销 | 小（只读需要的列） | 大（必须读所有列） |
| 适用场景 | 内表，可控的存储格式 | 外表，标准 Parquet 格式 |

### 8.3 设计权衡

**Iceberg Position Delete 的设计权衡**：
- ✅ **通用性**：适用于所有表结构，不需要指定 equality columns
- ✅ **精确性**：通过位置删除，不会误删
- ❌ **性能**：必须读取所有列，I/O 开销大

**Doris Unique Key MOW 的设计权衡**：
- ✅ **性能**：可以做列裁剪，I/O 开销小
- ✅ **可控性**：可以自定义存储格式，显式存储行位置
- ❌ **通用性**：只适用于 Doris 内表，不适用于标准 Parquet

## 9. 参考资料

1. [Trino Iceberg Connector Documentation](https://trino.io/docs/current/connector/iceberg.html)
2. [Apache Iceberg Position Delete Specification](https://iceberg.apache.org/spec/#position-delete-files)
3. [Parquet File Format Specification](https://parquet.apache.org/docs/file-format/)
4. [Doris Unique Key MOW 隐藏列处理机制](./Doris_Unique_Key_Hidden_Columns.md)
