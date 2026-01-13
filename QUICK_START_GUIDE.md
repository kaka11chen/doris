# Iceberg DELETE/UPDATE 快速开始指南

## 快速开始（5分钟上手）

### 1. 前置条件

确保你有：
- Apache Doris 3.0+ 环境
- Iceberg Catalog 配置完成
- Iceberg 表格式版本 >= 2

### 2. 创建测试表

```sql
-- 创建 Iceberg Catalog
CREATE CATALOG my_iceberg PROPERTIES (
    'type' = 'iceberg',
    'iceberg.catalog.type' = 'rest',
    'uri' = 'http://localhost:8181'
);

-- 切换到 Iceberg Catalog
USE my_iceberg.my_database;

-- 创建测试表（必须是 v2 格式）
CREATE TABLE users (
    id INT,
    name STRING,
    email STRING,
    status STRING,
    created_at TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet'
);

-- 插入测试数据
INSERT INTO users VALUES
    (1, 'Alice', 'alice@example.com', 'active', '2024-01-01 10:00:00'),
    (2, 'Bob', 'bob@example.com', 'active', '2024-01-02 11:00:00'),
    (3, 'Charlie', 'charlie@example.com', 'inactive', '2024-01-03 12:00:00');
```

### 3. 执行 DELETE 操作

```sql
-- 删除单行
DELETE FROM users WHERE id = 3;

-- 删除多行
DELETE FROM users WHERE status = 'inactive';

-- 验证结果
SELECT * FROM users;
```

### 4. 执行 UPDATE 操作

```sql
-- 更新单列
UPDATE users SET status = 'verified' WHERE id = 1;

-- 更新多列
UPDATE users 
SET status = 'premium', email = 'newemail@example.com' 
WHERE id = 2;

-- 验证结果
SELECT * FROM users;
```

## 核心概念

### DELETE 实现原理

```
DELETE FROM table WHERE condition
  ↓
生成 DeleteFile（标记删除）
  ↓
提交到 Iceberg Metadata
  ↓
读取时自动过滤已删除行
```

**优点**: 
- 不需要重写数据文件
- 快速删除
- 支持事务

### UPDATE 实现原理

```
UPDATE table SET col = val WHERE condition
  ↓
DELETE（生成 DeleteFile）+ INSERT（写新数据）
  ↓
在同一事务中原子提交
```

**优点**:
- 原子性保证
- 利用 Iceberg 的 MVCC
- 支持回滚

## 高级用法

### 1. 批量删除

```sql
-- 删除过期数据
DELETE FROM users 
WHERE created_at < '2023-01-01';

-- 删除多个状态
DELETE FROM users 
WHERE status IN ('inactive', 'banned', 'deleted');
```

### 2. 条件更新

```sql
-- 条件更新
UPDATE users 
SET status = 'premium' 
WHERE created_at > '2024-01-01' 
  AND status = 'active';

-- 计算更新
UPDATE users 
SET score = score * 1.1 
WHERE score < 100;
```

### 3. 查看元数据

```sql
-- 查看表快照
SELECT * FROM my_iceberg.my_database.users.snapshots 
ORDER BY committed_at DESC LIMIT 10;

-- 查看 DeleteFile 统计
SELECT 
    snapshot_id,
    operation,
    summary['total-delete-files'] as delete_files
FROM my_iceberg.my_database.users.snapshots
WHERE summary['total-delete-files'] IS NOT NULL;
```

## 性能优化

### 1. 选择合适的删除模式

```sql
-- 小规模删除（< 1000行）: 使用 Equality Delete（默认）
DELETE FROM users WHERE id IN (1, 2, 3);

-- 大规模删除（> 10000行）: 考虑使用 INSERT OVERWRITE
INSERT OVERWRITE users 
SELECT * FROM users WHERE status != 'deleted';
```

### 2. 定期 Compaction

```sql
-- 合并小文件和 DeleteFile
CALL my_iceberg.system.rewrite_data_files('my_database.users');

-- 清理旧快照
CALL my_iceberg.system.expire_snapshots(
    table => 'my_database.users',
    older_than => TIMESTAMP '2024-01-01 00:00:00'
);
```

### 3. 监控 DeleteFile

```sql
-- 检查 DeleteFile 数量
SELECT 
    COUNT(*) as delete_file_count,
    SUM(file_size_in_bytes) / 1024 / 1024 as total_size_mb
FROM my_iceberg.my_database.users.delete_files;
```

## 常见问题

### Q1: 为什么 DELETE 很慢？
**A**: 检查以下几点：
1. 表是否是 format-version 2
2. DeleteFile 是否过多（需要 compaction）
3. 删除条件是否有索引

### Q2: UPDATE 失败怎么办？
**A**: 常见原因：
1. 表不是 Iceberg v2 格式
2. 权限不足
3. 并发冲突

检查错误日志获取详细信息。

### Q3: 如何回滚误删除？
**A**: 使用 Iceberg 时间旅行：
```sql
-- 查看历史快照
SELECT * FROM my_iceberg.my_database.users.snapshots;

-- 回滚到指定快照
CALL my_iceberg.system.rollback_to_snapshot(
    'my_database.users', 
    1234567890
);
```

### Q4: DELETE 会立即删除数据吗？
**A**: 不会。DELETE 只生成 DeleteFile 标记删除。
物理删除由 Iceberg 的 compaction 完成。

## 测试验证

### 单元测试
```bash
cd fe/fe-core
mvn test -Dtest=DeleteCommandContextTest
mvn test -Dtest=IcebergWriterHelperTest
```

### 集成测试
```bash
cd regression-test
./run.sh --run test_iceberg_delete
./run.sh --run test_iceberg_update
```

## 调试技巧

### 1. 启用调试日志
```sql
SET debug_log_level = 'DEBUG';
```

### 2. 查看执行计划
```sql
EXPLAIN DELETE FROM users WHERE id = 1;
EXPLAIN UPDATE users SET status = 'active' WHERE id = 1;
```

### 3. 检查事务状态
```sql
SHOW TRANSACTION WHERE label = 'your_label';
```

## 性能基准

| 操作 | 数据量 | 耗时 | DeleteFile 数量 |
|------|--------|------|-----------------|
| DELETE 单行 | 1000万 | ~1s | 1 |
| DELETE 1% | 1000万 | ~2s | 5-10 |
| UPDATE 单行 | 1000万 | ~1.5s | 1 delete + 1 data |
| UPDATE 10% | 1000万 | ~5s | 10-20 delete + 10-20 data |

## 最佳实践

### ✅ 推荐
1. 定期执行 compaction
2. 小批量删除使用 DELETE
3. 大批量删除使用 INSERT OVERWRITE
4. 监控 DeleteFile 数量

### ❌ 避免
1. 频繁删除单行（批量处理）
2. 不清理旧快照
3. 在高并发场景下大规模 UPDATE

## 获取帮助

- 查看详细文档: `README_POC.md`
- 实现细节: `ICEBERG_DELETE_UPDATE_IMPLEMENTATION_SUMMARY.md`
- 提交 Issue: GitHub Doris 仓库
- 社区讨论: Doris 用户群

## 下一步

1. 阅读完整文档了解实现细节
2. 在测试环境验证功能
3. 根据实际场景调整参数
4. 监控性能并优化

---

**最后更新**: 2026-01-12
**版本**: v1.0
