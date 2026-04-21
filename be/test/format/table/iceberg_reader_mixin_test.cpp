// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gtest/gtest.h>

#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "format/table/iceberg_reader.h"
#include "storage/olap_common.h"

namespace doris {

// ============================================================================
// Helper: Build the Iceberg $row_id struct type.
//
// The $row_id column is Nullable<Struct<file_path:String, pos:Int64,
//   partition_spec_id:Int32, partition_data:String>>.
// ============================================================================
static DataTypePtr build_iceberg_rowid_type() {
    DataTypes field_types = {
            std::make_shared<DataTypeString>(), // file_path
            std::make_shared<DataTypeInt64>(),  // pos (row position)
            std::make_shared<DataTypeInt32>(),  // partition_spec_id
            std::make_shared<DataTypeString>(), // partition_data
    };
    Strings field_names = {"file_path", "pos", "partition_spec_id", "partition_data"};
    auto struct_type = std::make_shared<DataTypeStruct>(field_types, field_names);
    return std::make_shared<DataTypeNullable>(struct_type);
}

// Non-nullable variant.
static DataTypePtr build_iceberg_rowid_type_non_nullable() {
    DataTypes field_types = {
            std::make_shared<DataTypeString>(),
            std::make_shared<DataTypeInt64>(),
            std::make_shared<DataTypeInt32>(),
            std::make_shared<DataTypeString>(),
    };
    Strings field_names = {"file_path", "pos", "partition_spec_id", "partition_data"};
    return std::make_shared<DataTypeStruct>(field_types, field_names);
}

// Expose protected static mixin helpers through a test-only derived type.
class IcebergReaderMixinTestAccessor : public IcebergReaderMixin<ParquetReader> {
public:
    using IcebergReaderMixin<ParquetReader>::_build_iceberg_rowid_column;
    using IcebergReaderMixin<ParquetReader>::_sort_delete_rows;

    void set_delete_rows() override {}

protected:
    Status on_before_init_reader(ReaderInitContext* ctx) override { return Status::OK(); }

private:
    using DeleteFile = typename IcebergReaderMixin<ParquetReader>::DeleteFile;

    Status _read_position_delete_file(const TFileRangeDesc*, DeleteFile*) override {
        return Status::OK();
    }

    std::unique_ptr<GenericReader> _create_equality_reader(
            const TFileRangeDesc& delete_desc) override {
        return nullptr;
    }
};

// ============================================================================
// Test: _build_iceberg_rowid_column with nullable struct type
// ============================================================================
TEST(IcebergRowIdTest, BuildRowIdColumnNullable) {
    auto type = build_iceberg_rowid_type();
    std::vector<segment_v2::rowid_t> row_ids = {0, 5, 10, 15};
    MutableColumnPtr result;

    auto st = IcebergReaderMixinTestAccessor::_build_iceberg_rowid_column(
            type, "/data/file1.parquet", row_ids, 3, "{\"region\":\"us-east-1\"}", &result);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_TRUE(static_cast<bool>(result));
    EXPECT_EQ(result->size(), 4);

    // Verify it's a nullable struct.
    auto* nullable = check_and_get_column<ColumnNullable>(result.get());
    ASSERT_NE(nullable, nullptr);
    for (size_t i = 0; i < 4; i++) {
        EXPECT_FALSE(nullable->is_null_at(i)) << "Row " << i << " should not be NULL";
    }

    auto* struct_col = check_and_get_column<ColumnStruct>(nullable->get_nested_column_ptr().get());
    ASSERT_NE(struct_col, nullptr);
    ASSERT_GE(struct_col->tuple_size(), 4);

    // Verify file_path (field 0).
    auto& file_path_col = struct_col->get_column(0);
    EXPECT_EQ(file_path_col.size(), 4);
    for (size_t i = 0; i < 4; i++) {
        auto val = file_path_col.get_data_at(i);
        EXPECT_EQ(std::string(val.data, val.size), "/data/file1.parquet");
    }

    // Verify row positions (field 1).
    auto& row_pos_col = struct_col->get_column(1);
    EXPECT_EQ(row_pos_col.size(), 4);
    EXPECT_EQ(row_pos_col.get_int(0), 0);
    EXPECT_EQ(row_pos_col.get_int(1), 5);
    EXPECT_EQ(row_pos_col.get_int(2), 10);
    EXPECT_EQ(row_pos_col.get_int(3), 15);

    // Verify partition_spec_id (field 2).
    auto& spec_id_col = struct_col->get_column(2);
    EXPECT_EQ(spec_id_col.size(), 4);
    for (size_t i = 0; i < 4; i++) {
        EXPECT_EQ(spec_id_col.get_int(i), 3);
    }

    // Verify partition_data (field 3).
    auto& partition_data_col = struct_col->get_column(3);
    EXPECT_EQ(partition_data_col.size(), 4);
    for (size_t i = 0; i < 4; i++) {
        auto val = partition_data_col.get_data_at(i);
        EXPECT_EQ(std::string(val.data, val.size), "{\"region\":\"us-east-1\"}");
    }
}

// ============================================================================
// Test: _build_iceberg_rowid_column with non-nullable struct type
// ============================================================================
TEST(IcebergRowIdTest, BuildRowIdColumnNonNullable) {
    auto type = build_iceberg_rowid_type_non_nullable();
    std::vector<segment_v2::rowid_t> row_ids = {100, 200};
    MutableColumnPtr result;

    auto st = IcebergReaderMixinTestAccessor::_build_iceberg_rowid_column(type, "/data/file2.orc",
                                                                          row_ids, 7, "", &result);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_TRUE(static_cast<bool>(result));
    EXPECT_EQ(result->size(), 2);

    auto* struct_col = check_and_get_column<ColumnStruct>(result.get());
    ASSERT_NE(struct_col, nullptr);

    // Verify row positions.
    auto& row_pos_col = struct_col->get_column(1);
    EXPECT_EQ(row_pos_col.get_int(0), 100);
    EXPECT_EQ(row_pos_col.get_int(1), 200);

    // Verify spec id.
    auto& spec_id_col = struct_col->get_column(2);
    EXPECT_EQ(spec_id_col.get_int(0), 7);
    EXPECT_EQ(spec_id_col.get_int(1), 7);
}

// ============================================================================
// Test: _build_iceberg_rowid_column with empty row_ids
// ============================================================================
TEST(IcebergRowIdTest, BuildRowIdColumnEmptyRows) {
    auto type = build_iceberg_rowid_type();
    std::vector<segment_v2::rowid_t> row_ids;
    MutableColumnPtr result;

    auto st = IcebergReaderMixinTestAccessor::_build_iceberg_rowid_column(
            type, "/data/empty.parquet", row_ids, 0, "", &result);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_TRUE(static_cast<bool>(result));
    EXPECT_EQ(result->size(), 0);
}

// ============================================================================
// Test: _build_iceberg_rowid_column with large row count
// ============================================================================
TEST(IcebergRowIdTest, BuildRowIdColumnLargeBatch) {
    auto type = build_iceberg_rowid_type();
    const size_t num_rows = 10000;
    std::vector<segment_v2::rowid_t> row_ids(num_rows);
    for (size_t i = 0; i < num_rows; i++) {
        row_ids[i] = static_cast<segment_v2::rowid_t>(i * 3);
    }
    MutableColumnPtr result;

    auto st = IcebergReaderMixinTestAccessor::_build_iceberg_rowid_column(
            type, "/data/large.parquet", row_ids, 1, "{}", &result);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_TRUE(static_cast<bool>(result));
    EXPECT_EQ(result->size(), num_rows);

    auto* nullable = check_and_get_column<ColumnNullable>(result.get());
    auto* struct_col = check_and_get_column<ColumnStruct>(nullable->get_nested_column_ptr().get());
    auto& row_pos_col = struct_col->get_column(1);
    // Spot check a few positions.
    EXPECT_EQ(row_pos_col.get_int(0), 0);
    EXPECT_EQ(row_pos_col.get_int(1), 3);
    EXPECT_EQ(row_pos_col.get_int(9999), 29997);
}

// ============================================================================
// Test: _build_iceberg_rowid_column null type returns error
// ============================================================================
TEST(IcebergRowIdTest, BuildRowIdColumnNullTypeError) {
    std::vector<segment_v2::rowid_t> row_ids = {0};
    MutableColumnPtr result;

    auto st = IcebergReaderMixinTestAccessor::_build_iceberg_rowid_column(
            nullptr, "/data/f.parquet", row_ids, 0, "", &result);
    ASSERT_FALSE(st.ok());
    EXPECT_TRUE(st.is<ErrorCode::INVALID_ARGUMENT>());
}

// ============================================================================
// Test: _build_iceberg_rowid_column null output column returns error
// ============================================================================
TEST(IcebergRowIdTest, BuildRowIdColumnNullOutputError) {
    auto type = build_iceberg_rowid_type();
    std::vector<segment_v2::rowid_t> row_ids = {0};

    auto st = IcebergReaderMixinTestAccessor::_build_iceberg_rowid_column(type, "/data/f.parquet",
                                                                          row_ids, 0, "", nullptr);
    ASSERT_FALSE(st.ok());
    EXPECT_TRUE(st.is<ErrorCode::INVALID_ARGUMENT>());
}

// ============================================================================
// Test: _build_iceberg_rowid_column with wrong type (not struct) returns error
// ============================================================================
TEST(IcebergRowIdTest, BuildRowIdColumnWrongTypeError) {
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>());
    std::vector<segment_v2::rowid_t> row_ids = {0};
    MutableColumnPtr result;

    auto st = IcebergReaderMixinTestAccessor::_build_iceberg_rowid_column(type, "/data/f.parquet",
                                                                          row_ids, 0, "", &result);
    ASSERT_FALSE(st.ok());
}

// ============================================================================
// Test: _build_iceberg_rowid_column with struct having < 4 fields returns error
// ============================================================================
TEST(IcebergRowIdTest, BuildRowIdColumnTooFewFieldsError) {
    DataTypes field_types = {
            std::make_shared<DataTypeString>(),
            std::make_shared<DataTypeInt64>(),
    };
    Strings field_names = {"file_path", "pos"};
    auto struct_type = std::make_shared<DataTypeStruct>(field_types, field_names);
    auto type = std::make_shared<DataTypeNullable>(struct_type);

    std::vector<segment_v2::rowid_t> row_ids = {0};
    MutableColumnPtr result;

    auto st = IcebergReaderMixinTestAccessor::_build_iceberg_rowid_column(type, "/data/f.parquet",
                                                                          row_ids, 0, "", &result);
    ASSERT_FALSE(st.ok());
}

// ============================================================================
// Test: _build_iceberg_rowid_column partition_spec_id 0 and empty partition_data
// ============================================================================
TEST(IcebergRowIdTest, BuildRowIdColumnZeroSpecIdEmptyPartition) {
    auto type = build_iceberg_rowid_type();
    std::vector<segment_v2::rowid_t> row_ids = {42};
    MutableColumnPtr result;

    auto st = IcebergReaderMixinTestAccessor::_build_iceberg_rowid_column(
            type, "/warehouse/table/data/00000-0.parquet", row_ids, 0, "", &result);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(result->size(), 1);

    auto* nullable = check_and_get_column<ColumnNullable>(result.get());
    auto* struct_col = check_and_get_column<ColumnStruct>(nullable->get_nested_column_ptr().get());

    auto& spec_id_col = struct_col->get_column(2);
    EXPECT_EQ(spec_id_col.get_int(0), 0);

    auto& partition_data_col = struct_col->get_column(3);
    auto val = partition_data_col.get_data_at(0);
    EXPECT_EQ(std::string(val.data, val.size), "");
}

// ============================================================================
// Test: _sort_delete_rows merges multiple pre-sorted delete row arrays
// ============================================================================
TEST(IcebergSortDeleteTest, SingleArray) {
    std::vector<int64_t> arr1 = {1, 2, 3, 4, 5};
    std::vector<std::vector<int64_t>*> arrays = {&arr1};
    std::vector<int64_t> result;

    IcebergReaderMixinTestAccessor::_sort_delete_rows(arrays, arr1.size(), result);
    ASSERT_EQ(result.size(), 5);
    EXPECT_EQ(result[0], 1);
    EXPECT_EQ(result[1], 2);
    EXPECT_EQ(result[2], 3);
    EXPECT_EQ(result[3], 4);
    EXPECT_EQ(result[4], 5);
}

TEST(IcebergSortDeleteTest, TwoArraysMerged) {
    // Both inputs must be pre-sorted.
    std::vector<int64_t> arr1 = {1, 5, 10};
    std::vector<int64_t> arr2 = {3, 7, 12};
    std::vector<std::vector<int64_t>*> arrays = {&arr1, &arr2};
    std::vector<int64_t> result;

    IcebergReaderMixinTestAccessor::_sort_delete_rows(arrays, arr1.size() + arr2.size(), result);
    // Merged: 1, 3, 5, 7, 10, 12
    ASSERT_EQ(result.size(), 6);
    EXPECT_EQ(result[0], 1);
    EXPECT_EQ(result[1], 3);
    EXPECT_EQ(result[2], 5);
    EXPECT_EQ(result[3], 7);
    EXPECT_EQ(result[4], 10);
    EXPECT_EQ(result[5], 12);
}

TEST(IcebergSortDeleteTest, EmptyInput) {
    std::vector<std::vector<int64_t>*> arrays;
    std::vector<int64_t> result;

    IcebergReaderMixinTestAccessor::_sort_delete_rows(arrays, 0, result);
    EXPECT_TRUE(result.empty());
}

TEST(IcebergSortDeleteTest, SingleElementArrays) {
    std::vector<int64_t> arr1 = {100};
    std::vector<int64_t> arr2 = {50};
    std::vector<std::vector<int64_t>*> arrays = {&arr1, &arr2};
    std::vector<int64_t> result;

    IcebergReaderMixinTestAccessor::_sort_delete_rows(arrays, 2, result);
    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result[0], 50);
    EXPECT_EQ(result[1], 100);
}

// ============================================================================
// Block expand/shrink logic tests
// These replicate IcebergReaderMixin::_expand_block_if_need and
// _shrink_block_if_need without needing a real reader.
// ============================================================================
class IcebergBlockExpandShrinkTest : public ::testing::Test {
protected:
    // Replicate expand logic: add columns to a block, update name→idx map
    static Status expand_block(Block* block,
                               const std::vector<ColumnWithTypeAndName>& expand_columns,
                               std::unordered_map<std::string, uint32_t>* col_name_to_block_idx) {
        std::set<std::string> names;
        auto block_names = block->get_names();
        names.insert(block_names.begin(), block_names.end());
        for (auto col : expand_columns) {
            col.column->assume_mutable()->clear();
            if (names.contains(col.name)) {
                return Status::InternalError("Wrong expand column '{}'", col.name);
            }
            names.insert(col.name);
            (*col_name_to_block_idx)[col.name] = static_cast<uint32_t>(block->columns());
            block->insert(col);
        }
        return Status::OK();
    }

    // Replicate shrink logic: remove columns from a block, update name→idx map
    static Status shrink_block(Block* block, const std::vector<std::string>& expand_col_names,
                               std::unordered_map<std::string, uint32_t>* col_name_to_block_idx) {
        std::set<size_t> positions_to_erase;
        for (const std::string& expand_col : expand_col_names) {
            if (!col_name_to_block_idx->contains(expand_col)) {
                return Status::InternalError("Wrong erase column '{}'", expand_col);
            }
            positions_to_erase.emplace((*col_name_to_block_idx)[expand_col]);
        }
        block->erase(positions_to_erase);
        for (const std::string& expand_col : expand_col_names) {
            col_name_to_block_idx->erase(expand_col);
        }
        return Status::OK();
    }
};

TEST_F(IcebergBlockExpandShrinkTest, ExpandAddsColumns) {
    Block block;
    block.insert({ColumnInt32::create(), std::make_shared<DataTypeInt32>(), "id"});
    std::unordered_map<std::string, uint32_t> idx_map = {{"id", 0}};

    std::vector<ColumnWithTypeAndName> expand_cols = {
            {ColumnString::create(), std::make_shared<DataTypeString>(), "eq_col1"},
            {ColumnInt64::create(), std::make_shared<DataTypeInt64>(), "eq_col2"},
    };

    auto st = expand_block(&block, expand_cols, &idx_map);
    ASSERT_TRUE(st.ok());
    EXPECT_EQ(block.columns(), 3);
    EXPECT_EQ(idx_map["eq_col1"], 1);
    EXPECT_EQ(idx_map["eq_col2"], 2);
}

TEST_F(IcebergBlockExpandShrinkTest, ExpandDuplicateColumnError) {
    Block block;
    block.insert({ColumnInt32::create(), std::make_shared<DataTypeInt32>(), "id"});
    std::unordered_map<std::string, uint32_t> idx_map = {{"id", 0}};

    std::vector<ColumnWithTypeAndName> expand_cols = {
            {ColumnInt32::create(), std::make_shared<DataTypeInt32>(), "id"},
    };

    auto st = expand_block(&block, expand_cols, &idx_map);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("Wrong expand column"), std::string::npos);
}

TEST_F(IcebergBlockExpandShrinkTest, ExpandNoColumns) {
    Block block;
    block.insert({ColumnInt32::create(), std::make_shared<DataTypeInt32>(), "id"});
    std::unordered_map<std::string, uint32_t> idx_map = {{"id", 0}};

    auto st = expand_block(&block, {}, &idx_map);
    ASSERT_TRUE(st.ok());
    EXPECT_EQ(block.columns(), 1);
}

TEST_F(IcebergBlockExpandShrinkTest, ShrinkRemovesColumns) {
    Block block;
    block.insert({ColumnInt32::create(), std::make_shared<DataTypeInt32>(), "id"});
    block.insert({ColumnString::create(), std::make_shared<DataTypeString>(), "eq_col1"});
    block.insert({ColumnInt64::create(), std::make_shared<DataTypeInt64>(), "eq_col2"});
    std::unordered_map<std::string, uint32_t> idx_map = {{"id", 0}, {"eq_col1", 1}, {"eq_col2", 2}};

    auto st = shrink_block(&block, {"eq_col1", "eq_col2"}, &idx_map);
    ASSERT_TRUE(st.ok());
    EXPECT_EQ(block.columns(), 1);
    EXPECT_EQ(block.get_by_position(0).name, "id");
    EXPECT_FALSE(idx_map.contains("eq_col1"));
    EXPECT_FALSE(idx_map.contains("eq_col2"));
    EXPECT_TRUE(idx_map.contains("id"));
}

TEST_F(IcebergBlockExpandShrinkTest, ShrinkColumnNotFoundError) {
    Block block;
    block.insert({ColumnInt32::create(), std::make_shared<DataTypeInt32>(), "id"});
    std::unordered_map<std::string, uint32_t> idx_map = {{"id", 0}};

    auto st = shrink_block(&block, {"nonexistent"}, &idx_map);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("Wrong erase column"), std::string::npos);
}

TEST_F(IcebergBlockExpandShrinkTest, ExpandThenShrinkRoundTrip) {
    Block block;
    block.insert({ColumnInt32::create(), std::make_shared<DataTypeInt32>(), "id"});
    block.insert({ColumnString::create(), std::make_shared<DataTypeString>(), "name"});
    std::unordered_map<std::string, uint32_t> idx_map = {{"id", 0}, {"name", 1}};

    // Expand with 2 equality delete columns
    std::vector<ColumnWithTypeAndName> expand_cols = {
            {ColumnInt64::create(), std::make_shared<DataTypeInt64>(), "del_col1"},
            {ColumnInt64::create(), std::make_shared<DataTypeInt64>(), "del_col2"},
    };
    ASSERT_TRUE(expand_block(&block, expand_cols, &idx_map).ok());
    EXPECT_EQ(block.columns(), 4);

    // Shrink back
    ASSERT_TRUE(shrink_block(&block, {"del_col1", "del_col2"}, &idx_map).ok());
    EXPECT_EQ(block.columns(), 2);
    EXPECT_EQ(idx_map.size(), 2);
    EXPECT_EQ(block.get_by_position(0).name, "id");
    EXPECT_EQ(block.get_by_position(1).name, "name");
}

TEST_F(IcebergBlockExpandShrinkTest, ShrinkNoColumns) {
    Block block;
    block.insert({ColumnInt32::create(), std::make_shared<DataTypeInt32>(), "id"});
    std::unordered_map<std::string, uint32_t> idx_map = {{"id", 0}};

    auto st = shrink_block(&block, {}, &idx_map);
    ASSERT_TRUE(st.ok());
    EXPECT_EQ(block.columns(), 1);
}

} // namespace doris
