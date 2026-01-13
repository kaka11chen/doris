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

#define private public
#include "vec/sink/viceberg_merge_sink.h"
#undef private

#include <gtest/gtest.h>

#include "common/consts.h"
#include "common/object_pool.h"
#include "gen_cpp/DataSinks_types.h"
#include "gen_cpp/Types_types.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"
#include "util/runtime_profile.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_struct.h"
#include "vec/columns/column_vector.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {

class VIcebergMergeSinkTest : public testing::Test {
protected:
    static std::string test_schema_json() {
        return "{\"type\":\"struct\",\"schema-id\":0,\"fields\":["
               "{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"int\"},"
               "{\"id\":2,\"name\":\"name\",\"required\":false,\"type\":\"string\"}"
               "]}";
    }

    TDataSink build_sink() {
        TDataSink t_sink;
        t_sink.__set_type(TDataSinkType::ICEBERG_MERGE_SINK);

        TIcebergMergeSink merge_sink;
        merge_sink.__set_db_name("test_db");
        merge_sink.__set_tb_name("test_table");
        merge_sink.__set_schema_json(test_schema_json());
        merge_sink.__set_partition_spec_id(0);
        merge_sink.__set_file_format(TFileFormatType::FORMAT_PARQUET);
        merge_sink.__set_compression_type(TFileCompressType::SNAPPYBLOCK);
        merge_sink.__set_output_path("file:///tmp/iceberg_merge_sink");
        merge_sink.__set_original_output_path("file:///tmp/iceberg_merge_sink");
        merge_sink.__set_table_location("file:///tmp/iceberg_merge_sink");
        merge_sink.__set_file_type(TFileType::FILE_LOCAL);
        merge_sink.__set_delete_type(TFileContent::POSITION_DELETES);
        merge_sink.__set_partition_spec_id_for_delete(0);

        t_sink.__set_iceberg_merge_sink(merge_sink);
        return t_sink;
    }

    VExprContextSPtrs build_output_exprs(ObjectPool* pool, doris::RuntimeState* state,
                                         const doris::RowDescriptor& row_desc) {
        VExprContextSPtrs output_exprs;

        auto op_expr = std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeInt8>());
        op_expr->set_expr_name("operation");
        output_exprs.emplace_back(VExprContext::create_shared(op_expr));

        auto row_id_expr = std::make_shared<MockSlotRef>(1, std::make_shared<DataTypeStruct>(
                DataTypes {std::make_shared<DataTypeString>(), std::make_shared<DataTypeInt64>()},
                Strings {"file_path", "row_position"}));
        row_id_expr->set_expr_name(doris::BeConsts::ICEBERG_ROWID_COL);
        output_exprs.emplace_back(VExprContext::create_shared(row_id_expr));

        auto id_expr = std::make_shared<MockSlotRef>(2, std::make_shared<DataTypeInt32>());
        id_expr->set_expr_name("id");
        output_exprs.emplace_back(VExprContext::create_shared(id_expr));

        auto name_expr = std::make_shared<MockSlotRef>(3, std::make_shared<DataTypeString>());
        name_expr->set_expr_name("name");
        output_exprs.emplace_back(VExprContext::create_shared(name_expr));

        for (auto& ctx : output_exprs) {
            EXPECT_TRUE(ctx->prepare(state, row_desc).ok());
            EXPECT_TRUE(ctx->open(state).ok());
        }
        return output_exprs;
    }

    Block build_block() {
        Block block;

        auto op_col = ColumnInt8::create();
        op_col->insert_value(3);
        op_col->insert_value(2);
        op_col->insert_value(1);
        block.insert(ColumnWithTypeAndName(std::move(op_col), std::make_shared<DataTypeInt8>(),
                                           "operation"));

        auto file_path_col = ColumnString::create();
        file_path_col->insert_data("file1.parquet", 13);
        file_path_col->insert_data("file2.parquet", 13);
        file_path_col->insert_data("file3.parquet", 13);

        auto row_pos_col = ColumnInt64::create();
        row_pos_col->insert_value(10);
        row_pos_col->insert_value(20);
        row_pos_col->insert_value(30);

        Columns struct_cols;
        struct_cols.push_back(std::move(file_path_col));
        struct_cols.push_back(std::move(row_pos_col));

        auto struct_col = ColumnStruct::create(std::move(struct_cols));
        DataTypes struct_types;
        struct_types.push_back(std::make_shared<DataTypeString>());
        struct_types.push_back(std::make_shared<DataTypeInt64>());
        Strings field_names = {"file_path", "row_position"};
        auto struct_type = std::make_shared<DataTypeStruct>(struct_types, field_names);

        block.insert(ColumnWithTypeAndName(std::move(struct_col), struct_type,
                                           doris::BeConsts::ICEBERG_ROWID_COL));

        auto id_col = ColumnInt32::create();
        id_col->insert_value(1);
        id_col->insert_value(2);
        id_col->insert_value(3);
        block.insert(ColumnWithTypeAndName(std::move(id_col), std::make_shared<DataTypeInt32>(),
                                           "id"));

        auto name_col = ColumnString::create();
        name_col->insert_data("a", 1);
        name_col->insert_data("b", 1);
        name_col->insert_data("c", 1);
        block.insert(ColumnWithTypeAndName(std::move(name_col), std::make_shared<DataTypeString>(),
                                           "name"));

        return block;
    }
};

TEST_F(VIcebergMergeSinkTest, TestUpdateProducesDeleteAndInsert) {
    ObjectPool pool;
    MockRuntimeState state;

    DataTypes types {std::make_shared<DataTypeInt8>(),
                     std::make_shared<DataTypeStruct>(
                             DataTypes {std::make_shared<DataTypeString>(),
                                        std::make_shared<DataTypeInt64>()},
                             Strings {"file_path", "row_position"}),
                     std::make_shared<DataTypeInt32>(),
                     std::make_shared<DataTypeString>()};
    MockRowDescriptor row_desc(types, &pool);

    auto output_exprs = build_output_exprs(&pool, &state, row_desc);
    TDataSink t_sink = build_sink();

    auto sink = std::make_shared<VIcebergMergeSink>(t_sink, output_exprs, nullptr, nullptr);
    sink->set_skip_io(true);

    ASSERT_TRUE(sink->init_properties(&pool).ok());
    RuntimeProfile profile("iceberg_merge_sink");
    ASSERT_TRUE(sink->open(&state, &profile).ok());

    Block block = build_block();
    ASSERT_TRUE(sink->write(&state, block).ok());

    EXPECT_EQ(2, sink->_delete_row_count);
    EXPECT_EQ(2, sink->_insert_row_count);

    ASSERT_TRUE(sink->close(Status::OK()).ok());
}

} // namespace doris::vectorized
