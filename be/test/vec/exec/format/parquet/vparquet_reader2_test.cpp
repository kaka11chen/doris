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

#include <cctz/time_zone.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stddef.h>

#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "exec/olap_common.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_system.h"
#include "io/fs/local_file_system.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/timezone_utils.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_lazy.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/exec/format/parquet/vparquet_reader2.h"

namespace doris::vectorized {
class VExprContext;

class ParquetReader2Test : public testing::Test {
public:
    ParquetReader2Test() {}
};

TEST_F(ParquetReader2Test, BasicReadTest) {
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;

    t_table_desc.id = 0;
    t_table_desc.tableType = TTableType::HIVE_TABLE;
    t_table_desc.numCols = 0;
    t_table_desc.numClusteringCols = 0;
    t_desc_table.tableDescriptors.push_back(t_table_desc);
    t_desc_table.__isset.tableDescriptors = true;

    // init boolean and numeric slot
    std::vector<std::string> numeric_types = {"boolean_col", "tinyint_col", "smallint_col",
                                              "int_col",     "bigint_col",  "float_col",
                                              "double_col"};
    for (int i = 0; i < numeric_types.size(); i++) {
        TSlotDescriptor tslot_desc;
        {
            tslot_desc.id = i;
            tslot_desc.parent = 0;
            TTypeDesc type;
            {
                TTypeNode node;
                node.__set_type(TTypeNodeType::SCALAR);
                TScalarType scalar_type;
                scalar_type.__set_type(TPrimitiveType::type(i + 2));
                node.__set_scalar_type(scalar_type);
                type.types.push_back(node);
            }
            tslot_desc.slotType = type;
            tslot_desc.columnPos = 0;
            tslot_desc.byteOffset = 0;
            tslot_desc.nullIndicatorByte = 0;
            tslot_desc.nullIndicatorBit = -1;
            tslot_desc.colName = numeric_types[i];
            tslot_desc.slotIdx = 0;
            tslot_desc.isMaterialized = true;
            t_desc_table.slotDescriptors.push_back(tslot_desc);
        }
    }

    t_desc_table.__isset.slotDescriptors = true;
    {
        // TTupleDescriptor dest
        TTupleDescriptor t_tuple_desc;
        t_tuple_desc.id = 0;
        t_tuple_desc.byteSize = 16;
        t_tuple_desc.numNullBytes = 0;
        t_tuple_desc.tableId = 0;
        t_tuple_desc.__isset.tableId = true;
        t_desc_table.tupleDescriptors.push_back(t_tuple_desc);
    }
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    static_cast<void>(DescriptorTbl::create(&obj_pool, t_desc_table, &desc_tbl));

    auto slot_descs = desc_tbl->get_tuple_descriptor(0)->slots();
    auto local_fs = io::global_local_filesystem();
    io::FileReaderSPtr reader;
//    static_cast<void>(local_fs->open_file(
//            "./be/test/exec/test_data/parquet_scanner/type-decoder.parquet", &reader));

    ASSERT_TRUE(local_fs->open_file("/mnt/disk2/chenqi/clion/doris/be/test/exec/test_data/parquet_scanner/type-decoder.parquet", &reader));

    cctz::time_zone ctz;
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);
    auto tuple_desc = desc_tbl->get_tuple_descriptor(0);
    std::vector<std::string> column_names;
    for (int i = 0; i < slot_descs.size(); i++) {
        column_names.push_back(slot_descs[i]->col_name());
    }
    TFileScanRangeParams scan_params;
    TFileRangeDesc scan_range;
    {
        scan_range.start_offset = 0;
        scan_range.size = 1000;
    }
    auto p_reader =
            std::make_unique<ParquetReader2>(nullptr, scan_params, scan_range, 992, &ctz, nullptr, nullptr);
    p_reader->set_file_reader(reader);
    RuntimeState runtime_state((TQueryGlobals()));
    runtime_state.set_desc_tbl(desc_tbl);

    std::unordered_map<std::string, ColumnValueRangeType> colname_to_value_range;
    ASSERT_TRUE(p_reader->open());
    ASSERT_TRUE(p_reader->init_reader(column_names, nullptr));
    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            partition_columns;
    std::unordered_map<std::string, VExprContextSPtr> missing_columns;
    static_cast<void>(p_reader->set_fill_columns(partition_columns, missing_columns));
    BlockUPtr block = Block::create_unique();
    for (const auto& slot_desc : tuple_desc->slots()) {
        auto data_type =
                vectorized::DataTypeFactory::instance().create_data_type(slot_desc->type(), true);
        MutableColumnPtr data_column = data_type->create_column();
        block->insert(
                ColumnWithTypeAndName(std::move(data_column), data_type, slot_desc->col_name()));
    }
    bool eof = false;
    size_t read_row = 0;
    static_cast<void>(p_reader->get_next_block(block.get(), &read_row, &eof));
    for (int i = 0; auto& col : block->get_columns_with_type_and_name()) {
        ASSERT_EQ(col.column->size(), 10);
        ASSERT_EQ(col.name, numeric_types[i++]);
//        ColumnPtr column;
//        DataTypePtr type;
//        String name;
//        // 检测是否是 LazyColumn
//        if (is_column<LazyColumn>(*col.column)) {
//            fprintf(stdout, "column is LazyColumn\n");
//            // 如果是 LazyColumn，进行脱壳操作
//            const LazyColumn& lazy_column = assert_cast<const LazyColumn&>(*col.column);
//            column = lazy_column.get_column();
//            type = assert_cast<const DataTypeLazy*>(col.type.get())->get_nested_type();
//            name = col.name;
//        } else {
//            column = col.column;
//            type = col.type;
//            name = col.name;
//        }
//
//        // 根据列的类型进行处理
//        if (is_int32(type)) {
//            fprintf(stdout, "column is int32\n");
//            for (size_t i = 0; i < column->size(); ++i) {
//                int value = column->get_int(i);
//                // 对整型列进行处理
//                ASSERT_LE(value, 0);  // 假设值大于等于0
//            }
//        } else if (is_float(type)) {
//            fprintf(stdout, "column is float\n");
////            for (size_t i = 0; i < column->size(); ++i) {
////                float value = column
////                // 对浮点型列进行处理
////                ASSERT_GT(value, 0.0f);  // 假设值大于0.0
////            }
//        } else if (is_string(type)) {
//            fprintf(stdout, "column is string\n");
//            for (size_t i = 0; i < column->size(); ++i) {
//                const auto& value = column->get_data_at(i);
//                // 对字符串列进行处理
//                ASSERT_FALSE(value.empty());  // 假设字符串不为空
//            }
//        }
    }
//    EXPECT_TRUE(eof);
}

} // namespace doris::vectorized