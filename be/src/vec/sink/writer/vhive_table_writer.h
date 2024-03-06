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

#pragma once

#include <stddef.h>

#include <string>
#include <vector>

#include "common/status.h"
#include "vec/sink/writer/async_result_writer.h"
#include "vec/sink/writer/vhive_partition_writer.h"

namespace doris {
namespace vectorized {

class Block;

class VHiveTableWriter final : public AsyncResultWriter {
public:
    VHiveTableWriter(const TDataSink& t_sink, const VExprContextSPtrs& output_exprs);

    ~VHiveTableWriter() = default;

    Status init_properties(ObjectPool* pool);

    Status open(RuntimeState* state, RuntimeProfile* profile) override;

    Status write(vectorized::Block& block) override;

    Status close_idle_writers();

    Status close(Status) override;

private:
    std::shared_ptr<VHivePartitionWriter> _create_partition_writer(vectorized::Block& block,
                                                                   int position, int bucket_number);
    std::vector<std::string> _create_partition_values(vectorized::Block& block, int position);
    std::string _to_partition_value(const TypeDescriptor& type_desc,
                                    const ColumnWithTypeAndName& partition_column, int position);
    //    std::string _make_part_name(const std::vector<std::string>& columns,
    //                                const std::vector<std::string>& values);
    //    std::string _escape_path_name(const std::string& path);

    std::string _get_file_extension(TFileFormatType::type file_format_type,
                                    TWriteCompressionType::type write_compress_type);

    std::string _compute_file_name(int bucketNumber);

    //    const std::vector<TExpr>& _t_output_expr;
    //    VExprContextSPtrs _output_vexpr_ctxs;
    const TDataSink* _t_sink;
    RuntimeState* _state = nullptr;
    RuntimeProfile* _profile = nullptr;
    VExprContextSPtrs _data_col_expr_ctxs;
    std::vector<int> _partition_columns_input_index;
    VExprContextSPtrs _partition_col_expr_ctxs;
    std::unordered_map<std::string, std::shared_ptr<VHivePartitionWriter>> _partitions_to_writers;
    bool _overwrite = false;
};
} // namespace vectorized
} // namespace doris
