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

#include <gen_cpp/PlanNodes_types.h>

#include "common/status.h"
#include "io/fs/file_writer.h"
#include "vec/runtime/vfile_format_transformer.h"
#include "vec/sink/writer/async_result_writer.h"

namespace doris {
namespace vectorized {

class Block;

struct WriteInfo {
    std::string write_path;
    std::string target_path;
};

class VHivePartitionWriter {
public:
    VHivePartitionWriter(const TDataSink& t_sink, const std::string& partition_name,
                         TUpdateMode::type update_mode, const VExprContextSPtrs& output_expr_ctxs,
                         const std::vector<THiveColumn>& columns, WriteInfo write_info,
                         const std::string& file_name, TFileFormatType::type file_format_type,
                         THiveCompressionType::type hive_compress_type);

    Status init_properties(ObjectPool* pool) {
        _pool = pool;
        return Status::OK();
    }

    Status open(RuntimeState* state, RuntimeProfile* profile);

    Status write(vectorized::Block& block, IColumn::Filter* filter = nullptr);

    Status close(Status);

    Status commit();

    Status rollback();

    THivePartitionUpdate get_partition_update();

    int64_t written_len() { return _vfile_writer->written_len(); };

private:
    Status _projection_block(doris::vectorized::Block& input_block,
                             doris::vectorized::Block* output_block);

    Status _projection_and_filter_block(doris::vectorized::Block& input_block,
                                        const vectorized::IColumn::Filter* filter,
                                        doris::vectorized::Block* output_block);

    std::string _path;

    std::string _partition_name;

    TUpdateMode::type _update_mode;

    size_t _row_count = 0;
    size_t _input_size_in_bytes = 0;

    const VExprContextSPtrs& _vec_output_expr_ctxs;

    const std::vector<THiveColumn>& _columns;
    WriteInfo _write_info;
    std::string _file_name;
    TFileFormatType::type _file_format_type;
    THiveCompressionType::type _hive_compress_type;

    // If the result file format is plain text, like CSV, this _file_writer is owned by this FileResultWriter.
    // If the result file format is Parquet, this _file_writer is owned by _parquet_writer.
    std::unique_ptr<doris::io::FileWriter> _file_writer_impl;
    // convert block to parquet/orc/csv fomrat
    std::unique_ptr<VFileFormatTransformer> _vfile_writer;

    ObjectPool* _pool;

    //    bool _is_active;
};
} // namespace vectorized
} // namespace doris
