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

#include "vhive_partition_writer.h"

#include "io/file_factory.h"
#include "runtime/runtime_state.h"
#include "vec/core/materialize_block.h"
#include "vec/runtime/vparquet_transformer.h"

namespace doris {
namespace vectorized {

VHivePartitionWriter::VHivePartitionWriter(
        const TDataSink& t_sink, const std::string& partition_key, TUpdateMode::type update_mode,
        const VExprContextSPtrs& output_expr_ctxs,
        const std::vector<std::string>& data_column_names,
        const VExprContextSPtrs& data_col_expr_ctxs, WriteInfo write_info,
        TFileFormatType::type file_format_type, TWriteCompressionType::type write_compress_type)
        : _t_sink(t_sink),
          _partition_key(partition_key),
          _vec_output_expr_ctxs(output_expr_ctxs),
          _data_column_names(data_column_names),
          _data_col_expr_ctxs(data_col_expr_ctxs),
          _write_info(write_info),
          _file_format_type(file_format_type),
          _write_compress_type(write_compress_type),
          _file_writer_impl(nullptr),
          _vfile_writer(nullptr),
          _pool(nullptr)

{}

Status VHivePartitionWriter::close(Status status) {
    fprintf(stderr, "VHivePartitionWriter::close\n");
    if (status == Status::OK()) {
        RETURN_IF_ERROR(_vfile_writer->close());
    } else {
        //file_system.delete(_write_info.write_path);
    }
    return Status::OK();
}

Status VHivePartitionWriter::open(RuntimeState* state, RuntimeProfile* profile) {
    fprintf(stderr, "VHivePartitionWriter::open\n");
    THiveTableSink hive_table_sink = _t_sink.hive_table_sink;
    std::vector<TNetworkAddress> broker_addresses;
    std::map<std::string, std::string> properties;
    properties.insert({"fs.defaultFS", "hdfs://HDFS8000871"});
    properties.insert({"dfs.nameservices", "HDFS8000871"});
    properties.insert({"dfs.ha.namenodes.HDFS8000871", "nn1,nn2"});
    properties.insert({"dfs.namenode.rpc-address.HDFS8000871.nn1", "172.21.0.32:4007"});
    properties.insert({"dfs.namenode.rpc-address.HDFS8000871.nn2", "172.21.0.44:4007"});
    properties.insert(
            {"dfs.client.failover.proxy.provider.HDFS8000871",
             "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"});
    //    RETURN_IF_ERROR(FileFactory::create_file_writer(
    //            FileFactory::convert_storage_type(TStorageBackendType::HDFS), state->exec_env(),
    //            broker_addresses, properties,
    //            "hdfs://HDFS8000871/usr/hive/warehouse/cqtest.db/write_par_test_table/" +
    //                    _partition_key + "test.parquet",
    //            0, _file_writer_impl));

    RETURN_IF_ERROR(FileFactory::create_file_writer(
            FileFactory::convert_storage_type(TStorageBackendType::HDFS), state->exec_env(),
            broker_addresses, properties, _write_info.write_path, 0, _file_writer_impl));
    //    std::vector<TParquetSchema> parquet_schemas;
    //    {
    //        TParquetSchema parquet_schema;
    //        parquet_schema.schema_column_name = "user_id";
    //        parquet_schema.schema_data_type = TParquetDataType::INT64;
    //        parquet_schema.schema_repetition_type = TParquetRepetitionType::OPTIONAL;
    //        parquet_schemas.emplace_back(std::move(parquet_schema));
    //    }
    //    {
    //        TParquetSchema parquet_schema;
    //        parquet_schema.schema_column_name = "city";
    //        parquet_schema.schema_data_type = TParquetDataType::BYTE_ARRAY;
    //        parquet_schema.schema_repetition_type = TParquetRepetitionType::OPTIONAL;
    //        parquet_schemas.emplace_back(std::move(parquet_schema));
    //    }
    std::vector<TParquetSchema> parquet_schemas;
    for (int i = 0; i < _data_col_expr_ctxs.size(); i++) {
        VExprSPtr column_expr = _data_col_expr_ctxs[i]->root();
        TParquetSchema parquet_schema;
        parquet_schema.schema_column_name = _data_column_names[i];
        parquet_schema.schema_repetition_type = column_expr->is_nullable()
                                                        ? TParquetRepetitionType::OPTIONAL
                                                        : TParquetRepetitionType::REQUIRED;

        switch (column_expr->type().type) {
        case TYPE_BOOLEAN: {
            parquet_schema.schema_data_type = TParquetDataType::BOOLEAN;
        }
        case TYPE_TINYINT: {
        }
        case TYPE_SMALLINT: {
        }
        case TYPE_INT: {
            parquet_schema.schema_data_type = TParquetDataType::INT32;
        }
        case TYPE_BIGINT: {
            parquet_schema.schema_data_type = TParquetDataType::INT64;
        }
        case TYPE_FLOAT: {
            parquet_schema.schema_data_type = TParquetDataType::FLOAT;
        }
        case TYPE_DOUBLE: {
            parquet_schema.schema_data_type = TParquetDataType::DOUBLE;
        }
        case TYPE_CHAR:
        case TYPE_VARCHAR: {
        }
        case TYPE_DATE: {
        }
        case TYPE_DATETIME: {
        }
        case TYPE_DECIMAL32: {
        }

        case TYPE_DECIMAL64: {
        }
        case TYPE_DECIMAL128I: {
        }
        case TYPE_STRUCT: {
        }
        case TYPE_ARRAY: {
        }
        case TYPE_MAP: {
        }
        default: {
        }
        }
    }
    bool parquet_disable_dictionary = false;

    switch (_file_format_type) {
    case TFileFormatType::FORMAT_PARQUET: {
        TParquetCompressionType::type parquet_compression_type;
        switch (_write_compress_type) {
        case TWriteCompressionType::SNAPPY: {
            parquet_compression_type = TParquetCompressionType::SNAPPY;
            break;
        }
        case TWriteCompressionType::ZSTD: {
            parquet_compression_type = TParquetCompressionType::ZSTD;
            break;
        }
        default: {
            break;
        }
        }
        _vfile_writer.reset(new VParquetTransformer(
                state, _file_writer_impl.get(), _vec_output_expr_ctxs, parquet_schemas,
                parquet_compression_type, parquet_disable_dictionary,
                TParquetVersion::PARQUET_2_LATEST, false));
        break;
    }
    case TFileFormatType::FORMAT_ORC: {
        break;
    }
    default: {
        break;
    }
    }

    return _vfile_writer->open();
}

Status VHivePartitionWriter::write(vectorized::Block& block, vectorized::IColumn::Filter* filter) {
    fprintf(stderr, "VHivePartitionWriter::write\n");
    Block output_block;
    RETURN_IF_ERROR(_projection_and_filter_block(block, filter, &output_block));
    RETURN_IF_ERROR(_vfile_writer->write(output_block));

    //    auto num_rows = output_block.rows();
    //    for (int i = 0; i < num_rows; ++i) {
    //        RETURN_IF_ERROR(_insert_row(output_block, i));
    //    }
    return Status::OK();
}

Status VHivePartitionWriter::_projection_and_filter_block(doris::vectorized::Block& input_block,
                                                          const vectorized::IColumn::Filter* filter,
                                                          doris::vectorized::Block* output_block) {
    Status status = Status::OK();
    if (input_block.rows() == 0) {
        return status;
    }
    RETURN_IF_ERROR(vectorized::VExprContext::get_output_block_after_execute_exprs(
            _vec_output_expr_ctxs, input_block, output_block));
    materialize_block_inplace(*output_block);

    std::vector<uint32_t> columns_to_filter;
    int column_to_keep = input_block.columns();
    columns_to_filter.resize(column_to_keep);
    for (uint32_t i = 0; i < column_to_keep; ++i) {
        columns_to_filter[i] = i;
    }

    Block::filter_block_internal(output_block, columns_to_filter, *filter);

    return status;
}

Status VHivePartitionWriter::_projection_block(doris::vectorized::Block& input_block,
                                               doris::vectorized::Block* output_block) {
    Status status = Status::OK();
    if (input_block.rows() == 0) {
        return status;
    }
    RETURN_IF_ERROR(vectorized::VExprContext::get_output_block_after_execute_exprs(
            _vec_output_expr_ctxs, input_block, output_block));
    materialize_block_inplace(*output_block);
    return status;
}

Status VHivePartitionWriter::commit() {
    return Status::OK();
}

Status VHivePartitionWriter::rollback() {
    return Status::OK();
}

} // namespace vectorized
} // namespace doris