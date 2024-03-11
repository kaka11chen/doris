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
#include "vec/runtime/vorc_transformer.h"
#include "vec/runtime/vparquet_transformer.h"

namespace doris {
namespace vectorized {

VHivePartitionWriter::VHivePartitionWriter(
        const TDataSink& t_sink, const std::string& partition_name, TUpdateMode::type update_mode,
        const VExprContextSPtrs& output_expr_ctxs, const std::vector<THiveColumn>& columns,
        WriteInfo write_info, const std::string& file_name, TFileFormatType::type file_format_type,
        THiveCompressionType::type hive_compress_type)
        : _partition_name(partition_name),
          _update_mode(update_mode),
          _vec_output_expr_ctxs(output_expr_ctxs),
          _columns(columns),
          _write_info(write_info),
          _file_name(file_name),
          _file_format_type(file_format_type),
          _hive_compress_type(hive_compress_type),
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
    //    auto& hive_table_sink = _t_sink.hive_table_sink;
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
            broker_addresses, properties, fmt::format("{}/{}", _write_info.write_path, _file_name),
            0, _file_writer_impl));
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

    switch (_file_format_type) {
    case TFileFormatType::FORMAT_PARQUET: {
        fprintf(stderr, "FORMAT_PARQUET\n");
        bool parquet_disable_dictionary = false;
        TParquetCompressionType::type parquet_compression_type;
        switch (_hive_compress_type) {
        case THiveCompressionType::SNAPPY: {
            parquet_compression_type = TParquetCompressionType::SNAPPY;
            break;
        }
        case THiveCompressionType::ZSTD: {
            parquet_compression_type = TParquetCompressionType::ZSTD;
            break;
        }
        default: {
            return Status::InternalError("Unsupported type {} with parquet", _hive_compress_type);
        }
        }
        std::vector<TParquetSchema> parquet_schemas;
        for (int i = 0; i < _columns.size(); i++) {
            //            if (_columns[i].column_type != THiveColumnType::REGULAR) {
            //                continue;
            //            }
            VExprSPtr column_expr = _vec_output_expr_ctxs[i]->root();
            TParquetSchema parquet_schema;
            parquet_schema.schema_column_name = _columns[i].name;
            parquet_schema.schema_repetition_type = TParquetRepetitionType::OPTIONAL;
            fprintf(stderr, "parquet_schema.schema_column_name: %s\n",
                    parquet_schema.schema_column_name.c_str());

            switch (column_expr->type().type) {
            case TYPE_BOOLEAN: {
                parquet_schema.schema_data_type = TParquetDataType::BOOLEAN;
                break;
            }
            case TYPE_TINYINT: {
                parquet_schema.schema_data_type = TParquetDataType::INT32;
                break;
            }
            case TYPE_SMALLINT: {
                parquet_schema.schema_data_type = TParquetDataType::INT32;
                break;
            }
            case TYPE_INT: {
                parquet_schema.schema_data_type = TParquetDataType::INT32;
                break;
            }
            case TYPE_BIGINT: {
                parquet_schema.schema_data_type = TParquetDataType::INT64;
                break;
            }
            case TYPE_FLOAT: {
                parquet_schema.schema_data_type = TParquetDataType::FLOAT;
                break;
            }
            case TYPE_DOUBLE: {
                parquet_schema.schema_data_type = TParquetDataType::DOUBLE;
                break;
            }
            case TYPE_CHAR: {
                parquet_schema.schema_data_type = TParquetDataType::FIXED_LEN_BYTE_ARRAY;
            }
            case TYPE_VARCHAR: {
                parquet_schema.schema_data_type = TParquetDataType::BYTE_ARRAY;
                break;
            }
            case TYPE_STRING: {
                parquet_schema.schema_data_type = TParquetDataType::BYTE_ARRAY;
                break;
            }

            case TYPE_BINARY: {
                parquet_schema.schema_data_type = TParquetDataType::BYTE_ARRAY;
                break;
            }

            case TYPE_DATE: {
                parquet_schema.schema_data_type = TParquetDataType::BYTE_ARRAY;
                break;
            }
            case TYPE_DATETIME: {
                parquet_schema.schema_data_type = TParquetDataType::BYTE_ARRAY;
                break;
            }
            case TYPE_DATEV2: {
                parquet_schema.schema_data_type = TParquetDataType::BYTE_ARRAY;
                break;
            }
            case TYPE_DATETIMEV2: {
                parquet_schema.schema_data_type = TParquetDataType::BYTE_ARRAY;
                break;
            }
            case TYPE_DECIMAL32: {
                parquet_schema.schema_data_type = TParquetDataType::INT32;
                break;
            }

            case TYPE_DECIMAL64: {
                parquet_schema.schema_data_type = TParquetDataType::INT64;
                break;
            }
            case TYPE_DECIMAL128I: {
                parquet_schema.schema_data_type = TParquetDataType::FIXED_LEN_BYTE_ARRAY;
                break;
            }
            case TYPE_STRUCT: {
                break;
            }
            case TYPE_ARRAY: {
                break;
            }
            case TYPE_MAP: {
                break;
            }
            default: {
                return Status::InternalError("Unsupported type {}", column_expr->type().type);
            }
            }
            parquet_schemas.emplace_back(std::move(parquet_schema));
        }
        _vfile_writer.reset(new VParquetTransformer(
                state, _file_writer_impl.get(), _vec_output_expr_ctxs, parquet_schemas,
                parquet_compression_type, parquet_disable_dictionary,
                TParquetVersion::PARQUET_2_LATEST, false));
        return _vfile_writer->open();
    }
    case TFileFormatType::FORMAT_ORC: {
        fprintf(stderr, "FORMAT_ORC\n");
        orc::CompressionKind orc_compression_type;
        switch (_hive_compress_type) {
        case THiveCompressionType::SNAPPY: {
            fprintf(stderr, "snappy\n");
            orc_compression_type = orc::CompressionKind::CompressionKind_SNAPPY;
            break;
        }
        case THiveCompressionType::ZLIB: {
            fprintf(stderr, "zlib\n");
            orc_compression_type = orc::CompressionKind::CompressionKind_ZLIB;
            break;
        }
        case THiveCompressionType::ZSTD: {
            fprintf(stderr, "zstd\n");
            orc_compression_type = orc::CompressionKind::CompressionKind_ZSTD;
            break;
        }
        default: {
            return Status::InternalError("Unsupported type {} with orc", _hive_compress_type);
        }
        }
        orc_compression_type = orc::CompressionKind::CompressionKind_ZLIB;

        std::unique_ptr<orc::Type> orc_schema = orc::createStructType();
        for (int i = 0; i < _columns.size(); i++) {
            //            if (_columns[i].column_type != THiveColumnType::REGULAR) {
            //                continue;
            //            }
            VExprSPtr column_expr = _vec_output_expr_ctxs[i]->root();
            switch (column_expr->type().type) {
            case TYPE_BOOLEAN: {
                orc_schema->addStructField(_columns[i].name,
                                           orc::createPrimitiveType(orc::BOOLEAN));
                break;
            }
            case TYPE_TINYINT: {
                orc_schema->addStructField(_columns[i].name, orc::createPrimitiveType(orc::BYTE));
                break;
            }
            case TYPE_SMALLINT: {
                orc_schema->addStructField(_columns[i].name, orc::createPrimitiveType(orc::SHORT));
                break;
            }
            case TYPE_INT: {
                orc_schema->addStructField(_columns[i].name, orc::createPrimitiveType(orc::INT));
                break;
            }
            case TYPE_BIGINT: {
                fprintf(stderr, "TYPE_BIGINT\n");
                orc_schema->addStructField(_columns[i].name, orc::createPrimitiveType(orc::LONG));
                break;
            }
            case TYPE_FLOAT: {
                orc_schema->addStructField(_columns[i].name, orc::createPrimitiveType(orc::FLOAT));
                break;
            }
            case TYPE_DOUBLE: {
                orc_schema->addStructField(_columns[i].name, orc::createPrimitiveType(orc::DOUBLE));
                break;
            }
            case TYPE_CHAR: {
                orc_schema->addStructField(_columns[i].name, orc::createPrimitiveType(orc::CHAR));
            }
            case TYPE_VARCHAR: {
                fprintf(stderr, "TYPE_VARCHAR\n");
                orc_schema->addStructField(_columns[i].name, orc::createPrimitiveType(orc::STRING));
                break;
            }
            case TYPE_STRING: {
                orc_schema->addStructField(_columns[i].name, orc::createPrimitiveType(orc::STRING));
                break;
            }

            case TYPE_BINARY: {
                orc_schema->addStructField(_columns[i].name, orc::createPrimitiveType(orc::STRING));
                break;
            }

            case TYPE_DATE: {
                orc_schema->addStructField(_columns[i].name, orc::createPrimitiveType(orc::DATE));
                break;
            }
            case TYPE_DATETIME: {
                orc_schema->addStructField(_columns[i].name,
                                           orc::createPrimitiveType(orc::TIMESTAMP));
                break;
            }
            case TYPE_DATEV2: {
                orc_schema->addStructField(_columns[i].name, orc::createPrimitiveType(orc::DATE));
                break;
            }
            case TYPE_DATETIMEV2: {
                orc_schema->addStructField(_columns[i].name,
                                           orc::createPrimitiveType(orc::TIMESTAMP));
                break;
            }
            case TYPE_DECIMAL32: {
                orc_schema->addStructField(_columns[i].name,
                                           orc::createPrimitiveType(orc::DECIMAL));
                break;
            }

            case TYPE_DECIMAL64: {
                orc_schema->addStructField(_columns[i].name,
                                           orc::createPrimitiveType(orc::DECIMAL));
                break;
            }
            case TYPE_DECIMAL128I: {
                orc_schema->addStructField(_columns[i].name,
                                           orc::createPrimitiveType(orc::DECIMAL));
                break;
            }
            case TYPE_STRUCT: {
                break;
            }
            case TYPE_ARRAY: {
                break;
            }
            case TYPE_MAP: {
                break;
            }
            default: {
                return Status::InternalError("Unsupported type {}", column_expr->type().type);
            }
            }
        }

        _vfile_writer.reset(new VOrcTransformer(state, _file_writer_impl.get(),
                                                _vec_output_expr_ctxs, std::move(orc_schema), false,
                                                orc_compression_type));
        return _vfile_writer->open();
    }
    default: {
        return Status::InternalError("Unsupported file format type {}", _file_format_type);
    }
    }
}

Status VHivePartitionWriter::write(vectorized::Block& block, vectorized::IColumn::Filter* filter) {
    fprintf(stderr, "VHivePartitionWriter::write\n");
    Block output_block;
    RETURN_IF_ERROR(_projection_and_filter_block(block, filter, &output_block));
    RETURN_IF_ERROR(_vfile_writer->write(output_block));
    _row_count += block.rows();
    _input_size_in_bytes += block.bytes();

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

    if (filter == nullptr) {
        return status;
    }

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

THivePartitionUpdate VHivePartitionWriter::get_partition_update() {
    THivePartitionUpdate hive_partition_update;
    hive_partition_update.__set_name(_partition_name);
    hive_partition_update.__set_update_mode(_update_mode);
    THiveLocationParams location;
    location.__set_write_path(_write_info.write_path);
    location.__set_target_path(_write_info.target_path);
    hive_partition_update.__set_location(location);
    hive_partition_update.__set_file_names({_file_name});
    hive_partition_update.__set_row_count(_row_count);
    hive_partition_update.__set_file_size(_input_size_in_bytes);
    return hive_partition_update;
}

} // namespace vectorized
} // namespace doris