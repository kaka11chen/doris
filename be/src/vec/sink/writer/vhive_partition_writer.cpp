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
#include "io/fs/file_system.h"
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
        THiveCompressionType::type hive_compress_type,
        std::map<std::string, std::string>& hadoop_conf)
        : _partition_name(partition_name),
          _update_mode(update_mode),
          _vec_output_expr_ctxs(output_expr_ctxs),
          _columns(columns),
          _write_info(std::move(write_info)),
          _file_name(file_name),
          _file_format_type(file_format_type),
          _hive_compress_type(hive_compress_type),
          _hadoop_conf(hadoop_conf),
          _file_writer_impl(nullptr),
          _vfile_writer(nullptr),
          _pool(nullptr)

{}

Status VHivePartitionWriter::close(Status status) {
    if (status == Status::OK()) {
        RETURN_IF_ERROR(_vfile_writer->close());
    } else {
        RETURN_IF_ERROR(_file_writer_impl->fs()->delete_file(
                fmt::format("{}/{}", _write_info.write_path, _file_name)));
    }
    return Status::OK();
}

Status VHivePartitionWriter::open(RuntimeState* state, RuntimeProfile* profile) {
    fprintf(stderr, "VHivePartitionWriter::open\n");
    //    auto& hive_table_sink = _t_sink.hive_table_sink;
    std::vector<TNetworkAddress> broker_addresses;
    //    std::map<std::string, std::string> properties;
    //    properties.insert({"fs.defaultFS", "hdfs://HDFS8000871"});
    //    properties.insert({"dfs.nameservices", "HDFS8000871"});
    //    properties.insert({"dfs.ha.namenodes.HDFS8000871", "nn1,nn2"});
    //    properties.insert({"dfs.namenode.rpc-address.HDFS8000871.nn1", "172.21.0.32:4007"});
    //    properties.insert({"dfs.namenode.rpc-address.HDFS8000871.nn2", "172.21.0.44:4007"});
    //    properties.insert(
    //            {"dfs.client.failover.proxy.provider.HDFS8000871",
    //             "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"});
    for (auto& pair : _hadoop_conf) {
        fprintf(stderr, "key: %s, value: %s\n", pair.first.c_str(), pair.second.c_str());
    }
    RETURN_IF_ERROR(FileFactory::create_file_writer(
            _write_info.file_type, state->exec_env(), broker_addresses, _hadoop_conf,
            fmt::format("{}/{}", _write_info.write_path, _file_name), 0, _file_writer_impl));

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
            //            parquet_schema.schema_repetition_type = TParquetRepetitionType::OPTIONAL;
            //            fprintf(stderr, "parquet_schema.schema_column_name: %s\n",
            //                    parquet_schema.schema_column_name.c_str());

            //            switch (column_expr->type().type) {
            //            case TYPE_BOOLEAN: {
            //                parquet_schema.schema_data_type = TParquetDataType::BOOLEAN;
            //                break;
            //            }
            //            case TYPE_TINYINT: {
            //                parquet_schema.schema_data_type = TParquetDataType::INT32;
            //                break;
            //            }
            //            case TYPE_SMALLINT: {
            //                parquet_schema.schema_data_type = TParquetDataType::INT32;
            //                break;
            //            }
            //            case TYPE_INT: {
            //                parquet_schema.schema_data_type = TParquetDataType::INT32;
            //                break;
            //            }
            //            case TYPE_BIGINT: {
            //                parquet_schema.schema_data_type = TParquetDataType::INT64;
            //                break;
            //            }
            //            case TYPE_FLOAT: {
            //                parquet_schema.schema_data_type = TParquetDataType::FLOAT;
            //                break;
            //            }
            //            case TYPE_DOUBLE: {
            //                parquet_schema.schema_data_type = TParquetDataType::DOUBLE;
            //                break;
            //            }
            //            case TYPE_CHAR: {
            //                parquet_schema.schema_data_type = TParquetDataType::FIXED_LEN_BYTE_ARRAY;
            //                break;
            //            }
            //            case TYPE_VARCHAR: {
            //                parquet_schema.schema_data_type = TParquetDataType::BYTE_ARRAY;
            //                break;
            //            }
            //            case TYPE_STRING: {
            //                parquet_schema.schema_data_type = TParquetDataType::BYTE_ARRAY;
            //                break;
            //            }
            //
            //            case TYPE_BINARY: {
            //                parquet_schema.schema_data_type = TParquetDataType::BYTE_ARRAY;
            //                break;
            //            }
            //
            //                //            case TYPE_DATE: {
            //                //                parquet_schema.schema_data_type = TParquetDataType::BYTE_ARRAY;
            //                //                break;
            //                //            }
            //                //            case TYPE_DATETIME: {
            //                //                parquet_schema.schema_data_type = TParquetDataType::BYTE_ARRAY;
            //                //                break;
            //                //            }
            //            case TYPE_DATEV2: {
            //                parquet_schema.schema_data_type = TParquetDataType::INT32;
            //                break;
            //            }
            //            case TYPE_DATETIMEV2: {
            //                parquet_schema.schema_data_type = TParquetDataType::INT64;
            //                break;
            //            }
            //            case TYPE_DECIMAL32: {
            //                parquet_schema.schema_data_type = TParquetDataType::INT32;
            //                break;
            //            }
            //
            //            case TYPE_DECIMAL64: {
            //                parquet_schema.schema_data_type = TParquetDataType::INT64;
            //                break;
            //            }
            //            case TYPE_DECIMAL128I: {
            //                parquet_schema.schema_data_type = TParquetDataType::FIXED_LEN_BYTE_ARRAY;
            //                break;
            //            }
            //            case TYPE_STRUCT: {
            //                break;
            //            }
            //            case TYPE_ARRAY: {
            //                break;
            //            }
            //            case TYPE_MAP: {
            //                break;
            //            }
            //            default: {
            //                return Status::InternalError("Unsupported type {}", column_expr->type().type);
            //            }
            //            }
            parquet_schemas.emplace_back(std::move(parquet_schema));
        }
        _vfile_writer.reset(new VParquetTransformer(
                state, _file_writer_impl.get(), _vec_output_expr_ctxs, parquet_schemas,
                parquet_compression_type, parquet_disable_dictionary, TParquetVersion::PARQUET_1_0,
                false));
        return _vfile_writer->open();
    }
    case TFileFormatType::FORMAT_ORC: {
        fprintf(stderr, "FORMAT_ORC\n");
        orc::CompressionKind orc_compression_type;
        switch (_hive_compress_type) {
        case THiveCompressionType::SNAPPY: {
            orc_compression_type = orc::CompressionKind::CompressionKind_SNAPPY;
            break;
        }
        case THiveCompressionType::ZLIB: {
            orc_compression_type = orc::CompressionKind::CompressionKind_ZLIB;
            break;
        }
        case THiveCompressionType::ZSTD: {
            orc_compression_type = orc::CompressionKind::CompressionKind_ZSTD;
            break;
        }
        default: {
            return Status::InternalError("Unsupported type {} with orc", _hive_compress_type);
        }
        }
        orc_compression_type = orc::CompressionKind::CompressionKind_ZLIB;

        std::unique_ptr<orc::Type> root_schema = orc::createStructType();
        for (int i = 0; i < _columns.size(); i++) {
            //            if (_columns[i].column_type != THiveColumnType::REGULAR) {
            //                continue;
            //            }

            VExprSPtr column_expr = _vec_output_expr_ctxs[i]->root();
            root_schema->addStructField(_columns[i].name, _build_orc_type(column_expr->type()));
        }

        _vfile_writer.reset(new VOrcTransformer(state, _file_writer_impl.get(),
                                                _vec_output_expr_ctxs, std::move(root_schema),
                                                false, orc_compression_type));
        return _vfile_writer->open();
    }
    default: {
        return Status::InternalError("Unsupported file format type {}", _file_format_type);
    }
    }
}

std::unique_ptr<orc::Type> VHivePartitionWriter::_build_orc_type(TypeDescriptor type_descriptor) {
    switch (type_descriptor.type) {
    case TYPE_BOOLEAN: {
        fprintf(stderr, "TYPE_BOOLEAN\n");
        return orc::createPrimitiveType(orc::BOOLEAN);
    }
    case TYPE_TINYINT: {
        fprintf(stderr, "TYPE_TINYINT\n");
        return orc::createPrimitiveType(orc::BYTE);
    }
    case TYPE_SMALLINT: {
        fprintf(stderr, "TYPE_SMALLINT\n");
        return orc::createPrimitiveType(orc::SHORT);
    }
    case TYPE_INT: {
        fprintf(stderr, "TYPE_INT\n");
        return orc::createPrimitiveType(orc::INT);
    }
    case TYPE_BIGINT: {
        fprintf(stderr, "TYPE_BIGINT\n");
        return orc::createPrimitiveType(orc::LONG);
    }
    case TYPE_FLOAT: {
        fprintf(stderr, "TYPE_FLOAT\n");
        return orc::createPrimitiveType(orc::FLOAT);
    }
    case TYPE_DOUBLE: {
        fprintf(stderr, "TYPE_DOUBLE\n");
        return orc::createPrimitiveType(orc::DOUBLE);
    }
    case TYPE_CHAR: {
        fprintf(stderr, "TYPE_CHAR\n");
        return orc::createCharType(orc::CHAR, type_descriptor.len);
    }
    case TYPE_VARCHAR: {
        fprintf(stderr, "TYPE_VARCHAR\n");
        return orc::createCharType(orc::VARCHAR, type_descriptor.len);
    }
    case TYPE_STRING: {
        fprintf(stderr, "TYPE_STRING\n");
        return orc::createPrimitiveType(orc::STRING);
    }

    case TYPE_BINARY: {
        fprintf(stderr, "TYPE_BINARY\n");
        return orc::createPrimitiveType(orc::STRING);
    }

        //            case TYPE_DATE: {
        //                cur_schema->addStructField(_columns[i].name, orc::createPrimitiveType(orc::DATE));
        //                break;
        //            }
        //            case TYPE_DATETIME: {
        //                fprintf(stderr, "TYPE_DATETIME\n");
        //                cur_schema->addStructField(_columns[i].name,
        //                                           orc::createPrimitiveType(orc::TIMESTAMP));
        //                break;
        //            }
    case TYPE_DATEV2: {
        fprintf(stderr, "TYPE_DATEV2\n");
        return orc::createPrimitiveType(orc::DATE);
    }
    case TYPE_DATETIMEV2: {
        fprintf(stderr, "TYPE_DATETIMEV2\n");
        return orc::createPrimitiveType(orc::TIMESTAMP);
    }
    case TYPE_DECIMAL32: {
        fprintf(stderr, "TYPE_DECIMAL32\n");
        return orc::createDecimalType(type_descriptor.precision, type_descriptor.scale);
    }

    case TYPE_DECIMAL64: {
        fprintf(stderr, "TYPE_DECIMAL64\n");
        return orc::createDecimalType(type_descriptor.precision, type_descriptor.scale);
    }
    case TYPE_DECIMAL128I: {
        fprintf(stderr, "TYPE_DECIMAL128I\n");
        return orc::createDecimalType(type_descriptor.precision, type_descriptor.scale);
    }
    case TYPE_STRUCT: {
        fprintf(stderr, "TYPE_STRUCT\n");
        std::unique_ptr<orc::Type> struct_type = orc::createStructType();
        for (int j = 0; j < type_descriptor.children.size(); ++j) {
            struct_type->addStructField(type_descriptor.field_names[j],
                                        _build_orc_type(type_descriptor.children[j]));
        }
        return struct_type;
    }
    case TYPE_ARRAY: {
        fprintf(stderr, "TYPE_ARRAY\n");
        return orc::createListType(_build_orc_type(type_descriptor.children[0]));
    }
    case TYPE_MAP: {
        fprintf(stderr, "TYPE_MAP\n");
        return orc::createMapType(_build_orc_type(type_descriptor.children[0]),
                                  _build_orc_type(type_descriptor.children[1]));
    }
    default: {
        LOG(FATAL) << fmt::format("Unsupported type {}", type_descriptor.type);
        return nullptr;
    }
    }
}

Status VHivePartitionWriter::write(vectorized::Block& block, vectorized::IColumn::Filter* filter) {
    Block output_block;
    RETURN_IF_ERROR(_projection_and_filter_block(block, filter, &output_block));
    RETURN_IF_ERROR(_vfile_writer->write(output_block));
    _row_count += block.rows();
    _input_size_in_bytes += block.bytes();
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