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

#include "vhive_table_writer.h"

#include <gen_cpp/DataSinks_types.h>
#include <stdint.h>

#include <iomanip>
#include <memory>
#include <regex>
#include <sstream>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "runtime/decimalv2_value.h"
#include "runtime/define_primitive_type.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "util/binary_cast.hpp"
#include "util/runtime_profile.h"
#include "vec/columns/column.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/pod_array.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/materialize_block.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/runtime/vdatetime_value.h"
#include "vec/sink/writer/vhive_utils.h"

namespace doris {
namespace vectorized {

VHiveTableWriter::VHiveTableWriter(const TDataSink& t_sink,
                                   const VExprContextSPtrs& output_expr_ctxs)
        : AsyncResultWriter(output_expr_ctxs), _t_sink(t_sink) {
    DCHECK(_t_sink.__isset.hive_table_sink);
    //    fprintf(stderr, "_t_sink->hive_table_sink.table_name: %s\n",
    //            _t_sink.hive_table_sink.table_name.c_str());
    //    fprintf(stderr, "_t_sink->hive_table_sink.location.write_path: %s\n",
    //            _t_sink.hive_table_sink.location.write_path.c_str());
    //    for (int i = 0; i < _t_sink.hive_table_sink.columns.size(); ++i) {
    //        fprintf(stderr, "hive_table_sink.columns[%d].name: %s\n", i,
    //                _t_sink.hive_table_sink.columns[i].name.c_str());
    //    }
}

Status VHiveTableWriter::init_properties(ObjectPool* pool) {
    return Status::OK();
}

Status VHiveTableWriter::open(RuntimeState* state, RuntimeProfile* profile) {
    _state = state;
    _profile = profile;

    //    fprintf(stderr, "_t_sink.hive_table_sink.table_name: %s\n",
    //            _t_sink.hive_table_sink.table_name.c_str());
    //    fprintf(stderr, "_t_sink.hive_table_sink.location.write_path: %s\n",
    //            _t_sink.hive_table_sink.location.write_path.c_str());
    //    auto& hive_table_sink = _t_sink.hive_table_sink;
    for (int i = 0; i < _t_sink.hive_table_sink.columns.size(); ++i) {
        //        fprintf(stderr, "open(): hive_table_sink.columns[%d].name: %s, type: %d\n", i,
        //                _t_sink.hive_table_sink.columns[i].name.c_str(),
        //                _t_sink.hive_table_sink.columns[i].column_type);
        if (_t_sink.hive_table_sink.columns[i].column_type == THiveColumnType::PARTITION_KEY) {
            _partition_columns_input_index.push_back(i);
        }
    }

    //    _data_col_expr_ctxs =
    //            _vec_output_expr_ctxs.begin(),
    //            _vec_output_expr_ctxs.begin() + hive_table_sink.data_column_names.size());
    //    fprintf(stderr, "hive_table_sink.data_column_names.size(): %ld\n", hive_table_sink.data_column_names.size());
    //    fprintf(stderr, "_vec_output_expr_ctxs[1]->root()->expr_name().c_str(): %s\n", _vec_output_expr_ctxs[1]->root()->expr_name().c_str());
    //    _data_col_expr_ctxs.insert(
    //            _data_col_expr_ctxs.end(), _vec_output_expr_ctxs.begin(),
    //            _vec_output_expr_ctxs.begin() + hive_table_sink.data_column_names.size());
    //    _partition_col_expr_ctxs.insert(
    //            _partition_col_expr_ctxs.end(),
    //            _vec_output_expr_ctxs.begin() + hive_table_sink.data_column_names.size(),
    //            _vec_output_expr_ctxs.end());
    return Status::OK();
}

Status VHiveTableWriter::write(vectorized::Block& block) {
    //    fprintf(stderr, "HiveTableSink::send\n");
    std::unordered_map<std::shared_ptr<VHivePartitionWriter>, IColumn::Filter> writer_positions;

    auto& hive_table_sink = _t_sink.hive_table_sink;

    if (_partition_columns_input_index.empty()) {
        std::shared_ptr<VHivePartitionWriter> writer = _create_partition_writer(block, -1, 1);
        _partitions_to_writers.insert({"", writer});
        RETURN_IF_ERROR(writer->open(_state, _profile));
        RETURN_IF_ERROR(writer->write(block));
        return Status::OK();
    }

    for (int i = 0; i < block.rows(); ++i) {
        //        std::stringstream key_ss;
        //        for (size_t j = 0; j < _partition_columns_input_index.size(); j++) {
        //            vectorized::ColumnWithTypeAndName partition_column =
        //                    block.get_by_position(_partition_columns_input_index[j]);
        //            //key_ss << partition_column.name;
        //            key_ss << "city";
        //            key_ss << "=";
        //            key_ss << partition_column.column->get_data_at(i).to_string();
        //            key_ss << "/";
        //        }
        //        std::string key = key_ss.str();;
        std::vector<std::string> partition_values = _create_partition_values(block, i);
        std::string partition_name = VHiveUtils::make_partition_name(
                hive_table_sink.columns, _partition_columns_input_index, partition_values);
        //        fprintf(stderr, "partition_name: %s\n", partition_name.c_str());

        auto partition_writer_iter = _partitions_to_writers.find(partition_name);
        if (partition_writer_iter == _partitions_to_writers.end()) {
            //            std::vector<ExprContext*> data_col_exprs(
            //                    _output_expr.begin(), _output_expr.begin() + _data_column_names.size());
            //            auto writer = std::make_shared<RollingAsyncParquetWriter>(
            //                    tableInfo, data_col_exprs, _common_metrics.get(), add_hive_commit_info, state,
            //                    _driver_sequence);
            //            auto writer =
            //                    std::make_shared<VHivePartitionWriter>(*_t_sink, key, _vec_output_expr_ctxs);
            std::shared_ptr<VHivePartitionWriter> writer = _create_partition_writer(block, i, 1);
            //            if (state->enable_pipeline_exec()) {
            //                writer->start_writer(_state, _profile);
            //            } else {
            //                RETURN_IF_ERROR(writer->open(_state, _profile));
            //            }
            RETURN_IF_ERROR(writer->open(_state, _profile));
            IColumn::Filter filter(block.rows(), 0);
            filter[i] = 1;
            writer_positions.insert({writer, std::move(filter)});
            _partitions_to_writers.insert({partition_name, writer});
        } else {
            //            if (partition_writer_iter->second->written_len() > targetMaxFileSize) {
            //                partition_writer_iter->second->close(Status::OK());
            //                writer_positions.erase(partition_writer_iter->second);
            //                _partitions_to_writers.erase(partition_writer_iter->first);
            //                auto writer = _create_partition_writer(block, i, 1);
            //                RETURN_IF_ERROR(writer->open(_state, _profile));
            //                IColumn::Filter filter(block.rows(), 0);
            //                filter[i] = 1;
            //                writer_positions.insert({writer, std::move(filter)});
            //                _partitions_to_writers.insert({partition_name, writer});
            //            }
            auto writer_pos_iter = writer_positions.find(partition_writer_iter->second);
            if (writer_pos_iter == writer_positions.end()) {
                IColumn::Filter filter(block.rows(), 0);
                filter[i] = 1;
                writer_positions.insert({partition_writer_iter->second, std::move(filter)});
            } else {
                writer_pos_iter->second[i] = 1;
            }
        }
    }
    for (auto it = writer_positions.begin(); it != writer_positions.end(); ++it) {
        RETURN_IF_ERROR(it->first->write(block, &it->second));
    }
    return Status::OK();
}

//Status VHiveTableWriter::close_idle_writers() {
//
//}

Status VHiveTableWriter::close(Status status) {
    if (status == Status::OK()) {
        for (const auto& pair : _partitions_to_writers) {
            if (pair.second->close(status) != Status::OK()) {
                // log it.
            }
            _state->hive_partition_updates().emplace_back(pair.second->get_partition_update());
        }
        _partitions_to_writers.clear();
    } else {
    }
    return Status::OK();
}

std::shared_ptr<VHivePartitionWriter> VHiveTableWriter::_create_partition_writer(
        vectorized::Block& block, int position, int bucket_number) {
    auto& hive_table_sink = _t_sink.hive_table_sink;
    std::vector<std::string> partition_values;
    std::string partition_name;
    if (!_partition_columns_input_index.empty()) {
        partition_values = _create_partition_values(block, position);
        partition_name = VHiveUtils::make_partition_name(
                hive_table_sink.columns, _partition_columns_input_index, partition_values);
    }
    std::vector<THivePartition> partitions = hive_table_sink.partitions;
    THiveLocationParams write_location = hive_table_sink.location;
    const THivePartition* existing_partition = nullptr;
    bool existing_table = true;
    for (const auto& partition : partitions) {
        if (partition_values == partition.values) {
            existing_partition = &partition;
            break;
        }
    }
    TUpdateMode::type update_mode;
    WriteInfo write_info;
    TFileFormatType::type file_format_type;
    THiveCompressionType::type write_compress_type;
    if (existing_partition == nullptr) { // new partition
        if (existing_table == false) {   // new table
            update_mode = TUpdateMode::NEW;
            if (_partition_columns_input_index.empty()) { // new unpartitioned table
                write_info = {write_location.write_path, write_location.target_path};
            } else { // a new partition in a new partitioned table
                auto write_path = fmt::format("{}/{}", write_location.write_path, partition_name);
                auto target_path = fmt::format("{}/{}", write_location.target_path, partition_name);
                write_info = {write_path, target_path};
                //                if (write_info.write_path != write_info.target_path) {
                //                    if (file_system.directory_exists(write_info.target_path)) {
                //                        return Status::AlreadyExist("");
                //                    }
                //                }
            }
        } else { // a new partition in an existing partitioned table, or an existing unpartitioned table
            if (_partition_columns_input_index.empty()) { // an existing unpartitioned table
                update_mode = !_overwrite ? TUpdateMode::APPEND : TUpdateMode::OVERWRITE;
                write_info = {write_location.write_path, write_location.target_path};
                fprintf(stderr, "write_path: %s\n", write_info.write_path.c_str());
                fprintf(stderr, "target_path: %s\n", write_info.target_path.c_str());
            } else { // a new partition in an existing partitioned table
                update_mode = TUpdateMode::NEW;
                auto write_path = fmt::format("{}/{}", write_location.write_path, partition_name);
                auto target_path = fmt::format("{}/{}", write_location.target_path, partition_name);
                fprintf(stderr, "write_path: %s\n", write_path.c_str());
                fprintf(stderr, "target_path: %s\n", target_path.c_str());
                write_info = {write_path, target_path};
            }
            // need to get schema from existing table ?
        }
        file_format_type = hive_table_sink.file_format;
        write_compress_type = hive_table_sink.compression_type;
    } else { // existing partition
        if (!_overwrite) {
            update_mode = TUpdateMode::APPEND;
            auto write_path = fmt::format("{}/{}", existing_partition->location.write_path);
            auto target_path = fmt::format("{}/{}", existing_partition->location.target_path);
            write_info = {write_path, target_path};
            file_format_type = existing_partition->file_format;
            write_compress_type = hive_table_sink.compression_type;
        } else {
            update_mode = TUpdateMode::OVERWRITE;
            auto write_path = fmt::format("{}/{}", write_location.write_path, partition_name);
            auto target_path = fmt::format("{}/{}", write_location.target_path, partition_name);
            write_info = {write_path, target_path};
            file_format_type = hive_table_sink.file_format;
            write_compress_type = hive_table_sink.compression_type;
            // need to get schema from existing table ?
        }
    }

    return std::make_shared<VHivePartitionWriter>(
            _t_sink, partition_name, update_mode, _vec_output_expr_ctxs, hive_table_sink.columns,
            write_info,
            fmt::format("{}{}", _compute_file_name(bucket_number),
                        _get_file_extension(file_format_type, write_compress_type)),
            file_format_type, write_compress_type);
}

std::vector<std::string> VHiveTableWriter::_create_partition_values(vectorized::Block& block,
                                                                    int position) {
    std::vector<std::string> partition_values;
    for (int i = 0; i < _partition_columns_input_index.size(); ++i) {
        // Assuming `toPartitionValue` function converts column to string
        int partition_column_idx = _partition_columns_input_index[i];
        vectorized::ColumnWithTypeAndName partition_column =
                block.get_by_position(partition_column_idx);
        std::string value =
                _to_partition_value(_vec_output_expr_ctxs[partition_column_idx]->root()->type(),
                                    partition_column, position);
        //        fprintf(stderr, "value: %s\n", value.c_str());

        // Check if value contains only printable ASCII characters
        bool isValid = true;
        for (char c : value) {
            if (c < 0x20 || c > 0x7E) {
                isValid = false;
                break;
            }
        }

        if (!isValid) {
            // Encode value using Base16 encoding with space separator
            std::stringstream encoded;
            for (unsigned char c : value) {
                encoded << std::hex << std::setw(2) << std::setfill('0') << (int)c;
                encoded << " ";
            }
            throw std::invalid_argument(
                    "Hive partition keys can only contain printable ASCII characters (0x20 - "
                    "0x7E). Invalid value: " +
                    encoded.str());
        }

        partition_values.emplace_back(value);
    }

    return partition_values;
}

std::string VHiveTableWriter::_to_partition_value(const TypeDescriptor& type_desc,
                                                  const ColumnWithTypeAndName& partition_column,
                                                  int position) {
    ColumnPtr column;
    if (auto* nullable_column = check_and_get_column<ColumnNullable>(*partition_column.column)) {
        auto* __restrict null_map_data = nullable_column->get_null_map_data().data();
        if (null_map_data[position]) {
            fprintf(stderr, "position %d is null\n", position);
            return "__HIVE_DEFAULT_PARTITION__";
        }
        column = nullable_column->get_nested_column_ptr();
    } else {
        column = partition_column.column;
    }
    auto [item, size] = column->get_data_at(position);
    switch (type_desc.type) {
    case TYPE_BOOLEAN: {
        vectorized::Field field =
                vectorized::check_and_get_column<const ColumnUInt8>(*column)->operator[](position);
        return std::to_string(field.get<bool>());
    }
    case TYPE_TINYINT: {
        return std::to_string(*reinterpret_cast<const Int8*>(item));
    }
    case TYPE_SMALLINT: {
        return std::to_string(*reinterpret_cast<const Int16*>(item));
    }
    case TYPE_INT: {
        return std::to_string(*reinterpret_cast<const Int32*>(item));
    }
    case TYPE_BIGINT: {
        return std::to_string(*reinterpret_cast<const Int64*>(item));
    }
    case TYPE_FLOAT: {
        return std::to_string(*reinterpret_cast<const Float32*>(item));
    }
    case TYPE_DOUBLE: {
        return std::to_string(*reinterpret_cast<const Float64*>(item));
    }
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_STRING: {
        fprintf(stderr, "TYPE_STRING\n");
        //        vectorized::Field field =
        //                vectorized::check_and_get_column<const ColumnString>(*column)->
        //                operator[](position);
        //        return field.get<std::string>();
        return std::string(item, size);
    }
    case TYPE_DATE: {
        VecDateTimeValue value = binary_cast<int64_t, doris::VecDateTimeValue>(*(int64_t*)item);

        char buf[64];
        char* pos = value.to_string(buf);
        return std::string(buf, pos - buf - 1);
    }
    case TYPE_DATETIME: {
        VecDateTimeValue value = binary_cast<int64_t, doris::VecDateTimeValue>(*(int64_t*)item);

        char buf[64];
        char* pos = value.to_string(buf);
        return std::string(buf, pos - buf - 1);
        break;
    }
    case TYPE_DATEV2: {
        DateV2Value<DateV2ValueType> value =
                binary_cast<uint32_t, DateV2Value<DateV2ValueType>>(*(int32_t*)item);

        char buf[64];
        char* pos = value.to_string(buf);
        return std::string(buf, pos - buf - 1);
    }
    case TYPE_DATETIMEV2: {
        DateV2Value<DateTimeV2ValueType> value =
                binary_cast<uint64_t, DateV2Value<DateTimeV2ValueType>>(*(int64_t*)item);

        char buf[64];
        char* pos = value.to_string(buf, type_desc.scale);
        return std::string(buf, pos - buf - 1);
    }
    case TYPE_DECIMALV2: {
        Decimal128V2 value = *(Decimal128V2*)(item);
        return value.to_string(type_desc.scale);
    }
    case TYPE_DECIMAL32: {
        Decimal32 value = *(Decimal32*)(item);
        return value.to_string(type_desc.scale);
    }
    case TYPE_DECIMAL64: {
        Decimal64 value = *(Decimal64*)(item);
        return value.to_string(type_desc.scale);
    }
    case TYPE_DECIMAL128I: {
        Decimal128V3 value = *(Decimal128V3*)(item);
        return value.to_string(type_desc.scale);
    }
    case TYPE_DECIMAL256: {
        Decimal256 value = *(Decimal256*)(item);
        return value.to_string(type_desc.scale);
    }
    default: {
        LOG(WARNING) << fmt::format("Unsupported type for partition {}", type_desc.debug_string());
        return "";
    }
    }
}

//const std::locale ENGLISH("en_US.UTF-8");
//
//std::string VHiveTableWriter::_make_part_name(const std::vector<std::string>& columns,
//                                              const std::vector<std::string>& values) {
//    std::stringstream part_name;
//
//    for (size_t i = 0; i < columns.size(); i++) {
//        if (i > 0) {
//           part_name << '/';
//        }
//        std::string column = columns[i];
//        std::string value = values[i];
//        // Convert column to lowercase using English locale
//        transform(column.begin(), column.end(), column.begin(),
//                  [&](char c) { return tolower(c, ENGLISH); });
//        part_name << _escape_path_name(column) << '=' << _escape_path_name(value);
//    }
//
//    return part_name.str();
//}
//
//std::string VHiveTableWriter::_escape_path_name(const std::string& path) {
//    if (path.empty()) {
//        return "__HIVE_DEFAULT_PARTITION__";
//    }
//
//    // Fast-path detection, no escaping and therefore no copying necessary
//    std::smatch match;
//    if (!std::regex_search(path, match, PATH_CHAR_TO_ESCAPE)) {
//        return path;
//    }
//
//    // Slow path, escape beyond the first required escape character into a new string
//    std::stringstream sb;
//    size_t fromIndex = 0;
//    auto begin = path.begin(); // Iterator for the beginning of the string
//    auto end = path.end();     // Iterator for the end of the string
//    while (std::regex_search(begin + fromIndex, end, match, PATH_CHAR_TO_ESCAPE)) {
//        size_t escapeAtIndex = match.position() + fromIndex;
//        // preceding characters without escaping needed
//        if (escapeAtIndex > fromIndex) {
//            sb << path.substr(fromIndex, escapeAtIndex - fromIndex);
//        }
//        // escape single character
//        char c = path[escapeAtIndex];
//        sb << '%' << std::hex << std::uppercase << static_cast<int>(c >> 4)
//           << static_cast<int>(c & 0xF);
//        // find next character to escape
//        fromIndex = escapeAtIndex + 1;
//    }
//    // trailing characters without escaping needed
//    if (fromIndex < path.length()) {
//        sb << path.substr(fromIndex);
//    }
//    return sb.str();
//}

std::string VHiveTableWriter::_get_file_extension(TFileFormatType::type file_format_type,
                                                  THiveCompressionType::type write_compress_type) {
    switch (write_compress_type) {
    case THiveCompressionType::SNAPPY: {
        return ".snappy";
    }
    case THiveCompressionType::LZ4: {
        return ".lz4";
    }
    case THiveCompressionType::ZLIB: {
        return ".zlib";
    }
    case THiveCompressionType::ZSTD: {
        return ".zstd";
    }
    default: {
        return "";
    }
    }
}

std::string VHiveTableWriter::_compute_file_name(int bucketNumber) {
    //    if (bucketNumber.has_value()) {
    //        if (isCreateTransactionalTable) {
    //            return computeTransactionalBucketedFilename(bucketNumber.value());
    //        }
    //        return computeNonTransactionalBucketedFilename(queryId, bucketNumber.value());
    //    }
    //
    //    if (isCreateTransactionalTable) {
    //        string paddedBucket = string(BUCKET_NUMBER_PADDING - 1, '0') + TRANSACTIONAL_TABLE_PREFIX;
    //        random_device rd;
    //        mt19937 gen(rd());
    //        uniform_int_distribution<long long> dis(0, numeric_limits<long long>::max());
    //        long long leastSigBits = dis(gen);
    //        long long mostSigBits = dis(gen);
    //        stringstream uuidStream;
    //        uuidStream << setfill('0') << setw(16) << hex << leastSigBits << setw(16) << hex << mostSigBits;
    //        return paddedBucket + "_" + uuidStream.str();
    //    }
    //
    //    random_device rd;
    //    mt19937 gen(rd());
    //    uniform_int_distribution<long long> dis(0, numeric_limits<long long>::max());
    //    long long leastSigBits = dis(gen);
    //    long long mostSigBits = dis(gen);
    //    stringstream uuidStream;
    //    uuidStream << queryId << "_" << setfill('0') << setw(16) << hex << leastSigBits << setw(16) << hex << mostSigBits;
    //    return uuidStream.str();
    boost::uuids::uuid uuid = boost::uuids::random_generator()();

    std::string uuid_str = boost::uuids::to_string(uuid);
    fprintf(stderr, "query_id: %s\n", print_id(_state->query_id()).c_str());

    // 结合 fmt 库输出字符串
    return fmt::format("{}_{}", print_id(_state->query_id()), uuid_str);
}

} // namespace vectorized
} // namespace doris
