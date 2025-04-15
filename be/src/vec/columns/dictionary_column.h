#pragma once

#include "vec/columns/column.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "common/exception.h"
#include "vec/common/cow.h"
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <memory>
#include <utility>

#include <atomic>
#include <string>
#include <cstdint>

namespace doris::vectorized {


class DictionaryId {
private:
    static std::string nodeId;
    static std::atomic<long> sequenceGenerator;

    const int64_t mostSignificantBits;
    const int64_t leastSignificantBits;
    const int64_t sequenceId;

public:
    static DictionaryId randomDictionaryId() {
        // 生成随机 nodeId（这里简化为随机字符串，实际应使用 UUID 生成）
        if (nodeId.empty()) {
            nodeId = generateRandomNodeId();
        }
        return DictionaryId(
                *reinterpret_cast<const int64_t*>(nodeId.data()),
                *reinterpret_cast<const int64_t*>(nodeId.data() + 8),
                sequenceGenerator.fetch_add(1)
        );
    }

    DictionaryId(int64_t mostSignificantBits, int64_t leastSignificantBits, int64_t sequenceId)
            : mostSignificantBits(mostSignificantBits),
              leastSignificantBits(leastSignificantBits),
              sequenceId(sequenceId) {}

    int64_t getMostSignificantBits() const {
        return mostSignificantBits;
    }

    int64_t getLeastSignificantBits() const {
        return leastSignificantBits;
    }

    int64_t getSequenceId() const {
        return sequenceId;
    }

    bool operator==(const DictionaryId& other) const {
        return mostSignificantBits == other.mostSignificantBits &&
               leastSignificantBits == other.leastSignificantBits &&
               sequenceId == other.sequenceId;
    }

    bool operator!=(const DictionaryId& other) const {
        return !(*this == other);
    }

    struct Hash {
        size_t operator()(const DictionaryId& id) const {
            size_t hash = 31 + std::hash<int64_t>()(id.mostSignificantBits);
            hash = (hash * 31) + std::hash<int64_t>()(id.leastSignificantBits);
            hash = (hash * 31) + std::hash<int64_t>()(id.sequenceId);
            return hash;
        }
    };

private:
    static std::string generateRandomNodeId() {
        // 这里应该实现一个真正的 UUID 生成逻辑
        return "randomNodeId"; // 示例值
    }
};

template <typename T>
class DictionaryColumn final : public COWHelper<IColumn, DictionaryColumn<T>> {
    static_assert(IsNumber<T>);
    
private:
    friend class COWHelper<IColumn, DictionaryColumn>;

    DictionaryColumn() 
        : _ids_offset(0), 
          _row_count(0), 
          _dictionary_is_compacted(false), 
          _is_sequential_ids(false), 
          _dictionary_source_id(DictionaryId::randomDictionaryId()) {}

    DictionaryColumn(int ids_offset, int row_count, const ColumnPtr& dictionary, 
                     const std::vector<int32_t>& ids, bool dictionary_is_compacted, 
                     bool is_sequential_ids, DictionaryId dictionary_source_id)
        : _ids_offset(ids_offset), _row_count(row_count), 
          _dictionary(dictionary), _ids(ids), 
          _dictionary_is_compacted(dictionary_is_compacted), 
          _is_sequential_ids(is_sequential_ids), 
          _dictionary_source_id(dictionary_source_id) {
        CHECK(_row_count >= 0) << "rowCount is negative";
        CHECK(_ids.size() - _ids_offset >= _row_count) << "ids length is less than rowCount";
        CHECK(!(_is_sequential_ids && !_dictionary_is_compacted)) << "sequential ids flag is only valid for compacted dictionary";

        if (_dictionary_is_compacted) {
            _size_in_bytes = _dictionary->byte_size() + (sizeof(int32_t) * _row_count);
            _unique_ids = _dictionary->size();
        }
    }


public:
    using Self = DictionaryColumn;
    using value_type = T;
    using Container = PaddedPODArray<value_type>;
    using DictContainer = PaddedPODArray<StringRef>;
    using HashValueContainer = PaddedPODArray<uint32_t>; // used for bloom filter

    bool is_column_dictionary() const override { return true; }


    size_t size() const override { return _ids.size(); }

    [[noreturn]] StringRef get_data_at(size_t n) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "get_data_at not supported in ColumnDictionary");
    }

    void insert_from(const IColumn& src, size_t n) override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "insert_from not supported in ColumnDictionary");
    }

    void insert_range_from(const IColumn& src, size_t start, size_t length) override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "insert_range_from not supported in ColumnDictionary");
    }

    void insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
                             const uint32_t* indices_end) override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "insert_indices_from not supported in ColumnDictionary");
    }

    void update_hash_with_value(size_t n, SipHash& hash) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "update_hash_with_value not supported in ColumnDictionary");
    }

    void insert_data(const char* pos, size_t /*length*/) override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "insert_data not supported in ColumnDictionary");
    }

    void insert_default() override {
//        _codes.push_back(_dict.get_null_code());
    }

    void clear() override {
        _ids.clear();
        _ids_offset = 0;
        _row_count = 0;
    }

    // TODO: Make dict memory usage more precise
    size_t byte_size() const override {
//        return _codes.size() * sizeof(_codes[0]);
            return 0;
    }

    size_t allocated_bytes() const override { return byte_size(); }

    bool has_enough_capacity(const IColumn& src) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "has_enough_capacity not supported in ColumnDictionary");
    }

    void pop_back(size_t n) override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "pop_back not supported in ColumnDictionary");
    }

    void reserve(size_t n) override {
//        _codes.reserve(n);
    }

    std::string get_name() const override { return "ColumnDictionary"; }

    MutableColumnPtr clone_resized(size_t to_size) const override {
        return this->create(_ids_offset, to_size, _dictionary, _ids, 
                            _dictionary_is_compacted, _is_sequential_ids, _dictionary_source_id);
    }

    void insert(const Field& x) override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "insert not supported in ColumnDictionary");
    }

    Field operator[](size_t n) const override {
        // 从字典中查找 id 对应的值
        return _dictionary->operator[](_ids[n + _ids_offset]);
    }

    void get(size_t n, Field& res) const override { 
        // 通过 operator[] 获取值
        res = (*this)[n]; 
    }

//    Container& get_data() { return _codes; }
//
//    const Container& get_data() const { return _codes; }

    // it's impossible to use ComplexType as key , so we don't have to implement them
    [[noreturn]] StringRef serialize_value_into_arena(size_t n, Arena& arena,
                                                      char const*& begin) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "serialize_value_into_arena not supported in ColumnDictionary");
    }

    [[noreturn]] const char* deserialize_and_insert_from_arena(const char* pos) override {
        throw doris::Exception(
                ErrorCode::INTERNAL_ERROR,
                "deserialize_and_insert_from_arena not supported in ColumnDictionary");
    }

    [[noreturn]] StringRef get_raw_data() const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "get_raw_data not supported in ColumnDictionary");
    }

    [[noreturn]] bool structure_equals(const IColumn& rhs) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "structure_equals not supported in ColumnDictionary");
    }

    [[noreturn]] ColumnPtr filter(const IColumn::Filter& filt,
                                  ssize_t result_size_hint) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "filter not supported in ColumnDictionary");
    }

    [[noreturn]] size_t filter(const IColumn::Filter&) override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "filter not supported in ColumnDictionary");
    }

    [[noreturn]] ColumnPtr permute(const IColumn::Permutation& perm, size_t limit) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "permute not supported in ColumnDictionary");
    }

    [[noreturn]] ColumnPtr replicate(const IColumn::Offsets& replicate_offsets) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "replicate not supported in ColumnDictionary");
    }

    Status filter_by_selector(const uint16_t* sel, size_t sel_size, IColumn* col_ptr) override {
//        auto* res_col = assert_cast<vectorized::ColumnString*>(col_ptr);
//        _strings.resize(sel_size);
//        size_t length = 0;
//        for (size_t i = 0; i != sel_size; ++i) {
//            auto& value = _dict.get_value(_codes[sel[i]]);
//            _strings[i].data = value.data;
//            _strings[i].size = value.size;
//            length += value.size;
//        }
//        res_col->get_offsets().reserve(sel_size + res_col->get_offsets().size());
//        res_col->get_chars().reserve(length + res_col->get_chars().size());
//        res_col->insert_many_strings_without_reserve(_strings.data(), sel_size);
//        return Status::OK();
        return Status::OK();
    }

    void replace_column_data(const IColumn&, size_t row, size_t self_row = 0) override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "should not call replace_column_data in ColumnDictionary");
    }

//    /**
//     * Just insert dictionary data items, the items will append into _dict.
//     */
//    void insert_many_dict_data(const StringRef* dict_array, uint32_t dict_num) {
//        _dict.reserve(_dict.size() + dict_num);
//        for (uint32_t i = 0; i < dict_num; ++i) {
//            auto value = StringRef(dict_array[i].data, dict_array[i].size);
//            _dict.insert_value(value);
//        }
//    }

    void insert_many_dict_data(const int32_t* data_array, size_t start_index,
                               const StringRef* dict_array, size_t data_num,
                               uint32_t dict_num) override {
//        if (_dict.empty()) {
//            _dict.reserve(dict_num);
//            for (uint32_t i = 0; i < dict_num; ++i) {
//                auto value = StringRef(dict_array[i].data, dict_array[i].size);
//                _dict.insert_value(value);
//            }
//        }
//
//        char* end_ptr = (char*)_codes.get_end_ptr();
//        memcpy(end_ptr, data_array + start_index, data_num * sizeof(T));
//        end_ptr += data_num * sizeof(T);
//        _codes.set_end_ptr(end_ptr);
    }

    void convert_dict_codes_if_necessary() override {
//        // Avoid setting `_dict_sorted` to true when `_dict` is empty.
//        // Because `_dict` maybe keep empty after inserting some null rows.
//        if (_dict.empty()) {
//            return;
//        }
//
//        if (!is_dict_sorted()) {
//            _dict.sort();
//            _dict_sorted = true;
//        }
//
//        if (!is_dict_code_converted()) {
//            for (size_t i = 0; i < size(); ++i) {
//                _codes[i] = _dict.convert_code(_codes[i]);
//            }
//            _dict_code_converted = true;
//        }
    }
//
//    T find_code(const StringRef& value) const { return _dict.find_code(value); }
//
//    T find_code_by_bound(const StringRef& value, bool greater, bool eq) const {
//        return _dict.find_code_by_bound(value, greater, eq);
//    }
//
    void initialize_hash_values_for_runtime_filter() override {
//        _dict.initialize_hash_values_for_runtime_filter();
    }
//
//    uint32_t get_hash_value(uint32_t idx) const { return _dict.get_hash_value(_codes[idx], _type); }
//
//    template <typename HybridSetType>
//    void find_codes(const HybridSetType* values, std::vector<vectorized::UInt8>& selected) const {
//        return _dict.find_codes(values, selected);
//    }
//
    void set_rowset_segment_id(std::pair<RowsetId, uint32_t> rowset_segment_id) override {
//        _rowset_segment_id = rowset_segment_id;
    }
//
    std::pair<RowsetId, uint32_t> get_rowset_segment_id() const override {
//        return _rowset_segment_id;
        return {};
    }
//
//    bool is_dict_sorted() const { return _dict_sorted; }
//
//    bool is_dict_empty() const { return _dict.empty(); }
//
//    bool is_dict_code_converted() const { return _dict_code_converted; }

    MutableColumnPtr convert_to_predicate_column_if_dictionary() override {
//        if (is_dict_sorted() && !is_dict_code_converted()) {
//            convert_dict_codes_if_necessary();
//        }
//        // if type is OLAP_FIELD_TYPE_CHAR, we need to construct TYPE_CHAR PredicateColumnType,
//        // because the string length will different from varchar and string which needed to be processed after.
//        auto create_column = [this]() -> MutableColumnPtr {
//            if (_type == FieldType::OLAP_FIELD_TYPE_CHAR) {
//                return vectorized::PredicateColumnType<TYPE_CHAR>::create();
//            }
//            return vectorized::PredicateColumnType<TYPE_STRING>::create();
//        };
//
//        auto res = create_column();
//        res->reserve(_codes.capacity());
//        for (size_t i = 0; i < _codes.size(); ++i) {
//            auto& code = reinterpret_cast<T&>(_codes[i]);
//            auto value = _dict.get_value(code);
//            res->insert_data(value.data, value.size);
//        }
//        clear();
//        _dict.clear();
//        return res;
        return {};
    }
//
//    inline const StringRef& get_value(value_type code) const { return _dict.get_value(code); }
//
//    inline StringRef get_shrink_value(value_type code) const {
//        StringRef result = _dict.get_value(code);
//        if (_type == FieldType::OLAP_FIELD_TYPE_CHAR) {
//            result.size = strnlen(result.data, result.size);
//        }
//        return result;
//    }
//
//    size_t dict_size() const { return _dict.size(); }
//
//    std::string dict_debug_string() const { return _dict.debug_string(); }

//    class Dictionary {
//    public:
//        Dictionary() : _dict_data(new DictContainer()), _total_str_len(0) {}
//
//        void reserve(size_t n) { _dict_data->reserve(n); }
//
//        void insert_value(const StringRef& value) {
//            _dict_data->push_back_without_reserve(value);
//            _total_str_len += value.size;
//        }
//
//        T find_code(const StringRef& value) const {
//            // _dict_data->size will not exceed the range of T.
//            for (T i = 0; i < _dict_data->size(); i++) {
//                if ((*_dict_data)[i] == value) {
//                    return i;
//                }
//            }
//            return -2; // -1 is null code
//        }
//
//        T get_null_code() const { return -1; }
//
//        inline StringRef& get_value(T code) { return (*_dict_data)[code]; }
//
//        inline const StringRef& get_value(T code) const { return (*_dict_data)[code]; }
//
//        // The function is only used in the runtime filter feature
//        inline void initialize_hash_values_for_runtime_filter() {
//            if (_hash_values.empty()) {
//                _hash_values.resize(_dict_data->size());
//                _compute_hash_value_flags.resize(_dict_data->size());
//                _compute_hash_value_flags.assign(_dict_data->size(), 0);
//            }
//        }
//
//        inline uint32_t get_hash_value(T code, FieldType type) const {
//            if (_compute_hash_value_flags[code]) {
//                return _hash_values[code];
//            } else {
//                auto& sv = (*_dict_data)[code];
//                // The char data is stored in the disk with the schema length,
//                // and zeros are filled if the length is insufficient
//
//                // When reading data, use shrink_char_type_column_suffix_zero(_char_type_idx)
//                // Remove the suffix 0
//                // When writing data, use the CharField::consume function to fill in the trailing 0.
//
//                // For dictionary data of char type, sv.size is the schema length,
//                // so use strnlen to remove the 0 at the end to get the actual length.
//                size_t len = sv.size;
//                if (type == FieldType::OLAP_FIELD_TYPE_CHAR) {
//                    len = strnlen(sv.data, sv.size);
//                }
//                uint32_t hash_val = HashUtil::crc_hash(sv.data, static_cast<uint32_t>(len), 0);
//                _hash_values[code] = hash_val;
//                _compute_hash_value_flags[code] = 1;
//                return _hash_values[code];
//            }
//        }
//
//        // For > , code takes upper_bound - 1; For >= , code takes upper_bound
//        // For < , code takes upper_bound; For <=, code takes upper_bound - 1
//        // For example a sorted dict: <'b',0> <'c',1> <'d',2>
//        // Now the predicate value is 'ccc', 'ccc' is not in the dict, 'ccc' is between 'c' and 'd'.
//        // std::upper_bound(..., 'ccc') - begin, will return the encoding of 'd', which is 2
//        // If the predicate is col > 'ccc' and the value of upper_bound-1 is 1,
//        //  then evaluate code > 1 and the result is 'd'.
//        // If the predicate is col < 'ccc' and the value of upper_bound is 2,
//        //  evaluate code < 2, and the return result is 'b'.
//        // If the predicate is col >= 'ccc' and the value of upper_bound is 2,
//        //  evaluate code >= 2, and the return result is 'd'.
//        // If the predicate is col <= 'ccc' and the value of upper_bound-1 is 1,
//        //  evaluate code <= 1, and the returned result is 'b'.
//        // If the predicate is col < 'a', 'a' is also not in the dict, and 'a' is less than 'b',
//        //  so upper_bound is the code 0 of b, then evaluate code < 0 and returns empty
//        // If the predicate is col <= 'a' and upper_bound-1 is -1,
//        //  then evaluate code <= -1 and returns empty
//        T find_code_by_bound(const StringRef& value, bool greater, bool eq) const {
//            auto code = find_code(value);
//            if (code >= 0) {
//                return code;
//            }
//            auto bound =
//                    static_cast<T>(std::upper_bound(_dict_data->begin(), _dict_data->end(), value) -
//                                   _dict_data->begin());
//            return greater ? bound - greater + eq : bound - eq;
//        }
//
//        template <typename HybridSetType>
//        void find_codes(const HybridSetType* values,
//                        std::vector<vectorized::UInt8>& selected) const {
//            size_t dict_word_num = _dict_data->size();
//            selected.resize(dict_word_num);
//            selected.assign(dict_word_num, false);
//            for (size_t i = 0; i < _dict_data->size(); i++) {
//                if (values->find(&((*_dict_data)[i]))) {
//                    selected[i] = true;
//                }
//            }
//        }
//
//        void clear() {
//            _dict_data->clear();
//            _code_convert_table.clear();
//            _hash_values.clear();
//            _compute_hash_value_flags.clear();
//        }
//
//        void clear_hash_values() {
//            _hash_values.clear();
//            _compute_hash_value_flags.clear();
//        }
//
//        void sort() {
//            size_t dict_size = _dict_data->size();
//
//            _code_convert_table.reserve(dict_size);
//            _perm.resize(dict_size);
//            for (size_t i = 0; i < dict_size; ++i) {
//                _perm[i] = i;
//            }
//
//            std::sort(_perm.begin(), _perm.end(),
//                      [&dict_data = *_dict_data, &comparator = _comparator](const size_t a,
//                                                                            const size_t b) {
//                          return comparator(dict_data[a], dict_data[b]);
//                      });
//
//            auto new_dict_data = new DictContainer(dict_size);
//            for (size_t i = 0; i < dict_size; ++i) {
//                _code_convert_table[_perm[i]] = (T)i;
//                (*new_dict_data)[i] = (*_dict_data)[_perm[i]];
//            }
//            _dict_data.reset(new_dict_data);
//        }
//
//        T convert_code(const T& code) const {
//            if (get_null_code() == code) {
//                return code;
//            }
//            return _code_convert_table[code];
//        }
//
//        size_t byte_size() { return _dict_data->size() * sizeof((*_dict_data)[0]); }
//
//        bool empty() const { return _dict_data->empty(); }
//
//        size_t avg_str_len() { return empty() ? 0 : _total_str_len / _dict_data->size(); }
//
//        size_t size() const {
//            if (!_dict_data) {
//                return 0;
//            }
//            return _dict_data->size();
//        }
//
//        std::string debug_string() const {
//            std::string str = "[";
//            if (_dict_data) {
//                for (size_t i = 0; i < _dict_data->size(); i++) {
//                    if (i) {
//                        str += ',';
//                    }
//                    str += (*_dict_data)[i].to_string();
//                }
//            }
//            str += ']';
//            return str;
//        }
//
//    private:
//        StringRef::Comparator _comparator;
//        // dict code -> dict value
//        std::unique_ptr<DictContainer> _dict_data;
//        std::vector<T> _code_convert_table;
//        // hash value of origin string , used for bloom filter
//        // It's a trade-off of space for performance
//        // But in TPC-DS 1GB q60,we see no significant improvement.
//        // This may because the magnitude of the data is not large enough(in q60, only about 80k rows data is filtered for largest table)
//        // So we may need more test here.
//        mutable HashValueContainer _hash_values;
//        mutable std::vector<uint8> _compute_hash_value_flags;
//        IColumn::Permutation _perm;
//        size_t _total_str_len;
//    };

//private:
//    size_t _reserve_size;
//    bool _dict_sorted = false;
//    bool _dict_code_converted = false;
//    Dictionary _dict;
//    Container _codes;
//    FieldType _type;
//    std::pair<RowsetId, uint32_t> _rowset_segment_id;
//    std::vector<StringRef> _strings;

public:
//    DictionaryColumn(ColumnPtr dictionary, std::vector<int32_t> ids, bool dictionary_is_compacted = false, bool is_sequential_ids = false)
//            : _dictionary(std::move(dictionary)), _ids(std::move(ids)), _dictionary_is_compacted(dictionary_is_compacted), _is_sequential_ids(is_sequential_ids) {
//        if (_dictionary_is_compacted) {
//            _unique_ids = _dictionary->size();
//            _size_in_bytes = _dictionary->byte_size() + (sizeof(int32_t) * _ids.size());
//        }
//    }

//    using Base = COWHelper<IColumn, DictionaryColumn>;
//    static Ptr create(int row_count, const ColumnPtr& dictionary, const std::vector<int32_t>& ids) {
//        return DictionaryColumn::create(row_count, dictionary, ids);
//    }
//
//    template <typename... Args, typename = std::enable_if_t<IsMutableColumns<Args...>::value>>
//    static MutablePtr create(Args&&... args) {
//        return Base::create(std::forward<Args>(args)...);
//    }

//    // 创建字典列
//    static ColumnPtr create(int row_count, const ColumnPtr& dictionary, const std::vector<int32_t>& ids) {
//        return create_internal(0, row_count, dictionary, ids, DictionaryId::randomDictionaryId());
//    }
//
//    // 创建投影字典列
//    static ColumnPtr create_projected_dictionary_block(int row_count, ColumnPtr dictionary, const std::vector<int32_t>& ids, DictionaryId dictionary_source_id) {
//        return create_internal(0, row_count, dictionary, ids, dictionary_source_id);
//    }
//
//    // 内部创建方法
//    static ColumnPtr create_internal(int ids_offset, int row_count, const ColumnPtr& dictionary,
//                                    const std::vector<int32_t>& ids, DictionaryId dictionary_source_id) {
//        auto ptr = std::make_shared<DictionaryColumn>(ids_offset, row_count, dictionary, ids,
//                                                     false, false, dictionary_source_id);
//        return ColumnPtr(ptr);
//    }


    // 获取指定位置的值
    MutableColumnPtr get_shrinked_column() const{
//        auto new_dictionary = _dictionary->get_shrinked_column();
//        std::vector<int32_t> new_ids(_ids);
//        return DictionaryColumn::create(std::move(new_dictionary), std::move(new_ids));
            return {};
    }

    // 获取字典列
    ColumnPtr get_dictionary() const { return _dictionary; }

    // 获取 ID 数组
    const std::vector<int32_t>& get_ids() const { return _ids; }

    // 压缩字典列
//    ColumnPtr compact() const {
//        if (is_compact()) {
//            return shared_from_this();
//        }
//
//        // 确定哪些字典项被引用并构建重新索引
//        std::vector<int32_t> dictionary_positions_to_copy;
//        std::unordered_map<int32_t, int32_t> remap_index;
//        int new_index = 0;
//
//        for (int32_t id : _ids) {
//            if (remap_index.find(id) == remap_index.end()) {
//                remap_index[id] = new_index++;
//                dictionary_positions_to_copy.push_back(id);
//            }
//        }
//
//        // 如果所有字典项都被引用，则无需压缩
//        if (dictionary_positions_to_copy.size() == _dictionary->size()) {
//            return shared_from_this();
//        }
//
//        // 压缩字典
//        std::vector<int32_t> new_ids(_ids.size());
//        for (size_t i = 0; i < _ids.size(); ++i) {
//            new_ids[i] = remap_index[_ids[i]];
//        }
//
//        auto compact_dictionary = _dictionary->copy_positions(dictionary_positions_to_copy);
//        return DictionaryColumn::create(std::move(compact_dictionary), std::move(new_ids), true, _unique_ids == _ids.size());
//    }

//    // 检查字典是否已压缩
//    bool is_compact() const {
//        if (_unique_ids == -1) {
//            calculate_compact_size();
//        }
//        return _unique_ids == _dictionary->size();
//    }
//
//    // 获取唯一 ID 的数量
//    int get_unique_ids() const {
//        if (_unique_ids == -1) {
//            calculate_compact_size();
//        }
//        return _unique_ids;
//    }

    // 获取字典列的字节大小
//    size_t byte_size() const override {
//        if (_size_in_bytes == -1) {
//            calculate_compact_size();
//        }
//        return _size_in_bytes;
//    }

    // 获取逻辑字节大小
//    size_t get_logical_size_in_bytes() const {
//        if (_logical_size_in_bytes >= 0) {
//            return _logical_size_in_bytes;
//        }
//
//        // 计算逻辑大小
//        size_t size_in_bytes = 0;
//        std::vector<size_t> seen_sizes(_dictionary->size(), -1);
//        for (int32_t id : _ids) {
//            if (seen_sizes[id] == -1) {
//                seen_sizes[id] = _dictionary->get_region_size_in_bytes(id, 1);
//            }
//            size_in_bytes += seen_sizes[id];
//        }
//
//        _logical_size_in_bytes = size_in_bytes;
//        return size_in_bytes;
//    }

    // 获取指定区域的字节大小
//    size_t get_region_size_in_bytes(size_t position_offset, size_t length) const {
//        if (position_offset == 0 && length == size()) {
//            return byte_size();
//        }
//
//        int unique_ids = 0;
//        std::vector<bool> used_ids(_dictionary->size(), false);
//        for (size_t i = position_offset; i < position_offset + length; ++i) {
//            int32_t id = _ids[i];
//            if (!used_ids[id]) {
//                unique_ids++;
//                used_ids[id] = true;
//            }
//        }
//
//        return _dictionary->get_positions_size_in_bytes(used_ids, unique_ids) + (sizeof(int32_t) * length);
//    }
//
//    // 获取保留的字节大小
//    size_t get_retained_size_in_bytes() const override {
//        return _dictionary->get_retained_size_in_bytes() + (_ids.size() * sizeof(int32_t));
//    }
//
//    // 获取字典列的哈希值
//    uint32_t get_hash_value(size_t n) const {
//        return _dictionary->get_hash_value(_ids[n]);
//    }

//    // 获取字典列的排序状态
//    bool is_sorted() const {
//        return _is_sorted;
//    }
//
//    // 对字典列进行排序
//    void sort_dictionary() {
//        if (_is_sorted) {
//            return;
//        }
//
//        // 实现字典排序逻辑
//        // ...
//        _is_sorted = true;
//    }

private:
//    // 计算压缩大小
//    void calculate_compact_size() const {
//        int unique_ids = 0;
//        std::vector<bool> used_ids(_dictionary->size(), false);
//        bool is_sequential_ids = true;
//        int previous_id = -1;
//
//        for (int32_t id : _ids) {
//            if (!used_ids[id]) {
//                unique_ids++;
//                used_ids[id] = true;
//            }
//            if (is_sequential_ids) {
//                is_sequential_ids = previous_id < id;
//                previous_id = id;
//            }
//        }
//
//        _unique_ids = unique_ids;
//        _is_sequential_ids = is_sequential_ids;
//        _size_in_bytes = _dictionary->get_positions_size_in_bytes(used_ids, unique_ids) + (sizeof(int32_t) * _ids.size());
//    }

    int _ids_offset; // ID 数组的偏移量
    int _row_count; // 位置数量
    ColumnPtr _dictionary; // 字典列
    std::vector<int32_t> _ids; // ID 数组
    bool _dictionary_is_compacted; // 字典是否已压缩
    bool _is_sequential_ids; // ID 是否连续
    DictionaryId _dictionary_source_id; // 字典源 ID
    mutable int _unique_ids = -1; // 唯一 ID 的数量
    mutable size_t _size_in_bytes = -1; // 字节大小
};

} // namespace doris::vectorized