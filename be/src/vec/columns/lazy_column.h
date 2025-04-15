#pragma once

#include "vec/columns/column.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include <memory>
#include <vector>
#include <functional>

namespace doris::vectorized {

class LazyColumnLoader {
public:
    virtual ~LazyColumnLoader() = default;

    /**
     * Loads a lazy column. If possible, the loader should load the top-level column only.
     */
    virtual Result<ColumnPtr> load() = 0;
};

class LazyColumn final : public COWHelper<IColumn, LazyColumn> {

private:
    friend class COWHelper<IColumn, LazyColumn>;

    LazyColumn(std::shared_ptr<LazyColumnLoader> loader)
        :_lazy_data(std::make_shared<LazyData>(std::move(loader))) {}

    size_t size() const override {
        if (_size > 0) {
            return _size;
        }
        return get_column()->size();
    }

    void set_size(size_t size){
        _size = size;
    }

    Field operator[](size_t n) const override {
        return get_column()->operator[](n);
    }

    void get(size_t n, Field& res) const override {
        get_column()->get(n, res);
    }

    StringRef get_data_at(size_t n) const override {
        return get_column()->get_data_at(n);
    }

    void insert(const Field& x) override {
        get_mutable_column()->insert(x);
    }

    void insert_range_from(const IColumn& src, size_t start, size_t length) override {
        get_mutable_column()->insert_range_from(src, start, length);
    }

    void insert_data(const char* pos, size_t length) override {
        get_mutable_column()->insert_data(pos, length);
    }

    void insert_default() override {
        get_mutable_column()->insert_default();
    }

    size_t filter(const Filter& filter) override {
        return get_mutable_column()->filter(filter);
    }

    void pop_back(size_t n) override {
        get_mutable_column()->pop_back(n);
    }

    StringRef serialize_value_into_arena(size_t n, Arena& arena, char const*& begin) const override {
        return get_column()->serialize_value_into_arena(n, arena, begin);
    }

    const char* deserialize_and_insert_from_arena(const char* pos) override {
        return get_mutable_column()->deserialize_and_insert_from_arena(pos);
    }

    void update_hash_with_value(size_t n, SipHash& hash) const override {
        get_column()->update_hash_with_value(n, hash);
    }

    ColumnPtr filter(const Filter& filt, ssize_t result_size_hint) const override {
        return get_column()->filter(filt, result_size_hint);
    }

    ColumnPtr permute(const Permutation& perm, size_t limit) const override {
        return get_column()->permute(perm, limit);
    }

    ColumnPtr replicate(const Offsets& offsets) const override {
        return get_column()->replicate(offsets);
    }

    void get_permutation(bool reverse, size_t limit, int nan_direction_hint, Permutation& res) const override {
        get_column()->get_permutation(reverse, limit, nan_direction_hint, res);
    }

//    ColumnPtr cut(size_t start, size_t length) const override {
//        return get_column()->cut(start, length);
//    }

    void clear() override {
        _lazy_data->clear();
    }

//    bool is_lazy() const override {
//        return true;
//    }

    bool is_loaded() const {
        return _lazy_data->is_fully_loaded();
    }
//
//    ColumnPtr get_loaded_column() {
//        return _lazy_data->get_fully_loaded_column();
//    }

    ColumnPtr get_column() const {
        return _lazy_data->get_top_level_column();
    }

    MutableColumnPtr get_mutable_column() const {
        ColumnPtr column_ptr = get_column();
        MutableColumnPtr mutate_column_ptr = std::move(*column_ptr).mutate();
        return mutate_column_ptr;
    }

    std::string get_name() const override {
        return "LazyColumn";
    }

    void insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
                             const uint32_t* indices_end) override {
        get_mutable_column()->insert_indices_from(src, indices_begin, indices_end);
    }

    size_t byte_size() const override {
        return get_column()->byte_size();
    }

    size_t allocated_bytes() const override {
        return get_column()->allocated_bytes();
    }

    bool has_enough_capacity(const IColumn& src) const override {
        return get_column()->has_enough_capacity(src);
    }

    void replace_column_data(const IColumn& src, size_t start, size_t length) override {
        get_mutable_column()->replace_column_data(src, start, length);
    }

private:
    class LazyData {
    public:
        LazyData(std::shared_ptr<LazyColumnLoader> loader)
            : _loader(std::move(loader)) {}

        bool is_fully_loaded() const {
//            return _column && _column->is_loaded();
            return _column;
        }

        bool is_top_level_column_loaded() const {
            return _column;
        }

        ColumnPtr get_top_level_column() {
            load(false);
            return _column;
        }

//        ColumnPtr get_fully_loaded_column() {
//            if (_column) {
//                return _column->get_loaded_column();
//            }
//
//            load(true);
//            return _column;
//        }

        void clear() {
            _column = nullptr;
            _loader = nullptr;
        }

    private:
        void load(bool recursive) {
            if (_loader == nullptr) {
                return;
            }

            auto result = _loader->load();
            if (!result.has_value()) {
                throw std::runtime_error("Failed to load lazy column");
            }
            _column = result.value();
//            if (_column->size() != _size) {
//                throw std::runtime_error("Loaded column size does not match lazy column size");
//            }

//            if (recursive) {
//                _column = _column->get_loaded_column();
//            } else {
                // Load and remove directly nested lazy columns
                while (is_column<LazyColumn>(*_column)) {
                    _column = assert_cast<const LazyColumn&>(*_column).get_column();
                }
//            }

            // Clear reference to loader to free resources
            _loader = nullptr;
        }

//        size_t _position_count;
        std::shared_ptr<LazyColumnLoader> _loader;
        ColumnPtr _column;
    };

    size_t _size{0};
    std::shared_ptr<LazyData> _lazy_data;
};

} // namespace doris::vectorized