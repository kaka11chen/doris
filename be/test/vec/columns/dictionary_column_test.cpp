#include "vec/columns/dictionary_column.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/column_string.h"
#include "gtest/gtest.h"

namespace doris::vectorized {

TEST(DictionaryColumnTest, BasicTest) {
    // 创建字典列
    auto dictionary = ColumnString::create();
    dictionary->insert_data("apple", 5);
    dictionary->insert_data("banana", 6);
    dictionary->insert_data("cherry", 6);

    // 将列转换为 ColumnPtr 类型
    ColumnPtr column_dict_ptr = std::move(dictionary);

    std::vector<int32_t> ids = {0, 1, 2, 0, 1};
//    auto dict_column = DictionaryColumn<int32_t>::create(0, ids.size(), column_dict_ptr, ids, false, false, DictionaryId::randomDictionaryId());
    auto dict_column = DictionaryColumn<int32_t>::create(0, ids.size(), column_dict_ptr, ids, false, false, DictionaryId::randomDictionaryId());


    // 测试基本功能
    EXPECT_EQ(dict_column->size(), 5);
    EXPECT_EQ(dict_column->get_dictionary()->size(), 3);

    // 测试获取值
    Field field;
    dict_column->get(0, field);
    EXPECT_EQ(field.get<String>(), "apple");
    dict_column->get(1, field);
    EXPECT_EQ(field.get<String>(), "banana");
    dict_column->get(2, field);
    EXPECT_EQ(field.get<String>(), "cherry");
    dict_column->get(3, field);
    EXPECT_EQ(field.get<String>(), "apple");
    dict_column->get(4, field);
    EXPECT_EQ(field.get<String>(), "banana");

    ColumnPtr column;
    EXPECT_FALSE(column);
}

//TEST(DictionaryColumnTest, InvalidInputTest) {
//    // 测试非法输入
//    auto dictionary = ColumnString::create();
//    dictionary->insert_data("test", 4);
//
//    std::vector<int32_t> ids = {0, 1}; // 1 超出范围
//    EXPECT_DEATH(DictionaryColumn<int32_t>::create(ids.size(), dictionary, ids), ".*ids length is less than positionCount.*");
//
//    // 测试负数的 position_count
//    EXPECT_DEATH(DictionaryColumn<int32_t>::create(-1, dictionary, ids), ".*positionCount is negative.*");
//}
//
//TEST(DictionaryColumnTest, CompactedDictionaryTest) {
//    // 测试压缩字典
//    auto dictionary = ColumnString::create();
//    dictionary->insert_data("one", 3);
//    dictionary->insert_data("two", 3);
//    dictionary->insert_data("three", 5);
//
//    std::vector<int32_t> ids = {0, 1, 2, 0, 1};
//    auto dict_column = DictionaryColumn<int32_t>::create(ids.size(), dictionary, ids, true, false);
//
//    EXPECT_TRUE(dict_column->is_compact());
//    EXPECT_EQ(dict_column->get_unique_ids(), 3);
//}

} // namespace doris::vectorized 