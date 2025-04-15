#include "gtest/gtest.h"
#include "vec/columns/columns_number.h"
#include "vec/columns/lazy_column.h"

namespace doris::vectorized {

class LazyColumnTest : public testing::Test {

    // 自定义 LazyColumnLoader 实现，用于测试
    class TestLazyColumnLoader : public LazyColumnLoader {
    public:
        TestLazyColumnLoader(ColumnPtr column) : _column(std::move(column)) {}

        Result<ColumnPtr> load() override {
            return _column;
        }

    private:
        ColumnPtr _column;
    };
};

TEST(LazyColumnTest, BasicFunctionality) {
    // 创建一个测试列
    auto test_column = ColumnInt32::create();
    test_column->insert(1);
    test_column->insert(2);
    test_column->insert(3);

    ColumnPtr column_ptr = std::move(test_column);

    // 创建 LazyColumnLoader
    auto loader = std::make_shared<LazyColumnTest::TestLazyColumnLoader>(column_ptr);

    // 创建 LazyColumn
    LazyColumn lazy_column(loader);

//    // 测试 size()
//    EXPECT_EQ(lazy_column.size(), 3);

    // 测试 get_data_at()
    EXPECT_EQ(std::to_string(*reinterpret_cast<const int32_t*>(lazy_column.get_data_at(0).data)), "1");
    EXPECT_EQ(std::to_string(*reinterpret_cast<const int32_t*>(lazy_column.get_data_at(1).data)), "2");
    EXPECT_EQ(std::to_string(*reinterpret_cast<const int32_t*>(lazy_column.get_data_at(2).data)), "3");

//    // 测试 clone_empty()
//    auto empty_clone = lazy_column.clone_empty();
//    EXPECT_EQ(empty_clone->size(), 0);

    // 测试 clear()
    lazy_column.clear();
    EXPECT_EQ(lazy_column.size(), 0);
}

TEST(LazyColumnTest, LoadOnDemand) {
    // 创建一个测试列
    auto test_column = ColumnInt32::create();
    test_column->insert(10);
    test_column->insert(20);

    ColumnPtr column_ptr = std::move(test_column);

    // 创建 LazyColumnLoader
    auto loader = std::make_shared<LazyColumnTest::TestLazyColumnLoader>(column_ptr);

    // 创建 LazyColumn
    LazyColumn lazy_column(loader);

    // 测试加载前和加载后的行为
    EXPECT_FALSE(lazy_column.is_loaded());
    EXPECT_EQ(std::to_string(*reinterpret_cast<const int32_t*>(lazy_column.get_data_at(0).data)), "10"); // 触发加载
    EXPECT_TRUE(lazy_column.is_loaded());
}

} // namespace doris::vectorized 