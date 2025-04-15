#include "gtest/gtest.h"
#include "vec/exec/format/column_adaptation.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/columns/column_vector.h"
#include "vec/columns/column_const.h"
#include "vec/core/block.h"

namespace doris::vectorized {

//class MockMaskDeletedRowsFunction : public MaskDeletedRowsFunction {
//public:
//    ColumnPtr apply(const ColumnPtr& column) override {
//        return column;
//    }
//
//    int get_position_count() const override {
//        return _position_count;
//    }
//
//    void set_position_count(int count) {
//        _position_count = count;
//    }
//
//private:
//    int _position_count = 0;
//};

//TEST(ColumnAdaptationTest, NullColumnTest) {
//    auto data_type = DataTypeFactory::instance().create_data_type(
//            TypeIndex::Int32, true);
//    auto null_column_adaptation = std::make_shared<NullColumn>(data_type);
//
//    // 创建一个包含5行的空Block
//    Block block;
//    block.insert({ColumnInt32::create(5), data_type, "dummy"});
//
//    auto result = null_column_adaptation->column(block, std::nullopt);
//    ASSERT_EQ(result->size(), 5);
//    ASSERT_TRUE(is_column_const(*result));
//
//    // 检查所有值是否为 null
//    for (size_t i = 0; i < result->size(); ++i) {
//        ASSERT_TRUE(result->is_null_at(i));
//    }
//}
//
//TEST(ColumnAdaptationTest, SourceColumnTest) {
//    // 创建测试数据
//    auto col1 = ColumnInt32::create();
//    auto col2 = ColumnInt64::create();
//
//    // 填充数据
//    for (int i = 0; i < 5; ++i) {
//        col1->insert(i);
//        col2->insert(i * 10);
//    }
//
//    // 创建Block
//    Block block;
//    block.insert({std::move(col1), DataTypeFactory::instance().create_data_type(
//                                           TypeIndex::Int32, true), "col1"});
//    block.insert({std::move(col2), DataTypeFactory::instance().create_data_type(
//                                           TypeIndex::Int64, true), "col2"});
//
//    // 测试获取第0列
//    auto source_col0 = std::make_shared<SourceColumn>(0);
//    auto result_col0 = source_col0->column(block, std::nullopt);
//    ASSERT_EQ(result_col0->size(), 5);
//    for (int i = 0; i < 5; ++i) {
//        ASSERT_EQ(result_col0->get_int(i), i);
//    }
//
//    // 测试获取第1列
//    auto source_col1 = std::make_shared<SourceColumn>(1);
//    auto result_col1 = source_col1->column(block, std::nullopt);
//    ASSERT_EQ(result_col1->size(), 5);
//    for (int i = 0; i < 5; ++i) {
//        ASSERT_EQ(result_col1->get_int(i), i * 10);
//    }
//}

//TEST(ColumnAdaptationTest, SourceColumnInvalidIndexTest) {
//    // 创建空Block
//    Block block;
//
//    // 测试无效索引
//    EXPECT_THROW(std::make_shared<SourceColumn>(-1), std::exception);
//
//    // 测试超出范围的索引
//    auto source_col = std::make_shared<SourceColumn>(0);
//    EXPECT_THROW(source_col->column(block, std::nullopt), std::exception);
//}

//TEST(ColumnAdaptationTest, PositionAdaptationTest) {
//    auto position_adaptation = ColumnAdaptation::position_column();
//
//    MockMaskDeletedRowsFunction mask_function;
//    mask_function.set_position_count(4);
//
//    auto source_column = ColumnInt32::create();
//    source_column->insert(1);
//    source_column->insert(2);
//    source_column->insert(3);
//    source_column->insert(4);
//
//    auto result = position_adaptation->column(source_column, mask_function, 10, std::nullopt);
//    ASSERT_EQ(result->size(), 4);
//
//    auto& data = assert_cast<ColumnInt64&>(*result).get_data();
//    ASSERT_EQ(data[0], 10);
//    ASSERT_EQ(data[1], 11);
//    ASSERT_EQ(data[2], 12);
//    ASSERT_EQ(data[3], 13);
//}

//TEST(ColumnAdaptationTest, ConstantAdaptationTest) {
//    // 创建包含单个值的列
//    auto column = ColumnInt32::create();
//    column->insert(42);
//
//    // 将列转换为 ColumnPtr 类型
//    ColumnPtr column_ptr = std::move(column);
//
//    // 创建 ConstantAdaptation 实例
//    auto constant_adaptation = std::make_shared<ConstantAdaptation>(column_ptr);
//
//    // 创建测试 block，包含 3 行
//    Block block;
//    block.insert({ColumnInt32::create(3), DataTypeFactory::instance().create_data_type(TypeIndex::Int32, true), "dummy"});
//
//    // 获取结果列
//    auto result = constant_adaptation->column(block, std::nullopt);
//
//    // 验证结果
//    ASSERT_EQ(result->size(), 3);  // 列长度应与 block 行数一致
//    ASSERT_TRUE(is_column_const(*result));  // 应该是常量列
//    ASSERT_EQ(assert_cast<const ColumnConst&>(*result).get_int(0), 42);  // 值应为 42
//}

} // namespace doris::vectorized
