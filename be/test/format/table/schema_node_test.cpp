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

#include "format/table/table_schema_change_helper.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

namespace doris {

using Node = TableSchemaChangeHelper::Node;
using StructNode = TableSchemaChangeHelper::StructNode;
using ConstNode = TableSchemaChangeHelper::ConstNode;
using ScalarNode = TableSchemaChangeHelper::ScalarNode;
using ArrayNode = TableSchemaChangeHelper::ArrayNode;
using MapNode = TableSchemaChangeHelper::MapNode;

// ============================================================================
// Base Node — all methods should throw
// ============================================================================
class SchemaNodeTest : public ::testing::Test {};

TEST_F(SchemaNodeTest, BaseNodeGetChildrenThrows) {
    Node node;
    EXPECT_THROW(node.get_children_node("col"), std::logic_error);
}

TEST_F(SchemaNodeTest, BaseNodeGetChildrenByFileNameThrows) {
    Node node;
    EXPECT_THROW(node.get_children_node_by_file_column_name("col"), std::logic_error);
}

TEST_F(SchemaNodeTest, BaseNodeChildrenFileColumnNameThrows) {
    Node node;
    EXPECT_THROW(node.children_file_column_name("col"), std::logic_error);
}

TEST_F(SchemaNodeTest, BaseNodeChildrenColumnExistsThrows) {
    Node node;
    EXPECT_THROW(node.children_column_exists("col"), std::logic_error);
}

TEST_F(SchemaNodeTest, BaseNodeGetElementNodeThrows) {
    Node node;
    EXPECT_THROW(node.get_element_node(), std::logic_error);
}

TEST_F(SchemaNodeTest, BaseNodeGetKeyNodeThrows) {
    Node node;
    EXPECT_THROW(node.get_key_node(), std::logic_error);
}

TEST_F(SchemaNodeTest, BaseNodeGetValueNodeThrows) {
    Node node;
    EXPECT_THROW(node.get_value_node(), std::logic_error);
}

TEST_F(SchemaNodeTest, BaseNodeAddNotExistChildrenThrows) {
    Node node;
    EXPECT_THROW(node.add_not_exist_children("col"), std::logic_error);
}

TEST_F(SchemaNodeTest, BaseNodeAddChildrenThrows) {
    Node node;
    EXPECT_THROW(node.add_children("t", "f", std::make_shared<ScalarNode>()), std::logic_error);
}

// ============================================================================
// ConstNode — returns self for all operations
// ============================================================================
TEST_F(SchemaNodeTest, ConstNodeSingleton) {
    auto inst1 = ConstNode::get_instance();
    auto inst2 = ConstNode::get_instance();
    EXPECT_EQ(inst1.get(), inst2.get());
}

TEST_F(SchemaNodeTest, ConstNodeGetChildrenReturnsSelf) {
    auto node = ConstNode::get_instance();
    auto child = node->get_children_node("anything");
    EXPECT_EQ(child.get(), node.get());
}

TEST_F(SchemaNodeTest, ConstNodeGetChildrenByFileNameReturnsSelf) {
    auto node = ConstNode::get_instance();
    auto child = node->get_children_node_by_file_column_name("anything");
    EXPECT_EQ(child.get(), node.get());
}

TEST_F(SchemaNodeTest, ConstNodeChildrenFileColumnNameIdentity) {
    auto node = ConstNode::get_instance();
    // ConstNode treats table column name == file column name (no schema change)
    EXPECT_EQ(node->children_file_column_name("my_col"), "my_col");
    EXPECT_EQ(node->children_file_column_name("CamelCase"), "CamelCase");
}

TEST_F(SchemaNodeTest, ConstNodeSkipsExistenceCheckForIdentityMapping) {
    auto node = ConstNode::get_instance();
    // ConstNode is the no-schema-change sentinel: it does not verify real file
    // existence, it only signals "use the same child name directly".
    EXPECT_TRUE(node->children_column_exists("any_name"));
    EXPECT_TRUE(node->children_column_exists("nonexistent"));
}

TEST_F(SchemaNodeTest, ConstNodeGetElementReturnsSelf) {
    auto node = ConstNode::get_instance();
    EXPECT_EQ(node->get_element_node().get(), node.get());
}

TEST_F(SchemaNodeTest, ConstNodeGetKeyReturnsSelf) {
    auto node = ConstNode::get_instance();
    EXPECT_EQ(node->get_key_node().get(), node.get());
}

TEST_F(SchemaNodeTest, ConstNodeGetValueReturnsSelf) {
    auto node = ConstNode::get_instance();
    EXPECT_EQ(node->get_value_node().get(), node.get());
}

// ============================================================================
// ScalarNode — inherits ConstNode behavior
// ============================================================================
TEST_F(SchemaNodeTest, ScalarNodeInheritsConstBehavior) {
    auto scalar = std::make_shared<ScalarNode>();
    // ScalarNode should act like ConstNode but is a distinct type
    EXPECT_TRUE(scalar->children_column_exists("col"));
    EXPECT_EQ(scalar->children_file_column_name("col"), "col");
    // get_element_node returns ConstNode singleton (not the ScalarNode itself)
    auto elem = scalar->get_element_node();
    EXPECT_EQ(elem.get(), ConstNode::get_instance().get());
}

// ============================================================================
// StructNode — column mapping
// ============================================================================
TEST_F(SchemaNodeTest, StructNodeAddAndGetChildren) {
    auto root = std::make_shared<StructNode>();
    auto child = std::make_shared<ScalarNode>();
    root->add_children("table_col", "file_col", child);

    EXPECT_TRUE(root->children_column_exists("table_col"));
    EXPECT_EQ(root->children_file_column_name("table_col"), "file_col");
    EXPECT_EQ(root->get_children_node("table_col").get(), child.get());
}

TEST_F(SchemaNodeTest, StructNodeNotExistChildren) {
    auto root = std::make_shared<StructNode>();
    root->add_not_exist_children("missing_col");

    // Column registered but marked as not existing
    EXPECT_FALSE(root->children_column_exists("missing_col"));
}

TEST_F(SchemaNodeTest, StructNodeMultipleChildren) {
    auto root = std::make_shared<StructNode>();
    auto child1 = std::make_shared<ScalarNode>();
    auto child2 = std::make_shared<ScalarNode>();
    root->add_children("col_a", "file_a", child1);
    root->add_children("col_b", "file_b", child2);
    root->add_not_exist_children("col_c");

    EXPECT_TRUE(root->children_column_exists("col_a"));
    EXPECT_TRUE(root->children_column_exists("col_b"));
    EXPECT_FALSE(root->children_column_exists("col_c"));

    EXPECT_EQ(root->children_file_column_name("col_a"), "file_a");
    EXPECT_EQ(root->children_file_column_name("col_b"), "file_b");
    EXPECT_EQ(root->get_children_node("col_a").get(), child1.get());
    EXPECT_EQ(root->get_children_node("col_b").get(), child2.get());
}

TEST_F(SchemaNodeTest, StructNodeGetChildrenByFileColumnName) {
    auto root = std::make_shared<StructNode>();
    auto child = std::make_shared<ScalarNode>();
    root->add_children("table_col", "file_col", child);

    auto found = root->get_children_node_by_file_column_name("file_col");
    EXPECT_EQ(found.get(), child.get());
}

TEST_F(SchemaNodeTest, StructNodeGetChildrenByFileColumnNameNotFound) {
    auto root = std::make_shared<StructNode>();
    auto child = std::make_shared<ScalarNode>();
    root->add_children("table_col", "file_col", child);

    EXPECT_THROW(root->get_children_node_by_file_column_name("no_such_file_col"),
                 std::runtime_error);
}

TEST_F(SchemaNodeTest, StructNodeNestedStruct) {
    // table: struct<a: struct<x: int, y: int>>
    auto inner = std::make_shared<StructNode>();
    inner->add_children("x", "X", std::make_shared<ScalarNode>());
    inner->add_children("y", "Y", std::make_shared<ScalarNode>());

    auto outer = std::make_shared<StructNode>();
    outer->add_children("a", "A", inner);

    auto a_node = outer->get_children_node("a");
    EXPECT_EQ(a_node.get(), inner.get());
    EXPECT_TRUE(a_node->children_column_exists("x"));
    EXPECT_EQ(a_node->children_file_column_name("x"), "X");
}

TEST_F(SchemaNodeTest, StructNodeGetChildren) {
    auto root = std::make_shared<StructNode>();
    root->add_children("c1", "f1", std::make_shared<ScalarNode>());
    root->add_children("c2", "f2", std::make_shared<ScalarNode>());
    root->add_not_exist_children("c3");

    auto& children = root->get_children();
    EXPECT_EQ(children.size(), 3);
    EXPECT_TRUE(children.contains("c1"));
    EXPECT_TRUE(children.contains("c2"));
    EXPECT_TRUE(children.contains("c3"));
}

// ============================================================================
// ArrayNode — element access
// ============================================================================
TEST_F(SchemaNodeTest, ArrayNodeGetElement) {
    auto elem = std::make_shared<ScalarNode>();
    auto arr = std::make_shared<ArrayNode>(elem);

    EXPECT_EQ(arr->get_element_node().get(), elem.get());
}

TEST_F(SchemaNodeTest, ArrayNodeGetKeyThrows) {
    auto arr = std::make_shared<ArrayNode>(std::make_shared<ScalarNode>());
    EXPECT_THROW(arr->get_key_node(), std::logic_error);
}

TEST_F(SchemaNodeTest, ArrayNodeGetValueThrows) {
    auto arr = std::make_shared<ArrayNode>(std::make_shared<ScalarNode>());
    EXPECT_THROW(arr->get_value_node(), std::logic_error);
}

TEST_F(SchemaNodeTest, ArrayNodeNestedStruct) {
    // Array<Struct<a: int, b: string>>
    auto inner = std::make_shared<StructNode>();
    inner->add_children("a", "A", std::make_shared<ScalarNode>());
    inner->add_children("b", "B", std::make_shared<ScalarNode>());

    auto arr = std::make_shared<ArrayNode>(inner);
    auto elem = arr->get_element_node();
    EXPECT_TRUE(elem->children_column_exists("a"));
    EXPECT_EQ(elem->children_file_column_name("a"), "A");
}

// ============================================================================
// MapNode — key/value access
// ============================================================================
TEST_F(SchemaNodeTest, MapNodeGetKeyAndValue) {
    auto key = std::make_shared<ScalarNode>();
    auto val = std::make_shared<ScalarNode>();
    auto map = std::make_shared<MapNode>(key, val);

    EXPECT_EQ(map->get_key_node().get(), key.get());
    EXPECT_EQ(map->get_value_node().get(), val.get());
}

TEST_F(SchemaNodeTest, MapNodeGetElementThrows) {
    auto map = std::make_shared<MapNode>(std::make_shared<ScalarNode>(),
                                          std::make_shared<ScalarNode>());
    EXPECT_THROW(map->get_element_node(), std::logic_error);
}

TEST_F(SchemaNodeTest, MapNodeComplexValueType) {
    // Map<string, struct<x: int, y: array<int>>>
    auto inner_arr = std::make_shared<ArrayNode>(std::make_shared<ScalarNode>());
    auto inner_struct = std::make_shared<StructNode>();
    inner_struct->add_children("x", "x", std::make_shared<ScalarNode>());
    inner_struct->add_children("y", "y", inner_arr);

    auto map = std::make_shared<MapNode>(std::make_shared<ScalarNode>(), inner_struct);

    auto val = map->get_value_node();
    EXPECT_TRUE(val->children_column_exists("x"));
    EXPECT_TRUE(val->children_column_exists("y"));
    auto y_node = val->get_children_node("y");
    // y is an ArrayNode
    auto y_elem = y_node->get_element_node();
    EXPECT_NE(y_elem, nullptr);
}

// ============================================================================
// Deep nesting: Struct<Array<Map<String, Struct<...>>>>
// ============================================================================
TEST_F(SchemaNodeTest, DeepNesting5Levels) {
    // Level 5: scalar leaf
    auto leaf = std::make_shared<ScalarNode>();
    // Level 4: struct with leaf
    auto level4 = std::make_shared<StructNode>();
    level4->add_children("val", "val_file", leaf);
    // Level 3: map<string, level4>
    auto level3 = std::make_shared<MapNode>(std::make_shared<ScalarNode>(), level4);
    // Level 2: array<level3>
    auto level2 = std::make_shared<ArrayNode>(level3);
    // Level 1: struct with level2
    auto root = std::make_shared<StructNode>();
    root->add_children("data", "data_file", level2);

    // Navigate: root -> data -> element(map) -> value(struct) -> val(scalar)
    auto data_node = root->get_children_node("data");       // ArrayNode
    auto map_node = data_node->get_element_node();           // MapNode
    auto struct_node = map_node->get_value_node();           // StructNode (level4)
    EXPECT_TRUE(struct_node->children_column_exists("val"));
    EXPECT_EQ(struct_node->children_file_column_name("val"), "val_file");
}

// ============================================================================
// ColumnIdResult
// ============================================================================
TEST_F(SchemaNodeTest, ColumnIdResultDefault) {
    ColumnIdResult result;
    EXPECT_TRUE(result.column_ids.empty());
    EXPECT_TRUE(result.filter_column_ids.empty());
}

TEST_F(SchemaNodeTest, ColumnIdResultWithValues) {
    ColumnIdResult result({1, 2, 3}, {2, 3});
    EXPECT_EQ(result.column_ids.size(), 3);
    EXPECT_EQ(result.filter_column_ids.size(), 2);
    EXPECT_TRUE(result.column_ids.contains(1));
    EXPECT_TRUE(result.filter_column_ids.contains(2));
    EXPECT_FALSE(result.filter_column_ids.contains(1));
}

// ============================================================================
// debug() output
// ============================================================================
TEST_F(SchemaNodeTest, DebugOutputConstNode) {
    auto output = TableSchemaChangeHelper::debug(ConstNode::get_instance());
    EXPECT_FALSE(output.empty());
}

TEST_F(SchemaNodeTest, DebugOutputStructNode) {
    auto root = std::make_shared<StructNode>();
    root->add_children("a", "A", std::make_shared<ScalarNode>());
    root->add_not_exist_children("b");
    auto output = TableSchemaChangeHelper::debug(root);
    EXPECT_FALSE(output.empty());
    // Should contain column names in the debug output
    EXPECT_NE(output.find("a"), std::string::npos);
}

} // namespace doris
