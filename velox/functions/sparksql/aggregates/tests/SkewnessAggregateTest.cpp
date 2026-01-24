/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "velox/functions/sparksql/aggregates/Register.h"

using namespace facebook::velox::functions::aggregate::test;
using facebook::velox::exec::test::AssertQueryBuilder;
using facebook::velox::exec::test::PlanBuilder;

namespace facebook::velox::functions::aggregate::sparksql::test {
namespace {

class SkewnessAggregateTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    registerAggregateFunctions("spark_");
  }

  template <typename T>
  void testSkewness(const size_t size) {
    auto data = {
        makeRowVector({makeFlatVector<T>(size, [](auto row) { return row; })})};
    createDuckDbTable(data);
    testAggregations(data, {"c0"}, {"spark_skewness(c0)"}, "SELECT c0, skewness(c0) FROM tmp GROUP BY c0");
  }
};

TEST_F(SkewnessAggregateTest, basic) {
  testSkewness<double>(100);
}
} // namespace
} // namespace facebook::velox::functions::aggregate::sparksql::test
