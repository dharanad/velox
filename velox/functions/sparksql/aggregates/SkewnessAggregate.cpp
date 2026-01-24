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

#include "SkewnessAggregate.h"

#include "velox/exec/Aggregate.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::functions::aggregate::sparksql {
namespace {
struct SkewnessAccumulator {
  int64_t count {0};
  double sum {0.0};
  double sum_sqr {0.0};
  double sum_cub {0.0};
};

class SkewnessAggregate : public exec::Aggregate {
 public:
  explicit SkewnessAggregate(const TypePtr& resultType)
      : Aggregate(resultType) {}
  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(SkewnessAccumulator);
  }
  bool isFixedSize() const override {
    return true;
  }
  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    decodedRaw_.decode(*args[0], rows);
    rows.applyToSelected([&](vector_size_t i) {
      if (!decodedRaw_.isNullAt(i)) {
        updateInternalNonNull(groups[i], decodedRaw_.valueAt<double>(i));
      }
    });
  }
  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    decodedPartial_.decode(*args[0], rows);
    auto baseRowVector = decodedPartial_.base()->as<RowVector>();
    DecodedVector countDecoded {*baseRowVector->childAt(0), rows};
    DecodedVector sumDecoded {*baseRowVector->childAt(1), rows};
    DecodedVector sumSqrDecoded {*baseRowVector->childAt(2), rows};
    DecodedVector sumCubDecoded {*baseRowVector->childAt(3), rows};

    rows.applyToSelected([&](vector_size_t i) {
      updateInternalNonNull(
          groups[i],
          countDecoded.valueAt<int64_t>(i),
          sumDecoded.valueAt<double>(i),
          sumSqrDecoded.valueAt<double>(i),
          sumCubDecoded.valueAt<double>(i));
    });

  }
  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    // FIXME
    decodedRaw_.decode(*args[0], rows);
    rows.applyToSelected([&](vector_size_t i) {
      if (!decodedRaw_.isNullAt(i)) {
        updateInternalNonNull(group, decodedRaw_.valueAt<double>(i));
      }
    });
  }
  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    VELOX_NYI("addSingleGroupIntermediateResults");
  }
  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto vector = (*result)->asFlatVector<double>();
    VELOX_CHECK(vector);
    vector->resize(numGroups);

    uint64_t* rawNulls = getRawNulls(vector);

    double* rawValues = vector->mutableRawValues();
    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      auto* acc = accumulator(group);
      if (acc->count <= 2) {
        vector->setNull(i, true);
      } else {
        this->clearNull(rawNulls, i);
        rawValues[i] = 0.0;
      }
    }
  }
  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto rowVector = (*result)->as<RowVector>();
    auto countVector = rowVector->childAt(0)->asFlatVector<int64_t>();
    auto sumVector = rowVector->childAt(1)->asFlatVector<double>();
    auto sumSqrVector = rowVector->childAt(2)->asFlatVector<double>();
    auto sumCubVector = rowVector->childAt(3)->asFlatVector<double>();

    rowVector->resize(numGroups);
    countVector->resize(numGroups);
    sumVector->resize(numGroups);
    sumSqrVector->resize(numGroups);
    sumCubVector->resize(numGroups);
    rowVector->clearAllNulls();

    int64_t* rawCountVector = countVector->mutableRawValues();
    double* rawSumVector = sumVector->mutableRawValues();
    double* rawSumSqrVector = sumSqrVector->mutableRawValues();
    double* rawSumCubVector = sumCubVector->mutableRawValues();

    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      const auto* acc = accumulator(group);
      rawCountVector[i] = acc->count;
      rawSumVector[i] = acc->sum;
      rawSumSqrVector[i] = acc->sum_sqr;
      rawSumCubVector[i] = acc->sum_cub;
    }

  }

 protected:
  void initializeNewGroupsInternal(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    setAllNulls(groups, indices);
    for (const auto i : indices) {
      new (groups[i] + offset_) SkewnessAccumulator();
    }
  }

  SkewnessAccumulator* accumulator(char* group) const {
    return value<SkewnessAccumulator>(group);
  }

  void updateInternalNonNull(char* group, const double val) const {
    accumulator(group)->count += 1;
    accumulator(group)->sum += val;
    accumulator(group)->sum_sqr += std::pow(val, 2);
    accumulator(group)->sum_cub += std::pow(val, 3);
  }

  void updateInternalNonNull(
      char* group,
      const int64_t count,
      const double sum,
      const double sumSqr,
      const double sumCub) const {
    if (!count) {
      return;
    }
    accumulator(group)->count += count;
    accumulator(group)->sum += sum;
    accumulator(group)->sum_sqr += sumSqr;
    accumulator(group)->sum_cub += sumCub;
  }

  DecodedVector decodedPartial_;
  DecodedVector decodedRaw_;
};
} // namespace

exec::AggregateRegistrationResult registerSkewnessAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .argumentType("double")
          .intermediateType("ROW(bigint, double, double, double)")
          .returnType("double")
          .build()};
  const auto name = prefix + "skewness";
  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step /* step */,
          const std::vector<TypePtr>& /* argTypes */,
          const TypePtr& resultType,
          const core::QueryConfig& config) -> std::unique_ptr<exec::Aggregate> {
        return std::make_unique<SkewnessAggregate>(resultType);
      },
      withCompanionFunctions,
      overwrite);
}
} // namespace facebook::velox::functions::aggregate::sparksql
