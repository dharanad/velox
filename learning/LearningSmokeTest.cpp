#include <gmock/gmock.h>

#include "velox/vector/tests/utils/VectorTestBase.h"

class LearningSmokeTest : public testing::Test, public facebook::velox::test::VectorTestBase {
protected:
  static void SetUpTestCase() {
    facebook::velox::memory::MemoryManager::testingSetInstance(facebook::velox::memory::MemoryManager::Options{});
  
  }
};

TEST_F(LearningSmokeTest, helloSmokeTest) {
  const auto vec = makeFlatVector<int32_t>({1,2,3,4,5});
  ASSERT_EQ(vec->size(), 5);
}