add_executable(velox_learning_test LearningSmokeTest.cpp)
add_test(velox_learning_test velox_learning_test)
target_link_libraries(
        velox_learning_test
        velox_vector_test_lib
        velox
        GTest::gtest
        GTest::gtest_main)