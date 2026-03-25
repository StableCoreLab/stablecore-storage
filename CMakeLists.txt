cmake_minimum_required(VERSION 3.24)

project(stablecore_storage LANGUAGES CXX)

include(CTest)
include(FetchContent)
find_package(SQLite3 REQUIRED)

add_library(stablecore_storage
    Src/Factory.cpp
    Src/Batch/BatchOperations.cpp
    Src/Computed/ComputedRuntime.cpp
    Src/Diagnostics/Diagnostics.cpp
    Src/Migration/Migration.cpp
    Src/Memory/MemoryDatabase.cpp
    Src/Sqlite/SqliteAdapter.cpp
)

target_include_directories(stablecore_storage
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/Include
)

target_compile_features(stablecore_storage PUBLIC cxx_std_20)
target_link_libraries(stablecore_storage PRIVATE SQLite::SQLite3)

add_executable(stablecore_storage_memory_example
    Examples/MemoryExample.cpp
)

target_link_libraries(stablecore_storage_memory_example
    PRIVATE stablecore_storage
)

add_executable(stablecore_storage_product_example
    Examples/ProductIntegrationExample.cpp
)

target_link_libraries(stablecore_storage_product_example
    PRIVATE stablecore_storage
)

option(STABLECORE_STORAGE_BUILD_TESTS "Build tests" OFF)

if (STABLECORE_STORAGE_BUILD_TESTS)
    enable_testing()

    find_package(GTest CONFIG QUIET)

    if (NOT GTest_FOUND)
        set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

        FetchContent_Declare(
            googletest
            SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../ThirdParty/googletest-1.17.0"
        )

        FetchContent_MakeAvailable(googletest)
    endif()

    add_executable(stablecore_storage_tests
        Tests/ComputedTests.cpp
        Tests/M1Tests.cpp
        Tests/M2SqliteTests.cpp
        Tests/M3Tests.cpp
    )

    target_link_libraries(stablecore_storage_tests
        PRIVATE
            stablecore_storage
            GTest::gtest
            GTest::gtest_main
    )

    include(GoogleTest)
    gtest_discover_tests(stablecore_storage_tests)
endif()
