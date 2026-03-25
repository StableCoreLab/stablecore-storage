cmake_minimum_required(VERSION 3.24)

project(stablecore_storage LANGUAGES C CXX)

include(CTest)
include(FetchContent)

option(STABLECORE_STORAGE_BUILD_DB_EDITOR "Build the Qt database editor tool" OFF)

add_library(sqlite3 STATIC
    ${CMAKE_CURRENT_SOURCE_DIR}/../ThirdParty/sqlite/sqlite3.c
)

target_include_directories(sqlite3
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/../ThirdParty/sqlite
)

target_compile_definitions(sqlite3
    PRIVATE
        SQLITE_THREADSAFE=1
)

add_library(stablecore_storage
    Src/Factory.cpp
    Src/Batch/BatchOperations.cpp
    Src/Computed/ComputedRuntime.cpp
    Src/Computed/TableView.cpp
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

target_link_libraries(stablecore_storage
    PRIVATE
        sqlite3
)

add_executable(stablecore_storage_memory_example
    Examples/MemoryExample.cpp
)

target_link_libraries(stablecore_storage_memory_example
    PRIVATE
        stablecore_storage
)

add_executable(stablecore_storage_product_example
    Examples/ProductIntegrationExample.cpp
)

target_link_libraries(stablecore_storage_product_example
    PRIVATE
        stablecore_storage
)

if (STABLECORE_STORAGE_BUILD_DB_EDITOR)
    set(CMAKE_AUTOMOC ON)
    set(CMAKE_AUTOUIC OFF)
    set(CMAKE_AUTORCC OFF)

    find_package(Qt6 6.8 REQUIRED COMPONENTS Widgets)
    add_subdirectory(Tools/DatabaseEditor)
endif()

option(STABLECORE_STORAGE_BUILD_TESTS "Build tests" ON)

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
        Tests/PerformanceSmokeTests.cpp
        Tests/TableViewTests.cpp
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
