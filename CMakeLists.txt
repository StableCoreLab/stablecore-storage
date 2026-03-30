cmake_minimum_required(VERSION 3.24)

project(stablecore_storage LANGUAGES C CXX)

include(CTest)
include(FetchContent)

option(STABLECORE_STORAGE_BUILD_DB_EDITOR "Build the Qt database editor tool" OFF)

set(STABLECORE_STORAGE_BIN_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/Bin")

function(stablecore_configure_output_dirs target_name)
    if (MSVC)
        set_target_properties(${target_name} PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY "${STABLECORE_STORAGE_BIN_ROOT}/$<CONFIG>/$<PLATFORM_ID>"
            LIBRARY_OUTPUT_DIRECTORY "${STABLECORE_STORAGE_BIN_ROOT}/$<CONFIG>/$<PLATFORM_ID>"
            ARCHIVE_OUTPUT_DIRECTORY "${STABLECORE_STORAGE_BIN_ROOT}/$<CONFIG>/$<PLATFORM_ID>"
            PDB_OUTPUT_DIRECTORY "${STABLECORE_STORAGE_BIN_ROOT}/$<CONFIG>/$<PLATFORM_ID>"
            COMPILE_PDB_OUTPUT_DIRECTORY "${STABLECORE_STORAGE_BIN_ROOT}/$<CONFIG>/$<PLATFORM_ID>"
            PDB_NAME "${target_name}"
            COMPILE_PDB_NAME "${target_name}"
        )
    else()
        set_target_properties(${target_name} PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY "${STABLECORE_STORAGE_BIN_ROOT}/$<CONFIG>/$<PLATFORM_ID>"
            LIBRARY_OUTPUT_DIRECTORY "${STABLECORE_STORAGE_BIN_ROOT}/$<CONFIG>/$<PLATFORM_ID>"
            ARCHIVE_OUTPUT_DIRECTORY "${STABLECORE_STORAGE_BIN_ROOT}/$<CONFIG>/$<PLATFORM_ID>"
        )
    endif()
endfunction()

if (NOT DEFINED CMAKE_PREFIX_PATH AND DEFINED ENV{QT6_8_x64} AND NOT "$ENV{QT6_8_x64}" STREQUAL "")
    set(CMAKE_PREFIX_PATH "$ENV{QT6_8_x64}")
endif()

if (NOT DEFINED Qt6_DIR AND DEFINED ENV{QT6_8_x64} AND NOT "$ENV{QT6_8_x64}" STREQUAL "")
    set(Qt6_DIR "$ENV{QT6_8_x64}/lib/cmake/Qt6")
endif()

add_library(sqlite3 STATIC
    ${CMAKE_CURRENT_SOURCE_DIR}/../ThirdParty/sqlite/sqlite3.c
)
stablecore_configure_output_dirs(sqlite3)

target_include_directories(sqlite3
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/../ThirdParty/sqlite
)

target_compile_definitions(sqlite3
    PRIVATE
        SQLITE_THREADSAFE=1
)

add_library(stablecore_storage SHARED
    Src/Factory.cpp
    Src/Batch/BatchOperations.cpp
    Src/Computed/ComputedRuntime.cpp
    Src/Computed/TableView.cpp
    Src/Diagnostics/Diagnostics.cpp
    Src/Migration/Migration.cpp
    Src/Memory/MemoryDatabase.cpp
    Src/Sqlite/SqliteAdapter.cpp
)
stablecore_configure_output_dirs(stablecore_storage)

target_include_directories(stablecore_storage
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/Include
)

target_compile_features(stablecore_storage PUBLIC cxx_std_20)

set_target_properties(stablecore_storage PROPERTIES
    WINDOWS_EXPORT_ALL_SYMBOLS ON
)

target_link_libraries(stablecore_storage
    PRIVATE
        sqlite3
)

add_executable(stablecore_storage_memory_example
    Examples/MemoryExample.cpp
)
stablecore_configure_output_dirs(stablecore_storage_memory_example)

target_link_libraries(stablecore_storage_memory_example
    PRIVATE
        stablecore_storage
)

add_executable(stablecore_storage_product_example
    Examples/ProductIntegrationExample.cpp
)
stablecore_configure_output_dirs(stablecore_storage_product_example)

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
    stablecore_configure_output_dirs(stablecore_storage_tests)

    target_link_libraries(stablecore_storage_tests
        PRIVATE
            stablecore_storage
            GTest::gtest
            GTest::gtest_main
    )

    include(GoogleTest)
    gtest_discover_tests(stablecore_storage_tests)

    if (STABLECORE_STORAGE_BUILD_DB_EDITOR)
        add_executable(stablecore_storage_db_editor_tests
            Tests/DatabaseEditorSessionTests.cpp
            Tools/DatabaseEditor/DatabaseSession.cpp
            Tools/DatabaseEditor/DatabaseSession.h
        )
        stablecore_configure_output_dirs(stablecore_storage_db_editor_tests)

        target_include_directories(stablecore_storage_db_editor_tests
            PRIVATE
                ${CMAKE_CURRENT_SOURCE_DIR}/Tools/DatabaseEditor
        )

        target_link_libraries(stablecore_storage_db_editor_tests
            PRIVATE
                stablecore_storage
                GTest::gtest
                GTest::gtest_main
                Qt6::Core
        )

        target_compile_features(stablecore_storage_db_editor_tests PRIVATE cxx_std_20)
        include(GoogleTest)
        gtest_discover_tests(stablecore_storage_db_editor_tests)
    endif()
endif()
