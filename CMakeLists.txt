cmake_minimum_required(VERSION 3.24)

project(StablecoreStorage LANGUAGES C CXX)

include(CTest)
include(FetchContent)
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

option(STABLECORE_STORAGE_BUILD_DB_EDITOR "Build the Qt database editor tool" OFF)

set(STABLECORE_STORAGE_TARGET SCStorage)
set(STABLECORE_STORAGE_MEMORY_EXAMPLE_TARGET SCStorageMemoryExample)
set(STABLECORE_STORAGE_PRODUCT_EXAMPLE_TARGET SCStorageProductExample)
set(STABLECORE_STORAGE_TESTS_TARGET SCStorageTests)
set(STABLECORE_STORAGE_DB_EDITOR_TESTS_TARGET SCStorageDatabaseEditorTests)
set(STABLECORE_STORAGE_BIN_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../Bin")
set(STABLECORE_STORAGE_INSTALL_CMAKE_DIR "${CMAKE_INSTALL_LIBDIR}/cmake/StablecoreStorage")

if (MSVC AND CMAKE_SIZEOF_VOID_P EQUAL 8)
    set(STABLECORE_STORAGE_INSTALL_PLATFORM "x64")
else()
    set(STABLECORE_STORAGE_INSTALL_PLATFORM "${CMAKE_SYSTEM_PROCESSOR}")
endif()

set(STABLECORE_STORAGE_INSTALL_BIN_DIR "${CMAKE_INSTALL_BINDIR}/$<CONFIG>/${STABLECORE_STORAGE_INSTALL_PLATFORM}")
set(STABLECORE_STORAGE_INSTALL_LIB_DIR "${CMAKE_INSTALL_LIBDIR}/$<CONFIG>/${STABLECORE_STORAGE_INSTALL_PLATFORM}")

function(stablecore_configure_output_dirs target_name)
    if (MSVC AND CMAKE_GENERATOR MATCHES "Visual Studio")
        set(stablecore_output_platform "${CMAKE_VS_PLATFORM_NAME}")
    else()
        set(stablecore_output_platform "$<PLATFORM_ID>")
    endif()

    if (MSVC)
        set_target_properties(${target_name} PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY "${STABLECORE_STORAGE_BIN_ROOT}/$<CONFIG>/${stablecore_output_platform}"
            LIBRARY_OUTPUT_DIRECTORY "${STABLECORE_STORAGE_BIN_ROOT}/$<CONFIG>/${stablecore_output_platform}"
            ARCHIVE_OUTPUT_DIRECTORY "${STABLECORE_STORAGE_BIN_ROOT}/$<CONFIG>/${stablecore_output_platform}"
            PDB_OUTPUT_DIRECTORY "${STABLECORE_STORAGE_BIN_ROOT}/$<CONFIG>/${stablecore_output_platform}"
            COMPILE_PDB_OUTPUT_DIRECTORY "${STABLECORE_STORAGE_BIN_ROOT}/$<CONFIG>/${stablecore_output_platform}"
            PDB_NAME "${target_name}"
            COMPILE_PDB_NAME "${target_name}"
        )
    else()
        set_target_properties(${target_name} PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY "${STABLECORE_STORAGE_BIN_ROOT}/$<CONFIG>/${stablecore_output_platform}"
            LIBRARY_OUTPUT_DIRECTORY "${STABLECORE_STORAGE_BIN_ROOT}/$<CONFIG>/${stablecore_output_platform}"
            ARCHIVE_OUTPUT_DIRECTORY "${STABLECORE_STORAGE_BIN_ROOT}/$<CONFIG>/${stablecore_output_platform}"
        )
    endif()
endfunction()

if (NOT DEFINED CMAKE_PREFIX_PATH AND DEFINED ENV{QT6_8_x64} AND NOT "$ENV{QT6_8_x64}" STREQUAL "")
    set(CMAKE_PREFIX_PATH "$ENV{QT6_8_x64}")
endif()

if (NOT DEFINED Qt6_DIR AND DEFINED ENV{QT6_8_x64} AND NOT "$ENV{QT6_8_x64}" STREQUAL "")
    set(Qt6_DIR "$ENV{QT6_8_x64}/lib/cmake/Qt6")
endif()

add_library(${STABLECORE_STORAGE_TARGET} SHARED
    Src/Factory.cpp
    Src/Batch/BatchOperations.cpp
    Src/Computed/ComputedRuntime.cpp
    Src/Computed/TableView.cpp
    Src/Diagnostics/Diagnostics.cpp
    Src/Migration/Migration.cpp
    Src/Memory/MemoryDatabase.cpp
    Src/Sqlite/SqliteAdapter.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../ThirdParty/Sqlite/sqlite3.c
)
stablecore_configure_output_dirs(${STABLECORE_STORAGE_TARGET})

target_include_directories(${STABLECORE_STORAGE_TARGET}
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/Include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../ThirdParty/Sqlite>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

target_compile_features(${STABLECORE_STORAGE_TARGET} PUBLIC cxx_std_20)

if (MSVC)
    target_compile_options(${STABLECORE_STORAGE_TARGET} PRIVATE /utf-8 /MP)
endif()

set_target_properties(${STABLECORE_STORAGE_TARGET} PROPERTIES
    WINDOWS_EXPORT_ALL_SYMBOLS ON
)

add_executable(${STABLECORE_STORAGE_MEMORY_EXAMPLE_TARGET}
    Examples/MemoryExample.cpp
)
stablecore_configure_output_dirs(${STABLECORE_STORAGE_MEMORY_EXAMPLE_TARGET})

if (MSVC)
    target_compile_options(${STABLECORE_STORAGE_MEMORY_EXAMPLE_TARGET} PRIVATE /utf-8 /MP)
endif()

target_link_libraries(${STABLECORE_STORAGE_MEMORY_EXAMPLE_TARGET}
    PRIVATE
        ${STABLECORE_STORAGE_TARGET}
)

add_executable(${STABLECORE_STORAGE_PRODUCT_EXAMPLE_TARGET}
    Examples/ProductIntegrationExample.cpp
)
stablecore_configure_output_dirs(${STABLECORE_STORAGE_PRODUCT_EXAMPLE_TARGET})

if (MSVC)
    target_compile_options(${STABLECORE_STORAGE_PRODUCT_EXAMPLE_TARGET} PRIVATE /utf-8 /MP)
endif()

target_link_libraries(${STABLECORE_STORAGE_PRODUCT_EXAMPLE_TARGET}
    PRIVATE
        ${STABLECORE_STORAGE_TARGET}
)

if (STABLECORE_STORAGE_BUILD_DB_EDITOR)
    set(CMAKE_AUTOMOC ON)
    set(CMAKE_AUTOUIC OFF)
    set(CMAKE_AUTORCC OFF)

    find_package(Qt6 6.8 REQUIRED COMPONENTS Widgets)
    add_subdirectory(Tools/DatabaseEditor)
endif()

set(STABLECORE_STORAGE_PUBLIC_HEADERS
    Include/StableCore/Storage/Batch.h
    Include/StableCore/Storage/Computed.h
    Include/StableCore/Storage/Diagnostics.h
    Include/StableCore/Storage/Errors.h
    Include/StableCore/Storage/Factory.h
    Include/StableCore/Storage/Interfaces.h
    Include/StableCore/Storage/Migration.h
    Include/StableCore/Storage/RefCounted.h
    Include/StableCore/Storage/RefPtr.h
    Include/StableCore/Storage/Storage.h
    Include/StableCore/Storage/TableView.h
    Include/StableCore/Storage/Types.h
)

option(STABLECORE_STORAGE_BUILD_TESTS "Build tests" ON)

if (STABLECORE_STORAGE_BUILD_TESTS)
    enable_testing()

    find_package(GTest CONFIG QUIET)

    if (NOT GTest_FOUND)
        set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
        set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
        set(INSTALL_GMOCK OFF CACHE BOOL "" FORCE)

        FetchContent_Declare(
            googletest
            SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../ThirdParty/GoogleTest"
        )

        FetchContent_MakeAvailable(googletest)
    endif()

    add_executable(${STABLECORE_STORAGE_TESTS_TARGET}
        Tests/ComputedTests.cpp
        Tests/M1Tests.cpp
        Tests/M2SqliteTests.cpp
        Tests/M3Tests.cpp
        Tests/PerformanceSmokeTests.cpp
        Tests/TableViewTests.cpp
    )
    stablecore_configure_output_dirs(${STABLECORE_STORAGE_TESTS_TARGET})

    if (MSVC)
        target_compile_options(${STABLECORE_STORAGE_TESTS_TARGET} PRIVATE /utf-8 /MP)
    endif()

    target_link_libraries(${STABLECORE_STORAGE_TESTS_TARGET}
        PRIVATE
            ${STABLECORE_STORAGE_TARGET}
            GTest::gtest
            GTest::gtest_main
    )

    include(GoogleTest)
    gtest_discover_tests(${STABLECORE_STORAGE_TESTS_TARGET})

    if (STABLECORE_STORAGE_BUILD_DB_EDITOR)
        add_executable(${STABLECORE_STORAGE_DB_EDITOR_TESTS_TARGET}
            Tests/DatabaseEditorSessionTests.cpp
            Tools/DatabaseEditor/DatabaseSession.cpp
            Tools/DatabaseEditor/DatabaseSession.h
        )
        stablecore_configure_output_dirs(${STABLECORE_STORAGE_DB_EDITOR_TESTS_TARGET})

        if (MSVC)
            target_compile_options(${STABLECORE_STORAGE_DB_EDITOR_TESTS_TARGET} PRIVATE /utf-8 /MP)
        endif()

        target_include_directories(${STABLECORE_STORAGE_DB_EDITOR_TESTS_TARGET}
            PRIVATE
                ${CMAKE_CURRENT_SOURCE_DIR}/Tools/DatabaseEditor
        )

        target_link_libraries(${STABLECORE_STORAGE_DB_EDITOR_TESTS_TARGET}
            PRIVATE
                ${STABLECORE_STORAGE_TARGET}
                GTest::gtest
                GTest::gtest_main
                Qt6::Core
        )

        target_compile_features(${STABLECORE_STORAGE_DB_EDITOR_TESTS_TARGET} PRIVATE cxx_std_20)
        include(GoogleTest)
        gtest_discover_tests(${STABLECORE_STORAGE_DB_EDITOR_TESTS_TARGET})
    endif()
endif()

install(TARGETS ${STABLECORE_STORAGE_TARGET}
    EXPORT StablecoreStorageTargets
    RUNTIME DESTINATION ${STABLECORE_STORAGE_INSTALL_BIN_DIR}
    LIBRARY DESTINATION ${STABLECORE_STORAGE_INSTALL_BIN_DIR}
    ARCHIVE DESTINATION ${STABLECORE_STORAGE_INSTALL_LIB_DIR}
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

install(FILES ${STABLECORE_STORAGE_PUBLIC_HEADERS}
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/StableCore/Storage
)

install(TARGETS ${STABLECORE_STORAGE_MEMORY_EXAMPLE_TARGET} ${STABLECORE_STORAGE_PRODUCT_EXAMPLE_TARGET}
    RUNTIME DESTINATION ${STABLECORE_STORAGE_INSTALL_BIN_DIR}
)

if (STABLECORE_STORAGE_BUILD_DB_EDITOR)
    install(TARGETS SCStorageDatabaseEditor
        RUNTIME DESTINATION ${STABLECORE_STORAGE_INSTALL_BIN_DIR}
    )
endif()

install(EXPORT StablecoreStorageTargets
    FILE StablecoreStorageTargets.cmake
    NAMESPACE Stablecore::
    DESTINATION ${STABLECORE_STORAGE_INSTALL_CMAKE_DIR}
)

configure_package_config_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/StablecoreStorageConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/StablecoreStorageConfig.cmake
    INSTALL_DESTINATION ${STABLECORE_STORAGE_INSTALL_CMAKE_DIR}
)

write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/StablecoreStorageConfigVersion.cmake
    VERSION 1.0.0
    COMPATIBILITY SameMajorVersion
)

install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/StablecoreStorageConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/StablecoreStorageConfigVersion.cmake
    DESTINATION ${STABLECORE_STORAGE_INSTALL_CMAKE_DIR}
)
