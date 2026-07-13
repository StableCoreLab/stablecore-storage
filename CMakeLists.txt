cmake_minimum_required(VERSION 3.24)

project(SCStorage LANGUAGES C CXX)

# Add /FS flag for MSVC to allow parallel compilation without PDB conflicts
if (MSVC)
    string(APPEND CMAKE_CXX_FLAGS " /FS")
endif()

include(CTest)
include(FetchContent)
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
include(${CMAKE_CURRENT_LIST_DIR}/cmake/SCUtf8.cmake)

option(SCSTORAGE_BUILD_DB_EDITOR "Build the Qt database editor tool" ON)

set(SCSTORAGE_TARGET SCStorage)
set(SCSTORAGE_PRODUCT_EXAMPLE_TARGET SCStorageProductExample)
set(SCSTORAGE_SCHEMA_EDIT_EXAMPLE_TARGET SCStorageSchemaEditExample)
set(SCSTORAGE_TESTS_TARGET SCStorageTests)
set(SCSTORAGE_DB_EDITOR_TESTS_TARGET SCStorageDatabaseEditorTests)
set(SCSTORAGE_BIN_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../Bin")
set(CMAKE_INSTALL_BINDIR "Bin")
set(CMAKE_INSTALL_INCLUDEDIR "Include")
set(CMAKE_INSTALL_LIBDIR "Lib")
set(SCSTORAGE_INSTALL_CMAKE_DIR "${CMAKE_INSTALL_LIBDIR}/cmake/SCStorage")

if (MSVC AND CMAKE_SIZEOF_VOID_P EQUAL 8)
    set(SCSTORAGE_INSTALL_PLATFORM "x64")
else()
    set(SCSTORAGE_INSTALL_PLATFORM "${CMAKE_SYSTEM_PROCESSOR}")
endif()

set(SCSTORAGE_INSTALL_BIN_DIR "${CMAKE_INSTALL_BINDIR}/${SCSTORAGE_INSTALL_PLATFORM}/$<CONFIG>")
set(SCSTORAGE_INSTALL_LIB_DIR "${CMAKE_INSTALL_LIBDIR}/${SCSTORAGE_INSTALL_PLATFORM}/$<CONFIG>")
set(SCSTORAGE_INSTALL_INCLUDE_DIR "${CMAKE_INSTALL_INCLUDEDIR}/Storage")
set(SCSTORAGE_INSTALL_PDB_DIR "${SCSTORAGE_INSTALL_BIN_DIR}")

function(SCStorageConfigureOutputDirs target_name)
    if (MSVC AND CMAKE_GENERATOR MATCHES "Visual Studio")
        set(SCSTORAGE_OUTPUT_PLATFORM "${CMAKE_VS_PLATFORM_NAME}")
    else()
        set(SCSTORAGE_OUTPUT_PLATFORM "$<PLATFORM_ID>")
    endif()

    if (MSVC)
        set_target_properties(${target_name} PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY "${SCSTORAGE_BIN_ROOT}/$<CONFIG>/${SCSTORAGE_OUTPUT_PLATFORM}"
            LIBRARY_OUTPUT_DIRECTORY "${SCSTORAGE_BIN_ROOT}/$<CONFIG>/${SCSTORAGE_OUTPUT_PLATFORM}"
            ARCHIVE_OUTPUT_DIRECTORY "${SCSTORAGE_BIN_ROOT}/$<CONFIG>/${SCSTORAGE_OUTPUT_PLATFORM}"
            PDB_OUTPUT_DIRECTORY "${SCSTORAGE_BIN_ROOT}/$<CONFIG>/${SCSTORAGE_OUTPUT_PLATFORM}"
            COMPILE_PDB_OUTPUT_DIRECTORY "${SCSTORAGE_BIN_ROOT}/$<CONFIG>/${SCSTORAGE_OUTPUT_PLATFORM}"
            PDB_NAME "${target_name}"
            COMPILE_PDB_NAME "${target_name}"
            MSVC_DEBUG_INFORMATION_FORMAT ProgramDatabase
        )
        target_compile_options(${target_name} PRIVATE
            $<$<CONFIG:Release>:/Zi>
            $<$<CONFIG:RelWithDebInfo>:/Zi>
        )
        target_link_options(${target_name} PRIVATE
            $<$<CONFIG:Release>:/DEBUG>
            $<$<CONFIG:RelWithDebInfo>:/DEBUG>
        )
    else()
        set_target_properties(${target_name} PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY "${SCSTORAGE_BIN_ROOT}/$<CONFIG>/${SCSTORAGE_OUTPUT_PLATFORM}"
            LIBRARY_OUTPUT_DIRECTORY "${SCSTORAGE_BIN_ROOT}/$<CONFIG>/${SCSTORAGE_OUTPUT_PLATFORM}"
            ARCHIVE_OUTPUT_DIRECTORY "${SCSTORAGE_BIN_ROOT}/$<CONFIG>/${SCSTORAGE_OUTPUT_PLATFORM}"
        )
    endif()

    SCEnableUtf8ForTargets(${target_name})
endfunction()

function(SCStorageSourceGroupTree root_dir)
    if (ARGC GREATER 1)
        source_group(TREE "${root_dir}" FILES ${ARGN})
    endif()
endfunction()

if (NOT DEFINED CMAKE_PREFIX_PATH AND DEFINED ENV{QT6_8_x64} AND NOT "$ENV{QT6_8_x64}" STREQUAL "")
    set(CMAKE_PREFIX_PATH "$ENV{QT6_8_x64}")
endif()

if (NOT DEFINED Qt6_DIR AND DEFINED ENV{QT6_8_x64} AND NOT "$ENV{QT6_8_x64}" STREQUAL "")
    set(Qt6_DIR "$ENV{QT6_8_x64}/lib/cmake/Qt6")
endif()

# SCCommon dependency (via stablecore_ui package)
list(APPEND CMAKE_PREFIX_PATH "${CMAKE_CURRENT_SOURCE_DIR}/Extern/stablecore")
set(stablecore_ui_DIR "${CMAKE_CURRENT_SOURCE_DIR}/Extern/stablecore/Lib/cmake/stablecore_ui"
    CACHE PATH "stablecore_ui CMake directory" FORCE)
find_package(stablecore_ui CONFIG REQUIRED)
find_package(Qt6 6.8 REQUIRED COMPONENTS Core)

set(SCSTORAGE_LIBRARY_SOURCES
    Src/SCErrors.cpp
    Src/SCFactory.cpp
    Src/Batch/SCBatchOperations.cpp
    Src/Computed/SCComputedRuntime.cpp
    Src/Computed/SCTableView.cpp
    Src/Diagnostics/SCDiagnostics.cpp
    Src/Diagnostics/SCDebugPackage.cpp
    Src/Migration/SCMigration.cpp
    Src/Sqlite/SqliteUpgradeRegistry.cpp
    Src/Sqlite/SqliteUpgradeV5ToV6.cpp
    Src/Sqlite/SqliteUpgradeV6ToV7.cpp
    Src/Sqlite/SqliteUpgradeV7ToV8.cpp
    Src/Query/SCQueryBridge.cpp
    Src/Query/SCQueryExecutor.cpp
    Src/Query/SCQuerySqliteExecutor.cpp
    Src/Query/SCQueryPlanner.cpp
    Src/Schema/SCSchemaEdit.cpp
    Src/Sqlite/SCSqliteAdapter.cpp
    Src/Sqlite/SqliteImplCore.cpp
    Src/Sqlite/SqliteImplSchema.cpp
    Src/Sqlite/SqliteImplDatabase_Init.cpp
    Src/Sqlite/SqliteImplDatabase_Edit.cpp
    Src/Sqlite/SqliteImplDatabase_Constraints.cpp
    Src/Sqlite/SqliteImplDatabase_Index.cpp
)

set(SCSTORAGE_LIBRARY_PRIVATE_HEADERS
    Src/Query/SCQuerySqliteExecutor.h
    Src/Query/SCQuerySqliteIndexAccess.h
    Src/Sqlite/SqliteUpgradeInterfaces.h
    Src/Sqlite/SqliteUpgradeRegistry.h
    Src/Sqlite/SqliteImplCore.h
    Src/Sqlite/SqliteImplDatabase.h
)

set(SCSTORAGE_PUBLIC_HEADERS
    Include/SCBatch.h
    Include/ISCDebugPackage.h
    Include/ISCComputed.h
    Include/ISCDiagnostics.h
    Include/ISCQuery.h
    Include/SCErrors.h
    Include/SCFactory.h
    Include/ISCInterfaces.h
    Include/SCMigration.h
    Include/SCQuery.h
    Include/SCSchemaEdit.h
    Include/SCRefCounted.h
    Include/ISCRefPtr.h
    Include/SCStorage.h
    Include/ISCTableView.h
    Include/SCTypes.h
)

set(SCSTORAGE_PUBLIC_SDK_FILES
    SCStorage.natvis
    ${SCSTORAGE_PUBLIC_HEADERS}
)

add_library(${SCSTORAGE_TARGET} SHARED
    ${SCSTORAGE_LIBRARY_SOURCES}
    ${CMAKE_CURRENT_SOURCE_DIR}/../ThirdParty/Sqlite/sqlite3.c
)
SCStorageConfigureOutputDirs(${SCSTORAGE_TARGET})

target_link_libraries(${SCSTORAGE_TARGET}
    PUBLIC
        stablecore_ui::SCCommon
)

target_sources(${SCSTORAGE_TARGET}
    PRIVATE
        ${SCSTORAGE_PUBLIC_SDK_FILES}
        ${SCSTORAGE_LIBRARY_PRIVATE_HEADERS}
)

SCStorageSourceGroupTree("${CMAKE_CURRENT_SOURCE_DIR}"
    ${SCSTORAGE_LIBRARY_SOURCES}
    ${SCSTORAGE_LIBRARY_PRIVATE_HEADERS}
    ${SCSTORAGE_PUBLIC_SDK_FILES}
)
source_group("ThirdParty\\Sqlite" FILES ${CMAKE_CURRENT_SOURCE_DIR}/../ThirdParty/Sqlite/sqlite3.c)

target_include_directories(${SCSTORAGE_TARGET}
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/Include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../ThirdParty/Sqlite>
        $<INSTALL_INTERFACE:${SCSTORAGE_INSTALL_INCLUDE_DIR}>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/Src/Query
        ${CMAKE_CURRENT_SOURCE_DIR}/Src
)

target_compile_features(${SCSTORAGE_TARGET} PUBLIC cxx_std_20)
if (MSVC)
    target_compile_options(${SCSTORAGE_TARGET} PRIVATE /MP)
endif()

set_target_properties(${SCSTORAGE_TARGET} PROPERTIES
    WINDOWS_EXPORT_ALL_SYMBOLS ON
)

add_executable(${SCSTORAGE_PRODUCT_EXAMPLE_TARGET}
    Examples/ProductIntegrationExample.cpp
)
SCStorageConfigureOutputDirs(${SCSTORAGE_PRODUCT_EXAMPLE_TARGET})
if (MSVC)
    target_compile_options(${SCSTORAGE_PRODUCT_EXAMPLE_TARGET} PRIVATE /MP)
endif()

target_link_libraries(${SCSTORAGE_PRODUCT_EXAMPLE_TARGET}
    PRIVATE
        ${SCSTORAGE_TARGET}
)
SCStorageSourceGroupTree("${CMAKE_CURRENT_SOURCE_DIR}" Examples/ProductIntegrationExample.cpp)

add_executable(${SCSTORAGE_SCHEMA_EDIT_EXAMPLE_TARGET}
    Examples/SchemaEditExample.cpp
)
SCStorageConfigureOutputDirs(${SCSTORAGE_SCHEMA_EDIT_EXAMPLE_TARGET})
if (MSVC)
    target_compile_options(${SCSTORAGE_SCHEMA_EDIT_EXAMPLE_TARGET} PRIVATE /MP)
endif()

target_link_libraries(${SCSTORAGE_SCHEMA_EDIT_EXAMPLE_TARGET}
    PRIVATE
        ${SCSTORAGE_TARGET}
)
SCStorageSourceGroupTree("${CMAKE_CURRENT_SOURCE_DIR}" Examples/SchemaEditExample.cpp)

if (SCSTORAGE_BUILD_DB_EDITOR)
    list(APPEND CMAKE_PREFIX_PATH "${CMAKE_CURRENT_SOURCE_DIR}/Extern/stablecore")

    set(_SCSTORAGE_STABLECORE_UI_DIR
        "${CMAKE_CURRENT_SOURCE_DIR}/Extern/stablecore/Lib/cmake/stablecore_ui"
    )

    if (EXISTS "${_SCSTORAGE_STABLECORE_UI_DIR}/stablecore_uiConfig.cmake")
        set(stablecore_ui_DIR
            "${_SCSTORAGE_STABLECORE_UI_DIR}"
            CACHE PATH "stablecore_ui CMake directory"
            FORCE
        )
    endif()

    # DatabaseEditor is optional; missing UI dependencies must not block core solution generation.
    find_package(stablecore_ui CONFIG QUIET)

    if (stablecore_ui_FOUND)
        find_package(Qt6 6.8 QUIET COMPONENTS Widgets Svg)
    endif()

    if (stablecore_ui_FOUND AND Qt6_FOUND)
        set(CMAKE_AUTOMOC ON)
        set(CMAKE_AUTOUIC OFF)
        set(CMAKE_AUTORCC OFF)

        add_subdirectory(Tools/DatabaseEditor)
    else()
        set(SCSTORAGE_BUILD_DB_EDITOR OFF CACHE BOOL "Build the Qt database editor tool" FORCE)
        message(WARNING
            "Skipping SCStorageDatabaseEditor because optional dependencies are missing "
            "(stablecore_ui and/or Qt6 Widgets/Svg)."
        )
    endif()
endif()

option(SCSTORAGE_BUILD_TESTS "Build tests" ON)

if (SCSTORAGE_BUILD_TESTS)
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

    add_executable(${SCSTORAGE_TESTS_TARGET}
        Tests/Support/TestPaths.cpp
        Tests/Support/TestSqliteHelpers.cpp
        Tests/Support/TestSchemaBuilders.cpp
        Tests/Support/TestSeedData.cpp
        Tests/Support/TestQueryHelpers.cpp
        Tests/ComputedTests.cpp
        Tests/QueryPlannerBridgeTests.cpp
        Tests/Query/QueryExecutorRequireIndexTests.cpp
        Tests/Query/QueryExecutorCompositeIndexTests.cpp
        Tests/Query/QueryExecutorCompositeRangeTests.cpp
        Tests/Query/QueryExecutorCompositeOrderingTests.cpp
        Tests/Query/QueryExecutorCompositeNullDefaultTests.cpp
        Tests/Query/QueryExecutorTieBreakTests.cpp
        Tests/Query/QueryExecutorReopenUndoRedoTests.cpp
        Tests/PerformanceSmokeTests.cpp
        Tests/SchemaEdit/SchemaCreateTableTests.cpp
        Tests/SchemaEdit/SchemaPatchTests.cpp
        Tests/SchemaEdit/SchemaConstraintTests.cpp
        Tests/SchemaEdit/SchemaIndexTests.cpp
        Tests/SchemaEdit/SchemaCompositeIndexTests.cpp
        Tests/SchemaEdit/SchemaRollbackTests.cpp
        Tests/SchemaEdit/SchemaQueryIndexRepairTests.cpp
        Tests/SqliteBackend/SqliteBasicOperationsTests.cpp
        Tests/SqliteBackend/SqliteUndoRedoTests.cpp
        Tests/SqliteBackend/SqliteEditLogTests.cpp
        Tests/SqliteBackend/SqliteBackupTests.cpp
        Tests/SqliteBackend/SqliteUpgradeTests.cpp
        Tests/SqliteBackend/SqliteImportTests.cpp
        Tests/SqliteBackend/SqliteSchemaTests.cpp
        Tests/SqliteBackend/SqliteBatchTests.cpp
        Tests/SqliteBackend/SqliteReadOnlyOpenTests.cpp
        Tests/SqliteBackend/SqliteRelationTests.cpp
        Tests/StorageCore/StorageCoreValueTests.cpp
        Tests/StorageCore/StorageCoreSchemaTests.cpp
        Tests/StorageCore/StorageCoreTransactionTests.cpp
        Tests/StorageCore/StorageCoreRelationTests.cpp
        Tests/TableViewTests.cpp
    )
    SCStorageConfigureOutputDirs(${SCSTORAGE_TESTS_TARGET})
    if (MSVC)
        target_compile_options(${SCSTORAGE_TESTS_TARGET} PRIVATE /MP)
    endif()

    target_include_directories(${SCSTORAGE_TESTS_TARGET}
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/Tests
    )

    target_link_libraries(${SCSTORAGE_TESTS_TARGET}
        PRIVATE
            ${SCSTORAGE_TARGET}
            GTest::gtest
            GTest::gtest_main
    )
    SCStorageSourceGroupTree("${CMAKE_CURRENT_SOURCE_DIR}"
        Tests/Support/TestPaths.cpp
        Tests/Support/TestSqliteHelpers.cpp
        Tests/Support/TestSchemaBuilders.cpp
        Tests/Support/TestSeedData.cpp
        Tests/Support/TestQueryHelpers.cpp
        Tests/Support/TestFixtures.h
        Tests/ComputedTests.cpp
        Tests/QueryPlannerBridgeTests.cpp
        Tests/Query/QueryExecutorRequireIndexTests.cpp
        Tests/Query/QueryExecutorCompositeIndexTests.cpp
        Tests/Query/QueryExecutorCompositeRangeTests.cpp
        Tests/Query/QueryExecutorCompositeOrderingTests.cpp
        Tests/Query/QueryExecutorCompositeNullDefaultTests.cpp
        Tests/Query/QueryExecutorTieBreakTests.cpp
        Tests/Query/QueryExecutorReopenUndoRedoTests.cpp
        Tests/PerformanceSmokeTests.cpp
        Tests/SchemaEdit/SchemaCreateTableTests.cpp
        Tests/SchemaEdit/SchemaPatchTests.cpp
        Tests/SchemaEdit/SchemaConstraintTests.cpp
        Tests/SchemaEdit/SchemaIndexTests.cpp
        Tests/SchemaEdit/SchemaCompositeIndexTests.cpp
        Tests/SchemaEdit/SchemaRollbackTests.cpp
        Tests/SchemaEdit/SchemaQueryIndexRepairTests.cpp
        Tests/StorageCore/StorageCoreSchemaTests.cpp
        Tests/StorageCore/StorageCoreTransactionTests.cpp
        Tests/StorageCore/StorageCoreRelationTests.cpp
        Tests/StorageCore/StorageCoreValueTests.cpp
        Tests/SqliteBackend/SqliteBasicOperationsTests.cpp
        Tests/SqliteBackend/SqliteUndoRedoTests.cpp
        Tests/SqliteBackend/SqliteEditLogTests.cpp
        Tests/SqliteBackend/SqliteBackupTests.cpp
        Tests/SqliteBackend/SqliteUpgradeTests.cpp
        Tests/SqliteBackend/SqliteImportTests.cpp
        Tests/SqliteBackend/SqliteSchemaTests.cpp
        Tests/SqliteBackend/SqliteBatchTests.cpp
        Tests/SqliteBackend/SqliteReadOnlyOpenTests.cpp
        Tests/SqliteBackend/SqliteRelationTests.cpp
        Tests/TableViewTests.cpp
    )

    include(GoogleTest)
    gtest_discover_tests(${SCSTORAGE_TESTS_TARGET} DISCOVERY_MODE PRE_TEST)

    if (SCSTORAGE_BUILD_DB_EDITOR)
        add_executable(${SCSTORAGE_DB_EDITOR_TESTS_TARGET}
            Tests/DatabaseEditorSessionTests.cpp
            Tests/DatabaseEditorSchemaTableTests.cpp
            Tools/DatabaseEditor/SCDatabaseSession.cpp
            Tools/DatabaseEditor/SCDatabaseSession.h
            Tools/DatabaseEditor/SCSchemaTableImport.cpp
            Tools/DatabaseEditor/SCSchemaTableImport.h
            Tools/DatabaseEditor/SCSchemaTableGenerator.cpp
            Tools/DatabaseEditor/SCSchemaTableGenerator.h
        )
        SCStorageConfigureOutputDirs(${SCSTORAGE_DB_EDITOR_TESTS_TARGET})
        if (MSVC)
            target_compile_options(${SCSTORAGE_DB_EDITOR_TESTS_TARGET} PRIVATE /MP)
        endif()

        target_include_directories(${SCSTORAGE_DB_EDITOR_TESTS_TARGET}
            PRIVATE
                ${CMAKE_CURRENT_SOURCE_DIR}/Tools/DatabaseEditor
        )

        target_link_libraries(${SCSTORAGE_DB_EDITOR_TESTS_TARGET}
            PRIVATE
                ${SCSTORAGE_TARGET}
                GTest::gtest
                GTest::gtest_main
                Qt6::Core
        )
        SCStorageSourceGroupTree("${CMAKE_CURRENT_SOURCE_DIR}"
            Tests/DatabaseEditorSessionTests.cpp
            Tests/DatabaseEditorSchemaTableTests.cpp
            Tools/DatabaseEditor/SCDatabaseSession.cpp
            Tools/DatabaseEditor/SCDatabaseSession.h
            Tools/DatabaseEditor/SCSchemaTableImport.cpp
            Tools/DatabaseEditor/SCSchemaTableImport.h
            Tools/DatabaseEditor/SCSchemaTableGenerator.cpp
            Tools/DatabaseEditor/SCSchemaTableGenerator.h
        )

        target_compile_features(${SCSTORAGE_DB_EDITOR_TESTS_TARGET} PRIVATE cxx_std_20)
        include(GoogleTest)
        gtest_discover_tests(${SCSTORAGE_DB_EDITOR_TESTS_TARGET} DISCOVERY_MODE PRE_TEST)
    endif()
endif()

install(TARGETS ${SCSTORAGE_TARGET}
    EXPORT SCStorageTargets
    RUNTIME DESTINATION ${SCSTORAGE_INSTALL_BIN_DIR}
    LIBRARY DESTINATION ${SCSTORAGE_INSTALL_BIN_DIR}
    ARCHIVE DESTINATION ${SCSTORAGE_INSTALL_LIB_DIR}
    INCLUDES DESTINATION ${SCSTORAGE_INSTALL_INCLUDE_DIR}
)

install(FILES $<TARGET_PDB_FILE:${SCSTORAGE_TARGET}>
    CONFIGURATIONS Debug RelWithDebInfo Release
    DESTINATION ${SCSTORAGE_INSTALL_PDB_DIR}
)

install(FILES ${SCSTORAGE_PUBLIC_SDK_FILES}
    DESTINATION ${SCSTORAGE_INSTALL_INCLUDE_DIR}
)

install(TARGETS ${SCSTORAGE_PRODUCT_EXAMPLE_TARGET}
    RUNTIME DESTINATION ${SCSTORAGE_INSTALL_BIN_DIR}
)

install(FILES
    $<TARGET_PDB_FILE:${SCSTORAGE_PRODUCT_EXAMPLE_TARGET}>
    CONFIGURATIONS Debug RelWithDebInfo Release
    DESTINATION ${SCSTORAGE_INSTALL_PDB_DIR}
)

if (SCSTORAGE_BUILD_DB_EDITOR)
    install(TARGETS SCStorageDatabaseEditor
        RUNTIME DESTINATION ${SCSTORAGE_INSTALL_BIN_DIR}
    )

    install(FILES $<TARGET_PDB_FILE:SCStorageDatabaseEditor>
        CONFIGURATIONS Debug RelWithDebInfo Release
        DESTINATION ${SCSTORAGE_INSTALL_PDB_DIR}
    )
endif()

install(EXPORT SCStorageTargets
    FILE SCStorageTargets.cmake
    NAMESPACE SCStorage::
    DESTINATION ${SCSTORAGE_INSTALL_CMAKE_DIR}
)

configure_package_config_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/SCStorageConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/SCStorageConfig.cmake
    INSTALL_DESTINATION ${SCSTORAGE_INSTALL_CMAKE_DIR}
)

write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/SCStorageConfigVersion.cmake
    VERSION 1.0.0
    COMPATIBILITY SameMajorVersion
)

install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/SCStorageConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/SCStorageConfigVersion.cmake
    DESTINATION ${SCSTORAGE_INSTALL_CMAKE_DIR}
)
