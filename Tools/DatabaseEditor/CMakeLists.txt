add_executable(SCStorageDatabaseEditor WIN32
    SCAddColumnDialog.cpp
    SCAddColumnDialog.h
    SCComputedColumnDialog.cpp
    SCComputedColumnDialog.h
    SCConstraintEditorDialog.cpp
    SCConstraintEditorDialog.h
    SCDatabaseEditorMainWindow.cpp
    SCDatabaseEditorMainWindow.h
    SCDatabaseSession.cpp
    SCDatabaseSession.h
    SCIndexEditorDialog.cpp
    SCIndexEditorDialog.h
    SCSchemaTableImport.cpp
    SCSchemaTableImport.h
    SCSchemaTableImportDialog.cpp
    SCSchemaTableImportDialog.h
    SCSchemaTextPane.cpp
    SCSchemaTextPane.h
    SCSchemaTableDialog.cpp
    SCSchemaTableDialog.h
    SCSchemaTableGenerator.cpp
    SCSchemaTableGenerator.h
    SCTableDesignPane.cpp
    SCTableDesignPane.h
    SCRelationPickerDialog.cpp
    SCRelationPickerDialog.h
    main.cpp
)
set_target_properties(SCStorageDatabaseEditor PROPERTIES
    MACOSX_BUNDLE TRUE
)
SCStorageConfigureOutputDirs(SCStorageDatabaseEditor)
SCStorageSourceGroupTree("${CMAKE_CURRENT_SOURCE_DIR}"
    SCAddColumnDialog.cpp
    SCAddColumnDialog.h
    SCComputedColumnDialog.cpp
    SCComputedColumnDialog.h
    SCConstraintEditorDialog.cpp
    SCConstraintEditorDialog.h
    SCDatabaseEditorMainWindow.cpp
    SCDatabaseEditorMainWindow.h
    SCDatabaseSession.cpp
    SCDatabaseSession.h
    SCIndexEditorDialog.cpp
    SCIndexEditorDialog.h
    SCSchemaTableImport.cpp
    SCSchemaTableImport.h
    SCSchemaTableImportDialog.cpp
    SCSchemaTableImportDialog.h
    SCSchemaTextPane.cpp
    SCSchemaTextPane.h
    SCSchemaTableDialog.cpp
    SCSchemaTableDialog.h
    SCSchemaTableGenerator.cpp
    SCSchemaTableGenerator.h
    SCTableDesignPane.cpp
    SCTableDesignPane.h
    SCRelationPickerDialog.cpp
    SCRelationPickerDialog.h
    main.cpp
)

list(APPEND CMAKE_PREFIX_PATH "${CMAKE_CURRENT_SOURCE_DIR}/../../Extern/stablecore")
find_package(stablecore_ui CONFIG QUIET)
if (NOT stablecore_ui_FOUND)
    set(stablecore_ui_DIR
        "${CMAKE_CURRENT_SOURCE_DIR}/../../Extern/stablecore/Lib/cmake/stablecore_ui"
        CACHE PATH "stablecore_ui CMake directory"
    )
    find_package(stablecore_ui CONFIG REQUIRED)
endif()

target_link_libraries(SCStorageDatabaseEditor
        PRIVATE
        ${SCSTORAGE_TARGET}
        Qt6::Widgets
    stablecore_ui::SCTreeGrid
    stablecore_ui::SCCommon
)

target_compile_features(SCStorageDatabaseEditor PRIVATE cxx_std_20)
if (MSVC)
    target_compile_options(SCStorageDatabaseEditor PRIVATE /MP)
endif()
