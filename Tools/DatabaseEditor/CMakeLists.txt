add_executable(SCStorageDatabaseEditor WIN32
    SCAddColumnDialog.cpp
    SCAddColumnDialog.h
    SCComputedColumnDialog.cpp
    SCComputedColumnDialog.h
    SCDatabaseEditorMainWindow.cpp
    SCDatabaseEditorMainWindow.h
    SCDatabaseSession.cpp
    SCDatabaseSession.h
    SCSchemaTableImport.cpp
    SCSchemaTableImport.h
    SCSchemaTableImportDialog.cpp
    SCSchemaTableImportDialog.h
    SCSchemaTableDialog.cpp
    SCSchemaTableDialog.h
    SCSchemaTableGenerator.cpp
    SCSchemaTableGenerator.h
    SCRecordFilterProxyModel.cpp
    SCRecordFilterProxyModel.h
    SCRecordTableModel.cpp
    SCRecordTableModel.h
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
    SCDatabaseEditorMainWindow.cpp
    SCDatabaseEditorMainWindow.h
    SCDatabaseSession.cpp
    SCDatabaseSession.h
    SCSchemaTableImport.cpp
    SCSchemaTableImport.h
    SCSchemaTableImportDialog.cpp
    SCSchemaTableImportDialog.h
    SCSchemaTableDialog.cpp
    SCSchemaTableDialog.h
    SCSchemaTableGenerator.cpp
    SCSchemaTableGenerator.h
    SCRecordFilterProxyModel.cpp
    SCRecordFilterProxyModel.h
    SCRecordTableModel.cpp
    SCRecordTableModel.h
    SCRelationPickerDialog.cpp
    SCRelationPickerDialog.h
    main.cpp
)

target_link_libraries(SCStorageDatabaseEditor
        PRIVATE
        ${SCSTORAGE_TARGET}
        Qt6::Widgets
)

target_compile_features(SCStorageDatabaseEditor PRIVATE cxx_std_20)
if (MSVC)
    target_compile_options(SCStorageDatabaseEditor PRIVATE /MP)
endif()
