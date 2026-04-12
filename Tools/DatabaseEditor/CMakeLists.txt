add_executable(SCStorageDatabaseEditor
    SCAddColumnDialog.cpp
    SCAddColumnDialog.h
    SCComputedColumnDialog.cpp
    SCComputedColumnDialog.h
    SCDatabaseEditorMainWindow.cpp
    SCDatabaseEditorMainWindow.h
    SCDatabaseSession.cpp
    SCDatabaseSession.h
    SCRecordFilterProxyModel.cpp
    SCRecordFilterProxyModel.h
    SCRecordTableModel.cpp
    SCRecordTableModel.h
    SCRelationPickerDialog.cpp
    SCRelationPickerDialog.h
    main.cpp
)
SCStorageConfigureOutputDirs(SCStorageDatabaseEditor)

target_link_libraries(SCStorageDatabaseEditor
        PRIVATE
        ${SCSTORAGE_TARGET}
        Qt6::Widgets
)

target_compile_features(SCStorageDatabaseEditor PRIVATE cxx_std_20)
