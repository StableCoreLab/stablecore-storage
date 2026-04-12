add_executable(SCStorageDatabaseEditor
    AddColumnDialog.cpp
    AddColumnDialog.h
    ComputedColumnDialog.cpp
    ComputedColumnDialog.h
    DatabaseEditorMainWindow.cpp
    DatabaseEditorMainWindow.h
    DatabaseSession.cpp
    DatabaseSession.h
    RecordFilterProxyModel.cpp
    RecordFilterProxyModel.h
    RecordTableModel.cpp
    RecordTableModel.h
    RelationPickerDialog.cpp
    RelationPickerDialog.h
    main.cpp
)
stablecore_configure_output_dirs(SCStorageDatabaseEditor)

target_link_libraries(SCStorageDatabaseEditor
    PRIVATE
        ${STABLECORE_STORAGE_TARGET}
        Qt6::Widgets
)

target_compile_features(SCStorageDatabaseEditor PRIVATE cxx_std_20)
