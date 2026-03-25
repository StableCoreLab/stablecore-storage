add_executable(stablecore_storage_db_editor
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
stablecore_configure_output_dirs(stablecore_storage_db_editor)

target_link_libraries(stablecore_storage_db_editor
    PRIVATE
        stablecore_storage
        Qt6::Widgets
)

target_compile_features(stablecore_storage_db_editor PRIVATE cxx_std_20)
