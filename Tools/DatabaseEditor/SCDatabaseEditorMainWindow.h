#pragma once

#include <QDockWidget>
#include <QAction>
#include <QLineEdit>
#include <QLabel>
#include <QMainWindow>
#include <QPoint>
#include <QPlainTextEdit>
#include <QStackedWidget>
#include <QTabBar>
#include <QToolBar>
#include <QTabWidget>
#include <QTreeWidgetItem>
#include <QTreeWidget>

#include <SCTreeGrid/SCTreeGridCtrl.h>

#include "SCAddColumnDialog.h"
#include "SCComputedColumnDialog.h"
#include "SCDatabaseSession.h"
#include "SCRelationPickerDialog.h"

namespace StableCore::Storage::Editor
{

    class SCDatabaseEditorMainWindow final : public QMainWindow
    {
        Q_OBJECT

    public:
        explicit SCDatabaseEditorMainWindow(QWidget* parent = nullptr);

    private slots:
        void CreateDatabase();
        void OpenDatabase();
        void CloseDatabase();
        void CreateBackupCopy();
        void CreateTable();
        void CreateTableFromSchemaDescription();
        void DeleteSelectedTable();
        void OpenSchemaTableConverter();
        void AddColumn();
        void EditSelectedColumn();
        void DeleteSelectedColumn();
        void ConvertSelectedColumnToComputed();
        void AddSessionComputedColumn();
        void EditSelectedComputedColumn();
        void ConvertSelectedComputedToColumn();
        void DeleteSelectedComputedColumn();
        void AddRecord();
        void DeleteSelectedRecord();
        void SavePendingChanges();
        void DiscardPendingChanges();
        void EditSelectedRelation();
        void UndoLastAction();
        void RedoLastAction();
        void RefreshCurrentView();
        void ShowHealthSummary();
        void ShowEditLogSummary();
        void ExportDebugPackage();
        void ExportCurrentTableCsv();
        void ImportCsvIntoCurrentTable();
        void OnObjectExplorerContextMenuRequested(const QPoint& pos);
        void OnGridContextMenuRequested(const QPoint& pos);
        void OnTableSelectionChanged();
        void OnGridCellSelected(int row, int col);
        void OnFilterTextChanged(const QString& text);
        void OnWorkspaceModeChanged(int index);
        void RefreshCurrentDetailsPanel();
        void RefreshCurrentRecordPanel();
        void UpdateComputedColumnsPanel();
        void RefreshRelationPanel();
        void UpdateGridSummary();
        void UpdateEditLogPanel();
        void UpdateDatabaseStatusBar();
        void RefreshOverviewPanels();
        void RefreshObjectExplorer();

    private:
        enum class WorkspaceMode
        {
            Data = 0,
            Design = 1,
            SchemaText = 2,
        };

        struct GridRowData
        {
            StableCore::Storage::RecordId recordId{0};
            QString filterText;
        };

        int CurrentRow() const;
        int CurrentColumn() const;
        StableCore::Storage::RecordId RecordIdAt(int row) const;
        StableCore::Storage::SCTableViewColumnDef ColumnAt(int col) const;

        QString CurrentDesignColumnName() const;
        QString CurrentComputedColumnName() const;
        QString ExplorerSelectedTableName() const;
        void SelectDesignColumnByName(const QString& name);
        void SelectComputedColumnByName(const QString& name);
        void SelectTableInExplorer(const QString& tableName);
        void BuildUi();
        void BuildMenus();
        void WireGridCallbacks();
        void SetWorkspaceMode(WorkspaceMode mode);
        void RefreshWorkspaceHeader();
        void RefreshWorkspacePages();
        void RefreshGridData();
        bool RowPassesQuickFilter(int row) const;
        void ShowError(const QString& title, const QString& message);
        void SetStatusMessage(const QString& text);
        bool ExportCurrentTableCsvFile(const QString& filePath,
                                       QString* outError) const;
        bool ImportCsvIntoCurrentTableFile(const QString& filePath,
                                           QString* outError);

        SCDatabaseSession* session_{nullptr};

        // Grid state
        SCTreeGrid* dataTable_{nullptr};
        QVector<StableCore::Storage::SCTableViewColumnDef> columns_;
        QVector<GridRowData> visibleRows_;
        int currentRow_{-1};
        int currentColumn_{-1};

        QDockWidget* objectExplorerDock_{nullptr};
        QTreeWidget* objectTree_{nullptr};
        QLabel* openModeLabel_{nullptr};
        QLabel* currentTableLabel_{nullptr};
        QLabel* tableStatsLabel_{nullptr};
        QLabel* filterStateLabel_{nullptr};
        QLabel* transactionStateLabel_{nullptr};
        QWidget* workspaceHeader_{nullptr};
        QLabel* workspaceTitleLabel_{nullptr};
        QLabel* workspaceStatsLabel_{nullptr};
        QLabel* workspaceModeStateLabel_{nullptr};
        QTabBar* workspaceModeBar_{nullptr};
        QStackedWidget* workspaceStack_{nullptr};
        QWidget* tablePage_{nullptr};
        QToolBar* tableToolBar_{nullptr};
        QAction* closeDatabaseAction_{nullptr};
        QAction* undoAction_{nullptr};
        QAction* redoAction_{nullptr};
        QAction* savePendingChangesAction_{nullptr};
        QAction* discardPendingChangesAction_{nullptr};
        QLineEdit* filterEdit_{nullptr};
        class SCTableDesignPane* tableDesignPane_{nullptr};
        class SCSchemaTextPane* schemaTextPane_{nullptr};
        QDockWidget* currentDetailsDock_{nullptr};
        QTabWidget* currentDetailsTabs_{nullptr};
        QTreeWidget* recordTree_{nullptr};
        QTreeWidget* computedColumnsTree_{nullptr};
        QTreeWidget* relationTree_{nullptr};
        QDockWidget* bottomDock_{nullptr};
        QTabWidget* bottomTabs_{nullptr};
        QPlainTextEdit* diagnosticsText_{nullptr};
        QTreeWidget* editLogTree_{nullptr};
        QPlainTextEdit* editStateText_{nullptr};
        QPlainTextEdit* healthSummaryText_{nullptr};
        QPlainTextEdit* sqlPreviewText_{nullptr};
        QPlainTextEdit* debugPackageText_{nullptr};
        QLabel* statusLabel_{nullptr};
        WorkspaceMode workspaceMode_{WorkspaceMode::Data};
    };

}  // namespace StableCore::Storage::Editor
