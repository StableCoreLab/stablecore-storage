#pragma once

#include <QDockWidget>
#include <QLineEdit>
#include <QLabel>
#include <QMainWindow>
#include <QModelIndex>
#include <QPoint>
#include <QPlainTextEdit>
#include <QTableView>
#include <QToolBar>
#include <QTabWidget>
#include <QTreeWidgetItem>
#include <QTreeWidget>

#include "SCAddColumnDialog.h"
#include "SCComputedColumnDialog.h"
#include "SCDatabaseSession.h"
#include "SCRecordFilterProxyModel.h"
#include "SCRecordTableModel.h"
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
        void CreateBackupCopy();
        void CreateTable();
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
        void OnSchemaContextMenuRequested(const QPoint& pos);
        void OnGridContextMenuRequested(const QPoint& pos);
        void OnTableSelectionChanged();
        void OnGridSelectionChanged();
        void OnGridHeaderClicked(int logicalIndex);
        void OnFilterTextChanged(const QString& text);
        void UpdateSchemaInspector();
        void UpdateRecordInspector();
        void UpdateComputedColumnsPanel();
        void UpdateRelationInspector();
        void UpdateGridSummary();
        void UpdateEditLogPanel();
        void UpdateDatabaseStatusBar();
        void RefreshOverviewPanels();
        void RefreshObjectExplorer();

    private:
        QModelIndex CurrentSourceIndex() const;
        QString CurrentSchemaColumnName() const;
        QString CurrentComputedColumnName() const;
        void SelectSchemaColumnByName(const QString& name);
        void SelectComputedColumnByName(const QString& name);
        void BuildUi();
        void BuildMenus();
        void ShowError(const QString& title, const QString& message);
        void SetStatusMessage(const QString& text);
        bool ExportCurrentTableCsvFile(const QString& filePath,
                                       QString* outError) const;
        bool ImportCsvIntoCurrentTableFile(const QString& filePath,
                                           QString* outError);

        SCDatabaseSession* session_{nullptr};
        SCRecordTableModel* recordModel_{nullptr};
        SCRecordFilterProxyModel* filterModel_{nullptr};

        QDockWidget* objectExplorerDock_{nullptr};
        QTreeWidget* objectTree_{nullptr};
        QWidget* databaseStatusBar_{nullptr};
        QLabel* databasePathLabel_{nullptr};
        QLabel* openModeLabel_{nullptr};
        QLabel* currentTableLabel_{nullptr};
        QLabel* tableStatsLabel_{nullptr};
        QLabel* filterStateLabel_{nullptr};
        QLabel* transactionStateLabel_{nullptr};
        QWidget* tablePage_{nullptr};
        QLabel* tableTitleLabel_{nullptr};
        QToolBar* tableToolBar_{nullptr};
        QTableView* dataTable_{nullptr};
        QLineEdit* filterEdit_{nullptr};
        QDockWidget* inspectorDock_{nullptr};
        QTabWidget* inspectorTabs_{nullptr};
        QTreeWidget* schemaTree_{nullptr};
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
    };

}  // namespace StableCore::Storage::Editor
