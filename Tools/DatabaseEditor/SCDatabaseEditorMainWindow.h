#pragma once

#include <QDockWidget>
#include <QLineEdit>
#include <QLabel>
#include <QListWidget>
#include <QMainWindow>
#include <QModelIndex>
#include <QPlainTextEdit>
#include <QTableView>
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
    void CreateTable();
    void AddColumn();
    void AddSessionComputedColumn();
    void EditSelectedComputedColumn();
    void DeleteSelectedComputedColumn();
    void AddRecord();
    void DeleteSelectedRecord();
    void EditSelectedRelation();
    void UndoLastAction();
    void RedoLastAction();
    void RefreshCurrentView();
    void ShowHealthSummary();
    void ExportDebugPackage();
    void OnTableSelectionChanged();
    void OnGridSelectionChanged();
    void OnFilterTextChanged(const QString& text);
    void UpdateSchemaInspector();
    void UpdateRecordInspector();
    void UpdateComputedColumnsPanel();
    void UpdateGridSummary();

private:
    QModelIndex CurrentSourceIndex() const;
    QString CurrentComputedColumnName() const;
    void SelectComputedColumnByName(const QString& name);
    void BuildUi();
    void BuildMenus();
    void ShowError(const QString& title, const QString& message);
    void SetStatusMessage(const QString& text);

    SCDatabaseSession* session_{nullptr};
    SCRecordTableModel* recordModel_{nullptr};
    SCRecordFilterProxyModel* filterModel_{nullptr};

    QListWidget* tablesList_{nullptr};
    QTableView* dataTable_{nullptr};
    QLineEdit* filterEdit_{nullptr};
    QLabel* tableSummaryLabel_{nullptr};
    QTreeWidget* schemaTree_{nullptr};
    QTreeWidget* recordTree_{nullptr};
    QTreeWidget* computedColumnsTree_{nullptr};
    QPlainTextEdit* diagnosticsText_{nullptr};
    QLabel* statusLabel_{nullptr};
};

}  // namespace StableCore::Storage::Editor
