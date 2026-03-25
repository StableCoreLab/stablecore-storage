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

#include "AddColumnDialog.h"
#include "ComputedColumnDialog.h"
#include "DatabaseSession.h"
#include "RecordFilterProxyModel.h"
#include "RecordTableModel.h"
#include "RelationPickerDialog.h"

namespace stablecore::storage::editor
{

class DatabaseEditorMainWindow final : public QMainWindow
{
    Q_OBJECT

public:
    explicit DatabaseEditorMainWindow(QWidget* parent = nullptr);

private slots:
    void CreateDatabase();
    void OpenDatabase();
    void CreateTable();
    void AddColumn();
    void AddSessionComputedColumn();
    void AddRecord();
    void DeleteSelectedRecord();
    void EditSelectedRelation();
    void UndoLastAction();
    void RedoLastAction();
    void RefreshCurrentView();
    void ShowHealthSummary();
    void OnTableSelectionChanged();
    void OnGridSelectionChanged();
    void OnFilterTextChanged(const QString& text);
    void UpdateSchemaInspector();
    void UpdateRecordInspector();
    void UpdateComputedColumnsPanel();
    void UpdateGridSummary();

private:
    QModelIndex CurrentSourceIndex() const;
    void BuildUi();
    void BuildMenus();
    void ShowError(const QString& title, const QString& message);
    void SetStatusMessage(const QString& text);

    DatabaseSession* session_{nullptr};
    RecordTableModel* recordModel_{nullptr};
    RecordFilterProxyModel* filterModel_{nullptr};

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

}  // namespace stablecore::storage::editor
