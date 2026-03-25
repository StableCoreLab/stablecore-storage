#pragma once

#include <QDockWidget>
#include <QLabel>
#include <QListWidget>
#include <QMainWindow>
#include <QPlainTextEdit>
#include <QTableView>
#include <QTreeWidget>

#include "AddColumnDialog.h"
#include "DatabaseSession.h"
#include "RecordTableModel.h"

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
    void AddRecord();
    void DeleteSelectedRecord();
    void UndoLastAction();
    void RedoLastAction();
    void RefreshCurrentView();
    void ShowHealthSummary();
    void OnTableSelectionChanged();
    void UpdateSchemaInspector();
    void UpdateRecordInspector();

private:
    void BuildUi();
    void BuildMenus();
    void ShowError(const QString& title, const QString& message);
    void SetStatusMessage(const QString& text);

    DatabaseSession* session_{nullptr};
    RecordTableModel* recordModel_{nullptr};

    QListWidget* tablesList_{nullptr};
    QTableView* dataTable_{nullptr};
    QTreeWidget* schemaTree_{nullptr};
    QTreeWidget* recordTree_{nullptr};
    QPlainTextEdit* diagnosticsText_{nullptr};
    QLabel* statusLabel_{nullptr};
};

}  // namespace stablecore::storage::editor
