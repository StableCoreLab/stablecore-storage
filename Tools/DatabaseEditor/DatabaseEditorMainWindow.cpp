#include "DatabaseEditorMainWindow.h"

#include <QAction>
#include <QFileDialog>
#include <QHBoxLayout>
#include <QHeaderView>
#include <QInputDialog>
#include <QMenu>
#include <QMenuBar>
#include <QMessageBox>
#include <QSplitter>
#include <QStatusBar>
#include <QToolBar>
#include <QVBoxLayout>
#include <QWidget>

namespace sc = stablecore::storage;

namespace stablecore::storage::editor
{
namespace
{

QString ToQString(const std::wstring& text)
{
    return QString::fromStdWString(text);
}

QString ColumnKindToText(sc::ColumnKind kind)
{
    return kind == sc::ColumnKind::Relation ? QStringLiteral("Relation") : QStringLiteral("Fact");
}

QString ValueKindToText(sc::ValueKind kind)
{
    switch (kind)
    {
    case sc::ValueKind::Int64: return QStringLiteral("Int64");
    case sc::ValueKind::Double: return QStringLiteral("Double");
    case sc::ValueKind::Bool: return QStringLiteral("Bool");
    case sc::ValueKind::String: return QStringLiteral("String");
    case sc::ValueKind::RecordId: return QStringLiteral("RecordId");
    case sc::ValueKind::Enum: return QStringLiteral("Enum");
    case sc::ValueKind::Null:
    default: return QStringLiteral("Null");
    }
}

}  // namespace

DatabaseEditorMainWindow::DatabaseEditorMainWindow(QWidget* parent)
    : QMainWindow(parent)
    , session_(new DatabaseSession(this))
    , recordModel_(new RecordTableModel(session_, this))
{
    BuildUi();
    BuildMenus();

    connect(session_, &DatabaseSession::DatabaseOpened, this, [this]()
    {
        tablesList_->clear();
        tablesList_->addItems(session_->TableNames());
        diagnosticsText_->setPlainText(session_->BuildHealthSummary());
        SetStatusMessage(QStringLiteral("Database opened."));
    });
    connect(session_, &DatabaseSession::TablesChanged, this, [this]()
    {
        const QString current = session_->CurrentTableName();
        tablesList_->clear();
        tablesList_->addItems(session_->TableNames());
        if (!current.isEmpty())
        {
            const QList<QListWidgetItem*> matches = tablesList_->findItems(current, Qt::MatchExactly);
            if (!matches.isEmpty())
            {
                tablesList_->setCurrentItem(matches.front());
            }
        }
    });
    connect(session_, &DatabaseSession::CurrentTableChanged, this, [this]()
    {
        UpdateSchemaInspector();
        UpdateRecordInspector();
        diagnosticsText_->setPlainText(session_->BuildHealthSummary());
        SetStatusMessage(QStringLiteral("Table selected: ") + session_->CurrentTableName());
    });
    connect(dataTable_->selectionModel(), &QItemSelectionModel::selectionChanged, this, &DatabaseEditorMainWindow::UpdateRecordInspector);
}

void DatabaseEditorMainWindow::BuildUi()
{
    setWindowTitle(QStringLiteral("StableCore Database Editor"));
    resize(1400, 860);

    auto* splitter = new QSplitter(this);
    setCentralWidget(splitter);

    tablesList_ = new QListWidget(splitter);
    tablesList_->setMinimumWidth(220);
    connect(tablesList_, &QListWidget::itemSelectionChanged, this, &DatabaseEditorMainWindow::OnTableSelectionChanged);

    dataTable_ = new QTableView(splitter);
    dataTable_->setModel(recordModel_);
    dataTable_->horizontalHeader()->setStretchLastSection(true);
    dataTable_->setSelectionBehavior(QAbstractItemView::SelectRows);
    dataTable_->setSelectionMode(QAbstractItemView::SingleSelection);

    auto* inspectorDock = new QDockWidget(QStringLiteral("Inspector"), this);
    auto* inspectorWidget = new QWidget(inspectorDock);
    auto* inspectorLayout = new QVBoxLayout(inspectorWidget);

    schemaTree_ = new QTreeWidget(inspectorWidget);
    schemaTree_->setHeaderLabels({QStringLiteral("Schema Field"), QStringLiteral("Value")});
    inspectorLayout->addWidget(new QLabel(QStringLiteral("Schema"), inspectorWidget));
    inspectorLayout->addWidget(schemaTree_, 1);

    recordTree_ = new QTreeWidget(inspectorWidget);
    recordTree_->setHeaderLabels({QStringLiteral("Record Field"), QStringLiteral("Value")});
    inspectorLayout->addWidget(new QLabel(QStringLiteral("Selected Record"), inspectorWidget));
    inspectorLayout->addWidget(recordTree_, 1);

    inspectorDock->setWidget(inspectorWidget);
    addDockWidget(Qt::RightDockWidgetArea, inspectorDock);

    auto* diagnosticsDock = new QDockWidget(QStringLiteral("Diagnostics"), this);
    diagnosticsText_ = new QPlainTextEdit(diagnosticsDock);
    diagnosticsText_->setReadOnly(true);
    diagnosticsDock->setWidget(diagnosticsText_);
    addDockWidget(Qt::BottomDockWidgetArea, diagnosticsDock);

    statusLabel_ = new QLabel(QStringLiteral("No database opened."), this);
    statusBar()->addPermanentWidget(statusLabel_, 1);
}

void DatabaseEditorMainWindow::BuildMenus()
{
    auto* fileMenu = menuBar()->addMenu(QStringLiteral("&File"));
    fileMenu->addAction(QStringLiteral("New Database..."), this, &DatabaseEditorMainWindow::CreateDatabase);
    fileMenu->addAction(QStringLiteral("Open Database..."), this, &DatabaseEditorMainWindow::OpenDatabase);

    auto* tableMenu = menuBar()->addMenu(QStringLiteral("&Table"));
    tableMenu->addAction(QStringLiteral("Create Table..."), this, &DatabaseEditorMainWindow::CreateTable);
    tableMenu->addAction(QStringLiteral("Add Column..."), this, &DatabaseEditorMainWindow::AddColumn);
    tableMenu->addAction(QStringLiteral("Add Record"), this, &DatabaseEditorMainWindow::AddRecord);
    tableMenu->addAction(QStringLiteral("Delete Selected Record"), this, &DatabaseEditorMainWindow::DeleteSelectedRecord);
    tableMenu->addSeparator();
    tableMenu->addAction(QStringLiteral("Undo"), this, &DatabaseEditorMainWindow::UndoLastAction);
    tableMenu->addAction(QStringLiteral("Redo"), this, &DatabaseEditorMainWindow::RedoLastAction);
    tableMenu->addAction(QStringLiteral("Refresh"), this, &DatabaseEditorMainWindow::RefreshCurrentView);

    auto* toolsMenu = menuBar()->addMenu(QStringLiteral("&Tools"));
    toolsMenu->addAction(QStringLiteral("Show Health Summary"), this, &DatabaseEditorMainWindow::ShowHealthSummary);

    auto* toolbar = addToolBar(QStringLiteral("Main"));
    toolbar->addAction(QStringLiteral("Open"), this, &DatabaseEditorMainWindow::OpenDatabase);
    toolbar->addAction(QStringLiteral("New DB"), this, &DatabaseEditorMainWindow::CreateDatabase);
    toolbar->addSeparator();
    toolbar->addAction(QStringLiteral("New Table"), this, &DatabaseEditorMainWindow::CreateTable);
    toolbar->addAction(QStringLiteral("Add Column"), this, &DatabaseEditorMainWindow::AddColumn);
    toolbar->addAction(QStringLiteral("Add Record"), this, &DatabaseEditorMainWindow::AddRecord);
    toolbar->addAction(QStringLiteral("Delete Record"), this, &DatabaseEditorMainWindow::DeleteSelectedRecord);
    toolbar->addAction(QStringLiteral("Undo"), this, &DatabaseEditorMainWindow::UndoLastAction);
    toolbar->addAction(QStringLiteral("Redo"), this, &DatabaseEditorMainWindow::RedoLastAction);
    toolbar->addSeparator();
    toolbar->addAction(QStringLiteral("Refresh"), this, &DatabaseEditorMainWindow::RefreshCurrentView);
}

void DatabaseEditorMainWindow::CreateDatabase()
{
    const QString filePath = QFileDialog::getSaveFileName(this, QStringLiteral("Create Database"), QString(), QStringLiteral("SQLite Database (*.sqlite);;All Files (*)"));
    if (filePath.isEmpty())
    {
        return;
    }

    QString error;
    if (!session_->CreateDatabase(filePath, &error))
    {
        ShowError(QStringLiteral("Create Database Failed"), error);
    }
}

void DatabaseEditorMainWindow::OpenDatabase()
{
    const QString filePath = QFileDialog::getOpenFileName(this, QStringLiteral("Open Database"), QString(), QStringLiteral("SQLite Database (*.sqlite *.db);;All Files (*)"));
    if (filePath.isEmpty())
    {
        return;
    }

    QString error;
    if (!session_->OpenDatabase(filePath, &error))
    {
        ShowError(QStringLiteral("Open Database Failed"), error);
    }
}

void DatabaseEditorMainWindow::CreateTable()
{
    bool accepted = false;
    const QString tableName = QInputDialog::getText(
        this,
        QStringLiteral("Create Table"),
        QStringLiteral("Table Name"),
        QLineEdit::Normal,
        QString(),
        &accepted);
    if (!accepted || tableName.trimmed().isEmpty())
    {
        return;
    }

    QString error;
    if (!session_->CreateTable(tableName.trimmed(), &error))
    {
        ShowError(QStringLiteral("Create Table Failed"), error);
    }
}

void DatabaseEditorMainWindow::AddColumn()
{
    AddColumnDialog dialog(this);
    if (dialog.exec() != QDialog::Accepted)
    {
        return;
    }

    QString error;
    if (!session_->AddColumn(dialog.BuildColumnDef(), &error))
    {
        ShowError(QStringLiteral("Add Column Failed"), error);
        return;
    }

    UpdateSchemaInspector();
    recordModel_->Refresh();
}

void DatabaseEditorMainWindow::AddRecord()
{
    QString error;
    if (!session_->AddRecord(&error))
    {
        ShowError(QStringLiteral("Add Record Failed"), error);
    }
}

void DatabaseEditorMainWindow::DeleteSelectedRecord()
{
    const QModelIndex index = dataTable_->currentIndex();
    if (!index.isValid())
    {
        return;
    }

    const sc::RecordId recordId = recordModel_->RecordIdAt(index.row());
    if (recordId == 0)
    {
        return;
    }

    const QMessageBox::StandardButton answer = QMessageBox::question(
        this,
        QStringLiteral("Delete Record"),
        QStringLiteral("Delete record %1?").arg(recordId));
    if (answer != QMessageBox::Yes)
    {
        return;
    }

    QString error;
    if (!session_->DeleteRecord(recordId, &error))
    {
        ShowError(QStringLiteral("Delete Record Failed"), error);
    }
}

void DatabaseEditorMainWindow::UndoLastAction()
{
    QString error;
    if (!session_->Undo(&error))
    {
        ShowError(QStringLiteral("Undo Failed"), error);
        return;
    }

    recordModel_->Refresh();
    UpdateRecordInspector();
    diagnosticsText_->setPlainText(session_->BuildHealthSummary());
}

void DatabaseEditorMainWindow::RedoLastAction()
{
    QString error;
    if (!session_->Redo(&error))
    {
        ShowError(QStringLiteral("Redo Failed"), error);
        return;
    }

    recordModel_->Refresh();
    UpdateRecordInspector();
    diagnosticsText_->setPlainText(session_->BuildHealthSummary());
}

void DatabaseEditorMainWindow::RefreshCurrentView()
{
    QString error;
    if (!session_->Refresh(&error))
    {
        ShowError(QStringLiteral("Refresh Failed"), error);
        return;
    }

    recordModel_->Refresh();
    UpdateSchemaInspector();
    UpdateRecordInspector();
    diagnosticsText_->setPlainText(session_->BuildHealthSummary());
}

void DatabaseEditorMainWindow::ShowHealthSummary()
{
    diagnosticsText_->setPlainText(session_->BuildHealthSummary());
}

void DatabaseEditorMainWindow::OnTableSelectionChanged()
{
    const QListWidgetItem* item = tablesList_->currentItem();
    if (item == nullptr)
    {
        return;
    }

    QString error;
    if (!session_->SelectTable(item->text(), &error))
    {
        ShowError(QStringLiteral("Select Table Failed"), error);
        return;
    }

    recordModel_->Refresh();
}

void DatabaseEditorMainWindow::UpdateSchemaInspector()
{
    schemaTree_->clear();

    QVector<sc::ColumnDef> columns;
    QString error;
    if (!session_->BuildSchemaSnapshot(&columns, &error))
    {
        schemaTree_->addTopLevelItem(new QTreeWidgetItem({QStringLiteral("Error"), error}));
        return;
    }

    for (const sc::ColumnDef& column : columns)
    {
        auto* root = new QTreeWidgetItem(schemaTree_, {ToQString(column.name), ColumnKindToText(column.columnKind)});
        root->addChild(new QTreeWidgetItem({QStringLiteral("Display Name"), ToQString(column.displayName)}));
        root->addChild(new QTreeWidgetItem({QStringLiteral("Value Kind"), ValueKindToText(column.valueKind)}));
        root->addChild(new QTreeWidgetItem({QStringLiteral("Nullable"), column.nullable ? QStringLiteral("true") : QStringLiteral("false")}));
        root->addChild(new QTreeWidgetItem({QStringLiteral("Editable"), column.editable ? QStringLiteral("true") : QStringLiteral("false")}));
        root->addChild(new QTreeWidgetItem({QStringLiteral("Indexed"), column.indexed ? QStringLiteral("true") : QStringLiteral("false")}));
        root->addChild(new QTreeWidgetItem({QStringLiteral("Reference Table"), ToQString(column.referenceTable)}));
    }
    schemaTree_->expandAll();
}

void DatabaseEditorMainWindow::UpdateRecordInspector()
{
    recordTree_->clear();

    const QModelIndex index = dataTable_->currentIndex();
    if (!index.isValid())
    {
        return;
    }

    QVector<QPair<QString, QString>> fields;
    QString error;
    if (!session_->BuildRecordSnapshot(recordModel_->RecordIdAt(index.row()), &fields, &error))
    {
        recordTree_->addTopLevelItem(new QTreeWidgetItem({QStringLiteral("Error"), error}));
        return;
    }

    for (const auto& pair : fields)
    {
        recordTree_->addTopLevelItem(new QTreeWidgetItem({pair.first, pair.second}));
    }
    recordTree_->expandAll();
}

void DatabaseEditorMainWindow::ShowError(const QString& title, const QString& message)
{
    QMessageBox::critical(this, title, message);
    SetStatusMessage(title + QStringLiteral(": ") + message);
}

void DatabaseEditorMainWindow::SetStatusMessage(const QString& text)
{
    statusLabel_->setText(text);
}

}  // namespace stablecore::storage::editor
