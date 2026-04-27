#include "SCDatabaseEditorMainWindow.h"

#include <QAction>
#include <QDateTime>
#include <QFileDialog>
#include <QHBoxLayout>
#include <QHeaderView>
#include <QInputDialog>
#include <QItemSelectionModel>
#include <QMenu>
#include <QMenuBar>
#include <QMessageBox>
#include <QPushButton>
#include <QRegularExpression>
#include <QSignalBlocker>
#include <QSplitter>
#include <QStatusBar>
#include <QToolBar>
#include <QVBoxLayout>
#include <QWidget>

#include <vector>

namespace sc = StableCore::Storage;

namespace StableCore::Storage::Editor
{
    namespace
    {

        QString ToQString(const std::wstring& text)
        {
            return QString::fromStdWString(text);
        }

        QString ColumnKindToText(sc::ColumnKind kind)
        {
            return kind == sc::ColumnKind::Relation ? QStringLiteral("Relation")
                                                    : QStringLiteral("Fact");
        }

        QString ValueKindToText(sc::ValueKind kind)
        {
            switch (kind)
            {
                case sc::ValueKind::Int64:
                    return QStringLiteral("Int64");
                case sc::ValueKind::Double:
                    return QStringLiteral("Double");
                case sc::ValueKind::Bool:
                    return QStringLiteral("Bool");
                case sc::ValueKind::String:
                    return QStringLiteral("String");
                case sc::ValueKind::RecordId:
                    return QStringLiteral("RecordId");
                case sc::ValueKind::Enum:
                    return QStringLiteral("Enum");
                case sc::ValueKind::Null:
                default:
                    return QStringLiteral("Null");
            }
        }

        QString OpenModeToText(sc::SCDatabaseOpenMode mode)
        {
            switch (mode)
            {
                case sc::SCDatabaseOpenMode::Normal:
                    return QStringLiteral("Normal");
                case sc::SCDatabaseOpenMode::NoHistory:
                    return QStringLiteral("NoHistory");
                case sc::SCDatabaseOpenMode::ReadOnly:
                    return QStringLiteral("ReadOnly");
                default:
                    return QStringLiteral("Unknown");
            }
        }

        QString EditLogActionKindToText(sc::SCEditLogActionKind kind)
        {
            switch (kind)
            {
                case sc::SCEditLogActionKind::Commit:
                    return QStringLiteral("Commit");
                case sc::SCEditLogActionKind::Undo:
                    return QStringLiteral("Undo");
                case sc::SCEditLogActionKind::Redo:
                    return QStringLiteral("Redo");
                case sc::SCEditLogActionKind::Import:
                    return QStringLiteral("Import");
                case sc::SCEditLogActionKind::RuleWriteback:
                    return QStringLiteral("RuleWriteback");
                case sc::SCEditLogActionKind::SaveBaseline:
                    return QStringLiteral("SaveBaseline");
                default:
                    return QStringLiteral("Unknown");
            }
        }

        QString FormatUtcTimestamp(std::uint64_t timestampUtcMs)
        {
            if (timestampUtcMs == 0)
            {
                return QStringLiteral("-");
            }

            const QDateTime dateTime = QDateTime::fromMSecsSinceEpoch(
                static_cast<qint64>(timestampUtcMs), Qt::UTC);
            return dateTime.toString(Qt::ISODate);
        }

    }  // namespace

    SCDatabaseEditorMainWindow::SCDatabaseEditorMainWindow(QWidget* parent)
        : QMainWindow(parent),
          session_(new SCDatabaseSession(this)),
          recordModel_(new SCRecordTableModel(session_, this)),
          filterModel_(new SCRecordFilterProxyModel(this))
    {
        filterModel_->setSourceModel(recordModel_);
        BuildUi();
        BuildMenus();

        connect(session_, &SCDatabaseSession::DatabaseOpened, this, [this]() {
            const QSignalBlocker blocker(tablesList_);
            const QString current = session_->CurrentTableName();
            tablesList_->clear();
            tablesList_->addItems(session_->TableNames());
            if (!current.isEmpty())
            {
                const QList<QListWidgetItem*> matches =
                    tablesList_->findItems(current, Qt::MatchExactly);
                if (!matches.isEmpty())
                {
                    tablesList_->setCurrentItem(matches.front());
                }
            }
            RefreshOverviewPanels();
            UpdateGridSummary();
            SetStatusMessage(QStringLiteral("Database opened."));
        });
        connect(session_, &SCDatabaseSession::TablesChanged, this, [this]() {
            const QSignalBlocker blocker(tablesList_);
            const QString current = session_->CurrentTableName();
            tablesList_->clear();
            tablesList_->addItems(session_->TableNames());
            if (!current.isEmpty())
            {
                const QList<QListWidgetItem*> matches =
                    tablesList_->findItems(current, Qt::MatchExactly);
                if (!matches.isEmpty())
                {
                    tablesList_->setCurrentItem(matches.front());
                }
            }
            RefreshOverviewPanels();
        });
        connect(session_, &SCDatabaseSession::CurrentTableChanged, this,
                [this]() {
                    const QString current = session_->CurrentTableName();
                    if (!current.isEmpty())
                    {
                        const QSignalBlocker blocker(tablesList_);
                        const QList<QListWidgetItem*> matches =
                            tablesList_->findItems(current, Qt::MatchExactly);
                        if (!matches.isEmpty())
                        {
                            tablesList_->setCurrentItem(matches.front());
                        }
                    }
                    UpdateSchemaInspector();
                    UpdateRecordInspector();
                    UpdateComputedColumnsPanel();
                    RefreshOverviewPanels();
                    dataTable_->resizeColumnsToContents();
                    UpdateGridSummary();
                    SetStatusMessage(QStringLiteral("Table selected: ") +
                                     session_->CurrentTableName());
                });
        connect(recordModel_, &QAbstractItemModel::modelReset, this,
                &SCDatabaseEditorMainWindow::UpdateGridSummary);
        connect(dataTable_->selectionModel(),
                &QItemSelectionModel::selectionChanged, this,
                &SCDatabaseEditorMainWindow::OnGridSelectionChanged);
        connect(session_, &SCDatabaseSession::RecordsChanged, this,
                &SCDatabaseEditorMainWindow::RefreshOverviewPanels);
    }

    void SCDatabaseEditorMainWindow::BuildUi()
    {
        setWindowTitle(QStringLiteral("StableCore Database Editor"));
        resize(1400, 860);

        auto* splitter = new QSplitter(this);
        setCentralWidget(splitter);

        tablesList_ = new QListWidget(splitter);
        tablesList_->setMinimumWidth(220);
        connect(tablesList_, &QListWidget::itemSelectionChanged, this,
                &SCDatabaseEditorMainWindow::OnTableSelectionChanged);

        auto* centerWidget = new QWidget(splitter);
        auto* centerLayout = new QVBoxLayout(centerWidget);
        centerLayout->setContentsMargins(0, 0, 0, 0);

        auto* toolsLayout = new QHBoxLayout();
        tableSummaryLabel_ =
            new QLabel(QStringLiteral("No table selected"), centerWidget);
        filterEdit_ = new QLineEdit(centerWidget);
        filterEdit_->setPlaceholderText(QStringLiteral("Filter current table"));
        auto* clearFilterButton =
            new QPushButton(QStringLiteral("Clear"), centerWidget);
        auto* relationButton =
            new QPushButton(QStringLiteral("Pick Relation"), centerWidget);
        auto* computedButton = new QPushButton(
            QStringLiteral("Add Computed Column"), centerWidget);
        auto* editComputedButton =
            new QPushButton(QStringLiteral("Edit Computed"), centerWidget);
        auto* deleteComputedButton =
            new QPushButton(QStringLiteral("Delete Computed"), centerWidget);

        toolsLayout->addWidget(tableSummaryLabel_, 1);
        toolsLayout->addWidget(filterEdit_, 2);
        toolsLayout->addWidget(clearFilterButton);
        toolsLayout->addWidget(relationButton);
        toolsLayout->addWidget(computedButton);
        toolsLayout->addWidget(editComputedButton);
        toolsLayout->addWidget(deleteComputedButton);
        centerLayout->addLayout(toolsLayout);

        dataTable_ = new QTableView(centerWidget);
        dataTable_->setModel(filterModel_);
        dataTable_->horizontalHeader()->setStretchLastSection(true);
        dataTable_->horizontalHeader()->setSectionsMovable(true);
        dataTable_->setAlternatingRowColors(true);
        dataTable_->setSelectionBehavior(QAbstractItemView::SelectRows);
        dataTable_->setSelectionMode(QAbstractItemView::SingleSelection);
        dataTable_->setSortingEnabled(true);
        dataTable_->setWordWrap(false);
        centerLayout->addWidget(dataTable_, 1);

        connect(filterEdit_, &QLineEdit::textChanged, this,
                &SCDatabaseEditorMainWindow::OnFilterTextChanged);
        connect(clearFilterButton, &QPushButton::clicked, filterEdit_,
                &QLineEdit::clear);
        connect(relationButton, &QPushButton::clicked, this,
                &SCDatabaseEditorMainWindow::EditSelectedRelation);
        connect(computedButton, &QPushButton::clicked, this,
                &SCDatabaseEditorMainWindow::AddSessionComputedColumn);
        connect(editComputedButton, &QPushButton::clicked, this,
                &SCDatabaseEditorMainWindow::EditSelectedComputedColumn);
        connect(deleteComputedButton, &QPushButton::clicked, this,
                &SCDatabaseEditorMainWindow::DeleteSelectedComputedColumn);

        splitter->addWidget(centerWidget);

        auto* inspectorDock =
            new QDockWidget(QStringLiteral("Inspector"), this);
        auto* inspectorWidget = new QWidget(inspectorDock);
        auto* inspectorLayout = new QVBoxLayout(inspectorWidget);

        schemaTree_ = new QTreeWidget(inspectorWidget);
        schemaTree_->setHeaderLabels(
            {QStringLiteral("Schema Field"), QStringLiteral("SCValue")});
        inspectorLayout->addWidget(
            new QLabel(QStringLiteral("Schema"), inspectorWidget));
        inspectorLayout->addWidget(schemaTree_, 1);

        recordTree_ = new QTreeWidget(inspectorWidget);
        recordTree_->setHeaderLabels(
            {QStringLiteral("Record Field"), QStringLiteral("SCValue")});
        inspectorLayout->addWidget(
            new QLabel(QStringLiteral("Selected Record"), inspectorWidget));
        inspectorLayout->addWidget(recordTree_, 1);

        inspectorDock->setWidget(inspectorWidget);
        addDockWidget(Qt::RightDockWidgetArea, inspectorDock);

        auto* computedDock =
            new QDockWidget(QStringLiteral("Session Computed Columns"), this);
        computedColumnsTree_ = new QTreeWidget(computedDock);
        computedColumnsTree_->setHeaderLabels(
            {QStringLiteral("Column"), QStringLiteral("Definition")});
        computedDock->setWidget(computedColumnsTree_);
        addDockWidget(Qt::RightDockWidgetArea, computedDock);

        auto* diagnosticsDock =
            new QDockWidget(QStringLiteral("Diagnostics"), this);
        diagnosticsText_ = new QPlainTextEdit(diagnosticsDock);
        diagnosticsText_->setReadOnly(true);
        diagnosticsDock->setWidget(diagnosticsText_);
        addDockWidget(Qt::BottomDockWidgetArea, diagnosticsDock);
        diagnosticsText_->setPlainText(QStringLiteral("No database opened."));

        editLogDock_ =
            new QDockWidget(QStringLiteral("Edit Log / State Summary"), this);
        auto* editLogWidget = new QWidget(editLogDock_);
        auto* editLogLayout = new QVBoxLayout(editLogWidget);

        editStateText_ = new QPlainTextEdit(editLogWidget);
        editStateText_->setReadOnly(true);
        editStateText_->setMinimumHeight(110);
        editLogLayout->addWidget(
            new QLabel(QStringLiteral("State Summary"), editLogWidget));
        editLogLayout->addWidget(editStateText_);

        editLogTree_ = new QTreeWidget(editLogWidget);
        editLogTree_->setHeaderLabels(
            {QStringLiteral("Side"), QStringLiteral("Version"),
             QStringLiteral("Action"), QStringLiteral("Commit"),
             QStringLiteral("Timestamp"), QStringLiteral("Text"),
             QStringLiteral("Detail")});
        editLogLayout->addWidget(
            new QLabel(QStringLiteral("Edit Log Items"), editLogWidget));
        editLogLayout->addWidget(editLogTree_, 1);

        editLogDock_->setWidget(editLogWidget);
        addDockWidget(Qt::BottomDockWidgetArea, editLogDock_);
        tabifyDockWidget(diagnosticsDock, editLogDock_);
        editLogDock_->raise();
        editStateText_->setPlainText(QStringLiteral("No database opened."));

        statusLabel_ = new QLabel(QStringLiteral("No database opened."), this);
        statusBar()->addPermanentWidget(statusLabel_, 1);
    }

    void SCDatabaseEditorMainWindow::BuildMenus()
    {
        auto* fileMenu = menuBar()->addMenu(QStringLiteral("&File"));
        fileMenu->addAction(QStringLiteral("New Database..."), this,
                            &SCDatabaseEditorMainWindow::CreateDatabase);
        fileMenu->addAction(QStringLiteral("Open Database..."), this,
                            &SCDatabaseEditorMainWindow::OpenDatabase);

        auto* tableMenu = menuBar()->addMenu(QStringLiteral("&Table"));
        tableMenu->addAction(QStringLiteral("Create Table..."), this,
                             &SCDatabaseEditorMainWindow::CreateTable);
        tableMenu->addAction(QStringLiteral("Add Column..."), this,
                             &SCDatabaseEditorMainWindow::AddColumn);
        tableMenu->addAction(
            QStringLiteral("Add Session Computed Column..."), this,
            &SCDatabaseEditorMainWindow::AddSessionComputedColumn);
        tableMenu->addAction(
            QStringLiteral("Edit Selected Computed Column..."), this,
            &SCDatabaseEditorMainWindow::EditSelectedComputedColumn);
        tableMenu->addAction(
            QStringLiteral("Delete Selected Computed Column"), this,
            &SCDatabaseEditorMainWindow::DeleteSelectedComputedColumn);
        tableMenu->addAction(QStringLiteral("Add Record"), this,
                             &SCDatabaseEditorMainWindow::AddRecord);
        tableMenu->addAction(QStringLiteral("Delete Selected Record"), this,
                             &SCDatabaseEditorMainWindow::DeleteSelectedRecord);
        tableMenu->addAction(QStringLiteral("Pick Selected Relation..."), this,
                             &SCDatabaseEditorMainWindow::EditSelectedRelation);
        tableMenu->addSeparator();
        tableMenu->addAction(QStringLiteral("Undo"), this,
                             &SCDatabaseEditorMainWindow::UndoLastAction);
        tableMenu->addAction(QStringLiteral("Redo"), this,
                             &SCDatabaseEditorMainWindow::RedoLastAction);
        tableMenu->addAction(QStringLiteral("Refresh"), this,
                             &SCDatabaseEditorMainWindow::RefreshCurrentView);

        auto* toolsMenu = menuBar()->addMenu(QStringLiteral("&Tools"));
        toolsMenu->addAction(QStringLiteral("Show Health Summary"), this,
                             &SCDatabaseEditorMainWindow::ShowHealthSummary);
        toolsMenu->addAction(QStringLiteral("Show Edit Log / State Summary"),
                             this,
                             &SCDatabaseEditorMainWindow::ShowEditLogSummary);
        toolsMenu->addAction(QStringLiteral("Export Debug Package..."), this,
                             &SCDatabaseEditorMainWindow::ExportDebugPackage);

        auto* toolbar = addToolBar(QStringLiteral("Main"));
        toolbar->addAction(QStringLiteral("Open"), this,
                           &SCDatabaseEditorMainWindow::OpenDatabase);
        toolbar->addAction(QStringLiteral("New DB"), this,
                           &SCDatabaseEditorMainWindow::CreateDatabase);
        toolbar->addSeparator();
        toolbar->addAction(QStringLiteral("New Table"), this,
                           &SCDatabaseEditorMainWindow::CreateTable);
        toolbar->addAction(QStringLiteral("Add Column"), this,
                           &SCDatabaseEditorMainWindow::AddColumn);
        toolbar->addAction(
            QStringLiteral("Add Computed"), this,
            &SCDatabaseEditorMainWindow::AddSessionComputedColumn);
        toolbar->addAction(
            QStringLiteral("Edit Computed"), this,
            &SCDatabaseEditorMainWindow::EditSelectedComputedColumn);
        toolbar->addAction(
            QStringLiteral("Delete Computed"), this,
            &SCDatabaseEditorMainWindow::DeleteSelectedComputedColumn);
        toolbar->addAction(QStringLiteral("Add Record"), this,
                           &SCDatabaseEditorMainWindow::AddRecord);
        toolbar->addAction(QStringLiteral("Delete Record"), this,
                           &SCDatabaseEditorMainWindow::DeleteSelectedRecord);
        toolbar->addAction(QStringLiteral("Pick Relation"), this,
                           &SCDatabaseEditorMainWindow::EditSelectedRelation);
        toolbar->addAction(QStringLiteral("Undo"), this,
                           &SCDatabaseEditorMainWindow::UndoLastAction);
        toolbar->addAction(QStringLiteral("Redo"), this,
                           &SCDatabaseEditorMainWindow::RedoLastAction);
        toolbar->addSeparator();
        toolbar->addAction(QStringLiteral("Refresh"), this,
                           &SCDatabaseEditorMainWindow::RefreshCurrentView);
    }

    void SCDatabaseEditorMainWindow::CreateDatabase()
    {
        const QString filePath = QFileDialog::getSaveFileName(
            this, QStringLiteral("Create Database"), QString(),
            QStringLiteral("SQLite Database (*.sqlite);;All Files (*)"));
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

    void SCDatabaseEditorMainWindow::OpenDatabase()
    {
        const QString filePath = QFileDialog::getOpenFileName(
            this, QStringLiteral("Open Database"), QString(),
            QStringLiteral("SQLite Database (*.sqlite *.db);;All Files (*)"));
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

    void SCDatabaseEditorMainWindow::CreateTable()
    {
        bool accepted = false;
        const QString tableName = QInputDialog::getText(
            this, QStringLiteral("Create Table"), QStringLiteral("Table Name"),
            QLineEdit::Normal, QString(), &accepted);
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

    void SCDatabaseEditorMainWindow::AddColumn()
    {
        SCAddColumnDialog dialog(this);
        if (dialog.exec() != QDialog::Accepted)
        {
            return;
        }

        const sc::SCColumnDef column = dialog.BuildColumnDef();
        QString error;
        if (!session_->AddColumn(column, &error))
        {
            ShowError(QStringLiteral("Add Column Failed"), error);
            return;
        }

        UpdateSchemaInspector();
        recordModel_->Refresh();
        RefreshOverviewPanels();
        SetStatusMessage(QStringLiteral("Column added: ") +
                         ToQString(column.name));
    }

    void SCDatabaseEditorMainWindow::AddSessionComputedColumn()
    {
        if (!session_->CurrentTable())
        {
            ShowError(QStringLiteral("Add Computed Column Failed"),
                      QStringLiteral("No table is selected."));
            return;
        }

        SCComputedColumnDialog dialog(session_->CurrentTableName(), this);
        if (dialog.exec() != QDialog::Accepted)
        {
            return;
        }

        sc::SCComputedColumnDef definition;
        QString error;
        if (!dialog.BuildDefinition(&definition, &error))
        {
            ShowError(QStringLiteral("Add Computed Column Failed"), error);
            return;
        }

        if (!session_->AddSessionComputedColumn(definition, &error))
        {
            ShowError(QStringLiteral("Add Computed Column Failed"), error);
            return;
        }

        recordModel_->Refresh();
        UpdateComputedColumnsPanel();
        SelectComputedColumnByName(ToQString(definition.name));
        RefreshOverviewPanels();
        SetStatusMessage(QStringLiteral("Computed column added: ") +
                         ToQString(definition.name));
    }

    void SCDatabaseEditorMainWindow::EditSelectedComputedColumn()
    {
        const QString columnName = CurrentComputedColumnName();
        if (columnName.isEmpty())
        {
            ShowError(QStringLiteral("Edit Computed Column Failed"),
                      QStringLiteral("Select a computed column in the Session "
                                     "Computed Columns panel."));
            return;
        }

        sc::SCComputedColumnDef existing;
        QString error;
        if (!session_->GetSessionComputedColumn(columnName, &existing, &error))
        {
            ShowError(QStringLiteral("Edit Computed Column Failed"), error);
            return;
        }

        SCComputedColumnDialog dialog(session_->CurrentTableName(), existing,
                                      this);
        if (dialog.exec() != QDialog::Accepted)
        {
            return;
        }

        sc::SCComputedColumnDef updated;
        if (!dialog.BuildDefinition(&updated, &error))
        {
            ShowError(QStringLiteral("Edit Computed Column Failed"), error);
            return;
        }

        if (!session_->UpdateSessionComputedColumn(columnName, updated, &error))
        {
            ShowError(QStringLiteral("Edit Computed Column Failed"), error);
            return;
        }

        recordModel_->Refresh();
        UpdateComputedColumnsPanel();
        SelectComputedColumnByName(ToQString(updated.name));
        RefreshOverviewPanels();
        SetStatusMessage(QStringLiteral("Computed column updated: ") +
                         ToQString(updated.name));
    }

    void SCDatabaseEditorMainWindow::DeleteSelectedComputedColumn()
    {
        const QString columnName = CurrentComputedColumnName();
        if (columnName.isEmpty())
        {
            ShowError(QStringLiteral("Delete Computed Column Failed"),
                      QStringLiteral("Select a computed column in the Session "
                                     "Computed Columns panel."));
            return;
        }

        const QMessageBox::StandardButton answer = QMessageBox::question(
            this, QStringLiteral("Delete Computed Column"),
            QStringLiteral("Delete session computed column \"%1\"?")
                .arg(columnName));
        if (answer != QMessageBox::Yes)
        {
            return;
        }

        const QVector<sc::SCComputedColumnDef> columnsBeforeDelete =
            session_->CurrentSessionComputedColumns();
        QString fallbackSelection;
        for (int index = 0; index < columnsBeforeDelete.size(); ++index)
        {
            if (ToQString(columnsBeforeDelete[index].name)
                    .compare(columnName, Qt::CaseInsensitive) != 0)
            {
                continue;
            }

            if (index + 1 < columnsBeforeDelete.size())
            {
                fallbackSelection =
                    ToQString(columnsBeforeDelete[index + 1].name);
            } else if (index - 1 >= 0)
            {
                fallbackSelection =
                    ToQString(columnsBeforeDelete[index - 1].name);
            }
            break;
        }

        QString error;
        if (!session_->RemoveSessionComputedColumn(columnName, &error))
        {
            ShowError(QStringLiteral("Delete Computed Column Failed"), error);
            return;
        }

        recordModel_->Refresh();
        UpdateComputedColumnsPanel();
        SelectComputedColumnByName(fallbackSelection);
        RefreshOverviewPanels();
        SetStatusMessage(QStringLiteral("Computed column deleted: ") +
                         columnName);
    }

    void SCDatabaseEditorMainWindow::AddRecord()
    {
        QString error;
        if (!session_->AddRecord(&error))
        {
            ShowError(QStringLiteral("Add Record Failed"), error);
            return;
        }

        SetStatusMessage(QStringLiteral("Record added."));
    }

    void SCDatabaseEditorMainWindow::DeleteSelectedRecord()
    {
        const QModelIndex index = CurrentSourceIndex();
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
            this, QStringLiteral("Delete Record"),
            QStringLiteral("Delete record %1?").arg(recordId));
        if (answer != QMessageBox::Yes)
        {
            return;
        }

        QString error;
        if (!session_->DeleteRecord(recordId, &error))
        {
            ShowError(QStringLiteral("Delete Record Failed"), error);
            return;
        }

        SetStatusMessage(QStringLiteral("Record deleted: ") +
                         QString::number(recordId));
    }

    void SCDatabaseEditorMainWindow::EditSelectedRelation()
    {
        const QModelIndex index = CurrentSourceIndex();
        if (!index.isValid())
        {
            return;
        }

        const sc::SCTableViewColumnDef tableColumn =
            recordModel_->ColumnAt(index.column());
        if (tableColumn.layer != sc::TableColumnLayer::Fact)
        {
            ShowError(QStringLiteral("Pick Relation Failed"),
                      QStringLiteral(
                          "Computed columns cannot store relation values."));
            return;
        }

        sc::SCColumnDef column;
        QString error;
        if (!session_->GetColumnDef(ToQString(tableColumn.name), &column,
                                    &error))
        {
            ShowError(QStringLiteral("Pick Relation Failed"), error);
            return;
        }
        if (column.columnKind != sc::ColumnKind::Relation)
        {
            ShowError(
                QStringLiteral("Pick Relation Failed"),
                QStringLiteral("The selected column is not a relation field."));
            return;
        }

        QVector<SCDatabaseSession::RelationCandidate> candidates;
        if (!session_->BuildRelationCandidates(ToQString(column.referenceTable),
                                               &candidates, &error))
        {
            ShowError(QStringLiteral("Pick Relation Failed"), error);
            return;
        }

        SCRelationPickerDialog dialog(ToQString(column.referenceTable),
                                      candidates, this);
        if (dialog.exec() != QDialog::Accepted)
        {
            return;
        }

        if (!session_->SetCellValue(
                recordModel_->RecordIdAt(index.row()),
                ToQString(tableColumn.name),
                QVariant::fromValue<qlonglong>(dialog.SelectedRecordId()),
                &error))
        {
            ShowError(QStringLiteral("Pick Relation Failed"), error);
            return;
        }

        recordModel_->Refresh();
        SetStatusMessage(QStringLiteral("Relation updated."));
    }

    void SCDatabaseEditorMainWindow::UndoLastAction()
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

    void SCDatabaseEditorMainWindow::RedoLastAction()
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

    void SCDatabaseEditorMainWindow::RefreshCurrentView()
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
        UpdateComputedColumnsPanel();
        RefreshOverviewPanels();
    }

    void SCDatabaseEditorMainWindow::ShowHealthSummary()
    {
        diagnosticsText_->setPlainText(session_->BuildHealthSummary());
    }

    void SCDatabaseEditorMainWindow::ShowEditLogSummary()
    {
        if (editLogDock_ != nullptr)
        {
            editLogDock_->show();
            editLogDock_->raise();
        }
        UpdateEditLogPanel();
    }

    void SCDatabaseEditorMainWindow::ExportDebugPackage()
    {
        const QString filePath = QFileDialog::getSaveFileName(
            this, QStringLiteral("Export Debug Package"), QString(),
            QStringLiteral("Debug Package (*.scdbg);;All Files (*)"));
        if (filePath.isEmpty())
        {
            return;
        }

        sc::SCExportRequest request;
        request.mode = sc::SCExportMode::DebugPackage;
        request.assets = sc::BuildDefaultAssetSelection(request.mode);
        request.redaction = sc::BuildDefaultRedactionPolicy(request.mode);
        request.packageSize = sc::BuildDefaultPackageSizePolicy(request.mode);

        QString error;
        if (!session_->ExportDebugPackage(filePath, request, &error))
        {
            ShowError(QStringLiteral("Export Debug Package Failed"), error);
            return;
        }

        SetStatusMessage(QStringLiteral("Debug package exported: ") + filePath);
    }

    void SCDatabaseEditorMainWindow::RefreshOverviewPanels()
    {
        diagnosticsText_->setPlainText(session_->BuildHealthSummary());
        UpdateEditLogPanel();
    }

    void SCDatabaseEditorMainWindow::UpdateEditLogPanel()
    {
        if (editStateText_ == nullptr || editLogTree_ == nullptr)
        {
            return;
        }

        sc::SCEditingDatabaseState editingState;
        sc::SCEditLogState logState;
        QString error;
        if (!session_->GetEditingState(&editingState, &error))
        {
            editStateText_->setPlainText(
                QStringLiteral("Failed to load editing state: ") + error);
            editLogTree_->clear();
            return;
        }

        if (!session_->GetEditLogState(&logState, &error))
        {
            editStateText_->setPlainText(
                QStringLiteral("Failed to load edit log: ") + error);
            editLogTree_->clear();
            return;
        }

        QString summary;
        summary += QStringLiteral("Open: ") +
                   (editingState.open ? QStringLiteral("true")
                                      : QStringLiteral("false")) +
                   QLatin1Char('\n');
        summary += QStringLiteral("OpenMode: ") +
                   OpenModeToText(editingState.openMode) + QLatin1Char('\n');
        summary += QStringLiteral("Dirty: ") +
                   (editingState.dirty ? QStringLiteral("true")
                                       : QStringLiteral("false")) +
                   QLatin1Char('\n');
        summary += QStringLiteral("CurrentVersion: ") +
                   QString::number(
                       static_cast<qulonglong>(editingState.currentVersion)) +
                   QLatin1Char('\n');
        summary += QStringLiteral("BaselineVersion: ") +
                   QString::number(
                       static_cast<qulonglong>(editingState.baselineVersion)) +
                   QLatin1Char('\n');
        summary += QStringLiteral("UndoCount: ") +
                   QString::number(editingState.undoCount) + QLatin1Char('\n');
        summary += QStringLiteral("RedoCount: ") +
                   QString::number(editingState.redoCount) + QLatin1Char('\n');
        summary += QStringLiteral("UndoItems: ") +
                   QString::number(logState.undoItems.size()) +
                   QLatin1Char('\n');
        summary += QStringLiteral("RedoItems: ") +
                   QString::number(logState.redoItems.size()) +
                   QLatin1Char('\n');
        editStateText_->setPlainText(summary);

        editLogTree_->clear();

        auto appendItems = [this](const std::vector<sc::SCEditLogEntry>& items,
                                  const QString& side) {
            for (const sc::SCEditLogEntry& entry : items)
            {
                auto* row = new QTreeWidgetItem(editLogTree_);
                row->setText(0, side);
                row->setText(
                    1, QString::number(static_cast<qulonglong>(entry.version)));
                row->setText(2, EditLogActionKindToText(entry.kind));
                row->setText(3, QString::number(
                                    static_cast<qulonglong>(entry.commitId)));
                row->setText(4, FormatUtcTimestamp(entry.timestampUtcMs));
                row->setText(5, QString::fromStdWString(entry.displayText));
                row->setText(6, QString::fromStdWString(entry.detailText));
            }
        };

        appendItems(logState.undoItems, QStringLiteral("Undo"));
        appendItems(logState.redoItems, QStringLiteral("Redo"));
        editLogTree_->resizeColumnToContents(0);
        editLogTree_->resizeColumnToContents(1);
        editLogTree_->resizeColumnToContents(2);
        editLogTree_->resizeColumnToContents(3);
        editLogTree_->resizeColumnToContents(4);
        editLogTree_->resizeColumnToContents(5);
        editLogTree_->resizeColumnToContents(6);
    }

    void SCDatabaseEditorMainWindow::OnTableSelectionChanged()
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

    void SCDatabaseEditorMainWindow::OnGridSelectionChanged()
    {
        UpdateRecordInspector();
        UpdateGridSummary();
    }

    void SCDatabaseEditorMainWindow::OnFilterTextChanged(const QString& text)
    {
        filterModel_->setFilterRegularExpression(
            QRegularExpression(QRegularExpression::escape(text),
                               QRegularExpression::CaseInsensitiveOption));
        UpdateGridSummary();
    }

    void SCDatabaseEditorMainWindow::UpdateSchemaInspector()
    {
        schemaTree_->clear();

        QVector<sc::SCColumnDef> columns;
        QString error;
        if (!session_->BuildSchemaSnapshot(&columns, &error))
        {
            schemaTree_->addTopLevelItem(
                new QTreeWidgetItem({QStringLiteral("Error"), error}));
            return;
        }

        for (const sc::SCColumnDef& column : columns)
        {
            auto* root = new QTreeWidgetItem(
                schemaTree_,
                {ToQString(column.name), ColumnKindToText(column.columnKind)});
            root->addChild(
                new QTreeWidgetItem({QStringLiteral("Display Name"),
                                     ToQString(column.displayName)}));
            root->addChild(
                new QTreeWidgetItem({QStringLiteral("SCValue Kind"),
                                     ValueKindToText(column.valueKind)}));
            root->addChild(new QTreeWidgetItem(
                {QStringLiteral("Nullable"), column.nullable
                                                 ? QStringLiteral("true")
                                                 : QStringLiteral("false")}));
            root->addChild(new QTreeWidgetItem(
                {QStringLiteral("Editable"), column.editable
                                                 ? QStringLiteral("true")
                                                 : QStringLiteral("false")}));
            root->addChild(new QTreeWidgetItem(
                {QStringLiteral("Indexed"), column.indexed
                                                ? QStringLiteral("true")
                                                : QStringLiteral("false")}));
            root->addChild(
                new QTreeWidgetItem({QStringLiteral("Reference Table"),
                                     ToQString(column.referenceTable)}));
        }
        schemaTree_->expandAll();
    }

    void SCDatabaseEditorMainWindow::UpdateRecordInspector()
    {
        recordTree_->clear();

        const QModelIndex index = CurrentSourceIndex();
        if (!index.isValid())
        {
            return;
        }

        QVector<QPair<QString, QString>> fields;
        QString error;
        if (!session_->BuildRecordSnapshot(
                recordModel_->RecordIdAt(index.row()), &fields, &error))
        {
            recordTree_->addTopLevelItem(
                new QTreeWidgetItem({QStringLiteral("Error"), error}));
            return;
        }

        for (const auto& pair : fields)
        {
            recordTree_->addTopLevelItem(
                new QTreeWidgetItem({pair.first, pair.second}));
        }
        recordTree_->expandAll();
    }

    void SCDatabaseEditorMainWindow::UpdateComputedColumnsPanel()
    {
        const QString selectedName = CurrentComputedColumnName();
        computedColumnsTree_->clear();

        const QVector<sc::SCComputedColumnDef> columns =
            session_->CurrentSessionComputedColumns();
        for (const sc::SCComputedColumnDef& column : columns)
        {
            QString definition;
            switch (column.kind)
            {
                case sc::ComputedFieldKind::Expression:
                    definition = QStringLiteral("Expression: ") +
                                 ToQString(column.expression);
                    break;
                case sc::ComputedFieldKind::Rule:
                    definition =
                        QStringLiteral("Rule: ") + ToQString(column.ruleId);
                    break;
                case sc::ComputedFieldKind::Aggregate:
                    definition = QStringLiteral("Aggregate: ") +
                                 ToQString(column.aggregateRelation);
                    if (!column.aggregateField.empty())
                    {
                        definition += QStringLiteral(" / ") +
                                      ToQString(column.aggregateField);
                    }
                    break;
                default:
                    definition = QStringLiteral("Unknown");
                    break;
            }

            auto* root = new QTreeWidgetItem(
                computedColumnsTree_,
                {ToQString(column.displayName.empty() ? column.name
                                                      : column.displayName),
                 definition});
            root->setData(0, Qt::UserRole, ToQString(column.name));
            root->addChild(new QTreeWidgetItem(
                {QStringLiteral("Name"), ToQString(column.name)}));
            root->addChild(
                new QTreeWidgetItem({QStringLiteral("SCValue Kind"),
                                     ValueKindToText(column.valueKind)}));
            root->addChild(new QTreeWidgetItem(
                {QStringLiteral("Cacheable"), column.cacheable
                                                  ? QStringLiteral("true")
                                                  : QStringLiteral("false")}));
        }
        computedColumnsTree_->expandAll();
        SelectComputedColumnByName(selectedName);
    }

    void SCDatabaseEditorMainWindow::UpdateGridSummary()
    {
        const QString tableName = session_->CurrentTableName();
        if (tableName.isEmpty())
        {
            tableSummaryLabel_->setText(QStringLiteral("No table selected"));
            return;
        }

        const QModelIndex current = CurrentSourceIndex();
        const QString selected =
            current.isValid()
                ? QStringLiteral(" | Selected RecordId=%1")
                      .arg(recordModel_->RecordIdAt(current.row()))
                : QString();

        tableSummaryLabel_->setText(QStringLiteral("%1 | Rows %2/%3%4")
                                        .arg(tableName)
                                        .arg(filterModel_->rowCount())
                                        .arg(recordModel_->RowCountValue())
                                        .arg(selected));
    }

    QModelIndex SCDatabaseEditorMainWindow::CurrentSourceIndex() const
    {
        const QModelIndex proxyIndex = dataTable_->currentIndex();
        return proxyIndex.isValid() ? filterModel_->mapToSource(proxyIndex)
                                    : QModelIndex{};
    }

    QString SCDatabaseEditorMainWindow::CurrentComputedColumnName() const
    {
        QTreeWidgetItem* item = computedColumnsTree_->currentItem();
        if (item == nullptr)
        {
            return {};
        }

        while (item->parent() != nullptr)
        {
            item = item->parent();
        }
        return item->data(0, Qt::UserRole).toString();
    }

    void SCDatabaseEditorMainWindow::SelectComputedColumnByName(
        const QString& name)
    {
        if (name.isEmpty())
        {
            return;
        }

        for (int index = 0; index < computedColumnsTree_->topLevelItemCount();
             ++index)
        {
            QTreeWidgetItem* item = computedColumnsTree_->topLevelItem(index);
            if (item != nullptr &&
                item->data(0, Qt::UserRole)
                        .toString()
                        .compare(name, Qt::CaseInsensitive) == 0)
            {
                computedColumnsTree_->setCurrentItem(item);
                return;
            }
        }
    }

    void SCDatabaseEditorMainWindow::ShowError(const QString& title,
                                               const QString& message)
    {
        QMessageBox::critical(this, title, message);
        SetStatusMessage(title + QStringLiteral(": ") + message);
    }

    void SCDatabaseEditorMainWindow::SetStatusMessage(const QString& text)
    {
        statusLabel_->setText(text);
    }

}  // namespace StableCore::Storage::Editor
