#include "SCDatabaseEditorMainWindow.h"

#include <QAction>
#include <QAbstractItemView>
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
#include <QTabWidget>
#include <QVBoxLayout>
#include <QWidget>

#include <vector>

namespace sc = StableCore::Storage;

namespace StableCore::Storage::Editor
{
    namespace
    {
        constexpr int kInspectorSchemaTab = 0;
        constexpr int kInspectorRecordTab = 1;
        constexpr int kInspectorComputedTab = 2;
        constexpr int kInspectorRelationTab = 3;

        constexpr int kBottomDiagnosticsTab = 0;
        constexpr int kBottomEditLogTab = 1;
        constexpr int kBottomHealthSummaryTab = 2;
        constexpr int kBottomSqlPreviewTab = 3;
        constexpr int kBottomDebugPackageTab = 4;

        constexpr int kExplorerNodeTypeRole = Qt::UserRole;
        constexpr int kExplorerNodeNameRole = Qt::UserRole + 1;

        enum class ExplorerNodeType
        {
            Database,
            TablesRoot,
            Table,
            ComputedRoot,
            ComputedColumn,
            EditLog,
            Journal,
            Snapshots,
            Diagnostics,
        };

        QString ToQString(const std::wstring& text)
        {
            return QString::fromStdWString(text);
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

        QString SCValueToText(const sc::SCValue& value)
        {
            if (value.IsNull())
            {
                return QStringLiteral("-");
            }

            switch (value.GetKind())
            {
                case sc::ValueKind::Int64: {
                    std::int64_t v = 0;
                    if (value.AsInt64(&v) == sc::SC_OK)
                    {
                        return QString::number(v);
                    }
                    break;
                }
                case sc::ValueKind::Double: {
                    double v = 0.0;
                    if (value.AsDouble(&v) == sc::SC_OK)
                    {
                        return QString::number(v);
                    }
                    break;
                }
                case sc::ValueKind::Bool: {
                    bool v = false;
                    if (value.AsBool(&v) == sc::SC_OK)
                    {
                        return v ? QStringLiteral("Yes")
                                 : QStringLiteral("No");
                    }
                    break;
                }
                case sc::ValueKind::String: {
                    std::wstring v;
                    if (value.AsStringCopy(&v) == sc::SC_OK)
                    {
                        return ToQString(v);
                    }
                    break;
                }
                case sc::ValueKind::RecordId: {
                    sc::RecordId v = 0;
                    if (value.AsRecordId(&v) == sc::SC_OK)
                    {
                        return QString::number(static_cast<qulonglong>(v));
                    }
                    break;
                }
                case sc::ValueKind::Enum: {
                    std::wstring v;
                    if (value.AsEnumCopy(&v) == sc::SC_OK)
                    {
                        return ToQString(v);
                    }
                    break;
                }
                case sc::ValueKind::Null:
                default:
                    break;
            }

            return QStringLiteral("<value>");
        }

        QString BoolToText(bool value)
        {
            return value ? QStringLiteral("Yes") : QStringLiteral("No");
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

        QString ExplorerNodeTypeToText(ExplorerNodeType type)
        {
            switch (type)
            {
                case ExplorerNodeType::Database:
                    return QStringLiteral("Database");
                case ExplorerNodeType::TablesRoot:
                    return QStringLiteral("Tables");
                case ExplorerNodeType::Table:
                    return QStringLiteral("Table");
                case ExplorerNodeType::ComputedRoot:
                    return QStringLiteral("Computed Columns");
                case ExplorerNodeType::ComputedColumn:
                    return QStringLiteral("Computed Column");
                case ExplorerNodeType::EditLog:
                    return QStringLiteral("Edit Log");
                case ExplorerNodeType::Journal:
                    return QStringLiteral("Journal");
                case ExplorerNodeType::Snapshots:
                    return QStringLiteral("Snapshots");
                case ExplorerNodeType::Diagnostics:
                    return QStringLiteral("Diagnostics");
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

        sc::SCComputedColumnDef BuildComputedTemplate(
            const sc::SCColumnDef& column)
        {
            sc::SCComputedColumnDef computed;
            computed.name = column.name;
            computed.displayName = column.displayName.empty() ? column.name
                                                             : column.displayName;
            computed.valueKind = column.valueKind;
            computed.kind = sc::ComputedFieldKind::Expression;
            computed.cacheable = true;
            computed.editable = false;
            return computed;
        }

        sc::SCColumnDef BuildColumnTemplate(
            const sc::SCComputedColumnDef& column)
        {
            sc::SCColumnDef schemaColumn;
            schemaColumn.name = column.name;
            schemaColumn.displayName = column.displayName.empty()
                                           ? column.name
                                           : column.displayName;
            schemaColumn.valueKind = column.valueKind;
            schemaColumn.columnKind = sc::ColumnKind::Fact;
            schemaColumn.nullable = true;
            schemaColumn.editable = true;
            schemaColumn.userDefined = true;
            schemaColumn.indexed = false;
            schemaColumn.participatesInCalc = false;
            return schemaColumn;
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
        RefreshObjectExplorer();
        UpdateDatabaseStatusBar();
        RefreshOverviewPanels();
        UpdateGridSummary();

        connect(session_, &SCDatabaseSession::DatabaseOpened, this, [this]() {
            RefreshObjectExplorer();
            UpdateDatabaseStatusBar();
            RefreshOverviewPanels();
            UpdateGridSummary();
            SetStatusMessage(QStringLiteral("Database opened."));
        });
        connect(session_, &SCDatabaseSession::TablesChanged, this, [this]() {
            RefreshObjectExplorer();
            UpdateDatabaseStatusBar();
            RefreshOverviewPanels();
            UpdateGridSummary();
        });
        connect(session_, &SCDatabaseSession::CurrentTableChanged, this,
                [this]() {
                    RefreshObjectExplorer();
                    UpdateSchemaInspector();
                    UpdateRecordInspector();
                    UpdateComputedColumnsPanel();
                    UpdateRelationInspector();
                    UpdateDatabaseStatusBar();
                    UpdateGridSummary();
                    RefreshOverviewPanels();
                    dataTable_->resizeColumnsToContents();
                    SetStatusMessage(QStringLiteral("Table selected: ") +
                                     session_->CurrentTableName());
                });
        connect(recordModel_, &QAbstractItemModel::modelReset, this,
                &SCDatabaseEditorMainWindow::UpdateGridSummary);
        connect(dataTable_->selectionModel(),
                &QItemSelectionModel::selectionChanged, this,
                &SCDatabaseEditorMainWindow::OnGridSelectionChanged);
        connect(dataTable_->horizontalHeader(), &QHeaderView::sectionClicked,
                this, &SCDatabaseEditorMainWindow::OnGridHeaderClicked);
        connect(session_, &SCDatabaseSession::RecordsChanged, this,
                [this]() {
                    UpdateGridSummary();
                    UpdateRecordInspector();
                    UpdateRelationInspector();
                    RefreshOverviewPanels();
                });
    }

    void SCDatabaseEditorMainWindow::BuildUi()
    {
        setWindowTitle(QStringLiteral("StableCore Database Editor"));
        resize(1540, 920);

        auto* centralWidget = new QWidget(this);
        auto* centralLayout = new QVBoxLayout(centralWidget);
        centralLayout->setContentsMargins(0, 0, 0, 0);
        centralLayout->setSpacing(6);

        databaseStatusBar_ = new QWidget(centralWidget);
        auto* databaseStatusLayout = new QHBoxLayout(databaseStatusBar_);
        databaseStatusLayout->setContentsMargins(8, 6, 8, 6);
        databaseStatusLayout->setSpacing(12);

        databasePathLabel_ = new QLabel(QStringLiteral("Database: -"), databaseStatusBar_);
        openModeLabel_ = new QLabel(QStringLiteral("Mode: Closed"), databaseStatusBar_);
        currentTableLabel_ = new QLabel(QStringLiteral("Table: -"), databaseStatusBar_);
        tableStatsLabel_ = new QLabel(QStringLiteral("Records: 0"), databaseStatusBar_);
        filterStateLabel_ = new QLabel(QStringLiteral("Filter: Off"), databaseStatusBar_);
        transactionStateLabel_ =
            new QLabel(QStringLiteral("Transaction: Idle"), databaseStatusBar_);

        databaseStatusLayout->addWidget(databasePathLabel_);
        databaseStatusLayout->addWidget(openModeLabel_);
        databaseStatusLayout->addWidget(currentTableLabel_);
        databaseStatusLayout->addWidget(tableStatsLabel_);
        databaseStatusLayout->addWidget(filterStateLabel_);
        databaseStatusLayout->addWidget(transactionStateLabel_);
        databaseStatusLayout->addStretch(1);
        centralLayout->addWidget(databaseStatusBar_);

        tablePage_ = new QWidget(centralWidget);
        auto* tableLayout = new QVBoxLayout(tablePage_);
        tableLayout->setContentsMargins(0, 0, 0, 0);
        tableLayout->setSpacing(6);

        tableTitleLabel_ =
            new QLabel(QStringLiteral("No table selected"), tablePage_);
        tableTitleLabel_->setObjectName(QStringLiteral("tableTitleLabel"));
        tableTitleLabel_->setStyleSheet(
            QStringLiteral("font-size: 16px; font-weight: 600;"));
        tableLayout->addWidget(tableTitleLabel_);

        tableToolBar_ = new QToolBar(QStringLiteral("Table Tools"), tablePage_);
        tableToolBar_->setMovable(false);
        tableToolBar_->addAction(QStringLiteral("Add Record"), this,
                                &SCDatabaseEditorMainWindow::AddRecord);
        tableToolBar_->addAction(QStringLiteral("Delete Record"), this,
                                &SCDatabaseEditorMainWindow::DeleteSelectedRecord);
        tableToolBar_->addSeparator();
        tableToolBar_->addAction(QStringLiteral("Refresh"), this,
                                &SCDatabaseEditorMainWindow::RefreshCurrentView);
        tableToolBar_->addSeparator();
        tableToolBar_->addAction(QStringLiteral("Add Column"), this,
                                &SCDatabaseEditorMainWindow::AddColumn);
        tableToolBar_->addAction(QStringLiteral("Edit Column"), this,
                                &SCDatabaseEditorMainWindow::EditSelectedColumn);
        tableToolBar_->addAction(QStringLiteral("Pick Relation"), this,
                                &SCDatabaseEditorMainWindow::EditSelectedRelation);
        tableToolBar_->addSeparator();
        tableToolBar_->addAction(QStringLiteral("Add Computed"), this,
                                &SCDatabaseEditorMainWindow::AddSessionComputedColumn);
        tableToolBar_->addAction(QStringLiteral("Edit Computed"), this,
                                &SCDatabaseEditorMainWindow::EditSelectedComputedColumn);
        tableLayout->addWidget(tableToolBar_);

        auto* filterLayout = new QHBoxLayout();
        filterEdit_ = new QLineEdit(tablePage_);
        filterEdit_->setPlaceholderText(QStringLiteral("Filter current table"));
        auto* clearFilterButton = new QPushButton(QStringLiteral("Clear"), tablePage_);
        filterLayout->addWidget(new QLabel(QStringLiteral("Filter:"), tablePage_));
        filterLayout->addWidget(filterEdit_, 1);
        filterLayout->addWidget(clearFilterButton);
        tableLayout->addLayout(filterLayout);

        dataTable_ = new QTableView(tablePage_);
        dataTable_->setModel(filterModel_);
        dataTable_->horizontalHeader()->setStretchLastSection(true);
        dataTable_->horizontalHeader()->setSectionsMovable(true);
        dataTable_->setAlternatingRowColors(true);
        dataTable_->setSelectionBehavior(QAbstractItemView::SelectRows);
        dataTable_->setSelectionMode(QAbstractItemView::SingleSelection);
        dataTable_->setSortingEnabled(true);
        dataTable_->setWordWrap(false);
        tableLayout->addWidget(dataTable_, 1);

        centralLayout->addWidget(tablePage_, 1);
        setCentralWidget(centralWidget);

        connect(filterEdit_, &QLineEdit::textChanged, this,
                &SCDatabaseEditorMainWindow::OnFilterTextChanged);
        connect(clearFilterButton, &QPushButton::clicked, filterEdit_,
                &QLineEdit::clear);

        objectExplorerDock_ =
            new QDockWidget(QStringLiteral("Object Explorer"), this);
        objectExplorerDock_->setObjectName(QStringLiteral("objectExplorerDock"));
        objectExplorerDock_->setAllowedAreas(Qt::LeftDockWidgetArea |
                                            Qt::RightDockWidgetArea);
        objectTree_ = new QTreeWidget(objectExplorerDock_);
        objectTree_->setHeaderLabels(
            {QStringLiteral("Object"), QStringLiteral("Type")});
        objectTree_->setSelectionMode(QAbstractItemView::SingleSelection);
        objectTree_->setAlternatingRowColors(true);
        objectExplorerDock_->setWidget(objectTree_);
        addDockWidget(Qt::LeftDockWidgetArea, objectExplorerDock_);
        connect(objectTree_, &QTreeWidget::itemSelectionChanged, this,
                &SCDatabaseEditorMainWindow::OnTableSelectionChanged);

        inspectorDock_ = new QDockWidget(QStringLiteral("Inspector"), this);
        inspectorDock_->setObjectName(QStringLiteral("inspectorDock"));
        inspectorDock_->setAllowedAreas(Qt::RightDockWidgetArea);
        inspectorTabs_ = new QTabWidget(inspectorDock_);

        schemaTree_ = new QTreeWidget(inspectorTabs_);
        schemaTree_->setHeaderLabels(
            {QStringLiteral("Name"), QStringLiteral("Type"),
             QStringLiteral("Nullable"), QStringLiteral("Default"),
             QStringLiteral("Reference Table"), QStringLiteral("User Defined"),
             QStringLiteral("Computed")});
        inspectorTabs_->addTab(schemaTree_, QStringLiteral("Schema"));

        recordTree_ = new QTreeWidget(inspectorTabs_);
        recordTree_->setHeaderLabels(
            {QStringLiteral("Field"), QStringLiteral("Value")});
        inspectorTabs_->addTab(recordTree_, QStringLiteral("Current Record"));

        computedColumnsTree_ = new QTreeWidget(inspectorTabs_);
        computedColumnsTree_->setHeaderLabels(
            {QStringLiteral("Name"), QStringLiteral("Kind"),
             QStringLiteral("Expression"), QStringLiteral("Cacheable")});
        inspectorTabs_->addTab(computedColumnsTree_,
                               QStringLiteral("Computed"));

        relationTree_ = new QTreeWidget(inspectorTabs_);
        relationTree_->setHeaderLabels(
            {QStringLiteral("Field"), QStringLiteral("Target Table"),
             QStringLiteral("Target Field"), QStringLiteral("Status")});
        inspectorTabs_->addTab(relationTree_, QStringLiteral("Relation"));

        inspectorDock_->setWidget(inspectorTabs_);
        addDockWidget(Qt::RightDockWidgetArea, inspectorDock_);

        bottomDock_ = new QDockWidget(QStringLiteral("Bottom Panel"), this);
        bottomDock_->setObjectName(QStringLiteral("bottomDock"));
        bottomDock_->setAllowedAreas(Qt::BottomDockWidgetArea);
        bottomTabs_ = new QTabWidget(bottomDock_);

        diagnosticsText_ = new QPlainTextEdit(bottomTabs_);
        diagnosticsText_->setReadOnly(true);
        diagnosticsText_->setPlainText(QStringLiteral("No database opened."));
        bottomTabs_->addTab(diagnosticsText_, QStringLiteral("Diagnostics"));

        auto* editLogWidget = new QWidget(bottomTabs_);
        auto* editLogLayout = new QVBoxLayout(editLogWidget);
        editLogLayout->setContentsMargins(6, 6, 6, 6);
        editLogLayout->setSpacing(6);
        editStateText_ = new QPlainTextEdit(editLogWidget);
        editStateText_->setReadOnly(true);
        editStateText_->setMinimumHeight(110);
        editStateText_->setPlainText(QStringLiteral("No database opened."));
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
        bottomTabs_->addTab(editLogWidget, QStringLiteral("Edit Log"));

        healthSummaryText_ = new QPlainTextEdit(bottomTabs_);
        healthSummaryText_->setReadOnly(true);
        healthSummaryText_->setPlainText(QStringLiteral("No database opened."));
        bottomTabs_->addTab(healthSummaryText_, QStringLiteral("Health Summary"));

        sqlPreviewText_ = new QPlainTextEdit(bottomTabs_);
        sqlPreviewText_->setReadOnly(true);
        sqlPreviewText_->setPlainText(
            QStringLiteral("SQL preview is not available until a table is open."));
        bottomTabs_->addTab(sqlPreviewText_, QStringLiteral("SQL Preview"));

        debugPackageText_ = new QPlainTextEdit(bottomTabs_);
        debugPackageText_->setReadOnly(true);
        debugPackageText_->setPlainText(QStringLiteral("No debug package exported."));
        bottomTabs_->addTab(debugPackageText_, QStringLiteral("Debug Package"));

        bottomDock_->setWidget(bottomTabs_);
        addDockWidget(Qt::BottomDockWidgetArea, bottomDock_);

        statusLabel_ = new QLabel(QStringLiteral("Ready."), this);
        statusBar()->addPermanentWidget(statusLabel_, 1);
    }

    void SCDatabaseEditorMainWindow::BuildMenus()
    {
        auto* fileMenu = menuBar()->addMenu(QStringLiteral("&File"));
        fileMenu->addAction(QStringLiteral("New Database..."), this,
                            &SCDatabaseEditorMainWindow::CreateDatabase);
        fileMenu->addAction(QStringLiteral("Open Database..."), this,
                            &SCDatabaseEditorMainWindow::OpenDatabase);
        fileMenu->addAction(QStringLiteral("Create Backup Copy..."), this,
                            &SCDatabaseEditorMainWindow::CreateBackupCopy);
        fileMenu->addAction(QStringLiteral("Export Debug Package..."), this,
                            &SCDatabaseEditorMainWindow::ExportDebugPackage);

        auto* editMenu = menuBar()->addMenu(QStringLiteral("&Edit"));
        editMenu->addAction(QStringLiteral("Undo"), this,
                            &SCDatabaseEditorMainWindow::UndoLastAction);
        editMenu->addAction(QStringLiteral("Redo"), this,
                            &SCDatabaseEditorMainWindow::RedoLastAction);
        editMenu->addAction(QStringLiteral("Refresh"), this,
                            &SCDatabaseEditorMainWindow::RefreshCurrentView);

        auto* tableMenu = menuBar()->addMenu(QStringLiteral("&Table"));
        tableMenu->addAction(QStringLiteral("Create Table..."), this,
                             &SCDatabaseEditorMainWindow::CreateTable);
        tableMenu->addAction(QStringLiteral("Add Column..."), this,
                             &SCDatabaseEditorMainWindow::AddColumn);
        tableMenu->addAction(QStringLiteral("Edit Selected Column..."), this,
                             &SCDatabaseEditorMainWindow::EditSelectedColumn);
        tableMenu->addAction(QStringLiteral("Add Record"), this,
                             &SCDatabaseEditorMainWindow::AddRecord);
        tableMenu->addAction(QStringLiteral("Delete Selected Record"), this,
                             &SCDatabaseEditorMainWindow::DeleteSelectedRecord);
        tableMenu->addAction(QStringLiteral("Pick Selected Relation..."), this,
                             &SCDatabaseEditorMainWindow::EditSelectedRelation);

        auto* computedMenu =
            menuBar()->addMenu(QStringLiteral("&Computed Column"));
        computedMenu->addAction(QStringLiteral("Add Session Computed Column..."),
                                this,
                                &SCDatabaseEditorMainWindow::AddSessionComputedColumn);
        computedMenu->addAction(
            QStringLiteral("Edit Selected Computed Column..."), this,
            &SCDatabaseEditorMainWindow::EditSelectedComputedColumn);
        computedMenu->addAction(
            QStringLiteral("Convert Selected Column To Computed..."), this,
            &SCDatabaseEditorMainWindow::ConvertSelectedColumnToComputed);
        computedMenu->addAction(
            QStringLiteral("Convert Selected Computed Column To Column..."),
            this, &SCDatabaseEditorMainWindow::ConvertSelectedComputedToColumn);
        computedMenu->addAction(
            QStringLiteral("Delete Selected Computed Column"), this,
            &SCDatabaseEditorMainWindow::DeleteSelectedComputedColumn);

        auto* toolsMenu = menuBar()->addMenu(QStringLiteral("&Tools"));
        toolsMenu->addAction(QStringLiteral("Health Check"), this,
                             &SCDatabaseEditorMainWindow::ShowHealthSummary);
        toolsMenu->addAction(QStringLiteral("Show Edit Log / State Summary"),
                             this,
                             &SCDatabaseEditorMainWindow::ShowEditLogSummary);
        toolsMenu->addAction(QStringLiteral("Export Debug Package..."), this,
                             &SCDatabaseEditorMainWindow::ExportDebugPackage);

        auto* viewMenu = menuBar()->addMenu(QStringLiteral("&View"));
        viewMenu->addAction(objectExplorerDock_->toggleViewAction());
        viewMenu->addAction(inspectorDock_->toggleViewAction());
        viewMenu->addAction(bottomDock_->toggleViewAction());
        viewMenu->addSeparator();
        viewMenu->addAction(QStringLiteral("Reset Layout"), this, [this]() {
            objectExplorerDock_->show();
            inspectorDock_->show();
            bottomDock_->show();
            objectExplorerDock_->raise();
            inspectorDock_->raise();
            bottomDock_->raise();
        });

        auto* toolbar = addToolBar(QStringLiteral("Main"));
        toolbar->addAction(QStringLiteral("Open"), this,
                           &SCDatabaseEditorMainWindow::OpenDatabase);
        toolbar->addAction(QStringLiteral("New DB"), this,
                           &SCDatabaseEditorMainWindow::CreateDatabase);
        toolbar->addAction(QStringLiteral("Backup Copy"), this,
                           &SCDatabaseEditorMainWindow::CreateBackupCopy);
        toolbar->addAction(QStringLiteral("Refresh"), this,
                           &SCDatabaseEditorMainWindow::RefreshCurrentView);
        toolbar->addSeparator();
        toolbar->addAction(QStringLiteral("Undo"), this,
                           &SCDatabaseEditorMainWindow::UndoLastAction);
        toolbar->addAction(QStringLiteral("Redo"), this,
                           &SCDatabaseEditorMainWindow::RedoLastAction);
        toolbar->addSeparator();
        toolbar->addAction(QStringLiteral("Health Check"), this,
                           &SCDatabaseEditorMainWindow::ShowHealthSummary);
        toolbar->addAction(QStringLiteral("Debug Package"), this,
                           &SCDatabaseEditorMainWindow::ExportDebugPackage);
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

    void SCDatabaseEditorMainWindow::CreateBackupCopy()
    {
        if (!session_->IsOpen())
        {
            ShowError(QStringLiteral("Create Backup Copy Failed"),
                      QStringLiteral("No database is open."));
            return;
        }

        const QString defaultPath = session_->DatabasePath().isEmpty()
                                        ? QString()
                                        : session_->DatabasePath() +
                                              QStringLiteral("_backup.sqlite");
        const QString filePath = QFileDialog::getSaveFileName(
            this, QStringLiteral("Create Backup Copy"), defaultPath,
            QStringLiteral("SQLite Database (*.sqlite);;All Files (*)"));
        if (filePath.isEmpty())
        {
            return;
        }

        sc::SCBackupOptions options;
        options.overwriteExisting = true;

        sc::SCBackupResult result;
        QString error;
        if (!session_->CreateBackupCopy(filePath, options, &result, &error))
        {
            ShowError(QStringLiteral("Create Backup Copy Failed"), error);
            return;
        }

        SetStatusMessage(QStringLiteral("Backup copy created: ") + filePath);
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
        SelectSchemaColumnByName(ToQString(column.name));
        recordModel_->Refresh();
        UpdateRelationInspector();
        RefreshOverviewPanels();
        SetStatusMessage(QStringLiteral("Column added: ") +
                         ToQString(column.name));
    }

    void SCDatabaseEditorMainWindow::EditSelectedColumn()
    {
        const QString columnName = CurrentSchemaColumnName();
        if (columnName.isEmpty())
        {
            ShowError(QStringLiteral("Edit Column Failed"),
                      QStringLiteral("Select a schema field first."));
            return;
        }

        sc::SCColumnDef existing;
        QString error;
        if (!session_->GetColumnDef(columnName, &existing, &error))
        {
            ShowError(QStringLiteral("Edit Column Failed"), error);
            return;
        }

        SCAddColumnDialog dialog(existing, this);
        if (dialog.exec() != QDialog::Accepted)
        {
            return;
        }

        const sc::SCColumnDef updated = dialog.BuildColumnDef();
        if (updated.name.empty())
        {
            ShowError(QStringLiteral("Edit Column Failed"),
                      QStringLiteral("Column name is required."));
            return;
        }

        if (!session_->UpdateColumn(columnName, updated, &error))
        {
            ShowError(QStringLiteral("Edit Column Failed"), error);
            return;
        }

        UpdateSchemaInspector();
        SelectSchemaColumnByName(ToQString(updated.name));
        recordModel_->Refresh();
        UpdateRelationInspector();
        RefreshOverviewPanels();
        SetStatusMessage(QStringLiteral("Column updated: ") +
                         ToQString(updated.name));
    }

    void SCDatabaseEditorMainWindow::ConvertSelectedColumnToComputed()
    {
        const QString columnName = CurrentSchemaColumnName();
        if (columnName.isEmpty())
        {
            ShowError(QStringLiteral("Convert To Computed Failed"),
                      QStringLiteral("Select a schema field first."));
            return;
        }

        sc::SCColumnDef existing;
        QString error;
        if (!session_->GetColumnDef(columnName, &existing, &error))
        {
            ShowError(QStringLiteral("Convert To Computed Failed"), error);
            return;
        }

        SCComputedColumnDialog dialog(session_->CurrentTableName(),
                                      BuildComputedTemplate(existing), true,
                                      this);
        dialog.setWindowTitle(QStringLiteral("Convert Column To Computed"));
        if (dialog.exec() != QDialog::Accepted)
        {
            return;
        }

        sc::SCComputedColumnDef definition;
        if (!dialog.BuildDefinition(&definition, &error))
        {
            ShowError(QStringLiteral("Convert To Computed Failed"), error);
            return;
        }

        if (!session_->ConvertColumnToComputed(columnName, definition, &error))
        {
            ShowError(QStringLiteral("Convert To Computed Failed"), error);
            return;
        }

        UpdateSchemaInspector();
        UpdateComputedColumnsPanel();
        SelectComputedColumnByName(ToQString(definition.name));
        recordModel_->Refresh();
        UpdateRelationInspector();
        RefreshOverviewPanels();
        SetStatusMessage(QStringLiteral("Converted to computed column: ") +
                         ToQString(definition.name));
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
        UpdateRelationInspector();
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
        UpdateRelationInspector();
        RefreshOverviewPanels();
        SetStatusMessage(QStringLiteral("Computed column updated: ") +
                         ToQString(updated.name));
    }

    void SCDatabaseEditorMainWindow::ConvertSelectedComputedToColumn()
    {
        const QString columnName = CurrentComputedColumnName();
        if (columnName.isEmpty())
        {
            ShowError(QStringLiteral("Convert To Column Failed"),
                      QStringLiteral("Select a computed column first."));
            return;
        }

        sc::SCComputedColumnDef existing;
        QString error;
        if (!session_->GetSessionComputedColumn(columnName, &existing, &error))
        {
            ShowError(QStringLiteral("Convert To Column Failed"), error);
            return;
        }

        SCAddColumnDialog dialog(BuildColumnTemplate(existing), this);
        dialog.setWindowTitle(QStringLiteral("Convert Computed To Column"));
        if (dialog.exec() != QDialog::Accepted)
        {
            return;
        }

        const sc::SCColumnDef definition = dialog.BuildColumnDef();
        if (definition.name.empty())
        {
            ShowError(QStringLiteral("Convert To Column Failed"),
                      QStringLiteral("Column name is required."));
            return;
        }

        if (!session_->ConvertComputedToColumn(columnName, definition, &error))
        {
            ShowError(QStringLiteral("Convert To Column Failed"), error);
            return;
        }

        UpdateSchemaInspector();
        UpdateComputedColumnsPanel();
        SelectSchemaColumnByName(ToQString(definition.name));
        recordModel_->Refresh();
        UpdateRelationInspector();
        RefreshOverviewPanels();
        SetStatusMessage(QStringLiteral("Converted to column: ") +
                         ToQString(definition.name));
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
        UpdateRelationInspector();
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
        UpdateRelationInspector();
        RefreshOverviewPanels();
        UpdateGridSummary();
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
        UpdateRelationInspector();
        RefreshOverviewPanels();
        UpdateGridSummary();
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
        UpdateRelationInspector();
        RefreshOverviewPanels();
        UpdateGridSummary();
    }

    void SCDatabaseEditorMainWindow::ShowHealthSummary()
    {
        if (bottomTabs_ != nullptr)
        {
            bottomTabs_->setCurrentIndex(kBottomHealthSummaryTab);
        }
        RefreshOverviewPanels();
    }

    void SCDatabaseEditorMainWindow::ShowEditLogSummary()
    {
        if (bottomTabs_ != nullptr)
        {
            bottomTabs_->setCurrentIndex(kBottomEditLogTab);
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

        if (bottomTabs_ != nullptr)
        {
            bottomTabs_->setCurrentIndex(kBottomDebugPackageTab);
        }
        if (debugPackageText_ != nullptr)
        {
            debugPackageText_->setPlainText(
                QStringLiteral("Exported debug package:\n") + filePath);
        }
        SetStatusMessage(QStringLiteral("Debug package exported: ") + filePath);
    }

    void SCDatabaseEditorMainWindow::RefreshOverviewPanels()
    {
        diagnosticsText_->setPlainText(session_->BuildHealthSummary());
        if (healthSummaryText_ != nullptr)
        {
            healthSummaryText_->setPlainText(session_->BuildHealthSummary());
        }
        if (sqlPreviewText_ != nullptr)
        {
            sqlPreviewText_->setPlainText(
                QStringLiteral("SQL preview is not populated in this phase.\n"
                               "Use schema and record actions to inspect "
                               "changes."));
        }
        UpdateEditLogPanel();
        UpdateDatabaseStatusBar();
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
        if (objectTree_ == nullptr)
        {
            return;
        }

        QTreeWidgetItem* item = objectTree_->currentItem();
        if (item == nullptr)
        {
            return;
        }

        const ExplorerNodeType nodeType = static_cast<ExplorerNodeType>(
            item->data(0, kExplorerNodeTypeRole).toInt());
        const QString nodeName = item->data(0, kExplorerNodeNameRole).toString();

        switch (nodeType)
        {
            case ExplorerNodeType::Table:
                if (nodeName.compare(session_->CurrentTableName(),
                                    Qt::CaseInsensitive) != 0)
                {
                    QString error;
                    if (!session_->SelectTable(nodeName, &error))
                    {
                        ShowError(QStringLiteral("Select Table Failed"),
                                  error);
                        return;
                    }
                }
                if (inspectorTabs_ != nullptr)
                {
                    inspectorTabs_->setCurrentIndex(kInspectorSchemaTab);
                }
                break;
            case ExplorerNodeType::ComputedColumn:
                if (inspectorTabs_ != nullptr)
                {
                    inspectorTabs_->setCurrentIndex(kInspectorComputedTab);
                }
                SelectComputedColumnByName(nodeName);
                break;
            case ExplorerNodeType::Diagnostics:
                if (bottomTabs_ != nullptr)
                {
                    bottomTabs_->setCurrentIndex(kBottomDiagnosticsTab);
                }
                break;
            case ExplorerNodeType::EditLog:
            case ExplorerNodeType::Journal:
                if (bottomTabs_ != nullptr)
                {
                    bottomTabs_->setCurrentIndex(kBottomEditLogTab);
                }
                break;
            case ExplorerNodeType::Snapshots:
                if (bottomTabs_ != nullptr)
                {
                    bottomTabs_->setCurrentIndex(kBottomHealthSummaryTab);
                }
                break;
            case ExplorerNodeType::ComputedRoot:
                if (inspectorTabs_ != nullptr)
                {
                    inspectorTabs_->setCurrentIndex(kInspectorComputedTab);
                }
                break;
            case ExplorerNodeType::TablesRoot:
                if (inspectorTabs_ != nullptr)
                {
                    inspectorTabs_->setCurrentIndex(kInspectorSchemaTab);
                }
                break;
            case ExplorerNodeType::Database:
            default:
                break;
        }

        UpdateDatabaseStatusBar();
        UpdateGridSummary();
    }

    void SCDatabaseEditorMainWindow::OnGridSelectionChanged()
    {
        UpdateRecordInspector();
        UpdateRelationInspector();
        UpdateGridSummary();
    }

    void SCDatabaseEditorMainWindow::OnGridHeaderClicked(int logicalIndex)
    {
        const sc::SCTableViewColumnDef column = recordModel_->ColumnAt(logicalIndex);
        if (column.name.empty())
        {
            return;
        }

        if (inspectorTabs_ != nullptr)
        {
            inspectorTabs_->setCurrentIndex(kInspectorSchemaTab);
        }
        SelectSchemaColumnByName(ToQString(column.name));
        SetStatusMessage(QStringLiteral("Schema field selected: ") +
                         ToQString(column.name));
    }

    void SCDatabaseEditorMainWindow::OnFilterTextChanged(const QString& text)
    {
        filterModel_->setFilterRegularExpression(
            QRegularExpression(QRegularExpression::escape(text),
                               QRegularExpression::CaseInsensitiveOption));
        UpdateDatabaseStatusBar();
        UpdateGridSummary();
    }

    void SCDatabaseEditorMainWindow::UpdateSchemaInspector()
    {
        const QString selectedName = CurrentSchemaColumnName();
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
            auto* row = new QTreeWidgetItem(schemaTree_);
            row->setText(0, ToQString(column.displayName.empty()
                                          ? column.name
                                          : column.displayName));
            row->setText(1, ValueKindToText(column.valueKind));
            row->setText(2, BoolToText(column.nullable));
            row->setText(3, SCValueToText(column.defaultValue));
            row->setText(4, ToQString(column.referenceTable));
            row->setText(5, BoolToText(column.userDefined));
            row->setText(6, BoolToText(column.participatesInCalc));
            row->setData(0, Qt::UserRole, ToQString(column.name));
        }
        schemaTree_->resizeColumnToContents(0);
        schemaTree_->resizeColumnToContents(1);
        schemaTree_->resizeColumnToContents(2);
        schemaTree_->resizeColumnToContents(3);
        schemaTree_->resizeColumnToContents(4);
        schemaTree_->resizeColumnToContents(5);
        schemaTree_->resizeColumnToContents(6);
        SelectSchemaColumnByName(selectedName);
    }

    void SCDatabaseEditorMainWindow::UpdateRecordInspector()
    {
        if (recordTree_ == nullptr)
        {
            return;
        }

        const QString selectedField =
            recordTree_->currentItem() != nullptr
                ? recordTree_->currentItem()->text(0)
                : QString();
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
            auto* row = new QTreeWidgetItem(recordTree_);
            row->setText(0, pair.first);
            row->setText(1, pair.second);
        }
        recordTree_->resizeColumnToContents(0);
        recordTree_->resizeColumnToContents(1);
        if (!selectedField.isEmpty())
        {
            for (int index = 0; index < recordTree_->topLevelItemCount(); ++index)
            {
                QTreeWidgetItem* item = recordTree_->topLevelItem(index);
                if (item != nullptr &&
                    item->text(0).compare(selectedField, Qt::CaseInsensitive) ==
                        0)
                {
                    recordTree_->setCurrentItem(item);
                    break;
                }
            }
        }
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
                    definition = ToQString(column.expression);
                    break;
                case sc::ComputedFieldKind::Rule:
                    definition = QStringLiteral("Rule: ") +
                                 ToQString(column.ruleId);
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
                    definition = QStringLiteral("-");
                    break;
            }

            auto* row = new QTreeWidgetItem(computedColumnsTree_);
            row->setText(0, ToQString(column.displayName.empty()
                                          ? column.name
                                          : column.displayName));
            row->setText(1, ValueKindToText(column.valueKind));
            row->setText(2, definition);
            row->setText(3, BoolToText(column.cacheable));
            row->setData(0, Qt::UserRole, ToQString(column.name));
        }
        computedColumnsTree_->resizeColumnToContents(0);
        computedColumnsTree_->resizeColumnToContents(1);
        computedColumnsTree_->resizeColumnToContents(2);
        computedColumnsTree_->resizeColumnToContents(3);
        SelectComputedColumnByName(selectedName);
    }

    void SCDatabaseEditorMainWindow::UpdateRelationInspector()
    {
        if (relationTree_ == nullptr)
        {
            return;
        }

        const QString selectedName =
            relationTree_->currentItem() != nullptr
                ? relationTree_->currentItem()->data(0, Qt::UserRole).toString()
                : QString();
        relationTree_->clear();

        QVector<sc::SCColumnDef> columns;
        QString error;
        if (!session_->BuildSchemaSnapshot(&columns, &error))
        {
            relationTree_->addTopLevelItem(
                new QTreeWidgetItem({QStringLiteral("Error"), error}));
            return;
        }

        for (const sc::SCColumnDef& column : columns)
        {
            if (column.columnKind != sc::ColumnKind::Relation)
            {
                continue;
            }

            auto* row = new QTreeWidgetItem(relationTree_);
            row->setText(0, ToQString(column.displayName.empty()
                                          ? column.name
                                          : column.displayName));
            row->setText(1, ToQString(column.referenceTable));
            row->setText(2, QStringLiteral("-"));
            row->setText(3, column.referenceTable.empty()
                                 ? QStringLiteral("Unbound")
                                 : QStringLiteral("OK"));
            row->setData(0, Qt::UserRole, ToQString(column.name));
        }
        relationTree_->resizeColumnToContents(0);
        relationTree_->resizeColumnToContents(1);
        relationTree_->resizeColumnToContents(2);
        relationTree_->resizeColumnToContents(3);
        if (!selectedName.isEmpty())
        {
            for (int index = 0; index < relationTree_->topLevelItemCount(); ++index)
            {
                QTreeWidgetItem* item = relationTree_->topLevelItem(index);
                if (item != nullptr &&
                    item->data(0, Qt::UserRole)
                            .toString()
                            .compare(selectedName, Qt::CaseInsensitive) == 0)
                {
                    relationTree_->setCurrentItem(item);
                    break;
                }
            }
        }
    }

    void SCDatabaseEditorMainWindow::UpdateGridSummary()
    {
        const QString tableName = session_->CurrentTableName();
        if (tableName.isEmpty())
        {
            if (tableTitleLabel_ != nullptr)
            {
                tableTitleLabel_->setText(QStringLiteral("No table selected"));
            }
            if (tableStatsLabel_ != nullptr)
            {
                tableStatsLabel_->setText(QStringLiteral("Records: 0 | Fields: 0"));
            }
            UpdateDatabaseStatusBar();
            return;
        }

        const QModelIndex current = CurrentSourceIndex();
        const sc::RecordId selectedRecordId =
            current.isValid() ? recordModel_->RecordIdAt(current.row()) : 0;

        if (tableTitleLabel_ != nullptr)
        {
            tableTitleLabel_->setText(QStringLiteral("Current table: %1")
                                          .arg(tableName));
        }
        if (tableStatsLabel_ != nullptr)
        {
            tableStatsLabel_->setText(
                QStringLiteral("Records: %1 / %2 | Fields: %3%4")
                    .arg(filterModel_->rowCount())
                    .arg(recordModel_->RowCountValue())
                    .arg(recordModel_->columnCount())
                    .arg(selectedRecordId != 0
                             ? QStringLiteral(" | Selected: %1")
                                   .arg(static_cast<qulonglong>(selectedRecordId))
                             : QString()));
        }
        UpdateDatabaseStatusBar();
    }

    void SCDatabaseEditorMainWindow::RefreshObjectExplorer()
    {
        if (objectTree_ == nullptr)
        {
            return;
        }

        const QSignalBlocker blocker(objectTree_);
        objectTree_->clear();
        const QStringList tableNames = session_->TableNames();
        const QVector<sc::SCComputedColumnDef> computedColumns =
            session_->CurrentSessionComputedColumns();

        auto* databaseRoot = new QTreeWidgetItem(objectTree_);
        databaseRoot->setText(0, QStringLiteral("Database"));
        databaseRoot->setText(1,
                             session_->IsOpen() ? QStringLiteral("Open")
                                                : QStringLiteral("Closed"));
        databaseRoot->setData(0, kExplorerNodeTypeRole,
                              static_cast<int>(ExplorerNodeType::Database));
        databaseRoot->setData(0, kExplorerNodeNameRole, QStringLiteral("Database"));

        auto* tablesRoot = new QTreeWidgetItem(databaseRoot);
        tablesRoot->setText(0, QStringLiteral("Tables"));
        tablesRoot->setText(1, QString::number(tableNames.size()));
        tablesRoot->setData(0, kExplorerNodeTypeRole,
                            static_cast<int>(ExplorerNodeType::TablesRoot));
        tablesRoot->setData(0, kExplorerNodeNameRole, QStringLiteral("Tables"));

        QTreeWidgetItem* selectedItem = databaseRoot;
        const QString currentTable = session_->CurrentTableName();
        for (const QString& tableName : tableNames)
        {
            auto* tableItem = new QTreeWidgetItem(tablesRoot);
            tableItem->setText(0, tableName);
            tableItem->setText(1, QStringLiteral("Table"));
            tableItem->setData(0, kExplorerNodeTypeRole,
                               static_cast<int>(ExplorerNodeType::Table));
            tableItem->setData(0, kExplorerNodeNameRole, tableName);
            if (!currentTable.isEmpty() &&
                tableName.compare(currentTable, Qt::CaseInsensitive) == 0)
            {
                selectedItem = tableItem;
            }
        }

        auto* computedRoot = new QTreeWidgetItem(databaseRoot);
        computedRoot->setText(0, QStringLiteral("Computed Columns"));
        computedRoot->setText(1, QString::number(computedColumns.size()));
        computedRoot->setData(
            0, kExplorerNodeTypeRole,
            static_cast<int>(ExplorerNodeType::ComputedRoot));
        computedRoot->setData(0, kExplorerNodeNameRole,
                              QStringLiteral("Computed Columns"));

        for (const sc::SCComputedColumnDef& column : computedColumns)
        {
            auto* computedItem = new QTreeWidgetItem(computedRoot);
            computedItem->setText(0, ToQString(column.displayName.empty()
                                                   ? column.name
                                                   : column.displayName));
            computedItem->setText(1, QStringLiteral("Computed"));
            computedItem->setData(
                0, kExplorerNodeTypeRole,
                static_cast<int>(ExplorerNodeType::ComputedColumn));
            computedItem->setData(0, kExplorerNodeNameRole,
                                  ToQString(column.name));
        }

        auto* systemRoot = new QTreeWidgetItem(databaseRoot);
        systemRoot->setText(0, QStringLiteral("System"));
        systemRoot->setText(1, QStringLiteral("Navigation"));
        systemRoot->setData(0, kExplorerNodeTypeRole,
                            static_cast<int>(ExplorerNodeType::Database));
        systemRoot->setData(0, kExplorerNodeNameRole, QStringLiteral("System"));

        const auto addSystemNode =
            [&](const QString& text, ExplorerNodeType type) {
                auto* item = new QTreeWidgetItem(systemRoot);
                item->setText(0, text);
                item->setText(1, ExplorerNodeTypeToText(type));
                item->setData(0, kExplorerNodeTypeRole,
                              static_cast<int>(type));
                item->setData(0, kExplorerNodeNameRole, text);
                return item;
            };

        addSystemNode(QStringLiteral("Edit Log"), ExplorerNodeType::EditLog);
        addSystemNode(QStringLiteral("Journal"), ExplorerNodeType::Journal);
        addSystemNode(QStringLiteral("Snapshots"), ExplorerNodeType::Snapshots);
        addSystemNode(QStringLiteral("Diagnostics"),
                      ExplorerNodeType::Diagnostics);

        databaseRoot->setExpanded(true);
        tablesRoot->setExpanded(true);
        computedRoot->setExpanded(true);
        systemRoot->setExpanded(true);
        objectTree_->resizeColumnToContents(0);
        objectTree_->resizeColumnToContents(1);
        objectTree_->setCurrentItem(selectedItem);
    }

    void SCDatabaseEditorMainWindow::UpdateDatabaseStatusBar()
    {
        sc::SCEditingDatabaseState editingState;
        QString error;
        const bool stateLoaded = session_->GetEditingState(&editingState, &error);

        if (databasePathLabel_ != nullptr)
        {
            databasePathLabel_->setText(QStringLiteral("Database: %1")
                                            .arg(session_->DatabasePath().isEmpty()
                                                     ? QStringLiteral("-")
                                                     : session_->DatabasePath()));
        }

        if (openModeLabel_ != nullptr)
        {
            openModeLabel_->setText(
                QStringLiteral("Mode: %1")
                    .arg(stateLoaded ? OpenModeToText(editingState.openMode)
                                     : QStringLiteral("Closed")));
        }

        if (currentTableLabel_ != nullptr)
        {
            currentTableLabel_->setText(QStringLiteral("Table: %1")
                                            .arg(session_->CurrentTableName().isEmpty()
                                                     ? QStringLiteral("-")
                                                     : session_->CurrentTableName()));
        }

        if (tableStatsLabel_ != nullptr)
        {
            tableStatsLabel_->setText(
                QStringLiteral("Records: %1 / %2 | Fields: %3")
                    .arg(filterModel_->rowCount())
                    .arg(recordModel_->RowCountValue())
                    .arg(recordModel_->columnCount()));
        }

        if (filterStateLabel_ != nullptr)
        {
            const QString filterText = filterEdit_ != nullptr ? filterEdit_->text()
                                                             : QString();
            filterStateLabel_->setText(
                filterText.isEmpty()
                    ? QStringLiteral("Filter: Off")
                    : QStringLiteral("Filter: %1").arg(filterText));
        }

        if (transactionStateLabel_ != nullptr)
        {
            QString transactionState = QStringLiteral("Transaction: Closed");
            if (stateLoaded)
            {
                transactionState = QStringLiteral("Transaction: ");
                if (editingState.openMode == sc::SCDatabaseOpenMode::ReadOnly)
                {
                    transactionState += QStringLiteral("ReadOnly");
                }
                else if (editingState.dirty)
                {
                    transactionState += QStringLiteral("Dirty");
                }
                else
                {
                    transactionState += QStringLiteral("Idle");
                }
            }
            transactionStateLabel_->setText(transactionState);
        }
    }

    QModelIndex SCDatabaseEditorMainWindow::CurrentSourceIndex() const
    {
        const QModelIndex proxyIndex = dataTable_->currentIndex();
        return proxyIndex.isValid() ? filterModel_->mapToSource(proxyIndex)
                                    : QModelIndex{};
    }

    QString SCDatabaseEditorMainWindow::CurrentSchemaColumnName() const
    {
        QTreeWidgetItem* item = schemaTree_->currentItem();
        if (item == nullptr)
        {
            return {};
        }
        return item->data(0, Qt::UserRole).toString();
    }

    QString SCDatabaseEditorMainWindow::CurrentComputedColumnName() const
    {
        QTreeWidgetItem* item = computedColumnsTree_->currentItem();
        if (item == nullptr)
        {
            return {};
        }
        return item->data(0, Qt::UserRole).toString();
    }

    void SCDatabaseEditorMainWindow::SelectSchemaColumnByName(
        const QString& name)
    {
        if (name.isEmpty())
        {
            return;
        }

        for (int index = 0; index < schemaTree_->topLevelItemCount(); ++index)
        {
            QTreeWidgetItem* item = schemaTree_->topLevelItem(index);
            if (item != nullptr &&
                item->data(0, Qt::UserRole)
                        .toString()
                        .compare(name, Qt::CaseInsensitive) == 0)
            {
                schemaTree_->setCurrentItem(item);
                return;
            }
        }
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
        if (statusLabel_ != nullptr)
        {
            statusLabel_->setText(text);
        }
        statusBar()->showMessage(text, 5000);
    }

}  // namespace StableCore::Storage::Editor
