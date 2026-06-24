#include "SCDatabaseEditorMainWindow.h"

#include <cstdint>
#include <QAction>
#include <QByteArray>
#include <QDateTime>
#include <QFile>
#include <QFileDialog>
#include <QFrame>
#include <QIODevice>
#include <QHBoxLayout>
#include <QInputDialog>
#include <QMenu>
#include <QMenuBar>
#include <QMessageBox>
#include <QPushButton>
#include <QSaveFile>
#include <QRegularExpression>
#include <QSignalBlocker>
#include <QSplitter>
#include <QStringList>
#include <QStatusBar>
#include <QTabBar>
#include <QToolBar>
#include <QTabWidget>
#include <QVector>
#include <QVBoxLayout>
#include <QWidget>

#include <SCTreeGrid/SCTreeGridCtrl.h>

#include <functional>
#include <vector>

#include "SCBatch.h"
#include "SCBinaryUtils.h"
#include "SCSchemaTextPane.h"
#include "SCSchemaTableImportDialog.h"
#include "SCSchemaTableDialog.h"
#include "SCTableDesignPane.h"

namespace sc = StableCore::Storage;

namespace StableCore::Storage::Editor
{
    namespace
    {
        constexpr int kCurrentDetailsRecordTab = 0;
        constexpr int kCurrentDetailsComputedTab = 1;
        constexpr int kCurrentDetailsRelationTab = 2;

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
            TableColumns,
            TableConstraints,
            TableIndexes,
            TableRecords,
            ComputedRoot,
            ComputedColumn,
            ActivityRoot,
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
                case sc::ValueKind::Binary:
                    return QStringLiteral("Binary");
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
                        return v ? QStringLiteral("Yes") : QStringLiteral("No");
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
                case sc::ValueKind::Binary: {
                    std::vector<std::uint8_t> v;
                    if (value.AsBinaryCopy(&v) == sc::SC_OK)
                    {
                        return BinaryToHex(v);
                    }
                    break;
                }
                case sc::ValueKind::Null:
                default:
                    break;
            }

            return QStringLiteral("<Value>");
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
                    return QStringLiteral("No History");
                case sc::SCDatabaseOpenMode::ReadOnly:
                    return QStringLiteral("Read Only");
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
                    return QStringLiteral("Computed Column");
                case ExplorerNodeType::ComputedColumn:
                    return QStringLiteral("Computed Column");
                case ExplorerNodeType::ActivityRoot:
                    return QStringLiteral("Activity");
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
                    return QStringLiteral("Rule Writeback");
                case sc::SCEditLogActionKind::SaveBaseline:
                    return QStringLiteral("Save Baseline");
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
            computed.displayName =
                column.displayName.empty() ? column.name : column.displayName;
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
            schemaColumn.displayName =
                column.displayName.empty() ? column.name : column.displayName;
            schemaColumn.valueKind = column.valueKind;
            schemaColumn.columnKind = sc::ColumnKind::Fact;
            schemaColumn.nullable = true;
            schemaColumn.editable = true;
            schemaColumn.userDefined = true;
            schemaColumn.indexed = false;
            schemaColumn.participatesInCalc = false;
            return schemaColumn;
        }

        QString StorageErrorText(sc::ErrorCode error)
        {
            return QStringLiteral("Storage error: 0x") +
                   QString::number(static_cast<qulonglong>(error), 16);
        }

        QString EscapeCsvField(const QString& value)
        {
            const bool needsQuotes =
                value.contains(QLatin1Char(',')) ||
                value.contains(QLatin1Char('"')) ||
                value.contains(QLatin1Char('\n')) ||
                value.contains(QLatin1Char('\r')) ||
                (!value.isEmpty() &&
                 (value.front().isSpace() || value.back().isSpace()));

            QString escaped = value;
            escaped.replace(QLatin1Char('"'), QStringLiteral("\"\""));
            if (needsQuotes)
            {
                return QStringLiteral("\"") + escaped + QStringLiteral("\"");
            }
            return escaped;
        }

        bool ParseCsvText(const QString& input, QVector<QStringList>* outRows,
                          QString* outError)
        {
            if (outRows == nullptr)
            {
                if (outError != nullptr)
                {
                    *outError =
                        QStringLiteral("The CSV row output container is empty.");
                }
                return false;
            }

            outRows->clear();

            QString text = input;
            if (!text.isEmpty() && text.front() == QChar(0xFEFF))
            {
                text.remove(0, 1);
            }

            QString currentField;
            QStringList currentRow;
            bool inQuotes = false;
            bool lastWasRowDelimiter = false;
            bool afterQuotedField = false;

            const auto finishField = [&]() {
                currentRow.push_back(currentField);
                currentField.clear();
            };

            const auto finishRow = [&]() {
                finishField();
                outRows->push_back(currentRow);
                currentRow.clear();
            };

            for (qsizetype index = 0; index < text.size(); ++index)
            {
                const QChar ch = text[index];

                if (inQuotes)
                {
                    if (ch == QLatin1Char('"'))
                    {
                        if (index + 1 < text.size() &&
                            text[index + 1] == QLatin1Char('"'))
                        {
                            currentField.push_back(QLatin1Char('"'));
                            ++index;
                        } else
                        {
                            inQuotes = false;
                            afterQuotedField = true;
                        }
                    } else
                    {
                        currentField.push_back(ch);
                    }
                    lastWasRowDelimiter = false;
                    continue;
                }

                if (ch == QLatin1Char('"'))
                {
                    if (afterQuotedField)
                    {
                        if (outError != nullptr)
                        {
                            *outError = QStringLiteral(
                                "Unexpected character after quoted CSV field.");
                        }
                        return false;
                    }
                    if (!currentField.isEmpty())
                    {
                        if (outError != nullptr)
                        {
                            *outError = QStringLiteral(
                                "Unexpected quote inside unquoted CSV field.");
                        }
                        return false;
                    }
                    inQuotes = true;
                    afterQuotedField = false;
                    lastWasRowDelimiter = false;
                    continue;
                }

                if (ch == QLatin1Char(','))
                {
                    if (afterQuotedField)
                    {
                        afterQuotedField = false;
                    }
                    finishField();
                    lastWasRowDelimiter = false;
                    continue;
                }

                if (ch == QLatin1Char('\r'))
                {
                    if (afterQuotedField)
                    {
                        afterQuotedField = false;
                    }
                    if (index + 1 < text.size() &&
                        text[index + 1] == QLatin1Char('\n'))
                    {
                        ++index;
                    }
                    finishRow();
                    lastWasRowDelimiter = true;
                    continue;
                }

                if (ch == QLatin1Char('\n'))
                {
                    if (afterQuotedField)
                    {
                        afterQuotedField = false;
                    }
                    finishRow();
                    lastWasRowDelimiter = true;
                    continue;
                }

                if (afterQuotedField)
                {
                    if (outError != nullptr)
                    {
                        *outError = QStringLiteral(
                            "Unexpected character after a quoted CSV field.");
                    }
                    return false;
                }

                currentField.push_back(ch);
                lastWasRowDelimiter = false;
            }

            if (inQuotes)
            {
                if (outError != nullptr)
                {
                    *outError = QStringLiteral(
                        "CSV input ended inside a quoted field.");
                }
                return false;
            }

            if (!text.isEmpty() &&
                (!lastWasRowDelimiter || !currentField.isEmpty() ||
                 !currentRow.isEmpty()))
            {
                finishRow();
            }

            return true;
        }

        bool ParseCsvInteger(const QString& text, std::int64_t* outValue,
                             QString* outError)
        {
            bool ok = false;
            const std::int64_t value = text.trimmed().toLongLong(&ok);
            if (!ok)
            {
                if (outError != nullptr)
                {
                    *outError =
                        QStringLiteral("Invalid integer value: ") + text;
                }
                return false;
            }

            if (outValue != nullptr)
            {
                *outValue = value;
            }
            return true;
        }

        bool ParseCsvDouble(const QString& text, double* outValue,
                            QString* outError)
        {
            bool ok = false;
            const double value = text.trimmed().toDouble(&ok);
            if (!ok)
            {
                if (outError != nullptr)
                {
                    *outError =
                        QStringLiteral("Invalid decimal value: ") + text;
                }
                return false;
            }

            if (outValue != nullptr)
            {
                *outValue = value;
            }
            return true;
        }

        bool ParseCsvBool(const QString& text, bool* outValue,
                          QString* outError)
        {
            const QString normalized = text.trimmed().toLower();
            if (normalized == QStringLiteral("1") ||
                normalized == QStringLiteral("true") ||
                normalized == QStringLiteral("yes") ||
                normalized == QStringLiteral("y") ||
                normalized == QStringLiteral("on"))
            {
                if (outValue != nullptr)
                {
                    *outValue = true;
                }
                return true;
            }

            if (normalized == QStringLiteral("0") ||
                normalized == QStringLiteral("false") ||
                normalized == QStringLiteral("no") ||
                normalized == QStringLiteral("n") ||
                normalized == QStringLiteral("off"))
            {
                if (outValue != nullptr)
                {
                    *outValue = false;
                }
                return true;
            }

            if (outError != nullptr)
            {
                *outError = QStringLiteral("Invalid boolean value: ") + text;
            }
            return false;
        }

        bool CsvTextToValue(const sc::SCTableViewColumnDef& column,
                            const QString& text, sc::SCValue* outValue,
                            QString* outError)
        {
            if (outValue == nullptr)
            {
                if (outError != nullptr)
                {
                    *outError = QStringLiteral("Output value is empty.");
                }
                return false;
            }

            if (text.isEmpty())
            {
                switch (column.valueKind)
                {
                    case sc::ValueKind::String:
                        *outValue = sc::SCValue::FromString(std::wstring());
                        return true;
                    case sc::ValueKind::Enum:
                        *outValue = sc::SCValue::FromEnum(std::wstring());
                        return true;
                    case sc::ValueKind::Null:
                    default:
                        *outValue = sc::SCValue::Null();
                        return true;
                }
            }

            switch (column.valueKind)
            {
                case sc::ValueKind::Int64: {
                    std::int64_t value = 0;
                    if (!ParseCsvInteger(text, &value, outError))
                    {
                        return false;
                    }
                    *outValue = sc::SCValue::FromInt64(value);
                    return true;
                }
                case sc::ValueKind::Double: {
                    double value = 0.0;
                    if (!ParseCsvDouble(text, &value, outError))
                    {
                        return false;
                    }
                    *outValue = sc::SCValue::FromDouble(value);
                    return true;
                }
                case sc::ValueKind::Bool: {
                    bool value = false;
                    if (!ParseCsvBool(text, &value, outError))
                    {
                        return false;
                    }
                    *outValue = sc::SCValue::FromBool(value);
                    return true;
                }
                case sc::ValueKind::String:
                    *outValue = sc::SCValue::FromString(text.toStdWString());
                    return true;
                case sc::ValueKind::RecordId: {
                    std::int64_t value = 0;
                    if (!ParseCsvInteger(text, &value, outError))
                    {
                        return false;
                    }
                    if (value < 0)
                    {
                        if (outError != nullptr)
                        {
                            *outError = QStringLiteral(
                                            "RecordId cannot be negative: ") +
                                        text;
                        }
                        return false;
                    }
                    *outValue = sc::SCValue::FromRecordId(
                        static_cast<sc::RecordId>(value));
                    return true;
                }
                case sc::ValueKind::Enum:
                    *outValue = sc::SCValue::FromEnum(text.toStdWString());
                    return true;
                case sc::ValueKind::Binary: {
                    std::vector<std::uint8_t> bytes;
                    if (!ParseBinaryHex(text, &bytes, outError))
                    {
                        return false;
                    }
                    *outValue = sc::SCValue::FromBinary(std::move(bytes));
                    return true;
                }
                case sc::ValueKind::Null:
                default:
                    if (outError != nullptr)
                    {
                        *outError = QStringLiteral(
                            "CSV import does not support this column type.");
                    }
                    return false;
            }
        }

        int FindColumnIndex(const QVector<sc::SCTableViewColumnDef>& columns,
                            const QString& name)
        {
            for (int index = 0; index < columns.size(); ++index)
            {
                if (QString::fromStdWString(columns[index].name)
                        .compare(name, Qt::CaseInsensitive) == 0)
                {
                    return index;
                }
            }
            return -1;
        }

        bool IsBlankAllowedForRequiredColumn(sc::ValueKind kind) noexcept
        {
            switch (kind)
            {
                case sc::ValueKind::String:
                case sc::ValueKind::Enum:
                    return true;
                default:
                    return false;
            }
        }

    }  // namespace

    SCDatabaseEditorMainWindow::SCDatabaseEditorMainWindow(QWidget* parent)
        : QMainWindow(parent),
          session_(new SCDatabaseSession(this))
    {
        BuildUi();
        BuildMenus();
        WireGridCallbacks();
        RefreshObjectExplorer();
        UpdateDatabaseStatusBar();
        RefreshOverviewPanels();
        UpdateGridSummary();
        SetWorkspaceMode(WorkspaceMode::Data);

        connect(session_, &SCDatabaseSession::DatabaseOpened, this, [this]() {
            RefreshObjectExplorer();
            UpdateDatabaseStatusBar();
            RefreshOverviewPanels();
            UpdateGridSummary();
            RefreshWorkspaceHeader();
            RefreshWorkspacePages();
            SetStatusMessage(QStringLiteral("Database opened."));
            const QString dbPath = session_->DatabasePath();
            if (!dbPath.isEmpty())
            {
                setWindowTitle(QStringLiteral("Database Editor - %1").arg(dbPath));
            }
        });
        connect(session_, &SCDatabaseSession::TablesChanged, this, [this]() {
            RefreshObjectExplorer();
            UpdateDatabaseStatusBar();
            RefreshOverviewPanels();
            UpdateGridSummary();
            RefreshWorkspaceHeader();
            RefreshWorkspacePages();
        });
        connect(
            session_, &SCDatabaseSession::CurrentTableChanged, this, [this]() {
                RefreshGridData();
                RefreshObjectExplorer();
                RefreshCurrentDetailsPanel();
                RefreshCurrentRecordPanel();
                UpdateComputedColumnsPanel();
                RefreshRelationPanel();
                UpdateDatabaseStatusBar();
                UpdateGridSummary();
                RefreshOverviewPanels();
                RefreshWorkspaceHeader();
                RefreshWorkspacePages();
                const QString currentTableName = session_->CurrentTableName();
                SetStatusMessage(
                    currentTableName.isEmpty()
                        ? QStringLiteral("Table selection cleared.")
                        : QStringLiteral("Selected table: ") +
                              currentTableName);
            });
        connect(session_, &SCDatabaseSession::RecordsChanged, this, [this]() {
            UpdateGridSummary();
            RefreshCurrentRecordPanel();
            RefreshRelationPanel();
            RefreshOverviewPanels();
            UpdateDatabaseStatusBar();
            RefreshWorkspaceHeader();
            RefreshWorkspacePages();
        });
    }

    void SCDatabaseEditorMainWindow::BuildUi()
    {
        setWindowTitle(QStringLiteral("Database Editor"));
        resize(1600, 960);

        auto* centralWidget = new QWidget(this);
        auto* centralLayout = new QVBoxLayout(centralWidget);
        centralLayout->setContentsMargins(0, 0, 0, 0);
        centralLayout->setSpacing(6);

        workspaceHeader_ = new QWidget(centralWidget);
        auto* workspaceHeaderLayout = new QHBoxLayout(workspaceHeader_);
        workspaceHeaderLayout->setContentsMargins(6, 6, 6, 0);
        workspaceHeaderLayout->setSpacing(12);
        workspaceTitleLabel_ =
            new QLabel(QStringLiteral("Table: -"), workspaceHeader_);
        workspaceStatsLabel_ =
            new QLabel(QStringLiteral("Rows: 0 | Columns: 0"),
                       workspaceHeader_);
        workspaceModeStateLabel_ =
            new QLabel(QStringLiteral("Mode: Data"), workspaceHeader_);
        workspaceModeBar_ = new QTabBar(workspaceHeader_);
        workspaceModeBar_->addTab(QStringLiteral("Data"));
        workspaceModeBar_->addTab(QStringLiteral("Design"));
        workspaceModeBar_->addTab(QStringLiteral("Schema Text"));
        workspaceModeBar_->setExpanding(false);
        workspaceHeaderLayout->addWidget(workspaceTitleLabel_);
        workspaceHeaderLayout->addWidget(workspaceStatsLabel_);
        workspaceHeaderLayout->addStretch(1);
        workspaceHeaderLayout->addWidget(workspaceModeStateLabel_);
        workspaceHeaderLayout->addWidget(workspaceModeBar_);
        centralLayout->addWidget(workspaceHeader_);

        workspaceStack_ = new QStackedWidget(centralWidget);

        tablePage_ = new QWidget(centralWidget);
        auto* tableLayout = new QVBoxLayout(tablePage_);
        tableLayout->setContentsMargins(0, 0, 0, 0);
        tableLayout->setSpacing(6);


        auto* filterLayout = new QHBoxLayout();
        filterEdit_ = new QLineEdit(tablePage_);
        filterEdit_->setPlaceholderText(QStringLiteral("Filter current table"));
        auto* clearFilterButton =
            new QPushButton(QStringLiteral("Clear"), tablePage_);
        filterLayout->addWidget(
            new QLabel(QStringLiteral("Filter:"), tablePage_));
        filterLayout->addWidget(filterEdit_, 1);
        filterLayout->addWidget(clearFilterButton);
        tableLayout->addLayout(filterLayout);

        dataTable_ = new SCTreeGrid(tablePage_);
        dataTable_->SetSelectionMode(SCSelectionMode::Row);
        dataTable_->SetEnterKeyBehavior(SCEnterKeyBehavior::BeginEdit);
        dataTable_->setContextMenuPolicy(Qt::CustomContextMenu);
        tableLayout->addWidget(dataTable_, 1);

        workspaceStack_->addWidget(tablePage_);
        tableDesignPane_ = new SCTableDesignPane(session_, workspaceStack_);
        workspaceStack_->addWidget(tableDesignPane_);
        schemaTextPane_ = new SCSchemaTextPane(session_, workspaceStack_);
        workspaceStack_->addWidget(schemaTextPane_);

        centralLayout->addWidget(workspaceStack_, 1);
        setCentralWidget(centralWidget);

        connect(filterEdit_, &QLineEdit::textChanged, this,
                &SCDatabaseEditorMainWindow::OnFilterTextChanged);
        connect(clearFilterButton, &QPushButton::clicked, filterEdit_,
                &QLineEdit::clear);
        connect(dataTable_, &QWidget::customContextMenuRequested, this,
                &SCDatabaseEditorMainWindow::OnGridContextMenuRequested);
        connect(workspaceModeBar_, &QTabBar::currentChanged, this,
                &SCDatabaseEditorMainWindow::OnWorkspaceModeChanged);
        connect(tableDesignPane_, &SCTableDesignPane::StatusMessage, this,
                &SCDatabaseEditorMainWindow::SetStatusMessage);
        connect(schemaTextPane_, &SCSchemaTextPane::StatusMessage, this,
                &SCDatabaseEditorMainWindow::SetStatusMessage);
        connect(schemaTextPane_, &SCSchemaTextPane::TableImported, this,
                [this](const QString& tableName) {
                    SelectTableInExplorer(tableName);
                    SetWorkspaceMode(WorkspaceMode::Data);
                });

        objectExplorerDock_ =
            new QDockWidget(QStringLiteral("Object Explorer"), this);
        objectExplorerDock_->setObjectName(
            QStringLiteral("objectExplorerDock"));
        objectExplorerDock_->setAllowedAreas(Qt::LeftDockWidgetArea |
                                             Qt::RightDockWidgetArea);
        objectTree_ = new QTreeWidget(objectExplorerDock_);
        objectTree_->setHeaderLabels(
            {QStringLiteral("Object"), QStringLiteral("Type")});
        objectTree_->setSelectionMode(QAbstractItemView::SingleSelection);
        objectTree_->setAlternatingRowColors(true);
        objectTree_->setContextMenuPolicy(Qt::CustomContextMenu);
        objectExplorerDock_->setWidget(objectTree_);
        addDockWidget(Qt::LeftDockWidgetArea, objectExplorerDock_);
        connect(objectTree_, &QTreeWidget::itemSelectionChanged, this,
                &SCDatabaseEditorMainWindow::OnTableSelectionChanged);
        connect(
            objectTree_, &QWidget::customContextMenuRequested, this,
            &SCDatabaseEditorMainWindow::OnObjectExplorerContextMenuRequested);

        currentDetailsDock_ = new QDockWidget(QStringLiteral("Current Details"), this);
        currentDetailsDock_->setObjectName(QStringLiteral("inspectorDock"));
        currentDetailsDock_->setAllowedAreas(Qt::RightDockWidgetArea);
        currentDetailsTabs_ = new QTabWidget(currentDetailsDock_);

        recordTree_ = new QTreeWidget(currentDetailsTabs_);
        recordTree_->setHeaderLabels(
            {QStringLiteral("Field"), QStringLiteral("Value")});
        currentDetailsTabs_->addTab(recordTree_, QStringLiteral("Current Record"));

        computedColumnsTree_ = new QTreeWidget(currentDetailsTabs_);
        computedColumnsTree_->setHeaderLabels(
            {QStringLiteral("Name"), QStringLiteral("Type"),
             QStringLiteral("Expression"), QStringLiteral("Cacheable")});
        currentDetailsTabs_->addTab(computedColumnsTree_,
                               QStringLiteral("Computed Column"));

        relationTree_ = new QTreeWidget(currentDetailsTabs_);
        relationTree_->setHeaderLabels(
            {QStringLiteral("Field"), QStringLiteral("Target Table"),
            QStringLiteral("Target Field"), QStringLiteral("State")});
        currentDetailsTabs_->addTab(relationTree_, QStringLiteral("Relations"));

        currentDetailsDock_->setWidget(currentDetailsTabs_);
        addDockWidget(Qt::RightDockWidgetArea, currentDetailsDock_);

        bottomDock_ = new QDockWidget(QStringLiteral("Activity"), this);
        bottomDock_->setObjectName(QStringLiteral("bottomDock"));
        bottomDock_->setAllowedAreas(Qt::BottomDockWidgetArea);
        bottomTabs_ = new QTabWidget(bottomDock_);

        diagnosticsText_ = new QPlainTextEdit(bottomTabs_);
        diagnosticsText_->setReadOnly(true);
        diagnosticsText_->setPlainText(QStringLiteral("No database open."));
        bottomTabs_->addTab(diagnosticsText_, QStringLiteral("Diagnostics"));

        auto* editLogWidget = new QWidget(bottomTabs_);
        auto* editLogLayout = new QVBoxLayout(editLogWidget);
        editLogLayout->setContentsMargins(6, 6, 6, 6);
        editLogLayout->setSpacing(6);
        editStateText_ = new QPlainTextEdit(editLogWidget);
        editStateText_->setReadOnly(true);
        editStateText_->setMinimumHeight(110);
        editStateText_->setPlainText(QStringLiteral("No database open."));
        editLogLayout->addWidget(
            new QLabel(QStringLiteral("State Summary"), editLogWidget));
        editLogLayout->addWidget(editStateText_);
        editLogTree_ = new QTreeWidget(editLogWidget);
        editLogTree_->setHeaderLabels(
            {QStringLiteral("Operation"), QStringLiteral("Version"),
             QStringLiteral("Action"), QStringLiteral("Commit"),
             QStringLiteral("Timestamp"), QStringLiteral("Text"),
             QStringLiteral("Details")});
        editLogLayout->addWidget(
            new QLabel(QStringLiteral("Edit Log"), editLogWidget));
        editLogLayout->addWidget(editLogTree_, 1);
        bottomTabs_->addTab(editLogWidget, QStringLiteral("Edit Log"));

        healthSummaryText_ = new QPlainTextEdit(bottomTabs_);
        healthSummaryText_->setReadOnly(true);
        healthSummaryText_->setPlainText(QStringLiteral("No database open."));
        bottomTabs_->addTab(healthSummaryText_,
                            QStringLiteral("Health Summary"));

        sqlPreviewText_ = new QPlainTextEdit(bottomTabs_);
        sqlPreviewText_->setReadOnly(true);
        sqlPreviewText_->setPlainText(QStringLiteral(
            "SQL preview is unavailable until a table is opened."));
        bottomTabs_->addTab(sqlPreviewText_, QStringLiteral("SQL Preview"));

        debugPackageText_ = new QPlainTextEdit(bottomTabs_);
        debugPackageText_->setReadOnly(true);
        debugPackageText_->setPlainText(
            QStringLiteral("No debug package exported."));
        bottomTabs_->addTab(debugPackageText_, QStringLiteral("Export Debug Package"));

        bottomDock_->setWidget(bottomTabs_);
        addDockWidget(Qt::BottomDockWidgetArea, bottomDock_);

        // Status bar labels.
        openModeLabel_ = new QLabel(QStringLiteral("Mode: Closed"), this);
        currentTableLabel_ = new QLabel(QStringLiteral("Table: -"), this);
        tableStatsLabel_ = new QLabel(QStringLiteral("Rows: 0"), this);
        filterStateLabel_ = new QLabel(QStringLiteral("Filter: Off"), this);
        transactionStateLabel_ = new QLabel(QStringLiteral("Transaction: Idle"), this);

        // 添加分隔符
        auto* spacer1 = new QWidget(this);
        spacer1->setFixedWidth(12);
        auto* spacer2 = new QWidget(this);
        spacer2->setFixedWidth(12);
        auto* spacer3 = new QWidget(this);
        spacer3->setFixedWidth(12);
        auto* spacer4 = new QWidget(this);
        spacer4->setFixedWidth(12);

        statusBar()->addWidget(openModeLabel_);
        statusBar()->addWidget(spacer1);
        statusBar()->addWidget(currentTableLabel_);
        statusBar()->addWidget(spacer2);
        statusBar()->addWidget(tableStatsLabel_);
        statusBar()->addWidget(spacer3);
        statusBar()->addWidget(filterStateLabel_);
        statusBar()->addWidget(spacer4);
        statusBar()->addWidget(transactionStateLabel_);

        // 添加分隔线（使用 QFrame）
        auto* separator = new QFrame(this);
        separator->setFrameShape(QFrame::VLine);
        separator->setFrameShadow(QFrame::Sunken);
        statusBar()->addWidget(separator);

        statusLabel_ = new QLabel(QStringLiteral("Ready."), this);
        statusBar()->addPermanentWidget(statusLabel_, 1);
    }

    void SCDatabaseEditorMainWindow::BuildMenus()
    {
        auto* fileMenu = menuBar()->addMenu(QStringLiteral("File(&F)"));
        fileMenu->addAction(QStringLiteral("New Database..."), this,
                            &SCDatabaseEditorMainWindow::CreateDatabase);
        fileMenu->addAction(QStringLiteral("Open Database..."), this,
                            &SCDatabaseEditorMainWindow::OpenDatabase);
        fileMenu->addAction(QStringLiteral("Create Backup Copy..."), this,
                            &SCDatabaseEditorMainWindow::CreateBackupCopy);
        fileMenu->addAction(QStringLiteral("Export Debug Package..."), this,
                            &SCDatabaseEditorMainWindow::ExportDebugPackage);

        auto* editMenu = menuBar()->addMenu(QStringLiteral("Edit(&E)"));
        editMenu->addAction(QStringLiteral("Undo"), this,
                            &SCDatabaseEditorMainWindow::UndoLastAction);
        editMenu->addAction(QStringLiteral("Redo"), this,
                            &SCDatabaseEditorMainWindow::RedoLastAction);
        editMenu->addSeparator();
        savePendingChangesAction_ = editMenu->addAction(
            QStringLiteral("Save Pending Changes"), this,
            &SCDatabaseEditorMainWindow::SavePendingChanges);
        discardPendingChangesAction_ = editMenu->addAction(
            QStringLiteral("Discard Pending Changes"), this,
            &SCDatabaseEditorMainWindow::DiscardPendingChanges);
        editMenu->addAction(QStringLiteral("Refresh"), this,
                            &SCDatabaseEditorMainWindow::RefreshCurrentView);

        auto* tableMenu = menuBar()->addMenu(QStringLiteral("Table(&T)"));
        tableMenu->addAction(QStringLiteral("Create Table..."), this,
                             &SCDatabaseEditorMainWindow::CreateTable);
        tableMenu->addAction(
            QStringLiteral("Create Table From Schema..."), this,
            &SCDatabaseEditorMainWindow::CreateTableFromSchemaDescription);
        tableMenu->addAction(QStringLiteral("Delete Selected Table"), this,
                             &SCDatabaseEditorMainWindow::DeleteSelectedTable);
        tableMenu->addAction(
            QStringLiteral("Open Design Workspace"), this,
            &SCDatabaseEditorMainWindow::OpenSchemaTableConverter);
        tableMenu->addAction(QStringLiteral("Add Column..."), this,
                             &SCDatabaseEditorMainWindow::AddColumn);
        tableMenu->addAction(QStringLiteral("Edit Selected Column..."), this,
                             &SCDatabaseEditorMainWindow::EditSelectedColumn);
        tableMenu->addAction(QStringLiteral("Add Record"), this,
                             &SCDatabaseEditorMainWindow::AddRecord);
        tableMenu->addAction(QStringLiteral("Delete Selected Record"), this,
                             &SCDatabaseEditorMainWindow::DeleteSelectedRecord);
        tableMenu->addAction(QStringLiteral("Select Relation..."), this,
                             &SCDatabaseEditorMainWindow::EditSelectedRelation);
        tableMenu->addSeparator();
        tableMenu->addAction(
            QStringLiteral("Export CSV..."), this,
            &SCDatabaseEditorMainWindow::ExportCurrentTableCsv);
        tableMenu->addAction(
            QStringLiteral("Import CSV..."), this,
            &SCDatabaseEditorMainWindow::ImportCsvIntoCurrentTable);

        auto* computedMenu =
            menuBar()->addMenu(QStringLiteral("Computed(&C)"));
        computedMenu->addAction(
            QStringLiteral("Add Session Computed Column..."), this,
            &SCDatabaseEditorMainWindow::AddSessionComputedColumn);
        computedMenu->addAction(
            QStringLiteral("Edit Selected Computed Column..."), this,
            &SCDatabaseEditorMainWindow::EditSelectedComputedColumn);
        computedMenu->addAction(
            QStringLiteral("Convert Selected Column To Computed..."), this,
            &SCDatabaseEditorMainWindow::ConvertSelectedColumnToComputed);
        computedMenu->addAction(
            QStringLiteral("Convert Selected Computed To Column..."),
            this, &SCDatabaseEditorMainWindow::ConvertSelectedComputedToColumn);
        computedMenu->addAction(
            QStringLiteral("Delete Selected Computed Column"), this,
            &SCDatabaseEditorMainWindow::DeleteSelectedComputedColumn);

        auto* toolsMenu = menuBar()->addMenu(QStringLiteral("Tools(&T)"));
        toolsMenu->addAction(QStringLiteral("Health Summary"), this,
                             &SCDatabaseEditorMainWindow::ShowHealthSummary);
        toolsMenu->addAction(QStringLiteral("Show Edit Log / State Summary"),
                             this,
                             &SCDatabaseEditorMainWindow::ShowEditLogSummary);
        toolsMenu->addAction(QStringLiteral("Export Debug Package..."), this,
                             &SCDatabaseEditorMainWindow::ExportDebugPackage);

        auto* viewMenu = menuBar()->addMenu(QStringLiteral("View(&V)"));
        viewMenu->addAction(objectExplorerDock_->toggleViewAction());
        viewMenu->addAction(currentDetailsDock_->toggleViewAction());
        viewMenu->addAction(bottomDock_->toggleViewAction());
        viewMenu->addSeparator();
        viewMenu->addAction(QStringLiteral("Reset Layout"), this, [this]() {
            objectExplorerDock_->show();
            currentDetailsDock_->show();
            bottomDock_->show();
            objectExplorerDock_->raise();
            currentDetailsDock_->raise();
            bottomDock_->raise();
        });

        auto* toolbar = addToolBar(QStringLiteral("Main Toolbar"));
        toolbar->addAction(QStringLiteral("Open Database"), this,
                           &SCDatabaseEditorMainWindow::OpenDatabase);
        toolbar->addAction(QStringLiteral("New Database"), this,
                           &SCDatabaseEditorMainWindow::CreateDatabase);
        closeDatabaseAction_ =
            toolbar->addAction(QStringLiteral("Close Database"), this,
                               &SCDatabaseEditorMainWindow::CloseDatabase);
        toolbar->addAction(QStringLiteral("Create Backup Copy"), this,
                           &SCDatabaseEditorMainWindow::CreateBackupCopy);
        toolbar->addAction(QStringLiteral("Refresh"), this,
                           &SCDatabaseEditorMainWindow::RefreshCurrentView);
        toolbar->addSeparator();
        undoAction_ = toolbar->addAction(QStringLiteral("Undo"), this,
                           &SCDatabaseEditorMainWindow::UndoLastAction);
        redoAction_ = toolbar->addAction(QStringLiteral("Redo"), this,
                           &SCDatabaseEditorMainWindow::RedoLastAction);
        toolbar->addSeparator();
        toolbar->addAction(QStringLiteral("Health Summary"), this,
                           &SCDatabaseEditorMainWindow::ShowHealthSummary);
        toolbar->addAction(QStringLiteral("Export Debug Package"), this,
                           &SCDatabaseEditorMainWindow::ExportDebugPackage);

        // 表Operation工具栏 - 与主工具栏并列
        // Table operations toolbar, alongside the main toolbar.
        tableToolBar_ = addToolBar(QStringLiteral("Table Actions"));
        tableToolBar_->addAction(QStringLiteral("Add Record"), this,
                                 &SCDatabaseEditorMainWindow::AddRecord);
        tableToolBar_->addAction(
            QStringLiteral("Delete Record"), this,
            &SCDatabaseEditorMainWindow::DeleteSelectedRecord);
        tableToolBar_->addSeparator();
        tableToolBar_->addAction(
            QStringLiteral("Export CSV..."), this,
            &SCDatabaseEditorMainWindow::ExportCurrentTableCsv);
        tableToolBar_->addAction(
            QStringLiteral("Import CSV..."), this,
            &SCDatabaseEditorMainWindow::ImportCsvIntoCurrentTable);
        tableToolBar_->addSeparator();
        tableToolBar_->addAction(QStringLiteral("Add Column..."), this,
                                 &SCDatabaseEditorMainWindow::AddColumn);
        tableToolBar_->addAction(
            QStringLiteral("Edit Selected Column..."), this,
            &SCDatabaseEditorMainWindow::EditSelectedColumn);
        tableToolBar_->addAction(
            QStringLiteral("Select Relation..."), this,
            &SCDatabaseEditorMainWindow::EditSelectedRelation);
        tableToolBar_->addSeparator();
        tableToolBar_->addAction(
            QStringLiteral("Save Pending Changes"), this,
            &SCDatabaseEditorMainWindow::SavePendingChanges);
        tableToolBar_->addAction(
            QStringLiteral("Discard Pending Changes"), this,
            &SCDatabaseEditorMainWindow::DiscardPendingChanges);
        tableToolBar_->addSeparator();
        tableToolBar_->addAction(
            QStringLiteral("Add Computed Column"), this,
            &SCDatabaseEditorMainWindow::AddSessionComputedColumn);
        tableToolBar_->addAction(
            QStringLiteral("Edit Computed Column"), this,
            &SCDatabaseEditorMainWindow::EditSelectedComputedColumn);

        if (closeDatabaseAction_ != nullptr)
        {
            closeDatabaseAction_->setEnabled(session_->IsOpen());
        }
    }

    void SCDatabaseEditorMainWindow::CreateDatabase()
    {
        if (session_->IsOpen() && session_->HasPendingEdit())
        {
            const QMessageBox::StandardButton answer = QMessageBox::question(
            this, QStringLiteral("Create Database"),
            QStringLiteral("Pending changes will be discarded. Continue?"));
            if (answer != QMessageBox::Yes)
            {
                return;
            }
        }

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
            ShowError(QStringLiteral("Failed to create database"), error);
        }
    }

    void SCDatabaseEditorMainWindow::OpenDatabase()
    {
        if (session_->IsOpen() && session_->HasPendingEdit())
        {
            const QMessageBox::StandardButton answer = QMessageBox::question(
            this, QStringLiteral("Open Database"),
            QStringLiteral("Pending changes will be discarded. Continue?"));
            if (answer != QMessageBox::Yes)
            {
                return;
            }
        }

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
            ShowError(QStringLiteral("Failed to open database"), error);
        }
    }

    void SCDatabaseEditorMainWindow::CloseDatabase()
    {
        if (!session_->IsOpen())
        {
            SetStatusMessage(QStringLiteral("No database open."));
            return;
        }

        sc::SCEditingDatabaseState editingState;
        QString stateError;
        const bool stateLoaded =
            session_->GetEditingState(&editingState, &stateError);
        if (!stateLoaded && !stateError.isEmpty())
        {
            ShowError(QStringLiteral("Failed to close database"), stateError);
            return;
        }
        const bool hasPendingChanges = session_->HasPendingEdit();

        const QMessageBox::StandardButton answer = QMessageBox::question(
            this, QStringLiteral("Close Database"),
            hasPendingChanges
                ? QStringLiteral(
                      "Close the current database? Pending changes will be discarded.")
                : QStringLiteral("Close the current database?"));
        if (answer != QMessageBox::Yes)
        {
            return;
        }

        QString error;
        if (!session_->CloseDatabase(&error))
        {
            ShowError(QStringLiteral("Failed to close database"), error);
            return;
        }

        RefreshGridData();
        RefreshObjectExplorer();
        RefreshCurrentDetailsPanel();
        RefreshCurrentRecordPanel();
        UpdateComputedColumnsPanel();
        RefreshRelationPanel();
        UpdateDatabaseStatusBar();
        RefreshOverviewPanels();
        UpdateGridSummary();
        setWindowTitle(QStringLiteral("Database Editor"));
        SetStatusMessage(QStringLiteral("Database closed."));
    }

    void SCDatabaseEditorMainWindow::CreateBackupCopy()
    {
        if (!session_->IsOpen())
        {
            ShowError(QStringLiteral("Failed to create backup copy"),
                      QStringLiteral("No database open."));
            return;
        }

        const QString defaultPath =
            session_->DatabasePath().isEmpty()
                ? QString()
                : session_->DatabasePath() + QStringLiteral("_backup.sqlite");
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
            ShowError(QStringLiteral("Failed to create backup copy"), error);
            return;
        }

        SetStatusMessage(QStringLiteral("Backup copy created: ") + filePath);
    }

    void SCDatabaseEditorMainWindow::OpenSchemaTableConverter()
    {
        if (session_ == nullptr || !session_->IsOpen() ||
            session_->CurrentTableName().isEmpty())
        {
            ShowError(QStringLiteral("Open Design Workspace"),
                      QStringLiteral("Please select the current table first."));
            return;
        }

        SetWorkspaceMode(WorkspaceMode::Design);
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
            ShowError(QStringLiteral("Failed to create table"), error);
        }
    }

    void SCDatabaseEditorMainWindow::CreateTableFromSchemaDescription()
    {
        if (session_ == nullptr || !session_->IsOpen())
        {
            ShowError(QStringLiteral("Failed to create table from schema"),
                      QStringLiteral("No database open."));
            return;
        }

        SCSchemaTableImportDialog dialog(session_, this);
        dialog.exec();

        RefreshGridData();
        RefreshObjectExplorer();
        RefreshCurrentDetailsPanel();
        RefreshCurrentRecordPanel();
        UpdateComputedColumnsPanel();
        RefreshRelationPanel();
        UpdateDatabaseStatusBar();
        RefreshOverviewPanels();
        RefreshWorkspaceHeader();
        RefreshWorkspacePages();
        UpdateGridSummary();
    }

    void SCDatabaseEditorMainWindow::AddColumn()
    {
        if (session_->CurrentTableName().isEmpty() && objectTree_ != nullptr &&
            objectTree_->currentItem() != nullptr)
        {
            const ExplorerNodeType nodeType = static_cast<ExplorerNodeType>(
                objectTree_->currentItem()
                    ->data(0, kExplorerNodeTypeRole)
                    .toInt());
            if (nodeType == ExplorerNodeType::Table ||
                nodeType == ExplorerNodeType::TableColumns ||
                nodeType == ExplorerNodeType::TableConstraints ||
                nodeType == ExplorerNodeType::TableIndexes ||
                nodeType == ExplorerNodeType::TableRecords)
            {
                QString error;
                const QString nodeName = objectTree_->currentItem()
                                             ->data(0, kExplorerNodeNameRole)
                                             .toString();
                if (!session_->SelectTable(nodeName, &error))
                {
                    ShowError(QStringLiteral("Failed to add column"), error);
                    return;
                }
            }
        }

        if (session_->CurrentTableName().isEmpty())
        {
            ShowError(QStringLiteral("Failed to add column"),
                      QStringLiteral("Please select a table first."));
            return;
        }

        sc::SCEditingDatabaseState editingState;
        QString editingError;
        if (!session_->GetEditingState(&editingState, &editingError))
        {
            ShowError(QStringLiteral("Failed to add column"), editingError);
            return;
        }
        if (session_->HasPendingEdit())
        {
            ShowError(QStringLiteral("Failed to add column"),
                      QStringLiteral(
                          "Please save or discard pending changes before editing the schema."));
            return;
        }

        SCAddColumnDialog dialog(session_, this);
        bool tableHasRecords = false;
        QString stateError;
        if (!session_->CurrentTableHasRecords(&tableHasRecords, &stateError))
        {
            ShowError(QStringLiteral("Failed to add column"), stateError);
            return;
        }
        dialog.SetCurrentTableHasRecords(tableHasRecords);
        if (dialog.exec() != QDialog::Accepted)
        {
            return;
        }

        const sc::SCColumnDef column = dialog.BuildColumnDef();
        QString error;
        if (!session_->AddColumn(column, &error))
        {
            ShowError(QStringLiteral("Failed to add column"), error);
            return;
        }

        RefreshCurrentDetailsPanel();
        SelectDesignColumnByName(ToQString(column.name));
        RefreshGridData();
        RefreshRelationPanel();
        RefreshOverviewPanels();
        SetStatusMessage(QStringLiteral("Column added: ") +
                         ToQString(column.name));
    }

    void SCDatabaseEditorMainWindow::OnObjectExplorerContextMenuRequested(
        const QPoint& pos)
    {
        if (objectTree_ == nullptr)
        {
            return;
        }

        if (QTreeWidgetItem* item = objectTree_->itemAt(pos); item != nullptr)
        {
            objectTree_->setCurrentItem(item);
        }

        QTreeWidgetItem* current = objectTree_->currentItem();
        const ExplorerNodeType nodeType =
            current != nullptr
                ? static_cast<ExplorerNodeType>(
                      current->data(0, kExplorerNodeTypeRole).toInt())
                : ExplorerNodeType::Database;
        QMenu menu(objectTree_);

        if (nodeType == ExplorerNodeType::Table)
        {
            QAction* selectAction = menu.addAction(
                QStringLiteral("Select Table"), this,
                &SCDatabaseEditorMainWindow::OnTableSelectionChanged);
            selectAction->setEnabled(true);
            menu.addAction(QStringLiteral("Add Column..."), this,
                           &SCDatabaseEditorMainWindow::AddColumn);
            menu.addAction(QStringLiteral("Delete Table..."), this,
                           &SCDatabaseEditorMainWindow::DeleteSelectedTable);
        } else if (nodeType == ExplorerNodeType::TablesRoot)
        {
            menu.addAction(QStringLiteral("Create Table..."), this,
                           &SCDatabaseEditorMainWindow::CreateTable);
            menu.addAction(
                QStringLiteral("Create Table From Schema..."), this,
                &SCDatabaseEditorMainWindow::CreateTableFromSchemaDescription);
        } else if (nodeType == ExplorerNodeType::ComputedRoot)
        {
            menu.addAction(
                QStringLiteral("Add Session Computed Column..."), this,
                &SCDatabaseEditorMainWindow::AddSessionComputedColumn);
        } else
        {
            menu.addAction(QStringLiteral("Refresh"), this,
                           &SCDatabaseEditorMainWindow::RefreshCurrentView);
        }

        menu.exec(objectTree_->mapToGlobal(pos));
    }

    void SCDatabaseEditorMainWindow::DeleteSelectedTable()
    {
        if (session_ == nullptr || !session_->IsOpen())
        {
            ShowError(QStringLiteral("Failed to delete table"),
                      QStringLiteral("No database open."));
            return;
        }

        QString tableName;
        if (objectTree_ != nullptr && objectTree_->currentItem() != nullptr)
        {
            const ExplorerNodeType nodeType = static_cast<ExplorerNodeType>(
                objectTree_->currentItem()
                    ->data(0, kExplorerNodeTypeRole)
                    .toInt());
            if (nodeType == ExplorerNodeType::Table)
            {
                tableName = objectTree_->currentItem()
                                ->data(0, kExplorerNodeNameRole)
                                .toString();
            }
        }

        if (tableName.isEmpty())
        {
            tableName = session_->CurrentTableName();
        }

        if (tableName.isEmpty())
        {
            ShowError(QStringLiteral("Failed to delete table"),
                      QStringLiteral("Please select a table first."));
            return;
        }

        const QMessageBox::StandardButton answer = QMessageBox::question(
            this, QStringLiteral("Delete Table"),
            QStringLiteral("Delete Table \"%1\"?").arg(tableName));
        if (answer != QMessageBox::Yes)
        {
            return;
        }

        QString error;
        if (!session_->DeleteTable(tableName, &error))
        {
            ShowError(QStringLiteral("Failed to delete table"), error);
            return;
        }

        SetStatusMessage(QStringLiteral("Table deleted: ") + tableName);
    }

    void SCDatabaseEditorMainWindow::DeleteSelectedColumn()
    {
        const QString columnName = CurrentDesignColumnName();
        if (columnName.isEmpty())
        {
            ShowError(QStringLiteral("Failed to delete column"),
                      QStringLiteral("Please select a schema field first."));
            return;
        }

        const QMessageBox::StandardButton answer = QMessageBox::question(
            this, QStringLiteral("Delete Column"),
            QStringLiteral("Delete schema field \"%1\"?").arg(columnName));
        if (answer != QMessageBox::Yes)
        {
            return;
        }

        QVector<sc::SCColumnDef> columnsBeforeDelete;
        QString error;
        if (!session_->BuildSchemaSnapshot(&columnsBeforeDelete, &error))
        {
            ShowError(QStringLiteral("Failed to delete column"), error);
            return;
        }

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

        if (!session_->RemoveColumn(columnName, &error))
        {
            ShowError(QStringLiteral("Failed to delete column"), error);
            return;
        }

        RefreshCurrentDetailsPanel();
        SelectDesignColumnByName(fallbackSelection);
        RefreshGridData();
        RefreshRelationPanel();
        RefreshOverviewPanels();
        SetStatusMessage(QStringLiteral("Column deleted: ") + columnName);
    }

    void SCDatabaseEditorMainWindow::EditSelectedColumn()
    {
        const QString columnName = CurrentDesignColumnName();
        if (columnName.isEmpty())
        {
            ShowError(QStringLiteral("Failed to edit column"),
                      QStringLiteral("Please select a schema field first."));
            return;
        }

        sc::SCColumnDef existing;
        QString error;
        if (!session_->GetColumnDef(columnName, &existing, &error))
        {
            ShowError(QStringLiteral("Failed to edit column"), error);
            return;
        }

        sc::SCEditingDatabaseState editingState;
        QString editingError;
        if (!session_->GetEditingState(&editingState, &editingError))
        {
            ShowError(QStringLiteral("Failed to edit column"), editingError);
            return;
        }
        if (session_->HasPendingEdit())
        {
            ShowError(QStringLiteral("Failed to edit column"),
                      QStringLiteral(
                          "Please save or discard pending changes before editing the schema."));
            return;
        }

        SCAddColumnDialog dialog(session_, existing, this);
        bool tableHasRecords = false;
        QString stateError;
        if (!session_->CurrentTableHasRecords(&tableHasRecords, &stateError))
        {
            ShowError(QStringLiteral("Failed to edit column"), stateError);
            return;
        }
        dialog.SetCurrentTableHasRecords(tableHasRecords);
        if (dialog.exec() != QDialog::Accepted)
        {
            return;
        }

        const sc::SCColumnDef updated = dialog.BuildColumnDef();
        if (updated.name.empty())
        {
            ShowError(QStringLiteral("Failed to edit column"),
                      QStringLiteral("Column name is required."));
            return;
        }

        if (!session_->UpdateColumn(columnName, updated, &error))
        {
            ShowError(QStringLiteral("Failed to edit column"), error);
            return;
        }

        RefreshCurrentDetailsPanel();
        SelectDesignColumnByName(ToQString(updated.name));
        RefreshGridData();
        RefreshRelationPanel();
        RefreshOverviewPanels();
        SetStatusMessage(QStringLiteral("Column updated: ") +
                         ToQString(updated.name));
    }

    void SCDatabaseEditorMainWindow::ConvertSelectedColumnToComputed()
    {
        const QString columnName = CurrentDesignColumnName();
        if (columnName.isEmpty())
        {
            ShowError(QStringLiteral("Failed to convert to computed column"),
                      QStringLiteral("Please select a schema field first."));
            return;
        }

        sc::SCColumnDef existing;
        QString error;
        if (!session_->GetColumnDef(columnName, &existing, &error))
        {
            ShowError(QStringLiteral("Failed to convert to computed column"), error);
            return;
        }

        SCComputedColumnDialog dialog(session_->CurrentTableName(),
                                      BuildComputedTemplate(existing), true,
                                      this);
        dialog.setWindowTitle(QStringLiteral("Convert Column to Computed Column"));
        if (dialog.exec() != QDialog::Accepted)
        {
            return;
        }

        sc::SCComputedColumnDef definition;
        if (!dialog.BuildDefinition(&definition, &error))
        {
            ShowError(QStringLiteral("Failed to convert to computed column"), error);
            return;
        }

        if (!session_->ConvertColumnToComputed(columnName, definition, &error))
        {
            ShowError(QStringLiteral("Failed to convert to computed column"), error);
            return;
        }

        RefreshCurrentDetailsPanel();
        UpdateComputedColumnsPanel();
        SelectComputedColumnByName(ToQString(definition.name));
        RefreshGridData();
        RefreshRelationPanel();
        RefreshOverviewPanels();
        SetStatusMessage(QStringLiteral("Converted to computed column: ") +
                         ToQString(definition.name));
    }

    void SCDatabaseEditorMainWindow::AddSessionComputedColumn()
    {
        if (!session_->CurrentTable())
        {
            ShowError(QStringLiteral("Failed to add computed column"),
                      QStringLiteral("No table selected."));
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
            ShowError(QStringLiteral("Failed to add computed column"), error);
            return;
        }

        if (!session_->AddSessionComputedColumn(definition, &error))
        {
            ShowError(QStringLiteral("Failed to add computed column"), error);
            return;
        }

        RefreshGridData();
        UpdateComputedColumnsPanel();
        SelectComputedColumnByName(ToQString(definition.name));
        RefreshRelationPanel();
        RefreshOverviewPanels();
        SetStatusMessage(QStringLiteral("Computed column added: ") +
                         ToQString(definition.name));
    }

    void SCDatabaseEditorMainWindow::EditSelectedComputedColumn()
    {
        const QString columnName = CurrentComputedColumnName();
        if (columnName.isEmpty())
        {
            ShowError(QStringLiteral("Failed to edit computed column"),
                      QStringLiteral("Please select a computed column in the session pane."));
            return;
        }

        sc::SCComputedColumnDef existing;
        QString error;
        if (!session_->GetSessionComputedColumn(columnName, &existing, &error))
        {
            ShowError(QStringLiteral("Failed to edit computed column"), error);
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
            ShowError(QStringLiteral("Failed to edit computed column"), error);
            return;
        }

        if (!session_->UpdateSessionComputedColumn(columnName, updated, &error))
        {
            ShowError(QStringLiteral("Failed to edit computed column"), error);
            return;
        }

        RefreshGridData();
        UpdateComputedColumnsPanel();
        SelectComputedColumnByName(ToQString(updated.name));
        RefreshRelationPanel();
        RefreshOverviewPanels();
        SetStatusMessage(QStringLiteral("Computed column updated: ") +
                         ToQString(updated.name));
    }

    void SCDatabaseEditorMainWindow::ConvertSelectedComputedToColumn()
    {
        const QString columnName = CurrentComputedColumnName();
        if (columnName.isEmpty())
        {
            ShowError(QStringLiteral("Failed to convert to column"),
                      QStringLiteral("Please select a computed column first."));
            return;
        }

        sc::SCComputedColumnDef existing;
        QString error;
        if (!session_->GetSessionComputedColumn(columnName, &existing, &error))
        {
            ShowError(QStringLiteral("Failed to convert to column"), error);
            return;
        }

        sc::SCEditingDatabaseState editingState;
        QString editingError;
        if (!session_->GetEditingState(&editingState, &editingError))
        {
            ShowError(QStringLiteral("Failed to convert to column"), editingError);
            return;
        }
        if (session_->HasPendingEdit())
        {
            ShowError(QStringLiteral("Failed to convert to column"),
                      QStringLiteral(
                          "Please save or discard pending changes before editing the schema."));
            return;
        }

        SCAddColumnDialog dialog(session_, BuildColumnTemplate(existing), this);
        dialog.setWindowTitle(QStringLiteral("Convert Computed Column to Column"));
        bool tableHasRecords = false;
        QString stateError;
        if (!session_->CurrentTableHasRecords(&tableHasRecords, &stateError))
        {
            ShowError(QStringLiteral("Failed to convert to column"), stateError);
            return;
        }
        dialog.SetCurrentTableHasRecords(tableHasRecords);
        if (dialog.exec() != QDialog::Accepted)
        {
            return;
        }

        const sc::SCColumnDef definition = dialog.BuildColumnDef();
        if (definition.name.empty())
        {
            ShowError(QStringLiteral("Failed to convert to column"),
                      QStringLiteral("Column name is required."));
            return;
        }

        if (!session_->ConvertComputedToColumn(columnName, definition, &error))
        {
            ShowError(QStringLiteral("Failed to convert to column"), error);
            return;
        }

        RefreshCurrentDetailsPanel();
        UpdateComputedColumnsPanel();
        SelectDesignColumnByName(ToQString(definition.name));
        RefreshGridData();
        RefreshRelationPanel();
        RefreshOverviewPanels();
        SetStatusMessage(QStringLiteral("Converted to column: ") +
                         ToQString(definition.name));
    }

    void SCDatabaseEditorMainWindow::DeleteSelectedComputedColumn()
    {
        const QString columnName = CurrentComputedColumnName();
        if (columnName.isEmpty())
        {
            ShowError(QStringLiteral("Failed to delete computed column"),
                      QStringLiteral("Please select a computed column in the session pane."));
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
            ShowError(QStringLiteral("Failed to delete computed column"), error);
            return;
        }

        RefreshGridData();
        UpdateComputedColumnsPanel();
        SelectComputedColumnByName(fallbackSelection);
        RefreshRelationPanel();
        RefreshOverviewPanels();
        SetStatusMessage(QStringLiteral("Computed column deleted: ") +
                         columnName);
    }

    void SCDatabaseEditorMainWindow::AddRecord()
    {
        QString error;
        if (!session_->AddRecord(&error))
        {
            ShowError(QStringLiteral("Failed to add record"), error);
            return;
        }

        RefreshGridData();
        RefreshCurrentRecordPanel();
        RefreshRelationPanel();
        RefreshOverviewPanels();
        UpdateGridSummary();
        UpdateDatabaseStatusBar();

        sc::SCEditingDatabaseState editingState;
        if (session_->GetEditingState(&editingState, &error) &&
            session_->HasPendingEdit())
        {
            SetStatusMessage(QStringLiteral(
                "Record draft created. Fill required fields and then save pending changes."));
        } else
        {
            SetStatusMessage(QStringLiteral("Record added."));
        }
    }

    void SCDatabaseEditorMainWindow::SavePendingChanges()
    {
        QString error;
        if (!session_->SavePendingChanges(&error))
        {
            ShowError(QStringLiteral("Failed to save pending changes"), error);
            return;
        }

        RefreshGridData();
        RefreshCurrentRecordPanel();
        RefreshRelationPanel();
        RefreshOverviewPanels();
        UpdateGridSummary();
        UpdateDatabaseStatusBar();
        SetStatusMessage(QStringLiteral("Pending changes saved."));
    }

    void SCDatabaseEditorMainWindow::DiscardPendingChanges()
    {
        QString error;
        if (!session_->DiscardPendingChanges(&error))
        {
            ShowError(QStringLiteral("Failed to discard pending changes"), error);
            return;
        }

        RefreshGridData();
        RefreshCurrentRecordPanel();
        RefreshRelationPanel();
        RefreshOverviewPanels();
        UpdateGridSummary();
        UpdateDatabaseStatusBar();
        SetStatusMessage(QStringLiteral("Pending changes discarded."));
    }

    void SCDatabaseEditorMainWindow::DeleteSelectedRecord()
    {
        const QVector<int> selected = dataTable_->SelectedRows();
        if (selected.isEmpty())
        {
            return;
        }

        const int row = selected.first();
        const sc::RecordId recordId = RecordIdAt(row);
        if (recordId == 0)
        {
            return;
        }

        const QMessageBox::StandardButton answer = QMessageBox::question(
            this, QStringLiteral("Delete Record"),
            QStringLiteral("Delete Record %1?").arg(recordId));
        if (answer != QMessageBox::Yes)
        {
            return;
        }

        QString error;
        if (!session_->DeleteRecord(recordId, &error))
        {
            ShowError(QStringLiteral("Failed to delete record"), error);
            return;
        }

        SetStatusMessage(QStringLiteral("Record deleted: ") +
                         QString::number(recordId));
    }

    void SCDatabaseEditorMainWindow::EditSelectedRelation()
    {
        const int row = CurrentRow();
        const int col = CurrentColumn();
        if (row < 0 || col <= 0)
        {
            return;
        }

        const sc::SCTableViewColumnDef tableColumn =
            columns_[col - 1];
        if (tableColumn.layer != sc::TableColumnLayer::Fact)
        {
            ShowError(QStringLiteral("Failed to select relation"),
                      QStringLiteral("Computed columns cannot store relation values."));
            return;
        }

        sc::SCColumnDef column;
        QString error;
        if (!session_->GetColumnDef(ToQString(tableColumn.name), &column,
                                    &error))
        {
            ShowError(QStringLiteral("Failed to select relation"), error);
            return;
        }
        if (column.columnKind != sc::ColumnKind::Relation)
        {
            ShowError(QStringLiteral("Failed to select relation"),
                      QStringLiteral("The selected column is not a relation field."));
            return;
        }

        QVector<SCDatabaseSession::RelationCandidate> candidates;
        if (!session_->BuildRelationCandidates(ToQString(column.referenceTable),
                                               column, &candidates, &error))
        {
            ShowError(QStringLiteral("Failed to select relation"), error);
            return;
        }

        SCRelationPickerDialog dialog(ToQString(column.referenceTable),
                                      candidates, this);
        if (dialog.exec() != QDialog::Accepted)
        {
            return;
        }

        QVariant storedValue;
        if (!session_->GetRelationStoredValue(dialog.SelectedRecordId(), column,
                                              &storedValue, &error))
        {
            ShowError(QStringLiteral("Failed to select relation"), error);
            return;
        }

        if (!session_->SetCellValue(RecordIdAt(row),
                                    ToQString(tableColumn.name), storedValue,
                                    &error))
        {
            ShowError(QStringLiteral("Failed to select relation"), error);
            return;
        }

        RefreshGridData();
        SetStatusMessage(QStringLiteral("Relation updated."));
    }

    void SCDatabaseEditorMainWindow::OnGridContextMenuRequested(
        const QPoint& pos)
    {
        if (dataTable_ == nullptr)
        {
            return;
        }

        // Map click position to viewport and select the cell under cursor.
        // SCTreeGrid defaults: header height 28, row height 24.
        const QPoint viewportPos = dataTable_->viewport()->mapFrom(dataTable_, pos);
        constexpr int kHeaderHeight = 28;
        constexpr int kRowHeight = 24;
        const int row = (viewportPos.y() - kHeaderHeight) / kRowHeight;
        if (row >= 0 && row < static_cast<int>(visibleRows_.size()))
        {
            dataTable_->SetCurrentCell(row, 0);
        }
        else
        {
            currentRow_ = -1;
            currentColumn_ = -1;
        }

        QMenu menu(dataTable_);
        const bool canEditRows =
            session_->IsOpen() && !session_->CurrentTableName().isEmpty();
        QAction* addAction =
            menu.addAction(QStringLiteral("Add Record"), this,
                           &SCDatabaseEditorMainWindow::AddRecord);
        addAction->setEnabled(canEditRows);
        QAction* deleteAction =
            menu.addAction(QStringLiteral("Delete Record"), this,
                           &SCDatabaseEditorMainWindow::DeleteSelectedRecord);
        deleteAction->setEnabled(
            canEditRows && !dataTable_->SelectedRows().isEmpty());
        menu.exec(dataTable_->mapToGlobal(pos));
    }

    void SCDatabaseEditorMainWindow::UndoLastAction()
    {
        QString error;
        if (!session_->Undo(&error))
        {
            ShowError(QStringLiteral("Undo failed"), error);
            return;
        }

        RefreshGridData();
        RefreshCurrentRecordPanel();
        RefreshRelationPanel();
        RefreshOverviewPanels();
        UpdateGridSummary();
    }

    void SCDatabaseEditorMainWindow::RedoLastAction()
    {
        QString error;
        if (!session_->Redo(&error))
        {
            ShowError(QStringLiteral("Redo failed"), error);
            return;
        }

        RefreshGridData();
        RefreshCurrentRecordPanel();
        RefreshRelationPanel();
        RefreshOverviewPanels();
        UpdateGridSummary();
    }

    void SCDatabaseEditorMainWindow::RefreshCurrentView()
    {
        QString error;
        if (!session_->Refresh(&error))
        {
            ShowError(QStringLiteral("Refresh failed"), error);
            return;
        }

        RefreshGridData();
        RefreshCurrentDetailsPanel();
        RefreshCurrentRecordPanel();
        UpdateComputedColumnsPanel();
        RefreshRelationPanel();
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
            ShowError(QStringLiteral("Failed to export debug package"), error);
            return;
        }

        if (bottomTabs_ != nullptr)
        {
            bottomTabs_->setCurrentIndex(kBottomDebugPackageTab);
        }
        if (debugPackageText_ != nullptr)
        {
            debugPackageText_->setPlainText(
                QStringLiteral("Debug package exported:\n") + filePath);
        }
        SetStatusMessage(QStringLiteral("Debug package exported: ") + filePath);
    }

    void SCDatabaseEditorMainWindow::ExportCurrentTableCsv()
    {
        if (!session_->IsOpen() || session_->CurrentTableName().isEmpty())
        {
            ShowError(QStringLiteral("Failed to export CSV"),
                      QStringLiteral("Please select a table first before exporting CSV."));
            return;
        }

        const QString defaultFileName =
            session_->CurrentTableName() + QStringLiteral(".csv");
        const QString filePath = QFileDialog::getSaveFileName(
            this, QStringLiteral("Export CSV"), defaultFileName,
            QStringLiteral("CSV Files (*.csv);;All Files (*)"));
        if (filePath.isEmpty())
        {
            return;
        }

        QString error;
        if (!ExportCurrentTableCsvFile(filePath, &error))
        {
            ShowError(QStringLiteral("Failed to export CSV"), error);
            return;
        }

        SetStatusMessage(QStringLiteral("CSV exported: ") + filePath);
    }

    void SCDatabaseEditorMainWindow::ImportCsvIntoCurrentTable()
    {
        if (!session_->IsOpen() || session_->CurrentTableName().isEmpty())
        {
            ShowError(QStringLiteral("Failed to import CSV"),
                      QStringLiteral("Please select a table first."));
            return;
        }

        sc::SCEditingDatabaseState editingState;
        QString stateError;
        if (!session_->GetEditingState(&editingState, &stateError))
        {
            ShowError(QStringLiteral("Failed to import CSV"), stateError);
            return;
        }
        if (session_->HasPendingEdit())
        {
            ShowError(QStringLiteral("Failed to import CSV"),
                      QStringLiteral(
                          "Please save or discard pending changes before importing CSV."));
            return;
        }
        if (editingState.openMode == sc::SCDatabaseOpenMode::ReadOnly)
        {
            ShowError(
                QStringLiteral("Failed to import CSV"),
                QStringLiteral("CSV import requires a writable database."));
            return;
        }

        const QString defaultFileName =
            session_->CurrentTableName() + QStringLiteral(".csv");
        const QString filePath = QFileDialog::getOpenFileName(
            this, QStringLiteral("Import CSV"), defaultFileName,
            QStringLiteral("CSV Files (*.csv);;All Files (*)"));
        if (filePath.isEmpty())
        {
            return;
        }

        QString error;
        if (!ImportCsvIntoCurrentTableFile(filePath, &error))
        {
            ShowError(QStringLiteral("Failed to import CSV"), error);
            return;
        }

        SetStatusMessage(QStringLiteral("CSV imported: ") + filePath);
    }

    bool SCDatabaseEditorMainWindow::ExportCurrentTableCsvFile(
        const QString& filePath, QString* outError) const
    {
        if (!session_->IsOpen() || session_->CurrentTableName().isEmpty())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("No table selected.");
            }
            return false;
        }

        QSaveFile file(filePath);
        if (!file.open(QIODevice::WriteOnly | QIODevice::Text))
        {
            if (outError != nullptr)
            {
                *outError =
                    QStringLiteral("Unable to open CSV file for writing.");
            }
            return false;
        }

        QStringList headerFields;
        const int columnCount = columns_.size();
        headerFields.reserve(columnCount);
        for (int col = 0; col < columnCount; ++col)
        {
            headerFields.push_back(
                EscapeCsvField(QString::fromStdWString(columns_[col].name)));
        }

        const auto writeLine = [&](const QStringList& fields) -> bool {
            const QString line = fields.join(QStringLiteral(","));
            const QByteArray bytes = line.toUtf8();
            if (file.write(bytes) != bytes.size())
            {
                if (outError != nullptr)
                {
                    *outError = QStringLiteral("Unable to write CSV content.");
                }
                return false;
            }
            if (file.write("\n") != 1)
            {
                if (outError != nullptr)
                {
                    *outError =
                        QStringLiteral("Unable to write CSV newline.");
                }
                return false;
            }
            return true;
        };

        const QByteArray utf8Bom("\xEF\xBB\xBF", 3);
        if (file.write(utf8Bom) != utf8Bom.size())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Unable to write CSV header.");
            }
            return false;
        }

        if (!writeLine(headerFields))
        {
            return false;
        }

        const int rowCount = visibleRows_.size();
        for (int row = 0; row < rowCount; ++row)
        {
            QStringList fields;
            fields.reserve(columnCount);
            for (int col = 0; col < columnCount; ++col)
            {
                QVariant value;
                QString error;
                const bool ok = session_->GetCellDisplayValue(
                    visibleRows_[row].recordId,
                    QString::fromStdWString(columns_[col].name), &value,
                    &error);
                fields.push_back(
                    EscapeCsvField(ok ? value.toString() : QString()));
            }

            if (!writeLine(fields))
            {
                return false;
            }
        }

        if (!file.commit())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Unable to finalize CSV file.");
            }
            return false;
        }

        return true;
    }

    bool SCDatabaseEditorMainWindow::ImportCsvIntoCurrentTableFile(
        const QString& filePath, QString* outError)
    {
        if (!session_->IsOpen() || session_->CurrentTableName().isEmpty())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("No table selected.");
            }
            return false;
        }

        QFile file(filePath);
        if (!file.open(QIODevice::ReadOnly | QIODevice::Text))
        {
            if (outError != nullptr)
            {
                *outError =
                    QStringLiteral("Unable to open CSV file for reading.");
            }
            return false;
        }

        const QString text = QString::fromUtf8(file.readAll());
        QVector<QStringList> csvRows;
        if (!ParseCsvText(text, &csvRows, outError))
        {
            return false;
        }
        if (csvRows.isEmpty())
        {
            if (outError != nullptr)
            {
                *outError =
                    QStringLiteral("CSV file does not contain a header row.");
            }
            return false;
        }

        const QStringList headerRow = csvRows.first();
        csvRows.removeAt(0);
        const QVector<sc::SCTableViewColumnDef>& viewColumns = columns_;

        QVector<int> columnMapping;
        columnMapping.reserve(headerRow.size());
        QVector<bool> columnUsed(viewColumns.size(), false);
        QVector<int> headerIndexByColumn(viewColumns.size(), -1);
        int importableColumnCount = 0;
        for (const QString& rawHeader : headerRow)
        {
            const QString header = rawHeader.trimmed();
            if (header.isEmpty())
            {
                if (outError != nullptr)
                {
                    *outError = QStringLiteral(
                        "CSV header contains an empty column name.");
                }
                return false;
            }

            const int columnIndex = FindColumnIndex(viewColumns, header);
            if (columnIndex < 0)
            {
                if (outError != nullptr)
                {
                    *outError = QStringLiteral(
                                    "CSV header references unknown column: ") +
                                header;
                }
                return false;
            }

            const sc::SCTableViewColumnDef& definition =
                viewColumns[columnIndex];
            if (definition.layer == sc::TableColumnLayer::Fact &&
                definition.editable)
            {
                if (columnUsed[columnIndex])
                {
                    if (outError != nullptr)
                    {
                        *outError = QStringLiteral(
                                        "CSV header contains duplicate editable columns: ") +
                                    header;
                    }
                    return false;
                }
                columnUsed[columnIndex] = true;
                headerIndexByColumn[columnIndex] =
                    static_cast<int>(columnMapping.size());
                ++importableColumnCount;
                columnMapping.push_back(columnIndex);
            } else
            {
                columnMapping.push_back(-1);
            }
        }

        if (importableColumnCount == 0)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral(
                        "CSV file does not contain any editable columns.");
            }
            return false;
        }

        QVector<int> requiredColumns;
        requiredColumns.reserve(viewColumns.size());
        for (int columnIndex = 0; columnIndex < viewColumns.size();
             ++columnIndex)
        {
            const sc::SCTableViewColumnDef& definition =
                viewColumns[columnIndex];
            if (definition.layer != sc::TableColumnLayer::Fact ||
                !definition.editable)
            {
                continue;
            }
            sc::SCColumnDef schemaColumn;
            if (!session_->GetColumnDef(
                    QString::fromStdWString(definition.name), &schemaColumn,
                    outError))
            {
                return false;
            }
            if (!schemaColumn.nullable && schemaColumn.defaultValue.IsNull())
            {
                requiredColumns.push_back(columnIndex);
                if (headerIndexByColumn[columnIndex] < 0)
                {
                    if (outError != nullptr)
                    {
                        *outError =
                            QStringLiteral(
                                "CSV header is missing required column: ") +
                            QString::fromStdWString(definition.name);
                    }
                    return false;
                }
            }
        }

        const QString tableName = session_->CurrentTableName();
        std::vector<sc::SCBatchTableRequest> requests;
        requests.reserve(static_cast<std::size_t>(csvRows.size()));

        for (int rowIndex = 0; rowIndex < csvRows.size(); ++rowIndex)
        {
            const QStringList& row = csvRows[rowIndex];
            if (row.size() > headerRow.size())
            {
                if (outError != nullptr)
                {
                    *outError =
                        QStringLiteral(
                            "CSV row %1 has more fields than the header.")
                            .arg(rowIndex + 2);
                }
                return false;
            }

            sc::SCBatchTableRequest request;
            request.tableName = tableName.toStdWString();

            for (int requiredColumnIndex : requiredColumns)
            {
                const sc::SCTableViewColumnDef& definition =
                    viewColumns[requiredColumnIndex];
                sc::SCColumnDef schemaColumn;
                if (!session_->GetColumnDef(
                        QString::fromStdWString(definition.name), &schemaColumn,
                        outError))
                {
                    return false;
                }
                const int headerIndex =
                    headerIndexByColumn[requiredColumnIndex];
                const QString cellText = headerIndex < row.size()
                                             ? row[headerIndex].trimmed()
                                             : QString();

                if (!IsBlankAllowedForRequiredColumn(schemaColumn.valueKind) &&
                    cellText.isEmpty())
                {
                    if (outError != nullptr)
                    {
                        *outError =
                            QStringLiteral(
                                "CSV row %1 is missing required field \"%2\".")
                                .arg(rowIndex + 2)
                                .arg(QString::fromStdWString(definition.name));
                    }
                    return false;
                }
            }

            sc::SCBatchCreateRecordRequest createRecord;
            for (int headerIndex = 0; headerIndex < headerRow.size();
                 ++headerIndex)
            {
                const int columnIndex = columnMapping[headerIndex];
                if (columnIndex < 0)
                {
                    continue;
                }

                const sc::SCTableViewColumnDef& definition =
                    viewColumns[columnIndex];
                const QString cellText =
                    headerIndex < row.size() ? row[headerIndex] : QString();

                sc::SCValue value;
                if (!CsvTextToValue(definition, cellText, &value, outError))
                {
                    if (outError != nullptr && !outError->isEmpty())
                    {
                        *outError =
                            QStringLiteral("CSV row %1, column \"%2\": %3")
                                .arg(rowIndex + 2)
                                .arg(QString::fromStdWString(definition.name))
                                .arg(*outError);
                    }
                    return false;
                }

                sc::SCFieldValueAssignment assignment;
                assignment.fieldName = definition.name;
                assignment.SCValue = value;
                createRecord.values.push_back(std::move(assignment));
            }

            request.creates.push_back(std::move(createRecord));
            requests.emplace_back(std::move(request));
        }

        if (requests.empty())
        {
            return true;
        }

        sc::SCBatchExecutionOptions options;
        options.editName = L"ImportCSV";
        options.rollbackOnError = true;

        sc::SCBatchExecutionResult result;
        const sc::ErrorCode rc = sc::ExecuteBatchEdit(
            session_->Database(), requests, options, &result);
        if (sc::Failed(rc))
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("CSV import failed: ") +
                            StorageErrorText(rc);
            }
            return false;
        }

        QString refreshError;
        if (!session_->Refresh(&refreshError))
        {
            if (outError != nullptr)
            {
                *outError =
                    QStringLiteral("CSV import committed, but refresh failed: ") +
                            refreshError;
            }
            return false;
        }

        if (outError != nullptr)
        {
            outError->clear();
        }
        return true;
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
                QStringLiteral(
                    "SQL preview is not populated at this stage.\n"
                    "Use schema and record operations to inspect changes."));
        }
        UpdateEditLogPanel();
        UpdateDatabaseStatusBar();
        RefreshWorkspaceHeader();
        RefreshWorkspacePages();
    }

    void SCDatabaseEditorMainWindow::UpdateEditLogPanel()
    {
        if (editStateText_ == nullptr || editLogTree_ == nullptr)
        {
            return;
        }

        sc::SCEditLogState logState;
        sc::SCEditingDatabaseState editingState;
        QString error;
        if (!session_->GetEditingState(&editingState, &error))
        {
            editStateText_->setPlainText(
                QStringLiteral("Failed to load edit state: ") + error);
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
                   (editingState.open ? QStringLiteral("Yes")
                                      : QStringLiteral("No")) +
                   QLatin1Char('\n');
        summary += QStringLiteral("Open Mode: ") +
                   OpenModeToText(editingState.openMode) + QLatin1Char('\n');
        summary += QStringLiteral("Dirty: ") +
                   (editingState.dirty ? QStringLiteral("Yes")
                                       : QStringLiteral("No")) +
                   QLatin1Char('\n');
        summary += QStringLiteral("Current Version: ") +
                   QString::number(
                       static_cast<qulonglong>(editingState.currentVersion)) +
                   QLatin1Char('\n');
        summary += QStringLiteral("Baseline Version: ") +
                   QString::number(
                       static_cast<qulonglong>(editingState.baselineVersion)) +
                   QLatin1Char('\n');
        summary += QStringLiteral("Undo Count: ") +
                   QString::number(editingState.undoCount) + QLatin1Char('\n');
        summary += QStringLiteral("Redo Count: ") +
                   QString::number(editingState.redoCount) + QLatin1Char('\n');
        summary += QStringLiteral("Undo Items: ") +
                   QString::number(logState.undoItems.size()) +
                   QLatin1Char('\n');
        summary += QStringLiteral("Redo Items: ") +
                   QString::number(logState.redoItems.size()) +
                   QLatin1Char('\n');
        editStateText_->setPlainText(summary);

        editLogTree_->clear();

        auto appendItems = [this](const std::vector<sc::SCEditLogEntry>& items) {
            for (const sc::SCEditLogEntry& item : items)
            {
                auto* treeItem = new QTreeWidgetItem(editLogTree_);
                treeItem->setText(0, ToQString(item.displayText));
                treeItem->setText(
                    1, QString::number(
                           static_cast<qulonglong>(item.version)));
                treeItem->setText(
                    2, EditLogActionKindToText(item.kind));
                treeItem->setText(
                    3, QStringLiteral("-"));
                treeItem->setText(
                    4, FormatUtcTimestamp(item.timestampUtcMs));
                treeItem->setText(5, ToQString(item.displayText));
                treeItem->setText(
                    6, ToQString(item.detailText));
            }
        };

        appendItems(logState.undoItems);
        appendItems(logState.redoItems);

        for (int col = 0; col < editLogTree_->columnCount(); ++col)
        {
            editLogTree_->resizeColumnToContents(col);
        }
    }

    void SCDatabaseEditorMainWindow::WireGridCallbacks()
    {
        if (dataTable_ == nullptr)
        {
            return;
        }

        dataTable_->OnCurrentCellChanged = [this](int row, int col, int, int) {
            currentRow_ = row;
            currentColumn_ = col;
            OnGridCellSelected(row, col);
        };

        dataTable_->OnCommitEditText = [this](SCEditorContext& context) {
            const int row = context.Row;
            const int col = context.Column;
            if (row < 0 || col < 0 ||
                row >= visibleRows_.size() ||
                col == 0 ||
                col > columns_.size())
            {
                context.ErrorText = QStringLiteral("Invalid cell");
                return false;
            }

            const sc::RecordId recordId = visibleRows_[row].recordId;
            const int dataColumn = col - 1;
            const QString fieldName =
                ToQString(columns_[dataColumn].name);
            QString error;
            if (!session_->SetCellValue(
                    recordId, fieldName, QVariant(context.NewText), &error))
            {
                context.ErrorText = error;
                dataTable_->Refresh();
                return false;
            }

            RefreshGridData();
            return true;
        };

        dataTable_->OnFilterRow = [this](int row) {
            return RowPassesQuickFilter(row);
        };
    }

    void SCDatabaseEditorMainWindow::RefreshGridData()
    {
        if (dataTable_ == nullptr)
        {
            return;
        }

        if (!session_->IsOpen() || session_->CurrentTableName().isEmpty())
        {
            columns_.clear();
            visibleRows_.clear();
            dataTable_->OnGetRowCount = []() { return 0; };
            dataTable_->OnGetColumnCount = []() { return 0; };
            dataTable_->OnGetHeaderText = [](int) { return QString(); };
            dataTable_->OnGetCellData = [](int, int) {
                SCCellData data;
                return data;
            };
            dataTable_->Refresh();
            UpdateGridSummary();
            return;
        }

        QString error;

        // Populate columns from the computed table view (includes both
        // permanent schema columns and session-level computed columns).
        columns_.clear();
        std::int32_t columnCount = 0;
        const sc::ErrorCode ccRc =
            session_->CurrentTableView()->GetColumnCount(&columnCount);
        if (sc::Failed(ccRc))
        {
            ShowError(QStringLiteral("Failed to load schema"), QString::number(ccRc));
            return;
        }

        for (std::int32_t i = 0; i < columnCount; ++i)
        {
            sc::SCTableViewColumnDef col;
            if (sc::Failed(
                    session_->CurrentTableView()->GetColumn(i, &col)))
            {
                continue;
            }
            columns_.push_back(col);
        }

        sc::SCRecordCursorPtr cursor;
        const sc::ErrorCode rc = session_->CurrentTableView()->EnumerateRecords(cursor);
        if (sc::Failed(rc))
        {
            ShowError(QStringLiteral("Failed to enumerate records"), QString::number(rc));
            return;
        }

        visibleRows_.clear();
        sc::SCRecordPtr record;
        while (cursor->Next(record) == sc::SC_OK && record)
        {
            GridRowData rowData;
            rowData.recordId = record->GetId();
            visibleRows_.push_back(rowData);
        }

        // Build filter text cache for each row (used by RowPassesQuickFilter)
        for (int row = 0; row < static_cast<int>(visibleRows_.size()); ++row)
        {
            QStringList rowTexts;
            rowTexts.push_back(QString::number(
                static_cast<qlonglong>(visibleRows_[row].recordId)));
            for (int col = 0;
                 col < static_cast<int>(columns_.size());
                 ++col)
            {
                QVariant value;
                QString error;
                if (session_->GetCellDisplayValue(
                        visibleRows_[row].recordId,
                        QString::fromStdWString(columns_[col].name),
                        &value, &error))
                {
                    rowTexts << value.toString();
                }
            }
            visibleRows_[row].filterText =
                rowTexts.join(QLatin1Char(' '));
        }

        const int visibleColumnCount =
            static_cast<int>(columns_.size()) + 1;

        dataTable_->OnGetRowCount = [this]() {
            return static_cast<int>(visibleRows_.size());
        };

        dataTable_->OnGetColumnCount = [visibleColumnCount]() {
            return visibleColumnCount;
        };

        dataTable_->OnGetColumnDef = [this](int col) {
            ::SCColumnDef def;
            if (col == 0)
            {
                def.Title = QStringLiteral("RecordId");
                def.Editable = false;
                def.Width = 110;
                return def;
            }

            const int dataColumn = col - 1;
            if (dataColumn >= 0 && dataColumn < static_cast<int>(columns_.size()))
            {
                def.Title = ToQString(columns_[dataColumn].displayName);
                def.Editable = columns_[dataColumn].editable;
                def.Width = 120;
            }
            return def;
        };

        dataTable_->OnGetHeaderText = [this](int col) {
            if (col == 0)
            {
                return QStringLiteral("RecordId");
            }

            const int dataColumn = col - 1;
            if (dataColumn >= 0 && dataColumn < static_cast<int>(columns_.size()))
            {
                return ToQString(columns_[dataColumn].displayName);
            }
            return QString();
        };

        dataTable_->OnGetCellData = [this](int row, int col) {
            SCCellData data;
            if (row < 0 || row >= static_cast<int>(visibleRows_.size()) ||
                col < 0 || col > static_cast<int>(columns_.size()))
            {
                return data;
            }

            if (col == 0)
            {
                const auto recordId = visibleRows_[row].recordId;
                data.ValueType = SCCellValueType::Text;
                data.Text = QString::number(static_cast<qlonglong>(recordId));
                data.DisplayText = data.Text;
                data.ReadOnly = true;
                return data;
            }

            const int dataColumn = col - 1;
            QVariant value;
            QString error;
            const bool ok = session_->GetCellDisplayValue(
                visibleRows_[row].recordId,
                QString::fromStdWString(columns_[dataColumn].name),
                &value, &error);

            if (ok)
            {
                data.ValueType = SCCellValueType::Text;
                data.Text = value.toString();
                data.DisplayText = value.toString();
                data.ReadOnly = !columns_[dataColumn].editable;
            }

            return data;
        };

        dataTable_->Refresh();
        UpdateGridSummary();
        update();
    }

    void SCDatabaseEditorMainWindow::RefreshObjectExplorer()
    {
        if (objectTree_ == nullptr)
        {
            return;
        }

        const QSignalBlocker blocker(objectTree_);
        objectTree_->clear();

        auto* databaseItem =
            new QTreeWidgetItem(objectTree_);
        databaseItem->setText(0, QStringLiteral("Database"));
        databaseItem->setData(0, kExplorerNodeTypeRole,
                              static_cast<int>(
                                  ExplorerNodeType::Database));

        if (!session_->IsOpen())
        {
            objectTree_->expandAll();
            return;
        }

        auto* tablesRoot =
            new QTreeWidgetItem(databaseItem);
        tablesRoot->setText(0, QStringLiteral("Tables"));
        tablesRoot->setData(0, kExplorerNodeTypeRole,
                            static_cast<int>(
                                ExplorerNodeType::TablesRoot));

        const QStringList tableNames = session_->TableNames();
        for (const QString& name : tableNames)
        {
            auto* tableItem =
                new QTreeWidgetItem(tablesRoot);
            tableItem->setText(0, name);
            tableItem->setText(
                1, ExplorerNodeTypeToText(ExplorerNodeType::Table));
            tableItem->setData(
                0, kExplorerNodeTypeRole,
                static_cast<int>(ExplorerNodeType::Table));
            tableItem->setData(
                0, kExplorerNodeNameRole, name);

            auto* columnsItem = new QTreeWidgetItem(tableItem);
            columnsItem->setText(0, QStringLiteral("Columns"));
            columnsItem->setText(1, QStringLiteral("Design"));
            columnsItem->setData(
                0, kExplorerNodeTypeRole,
                static_cast<int>(ExplorerNodeType::TableColumns));
            columnsItem->setData(0, kExplorerNodeNameRole, name);

            auto* constraintsItem = new QTreeWidgetItem(tableItem);
            constraintsItem->setText(0, QStringLiteral("Constraints"));
            constraintsItem->setText(1, QStringLiteral("Design"));
            constraintsItem->setData(
                0, kExplorerNodeTypeRole,
                static_cast<int>(ExplorerNodeType::TableConstraints));
            constraintsItem->setData(0, kExplorerNodeNameRole, name);

            auto* indexesItem = new QTreeWidgetItem(tableItem);
            indexesItem->setText(0, QStringLiteral("Indexes"));
            indexesItem->setText(1, QStringLiteral("Design"));
            indexesItem->setData(
                0, kExplorerNodeTypeRole,
                static_cast<int>(ExplorerNodeType::TableIndexes));
            indexesItem->setData(0, kExplorerNodeNameRole, name);

            auto* recordsItem = new QTreeWidgetItem(tableItem);
            recordsItem->setText(0, QStringLiteral("Records"));
            recordsItem->setText(1, QStringLiteral("Data"));
            recordsItem->setData(
                0, kExplorerNodeTypeRole,
                static_cast<int>(ExplorerNodeType::TableRecords));
            recordsItem->setData(0, kExplorerNodeNameRole, name);

            if (!session_->CurrentTableName().isEmpty() &&
                name.compare(session_->CurrentTableName(),
                             Qt::CaseInsensitive) == 0)
            {
                objectTree_->setCurrentItem(tableItem);
            }
        }

        const QVector<sc::SCComputedColumnDef> computedColumns =
            session_->CurrentSessionComputedColumns();

        auto* computedRoot =
            new QTreeWidgetItem(databaseItem);
        computedRoot->setText(
            0,
            QStringLiteral("Computed Columns (%1)").arg(computedColumns.size()));
        computedRoot->setData(0, kExplorerNodeTypeRole,
                              static_cast<int>(
                                  ExplorerNodeType::ComputedRoot));

        for (const sc::SCComputedColumnDef& col : computedColumns)
        {
            auto* colItem =
                new QTreeWidgetItem(computedRoot);
            colItem->setText(0, ToQString(col.displayName));
            colItem->setText(
                1,
                ExplorerNodeTypeToText(
                    ExplorerNodeType::ComputedColumn));
            colItem->setData(
                0, kExplorerNodeTypeRole,
                static_cast<int>(
                    ExplorerNodeType::ComputedColumn));
            colItem->setData(
                0, kExplorerNodeNameRole,
                ToQString(col.name));
        }

        auto* systemRoot =
            new QTreeWidgetItem(objectTree_);
        systemRoot->setText(0, QStringLiteral("Activity"));
        systemRoot->setData(0, kExplorerNodeTypeRole,
                            static_cast<int>(
                                ExplorerNodeType::ActivityRoot));

        auto* editLogItem =
            new QTreeWidgetItem(systemRoot);
        editLogItem->setText(0, QStringLiteral("Edit Log"));
        editLogItem->setText(
            1,
            ExplorerNodeTypeToText(ExplorerNodeType::EditLog));
        editLogItem->setData(0, kExplorerNodeTypeRole,
                             static_cast<int>(
                                 ExplorerNodeType::EditLog));

        auto* journalItem =
            new QTreeWidgetItem(systemRoot);
        journalItem->setText(0, QStringLiteral("Journal"));
        journalItem->setText(
            1,
            ExplorerNodeTypeToText(ExplorerNodeType::Journal));
        journalItem->setData(0, kExplorerNodeTypeRole,
                             static_cast<int>(
                                 ExplorerNodeType::Journal));

        auto* snapshotsItem =
            new QTreeWidgetItem(systemRoot);
        snapshotsItem->setText(0, QStringLiteral("Snapshots"));
        snapshotsItem->setText(
            1,
            ExplorerNodeTypeToText(
                ExplorerNodeType::Snapshots));
        snapshotsItem->setData(0, kExplorerNodeTypeRole,
                               static_cast<int>(
                                   ExplorerNodeType::Snapshots));

        auto* diagnosticsItem =
            new QTreeWidgetItem(systemRoot);
        diagnosticsItem->setText(0, QStringLiteral("Diagnostics"));
        diagnosticsItem->setText(
            1,
            ExplorerNodeTypeToText(
                ExplorerNodeType::Diagnostics));
        diagnosticsItem->setData(0, kExplorerNodeTypeRole,
                                 static_cast<int>(
                                     ExplorerNodeType::Diagnostics));

        objectTree_->expandAll();
    }

    void SCDatabaseEditorMainWindow::OnTableSelectionChanged()
    {
        if (objectTree_ == nullptr)
        {
            return;
        }

        QTreeWidgetItem* current = objectTree_->currentItem();
        if (current == nullptr)
        {
            return;
        }

        const ExplorerNodeType nodeType =
            static_cast<ExplorerNodeType>(
                current->data(0, kExplorerNodeTypeRole).toInt());
        const QString nodeName =
            current->data(0, kExplorerNodeNameRole).toString();

        switch (nodeType)
        {
            case ExplorerNodeType::Table:
                if (nodeName.compare(session_->CurrentTableName(),
                                     Qt::CaseInsensitive) != 0)
                {
                    QString error;
                    if (!session_->SelectTable(nodeName, &error))
                    {
                        ShowError(QStringLiteral("Failed to select table"), error);
                    }
                }
                if (currentDetailsTabs_ != nullptr && kCurrentDetailsRecordTab >= 0)
                {
                    currentDetailsTabs_->setCurrentIndex(kCurrentDetailsRecordTab);
                }
                SetWorkspaceMode(WorkspaceMode::Data);
                break;
            case ExplorerNodeType::TableColumns:
            case ExplorerNodeType::TableConstraints:
            case ExplorerNodeType::TableIndexes:
            case ExplorerNodeType::TableRecords:
                if (!nodeName.isEmpty() &&
                    nodeName.compare(session_->CurrentTableName(),
                                     Qt::CaseInsensitive) != 0)
                {
                    QString error;
                    if (!session_->SelectTable(nodeName, &error))
                    {
                        ShowError(QStringLiteral("Select table failed"), error);
                        break;
                    }
                }
                if (nodeType == ExplorerNodeType::TableRecords &&
                    currentDetailsTabs_ != nullptr)
                {
                    currentDetailsTabs_->setCurrentIndex(
                        kCurrentDetailsRecordTab);
                }
                SetWorkspaceMode(
                    nodeType == ExplorerNodeType::TableRecords
                        ? WorkspaceMode::Data
                        : WorkspaceMode::Design);
                break;
            case ExplorerNodeType::ComputedColumn:
                if (currentDetailsTabs_ != nullptr)
                {
                    currentDetailsTabs_->setCurrentIndex(kCurrentDetailsComputedTab);
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
                if (currentDetailsTabs_ != nullptr)
                {
                    currentDetailsTabs_->setCurrentIndex(kCurrentDetailsComputedTab);
                }
                break;
            case ExplorerNodeType::TablesRoot:
            case ExplorerNodeType::Database:
            case ExplorerNodeType::ActivityRoot:
            default:
                break;
        }
    }

    void SCDatabaseEditorMainWindow::OnFilterTextChanged(
        const QString& /*text*/)
    {
        if (dataTable_ == nullptr)
        {
            return;
        }

        dataTable_->Refresh();
        UpdateGridSummary();
        update();
    }

    void SCDatabaseEditorMainWindow::OnWorkspaceModeChanged(int index)
    {
        if (index < 0)
        {
            return;
        }

        SetWorkspaceMode(static_cast<WorkspaceMode>(index));
    }

    void SCDatabaseEditorMainWindow::OnGridCellSelected(
        int row, int col)
    {
        Q_UNUSED(row);
        Q_UNUSED(col);
        RefreshCurrentRecordPanel();
        RefreshRelationPanel();
    }

    int SCDatabaseEditorMainWindow::CurrentRow() const
    {
        return currentRow_;
    }

    int SCDatabaseEditorMainWindow::CurrentColumn() const
    {
        return currentColumn_;
    }

    bool SCDatabaseEditorMainWindow::RowPassesQuickFilter(
        int row) const
    {
        if (filterEdit_ == nullptr)
        {
            return true;
        }

        const QString filter = filterEdit_->text().trimmed();
        if (filter.isEmpty())
        {
            return true;
        }

        if (row < 0 || row >= static_cast<int>(visibleRows_.size()))
        {
            return false;
        }

        return visibleRows_[row].filterText.contains(
            filter, Qt::CaseInsensitive);
    }

    sc::RecordId SCDatabaseEditorMainWindow::RecordIdAt(
        int row) const
    {
        if (row < 0 || row >= visibleRows_.size())
        {
            return 0;
        }
        return visibleRows_[row].recordId;
    }

    sc::SCTableViewColumnDef SCDatabaseEditorMainWindow::ColumnAt(
        int col) const
    {
        if (col < 0 || col >= columns_.size())
        {
            return {};
        }
        return columns_[col];
    }

    QString SCDatabaseEditorMainWindow::CurrentDesignColumnName()
        const
    {
        if (tableDesignPane_ == nullptr)
        {
            return {};
        }

        return tableDesignPane_->CurrentColumnName();
    }

    QString SCDatabaseEditorMainWindow::CurrentComputedColumnName()
        const
    {
        if (computedColumnsTree_ == nullptr)
        {
            return {};
        }

        QTreeWidgetItem* item =
            computedColumnsTree_->currentItem();
        if (item == nullptr)
        {
            return {};
        }

        return item->text(0);
    }

    QString SCDatabaseEditorMainWindow::ExplorerSelectedTableName() const
    {
        if (objectTree_ == nullptr || objectTree_->currentItem() == nullptr)
        {
            return {};
        }

        return objectTree_->currentItem()
            ->data(0, kExplorerNodeNameRole)
            .toString();
    }

    void SCDatabaseEditorMainWindow::SelectDesignColumnByName(
        const QString& name)
    {
        if (tableDesignPane_ == nullptr || name.isEmpty())
        {
            return;
        }

        tableDesignPane_->SelectColumnByName(name);
    }

    void SCDatabaseEditorMainWindow::SelectComputedColumnByName(
        const QString& name)
    {
        if (computedColumnsTree_ == nullptr || name.isEmpty())
        {
            return;
        }

        for (int index = 0;
             index < computedColumnsTree_->topLevelItemCount();
             ++index)
        {
            QTreeWidgetItem* item =
                computedColumnsTree_->topLevelItem(index);
            if (item != nullptr &&
                item->text(0)
                        .compare(name, Qt::CaseInsensitive) ==
                    0)
            {
                computedColumnsTree_->setCurrentItem(item);
                return;
            }
        }
    }

    void SCDatabaseEditorMainWindow::SelectTableInExplorer(
        const QString& tableName)
    {
        if (objectTree_ == nullptr || tableName.isEmpty())
        {
            return;
        }

        std::function<QTreeWidgetItem*(QTreeWidgetItem*)> findInBranch =
            [&](QTreeWidgetItem* item) -> QTreeWidgetItem* {
            if (item == nullptr)
            {
                return nullptr;
            }

            const ExplorerNodeType nodeType = static_cast<ExplorerNodeType>(
                item->data(0, kExplorerNodeTypeRole).toInt());
            const QString nodeName =
                item->data(0, kExplorerNodeNameRole).toString();
            if (nodeType == ExplorerNodeType::Table &&
                nodeName.compare(tableName, Qt::CaseInsensitive) == 0)
            {
                return item;
            }

            for (int index = 0; index < item->childCount(); ++index)
            {
                if (QTreeWidgetItem* match = findInBranch(item->child(index)))
                {
                    return match;
                }
            }

            return nullptr;
        };

        for (int index = 0; index < objectTree_->topLevelItemCount(); ++index)
        {
            if (QTreeWidgetItem* match =
                    findInBranch(objectTree_->topLevelItem(index)))
            {
                objectTree_->setCurrentItem(match);
                return;
            }
        }
    }

    void SCDatabaseEditorMainWindow::SetWorkspaceMode(WorkspaceMode mode)
    {
        workspaceMode_ = mode;

        if (workspaceModeBar_ != nullptr &&
            workspaceModeBar_->currentIndex() != static_cast<int>(mode))
        {
            const QSignalBlocker blocker(workspaceModeBar_);
            workspaceModeBar_->setCurrentIndex(static_cast<int>(mode));
        }

        if (workspaceStack_ != nullptr)
        {
            workspaceStack_->setCurrentIndex(static_cast<int>(mode));
        }

        RefreshWorkspaceHeader();
        RefreshWorkspacePages();
    }

    void SCDatabaseEditorMainWindow::RefreshWorkspaceHeader()
    {
        if (workspaceTitleLabel_ != nullptr)
        {
            const QString currentTableName = session_->CurrentTableName();
            workspaceTitleLabel_->setText(
                currentTableName.isEmpty()
                    ? QStringLiteral("Table: -")
                    : QStringLiteral("Table: %1").arg(currentTableName));
        }

        if (workspaceStatsLabel_ != nullptr)
        {
            workspaceStatsLabel_->setText(
                QStringLiteral("Rows: %1 | Columns: %2")
                    .arg(visibleRows_.size())
                    .arg(columns_.size()));
        }

        if (workspaceModeStateLabel_ != nullptr)
        {
            QString modeText = QStringLiteral("Data");
            switch (workspaceMode_)
            {
                case WorkspaceMode::Design:
                    modeText = QStringLiteral("Design");
                    break;
                case WorkspaceMode::SchemaText:
                    modeText = QStringLiteral("Schema Text");
                    break;
                case WorkspaceMode::Data:
                default:
                    break;
            }

            workspaceModeStateLabel_->setText(QStringLiteral("Mode: %1").arg(modeText));
        }
    }

    void SCDatabaseEditorMainWindow::RefreshWorkspacePages()
    {
        if (tableDesignPane_ != nullptr)
        {
            tableDesignPane_->Refresh();
        }
        if (schemaTextPane_ != nullptr)
        {
            schemaTextPane_->Refresh();
        }
    }

    void SCDatabaseEditorMainWindow::RefreshCurrentDetailsPanel()
    {
        Q_UNUSED(session_);
    }

    void SCDatabaseEditorMainWindow::RefreshCurrentRecordPanel()
    {
        if (recordTree_ == nullptr)
        {
            return;
        }

        recordTree_->clear();

        const int row = CurrentRow();
        if (row < 0 || row >= visibleRows_.size())
        {
            return;
        }

        const sc::RecordId recordId =
            visibleRows_[row].recordId;

        auto* idItem = new QTreeWidgetItem(recordTree_);
        idItem->setText(0, QStringLiteral("Record ID"));
        idItem->setText(1, QString::number(recordId));

        for (int col = 0; col < columns_.size(); ++col)
        {
            auto* item =
                new QTreeWidgetItem(recordTree_);
            item->setText(0, ToQString(columns_[col].name));

            QVariant value;
            QString error;
            const bool ok = session_->GetCellDisplayValue(
                recordId,
                QString::fromStdWString(columns_[col].name),
                &value, &error);
            item->setText(1, ok ? value.toString()
                                : QStringLiteral("<Error>"));
        }

        for (int col = 0;
             col < recordTree_->columnCount();
             ++col)
        {
            recordTree_->resizeColumnToContents(col);
        }
    }

    void SCDatabaseEditorMainWindow::UpdateComputedColumnsPanel()
    {
        if (computedColumnsTree_ == nullptr)
        {
            return;
        }

        computedColumnsTree_->clear();

        if (!session_->IsOpen() ||
            session_->CurrentTableName().isEmpty())
        {
            return;
        }

        const QVector<sc::SCComputedColumnDef> computed =
            session_->CurrentSessionComputedColumns();
        for (const sc::SCComputedColumnDef& def : computed)
        {
            auto* item = new QTreeWidgetItem(
                computedColumnsTree_);
            item->setText(0, ToQString(def.name));
            item->setText(
                1, ValueKindToText(def.valueKind));
            item->setText(
                2,
                (def.kind ==
                 sc::ComputedFieldKind::Expression)
                    ? ToQString(def.expression)
                    : QStringLiteral("<Custom>"));
            item->setText(
                3, BoolToText(def.cacheable));
        }

        for (int col = 0;
             col < computedColumnsTree_->columnCount();
             ++col)
        {
            computedColumnsTree_->resizeColumnToContents(
                col);
        }
    }

    void SCDatabaseEditorMainWindow::RefreshRelationPanel()
    {
        if (relationTree_ == nullptr)
        {
            return;
        }

        relationTree_->clear();

        if (!session_->IsOpen() ||
            session_->CurrentTableName().isEmpty())
        {
            return;
        }

        const int row = CurrentRow();
        if (row < 0 || row >= visibleRows_.size())
        {
            return;
        }

        const sc::RecordId recordId =
            visibleRows_[row].recordId;

        for (int col = 0; col < columns_.size(); ++col)
        {
            const sc::SCTableViewColumnDef& colDef =
                columns_[col];
            sc::SCColumnDef schemaColumn;
            QString error;
            if (!session_->GetColumnDef(
                    ToQString(colDef.name), &schemaColumn,
                    &error))
            {
                continue;
            }

            if (schemaColumn.columnKind !=
                sc::ColumnKind::Relation)
            {
                continue;
            }

            auto* item =
                new QTreeWidgetItem(relationTree_);
            item->setText(0, ToQString(colDef.name));
            item->setText(
                1, ToQString(schemaColumn.referenceTable));
            item->setText(
                2,
                ToQString(
                    schemaColumn.referenceStorageColumn));

            QVariant value;
            const bool ok = session_->GetCellDisplayValue(
                recordId,
                QString::fromStdWString(colDef.name),
                &value, &error);
            item->setText(
                3, ok ? value.toString()
                      : QStringLiteral("<Error>"));
        }

        for (int col = 0;
             col < relationTree_->columnCount();
             ++col)
        {
            relationTree_->resizeColumnToContents(col);
        }
    }

    void SCDatabaseEditorMainWindow::UpdateGridSummary()
    {
        sc::SCEditingDatabaseState state;
        const int rowCount =
            static_cast<int>(visibleRows_.size());
        const int colCount =
            (session_->IsOpen() && !session_->CurrentTableName().isEmpty())
                ? static_cast<int>(columns_.size()) + 1
                : 0;

        if (tableStatsLabel_ != nullptr)
        {
            tableStatsLabel_->setText(
                QStringLiteral("Rows: %1  Columns: %2")
                    .arg(rowCount)
                    .arg(colCount));
        }

        if (filterStateLabel_ != nullptr)
        {
            const bool hasFilter =
                filterEdit_ != nullptr &&
                !filterEdit_->text().trimmed().isEmpty();
            filterStateLabel_->setText(
                hasFilter
                    ? QStringLiteral("Filter: On")
                    : QStringLiteral("Filter: Off"));
        }

        if (transactionStateLabel_ != nullptr)
        {
            QString error;
            const bool ok =
                session_->GetEditingState(&state, &error);
            if (ok && state.dirty)
            {
                transactionStateLabel_->setText(
                    QStringLiteral("Transaction: Uncommitted"));
            } else if (ok)
            {
                transactionStateLabel_->setText(
                    QStringLiteral("Transaction: Idle"));
            } else
            {
                transactionStateLabel_->setText(
                    QStringLiteral("Transaction: Unknown"));
            }
        }
    }

    void SCDatabaseEditorMainWindow::UpdateDatabaseStatusBar()
    {
        sc::SCEditingDatabaseState state;
        QString error;
        const bool stateLoaded =
            session_->GetEditingState(&state, &error);

        if (openModeLabel_ != nullptr)
        {
            openModeLabel_->setText(
                QStringLiteral("Mode: %1")
                    .arg(stateLoaded
                             ? OpenModeToText(state.openMode)
                             : QStringLiteral("Closed")));
        }

        if (closeDatabaseAction_ != nullptr)
        {
            closeDatabaseAction_->setEnabled(stateLoaded);
        }

        if (currentTableLabel_ != nullptr)
        {
            const QString name =
                session_->CurrentTableName();
            currentTableLabel_->setText(
                name.isEmpty()
                    ? QStringLiteral("Table: -")
                    : QStringLiteral("Table: ") + name);
        }

        if (savePendingChangesAction_ != nullptr)
        {
            savePendingChangesAction_->setEnabled(
                stateLoaded && session_->HasPendingEdit() &&
                state.openMode != sc::SCDatabaseOpenMode::ReadOnly);
        }

        if (discardPendingChangesAction_ != nullptr)
        {
            discardPendingChangesAction_->setEnabled(
                stateLoaded && session_->HasPendingEdit() &&
                state.openMode != sc::SCDatabaseOpenMode::ReadOnly);
        }

        if (undoAction_ != nullptr)
        {
            undoAction_->setEnabled(
                stateLoaded && state.undoCount > 0 &&
                state.openMode != sc::SCDatabaseOpenMode::ReadOnly);
        }

        if (redoAction_ != nullptr)
        {
            redoAction_->setEnabled(
                stateLoaded && state.redoCount > 0 &&
                state.openMode != sc::SCDatabaseOpenMode::ReadOnly);
        }

        UpdateGridSummary();
    }

    void SCDatabaseEditorMainWindow::ShowError(
        const QString& title, const QString& message)
    {
        SetStatusMessage(title +
                         QStringLiteral(": ") + message);
        QMessageBox::warning(this, title, message);
    }

    void SCDatabaseEditorMainWindow::SetStatusMessage(
        const QString& message)
    {
        if (statusLabel_ != nullptr)
        {
            statusLabel_->setText(message);
        }
        if (statusBar() != nullptr)
        {
            statusBar()->showMessage(message, 5000);
        }
    }

}  // namespace StableCore::Storage::Editor
