#include "SCDatabaseEditorMainWindow.h"

#include <cstdint>
#include <QAction>
#include <QByteArray>
#include <QDateTime>
#include <QFile>
#include <QFileDialog>
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
#include <QToolBar>
#include <QTabWidget>
#include <QVector>
#include <QVBoxLayout>
#include <QWidget>

#include <SCTreeGrid/SCTreeGridCtrl.h>

#include <vector>

#include "SCBatch.h"
#include "SCBinaryUtils.h"
#include "SCSchemaTableImportDialog.h"
#include "SCSchemaTableDialog.h"

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
            SystemRoot,
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
                        return v ? QStringLiteral("是") : QStringLiteral("否");
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

            return QStringLiteral("<值>");
        }

        QString BoolToText(bool value)
        {
            return value ? QStringLiteral("是") : QStringLiteral("否");
        }

        QString OpenModeToText(sc::SCDatabaseOpenMode mode)
        {
            switch (mode)
            {
                case sc::SCDatabaseOpenMode::Normal:
                    return QStringLiteral("普通");
                case sc::SCDatabaseOpenMode::NoHistory:
                    return QStringLiteral("无历史");
                case sc::SCDatabaseOpenMode::ReadOnly:
                    return QStringLiteral("只读");
                default:
                    return QStringLiteral("未知");
            }
        }

        QString ExplorerNodeTypeToText(ExplorerNodeType type)
        {
            switch (type)
            {
                case ExplorerNodeType::Database:
                    return QStringLiteral("数据库");
                case ExplorerNodeType::TablesRoot:
                    return QStringLiteral("表");
                case ExplorerNodeType::Table:
                    return QStringLiteral("表");
                case ExplorerNodeType::ComputedRoot:
                    return QStringLiteral("计算列");
                case ExplorerNodeType::ComputedColumn:
                    return QStringLiteral("计算列");
                case ExplorerNodeType::SystemRoot:
                    return QStringLiteral("系统");
                case ExplorerNodeType::EditLog:
                    return QStringLiteral("编辑日志");
                case ExplorerNodeType::Journal:
                    return QStringLiteral("日志");
                case ExplorerNodeType::Snapshots:
                    return QStringLiteral("快照");
                case ExplorerNodeType::Diagnostics:
                    return QStringLiteral("诊断");
                default:
                    return QStringLiteral("未知");
            }
        }

        QString EditLogActionKindToText(sc::SCEditLogActionKind kind)
        {
            switch (kind)
            {
                case sc::SCEditLogActionKind::Commit:
                    return QStringLiteral("提交");
                case sc::SCEditLogActionKind::Undo:
                    return QStringLiteral("撤销");
                case sc::SCEditLogActionKind::Redo:
                    return QStringLiteral("重做");
                case sc::SCEditLogActionKind::Import:
                    return QStringLiteral("导入");
                case sc::SCEditLogActionKind::RuleWriteback:
                    return QStringLiteral("规则回写");
                case sc::SCEditLogActionKind::SaveBaseline:
                    return QStringLiteral("保存基线");
                default:
                    return QStringLiteral("未知");
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
            return QStringLiteral("存储错误: 0x") +
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
                        QStringLiteral("输出CSV行容器为空。");
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
                                "引号CSV字段后出现意外字符。");
                        }
                        return false;
                    }
                    if (!currentField.isEmpty())
                    {
                        if (outError != nullptr)
                        {
                            *outError = QStringLiteral(
                                "非引号CSV字段内出现意外引号。");
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
                        "CSV输入在引号字段内结束。");
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
                        QStringLiteral("无效整数值: ") + text;
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
                        QStringLiteral("无效小数值: ") + text;
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
                *outError = QStringLiteral("无效布尔值: ") + text;
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
                    *outError = QStringLiteral("输出值为空。");
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

        connect(session_, &SCDatabaseSession::DatabaseOpened, this, [this]() {
            RefreshObjectExplorer();
            UpdateDatabaseStatusBar();
            RefreshOverviewPanels();
            UpdateGridSummary();
            SetStatusMessage(QStringLiteral("数据库已打开。"));
            const QString dbPath = session_->DatabasePath();
            if (!dbPath.isEmpty())
            {
                setWindowTitle(QStringLiteral("数据库编辑器 - %1").arg(dbPath));
            }
        });
        connect(session_, &SCDatabaseSession::TablesChanged, this, [this]() {
            RefreshObjectExplorer();
            UpdateDatabaseStatusBar();
            RefreshOverviewPanels();
            UpdateGridSummary();
        });
        connect(
            session_, &SCDatabaseSession::CurrentTableChanged, this, [this]() {
                RefreshGridData();
                RefreshObjectExplorer();
                UpdateSchemaInspector();
                UpdateRecordInspector();
                UpdateComputedColumnsPanel();
                UpdateRelationInspector();
                UpdateDatabaseStatusBar();
                UpdateGridSummary();
                RefreshOverviewPanels();
                const QString currentTableName = session_->CurrentTableName();
                SetStatusMessage(
                    currentTableName.isEmpty()
                        ? QStringLiteral("表选择已清除。")
                        : QStringLiteral("已选择表: ") +
                              currentTableName);
            });
        connect(session_, &SCDatabaseSession::RecordsChanged, this, [this]() {
            UpdateGridSummary();
            UpdateRecordInspector();
            UpdateRelationInspector();
            RefreshOverviewPanels();
            UpdateDatabaseStatusBar();
        });
    }

    void SCDatabaseEditorMainWindow::BuildUi()
    {
        setWindowTitle(QStringLiteral("数据库编辑器"));
        resize(1540, 920);

        auto* centralWidget = new QWidget(this);
        auto* centralLayout = new QVBoxLayout(centralWidget);
        centralLayout->setContentsMargins(0, 0, 0, 0);
        centralLayout->setSpacing(6);

        // 数据库状态标签将添加到底部状态栏，不在中央区域显示
        tablePage_ = new QWidget(centralWidget);
        auto* tableLayout = new QVBoxLayout(tablePage_);
        tableLayout->setContentsMargins(0, 0, 0, 0);
        tableLayout->setSpacing(6);

        // tableTitleLabel_ 已移除，表信息显示在状态栏的 currentTableLabel_ 中

        auto* filterLayout = new QHBoxLayout();
        filterEdit_ = new QLineEdit(tablePage_);
        filterEdit_->setPlaceholderText(QStringLiteral("筛选当前表"));
        auto* clearFilterButton =
            new QPushButton(QStringLiteral("清除"), tablePage_);
        filterLayout->addWidget(
            new QLabel(QStringLiteral("筛选:"), tablePage_));
        filterLayout->addWidget(filterEdit_, 1);
        filterLayout->addWidget(clearFilterButton);
        tableLayout->addLayout(filterLayout);

        dataTable_ = new SCTreeGrid(tablePage_);
        dataTable_->SetSelectionMode(SCSelectionMode::Row);
        dataTable_->SetEnterKeyBehavior(SCEnterKeyBehavior::BeginEdit);
        dataTable_->setContextMenuPolicy(Qt::CustomContextMenu);
        tableLayout->addWidget(dataTable_, 1);

        centralLayout->addWidget(tablePage_, 1);
        setCentralWidget(centralWidget);

        connect(filterEdit_, &QLineEdit::textChanged, this,
                &SCDatabaseEditorMainWindow::OnFilterTextChanged);
        connect(clearFilterButton, &QPushButton::clicked, filterEdit_,
                &QLineEdit::clear);
        connect(dataTable_, &QWidget::customContextMenuRequested, this,
                &SCDatabaseEditorMainWindow::OnGridContextMenuRequested);

        objectExplorerDock_ =
            new QDockWidget(QStringLiteral("对象资源管理器"), this);
        objectExplorerDock_->setObjectName(
            QStringLiteral("objectExplorerDock"));
        objectExplorerDock_->setAllowedAreas(Qt::LeftDockWidgetArea |
                                             Qt::RightDockWidgetArea);
        objectTree_ = new QTreeWidget(objectExplorerDock_);
        objectTree_->setHeaderLabels(
            {QStringLiteral("对象"), QStringLiteral("类型")});
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

        inspectorDock_ = new QDockWidget(QStringLiteral("检查器"), this);
        inspectorDock_->setObjectName(QStringLiteral("inspectorDock"));
        inspectorDock_->setAllowedAreas(Qt::RightDockWidgetArea);
        inspectorTabs_ = new QTabWidget(inspectorDock_);

        schemaTree_ = new QTreeWidget(inspectorTabs_);
        schemaTree_->setHeaderLabels(
            {QStringLiteral("字段名"), QStringLiteral("显示名"),
             QStringLiteral("类型"), QStringLiteral("可为空"),
             QStringLiteral("默认值"), QStringLiteral("引用表"),
             QStringLiteral("用户定义"), QStringLiteral("计算列")});
        schemaTree_->setContextMenuPolicy(Qt::CustomContextMenu);
        inspectorTabs_->addTab(schemaTree_, QStringLiteral("模式"));

        recordTree_ = new QTreeWidget(inspectorTabs_);
        recordTree_->setHeaderLabels(
            {QStringLiteral("字段"), QStringLiteral("值")});
        inspectorTabs_->addTab(recordTree_, QStringLiteral("当前记录"));

        computedColumnsTree_ = new QTreeWidget(inspectorTabs_);
        computedColumnsTree_->setHeaderLabels(
            {QStringLiteral("名称"), QStringLiteral("类型"),
             QStringLiteral("表达式"), QStringLiteral("可缓存")});
        inspectorTabs_->addTab(computedColumnsTree_,
                               QStringLiteral("计算列"));

        relationTree_ = new QTreeWidget(inspectorTabs_);
        relationTree_->setHeaderLabels(
            {QStringLiteral("字段"), QStringLiteral("目标表"),
             QStringLiteral("目标字段"), QStringLiteral("状态")});
        inspectorTabs_->addTab(relationTree_, QStringLiteral("关联"));

        connect(schemaTree_, &QWidget::customContextMenuRequested, this,
                &SCDatabaseEditorMainWindow::OnSchemaContextMenuRequested);

        inspectorDock_->setWidget(inspectorTabs_);
        addDockWidget(Qt::RightDockWidgetArea, inspectorDock_);

        bottomDock_ = new QDockWidget(QStringLiteral("底部面板"), this);
        bottomDock_->setObjectName(QStringLiteral("bottomDock"));
        bottomDock_->setAllowedAreas(Qt::BottomDockWidgetArea);
        bottomTabs_ = new QTabWidget(bottomDock_);

        diagnosticsText_ = new QPlainTextEdit(bottomTabs_);
        diagnosticsText_->setReadOnly(true);
        diagnosticsText_->setPlainText(QStringLiteral("未打开数据库。"));
        bottomTabs_->addTab(diagnosticsText_, QStringLiteral("诊断"));

        auto* editLogWidget = new QWidget(bottomTabs_);
        auto* editLogLayout = new QVBoxLayout(editLogWidget);
        editLogLayout->setContentsMargins(6, 6, 6, 6);
        editLogLayout->setSpacing(6);
        editStateText_ = new QPlainTextEdit(editLogWidget);
        editStateText_->setReadOnly(true);
        editStateText_->setMinimumHeight(110);
        editStateText_->setPlainText(QStringLiteral("未打开数据库。"));
        editLogLayout->addWidget(
            new QLabel(QStringLiteral("状态摘要"), editLogWidget));
        editLogLayout->addWidget(editStateText_);
        editLogTree_ = new QTreeWidget(editLogWidget);
        editLogTree_->setHeaderLabels(
            {QStringLiteral("操作"), QStringLiteral("版本"),
             QStringLiteral("动作"), QStringLiteral("提交"),
             QStringLiteral("时间戳"), QStringLiteral("文本"),
             QStringLiteral("详情")});
        editLogLayout->addWidget(
            new QLabel(QStringLiteral("编辑日志项"), editLogWidget));
        editLogLayout->addWidget(editLogTree_, 1);
        bottomTabs_->addTab(editLogWidget, QStringLiteral("编辑日志"));

        healthSummaryText_ = new QPlainTextEdit(bottomTabs_);
        healthSummaryText_->setReadOnly(true);
        healthSummaryText_->setPlainText(QStringLiteral("未打开数据库。"));
        bottomTabs_->addTab(healthSummaryText_,
                            QStringLiteral("健康摘要"));

        sqlPreviewText_ = new QPlainTextEdit(bottomTabs_);
        sqlPreviewText_->setReadOnly(true);
        sqlPreviewText_->setPlainText(QStringLiteral(
            "SQL 预览在打开表之前不可用。"));
        bottomTabs_->addTab(sqlPreviewText_, QStringLiteral("SQL 预览"));

        debugPackageText_ = new QPlainTextEdit(bottomTabs_);
        debugPackageText_->setReadOnly(true);
        debugPackageText_->setPlainText(
            QStringLiteral("未导出调试包。"));
        bottomTabs_->addTab(debugPackageText_, QStringLiteral("调试包"));

        bottomDock_->setWidget(bottomTabs_);
        addDockWidget(Qt::BottomDockWidgetArea, bottomDock_);

        // 数据库状态标签 - 添加到底部状态栏
        openModeLabel_ = new QLabel(QStringLiteral("模式: 已关闭"), this);
        currentTableLabel_ = new QLabel(QStringLiteral("表: -"), this);
        tableStatsLabel_ = new QLabel(QStringLiteral("记录: 0"), this);
        filterStateLabel_ = new QLabel(QStringLiteral("筛选: 关闭"), this);
        transactionStateLabel_ = new QLabel(QStringLiteral("事务: 空闲"), this);

        // 添加分隔符
        auto* spacer1 = new QWidget(this);
        spacer1->setFixedWidth(12);
        auto* spacer2 = new QWidget(this);
        spacer2->setFixedWidth(12);
        auto* spacer3 = new QWidget(this);
        spacer3->setFixedWidth(12);
        auto* spacer4 = new QWidget(this);
        spacer4->setFixedWidth(12);

        // 添加到状态栏（左侧）
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

        statusLabel_ = new QLabel(QStringLiteral("就绪。"), this);
        statusBar()->addPermanentWidget(statusLabel_, 1);
    }

    void SCDatabaseEditorMainWindow::BuildMenus()
    {
        auto* fileMenu = menuBar()->addMenu(QStringLiteral("文件(&F)"));
        fileMenu->addAction(QStringLiteral("新建数据库..."), this,
                            &SCDatabaseEditorMainWindow::CreateDatabase);
        fileMenu->addAction(QStringLiteral("打开数据库..."), this,
                            &SCDatabaseEditorMainWindow::OpenDatabase);
        fileMenu->addAction(QStringLiteral("创建备份副本..."), this,
                            &SCDatabaseEditorMainWindow::CreateBackupCopy);
        fileMenu->addAction(QStringLiteral("导出调试包..."), this,
                            &SCDatabaseEditorMainWindow::ExportDebugPackage);

        auto* editMenu = menuBar()->addMenu(QStringLiteral("编辑(&E)"));
        editMenu->addAction(QStringLiteral("撤销"), this,
                            &SCDatabaseEditorMainWindow::UndoLastAction);
        editMenu->addAction(QStringLiteral("重做"), this,
                            &SCDatabaseEditorMainWindow::RedoLastAction);
        editMenu->addSeparator();
        savePendingChangesAction_ = editMenu->addAction(
            QStringLiteral("保存待更改"), this,
            &SCDatabaseEditorMainWindow::SavePendingChanges);
        discardPendingChangesAction_ = editMenu->addAction(
            QStringLiteral("放弃待更改"), this,
            &SCDatabaseEditorMainWindow::DiscardPendingChanges);
        editMenu->addAction(QStringLiteral("刷新"), this,
                            &SCDatabaseEditorMainWindow::RefreshCurrentView);

        auto* tableMenu = menuBar()->addMenu(QStringLiteral("表(&T)"));
        tableMenu->addAction(QStringLiteral("创建表..."), this,
                             &SCDatabaseEditorMainWindow::CreateTable);
        tableMenu->addAction(
            QStringLiteral("从模式创建表..."), this,
            &SCDatabaseEditorMainWindow::CreateTableFromSchemaDescription);
        tableMenu->addAction(QStringLiteral("删除选中的表"), this,
                             &SCDatabaseEditorMainWindow::DeleteSelectedTable);
        tableMenu->addAction(
            QStringLiteral("表结构..."), this,
            &SCDatabaseEditorMainWindow::OpenSchemaTableConverter);
        tableMenu->addAction(QStringLiteral("添加列..."), this,
                             &SCDatabaseEditorMainWindow::AddColumn);
        tableMenu->addAction(QStringLiteral("编辑选中的列..."), this,
                             &SCDatabaseEditorMainWindow::EditSelectedColumn);
        tableMenu->addAction(QStringLiteral("添加记录"), this,
                             &SCDatabaseEditorMainWindow::AddRecord);
        tableMenu->addAction(QStringLiteral("删除选中的记录"), this,
                             &SCDatabaseEditorMainWindow::DeleteSelectedRecord);
        tableMenu->addAction(QStringLiteral("选择关联..."), this,
                             &SCDatabaseEditorMainWindow::EditSelectedRelation);
        tableMenu->addSeparator();
        tableMenu->addAction(
            QStringLiteral("导出 CSV..."), this,
            &SCDatabaseEditorMainWindow::ExportCurrentTableCsv);
        tableMenu->addAction(
            QStringLiteral("导入 CSV..."), this,
            &SCDatabaseEditorMainWindow::ImportCsvIntoCurrentTable);

        auto* computedMenu =
            menuBar()->addMenu(QStringLiteral("计算列(&C)"));
        computedMenu->addAction(
            QStringLiteral("添加会话计算列..."), this,
            &SCDatabaseEditorMainWindow::AddSessionComputedColumn);
        computedMenu->addAction(
            QStringLiteral("编辑选中的计算列..."), this,
            &SCDatabaseEditorMainWindow::EditSelectedComputedColumn);
        computedMenu->addAction(
            QStringLiteral("将选中列转换为计算列..."), this,
            &SCDatabaseEditorMainWindow::ConvertSelectedColumnToComputed);
        computedMenu->addAction(
            QStringLiteral("将选中计算列转换为列..."),
            this, &SCDatabaseEditorMainWindow::ConvertSelectedComputedToColumn);
        computedMenu->addAction(
            QStringLiteral("删除选中的计算列"), this,
            &SCDatabaseEditorMainWindow::DeleteSelectedComputedColumn);

        auto* toolsMenu = menuBar()->addMenu(QStringLiteral("工具(&T)"));
        toolsMenu->addAction(QStringLiteral("健康检查"), this,
                             &SCDatabaseEditorMainWindow::ShowHealthSummary);
        toolsMenu->addAction(QStringLiteral("显示编辑日志/状态摘要"),
                             this,
                             &SCDatabaseEditorMainWindow::ShowEditLogSummary);
        toolsMenu->addAction(QStringLiteral("导出调试包..."), this,
                             &SCDatabaseEditorMainWindow::ExportDebugPackage);

        auto* viewMenu = menuBar()->addMenu(QStringLiteral("查看(&V)"));
        viewMenu->addAction(objectExplorerDock_->toggleViewAction());
        viewMenu->addAction(inspectorDock_->toggleViewAction());
        viewMenu->addAction(bottomDock_->toggleViewAction());
        viewMenu->addSeparator();
        viewMenu->addAction(QStringLiteral("重置布局"), this, [this]() {
            objectExplorerDock_->show();
            inspectorDock_->show();
            bottomDock_->show();
            objectExplorerDock_->raise();
            inspectorDock_->raise();
            bottomDock_->raise();
        });

        auto* toolbar = addToolBar(QStringLiteral("主工具栏"));
        toolbar->addAction(QStringLiteral("打开"), this,
                           &SCDatabaseEditorMainWindow::OpenDatabase);
        toolbar->addAction(QStringLiteral("新建数据库"), this,
                           &SCDatabaseEditorMainWindow::CreateDatabase);
        closeDatabaseAction_ =
            toolbar->addAction(QStringLiteral("关闭"), this,
                               &SCDatabaseEditorMainWindow::CloseDatabase);
        toolbar->addAction(QStringLiteral("备份副本"), this,
                           &SCDatabaseEditorMainWindow::CreateBackupCopy);
        toolbar->addAction(QStringLiteral("刷新"), this,
                           &SCDatabaseEditorMainWindow::RefreshCurrentView);
        toolbar->addSeparator();
        undoAction_ = toolbar->addAction(QStringLiteral("撤销"), this,
                           &SCDatabaseEditorMainWindow::UndoLastAction);
        redoAction_ = toolbar->addAction(QStringLiteral("重做"), this,
                           &SCDatabaseEditorMainWindow::RedoLastAction);
        toolbar->addSeparator();
        toolbar->addAction(QStringLiteral("健康检查"), this,
                           &SCDatabaseEditorMainWindow::ShowHealthSummary);
        toolbar->addAction(QStringLiteral("调试包"), this,
                           &SCDatabaseEditorMainWindow::ExportDebugPackage);

        // 表操作工具栏 - 与主工具栏并列
        tableToolBar_ = addToolBar(QStringLiteral("表操作"));
        tableToolBar_->addAction(QStringLiteral("添加记录"), this,
                                 &SCDatabaseEditorMainWindow::AddRecord);
        tableToolBar_->addAction(
            QStringLiteral("删除记录"), this,
            &SCDatabaseEditorMainWindow::DeleteSelectedRecord);
        tableToolBar_->addSeparator();
        tableToolBar_->addAction(
            QStringLiteral("导出 CSV..."), this,
            &SCDatabaseEditorMainWindow::ExportCurrentTableCsv);
        tableToolBar_->addAction(
            QStringLiteral("导入 CSV..."), this,
            &SCDatabaseEditorMainWindow::ImportCsvIntoCurrentTable);
        tableToolBar_->addSeparator();
        tableToolBar_->addAction(QStringLiteral("添加列"), this,
                                 &SCDatabaseEditorMainWindow::AddColumn);
        tableToolBar_->addAction(
            QStringLiteral("编辑列"), this,
            &SCDatabaseEditorMainWindow::EditSelectedColumn);
        tableToolBar_->addAction(
            QStringLiteral("选择关联"), this,
            &SCDatabaseEditorMainWindow::EditSelectedRelation);
        tableToolBar_->addSeparator();
        tableToolBar_->addAction(
            QStringLiteral("保存待更改"), this,
            &SCDatabaseEditorMainWindow::SavePendingChanges);
        tableToolBar_->addAction(
            QStringLiteral("放弃待更改"), this,
            &SCDatabaseEditorMainWindow::DiscardPendingChanges);
        tableToolBar_->addSeparator();
        tableToolBar_->addAction(
            QStringLiteral("添加计算列"), this,
            &SCDatabaseEditorMainWindow::AddSessionComputedColumn);
        tableToolBar_->addAction(
            QStringLiteral("编辑计算列"), this,
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
            this, QStringLiteral("创建数据库"),
            QStringLiteral("待更改将被丢弃。继续？"));
            if (answer != QMessageBox::Yes)
            {
                return;
            }
        }

        const QString filePath = QFileDialog::getSaveFileName(
            this, QStringLiteral("创建数据库"), QString(),
            QStringLiteral("SQLite数据库 (*.sqlite);;所有文件 (*)"));
        if (filePath.isEmpty())
        {
            return;
        }

        QString error;
        if (!session_->CreateDatabase(filePath, &error))
        {
            ShowError(QStringLiteral("创建数据库失败"), error);
        }
    }

    void SCDatabaseEditorMainWindow::OpenDatabase()
    {
        if (session_->IsOpen() && session_->HasPendingEdit())
        {
            const QMessageBox::StandardButton answer = QMessageBox::question(
            this, QStringLiteral("打开数据库"),
            QStringLiteral("待更改将被丢弃。继续？"));
            if (answer != QMessageBox::Yes)
            {
                return;
            }
        }

        const QString filePath = QFileDialog::getOpenFileName(
            this, QStringLiteral("打开数据库"), QString(),
            QStringLiteral("SQLite数据库 (*.sqlite *.db);;所有文件 (*)"));
        if (filePath.isEmpty())
        {
            return;
        }

        QString error;
        if (!session_->OpenDatabase(filePath, &error))
        {
            ShowError(QStringLiteral("打开数据库失败"), error);
        }
    }

    void SCDatabaseEditorMainWindow::CloseDatabase()
    {
        if (!session_->IsOpen())
        {
            SetStatusMessage(QStringLiteral("未打开数据库。"));
            return;
        }

        sc::SCEditingDatabaseState editingState;
        QString stateError;
        const bool stateLoaded =
            session_->GetEditingState(&editingState, &stateError);
        if (!stateLoaded && !stateError.isEmpty())
        {
            ShowError(QStringLiteral("关闭数据库失败"), stateError);
            return;
        }
        const bool hasPendingChanges = session_->HasPendingEdit();

        const QMessageBox::StandardButton answer = QMessageBox::question(
            this, QStringLiteral("关闭数据库"),
            hasPendingChanges
                ? QStringLiteral(
                      "关闭当前数据库？待更改将被丢弃。")
                : QStringLiteral("关闭当前数据库？"));
        if (answer != QMessageBox::Yes)
        {
            return;
        }

        QString error;
        if (!session_->CloseDatabase(&error))
        {
            ShowError(QStringLiteral("关闭数据库失败"), error);
            return;
        }

        RefreshGridData();
        RefreshObjectExplorer();
        UpdateSchemaInspector();
        UpdateRecordInspector();
        UpdateComputedColumnsPanel();
        UpdateRelationInspector();
        UpdateDatabaseStatusBar();
        RefreshOverviewPanels();
        UpdateGridSummary();
        setWindowTitle(QStringLiteral("数据库编辑器"));
        SetStatusMessage(QStringLiteral("数据库已关闭。"));
    }

    void SCDatabaseEditorMainWindow::CreateBackupCopy()
    {
        if (!session_->IsOpen())
        {
            ShowError(QStringLiteral("创建备份副本失败"),
                      QStringLiteral("未打开数据库。"));
            return;
        }

        const QString defaultPath =
            session_->DatabasePath().isEmpty()
                ? QString()
                : session_->DatabasePath() + QStringLiteral("_backup.sqlite");
        const QString filePath = QFileDialog::getSaveFileName(
            this, QStringLiteral("创建备份副本"), defaultPath,
            QStringLiteral("SQLite数据库 (*.sqlite);;所有文件 (*)"));
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
            ShowError(QStringLiteral("创建备份副本失败"), error);
            return;
        }

        SetStatusMessage(QStringLiteral("备份副本已创建: ") + filePath);
    }

    void SCDatabaseEditorMainWindow::OpenSchemaTableConverter()
    {
        if (session_ == nullptr || !session_->IsOpen() ||
            session_->CurrentTableName().isEmpty())
        {
            ShowError(QStringLiteral("表结构"),
                      QStringLiteral("请先选择当前表。"));
            return;
        }

        SCSchemaTableDialog dialog(session_, this);
        dialog.exec();
    }

    void SCDatabaseEditorMainWindow::CreateTable()
    {
        bool accepted = false;
        const QString tableName = QInputDialog::getText(
            this, QStringLiteral("创建表"), QStringLiteral("表名"),
            QLineEdit::Normal, QString(), &accepted);
        if (!accepted || tableName.trimmed().isEmpty())
        {
            return;
        }

        QString error;
        if (!session_->CreateTable(tableName.trimmed(), &error))
        {
            ShowError(QStringLiteral("创建表失败"), error);
        }
    }

    void SCDatabaseEditorMainWindow::CreateTableFromSchemaDescription()
    {
        if (session_ == nullptr || !session_->IsOpen())
        {
            ShowError(QStringLiteral("从模式创建表失败"),
                      QStringLiteral("未打开数据库。"));
            return;
        }

        SCSchemaTableImportDialog dialog(session_, this);
        dialog.exec();
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
            if (nodeType == ExplorerNodeType::Table)
            {
                QString error;
                const QString nodeName = objectTree_->currentItem()
                                             ->data(0, kExplorerNodeNameRole)
                                             .toString();
                if (!session_->SelectTable(nodeName, &error))
                {
                    ShowError(QStringLiteral("添加列失败"), error);
                    return;
                }
            }
        }

        if (session_->CurrentTableName().isEmpty())
        {
            ShowError(QStringLiteral("添加列失败"),
                      QStringLiteral("请先选择表。"));
            return;
        }

        sc::SCEditingDatabaseState editingState;
        QString editingError;
        if (!session_->GetEditingState(&editingState, &editingError))
        {
            ShowError(QStringLiteral("添加列失败"), editingError);
            return;
        }
        if (session_->HasPendingEdit())
        {
            ShowError(QStringLiteral("添加列失败"),
                      QStringLiteral(
                          "请先保存或放弃待更改再修改模式。"));
            return;
        }

        SCAddColumnDialog dialog(session_, this);
        bool tableHasRecords = false;
        QString stateError;
        if (!session_->CurrentTableHasRecords(&tableHasRecords, &stateError))
        {
            ShowError(QStringLiteral("添加列失败"), stateError);
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
            ShowError(QStringLiteral("添加列失败"), error);
            return;
        }

        UpdateSchemaInspector();
        SelectSchemaColumnByName(ToQString(column.name));
        RefreshGridData();
        UpdateRelationInspector();
        RefreshOverviewPanels();
        SetStatusMessage(QStringLiteral("列已添加: ") +
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
                QStringLiteral("选择表"), this,
                &SCDatabaseEditorMainWindow::OnTableSelectionChanged);
            selectAction->setEnabled(true);
            menu.addAction(QStringLiteral("添加列..."), this,
                           &SCDatabaseEditorMainWindow::AddColumn);
            menu.addAction(QStringLiteral("删除表..."), this,
                           &SCDatabaseEditorMainWindow::DeleteSelectedTable);
        } else if (nodeType == ExplorerNodeType::TablesRoot)
        {
            menu.addAction(QStringLiteral("创建表..."), this,
                           &SCDatabaseEditorMainWindow::CreateTable);
            menu.addAction(
                QStringLiteral("从模式创建表..."), this,
                &SCDatabaseEditorMainWindow::CreateTableFromSchemaDescription);
        } else if (nodeType == ExplorerNodeType::ComputedRoot)
        {
            menu.addAction(
                QStringLiteral("添加会话计算列..."), this,
                &SCDatabaseEditorMainWindow::AddSessionComputedColumn);
        } else
        {
            menu.addAction(QStringLiteral("刷新"), this,
                           &SCDatabaseEditorMainWindow::RefreshCurrentView);
        }

        menu.exec(objectTree_->mapToGlobal(pos));
    }

    void SCDatabaseEditorMainWindow::DeleteSelectedTable()
    {
        if (session_ == nullptr || !session_->IsOpen())
        {
            ShowError(QStringLiteral("删除表失败"),
                      QStringLiteral("未打开数据库。"));
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
            ShowError(QStringLiteral("删除表失败"),
                      QStringLiteral("请先选择表。"));
            return;
        }

        const QMessageBox::StandardButton answer = QMessageBox::question(
            this, QStringLiteral("删除表"),
            QStringLiteral("删除表 \"%1\"?").arg(tableName));
        if (answer != QMessageBox::Yes)
        {
            return;
        }

        QString error;
        if (!session_->DeleteTable(tableName, &error))
        {
            ShowError(QStringLiteral("删除表失败"), error);
            return;
        }

        SetStatusMessage(QStringLiteral("表已删除: ") + tableName);
    }

    void SCDatabaseEditorMainWindow::DeleteSelectedColumn()
    {
        const QString columnName = CurrentSchemaColumnName();
        if (columnName.isEmpty())
        {
            ShowError(QStringLiteral("删除列失败"),
                      QStringLiteral("请先选择模式字段。"));
            return;
        }

        const QMessageBox::StandardButton answer = QMessageBox::question(
            this, QStringLiteral("删除列"),
            QStringLiteral("删除模式字段 \"%1\"?").arg(columnName));
        if (answer != QMessageBox::Yes)
        {
            return;
        }

        QVector<sc::SCColumnDef> columnsBeforeDelete;
        QString error;
        if (!session_->BuildSchemaSnapshot(&columnsBeforeDelete, &error))
        {
            ShowError(QStringLiteral("删除列失败"), error);
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
            ShowError(QStringLiteral("删除列失败"), error);
            return;
        }

        UpdateSchemaInspector();
        SelectSchemaColumnByName(fallbackSelection);
        RefreshGridData();
        UpdateRelationInspector();
        RefreshOverviewPanels();
        SetStatusMessage(QStringLiteral("列已删除: ") + columnName);
    }

    void SCDatabaseEditorMainWindow::EditSelectedColumn()
    {
        const QString columnName = CurrentSchemaColumnName();
        if (columnName.isEmpty())
        {
            ShowError(QStringLiteral("编辑列失败"),
                      QStringLiteral("请先选择模式字段。"));
            return;
        }

        sc::SCColumnDef existing;
        QString error;
        if (!session_->GetColumnDef(columnName, &existing, &error))
        {
            ShowError(QStringLiteral("编辑列失败"), error);
            return;
        }

        sc::SCEditingDatabaseState editingState;
        QString editingError;
        if (!session_->GetEditingState(&editingState, &editingError))
        {
            ShowError(QStringLiteral("编辑列失败"), editingError);
            return;
        }
        if (session_->HasPendingEdit())
        {
            ShowError(QStringLiteral("编辑列失败"),
                      QStringLiteral(
                          "请先保存或放弃待更改再修改模式。"));
            return;
        }

        SCAddColumnDialog dialog(session_, existing, this);
        bool tableHasRecords = false;
        QString stateError;
        if (!session_->CurrentTableHasRecords(&tableHasRecords, &stateError))
        {
            ShowError(QStringLiteral("编辑列失败"), stateError);
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
            ShowError(QStringLiteral("编辑列失败"),
                      QStringLiteral("需要填写列名。"));
            return;
        }

        if (!session_->UpdateColumn(columnName, updated, &error))
        {
            ShowError(QStringLiteral("编辑列失败"), error);
            return;
        }

        UpdateSchemaInspector();
        SelectSchemaColumnByName(ToQString(updated.name));
        RefreshGridData();
        UpdateRelationInspector();
        RefreshOverviewPanels();
        SetStatusMessage(QStringLiteral("列已更新: ") +
                         ToQString(updated.name));
    }

    void SCDatabaseEditorMainWindow::ConvertSelectedColumnToComputed()
    {
        const QString columnName = CurrentSchemaColumnName();
        if (columnName.isEmpty())
        {
            ShowError(QStringLiteral("转换为计算列失败"),
                      QStringLiteral("请先选择模式字段。"));
            return;
        }

        sc::SCColumnDef existing;
        QString error;
        if (!session_->GetColumnDef(columnName, &existing, &error))
        {
            ShowError(QStringLiteral("转换为计算列失败"), error);
            return;
        }

        SCComputedColumnDialog dialog(session_->CurrentTableName(),
                                      BuildComputedTemplate(existing), true,
                                      this);
        dialog.setWindowTitle(QStringLiteral("转换列为计算列"));
        if (dialog.exec() != QDialog::Accepted)
        {
            return;
        }

        sc::SCComputedColumnDef definition;
        if (!dialog.BuildDefinition(&definition, &error))
        {
            ShowError(QStringLiteral("转换为计算列失败"), error);
            return;
        }

        if (!session_->ConvertColumnToComputed(columnName, definition, &error))
        {
            ShowError(QStringLiteral("转换为计算列失败"), error);
            return;
        }

        UpdateSchemaInspector();
        UpdateComputedColumnsPanel();
        SelectComputedColumnByName(ToQString(definition.name));
        RefreshGridData();
        UpdateRelationInspector();
        RefreshOverviewPanels();
        SetStatusMessage(QStringLiteral("已转换为计算列: ") +
                         ToQString(definition.name));
    }

    void SCDatabaseEditorMainWindow::AddSessionComputedColumn()
    {
        if (!session_->CurrentTable())
        {
            ShowError(QStringLiteral("添加计算列失败"),
                      QStringLiteral("未选择表。"));
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
            ShowError(QStringLiteral("添加计算列失败"), error);
            return;
        }

        if (!session_->AddSessionComputedColumn(definition, &error))
        {
            ShowError(QStringLiteral("添加计算列失败"), error);
            return;
        }

        RefreshGridData();
        UpdateComputedColumnsPanel();
        SelectComputedColumnByName(ToQString(definition.name));
        UpdateRelationInspector();
        RefreshOverviewPanels();
        SetStatusMessage(QStringLiteral("计算列已添加: ") +
                         ToQString(definition.name));
    }

    void SCDatabaseEditorMainWindow::EditSelectedComputedColumn()
    {
        const QString columnName = CurrentComputedColumnName();
        if (columnName.isEmpty())
        {
            ShowError(QStringLiteral("编辑计算列失败"),
                      QStringLiteral("请在会话计算列面板中选择计算列。"));
            return;
        }

        sc::SCComputedColumnDef existing;
        QString error;
        if (!session_->GetSessionComputedColumn(columnName, &existing, &error))
        {
            ShowError(QStringLiteral("编辑计算列失败"), error);
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
            ShowError(QStringLiteral("编辑计算列失败"), error);
            return;
        }

        if (!session_->UpdateSessionComputedColumn(columnName, updated, &error))
        {
            ShowError(QStringLiteral("编辑计算列失败"), error);
            return;
        }

        RefreshGridData();
        UpdateComputedColumnsPanel();
        SelectComputedColumnByName(ToQString(updated.name));
        UpdateRelationInspector();
        RefreshOverviewPanels();
        SetStatusMessage(QStringLiteral("计算列已更新: ") +
                         ToQString(updated.name));
    }

    void SCDatabaseEditorMainWindow::ConvertSelectedComputedToColumn()
    {
        const QString columnName = CurrentComputedColumnName();
        if (columnName.isEmpty())
        {
            ShowError(QStringLiteral("转换为列失败"),
                      QStringLiteral("请先选择计算列。"));
            return;
        }

        sc::SCComputedColumnDef existing;
        QString error;
        if (!session_->GetSessionComputedColumn(columnName, &existing, &error))
        {
            ShowError(QStringLiteral("转换为列失败"), error);
            return;
        }

        sc::SCEditingDatabaseState editingState;
        QString editingError;
        if (!session_->GetEditingState(&editingState, &editingError))
        {
            ShowError(QStringLiteral("转换为列失败"), editingError);
            return;
        }
        if (session_->HasPendingEdit())
        {
            ShowError(QStringLiteral("转换为列失败"),
                      QStringLiteral(
                          "请先保存或放弃待更改再修改模式。"));
            return;
        }

        SCAddColumnDialog dialog(session_, BuildColumnTemplate(existing), this);
        dialog.setWindowTitle(QStringLiteral("转换计算列为列"));
        bool tableHasRecords = false;
        QString stateError;
        if (!session_->CurrentTableHasRecords(&tableHasRecords, &stateError))
        {
            ShowError(QStringLiteral("转换为列失败"), stateError);
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
            ShowError(QStringLiteral("转换为列失败"),
                      QStringLiteral("需要填写列名。"));
            return;
        }

        if (!session_->ConvertComputedToColumn(columnName, definition, &error))
        {
            ShowError(QStringLiteral("转换为列失败"), error);
            return;
        }

        UpdateSchemaInspector();
        UpdateComputedColumnsPanel();
        SelectSchemaColumnByName(ToQString(definition.name));
        RefreshGridData();
        UpdateRelationInspector();
        RefreshOverviewPanels();
        SetStatusMessage(QStringLiteral("已转换为列: ") +
                         ToQString(definition.name));
    }

    void SCDatabaseEditorMainWindow::DeleteSelectedComputedColumn()
    {
        const QString columnName = CurrentComputedColumnName();
        if (columnName.isEmpty())
        {
            ShowError(QStringLiteral("删除计算列失败"),
                      QStringLiteral("请在会话计算列面板中选择计算列。"));
            return;
        }

        const QMessageBox::StandardButton answer = QMessageBox::question(
            this, QStringLiteral("删除计算列"),
            QStringLiteral("删除会话计算列 \"%1\"?")
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
            ShowError(QStringLiteral("删除计算列失败"), error);
            return;
        }

        RefreshGridData();
        UpdateComputedColumnsPanel();
        SelectComputedColumnByName(fallbackSelection);
        UpdateRelationInspector();
        RefreshOverviewPanels();
        SetStatusMessage(QStringLiteral("计算列已删除: ") +
                         columnName);
    }

    void SCDatabaseEditorMainWindow::AddRecord()
    {
        QString error;
        if (!session_->AddRecord(&error))
        {
            ShowError(QStringLiteral("添加记录失败"), error);
            return;
        }

        RefreshGridData();
        UpdateRecordInspector();
        UpdateRelationInspector();
        RefreshOverviewPanels();
        UpdateGridSummary();
        UpdateDatabaseStatusBar();

        sc::SCEditingDatabaseState editingState;
        if (session_->GetEditingState(&editingState, &error) &&
            session_->HasPendingEdit())
        {
            SetStatusMessage(QStringLiteral(
                "记录草稿已创建。填写必填字段后保存待更改。"));
        } else
        {
            SetStatusMessage(QStringLiteral("记录已添加。"));
        }
    }

    void SCDatabaseEditorMainWindow::SavePendingChanges()
    {
        QString error;
        if (!session_->SavePendingChanges(&error))
        {
            ShowError(QStringLiteral("保存待更改失败"), error);
            return;
        }

        RefreshGridData();
        UpdateRecordInspector();
        UpdateRelationInspector();
        RefreshOverviewPanels();
        UpdateGridSummary();
        UpdateDatabaseStatusBar();
        SetStatusMessage(QStringLiteral("待更改已保存。"));
    }

    void SCDatabaseEditorMainWindow::DiscardPendingChanges()
    {
        QString error;
        if (!session_->DiscardPendingChanges(&error))
        {
            ShowError(QStringLiteral("放弃待更改失败"), error);
            return;
        }

        RefreshGridData();
        UpdateRecordInspector();
        UpdateRelationInspector();
        RefreshOverviewPanels();
        UpdateGridSummary();
        UpdateDatabaseStatusBar();
        SetStatusMessage(QStringLiteral("待更改已丢弃。"));
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
            this, QStringLiteral("删除记录"),
            QStringLiteral("删除记录 %1?").arg(recordId));
        if (answer != QMessageBox::Yes)
        {
            return;
        }

        QString error;
        if (!session_->DeleteRecord(recordId, &error))
        {
            ShowError(QStringLiteral("删除记录失败"), error);
            return;
        }

        SetStatusMessage(QStringLiteral("记录已删除: ") +
                         QString::number(recordId));
    }

    void SCDatabaseEditorMainWindow::EditSelectedRelation()
    {
        const int row = CurrentRow();
        const int col = CurrentColumn();
        if (row < 0 || col < 0)
        {
            return;
        }

        const sc::SCTableViewColumnDef tableColumn =
            columns_[col];
        if (tableColumn.layer != sc::TableColumnLayer::Fact)
        {
            ShowError(QStringLiteral("选择关系失败"),
                      QStringLiteral("计算列不能存储关系值。"));
            return;
        }

        sc::SCColumnDef column;
        QString error;
        if (!session_->GetColumnDef(ToQString(tableColumn.name), &column,
                                    &error))
        {
            ShowError(QStringLiteral("选择关系失败"), error);
            return;
        }
        if (column.columnKind != sc::ColumnKind::Relation)
        {
            ShowError(QStringLiteral("选择关系失败"),
                      QStringLiteral("所选列不是关系字段。"));
            return;
        }

        QVector<SCDatabaseSession::RelationCandidate> candidates;
        if (!session_->BuildRelationCandidates(ToQString(column.referenceTable),
                                               column, &candidates, &error))
        {
            ShowError(QStringLiteral("选择关系失败"), error);
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
            ShowError(QStringLiteral("选择关系失败"), error);
            return;
        }

        if (!session_->SetCellValue(RecordIdAt(row),
                                    ToQString(tableColumn.name), storedValue,
                                    &error))
        {
            ShowError(QStringLiteral("选择关系失败"), error);
            return;
        }

        RefreshGridData();
        SetStatusMessage(QStringLiteral("关系已更新。"));
    }

    void SCDatabaseEditorMainWindow::OnSchemaContextMenuRequested(
        const QPoint& pos)
    {
        if (schemaTree_ == nullptr)
        {
            return;
        }

        if (QTreeWidgetItem* item = schemaTree_->itemAt(pos); item != nullptr)
        {
            schemaTree_->setCurrentItem(item);
        }

        QMenu menu(schemaTree_);
        const bool canEditSchema =
            session_->IsOpen() && !session_->CurrentTableName().isEmpty();
        QAction* addAction =
            menu.addAction(QStringLiteral("添加列..."), this,
                           &SCDatabaseEditorMainWindow::AddColumn);
        addAction->setEnabled(canEditSchema);
        QAction* editAction =
            menu.addAction(QStringLiteral("编辑列..."), this,
                           &SCDatabaseEditorMainWindow::EditSelectedColumn);
        editAction->setEnabled(canEditSchema &&
                               !CurrentSchemaColumnName().isEmpty());
        QAction* deleteAction =
            menu.addAction(QStringLiteral("删除列..."), this,
                           &SCDatabaseEditorMainWindow::DeleteSelectedColumn);
        deleteAction->setEnabled(canEditSchema &&
                                 !CurrentSchemaColumnName().isEmpty());
        menu.exec(schemaTree_->mapToGlobal(pos));
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
            menu.addAction(QStringLiteral("添加行"), this,
                           &SCDatabaseEditorMainWindow::AddRecord);
        addAction->setEnabled(canEditRows);
        QAction* deleteAction =
            menu.addAction(QStringLiteral("删除行"), this,
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
            ShowError(QStringLiteral("撤销失败"), error);
            return;
        }

        RefreshGridData();
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
            ShowError(QStringLiteral("重做失败"), error);
            return;
        }

        RefreshGridData();
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
            ShowError(QStringLiteral("刷新失败"), error);
            return;
        }

        RefreshGridData();
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
            this, QStringLiteral("导出调试包"), QString(),
            QStringLiteral("调试包 (*.scdbg);;所有文件 (*)"));
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
            ShowError(QStringLiteral("导出调试包失败"), error);
            return;
        }

        if (bottomTabs_ != nullptr)
        {
            bottomTabs_->setCurrentIndex(kBottomDebugPackageTab);
        }
        if (debugPackageText_ != nullptr)
        {
            debugPackageText_->setPlainText(
                QStringLiteral("已导出调试包:\n") + filePath);
        }
        SetStatusMessage(QStringLiteral("调试包已导出: ") + filePath);
    }

    void SCDatabaseEditorMainWindow::ExportCurrentTableCsv()
    {
        if (!session_->IsOpen() || session_->CurrentTableName().isEmpty())
        {
            ShowError(QStringLiteral("导出CSV失败"),
                      QStringLiteral("导出CSV请先选择表。"));
            return;
        }

        const QString defaultFileName =
            session_->CurrentTableName() + QStringLiteral(".csv");
        const QString filePath = QFileDialog::getSaveFileName(
            this, QStringLiteral("导出CSV"), defaultFileName,
            QStringLiteral("CSV文件 (*.csv);;所有文件 (*)"));
        if (filePath.isEmpty())
        {
            return;
        }

        QString error;
        if (!ExportCurrentTableCsvFile(filePath, &error))
        {
            ShowError(QStringLiteral("导出CSV失败"), error);
            return;
        }

        SetStatusMessage(QStringLiteral("CSV已导出: ") + filePath);
    }

    void SCDatabaseEditorMainWindow::ImportCsvIntoCurrentTable()
    {
        if (!session_->IsOpen() || session_->CurrentTableName().isEmpty())
        {
            ShowError(QStringLiteral("导入CSV失败"),
                      QStringLiteral("导入CSV请先选择表。"));
            return;
        }

        sc::SCEditingDatabaseState editingState;
        QString stateError;
        if (!session_->GetEditingState(&editingState, &stateError))
        {
            ShowError(QStringLiteral("导入CSV失败"), stateError);
            return;
        }
        if (session_->HasPendingEdit())
        {
            ShowError(QStringLiteral("导入CSV失败"),
                      QStringLiteral(
                          "导入CSV请先保存或放弃待更改。"));
            return;
        }
        if (editingState.openMode == sc::SCDatabaseOpenMode::ReadOnly)
        {
            ShowError(
                QStringLiteral("导入CSV失败"),
                QStringLiteral("CSV导入需要可写入的数据库。"));
            return;
        }

        const QString defaultFileName =
            session_->CurrentTableName() + QStringLiteral(".csv");
        const QString filePath = QFileDialog::getOpenFileName(
            this, QStringLiteral("导入CSV"), defaultFileName,
            QStringLiteral("CSV文件 (*.csv);;所有文件 (*)"));
        if (filePath.isEmpty())
        {
            return;
        }

        QString error;
        if (!ImportCsvIntoCurrentTableFile(filePath, &error))
        {
            ShowError(QStringLiteral("导入CSV失败"), error);
            return;
        }

        SetStatusMessage(QStringLiteral("CSV已导入: ") + filePath);
    }

    bool SCDatabaseEditorMainWindow::ExportCurrentTableCsvFile(
        const QString& filePath, QString* outError) const
    {
        if (!session_->IsOpen() || session_->CurrentTableName().isEmpty())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("未选择表。");
            }
            return false;
        }

        QSaveFile file(filePath);
        if (!file.open(QIODevice::WriteOnly | QIODevice::Text))
        {
            if (outError != nullptr)
            {
                *outError =
                    QStringLiteral("无法打开CSV文件进行写入。");
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
                    *outError = QStringLiteral("无法写入CSV内容。");
                }
                return false;
            }
            if (file.write("\n") != 1)
            {
                if (outError != nullptr)
                {
                    *outError =
                        QStringLiteral("无法写入CSV换行符。");
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
                *outError = QStringLiteral("无法写入CSV表头。");
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
                *outError = QStringLiteral("无法完成CSV文件。");
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
                *outError = QStringLiteral("未选择表。");
            }
            return false;
        }

        QFile file(filePath);
        if (!file.open(QIODevice::ReadOnly | QIODevice::Text))
        {
            if (outError != nullptr)
            {
                *outError =
                    QStringLiteral("无法打开CSV文件进行读取。");
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
                    QStringLiteral("CSV文件不含表头行。");
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
                                        "CSV表头包含重复的可编辑列: ") +
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
                        "CSV文件不含任何可编辑列。");
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
                                "CSV第%1行缺少必填字段 \"%2\"。")
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
                            QStringLiteral("CSV第%1行, 列 \"%2\": %3")
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
        options.editName = L"导入CSV";
        options.rollbackOnError = true;

        sc::SCBatchExecutionResult result;
        const sc::ErrorCode rc = sc::ExecuteBatchEdit(
            session_->Database(), requests, options, &result);
        if (sc::Failed(rc))
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("CSV导入失败: ") +
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
                    QStringLiteral("CSV导入已提交, 但刷新失败: ") +
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
                QStringLiteral("SQL 预览在此阶段未填充。\n"
                               "使用模式和记录操作来检查更改。"));
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
                QStringLiteral("加载编辑状态失败: ") + error);
            editLogTree_->clear();
            return;
        }

        if (!session_->GetEditLogState(&logState, &error))
        {
            editStateText_->setPlainText(
                QStringLiteral("加载编辑日志失败: ") + error);
            editLogTree_->clear();
            return;
        }

        QString summary;
        summary += QStringLiteral("打开: ") +
                   (editingState.open ? QStringLiteral("是")
                                      : QStringLiteral("否")) +
                   QLatin1Char('\n');
        summary += QStringLiteral("打开模式: ") +
                   OpenModeToText(editingState.openMode) + QLatin1Char('\n');
        summary += QStringLiteral("脏: ") +
                   (editingState.dirty ? QStringLiteral("是")
                                       : QStringLiteral("否")) +
                   QLatin1Char('\n');
        summary += QStringLiteral("当前版本: ") +
                   QString::number(
                       static_cast<qulonglong>(editingState.currentVersion)) +
                   QLatin1Char('\n');
        summary += QStringLiteral("基线版本: ") +
                   QString::number(
                       static_cast<qulonglong>(editingState.baselineVersion)) +
                   QLatin1Char('\n');
        summary += QStringLiteral("撤销次数: ") +
                   QString::number(editingState.undoCount) + QLatin1Char('\n');
        summary += QStringLiteral("重做次数: ") +
                   QString::number(editingState.redoCount) + QLatin1Char('\n');
        summary += QStringLiteral("撤销项: ") +
                   QString::number(logState.undoItems.size()) +
                   QLatin1Char('\n');
        summary += QStringLiteral("重做项: ") +
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
                col >= columns_.size())
            {
                context.ErrorText = QStringLiteral("无效单元格");
                return false;
            }

            const sc::RecordId recordId = visibleRows_[row].recordId;
            const QString fieldName =
                ToQString(columns_[col].name);
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
            ShowError(QStringLiteral("加载模式失败"), QString::number(ccRc));
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
            ShowError(QStringLiteral("枚举记录失败"), QString::number(rc));
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

        dataTable_->OnGetRowCount = [this]() {
            return static_cast<int>(visibleRows_.size());
        };

        dataTable_->OnGetColumnCount = [this]() {
            return static_cast<int>(columns_.size());
        };

        dataTable_->OnGetColumnDef = [this](int col) {
            ::SCColumnDef def;
            if (col >= 0 && col < static_cast<int>(columns_.size()))
            {
                def.Title = ToQString(columns_[col].displayName);
                def.Editable = columns_[col].editable;
                def.Width = 120;
            }
            return def;
        };

        dataTable_->OnGetHeaderText = [this](int col) {
            if (col >= 0 && col < static_cast<int>(columns_.size()))
            {
                return ToQString(columns_[col].displayName);
            }
            return QString();
        };

        dataTable_->OnGetCellData = [this](int row, int col) {
            SCCellData data;
            if (row < 0 || row >= static_cast<int>(visibleRows_.size()) ||
                col < 0 || col >= static_cast<int>(columns_.size()))
            {
                return data;
            }

            QVariant value;
            QString error;
            const bool ok = session_->GetCellDisplayValue(
                visibleRows_[row].recordId,
                QString::fromStdWString(columns_[col].name),
                &value, &error);

            if (ok)
            {
                data.ValueType = SCCellValueType::Text;
                data.Text = value.toString();
                data.DisplayText = value.toString();
                data.ReadOnly = !columns_[col].editable;
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
        databaseItem->setText(0, QStringLiteral("数据库"));
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
        tablesRoot->setText(0, QStringLiteral("表"));
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
            QStringLiteral("计算列 (%1)").arg(computedColumns.size()));
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
        systemRoot->setText(0, QStringLiteral("系统"));
        systemRoot->setData(0, kExplorerNodeTypeRole,
                            static_cast<int>(
                                ExplorerNodeType::SystemRoot));

        auto* editLogItem =
            new QTreeWidgetItem(systemRoot);
        editLogItem->setText(0, QStringLiteral("编辑日志"));
        editLogItem->setText(
            1,
            ExplorerNodeTypeToText(ExplorerNodeType::EditLog));
        editLogItem->setData(0, kExplorerNodeTypeRole,
                             static_cast<int>(
                                 ExplorerNodeType::EditLog));

        auto* journalItem =
            new QTreeWidgetItem(systemRoot);
        journalItem->setText(0, QStringLiteral("日志"));
        journalItem->setText(
            1,
            ExplorerNodeTypeToText(ExplorerNodeType::Journal));
        journalItem->setData(0, kExplorerNodeTypeRole,
                             static_cast<int>(
                                 ExplorerNodeType::Journal));

        auto* snapshotsItem =
            new QTreeWidgetItem(systemRoot);
        snapshotsItem->setText(0, QStringLiteral("快照"));
        snapshotsItem->setText(
            1,
            ExplorerNodeTypeToText(
                ExplorerNodeType::Snapshots));
        snapshotsItem->setData(0, kExplorerNodeTypeRole,
                               static_cast<int>(
                                   ExplorerNodeType::Snapshots));

        auto* diagnosticsItem =
            new QTreeWidgetItem(systemRoot);
        diagnosticsItem->setText(0, QStringLiteral("诊断"));
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
                        ShowError(QStringLiteral("选择表失败"), error);
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
            case ExplorerNodeType::SystemRoot:
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

    void SCDatabaseEditorMainWindow::OnGridCellSelected(
        int row, int col)
    {
        Q_UNUSED(row);
        Q_UNUSED(col);
        UpdateRecordInspector();
        UpdateRelationInspector();
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

    QString SCDatabaseEditorMainWindow::CurrentSchemaColumnName()
        const
    {
        if (schemaTree_ == nullptr)
        {
            return {};
        }

        QTreeWidgetItem* item = schemaTree_->currentItem();
        if (item == nullptr)
        {
            return {};
        }

        return item->text(0);
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

    void SCDatabaseEditorMainWindow::SelectSchemaColumnByName(
        const QString& name)
    {
        if (schemaTree_ == nullptr || name.isEmpty())
        {
            return;
        }

        for (int index = 0;
             index < schemaTree_->topLevelItemCount();
             ++index)
        {
            QTreeWidgetItem* item =
                schemaTree_->topLevelItem(index);
            if (item != nullptr &&
                item->text(0)
                        .compare(name, Qt::CaseInsensitive) ==
                    0)
            {
                schemaTree_->setCurrentItem(item);
                return;
            }
        }
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

    void SCDatabaseEditorMainWindow::UpdateSchemaInspector()
    {
        if (schemaTree_ == nullptr)
        {
            return;
        }

        schemaTree_->clear();

        if (!session_->IsOpen() ||
            session_->CurrentTableName().isEmpty())
        {
            return;
        }

        QVector<sc::SCColumnDef> columns;
        QString error;
        if (!session_->BuildSchemaSnapshot(&columns, &error))
        {
            return;
        }

        for (const sc::SCColumnDef& column : columns)
        {
            auto* item =
                new QTreeWidgetItem(schemaTree_);
            item->setText(0, ToQString(column.name));
            item->setText(1, ToQString(column.displayName));
            item->setText(
                2, ValueKindToText(column.valueKind));
            item->setText(
                3,
                BoolToText(column.nullable));
            item->setText(
                4, SCValueToText(column.defaultValue));
            item->setText(
                5, ToQString(column.referenceTable));
            item->setText(
                6,
                BoolToText(column.userDefined));
            item->setText(
                7,
                BoolToText(
                    column.participatesInCalc));
        }

        for (int col = 0;
             col < schemaTree_->columnCount();
             ++col)
        {
            schemaTree_->resizeColumnToContents(col);
        }
    }

    void SCDatabaseEditorMainWindow::UpdateRecordInspector()
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
        idItem->setText(0, QStringLiteral("记录ID"));
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
                                : QStringLiteral("<错误>"));
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
                    : QStringLiteral("<自定义>"));
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

    void SCDatabaseEditorMainWindow::UpdateRelationInspector()
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
                      : QStringLiteral("<错误>"));
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
        const int rowCount =
            static_cast<int>(visibleRows_.size());
        const int colCount =
            static_cast<int>(columns_.size());

        if (tableStatsLabel_ != nullptr)
        {
            tableStatsLabel_->setText(
                QStringLiteral("记录: %1  列: %2")
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
                    ? QStringLiteral("筛选: 开启")
                    : QStringLiteral("筛选: 关闭"));
        }

        if (transactionStateLabel_ != nullptr)
        {
            sc::SCEditingDatabaseState state;
            QString error;
            const bool ok =
                session_->GetEditingState(&state, &error);
            if (ok && state.dirty)
            {
                transactionStateLabel_->setText(
                    QStringLiteral("事务: 未保存"));
            } else if (ok)
            {
                transactionStateLabel_->setText(
                    QStringLiteral("事务: 空闲"));
            } else
            {
                transactionStateLabel_->setText(
                    QStringLiteral("事务: 未知"));
            }
        }
    }

    void SCDatabaseEditorMainWindow::UpdateDatabaseStatusBar()
    {
        QString error;
        sc::SCEditingDatabaseState state;
        const bool stateLoaded =
            session_->GetEditingState(&state, &error);

        if (openModeLabel_ != nullptr)
        {
            openModeLabel_->setText(
                QStringLiteral("模式: %1")
                    .arg(stateLoaded
                             ? OpenModeToText(state.openMode)
                             : QStringLiteral("已关闭")));
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
                    ? QStringLiteral("表: -")
                    : QStringLiteral("表: ") + name);
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
