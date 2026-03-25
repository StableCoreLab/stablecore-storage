#include "DatabaseSession.h"

#include <algorithm>

#include <QStringList>

namespace sc = stablecore::storage;

namespace stablecore::storage::editor
{
namespace
{

QString ToQString(const std::wstring& text)
{
    return QString::fromStdWString(text);
}

QString ValueToDisplayString(const sc::Value& value)
{
    switch (value.GetKind())
    {
    case sc::ValueKind::Null:
        return QString();
    case sc::ValueKind::Int64:
    {
        std::int64_t v = 0;
        value.AsInt64(&v);
        return QString::number(v);
    }
    case sc::ValueKind::Double:
    {
        double v = 0.0;
        value.AsDouble(&v);
        return QString::number(v, 'g', 12);
    }
    case sc::ValueKind::Bool:
    {
        bool v = false;
        value.AsBool(&v);
        return v ? QStringLiteral("true") : QStringLiteral("false");
    }
    case sc::ValueKind::String:
    {
        std::wstring v;
        value.AsStringCopy(&v);
        return ToQString(v);
    }
    case sc::ValueKind::RecordId:
    {
        sc::RecordId v = 0;
        value.AsRecordId(&v);
        return QString::number(v);
    }
    case sc::ValueKind::Enum:
    {
        std::wstring v;
        value.AsEnumCopy(&v);
        return ToQString(v);
    }
    default:
        return QStringLiteral("<unsupported>");
    }
}

QString PickRecordLabel(
    const QVector<QPair<QString, QString>>& previewFields,
    sc::RecordId recordId)
{
    static const QStringList preferredNames = {
        QStringLiteral("Name"),
        QStringLiteral("Title"),
        QStringLiteral("Code")
    };

    for (const QString& preferred : preferredNames)
    {
        for (const auto& field : previewFields)
        {
            if (field.first.compare(preferred, Qt::CaseInsensitive) == 0 && !field.second.trimmed().isEmpty())
            {
                return field.second;
            }
        }
    }

    for (const auto& field : previewFields)
    {
        if (!field.second.trimmed().isEmpty())
        {
            return field.first + QStringLiteral(": ") + field.second;
        }
    }

    return QStringLiteral("Record %1").arg(recordId);
}

}  // namespace

DatabaseSession::DatabaseSession(QObject* parent)
    : QObject(parent)
{
}

bool DatabaseSession::IsOpen() const noexcept
{
    return static_cast<bool>(db_);
}

QString DatabaseSession::DatabasePath() const
{
    return databasePath_;
}

QString DatabaseSession::CurrentTableName() const
{
    return currentTableName_;
}

QStringList DatabaseSession::TableNames() const
{
    return tableNames_;
}

sc::IDatabase* DatabaseSession::Database() const noexcept
{
    return db_.Get();
}

sc::ITable* DatabaseSession::CurrentTable() const noexcept
{
    return currentTable_.Get();
}

sc::IComputedTableView* DatabaseSession::CurrentTableView() const noexcept
{
    return currentTableView_.Get();
}

bool DatabaseSession::CreateDatabase(const QString& filePath, QString* outError)
{
    db_.Reset();
    currentTable_.Reset();
    currentTableView_.Reset();
    sessionComputedColumnsByTable_.clear();

    const sc::ErrorCode rc = sc::CreateSqliteDatabase(filePath.toStdWString().c_str(), db_);
    if (sc::Failed(rc))
    {
        if (outError != nullptr)
        {
            *outError = ErrorToString(rc);
        }
        return false;
    }

    databasePath_ = filePath;
    currentTableName_.clear();
    LoadTableNames(outError);
    emit DatabaseOpened();
    emit TablesChanged();
    return true;
}

bool DatabaseSession::OpenDatabase(const QString& filePath, QString* outError)
{
    return CreateDatabase(filePath, outError);
}

bool DatabaseSession::Refresh(QString* outError)
{
    if (!IsOpen())
    {
        if (outError != nullptr)
        {
            *outError = QStringLiteral("No database is open.");
        }
        return false;
    }

    if (!LoadTableNames(outError))
    {
        return false;
    }

    if (!currentTableName_.isEmpty())
    {
        return SelectTable(currentTableName_, outError);
    }

    emit TablesChanged();
    emit RecordsChanged();
    return true;
}

bool DatabaseSession::CreateTable(const QString& tableName, QString* outError)
{
    if (!IsOpen())
    {
        if (outError != nullptr)
        {
            *outError = QStringLiteral("No database is open.");
        }
        return false;
    }

    sc::TablePtr table;
    const sc::ErrorCode rc = db_->CreateTable(tableName.toStdWString().c_str(), table);
    if (sc::Failed(rc))
    {
        if (outError != nullptr)
        {
            *outError = ErrorToString(rc);
        }
        return false;
    }

    LoadTableNames(outError);
    emit TablesChanged();
    return SelectTable(tableName, outError);
}

bool DatabaseSession::SelectTable(const QString& tableName, QString* outError)
{
    if (!IsOpen())
    {
        if (outError != nullptr)
        {
            *outError = QStringLiteral("No database is open.");
        }
        return false;
    }

    sc::TablePtr table;
    sc::ErrorCode rc = db_->GetTable(tableName.toStdWString().c_str(), table);
    if (sc::Failed(rc))
    {
        if (outError != nullptr)
        {
            *outError = ErrorToString(rc);
        }
        return false;
    }

    currentTable_ = table;
    currentTableName_ = tableName;
    if (!RebuildCurrentTableView(outError))
    {
        return false;
    }

    emit CurrentTableChanged();
    emit RecordsChanged();
    return true;
}

bool DatabaseSession::AddColumn(const sc::ColumnDef& column, QString* outError)
{
    if (!currentTable_)
    {
        if (outError != nullptr)
        {
            *outError = QStringLiteral("No table is selected.");
        }
        return false;
    }

    sc::SchemaPtr schema;
    sc::ErrorCode rc = currentTable_->GetSchema(schema);
    if (sc::Failed(rc))
    {
        if (outError != nullptr)
        {
            *outError = ErrorToString(rc);
        }
        return false;
    }

    rc = schema->AddColumn(column);
    if (sc::Failed(rc))
    {
        if (outError != nullptr)
        {
            *outError = ErrorToString(rc);
        }
        return false;
    }

    emit CurrentTableChanged();
    emit RecordsChanged();
    return true;
}

bool DatabaseSession::AddRecord(QString* outError)
{
    if (!currentTable_)
    {
        if (outError != nullptr)
        {
            *outError = QStringLiteral("No table is selected.");
        }
        return false;
    }

    const bool ok = BeginAndCommitSingleAction(
        L"Add Record",
        [&]()
        {
            sc::RecordPtr record;
            return currentTable_->CreateRecord(record);
        },
        outError);

    if (ok)
    {
        emit RecordsChanged();
    }
    return ok;
}

bool DatabaseSession::DeleteRecord(sc::RecordId recordId, QString* outError)
{
    if (!currentTable_)
    {
        if (outError != nullptr)
        {
            *outError = QStringLiteral("No table is selected.");
        }
        return false;
    }

    const bool ok = BeginAndCommitSingleAction(
        L"Delete Record",
        [&]()
        {
            return currentTable_->DeleteRecord(recordId);
        },
        outError);

    if (ok)
    {
        emit RecordsChanged();
    }
    return ok;
}

bool DatabaseSession::Undo(QString* outError)
{
    if (!IsOpen())
    {
        if (outError != nullptr)
        {
            *outError = QStringLiteral("No database is open.");
        }
        return false;
    }

    const sc::ErrorCode rc = db_->Undo();
    if (sc::Failed(rc))
    {
        if (outError != nullptr)
        {
            *outError = ErrorToString(rc);
        }
        return false;
    }

    emit RecordsChanged();
    return true;
}

bool DatabaseSession::Redo(QString* outError)
{
    if (!IsOpen())
    {
        if (outError != nullptr)
        {
            *outError = QStringLiteral("No database is open.");
        }
        return false;
    }

    const sc::ErrorCode rc = db_->Redo();
    if (sc::Failed(rc))
    {
        if (outError != nullptr)
        {
            *outError = ErrorToString(rc);
        }
        return false;
    }

    emit RecordsChanged();
    return true;
}

bool DatabaseSession::SetCellValue(
    sc::RecordId recordId,
    const QString& columnName,
    const QVariant& value,
    QString* outError)
{
    if (!currentTable_)
    {
        if (outError != nullptr)
        {
            *outError = QStringLiteral("No table is selected.");
        }
        return false;
    }

    sc::SchemaPtr schema;
    sc::ErrorCode rc = currentTable_->GetSchema(schema);
    if (sc::Failed(rc))
    {
        if (outError != nullptr)
        {
            *outError = ErrorToString(rc);
        }
        return false;
    }

    sc::ColumnDef column;
    rc = schema->FindColumn(columnName.toStdWString().c_str(), &column);
    if (sc::Failed(rc))
    {
        if (outError != nullptr)
        {
            *outError = ErrorToString(rc);
        }
        return false;
    }

    sc::Value storageValue;
    rc = ConvertVariantToValue(column, value, &storageValue);
    if (sc::Failed(rc))
    {
        if (outError != nullptr)
        {
            *outError = ErrorToString(rc);
        }
        return false;
    }

    const bool ok = BeginAndCommitSingleAction(
        L"Edit Cell",
        [&]()
        {
            sc::RecordPtr record;
            sc::ErrorCode getRc = currentTable_->GetRecord(recordId, record);
            if (sc::Failed(getRc))
            {
                return getRc;
            }
            return record->SetValue(columnName.toStdWString().c_str(), storageValue);
        },
        outError);

    if (ok)
    {
        emit RecordsChanged();
    }
    return ok;
}

bool DatabaseSession::GetColumnDef(const QString& columnName, sc::ColumnDef* outColumn, QString* outError) const
{
    if (outColumn == nullptr)
    {
        if (outError != nullptr)
        {
            *outError = QStringLiteral("Output column is null.");
        }
        return false;
    }
    if (!currentTable_)
    {
        if (outError != nullptr)
        {
            *outError = QStringLiteral("No table is selected.");
        }
        return false;
    }

    sc::SchemaPtr schema;
    sc::ErrorCode rc = currentTable_->GetSchema(schema);
    if (sc::Failed(rc))
    {
        if (outError != nullptr)
        {
            *outError = ErrorToString(rc);
        }
        return false;
    }

    rc = schema->FindColumn(columnName.toStdWString().c_str(), outColumn);
    if (sc::Failed(rc))
    {
        if (outError != nullptr)
        {
            *outError = ErrorToString(rc);
        }
        return false;
    }

    return true;
}

bool DatabaseSession::BuildRelationCandidates(
    const QString& targetTableName,
    QVector<RelationCandidate>* outCandidates,
    QString* outError) const
{
    if (outCandidates == nullptr)
    {
        if (outError != nullptr)
        {
            *outError = QStringLiteral("Output candidates is null.");
        }
        return false;
    }
    outCandidates->clear();

    if (!db_)
    {
        if (outError != nullptr)
        {
            *outError = QStringLiteral("No database is open.");
        }
        return false;
    }

    sc::TablePtr targetTable;
    sc::ErrorCode rc = db_->GetTable(targetTableName.toStdWString().c_str(), targetTable);
    if (sc::Failed(rc))
    {
        if (outError != nullptr)
        {
            *outError = ErrorToString(rc);
        }
        return false;
    }

    sc::SchemaPtr schema;
    rc = targetTable->GetSchema(schema);
    if (sc::Failed(rc))
    {
        if (outError != nullptr)
        {
            *outError = ErrorToString(rc);
        }
        return false;
    }

    QVector<sc::ColumnDef> columns;
    std::int32_t columnCount = 0;
    rc = schema->GetColumnCount(&columnCount);
    if (sc::Failed(rc))
    {
        if (outError != nullptr)
        {
            *outError = ErrorToString(rc);
        }
        return false;
    }

    for (std::int32_t index = 0; index < columnCount; ++index)
    {
        sc::ColumnDef column;
        rc = schema->GetColumn(index, &column);
        if (sc::Failed(rc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(rc);
            }
            return false;
        }
        columns.push_back(column);
    }

    sc::RecordCursorPtr cursor;
    rc = targetTable->EnumerateRecords(cursor);
    if (sc::Failed(rc))
    {
        if (outError != nullptr)
        {
            *outError = ErrorToString(rc);
        }
        return false;
    }

    bool hasRow = false;
    while (cursor->MoveNext(&hasRow) == sc::SC_OK && hasRow)
    {
        sc::RecordPtr record;
        rc = cursor->GetCurrent(record);
        if (sc::Failed(rc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(rc);
            }
            return false;
        }

        RelationCandidate candidate;
        candidate.recordId = record->GetId();
        candidate.previewFields.push_back(qMakePair(QStringLiteral("RecordId"), QString::number(candidate.recordId)));

        for (const sc::ColumnDef& column : columns)
        {
            if (column.valueKind == sc::ValueKind::Null)
            {
                continue;
            }

            sc::Value value;
            const sc::ErrorCode valueRc = record->GetValue(column.name.c_str(), &value);
            if (valueRc == sc::SC_E_VALUE_IS_NULL)
            {
                continue;
            }
            if (sc::Failed(valueRc))
            {
                continue;
            }

            candidate.previewFields.push_back(qMakePair(
                ToQString(column.displayName.empty() ? column.name : column.displayName),
                ValueToDisplayString(value)));
        }

        candidate.label = PickRecordLabel(candidate.previewFields, candidate.recordId);
        outCandidates->push_back(candidate);
    }

    std::sort(
        outCandidates->begin(),
        outCandidates->end(),
        [](const RelationCandidate& left, const RelationCandidate& right)
        {
            return left.label.localeAwareCompare(right.label) < 0;
        });
    return true;
}

bool DatabaseSession::AddSessionComputedColumn(const sc::ComputedColumnDef& column, QString* outError)
{
    if (!currentTableView_)
    {
        if (outError != nullptr)
        {
            *outError = QStringLiteral("No table is selected.");
        }
        return false;
    }

    QVector<sc::ComputedColumnDef>* columns = CurrentSessionComputedColumnsStorage();
    if (columns == nullptr)
    {
        if (outError != nullptr)
        {
            *outError = QStringLiteral("No table is selected.");
        }
        return false;
    }

    const QString requestedName = ToQString(column.name).trimmed();
    if (requestedName.isEmpty())
    {
        if (outError != nullptr)
        {
            *outError = QStringLiteral("Computed column name is required.");
        }
        return false;
    }

    for (const sc::ComputedColumnDef& existing : *columns)
    {
        if (ToQString(existing.name).compare(requestedName, Qt::CaseInsensitive) == 0)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("A computed column with the same name already exists.");
            }
            return false;
        }
    }

    columns->push_back(column);
    if (!RebuildCurrentTableView(outError))
    {
        columns->removeLast();
        return false;
    }

    emit CurrentTableChanged();
    emit RecordsChanged();
    return true;
}

bool DatabaseSession::UpdateSessionComputedColumn(
    const QString& originalName,
    const sc::ComputedColumnDef& column,
    QString* outError)
{
    QVector<sc::ComputedColumnDef>* columns = CurrentSessionComputedColumnsStorage();
    if (columns == nullptr)
    {
        if (outError != nullptr)
        {
            *outError = QStringLiteral("No table is selected.");
        }
        return false;
    }

    const QString originalKey = originalName.trimmed();
    const QString newName = ToQString(column.name).trimmed();
    if (originalKey.isEmpty() || newName.isEmpty())
    {
        if (outError != nullptr)
        {
            *outError = QStringLiteral("Computed column name is required.");
        }
        return false;
    }

    int targetIndex = -1;
    for (int index = 0; index < columns->size(); ++index)
    {
        const QString existingName = ToQString(columns->at(index).name);
        if (existingName.compare(originalKey, Qt::CaseInsensitive) == 0)
        {
            targetIndex = index;
        }
        else if (existingName.compare(newName, Qt::CaseInsensitive) == 0)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("A computed column with the same name already exists.");
            }
            return false;
        }
    }

    if (targetIndex < 0)
    {
        if (outError != nullptr)
        {
            *outError = QStringLiteral("The selected computed column no longer exists.");
        }
        return false;
    }

    const sc::ComputedColumnDef previous = columns->at(targetIndex);
    (*columns)[targetIndex] = column;
    if (!RebuildCurrentTableView(outError))
    {
        (*columns)[targetIndex] = previous;
        return false;
    }

    emit CurrentTableChanged();
    emit RecordsChanged();
    return true;
}

bool DatabaseSession::RemoveSessionComputedColumn(const QString& name, QString* outError)
{
    QVector<sc::ComputedColumnDef>* columns = CurrentSessionComputedColumnsStorage();
    if (columns == nullptr)
    {
        if (outError != nullptr)
        {
            *outError = QStringLiteral("No table is selected.");
        }
        return false;
    }

    const QString key = name.trimmed();
    for (int index = 0; index < columns->size(); ++index)
    {
        if (ToQString(columns->at(index).name).compare(key, Qt::CaseInsensitive) != 0)
        {
            continue;
        }

        const sc::ComputedColumnDef removed = columns->at(index);
        columns->removeAt(index);
        if (!RebuildCurrentTableView(outError))
        {
            columns->insert(index, removed);
            return false;
        }

        emit CurrentTableChanged();
        emit RecordsChanged();
        return true;
    }

    if (outError != nullptr)
    {
        *outError = QStringLiteral("The selected computed column no longer exists.");
    }
    return false;
}

bool DatabaseSession::GetSessionComputedColumn(
    const QString& name,
    sc::ComputedColumnDef* outColumn,
    QString* outError) const
{
    if (outColumn == nullptr)
    {
        if (outError != nullptr)
        {
            *outError = QStringLiteral("Output column is null.");
        }
        return false;
    }

    const QVector<sc::ComputedColumnDef>* columns = CurrentSessionComputedColumnsStorage();
    if (columns == nullptr)
    {
        if (outError != nullptr)
        {
            *outError = QStringLiteral("No table is selected.");
        }
        return false;
    }

    const QString key = name.trimmed();
    for (const sc::ComputedColumnDef& column : *columns)
    {
        if (ToQString(column.name).compare(key, Qt::CaseInsensitive) == 0)
        {
            *outColumn = column;
            return true;
        }
    }

    if (outError != nullptr)
    {
        *outError = QStringLiteral("The selected computed column no longer exists.");
    }
    return false;
}

QVector<sc::ComputedColumnDef> DatabaseSession::CurrentSessionComputedColumns() const
{
    return sessionComputedColumnsByTable_.value(currentTableName_);
}

QString DatabaseSession::BuildHealthSummary() const
{
    if (!IsOpen())
    {
        return QStringLiteral("No database is open.");
    }

    sc::StorageHealthReport report;
    sc::BuildStorageHealthReport(db_.Get(), L"SQLite", &report);

    QString summary;
    summary += QStringLiteral("Backend: ") + ToQString(report.backendName) + QLatin1Char('\n');
    summary += QStringLiteral("Version: ") + QString::number(static_cast<qulonglong>(report.currentVersion)) + QLatin1Char('\n');
    summary += QStringLiteral("Diagnostics:\n");
    for (const sc::DiagnosticEntry& entry : report.diagnostics)
    {
        summary += QStringLiteral("- [") + ToQString(entry.category) + QStringLiteral("] ") + ToQString(entry.message) + QLatin1Char('\n');
    }
    return summary;
}

bool DatabaseSession::BuildSchemaSnapshot(QVector<sc::ColumnDef>* outColumns, QString* outError) const
{
    if (outColumns == nullptr)
    {
        if (outError != nullptr)
        {
            *outError = QStringLiteral("Output container is null.");
        }
        return false;
    }
    outColumns->clear();

    if (!currentTable_)
    {
        return true;
    }

    sc::SchemaPtr schema;
    sc::ErrorCode rc = currentTable_->GetSchema(schema);
    if (sc::Failed(rc))
    {
        if (outError != nullptr)
        {
            *outError = ErrorToString(rc);
        }
        return false;
    }

    std::int32_t count = 0;
    rc = schema->GetColumnCount(&count);
    if (sc::Failed(rc))
    {
        if (outError != nullptr)
        {
            *outError = ErrorToString(rc);
        }
        return false;
    }

    for (std::int32_t index = 0; index < count; ++index)
    {
        sc::ColumnDef column;
        rc = schema->GetColumn(index, &column);
        if (sc::Failed(rc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(rc);
            }
            return false;
        }
        outColumns->push_back(column);
    }

    return true;
}

bool DatabaseSession::BuildRecordSnapshot(
    sc::RecordId recordId,
    QVector<QPair<QString, QString>>* outFields,
    QString* outError) const
{
    if (outFields == nullptr)
    {
        if (outError != nullptr)
        {
            *outError = QStringLiteral("Output container is null.");
        }
        return false;
    }
    outFields->clear();

    if (!currentTable_)
    {
        return true;
    }

    sc::RecordPtr record;
    sc::ErrorCode rc = currentTable_->GetRecord(recordId, record);
    if (sc::Failed(rc))
    {
        if (outError != nullptr)
        {
            *outError = ErrorToString(rc);
        }
        return false;
    }

    QVector<sc::ColumnDef> columns;
    if (!BuildSchemaSnapshot(&columns, outError))
    {
        return false;
    }

    outFields->push_back(qMakePair(QStringLiteral("RecordId"), QString::number(recordId)));
    outFields->push_back(qMakePair(QStringLiteral("Deleted"), record->IsDeleted() ? QStringLiteral("true") : QStringLiteral("false")));
    outFields->push_back(qMakePair(QStringLiteral("LastModifiedVersion"), QString::number(static_cast<qulonglong>(record->GetLastModifiedVersion()))));

    for (const sc::ColumnDef& column : columns)
    {
        sc::Value value;
        rc = record->GetValue(column.name.c_str(), &value);
        if (rc == sc::SC_E_VALUE_IS_NULL)
        {
            outFields->push_back(qMakePair(ToQString(column.name), QString()));
            continue;
        }
        if (sc::Failed(rc))
        {
            outFields->push_back(qMakePair(ToQString(column.name), QStringLiteral("<error>")));
            continue;
        }
        outFields->push_back(qMakePair(ToQString(column.name), ValueToDisplayString(value)));
    }

    return true;
}

QVector<sc::ComputedColumnDef>* DatabaseSession::CurrentSessionComputedColumnsStorage()
{
    return currentTableName_.isEmpty() ? nullptr : &sessionComputedColumnsByTable_[currentTableName_];
}

const QVector<sc::ComputedColumnDef>* DatabaseSession::CurrentSessionComputedColumnsStorage() const
{
    if (currentTableName_.isEmpty())
    {
        return nullptr;
    }

    const auto it = sessionComputedColumnsByTable_.find(currentTableName_);
    return it == sessionComputedColumnsByTable_.end() ? nullptr : &it.value();
}

bool DatabaseSession::LoadTableNames(QString* outError)
{
    tableNames_.clear();

    std::int32_t count = 0;
    sc::ErrorCode rc = db_->GetTableCount(&count);
    if (sc::Failed(rc))
    {
        if (outError != nullptr)
        {
            *outError = ErrorToString(rc);
        }
        return false;
    }

    for (std::int32_t index = 0; index < count; ++index)
    {
        std::wstring name;
        rc = db_->GetTableName(index, &name);
        if (sc::Failed(rc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(rc);
            }
            return false;
        }
        tableNames_.push_back(ToQString(name));
    }
    return true;
}

bool DatabaseSession::RebuildCurrentTableView(QString* outError)
{
    currentTableView_.Reset();

    sc::ComputedTableViewPtr view;
    sc::ErrorCode rc = sc::CreateComputedTableView(db_.Get(), currentTableName_.toStdWString().c_str(), nullptr, view);
    if (sc::Failed(rc))
    {
        if (outError != nullptr)
        {
            *outError = ErrorToString(rc);
        }
        return false;
    }

    const QVector<sc::ComputedColumnDef> computedColumns = sessionComputedColumnsByTable_.value(currentTableName_);
    for (const sc::ComputedColumnDef& column : computedColumns)
    {
        rc = view->AddComputedColumn(column);
        if (sc::Failed(rc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(rc);
            }
            return false;
        }
    }

    currentTableView_ = view;
    return true;
}

bool DatabaseSession::BeginAndCommitSingleAction(
    const wchar_t* actionName,
    const std::function<sc::ErrorCode()>& action,
    QString* outError)
{
    sc::EditPtr edit;
    sc::ErrorCode rc = db_->BeginEdit(actionName, edit);
    if (sc::Failed(rc))
    {
        if (outError != nullptr)
        {
            *outError = ErrorToString(rc);
        }
        return false;
    }

    rc = action();
    if (sc::Failed(rc))
    {
        db_->Rollback(edit.Get());
        if (outError != nullptr)
        {
            *outError = ErrorToString(rc);
        }
        return false;
    }

    rc = db_->Commit(edit.Get());
    if (sc::Failed(rc))
    {
        if (outError != nullptr)
        {
            *outError = ErrorToString(rc);
        }
        return false;
    }

    return true;
}

sc::ErrorCode DatabaseSession::ConvertVariantToValue(
    const sc::ColumnDef& column,
    const QVariant& input,
    sc::Value* outValue) const
{
    if (outValue == nullptr)
    {
        return sc::SC_E_POINTER;
    }

    if (!input.isValid() || input.isNull())
    {
        *outValue = sc::Value::Null();
        return sc::SC_OK;
    }

    switch (column.valueKind)
    {
    case sc::ValueKind::Int64:
        *outValue = sc::Value::FromInt64(input.toLongLong());
        return sc::SC_OK;
    case sc::ValueKind::Double:
        *outValue = sc::Value::FromDouble(input.toDouble());
        return sc::SC_OK;
    case sc::ValueKind::Bool:
        *outValue = sc::Value::FromBool(input.toBool());
        return sc::SC_OK;
    case sc::ValueKind::String:
        *outValue = sc::Value::FromString(input.toString().toStdWString());
        return sc::SC_OK;
    case sc::ValueKind::RecordId:
        *outValue = input.toString().trimmed().isEmpty()
            ? sc::Value::Null()
            : sc::Value::FromRecordId(input.toLongLong());
        return sc::SC_OK;
    case sc::ValueKind::Enum:
        *outValue = sc::Value::FromEnum(input.toString().toStdWString());
        return sc::SC_OK;
    case sc::ValueKind::Null:
    default:
        return sc::SC_E_TYPE_MISMATCH;
    }
}

QString DatabaseSession::ErrorToString(sc::ErrorCode error) const
{
    return QStringLiteral("Storage error: 0x") + QString::number(static_cast<qulonglong>(error), 16);
}

}  // namespace stablecore::storage::editor
