#include "SCDatabaseSession.h"

#include <algorithm>

#include <QIODevice>
#include <QSaveFile>
#include <QStringList>

namespace sc = StableCore::Storage;

namespace StableCore::Storage::Editor
{
    namespace
    {

        QString ToQString(const std::wstring& text)
        {
            return QString::fromStdWString(text);
        }

        bool HasComputedColumnNameConflict(
            const QHash<QString, QVector<sc::SCComputedColumnDef>>&
                computedColumnsByTable,
            const QString& tableName, const QString& columnName)
        {
            const auto it = computedColumnsByTable.constFind(tableName);
            if (it == computedColumnsByTable.constEnd())
            {
                return false;
            }

            for (const sc::SCComputedColumnDef& existing : it.value())
            {
                if (ToQString(existing.name)
                        .compare(columnName, Qt::CaseInsensitive) == 0)
                {
                    return true;
                }
            }
            return false;
        }

        QString ValueToDisplayString(const sc::SCValue& SCValue)
        {
            switch (SCValue.GetKind())
            {
                case sc::ValueKind::Null:
                    return QString();
                case sc::ValueKind::Int64: {
                    std::int64_t v = 0;
                    SCValue.AsInt64(&v);
                    return QString::number(v);
                }
                case sc::ValueKind::Double: {
                    double v = 0.0;
                    SCValue.AsDouble(&v);
                    return QString::number(v, 'g', 12);
                }
                case sc::ValueKind::Bool: {
                    bool v = false;
                    SCValue.AsBool(&v);
                    return v ? QStringLiteral("true") : QStringLiteral("false");
                }
                case sc::ValueKind::String: {
                    std::wstring v;
                    SCValue.AsStringCopy(&v);
                    return ToQString(v);
                }
                case sc::ValueKind::RecordId: {
                    sc::RecordId v = 0;
                    SCValue.AsRecordId(&v);
                    return QString::number(v);
                }
                case sc::ValueKind::Enum: {
                    std::wstring v;
                    SCValue.AsEnumCopy(&v);
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
            static const QStringList preferredNames = {QStringLiteral("Name"),
                                                       QStringLiteral("Title"),
                                                       QStringLiteral("Code")};

            for (const QString& preferred : preferredNames)
            {
                for (const auto& field : previewFields)
                {
                    if (field.first.compare(preferred, Qt::CaseInsensitive) ==
                            0 &&
                        !field.second.trimmed().isEmpty())
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

        QString MaskIfNeeded(const QString& value, bool redact)
        {
            if (!redact || value.isEmpty())
            {
                return value;
            }

            return QString(value.size(), QLatin1Char('*'));
        }

        sc::ErrorCode SaveFileWriteCallback(void* userData, const void* data,
                                            std::size_t size,
                                            std::size_t* bytesWritten)
        {
            if (userData == nullptr)
            {
                return sc::SC_E_EXPORT_INVALID_STATE;
            }

            auto* file = static_cast<QSaveFile*>(userData);
            const qint64 written = file->write(static_cast<const char*>(data),
                                               static_cast<qint64>(size));
            if (written < 0)
            {
                if (bytesWritten != nullptr)
                {
                    *bytesWritten = 0;
                }
                return sc::SC_E_EXPORT_WRITE_FAILED;
            }

            if (bytesWritten != nullptr)
            {
                *bytesWritten = static_cast<std::size_t>(written);
            }
            return written == static_cast<qint64>(size)
                       ? sc::SC_OK
                       : sc::SC_E_EXPORT_WRITE_FAILED;
        }

        bool WriteDebugPackageLine(sc::SCStreamingExportContext* context,
                                   const QString& line,
                                   const sc::SCPackageSizePolicy& sizePolicy,
                                   QString* outError)
        {
            const sc::ErrorCode rc = sc::WriteExportLine(
                context, sc::Utf8Encode(line.toStdWString()), sizePolicy);
            if (sc::Failed(rc))
            {
                if (outError != nullptr)
                {
                    if (rc == sc::SC_E_EXPORT_TOO_LARGE)
                    {
                        *outError = QStringLiteral(
                            "Export package exceeds the configured size "
                            "limit.");
                    } else if (rc == sc::SC_E_EXPORT_CANCELLED)
                    {
                        *outError =
                            QStringLiteral("Export package was cancelled.");
                    } else
                    {
                        *outError =
                            QStringLiteral("Export package write failed.");
                    }
                }
                return false;
            }

            return true;
        }

    }  // namespace

    SCDatabaseSession::SCDatabaseSession(QObject* parent) : QObject(parent)
    {
    }

    bool SCDatabaseSession::IsOpen() const noexcept
    {
        return static_cast<bool>(db_);
    }

    QString SCDatabaseSession::DatabasePath() const
    {
        return databasePath_;
    }

    QString SCDatabaseSession::CurrentTableName() const
    {
        return currentTableName_;
    }

    QStringList SCDatabaseSession::TableNames() const
    {
        return tableNames_;
    }

    sc::ISCDatabase* SCDatabaseSession::Database() const noexcept
    {
        return db_.Get();
    }

    sc::ISCTable* SCDatabaseSession::CurrentTable() const noexcept
    {
        return currentTable_.Get();
    }

    sc::ISCComputedTableView* SCDatabaseSession::CurrentTableView()
        const noexcept
    {
        return currentTableView_.Get();
    }

    bool SCDatabaseSession::CreateDatabase(const QString& filePath,
                                           QString* outError)
    {
        db_.Reset();
        currentTable_.Reset();
        currentTableView_.Reset();
        sessionComputedColumnsByTable_.clear();

        sc::SCOpenDatabaseOptions options;
        options.openMode = sc::SCDatabaseOpenMode::Normal;
        const sc::ErrorCode rc = sc::CreateFileDatabase(
            filePath.toStdWString().c_str(), options, db_);
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

    bool SCDatabaseSession::OpenDatabase(const QString& filePath,
                                         QString* outError)
    {
        return CreateDatabase(filePath, outError);
    }

    bool SCDatabaseSession::GetEditLogState(sc::SCEditLogState* outState,
                                            QString* outError) const
    {
        if (outState == nullptr)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Output edit log state is null.");
            }
            return false;
        }
        if (!db_)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("No database is open.");
            }
            return false;
        }

        const sc::ErrorCode rc = db_->GetEditLogState(outState);
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

    bool SCDatabaseSession::GetEditingState(
        sc::SCEditingDatabaseState* outState, QString* outError) const
    {
        if (outState == nullptr)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Output editing state is null.");
            }
            return false;
        }
        if (!db_)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("No database is open.");
            }
            return false;
        }

        const sc::ErrorCode rc = db_->GetEditingState(outState);
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

    bool SCDatabaseSession::Refresh(QString* outError)
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

    bool SCDatabaseSession::CreateTable(const QString& tableName,
                                        QString* outError)
    {
        if (!IsOpen())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("No database is open.");
            }
            return false;
        }

        sc::SCTablePtr table;
        const sc::ErrorCode rc =
            db_->CreateTable(tableName.toStdWString().c_str(), table);
        if (sc::Failed(rc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(rc);
            }
            return false;
        }

        if (!LoadTableNames(outError))
        {
            return false;
        }

        if (!SelectTable(tableName, outError))
        {
            return false;
        }

        emit TablesChanged();
        return true;
    }

    bool SCDatabaseSession::SelectTable(const QString& tableName,
                                        QString* outError)
    {
        if (!IsOpen())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("No database is open.");
            }
            return false;
        }

        sc::SCTablePtr table;
        sc::ErrorCode rc =
            db_->GetTable(tableName.toStdWString().c_str(), table);
        if (sc::Failed(rc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(rc);
            }
            return false;
        }

        const QString previousTableName = currentTableName_;
        sc::SCTablePtr previousTable = currentTable_;
        sc::SCComputedTableViewPtr previousTableView = currentTableView_;

        currentTable_ = table;
        currentTableName_ = tableName;
        if (!RebuildCurrentTableView(outError))
        {
            currentTable_ = previousTable;
            currentTableView_ = previousTableView;
            currentTableName_ = previousTableName;
            return false;
        }

        emit CurrentTableChanged();
        emit RecordsChanged();
        return true;
    }

    bool SCDatabaseSession::AddColumn(const sc::SCColumnDef& column,
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

        const QString requestedName = ToQString(column.name).trimmed();
        if (requestedName.isEmpty())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Column name is required.");
            }
            return false;
        }

        if (HasComputedColumnNameConflict(sessionComputedColumnsByTable_,
                                          currentTableName_, requestedName))
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral(
                    "A computed column with the same name already exists.");
            }
            return false;
        }

        sc::SCSchemaPtr schema;
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

        const sc::SCComputedTableViewPtr previousTableView = currentTableView_;
        if (currentTableView_)
        {
            if (!RebuildCurrentTableView(outError))
            {
                const sc::ErrorCode rollbackRc =
                    schema->RemoveColumn(column.name.c_str());
                if (sc::Succeeded(rollbackRc))
                {
                    currentTableView_ = previousTableView;
                } else if (outError != nullptr)
                {
                    *outError = ErrorToString(rollbackRc);
                }
                emit CurrentTableChanged();
                emit RecordsChanged();
                return false;
            }
        }

        emit CurrentTableChanged();
        emit RecordsChanged();
        return true;
    }

    bool SCDatabaseSession::AddRecord(QString* outError)
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
            [&]() {
                sc::SCRecordPtr record;
                return currentTable_->CreateRecord(record);
            },
            outError);

        if (ok)
        {
            emit RecordsChanged();
        }
        return ok;
    }

    void SCDatabaseSession::SetForceRebuildCurrentTableViewFailureForTest(
        bool enabled)
    {
        forceRebuildCurrentTableViewFailureForTest_ = enabled;
    }

    bool SCDatabaseSession::DeleteRecord(sc::RecordId recordId,
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

        const bool ok = BeginAndCommitSingleAction(
            L"Delete Record",
            [&]() { return currentTable_->DeleteRecord(recordId); }, outError);

        if (ok)
        {
            emit RecordsChanged();
        }
        return ok;
    }

    bool SCDatabaseSession::Undo(QString* outError)
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

    bool SCDatabaseSession::Redo(QString* outError)
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

    bool SCDatabaseSession::SetCellValue(sc::RecordId recordId,
                                         const QString& columnName,
                                         const QVariant& SCValue,
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

        sc::SCSchemaPtr schema;
        sc::ErrorCode rc = currentTable_->GetSchema(schema);
        if (sc::Failed(rc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(rc);
            }
            return false;
        }

        sc::SCColumnDef column;
        rc = schema->FindColumn(columnName.toStdWString().c_str(), &column);
        if (sc::Failed(rc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(rc);
            }
            return false;
        }

        sc::SCValue storageValue;
        rc = ConvertVariantToValue(column, SCValue, &storageValue);
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
            [&]() {
                sc::SCRecordPtr record;
                sc::ErrorCode getRc =
                    currentTable_->GetRecord(recordId, record);
                if (sc::Failed(getRc))
                {
                    return getRc;
                }
                return record->SetValue(columnName.toStdWString().c_str(),
                                        storageValue);
            },
            outError);

        if (ok)
        {
            emit RecordsChanged();
        }
        return ok;
    }

    bool SCDatabaseSession::GetColumnDef(const QString& columnName,
                                         sc::SCColumnDef* outColumn,
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
        if (!currentTable_)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("No table is selected.");
            }
            return false;
        }

        sc::SCSchemaPtr schema;
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

    bool SCDatabaseSession::BuildRelationCandidates(
        const QString& targetTableName,
        QVector<RelationCandidate>* outCandidates, QString* outError) const
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

        sc::SCTablePtr targetTable;
        sc::ErrorCode rc =
            db_->GetTable(targetTableName.toStdWString().c_str(), targetTable);
        if (sc::Failed(rc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(rc);
            }
            return false;
        }

        sc::SCSchemaPtr schema;
        rc = targetTable->GetSchema(schema);
        if (sc::Failed(rc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(rc);
            }
            return false;
        }

        QVector<sc::SCColumnDef> columns;
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
            sc::SCColumnDef column;
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

        sc::SCRecordCursorPtr cursor;
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
            sc::SCRecordPtr record;
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
            candidate.previewFields.push_back(
                qMakePair(QStringLiteral("RecordId"),
                          QString::number(candidate.recordId)));

            for (const sc::SCColumnDef& column : columns)
            {
                if (column.valueKind == sc::ValueKind::Null)
                {
                    continue;
                }

                sc::SCValue SCValue;
                const sc::ErrorCode valueRc =
                    record->GetValue(column.name.c_str(), &SCValue);
                if (valueRc == sc::SC_E_VALUE_IS_NULL)
                {
                    continue;
                }
                if (sc::Failed(valueRc))
                {
                    continue;
                }

                candidate.previewFields.push_back(qMakePair(
                    ToQString(column.displayName.empty() ? column.name
                                                         : column.displayName),
                    ValueToDisplayString(SCValue)));
            }

            candidate.label =
                PickRecordLabel(candidate.previewFields, candidate.recordId);
            outCandidates->push_back(candidate);
        }

        std::sort(
            outCandidates->begin(), outCandidates->end(),
            [](const RelationCandidate& left, const RelationCandidate& right) {
                return left.label.localeAwareCompare(right.label) < 0;
            });
        return true;
    }

    bool SCDatabaseSession::AddSessionComputedColumn(
        const sc::SCComputedColumnDef& column, QString* outError)
    {
        if (!currentTableView_)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("No table is selected.");
            }
            return false;
        }

        QVector<sc::SCComputedColumnDef>* columns =
            CurrentSessionComputedColumnsStorage();
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

        for (const sc::SCComputedColumnDef& existing : *columns)
        {
            if (ToQString(existing.name)
                    .compare(requestedName, Qt::CaseInsensitive) == 0)
            {
                if (outError != nullptr)
                {
                    *outError = QStringLiteral(
                        "A computed column with the same name already exists.");
                }
                return false;
            }
        }

        if (column.dependencies.factFields.empty() &&
            column.dependencies.relationFields.empty())
        {
            if (outError != nullptr)
            {
                *outError =
                    QStringLiteral("At least one dependency is required.");
            }
            return false;
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

    bool SCDatabaseSession::UpdateSessionComputedColumn(
        const QString& originalName, const sc::SCComputedColumnDef& column,
        QString* outError)
    {
        QVector<sc::SCComputedColumnDef>* columns =
            CurrentSessionComputedColumnsStorage();
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

        if (column.dependencies.factFields.empty() &&
            column.dependencies.relationFields.empty())
        {
            if (outError != nullptr)
            {
                *outError =
                    QStringLiteral("At least one dependency is required.");
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
            } else if (existingName.compare(newName, Qt::CaseInsensitive) == 0)
            {
                if (outError != nullptr)
                {
                    *outError = QStringLiteral(
                        "A computed column with the same name already exists.");
                }
                return false;
            }
        }

        if (targetIndex < 0)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral(
                    "The selected computed column no longer exists.");
            }
            return false;
        }

        const sc::SCComputedColumnDef previous = columns->at(targetIndex);
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

    bool SCDatabaseSession::RemoveSessionComputedColumn(const QString& name,
                                                        QString* outError)
    {
        QVector<sc::SCComputedColumnDef>* columns =
            CurrentSessionComputedColumnsStorage();
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
            if (ToQString(columns->at(index).name)
                    .compare(key, Qt::CaseInsensitive) != 0)
            {
                continue;
            }

            const sc::SCComputedColumnDef removed = columns->at(index);
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
            *outError = QStringLiteral(
                "The selected computed column no longer exists.");
        }
        return false;
    }

    bool SCDatabaseSession::GetSessionComputedColumn(
        const QString& name, sc::SCComputedColumnDef* outColumn,
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

        const QVector<sc::SCComputedColumnDef>* columns =
            CurrentSessionComputedColumnsStorage();
        if (columns == nullptr)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("No table is selected.");
            }
            return false;
        }

        const QString key = name.trimmed();
        for (const sc::SCComputedColumnDef& column : *columns)
        {
            if (ToQString(column.name).compare(key, Qt::CaseInsensitive) == 0)
            {
                *outColumn = column;
                return true;
            }
        }

        if (outError != nullptr)
        {
            *outError = QStringLiteral(
                "The selected computed column no longer exists.");
        }
        return false;
    }

    QVector<sc::SCComputedColumnDef>
    SCDatabaseSession::CurrentSessionComputedColumns() const
    {
        return sessionComputedColumnsByTable_.value(currentTableName_);
    }

    QString SCDatabaseSession::BuildHealthSummary() const
    {
        if (!IsOpen())
        {
            return QStringLiteral("No database is open.");
        }

        sc::SCStorageHealthReport report;
        sc::BuildStorageHealthReport(db_.Get(), L"SQLite", &report);

        QString summary;
        summary += QStringLiteral("Backend: ") + ToQString(report.backendName) +
                   QLatin1Char('\n');
        summary +=
            QStringLiteral("Version: ") +
            QString::number(static_cast<qulonglong>(report.currentVersion)) +
            QLatin1Char('\n');
        summary += QStringLiteral("Diagnostics:\n");
        for (const sc::SCDiagnosticEntry& entry : report.diagnostics)
        {
            summary += QStringLiteral("- [") + ToQString(entry.category) +
                       QStringLiteral("] ") + ToQString(entry.message) +
                       QLatin1Char('\n');
        }
        return summary;
    }

    bool SCDatabaseSession::BuildSchemaSnapshot(
        QVector<sc::SCColumnDef>* outColumns, QString* outError) const
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

        sc::SCSchemaPtr schema;
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
            sc::SCColumnDef column;
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

    bool SCDatabaseSession::BuildRecordSnapshot(
        sc::RecordId recordId, QVector<QPair<QString, QString>>* outFields,
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

        sc::SCRecordPtr record;
        sc::ErrorCode rc = currentTable_->GetRecord(recordId, record);
        if (sc::Failed(rc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(rc);
            }
            return false;
        }

        QVector<sc::SCColumnDef> columns;
        if (!BuildSchemaSnapshot(&columns, outError))
        {
            return false;
        }

        outFields->push_back(
            qMakePair(QStringLiteral("RecordId"), QString::number(recordId)));
        outFields->push_back(qMakePair(QStringLiteral("Deleted"),
                                       record->IsDeleted()
                                           ? QStringLiteral("true")
                                           : QStringLiteral("false")));
        outFields->push_back(qMakePair(QStringLiteral("LastModifiedVersion"),
                                       QString::number(static_cast<qulonglong>(
                                           record->GetLastModifiedVersion()))));

        for (const sc::SCColumnDef& column : columns)
        {
            sc::SCValue SCValue;
            rc = record->GetValue(column.name.c_str(), &SCValue);
            if (rc == sc::SC_E_VALUE_IS_NULL)
            {
                outFields->push_back(
                    qMakePair(ToQString(column.name), QString()));
                continue;
            }
            if (sc::Failed(rc))
            {
                outFields->push_back(qMakePair(ToQString(column.name),
                                               QStringLiteral("<error>")));
                continue;
            }
            outFields->push_back(qMakePair(ToQString(column.name),
                                           ValueToDisplayString(SCValue)));
        }

        return true;
    }

    QVector<sc::SCComputedColumnDef>*
    SCDatabaseSession::CurrentSessionComputedColumnsStorage()
    {
        return currentTableName_.isEmpty()
                   ? nullptr
                   : &sessionComputedColumnsByTable_[currentTableName_];
    }

    const QVector<sc::SCComputedColumnDef>*
    SCDatabaseSession::CurrentSessionComputedColumnsStorage() const
    {
        if (currentTableName_.isEmpty())
        {
            return nullptr;
        }

        const auto it = sessionComputedColumnsByTable_.find(currentTableName_);
        return it == sessionComputedColumnsByTable_.end() ? nullptr
                                                          : &it.value();
    }

    bool SCDatabaseSession::LoadTableNames(QString* outError)
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

    bool SCDatabaseSession::RebuildCurrentTableView(QString* outError)
    {
        if (forceRebuildCurrentTableViewFailureForTest_)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Forced rebuild failure for test.");
            }
            return false;
        }

        currentTableView_.Reset();

        sc::SCComputedTableViewPtr view;
        sc::ErrorCode rc = sc::CreateComputedTableView(
            db_.Get(), currentTableName_.toStdWString().c_str(), nullptr, view);
        if (sc::Failed(rc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(rc);
            }
            return false;
        }

        const QVector<sc::SCComputedColumnDef> computedColumns =
            sessionComputedColumnsByTable_.value(currentTableName_);
        for (const sc::SCComputedColumnDef& column : computedColumns)
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

    bool SCDatabaseSession::BeginAndCommitSingleAction(
        const wchar_t* actionName, const std::function<sc::ErrorCode()>& action,
        QString* outError)
    {
        sc::SCEditPtr edit;
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

    bool SCDatabaseSession::ExportDebugPackage(
        const QString& filePath, const sc::SCExportRequest& request,
        QString* outError) const
    {
        if (!IsOpen())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("No database is open.");
            }
            return false;
        }

        QSaveFile file(filePath);
        if (!file.open(QIODevice::WriteOnly | QIODevice::Text))
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral(
                    "Failed to open debug package file for writing.");
            }
            return false;
        }

        sc::SCStreamingExportContext context;
        context.write = &SaveFileWriteCallback;
        context.userData = &file;

        const sc::SCPackageSizePolicy sizePolicy =
            request.packageSize.maxBytes > 0
                ? request.packageSize
                : sc::BuildDefaultPackageSizePolicy(request.mode);
        const sc::SCRedactionPolicy redactionPolicy =
            request.redaction.redactPaths ||
                    request.redaction.redactUserNames ||
                    request.redaction.redactSensitiveText ||
                    request.redaction.redactReplayPayloads
                ? request.redaction
                : sc::BuildDefaultRedactionPolicy(request.mode);
        const sc::SCAssetSelection assets =
            request.assets.includeProject ||
                    request.assets.includeSystemConfig ||
                    request.assets.includeUserConfig ||
                    request.assets.includeReplayJournal ||
                    request.assets.includeReplaySnapshot ||
                    request.assets.includeReplaySession ||
                    request.assets.includeDiagnostics ||
                    request.assets.includeLog
                ? request.assets
                : sc::BuildDefaultAssetSelection(request.mode);

        if (!WriteDebugPackageLine(&context, QStringLiteral("[DebugPackage]"),
                                   sizePolicy, outError))
        {
            return false;
        }
        if (!WriteDebugPackageLine(&context,
                                   QStringLiteral("ExportMode=DebugPackage"),
                                   sizePolicy, outError))
        {
            return false;
        }
        if (!WriteDebugPackageLine(
                &context,
                QStringLiteral("Assets.Project=") +
                    (assets.includeProject ? QStringLiteral("true")
                                           : QStringLiteral("false")),
                sizePolicy, outError))
        {
            return false;
        }
        if (!WriteDebugPackageLine(
                &context,
                QStringLiteral("Assets.SystemConfig=") +
                    (assets.includeSystemConfig ? QStringLiteral("true")
                                                : QStringLiteral("false")),
                sizePolicy, outError))
        {
            return false;
        }
        if (!WriteDebugPackageLine(
                &context,
                QStringLiteral("Assets.UserConfig=") +
                    (assets.includeUserConfig ? QStringLiteral("true")
                                              : QStringLiteral("false")),
                sizePolicy, outError))
        {
            return false;
        }
        if (!WriteDebugPackageLine(
                &context,
                QStringLiteral("Assets.ReplayJournal=") +
                    (assets.includeReplayJournal ? QStringLiteral("true")
                                                 : QStringLiteral("false")),
                sizePolicy, outError))
        {
            return false;
        }
        if (!WriteDebugPackageLine(
                &context,
                QStringLiteral("Assets.ReplaySnapshot=") +
                    (assets.includeReplaySnapshot ? QStringLiteral("true")
                                                  : QStringLiteral("false")),
                sizePolicy, outError))
        {
            return false;
        }
        if (!WriteDebugPackageLine(
                &context,
                QStringLiteral("Assets.ReplaySession=") +
                    (assets.includeReplaySession ? QStringLiteral("true")
                                                 : QStringLiteral("false")),
                sizePolicy, outError))
        {
            return false;
        }
        if (!WriteDebugPackageLine(
                &context,
                QStringLiteral("Assets.Diagnostics=") +
                    (assets.includeDiagnostics ? QStringLiteral("true")
                                               : QStringLiteral("false")),
                sizePolicy, outError))
        {
            return false;
        }
        if (!WriteDebugPackageLine(
                &context,
                QStringLiteral("Assets.Log=") + (assets.includeLog
                                                     ? QStringLiteral("true")
                                                     : QStringLiteral("false")),
                sizePolicy, outError))
        {
            return false;
        }
        if (!WriteDebugPackageLine(&context,
                                   QStringLiteral("PackageSize.MaxBytes=") +
                                       QString::number(sizePolicy.maxBytes),
                                   sizePolicy, outError))
        {
            return false;
        }
        if (!WriteDebugPackageLine(
                &context,
                QStringLiteral("Redaction.Paths=") +
                    (redactionPolicy.redactPaths ? QStringLiteral("true")
                                                 : QStringLiteral("false")),
                sizePolicy, outError))
        {
            return false;
        }
        if (!WriteDebugPackageLine(
                &context,
                QStringLiteral("Redaction.UserNames=") +
                    (redactionPolicy.redactUserNames ? QStringLiteral("true")
                                                     : QStringLiteral("false")),
                sizePolicy, outError))
        {
            return false;
        }
        if (!WriteDebugPackageLine(&context,
                                   QStringLiteral("Redaction.SensitiveText=") +
                                       (redactionPolicy.redactSensitiveText
                                            ? QStringLiteral("true")
                                            : QStringLiteral("false")),
                                   sizePolicy, outError))
        {
            return false;
        }
        if (!WriteDebugPackageLine(&context,
                                   QStringLiteral("Redaction.ReplayPayloads=") +
                                       (redactionPolicy.redactReplayPayloads
                                            ? QStringLiteral("true")
                                            : QStringLiteral("false")),
                                   sizePolicy, outError))
        {
            return false;
        }

        if (assets.includeProject)
        {
            if (!WriteDebugPackageLine(&context, QStringLiteral("[Project]"),
                                       sizePolicy, outError))
            {
                return false;
            }
            if (!WriteDebugPackageLine(
                    &context,
                    QStringLiteral("DatabasePath=") +
                        MaskIfNeeded(databasePath_,
                                     redactionPolicy.redactPaths),
                    sizePolicy, outError))
            {
                return false;
            }
            if (!WriteDebugPackageLine(
                    &context,
                    QStringLiteral("CurrentTable=") +
                        MaskIfNeeded(currentTableName_,
                                     redactionPolicy.redactSensitiveText),
                    sizePolicy, outError))
            {
                return false;
            }
            if (!WriteDebugPackageLine(&context,
                                       QStringLiteral("TableCount=") +
                                           QString::number(tableNames_.size()),
                                       sizePolicy, outError))
            {
                return false;
            }
        }

        if (assets.includeDiagnostics)
        {
            sc::SCStorageHealthReport report;
            const sc::ErrorCode rc =
                sc::BuildStorageHealthReport(db_.Get(), L"SQLite", &report);
            if (sc::Failed(rc))
            {
                if (outError != nullptr)
                {
                    *outError = QStringLiteral(
                        "Failed to build storage health report.");
                }
                return false;
            }

            if (!WriteDebugPackageLine(&context,
                                       QStringLiteral("[Diagnostics]"),
                                       sizePolicy, outError))
            {
                return false;
            }
            if (!WriteDebugPackageLine(
                    &context,
                    QStringLiteral("Backend=") +
                        QString::fromStdWString(report.backendName),
                    sizePolicy, outError))
            {
                return false;
            }
            if (!WriteDebugPackageLine(
                    &context,
                    QStringLiteral("SchemaVersion=") +
                        QString::number(
                            static_cast<qulonglong>(report.currentVersion)),
                    sizePolicy, outError))
            {
                return false;
            }
            for (const sc::SCDiagnosticEntry& entry : report.diagnostics)
            {
                const QString severity =
                    entry.severity == sc::SCDiagnosticSeverity::Error
                        ? QStringLiteral("Error")
                    : entry.severity == sc::SCDiagnosticSeverity::Warning
                        ? QStringLiteral("Warning")
                        : QStringLiteral("Info");
                if (!WriteDebugPackageLine(
                        &context,
                        QStringLiteral("Diagnostic=") + severity +
                            QStringLiteral("|") +
                            QString::fromStdWString(entry.category) +
                            QStringLiteral("|") +
                            QString::fromStdWString(entry.message),
                        sizePolicy, outError))
                {
                    return false;
                }
            }
        }

        if (!file.commit())
        {
            if (outError != nullptr)
            {
                *outError =
                    QStringLiteral("Failed to finalize debug package file.");
            }
            return false;
        }

        return true;
    }

    sc::ErrorCode SCDatabaseSession::ConvertVariantToValue(
        const sc::SCColumnDef& column, const QVariant& input,
        sc::SCValue* outValue) const
    {
        if (outValue == nullptr)
        {
            return sc::SC_E_POINTER;
        }

        if (!input.isValid() || input.isNull())
        {
            *outValue = sc::SCValue::Null();
            return sc::SC_OK;
        }

        switch (column.valueKind)
        {
            case sc::ValueKind::Int64:
                *outValue = sc::SCValue::FromInt64(input.toLongLong());
                return sc::SC_OK;
            case sc::ValueKind::Double:
                *outValue = sc::SCValue::FromDouble(input.toDouble());
                return sc::SC_OK;
            case sc::ValueKind::Bool:
                *outValue = sc::SCValue::FromBool(input.toBool());
                return sc::SC_OK;
            case sc::ValueKind::String:
                *outValue =
                    sc::SCValue::FromString(input.toString().toStdWString());
                return sc::SC_OK;
            case sc::ValueKind::RecordId:
                *outValue = input.toString().trimmed().isEmpty()
                                ? sc::SCValue::Null()
                                : sc::SCValue::FromRecordId(input.toLongLong());
                return sc::SC_OK;
            case sc::ValueKind::Enum:
                *outValue =
                    sc::SCValue::FromEnum(input.toString().toStdWString());
                return sc::SC_OK;
            case sc::ValueKind::Null:
            default:
                return sc::SC_E_TYPE_MISMATCH;
        }
    }

    QString SCDatabaseSession::ErrorToString(sc::ErrorCode error) const
    {
        return QStringLiteral("Storage error: 0x") +
               QString::number(static_cast<qulonglong>(error), 16);
    }

}  // namespace StableCore::Storage::Editor
