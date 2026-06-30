#include "SCDatabaseSession.h"

#include <algorithm>
#include <cstdint>
#include <unordered_map>
#include <utility>
#include <vector>

#include <QByteArray>
#include <QIODevice>
#include <QMetaType>
#include <QSaveFile>
#include <QStringList>

#include "SCBinaryUtils.h"
#include "SCSchemaTableImport.h"

namespace sc = StableCore::Storage;

namespace StableCore::Storage::Editor
{
    namespace
    {

        QString ToQString(const std::wstring& text)
        {
            return QString::fromStdWString(text);
        }

        bool EqualsIgnoreCase(const std::wstring& lhs, const QString& rhs)
        {
            return QString::fromStdWString(lhs).compare(rhs, Qt::CaseInsensitive) == 0;
        }

        bool EqualsIgnoreCase(const std::wstring& lhs, const std::wstring& rhs)
        {
            return QString::fromStdWString(lhs)
                       .compare(QString::fromStdWString(rhs), Qt::CaseInsensitive) == 0;
        }

        void ReplaceDependencyFieldName(std::vector<sc::SCFieldDependency>* dependencies,
                                        const std::wstring& tableName,
                                        const std::wstring& oldFieldName,
                                        const std::wstring& newFieldName)
        {
            if (dependencies == nullptr)
            {
                return;
            }

            for (sc::SCFieldDependency& dependency : *dependencies)
            {
                if (EqualsIgnoreCase(dependency.tableName,
                                     QString::fromStdWString(tableName)) &&
                    EqualsIgnoreCase(dependency.fieldName,
                                     QString::fromStdWString(oldFieldName)))
                {
                    dependency.fieldName = newFieldName;
                }
            }
        }

        void ReplaceDependencyTableName(std::vector<sc::SCFieldDependency>* dependencies,
                                        const std::wstring& oldTableName,
                                        const std::wstring& newTableName)
        {
            if (dependencies == nullptr)
            {
                return;
            }

            for (sc::SCFieldDependency& dependency : *dependencies)
            {
                if (EqualsIgnoreCase(dependency.tableName,
                                     QString::fromStdWString(oldTableName)))
                {
                    dependency.tableName = newTableName;
                }
            }
        }

        bool TryGetRenameTableChange(const sc::SCDataChange& change,
                                    QString* outFromName,
                                    QString* outToName)
        {
            if (!change.structuralChange ||
                change.kind != sc::ChangeKind::TableRenamed ||
                change.oldValue.IsNull() ||
                change.newValue.IsNull())
            {
                return false;
            }

            std::wstring fromName;
            std::wstring toName;
            if (sc::Failed(change.oldValue.AsStringCopy(&fromName)) ||
                sc::Failed(change.newValue.AsStringCopy(&toName)))
            {
                return false;
            }

            const QString from = ToQString(fromName);
            const QString to = ToQString(toName);
            if (from.isEmpty() || to.isEmpty() ||
                from.compare(to, Qt::CaseInsensitive) == 0)
            {
                return false;
            }

            if (outFromName != nullptr)
            {
                *outFromName = from;
            }
            if (outToName != nullptr)
            {
                *outToName = to;
            }
            return true;
        }

        void RewriteComputedColumnsForTableRename(
            QHash<QString, QVector<sc::SCComputedColumnDef>>* computedColumnsByTable,
            const QString& fromName,
            const QString& toName)
        {
            if (computedColumnsByTable == nullptr ||
                fromName.compare(toName, Qt::CaseInsensitive) == 0)
            {
                return;
            }

            QHash<QString, QVector<sc::SCComputedColumnDef>> rewritten;
            const std::wstring fromNameW = fromName.toStdWString();
            const std::wstring toNameW = toName.toStdWString();

            for (auto it = computedColumnsByTable->constBegin();
                 it != computedColumnsByTable->constEnd(); ++it)
            {
                QString key = it.key();
                if (key.compare(fromName, Qt::CaseInsensitive) == 0)
                {
                    key = toName;
                }

                QVector<sc::SCComputedColumnDef> columns = it.value();
                for (sc::SCComputedColumnDef& column : columns)
                {
                    ReplaceDependencyTableName(
                        &column.dependencies.factFields, fromNameW, toNameW);
                    ReplaceDependencyTableName(
                        &column.dependencies.relationFields, fromNameW, toNameW);
                }
                rewritten.insert(key, std::move(columns));
            }

            *computedColumnsByTable = std::move(rewritten);
        }

        bool ApplyObservedRenameChangesToComputedColumns(
            QHash<QString, QVector<sc::SCComputedColumnDef>>* computedColumnsByTable,
            const sc::SCChangeSet& changeSet)
        {
            if (computedColumnsByTable == nullptr)
            {
                return false;
            }

            bool rewritten = false;
            for (const sc::SCDataChange& change : changeSet.changes)
            {
                QString fromName;
                QString toName;
                if (!TryGetRenameTableChange(change, &fromName, &toName))
                {
                    continue;
                }

                RewriteComputedColumnsForTableRename(computedColumnsByTable,
                                                     fromName, toName);
                rewritten = true;
            }
            return rewritten;
        }

        QString RewriteTableNameForObservedRenames(
            const QString& tableName,
            const sc::SCChangeSet& changeSet)
        {
            QString rewrittenName = tableName;
            if (rewrittenName.isEmpty())
            {
                return rewrittenName;
            }

            for (const sc::SCDataChange& change : changeSet.changes)
            {
                QString fromName;
                QString toName;
                if (!TryGetRenameTableChange(change, &fromName, &toName))
                {
                    continue;
                }
                if (rewrittenName.compare(fromName, Qt::CaseInsensitive) == 0)
                {
                    rewrittenName = toName;
                }
            }
            return rewrittenName;
        }

        class SessionDatabaseObserver;

        QHash<const SCDatabaseSession*, sc::SCChangeSet>& ObservedChangeSets();
        std::unordered_map<const SCDatabaseSession*, SessionDatabaseObserver*>&
        SessionObservers();

        class SessionDatabaseObserver final : public QObject,
                                             public sc::ISCDatabaseObserver
        {
        public:
            SessionDatabaseObserver(SCDatabaseSession* session, sc::SCDbPtr db)
                : QObject(session), session_(session), db_(std::move(db))
            {
            }

            ~SessionDatabaseObserver() override
            {
                DetachFromDatabase();
                ObservedChangeSets().remove(session_);
                SessionObservers().erase(session_);
            }

            void DetachFromDatabase()
            {
                if (db_)
                {
                    db_->RemoveObserver(this);
                    db_.Reset();
                }
            }

            void OnDatabaseChanged(const sc::SCChangeSet& changeSet) override;

        private:
            SCDatabaseSession* session_{nullptr};
            sc::SCDbPtr db_;
        };

        QHash<const SCDatabaseSession*, sc::SCChangeSet>& ObservedChangeSets()
        {
            static QHash<const SCDatabaseSession*, sc::SCChangeSet> changeSets;
            return changeSets;
        }

        std::unordered_map<const SCDatabaseSession*, SessionDatabaseObserver*>&
        SessionObservers()
        {
            static std::unordered_map<const SCDatabaseSession*,
                                      SessionDatabaseObserver*>
                observers;
            return observers;
        }

        void SessionDatabaseObserver::OnDatabaseChanged(
            const sc::SCChangeSet& changeSet)
        {
            if (session_ != nullptr)
            {
                ObservedChangeSets().insert(session_, changeSet);
            }
        }

        bool RegisterSessionDatabaseObserver(SCDatabaseSession* session,
                                             const sc::SCDbPtr& db,
                                             QString* outError)
        {
            if (session == nullptr || !db)
            {
                return false;
            }

            auto* observer = new SessionDatabaseObserver(session, db);
            const sc::ErrorCode rc = db->AddObserver(observer);
            if (sc::Failed(rc))
            {
                delete observer;
                if (outError != nullptr)
                {
                    *outError = QStringLiteral("Failed to register database observer: ") +
                                QString::number(static_cast<int>(rc), 16);
                }
                return false;
            }

            SessionObservers().emplace(session, observer);
            return true;
        }

        void UnregisterSessionDatabaseObserver(SCDatabaseSession* session,
                                               const sc::SCDbPtr& db)
        {
            auto& observers = SessionObservers();
            const auto observerIt = observers.find(session);
            if (db && observerIt != observers.end())
            {
                observerIt->second->DetachFromDatabase();
            }
            if (observerIt != observers.end())
            {
                delete observerIt->second;
            }
            ObservedChangeSets().remove(session);
        }

        QString CanonicalizeTableName(const QStringList& tableNames,
                                      const QString& requestedName)
        {
            for (const QString& existingName : tableNames)
            {
                if (existingName.compare(requestedName, Qt::CaseInsensitive) == 0)
                {
                    return existingName;
                }
            }
            return requestedName;
        }

        class PreviewSchema final : public sc::ISCSchema,
                                    public sc::SCRefCountedObject
        {
        public:
            explicit PreviewSchema(std::vector<sc::SCColumnDef> columns)
                : columns_(std::move(columns))
            {
            }

            sc::ErrorCode GetColumnCount(std::int32_t* outCount) override
            {
                if (outCount == nullptr)
                {
                    return sc::SC_E_POINTER;
                }
                *outCount = static_cast<std::int32_t>(columns_.size());
                return sc::SC_OK;
            }

            sc::ErrorCode GetColumn(std::int32_t index,
                                    sc::SCColumnDef* outDef) override
            {
                if (outDef == nullptr)
                {
                    return sc::SC_E_POINTER;
                }
                if (index < 0 ||
                    static_cast<std::size_t>(index) >= columns_.size())
                {
                    return sc::SC_E_INVALIDARG;
                }
                *outDef = columns_[static_cast<std::size_t>(index)];
                return sc::SC_OK;
            }

            sc::ErrorCode FindColumn(const wchar_t* name,
                                     sc::SCColumnDef* outDef) override
            {
                if (name == nullptr)
                {
                    return sc::SC_E_INVALIDARG;
                }
                if (outDef == nullptr)
                {
                    return sc::SC_E_POINTER;
                }

                const QString requestedName = QString::fromWCharArray(name);
                for (const sc::SCColumnDef& column : columns_)
                {
                    if (ToQString(column.name)
                            .compare(requestedName, Qt::CaseInsensitive) == 0)
                    {
                        *outDef = column;
                        return sc::SC_OK;
                    }
                }

                return sc::SC_E_COLUMN_NOT_FOUND;
            }

            sc::ErrorCode GetSchemaSnapshot(
                sc::SCTableSchemaSnapshot* outSnapshot) override
            {
                if (outSnapshot == nullptr)
                {
                    return sc::SC_E_POINTER;
                }

                outSnapshot->table.name.clear();
                outSnapshot->table.description.clear();
                outSnapshot->columns = columns_;
                outSnapshot->constraints.clear();
                outSnapshot->indexes.clear();

                return sc::SC_OK;
            }

            sc::ErrorCode AddColumn(const sc::SCColumnDef&) override
            {
                return sc::SC_E_NOTIMPL;
            }

            sc::ErrorCode UpdateColumn(const sc::SCColumnDef&) override
            {
                return sc::SC_E_NOTIMPL;
            }

            sc::ErrorCode RemoveColumn(const wchar_t*) override
            {
                return sc::SC_E_NOTIMPL;
            }

        private:
            std::vector<sc::SCColumnDef> columns_;
        };

        class PreviewTable final : public sc::ISCTable,
                                   public sc::SCRefCountedObject
        {
        public:
            PreviewTable(sc::SCTablePtr inner, sc::SCSchemaPtr schema)
                : inner_(std::move(inner)), schema_(std::move(schema))
            {
            }

            sc::ErrorCode GetRecord(sc::RecordId id,
                                    sc::SCRecordPtr& outRecord) override
            {
                return inner_->GetRecord(id, outRecord);
            }
            sc::ErrorCode CreateRecord(sc::SCRecordPtr& outRecord) override
            {
                return inner_->CreateRecord(outRecord);
            }
            sc::ErrorCode DeleteRecord(sc::RecordId id) override
            {
                return inner_->DeleteRecord(id);
            }
            sc::ErrorCode GetSchema(sc::SCSchemaPtr& outSchema) override
            {
                outSchema = schema_;
                return sc::SC_OK;
            }
            sc::ErrorCode EnumerateRecords(
                sc::SCRecordCursorPtr& outCursor) override
            {
                return inner_->EnumerateRecords(outCursor);
            }
            sc::ErrorCode FindRecords(const sc::SCQueryCondition& condition,
                                      sc::SCRecordCursorPtr& outCursor) override
            {
                return inner_->FindRecords(condition, outCursor);
            }

        private:
            sc::SCTablePtr inner_;
            sc::SCSchemaPtr schema_;
        };

        class PreviewDatabase final : public sc::ISCDatabase,
                                      public sc::SCRefCountedObject
        {
        public:
            PreviewDatabase(sc::SCDbPtr inner, std::wstring tableName,
                            sc::SCTablePtr previewTable)
                : inner_(std::move(inner)),
                  tableName_(std::move(tableName)),
                  previewTable_(std::move(previewTable))
            {
            }

            sc::ErrorCode BeginEdit(const wchar_t* name,
                                    sc::SCEditPtr& outEdit) override
            {
                return inner_->BeginEdit(name, outEdit);
            }
            sc::ErrorCode Commit(sc::ISCEditSession* edit) override
            {
                return inner_->Commit(edit);
            }
            sc::ErrorCode Rollback(sc::ISCEditSession* edit) override
            {
                return inner_->Rollback(edit);
            }
            sc::ErrorCode Undo() override
            {
                return inner_->Undo();
            }
            sc::ErrorCode Redo() override
            {
                return inner_->Redo();
            }
            sc::ErrorCode GetTableCount(std::int32_t* outCount) override
            {
                return inner_->GetTableCount(outCount);
            }
            sc::ErrorCode GetTableName(std::int32_t index,
                                       std::wstring* outName) override
            {
                return inner_->GetTableName(index, outName);
            }
            sc::ErrorCode GetTable(const wchar_t* name,
                                   sc::SCTablePtr& outTable) override
            {
                if (name != nullptr && tableName_ == name)
                {
                    outTable = previewTable_;
                    return sc::SC_OK;
                }
                return inner_->GetTable(name, outTable);
            }
            sc::ErrorCode CreateTable(const wchar_t* name,
                                      sc::SCTablePtr& outTable) override
            {
                return inner_->CreateTable(name, outTable);
            }
            sc::ErrorCode DeleteTable(const wchar_t* name) override
            {
                return inner_->DeleteTable(name);
            }
            sc::ErrorCode RenameTable(const wchar_t* originalName,
                                      const wchar_t* newName) override
            {
                return inner_->RenameTable(originalName, newName);
            }
            sc::ErrorCode ClearColumnValues(sc::ISCTable* table,
                                            const wchar_t* name) override
            {
                return inner_->ClearColumnValues(table, name);
            }
            sc::ErrorCode ExecuteUpgradePlan(
                const sc::SCUpgradePlan& plan, bool confirmed,
                sc::SCUpgradeResult* outResult) override
            {
                return inner_->ExecuteUpgradePlan(plan, confirmed, outResult);
            }
            sc::ErrorCode BeginImportSession(
                const sc::SCImportSessionOptions& options,
                sc::SCImportStagingArea* outSession) override
            {
                return inner_->BeginImportSession(options, outSession);
            }
            sc::ErrorCode AppendImportChunk(
                sc::SCImportStagingArea* session,
                const sc::SCImportChunk& chunk,
                sc::SCImportCheckpoint* outCheckpoint) override
            {
                return inner_->AppendImportChunk(session, chunk, outCheckpoint);
            }
            sc::ErrorCode LoadImportRecoveryState(
                std::uint64_t sessionId,
                sc::SCImportRecoveryState* outState) override
            {
                return inner_->LoadImportRecoveryState(sessionId, outState);
            }
            sc::ErrorCode FinalizeImportSession(
                const sc::SCImportFinalizeCommit& commit,
                sc::SCImportRecoveryState* outState) override
            {
                return inner_->FinalizeImportSession(commit, outState);
            }
            sc::ErrorCode AbortImportSession(std::uint64_t sessionId) override
            {
                return inner_->AbortImportSession(sessionId);
            }
            sc::ErrorCode AddObserver(
                sc::ISCDatabaseObserver* observer) override
            {
                return inner_->AddObserver(observer);
            }
            sc::ErrorCode RemoveObserver(
                sc::ISCDatabaseObserver* observer) override
            {
                return inner_->RemoveObserver(observer);
            }
            sc::ErrorCode CreateBackupCopy(
                const wchar_t* targetPath, const sc::SCBackupOptions& options,
                sc::SCBackupResult* outResult) override
            {
                return inner_->CreateBackupCopy(targetPath, options, outResult);
            }
            sc::VersionId GetCurrentVersion() const noexcept override
            {
                return inner_->GetCurrentVersion();
            }
            std::int32_t GetSchemaVersion() const noexcept override
            {
                return inner_->GetSchemaVersion();
            }

        private:
            sc::SCDbPtr inner_;
            std::wstring tableName_;
            sc::SCTablePtr previewTable_;
        };

        bool BuildComputedTableView(
            sc::ISCDatabase* database, const std::wstring& tableName,
            const QVector<sc::SCComputedColumnDef>& computedColumns,
            const std::function<QString(sc::ErrorCode)>& errorToString,
            sc::SCComputedTableViewPtr* outView, QString* outError)
        {
            if (database == nullptr || tableName.empty())
            {
                if (outError != nullptr)
                {
                    *outError = QStringLiteral("未打开数据库。");
                }
                return false;
            }
            if (outView == nullptr)
            {
                if (outError != nullptr)
                {
                    *outError = QStringLiteral("Output view is null.");
                }
                return false;
            }

            sc::SCComputedTableViewPtr view;
            sc::ErrorCode rc = sc::CreateComputedTableView(
                database, tableName.c_str(), nullptr, view);
            if (sc::Failed(rc))
            {
                if (outError != nullptr)
                {
                    *outError = errorToString(rc);
                }
                return false;
            }

            for (const sc::SCComputedColumnDef& column : computedColumns)
            {
                rc = view->AddComputedColumn(column);
                if (sc::Failed(rc))
                {
                    if (outError != nullptr)
                    {
                        *outError = errorToString(rc);
                    }
                    return false;
                }
            }

            *outView = view;
            return true;
        }

        bool BuildComputedTableViewForState(
            const sc::SCDbPtr& db,
            const sc::SCTablePtr& currentTable,
            const QString& tableName,
            const QHash<QString, QVector<sc::SCComputedColumnDef>>&
                computedColumnsByTable,
            bool forceFailureForTest,
            const std::function<QString(sc::ErrorCode)>& errorToString,
            sc::SCComputedTableViewPtr* outView,
            QString* outError)
        {
            if (outView == nullptr)
            {
                if (outError != nullptr)
                {
                    *outError = QStringLiteral("Output view is null.");
                }
                return false;
            }
            outView->Reset();

            if (forceFailureForTest)
            {
                if (outError != nullptr)
                {
                    *outError = QStringLiteral("Forced rebuild failure for test.");
                }
                return false;
            }

            if (!db || !currentTable || tableName.isEmpty())
            {
                if (outError != nullptr)
                {
                    *outError = QStringLiteral("No table is selected.");
                }
                return false;
            }

            sc::SCSchemaPtr schema;
            const sc::ErrorCode schemaRc = currentTable->GetSchema(schema);
            if (sc::Failed(schemaRc))
            {
                if (outError != nullptr)
                {
                    *outError = errorToString(schemaRc);
                }
                return false;
            }

            sc::SCTablePtr previewTable =
                sc::SCMakeRef<PreviewTable>(currentTable, schema);
            sc::SCDbPtr previewDb = sc::SCMakeRef<PreviewDatabase>(
                db, tableName.toStdWString(), previewTable);

            const QVector<sc::SCComputedColumnDef> computedColumns =
                computedColumnsByTable.value(tableName);
            return BuildComputedTableView(
                previewDb.Get(), tableName.toStdWString(), computedColumns,
                errorToString, outView, outError);
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
                case sc::ValueKind::Binary: {
                    std::vector<std::uint8_t> v;
                    if (SCValue.AsBinaryCopy(&v) == sc::SC_OK)
                    {
                        return BinaryToHex(v);
                    }
                    break;
                }
                default:
                    break;
            }

            return QStringLiteral("<unsupported>");
        }

        QVariant ValueToVariant(const sc::SCValue& SCValue)
        {
            switch (SCValue.GetKind())
            {
                case sc::ValueKind::Null:
                    return QVariant{};
                case sc::ValueKind::Int64: {
                    std::int64_t v = 0;
                    if (SCValue.AsInt64(&v) == sc::SC_OK)
                    {
                        return QVariant::fromValue<qlonglong>(v);
                    }
                    break;
                }
                case sc::ValueKind::Double: {
                    double v = 0.0;
                    if (SCValue.AsDouble(&v) == sc::SC_OK)
                    {
                        return v;
                    }
                    break;
                }
                case sc::ValueKind::Bool: {
                    bool v = false;
                    if (SCValue.AsBool(&v) == sc::SC_OK)
                    {
                        return v;
                    }
                    break;
                }
                case sc::ValueKind::String: {
                    std::wstring v;
                    if (SCValue.AsStringCopy(&v) == sc::SC_OK)
                    {
                        return ToQString(v);
                    }
                    break;
                }
                case sc::ValueKind::RecordId: {
                    sc::RecordId v = 0;
                    if (SCValue.AsRecordId(&v) == sc::SC_OK)
                    {
                        return QVariant::fromValue<qlonglong>(v);
                    }
                    break;
                }
                case sc::ValueKind::Enum: {
                    std::wstring v;
                    if (SCValue.AsEnumCopy(&v) == sc::SC_OK)
                    {
                        return ToQString(v);
                    }
                    break;
                }
                case sc::ValueKind::Binary: {
                    std::vector<std::uint8_t> v;
                    if (SCValue.AsBinaryCopy(&v) == sc::SC_OK)
                    {
                        return BinaryToHex(v);
                    }
                    break;
                }
                default:
                    break;
            }
            return QVariant{};
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

        struct ColumnValueSnapshot
        {
            sc::RecordId recordId{0};
            sc::SCValue value;
        };

        bool SnapshotColumnValues(sc::ISCTable* table,
                                  const QString& columnName,
                                  QVector<ColumnValueSnapshot>* outValues,
                                  QString* outError)
        {
            if (outValues == nullptr)
            {
                if (outError != nullptr)
                {
                    *outError =
                        QStringLiteral("Output snapshot container is null.");
                }
                return false;
            }
            outValues->clear();
            if (table == nullptr)
            {
                if (outError != nullptr)
                {
                    *outError = QStringLiteral("No table is selected.");
                }
                return false;
            }

            sc::SCRecordCursorPtr cursor;
            const sc::ErrorCode rc = table->EnumerateRecords(cursor);
            if (sc::Failed(rc))
            {
                if (outError != nullptr)
                {
                    *outError =
                        QStringLiteral("Failed to enumerate records: ") +
                        QString::number(static_cast<qulonglong>(rc), 16);
                }
                return false;
            }

            sc::SCRecordPtr record;
            while (cursor->Next(record) == sc::SC_OK && record)
            {
                if (record->IsDeleted())
                {
                    continue;
                }

                ColumnValueSnapshot snapshot;
                snapshot.recordId = record->GetId();
                const sc::ErrorCode valueRc = record->GetValue(
                    columnName.toStdWString().c_str(), &snapshot.value);
                if (valueRc == sc::SC_E_VALUE_IS_NULL)
                {
                    snapshot.value = sc::SCValue::Null();
                } else if (sc::Failed(valueRc))
                {
                    if (outError != nullptr)
                    {
                        *outError =
                            QStringLiteral("Failed to read column value: ") +
                            QString::number(static_cast<qulonglong>(valueRc),
                                            16);
                    }
                    return false;
                }

                outValues->push_back(snapshot);
            }

            return true;
        }

        bool RestoreColumnValues(sc::SCDbPtr db, sc::ISCTable* table,
                                 const QString& columnName,
                                 const QVector<ColumnValueSnapshot>& snapshots,
                                 QString* outError)
        {
            if (snapshots.isEmpty())
            {
                return true;
            }
            if (!db || table == nullptr)
            {
                if (outError != nullptr)
                {
                    *outError = QStringLiteral("未打开数据库。");
                }
                return false;
            }

            sc::SCEditPtr edit;
            const sc::ErrorCode beginRc =
                db->BeginEdit(L"Restore Column Values", edit);
            if (sc::Failed(beginRc))
            {
                if (outError != nullptr)
                {
                    *outError =
                        QStringLiteral("Failed to begin restore edit: ") +
                        QString::number(static_cast<qulonglong>(beginRc), 16);
                }
                return false;
            }

            for (const ColumnValueSnapshot& snapshot : snapshots)
            {
                sc::SCRecordPtr record;
                const sc::ErrorCode getRc =
                    table->GetRecord(snapshot.recordId, record);
                if (sc::Failed(getRc))
                {
                    db->Rollback(edit.Get());
                    if (outError != nullptr)
                    {
                        *outError =
                            QStringLiteral(
                                "Failed to reload record during restore: ") +
                            QString::number(static_cast<qulonglong>(getRc), 16);
                    }
                    return false;
                }

                const sc::ErrorCode setRc = record->SetValue(
                    columnName.toStdWString().c_str(), snapshot.value);
                if (sc::Failed(setRc))
                {
                    db->Rollback(edit.Get());
                    if (outError != nullptr)
                    {
                        *outError =
                            QStringLiteral("Failed to restore column value: ") +
                            QString::number(static_cast<qulonglong>(setRc), 16);
                    }
                    return false;
                }
            }

            const sc::ErrorCode commitRc = db->Commit(edit.Get());
            if (sc::Failed(commitRc))
            {
                if (outError != nullptr)
                {
                    *outError =
                        QStringLiteral("Failed to commit restore edit: ") +
                        QString::number(static_cast<qulonglong>(commitRc), 16);
                }
                return false;
            }

            return true;
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

        bool RollbackEditAndReport(
            sc::SCDbPtr db, sc::ISCEditSession* edit,
            const QString& primaryError, const QString& context,
            const std::function<QString(sc::ErrorCode)>& formatError,
            QString* outError)
        {
            if (db == nullptr || edit == nullptr)
            {
                if (outError != nullptr)
                {
                    *outError = primaryError +
                                QStringLiteral(" (rollback unavailable)");
                }
                return false;
            }

            const sc::ErrorCode rollbackRc = db->Rollback(edit);
            if (sc::Failed(rollbackRc))
            {
                if (outError != nullptr)
                {
                    *outError =
                        (context.isEmpty()
                             ? primaryError
                             : context + QStringLiteral(": ") + primaryError) +
                        QStringLiteral(" (rollback failed: ") +
                        formatError(rollbackRc) + QStringLiteral(")");
                }
                return false;
            }

            if (outError != nullptr)
            {
                *outError = primaryError;
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
        CloseDatabase(nullptr);

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

        if (!RegisterSessionDatabaseObserver(this, db_, outError))
        {
            CloseDatabase(nullptr);
            return false;
        }

        databasePath_ = filePath;
        currentTableName_.clear();
        if (!LoadTableNames(outError))
        {
            CloseDatabase(nullptr);
            return false;
        }
        emit DatabaseOpened();
        emit TablesChanged();
        return true;
    }

    bool SCDatabaseSession::OpenDatabase(const QString& filePath,
                                         QString* outError)
    {
        return CreateDatabase(filePath, outError);
    }

    bool SCDatabaseSession::CloseDatabase(QString* outError)
    {
        if (pendingEdit_ && db_)
        {
            const sc::ErrorCode rollbackRc = db_->Rollback(pendingEdit_.Get());
            if (sc::Failed(rollbackRc))
            {
                if (outError != nullptr)
                {
                    *outError = ErrorToString(rollbackRc);
                }
                return false;
            }
            pendingEdit_.Reset();
        }

        if (db_)
        {
            UnregisterSessionDatabaseObserver(this, db_);
        }

        if (!db_ && currentTable_.Get() == nullptr &&
            currentTableView_.Get() == nullptr && databasePath_.isEmpty() &&
            currentTableName_.isEmpty() && tableNames_.isEmpty() &&
            sessionComputedColumnsByTable_.isEmpty() && !importSessionActive_)
        {
            if (outError != nullptr)
            {
                outError->clear();
            }
            return true;
        }

        if (importSessionActive_)
        {
            importSession_ = sc::SCImportStagingArea{};
            importSessionActive_ = false;
        }

        db_.Reset();
        currentTable_.Reset();
        currentTableView_.Reset();
        sessionComputedColumnsByTable_.clear();
        tableNames_.clear();
        databasePath_.clear();
        currentTableName_.clear();
        if (outError != nullptr)
        {
            outError->clear();
        }
        return true;
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
                *outError = QStringLiteral("未打开数据库。");
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
                *outError = QStringLiteral("未打开数据库。");
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
                *outError = QStringLiteral("未打开数据库。");
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
                *outError = QStringLiteral("未打开数据库。");
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

    bool SCDatabaseSession::CreateTableFromSchema(
        const SCSchemaTableImportResult& schema, QString* outError)
    {
        if (!IsOpen())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("未打开数据库。");
            }
            return false;
        }

        const QString tableName = schema.tableName.trimmed();
        if (tableName.isEmpty())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Table name is required.");
            }
            return false;
        }

        if (schema.columns.isEmpty())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("No columns were provided.");
            }
            return false;
        }

        sc::SCTablePtr existingTable;
        const sc::ErrorCode lookupRc =
            db_->GetTable(tableName.toStdWString().c_str(), existingTable);
        if (lookupRc == sc::SC_OK)
        {
            if (outError != nullptr)
            {
                *outError =
                    QStringLiteral("Table already exists: ") + tableName;
            }
            return false;
        }
        if (lookupRc != sc::SC_E_TABLE_NOT_FOUND)
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(lookupRc);
            }
            return false;
        }

        const QString previousTableName = currentTableName_;
        QStringList seenColumns;
        for (const sc::SCColumnDef& column : schema.columns)
        {
            const QString columnName = ToQString(column.name).trimmed();
            if (columnName.isEmpty())
            {
                if (outError != nullptr)
                {
                    *outError = QStringLiteral("A column name is empty.");
                }
                return false;
            }
            if (seenColumns.contains(columnName, Qt::CaseInsensitive))
            {
                if (outError != nullptr)
                {
                    *outError =
                        QStringLiteral("Duplicate column name: ") + columnName;
                }
                return false;
            }
            seenColumns.push_back(columnName);
        }

        QVector<sc::SCConstraintDef> sortedConstraints = schema.constraints;
        std::stable_sort(
            sortedConstraints.begin(), sortedConstraints.end(),
            [](const sc::SCConstraintDef& lhs, const sc::SCConstraintDef& rhs) {
                const auto rank = [](sc::SCConstraintKind kind) {
                    switch (kind)
                    {
                        case sc::SCConstraintKind::PrimaryKey:
                            return 0;
                        case sc::SCConstraintKind::Unique:
                            return 1;
                        case sc::SCConstraintKind::Check:
                            return 2;
                        case sc::SCConstraintKind::ForeignKey:
                            return 3;
                        default:
                            return 4;
                    }
                };
                return rank(lhs.kind) < rank(rhs.kind);
            });

        QString createError;
        if (!CreateTable(tableName, &createError))
        {
            if (outError != nullptr)
            {
                *outError = createError;
            }
            return false;
        }

        const auto cleanupImportedTable = [this, &previousTableName,
                                           &tableName](QString* outCleanupError) {
            QString deleteError;
            const bool deleted = DeleteTable(tableName, &deleteError);

            QString restoreError;
            if (!previousTableName.isEmpty() &&
                previousTableName.compare(tableName, Qt::CaseInsensitive) != 0)
            {
                SelectTable(previousTableName, &restoreError);
            }

            if (outCleanupError != nullptr)
            {
                if (!deleted)
                {
                    *outCleanupError = QStringLiteral(
                                           "cleanup delete failed: ") +
                                       deleteError;
                    if (!restoreError.isEmpty())
                    {
                        *outCleanupError += QStringLiteral(
                                                " (restore selection failed: ") +
                                            restoreError + QStringLiteral(")");
                    }
                } else if (!restoreError.isEmpty())
                {
                    *outCleanupError = QStringLiteral(
                                           "restore selection failed: ") +
                                       restoreError;
                } else
                {
                    outCleanupError->clear();
                }
            }

            return deleted;
        };

        for (const sc::SCColumnDef& column : schema.columns)
        {
            QString addError;
            if (!AddColumn(column, &addError))
            {
                QString cleanupError;
                cleanupImportedTable(&cleanupError);

                Refresh(nullptr);
                if (outError != nullptr)
                {
                    *outError =
                        QStringLiteral("Failed to add imported column \"") +
                        ToQString(column.name) + QStringLiteral("\": ") +
                        addError;
                    if (!cleanupError.isEmpty())
                    {
                        *outError += QStringLiteral(" (") + cleanupError +
                                     QStringLiteral(")");
                    }
                }
                return false;
            }
        }

        for (const sc::SCConstraintDef& constraint : sortedConstraints)
        {
            QString addError;
            if (!AddConstraint(constraint, &addError))
            {
                QString cleanupError;
                cleanupImportedTable(&cleanupError);

                Refresh(nullptr);
                if (outError != nullptr)
                {
                    *outError =
                        QStringLiteral("Failed to add imported constraint \"") +
                        ToQString(constraint.name) + QStringLiteral("\": ") +
                        addError;
                    if (!cleanupError.isEmpty())
                    {
                        *outError += QStringLiteral(" (") + cleanupError +
                                     QStringLiteral(")");
                    }
                }
                return false;
            }
        }

        for (const SCSchemaTableImportIndex& index : schema.indexes)
        {
            sc::SCIndexDef indexDef;
            indexDef.name = index.name.toStdWString();
            indexDef.sourceKind = sc::SCSchemaSourceKind::Explicit;
            for (const sc::SCIndexColumnDef& column : index.columns)
            {
                indexDef.columns.push_back(column);
            }

            QString addError;
            if (!AddIndex(indexDef, &addError))
            {
                QString cleanupError;
                cleanupImportedTable(&cleanupError);

                Refresh(nullptr);
                if (outError != nullptr)
                {
                    *outError =
                        QStringLiteral("Failed to add imported index \"") +
                        index.name + QStringLiteral("\": ") + addError;
                    if (!cleanupError.isEmpty())
                    {
                        *outError += QStringLiteral(" (") + cleanupError +
                                     QStringLiteral(")");
                    }
                }
                return false;
            }
        }

        return true;
    }

    bool SCDatabaseSession::DeleteTable(const QString& tableName,
                                        QString* outError)
    {
        if (!IsOpen())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("未打开数据库。");
            }
            return false;
        }

        const QString requestedName = tableName.trimmed();
        if (requestedName.isEmpty())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Table name is required.");
            }
            return false;
        }

        QString canonicalName = requestedName;
        for (const QString& existingName : tableNames_)
        {
            if (existingName.compare(requestedName, Qt::CaseInsensitive) == 0)
            {
                canonicalName = existingName;
                break;
            }
        }

        const QStringList previousTableNames = tableNames_;
        const bool deletingCurrent =
            currentTableName_.compare(canonicalName, Qt::CaseInsensitive) == 0;

        const bool ok = BeginAndCommitSingleAction(
            L"Delete Table",
            [this, canonicalNameW = canonicalName.toStdWString()]() {
                return db_->DeleteTable(canonicalNameW.c_str());
            },
            outError);
        if (!ok)
        {
            return false;
        }

        sessionComputedColumnsByTable_.remove(canonicalName);

        if (deletingCurrent)
        {
            currentTable_.Reset();
            currentTableView_.Reset();
            currentTableName_.clear();
        }

        if (!LoadTableNames(outError))
        {
            return false;
        }

        if (!deletingCurrent)
        {
            emit TablesChanged();
            return true;
        }

        QString fallbackSelection;
        const int deletedIndex =
            previousTableNames.indexOf(canonicalName, 0, Qt::CaseInsensitive);
        if (deletedIndex >= 0)
        {
            if (deletedIndex + 1 < previousTableNames.size())
            {
                fallbackSelection = previousTableNames.at(deletedIndex + 1);
            } else if (deletedIndex - 1 >= 0)
            {
                fallbackSelection = previousTableNames.at(deletedIndex - 1);
            }
        }

        if (!fallbackSelection.isEmpty())
        {
            QString selectError;
            if (!SelectTable(fallbackSelection, &selectError))
            {
                if (outError != nullptr)
                {
                    *outError = selectError;
                }
                return false;
            }
        } else
        {
            emit CurrentTableChanged();
            emit RecordsChanged();
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
                *outError = QStringLiteral("未打开数据库。");
            }
            return false;
        }

        if (HasPendingEdit() &&
            currentTableName_.compare(tableName, Qt::CaseInsensitive) != 0)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral(
                    "Save or discard pending changes before switching tables.");
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

    bool SCDatabaseSession::RenameTable(const QString& originalName,
                                        const QString& newName,
                                        QString* outError)
    {
        if (!IsOpen())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("未打开数据库。");
            }
            return false;
        }

        if (HasPendingEdit())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("修改结构前请先保存或放弃当前编辑。");
            }
            return false;
        }

        const QString requestedOriginal = originalName.trimmed();
        const QString requestedNew = newName.trimmed();
        if (requestedOriginal.isEmpty() || requestedNew.isEmpty())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("表名不能为空。");
            }
            return false;
        }

        const QString canonicalOriginal =
            CanonicalizeTableName(tableNames_, requestedOriginal);
        if (canonicalOriginal.compare(requestedNew, Qt::CaseInsensitive) == 0)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("表名已被占用。");
            }
            return false;
        }

        for (const QString& existingName : tableNames_)
        {
            if (existingName.compare(requestedNew, Qt::CaseInsensitive) == 0 &&
                existingName.compare(canonicalOriginal, Qt::CaseInsensitive) != 0)
            {
                if (outError != nullptr)
                {
                    *outError = QStringLiteral("表名已被占用。");
                }
                return false;
            }
        }

        sc::SCEditPtr edit;
        sc::ErrorCode rc = db_->BeginEdit(L"Rename Table", edit);
        if (sc::Failed(rc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(rc);
            }
            return false;
        }

        rc = db_->RenameTable(canonicalOriginal.toStdWString().c_str(),
                              requestedNew.toStdWString().c_str());
        if (sc::Failed(rc))
        {
            RollbackEditAndReport(
                db_, edit.Get(), ErrorToString(rc),
                QStringLiteral("Rename table"),
                [this](sc::ErrorCode error) { return ErrorToString(error); },
                outError);
            return false;
        }

        QHash<QString, QVector<sc::SCComputedColumnDef>> rewrittenComputedColumns =
            sessionComputedColumnsByTable_;
        RewriteComputedColumnsForTableRename(&rewrittenComputedColumns,
                                             canonicalOriginal, requestedNew);

        QStringList rewrittenTableNames = tableNames_;
        for (QString& tableName : rewrittenTableNames)
        {
            if (tableName.compare(canonicalOriginal, Qt::CaseInsensitive) == 0)
            {
                tableName = requestedNew;
            }
        }

        const QString rebuiltCurrentTableName =
            currentTable_ &&
                    currentTableName_.compare(canonicalOriginal, Qt::CaseInsensitive) == 0
                ? requestedNew
                : currentTableName_;

        sc::SCComputedTableViewPtr previewView = currentTableView_;
        if (currentTable_)
        {
            if (!BuildComputedTableViewForState(
                    db_, currentTable_, rebuiltCurrentTableName,
                    rewrittenComputedColumns,
                    forceRebuildCurrentTableViewFailureForTest_,
                    [this](sc::ErrorCode error) { return ErrorToString(error); },
                    &previewView, outError))
            {
                RollbackEditAndReport(
                    db_, edit.Get(),
                    outError != nullptr ? *outError
                                        : QStringLiteral("Failed to rebuild current table view."),
                    QStringLiteral("Rename table preview"),
                    [this](sc::ErrorCode error) { return ErrorToString(error); },
                    outError);
                return false;
            }
        }

        rc = db_->Commit(edit.Get());
        if (sc::Failed(rc))
        {
            RollbackEditAndReport(
                db_, edit.Get(), ErrorToString(rc),
                QStringLiteral("Rename table commit"),
                [this](sc::ErrorCode error) { return ErrorToString(error); },
                outError);
            return false;
        }

        sessionComputedColumnsByTable_ = std::move(rewrittenComputedColumns);
        tableNames_ = std::move(rewrittenTableNames);
        currentTableName_ = rebuiltCurrentTableName;
        if (currentTable_)
        {
            currentTableView_ = previewView;
            emit CurrentTableChanged();
            emit RecordsChanged();
        }

        emit TablesChanged();
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
                    "同名计算字段已存在。");
            }
            return false;
        }

        return ApplyColumnMutation(
            L"添加字段",
            [this, column](sc::SCSchemaPtr& schema,
                           sc::SCComputedTableViewPtr* outPreviewView,
                           QString* outError) -> sc::ErrorCode {
                const sc::ErrorCode addRc = schema->AddColumn(column);
                if (sc::Failed(addRc))
                {
                    return addRc;
                }
                if (!BuildCurrentTableViewPreview(outPreviewView, outError))
                {
                    return sc::SC_E_FAIL;
                }
                return sc::SC_OK;
            },
            []() {}, outError);
    }

    bool SCDatabaseSession::CurrentColumnHasNullValues(
        const QString& columnName, bool* outHasNullValues, QString* outError) const
    {
        if (outHasNullValues == nullptr)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("输出标志为空。");
            }
            return false;
        }
        *outHasNullValues = false;

        if (!currentTable_)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("当前未选择数据表。");
            }
            return false;
        }

        const QString requestedName = columnName.trimmed();
        if (requestedName.isEmpty())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("字段名称不能为空。");
            }
            return false;
        }

        sc::SCSchemaPtr schema;
        const sc::ErrorCode schemaRc = currentTable_->GetSchema(schema);
        if (sc::Failed(schemaRc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(schemaRc);
            }
            return false;
        }

        sc::SCColumnDef column;
        const sc::ErrorCode columnRc =
            schema->FindColumn(requestedName.toStdWString().c_str(), &column);
        if (sc::Failed(columnRc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(columnRc);
            }
            return false;
        }

        sc::SCRecordCursorPtr cursor;
        const sc::ErrorCode cursorRc = currentTable_->EnumerateRecords(cursor);
        if (sc::Failed(cursorRc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(cursorRc);
            }
            return false;
        }

        sc::SCRecordPtr record;
        while (cursor->Next(record) == sc::SC_OK && record)
        {
            if (record->IsDeleted())
            {
                continue;
            }

            sc::SCValue value;
            const sc::ErrorCode valueRc = record->GetValue(
                requestedName.toStdWString().c_str(), &value);
            if (valueRc == sc::SC_E_VALUE_IS_NULL)
            {
                *outHasNullValues = true;
                return true;
            }
            if (sc::Failed(valueRc))
            {
                if (outError != nullptr)
                {
                    *outError = ErrorToString(valueRc);
                }
                return false;
            }
        }

        return true;
    }

    bool SCDatabaseSession::RemoveColumn(const QString& columnName,
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

        const QString requestedName = columnName.trimmed();
        if (requestedName.isEmpty())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Column name is required.");
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

        sc::SCColumnDef existingColumn;
        const std::wstring requestedNameW = requestedName.toStdWString();
        rc = schema->FindColumn(requestedNameW.c_str(), &existingColumn);
        if (sc::Failed(rc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(rc);
            }
            return false;
        }

        return ApplyColumnMutation(
            L"删除字段",
            [this, requestedNameW](sc::SCSchemaPtr& schema,
                                   sc::SCComputedTableViewPtr* outPreviewView,
                                   QString* outError) -> sc::ErrorCode {
                const sc::ErrorCode removeRc =
                    schema->RemoveColumn(requestedNameW.c_str());
                if (sc::Failed(removeRc))
                {
                    return removeRc;
                }
                if (!BuildCurrentTableViewPreview(outPreviewView, outError))
                {
                    return sc::SC_E_FAIL;
                }
                return sc::SC_OK;
            },
            []() {}, outError);
    }

    bool SCDatabaseSession::UpdateColumn(const QString& originalName,
                                         const sc::SCColumnDef& column,
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

        const QString originalKey = originalName.trimmed();
        const QString requestedName = ToQString(column.name).trimmed();
        if (originalKey.isEmpty() || requestedName.isEmpty())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Column name is required.");
            }
            return false;
        }

        const bool renaming =
            originalKey.compare(requestedName, Qt::CaseInsensitive) != 0;

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

        sc::SCColumnDef existingColumn;
        rc = schema->FindColumn(originalKey.toStdWString().c_str(),
                                &existingColumn);
        if (sc::Failed(rc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(rc);
            }
            return false;
        }

        const std::wstring originalKeyW = originalKey.toStdWString();
        const std::wstring requestedNameW = requestedName.toStdWString();
        const std::wstring sourceTableNameW = currentTableName_.toStdWString();
        QVector<sc::SCConstraintDef> constraintsToRewrite;
        QVector<sc::SCIndexDef> indexesToRewrite;

        if (renaming)
        {
            sc::SCColumnDef requestedColumn;
            const sc::ErrorCode requestedRc =
                schema->FindColumn(requestedName.toStdWString().c_str(),
                                   &requestedColumn);
            if (requestedRc != sc::SC_E_COLUMN_NOT_FOUND)
            {
                if (sc::Succeeded(requestedRc))
                {
                    if (outError != nullptr)
                    {
                        *outError = QStringLiteral("字段名已被占用。");
                    }
                } else if (outError != nullptr)
                {
                    *outError = ErrorToString(requestedRc);
                }
                return false;
            }

            if (HasComputedColumnNameConflict(sessionComputedColumnsByTable_,
                                              currentTableName_, requestedName))
            {
                if (outError != nullptr)
                {
                    *outError = QStringLiteral(
                        "同名计算字段已存在。");
                }
                return false;
            }

            sc::SCTableSchemaSnapshot snapshot;
            const sc::ErrorCode snapshotRc = schema->GetSchemaSnapshot(&snapshot);
            if (sc::Failed(snapshotRc))
            {
                if (outError != nullptr)
                {
                    *outError = ErrorToString(snapshotRc);
                }
                return false;
            }

            const auto isOriginalField = [&originalKey](const std::wstring& value) {
                return EqualsIgnoreCase(value, originalKey);
            };

            for (const sc::SCConstraintDef& constraint : snapshot.constraints)
            {
                bool touchesRenamedField = false;
                sc::SCConstraintDef rewritten = constraint;
                for (std::wstring& columnName : rewritten.columns)
                {
                    if (isOriginalField(columnName))
                    {
                        columnName = requestedNameW;
                        touchesRenamedField = true;
                    }
                }
                for (std::wstring& referencedName : rewritten.referencedColumns)
                {
                    if (isOriginalField(referencedName))
                    {
                        referencedName = requestedNameW;
                        touchesRenamedField = true;
                    }
                }
                if (touchesRenamedField)
                {
                    constraintsToRewrite.push_back(std::move(rewritten));
                }
            }

            for (const sc::SCIndexDef& index : snapshot.indexes)
            {
                bool touchesRenamedField = false;
                sc::SCIndexDef rewritten = index;
                for (sc::SCIndexColumnDef& indexColumn : rewritten.columns)
                {
                    if (isOriginalField(indexColumn.columnName))
                    {
                        indexColumn.columnName = requestedNameW;
                        touchesRenamedField = true;
                    }
                }
                if (touchesRenamedField)
                {
                    indexesToRewrite.push_back(std::move(rewritten));
                }
            }

        }

        if (!column.nullable)
        {
            bool hasNullValues = false;
            QString nullError;
            if (!CurrentColumnHasNullValues(
                    originalKey, &hasNullValues, &nullError))
            {
                if (outError != nullptr)
                {
                    *outError = nullError;
                }
                return false;
            }
            if (hasNullValues)
            {
                if (outError != nullptr)
                {
                    *outError = QStringLiteral("该字段仍存在空值，不能设置为非空。");
                }
                return false;
            }
        }

        if (!renaming)
        {
            return ApplyColumnMutation(
                L"编辑字段",
                [this, column](sc::SCSchemaPtr& schema,
                               sc::SCComputedTableViewPtr* outPreviewView,
                               QString* outError) -> sc::ErrorCode {
                    const sc::ErrorCode updateRc = schema->UpdateColumn(column);
                    if (sc::Failed(updateRc))
                    {
                        return updateRc;
                    }
                    if (!BuildCurrentTableViewPreview(outPreviewView, outError))
                    {
                        return sc::SC_E_FAIL;
                    }
                    return sc::SC_OK;
                },
                []() {}, outError);
        }

        QVector<ColumnValueSnapshot> valueSnapshots;
        if (!SnapshotColumnValues(currentTable_.Get(), originalKey, &valueSnapshots,
                                  outError))
        {
            return false;
        }

        const QHash<QString, QVector<sc::SCComputedColumnDef>> previousComputedColumns =
            sessionComputedColumnsByTable_;
        bool computedColumnsRewritten = false;
        QVector<QString> constraintsToRemove;
        QVector<sc::SCConstraintDef> constraintsToAdd;
        QVector<QString> indexesToRemove;
        QVector<sc::SCIndexDef> indexesToAdd;
        if (renaming)
        {
            for (const sc::SCConstraintDef& constraint : constraintsToRewrite)
            {
                constraintsToRemove.push_back(QString::fromStdWString(constraint.name));
                constraintsToAdd.push_back(constraint);
            }
            for (const sc::SCIndexDef& index : indexesToRewrite)
            {
                indexesToRemove.push_back(QString::fromStdWString(index.name));
                indexesToAdd.push_back(index);
            }
        }
        sc::SCColumnDef temporaryColumn = column;
        temporaryColumn.nullable = true;
        return ApplyColumnMutation(
            L"编辑字段",
            [this, originalKeyW, requestedNameW, sourceTableNameW, column,
             temporaryColumn, valueSnapshots, constraintsToRemove,
             constraintsToAdd, indexesToRemove, indexesToAdd,
             &computedColumnsRewritten](
                sc::SCSchemaPtr& schema, sc::SCComputedTableViewPtr* outPreviewView,
                QString* outError) -> sc::ErrorCode {
                for (const QString& constraintName : constraintsToRemove)
                {
                    const sc::ErrorCode removeConstraintRc =
                        schema->RemoveConstraint(constraintName.toStdWString().c_str());
                    if (sc::Failed(removeConstraintRc))
                    {
                        return removeConstraintRc;
                    }
                }
                for (const QString& indexName : indexesToRemove)
                {
                    const sc::ErrorCode removeIndexRc =
                        schema->RemoveIndex(indexName.toStdWString().c_str());
                    if (sc::Failed(removeIndexRc))
                    {
                        return removeIndexRc;
                    }
                }

                const sc::ErrorCode removeRc =
                    schema->RemoveColumn(originalKeyW.c_str());
                if (sc::Failed(removeRc))
                {
                    return removeRc;
                }

                const sc::ErrorCode addRc = schema->AddColumn(temporaryColumn);
                if (sc::Failed(addRc))
                {
                    return addRc;
                }

                for (const ColumnValueSnapshot& snapshot : valueSnapshots)
                {
                    sc::SCRecordPtr record;
                    const sc::ErrorCode getRc =
                        currentTable_->GetRecord(snapshot.recordId, record);
                    if (sc::Failed(getRc) || !record)
                    {
                        return sc::Failed(getRc) ? getRc : sc::SC_E_FAIL;
                    }

                    const sc::ErrorCode setRc = record->SetValue(
                        requestedNameW.c_str(), snapshot.value);
                    if (sc::Failed(setRc))
                    {
                        return setRc;
                    }
                }

                sc::SCSchemaPtr refreshedSchema;
                const sc::ErrorCode refreshRc = currentTable_->GetSchema(refreshedSchema);
                if (sc::Failed(refreshRc))
                {
                    return refreshRc;
                }

                const sc::ErrorCode updateRc = refreshedSchema->UpdateColumn(column);
                if (sc::Failed(updateRc))
                {
                    return updateRc;
                }

                // Restore source-table constraints/indexes before rewriting incoming
                // relation fields so their validation sees the final source schema.
                for (const sc::SCConstraintDef& constraint : constraintsToAdd)
                {
                    const sc::ErrorCode addConstraintRc = schema->AddConstraint(constraint);
                    if (sc::Failed(addConstraintRc))
                    {
                        return addConstraintRc;
                    }
                }
                for (const sc::SCIndexDef& index : indexesToAdd)
                {
                    const sc::ErrorCode addIndexRc = schema->AddIndex(index);
                    if (sc::Failed(addIndexRc))
                    {
                        return addIndexRc;
                    }
                }

                const QString sourceTableName = QString::fromStdWString(sourceTableNameW);
                const QString originalColumnName = QString::fromStdWString(originalKeyW);
                for (const QString& tableName : tableNames_)
                {
                    sc::SCTablePtr relatedTable;
                    const sc::ErrorCode tableRc =
                        db_->GetTable(tableName.toStdWString().c_str(), relatedTable);
                    if (sc::Failed(tableRc))
                    {
                        return tableRc;
                    }
                    if (!relatedTable)
                    {
                        continue;
                    }

                    sc::SCSchemaPtr relatedSchema;
                    const sc::ErrorCode schemaRc = relatedTable->GetSchema(relatedSchema);
                    if (sc::Failed(schemaRc) || !relatedSchema)
                    {
                        return sc::Failed(schemaRc) ? schemaRc : sc::SC_E_FAIL;
                    }

                    std::int32_t relatedColumnCount = 0;
                    const sc::ErrorCode columnCountRc =
                        relatedSchema->GetColumnCount(&relatedColumnCount);
                    if (sc::Failed(columnCountRc))
                    {
                        return columnCountRc;
                    }

                    for (std::int32_t columnIndex = 0; columnIndex < relatedColumnCount;
                         ++columnIndex)
                    {
                        sc::SCColumnDef relatedColumn;
                        const sc::ErrorCode relatedColumnRc =
                            relatedSchema->GetColumn(columnIndex, &relatedColumn);
                        if (sc::Failed(relatedColumnRc))
                        {
                            return relatedColumnRc;
                        }

                        if (relatedColumn.columnKind == sc::ColumnKind::Relation &&
                            EqualsIgnoreCase(relatedColumn.referenceTable,
                                             sourceTableName))
                        {
                            bool relationChanged = false;
                            if (EqualsIgnoreCase(relatedColumn.referenceStorageColumn,
                                                 originalColumnName))
                            {
                                relatedColumn.referenceStorageColumn =
                                    requestedNameW;
                                relationChanged = true;
                            }
                            if (EqualsIgnoreCase(relatedColumn.referenceDisplayColumn,
                                                 originalColumnName))
                            {
                                relatedColumn.referenceDisplayColumn =
                                    requestedNameW;
                                relationChanged = true;
                            }
                            if (relationChanged)
                            {
                                const sc::ErrorCode relationUpdateRc =
                                    relatedSchema->UpdateColumn(relatedColumn);
                                if (sc::Failed(relationUpdateRc))
                                {
                                    return relationUpdateRc;
                                }
                            }
                        }
                    }

                    sc::SCTableSchemaSnapshot relatedSnapshot;
                    const sc::ErrorCode relatedSnapshotRc =
                        relatedSchema->GetSchemaSnapshot(&relatedSnapshot);
                    if (sc::Failed(relatedSnapshotRc))
                    {
                        return relatedSnapshotRc;
                    }

                    for (const sc::SCConstraintDef& relatedConstraint :
                         relatedSnapshot.constraints)
                    {
                        if (!EqualsIgnoreCase(relatedConstraint.referencedTable,
                                              sourceTableNameW))
                        {
                            continue;
                        }

                        bool constraintChanged = false;
                        sc::SCConstraintDef rewrittenConstraint = relatedConstraint;
                        for (std::wstring& referencedName :
                             rewrittenConstraint.referencedColumns)
                        {
                            if (EqualsIgnoreCase(referencedName, originalKeyW))
                            {
                                referencedName = requestedNameW;
                                constraintChanged = true;
                            }
                        }

                        if (constraintChanged)
                        {
                            const sc::ErrorCode removeConstraintRc =
                                relatedSchema->RemoveConstraint(
                                    relatedConstraint.name.c_str());
                            if (sc::Failed(removeConstraintRc))
                            {
                                return removeConstraintRc;
                            }
                            const sc::ErrorCode addConstraintRc =
                                relatedSchema->AddConstraint(rewrittenConstraint);
                            if (sc::Failed(addConstraintRc))
                            {
                                return addConstraintRc;
                            }
                        }
                    }
                }

                if (!computedColumnsRewritten)
                {
                    for (auto it = sessionComputedColumnsByTable_.begin();
                         it != sessionComputedColumnsByTable_.end(); ++it)
                    {
                        for (sc::SCComputedColumnDef& computedColumn : it.value())
                        {
                            ReplaceDependencyFieldName(
                                &computedColumn.dependencies.factFields,
                                sourceTableNameW, originalKeyW, requestedNameW);
                            ReplaceDependencyFieldName(
                                &computedColumn.dependencies.relationFields,
                                sourceTableNameW, originalKeyW, requestedNameW);
                        }
                    }
                    computedColumnsRewritten = true;
                }

                if (!BuildCurrentTableViewPreview(outPreviewView, outError))
                {
                    return sc::SC_E_FAIL;
                }
                return sc::SC_OK;
            },
            [this, previousComputedColumns, &computedColumnsRewritten]() {
                if (computedColumnsRewritten)
                {
                    sessionComputedColumnsByTable_ = previousComputedColumns;
                }
            },
            outError);
    }

    bool SCDatabaseSession::ConvertColumnToComputed(
        const QString& columnName,
        const sc::SCComputedColumnDef& computedColumn, QString* outError)
    {
        if (!currentTable_ || !currentTableView_)
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

        const QString sourceName = columnName.trimmed();
        const QString targetName = ToQString(computedColumn.name).trimmed();
        if (sourceName.isEmpty() || targetName.isEmpty())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Column name is required.");
            }
            return false;
        }
        if (sourceName.compare(targetName, Qt::CaseInsensitive) != 0)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral(
                    "Converted computed column must keep the same name.");
            }
            return false;
        }
        for (const sc::SCComputedColumnDef& existing : *columns)
        {
            if (ToQString(existing.name)
                    .compare(targetName, Qt::CaseInsensitive) == 0)
            {
                if (outError != nullptr)
                {
                    *outError = QStringLiteral(
                        "同名计算字段已存在。");
                }
                return false;
            }
        }
        if (computedColumn.dependencies.factFields.empty() &&
            computedColumn.dependencies.relationFields.empty())
        {
            if (outError != nullptr)
            {
                *outError =
                    QStringLiteral("At least one dependency is required.");
            }
            return false;
        }
        const auto referencesSourceColumn =
            [this, &sourceName](
                const std::vector<sc::SCFieldDependency>& dependencies) {
                for (const sc::SCFieldDependency& dependency : dependencies)
                {
                    if (ToQString(dependency.tableName)
                                .compare(currentTableName_,
                                         Qt::CaseInsensitive) == 0 &&
                        ToQString(dependency.fieldName)
                                .compare(sourceName, Qt::CaseInsensitive) == 0)
                    {
                        return true;
                    }
                }
                return false;
            };

        bool computedColumnApplied = false;
        return ApplyColumnMutation(
            L"Convert Column To Computed",
            [this, &columns, computedColumn, sourceName,
             &computedColumnApplied](sc::SCSchemaPtr& schema,
                                     sc::SCComputedTableViewPtr* outPreviewView,
                                     QString* outError) -> sc::ErrorCode {
                const sc::ErrorCode clearRc = db_->ClearColumnValues(
                    currentTable_.Get(), sourceName.toStdWString().c_str());
                if (sc::Failed(clearRc))
                {
                    return clearRc;
                }

                const sc::ErrorCode removeRc =
                    schema->RemoveColumn(sourceName.toStdWString().c_str());
                if (sc::Failed(removeRc))
                {
                    return removeRc;
                }

                columns->push_back(computedColumn);
                computedColumnApplied = true;
                if (!BuildCurrentTableViewPreview(outPreviewView, outError))
                {
                    columns->removeLast();
                    computedColumnApplied = false;
                    return sc::SC_E_FAIL;
                }
                return sc::SC_OK;
            },
            [&columns, &computedColumnApplied]() {
                if (computedColumnApplied && !columns->isEmpty())
                {
                    columns->removeLast();
                    computedColumnApplied = false;
                }
            },
            outError);
    }

    bool SCDatabaseSession::ConvertComputedToColumn(
        const QString& computedName, const sc::SCColumnDef& column,
        QString* outError)
    {
        if (!currentTable_ || !currentTableView_)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("No table is selected.");
            }
            return false;
        }

        QVector<sc::SCComputedColumnDef>* computedColumns =
            CurrentSessionComputedColumnsStorage();
        if (computedColumns == nullptr)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("No table is selected.");
            }
            return false;
        }

        const QString sourceName = computedName.trimmed();
        const QString targetName = ToQString(column.name).trimmed();
        if (sourceName.isEmpty() || targetName.isEmpty())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Column name is required.");
            }
            return false;
        }
        if (sourceName.compare(targetName, Qt::CaseInsensitive) != 0)
        {
            if (outError != nullptr)
            {
                *outError =
                    QStringLiteral("Converted column must keep the same name.");
            }
            return false;
        }

        int targetIndex = -1;
        sc::SCComputedColumnDef removedComputed;
        for (int index = 0; index < computedColumns->size(); ++index)
        {
            if (ToQString(computedColumns->at(index).name)
                    .compare(sourceName, Qt::CaseInsensitive) != 0)
            {
                continue;
            }
            targetIndex = index;
            removedComputed = computedColumns->at(index);
            break;
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
        sc::SCColumnDef existingColumn;
        if (sc::Succeeded(schema->FindColumn(sourceName.toStdWString().c_str(),
                                             &existingColumn)))
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral(
                    "A schema column with the same name already exists.");
            }
            return false;
        }

        bool computedColumnRemoved = false;
        return ApplyColumnMutation(
            L"Convert Computed To Column",
            [this, &computedColumns, removedComputed, targetIndex, column,
             &computedColumnRemoved](sc::SCSchemaPtr& schema,
                                     sc::SCComputedTableViewPtr* outPreviewView,
                                     QString* outError) -> sc::ErrorCode {
                computedColumns->removeAt(targetIndex);
                computedColumnRemoved = true;

                const sc::ErrorCode addRc = schema->AddColumn(column);
                if (sc::Failed(addRc))
                {
                    computedColumns->insert(targetIndex, removedComputed);
                    computedColumnRemoved = false;
                    return addRc;
                }

                if (!BuildCurrentTableViewPreview(outPreviewView, outError))
                {
                    const sc::ErrorCode removeRc =
                        schema->RemoveColumn(column.name.c_str());
                    (void)removeRc;
                    computedColumns->insert(targetIndex, removedComputed);
                    computedColumnRemoved = false;
                    return sc::SC_E_FAIL;
                }
                return sc::SC_OK;
            },
            [&computedColumns, removedComputed, targetIndex,
             &computedColumnRemoved]() {
                if (computedColumnRemoved)
                {
                    computedColumns->insert(targetIndex, removedComputed);
                    computedColumnRemoved = false;
                }
            },
            outError);
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

        bool requiresPendingEdit = pendingEdit_.Get() != nullptr;
        if (!requiresPendingEdit)
        {
            sc::SCSchemaPtr schema;
            const sc::ErrorCode schemaRc = currentTable_->GetSchema(schema);
            if (sc::Failed(schemaRc))
            {
                if (outError != nullptr)
                {
                    *outError = ErrorToString(schemaRc);
                }
                return false;
            }

            std::int32_t columnCount = 0;
            const sc::ErrorCode countRc = schema->GetColumnCount(&columnCount);
            if (sc::Failed(countRc))
            {
                if (outError != nullptr)
                {
                    *outError = ErrorToString(countRc);
                }
                return false;
            }

            for (std::int32_t index = 0; index < columnCount; ++index)
            {
                sc::SCColumnDef column;
                const sc::ErrorCode columnRc =
                    schema->GetColumn(index, &column);
                if (sc::Failed(columnRc))
                {
                    if (outError != nullptr)
                    {
                        *outError = ErrorToString(columnRc);
                    }
                    return false;
                }

                if (!column.nullable && column.defaultValue.IsNull())
                {
                    requiresPendingEdit = true;
                    break;
                }
            }
        }

        if (!requiresPendingEdit)
        {
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

        if (!pendingEdit_)
        {
            const sc::ErrorCode beginRc =
                db_->BeginEdit(L"Add Record", pendingEdit_);
            if (sc::Failed(beginRc))
            {
                if (outError != nullptr)
                {
                    *outError = ErrorToString(beginRc);
                }
                return false;
            }
        }

        sc::SCRecordPtr record;
        const sc::ErrorCode createRc = currentTable_->CreateRecord(record);
        if (sc::Failed(createRc))
        {
            if (pendingEdit_)
            {
                const sc::ErrorCode rollbackRc =
                    db_->Rollback(pendingEdit_.Get());
                if (sc::Failed(rollbackRc) && outError != nullptr)
                {
                    *outError = ErrorToString(rollbackRc);
                }
                pendingEdit_.Reset();
            }
            if (outError != nullptr && outError->isEmpty())
            {
                *outError = ErrorToString(createRc);
            }
            return false;
        }

        emit RecordsChanged();
        return true;
    }

    bool SCDatabaseSession::CreateBackupCopy(const QString& targetPath,
                                             const sc::SCBackupOptions& options,
                                             sc::SCBackupResult* outResult,
                                             QString* outError) const
    {
        if (!IsOpen())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("未打开数据库。");
            }
            return false;
        }

        if (targetPath.trimmed().isEmpty())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Target backup path is required.");
            }
            return false;
        }

        if (pendingEdit_)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral(
                    "Save or discard pending changes before creating a "
                    "backup.");
            }
            return false;
        }

        const sc::ErrorCode rc = db_->CreateBackupCopy(
            targetPath.toStdWString().c_str(), options, outResult);
        if (sc::Failed(rc))
        {
            if (outError != nullptr)
            {
                *outError = rc == sc::SC_E_NOTIMPL
                                ? QStringLiteral(
                                      "Backup copy is not supported by the "
                                      "current backend.")
                                : ErrorToString(rc);
            }
            return false;
        }
        return true;
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

        if (HasPendingEdit())
        {
            const sc::ErrorCode deleteRc =
                currentTable_->DeleteRecord(recordId);
            if (sc::Failed(deleteRc))
            {
                if (outError != nullptr)
                {
                    *outError = ErrorToString(deleteRc);
                }
                return false;
            }
            emit RecordsChanged();
            return true;
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

    bool SCDatabaseSession::SavePendingChanges(QString* outError)
    {
        if (!db_)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("未打开数据库。");
            }
            return false;
        }

        if (!pendingEdit_)
        {
            if (outError != nullptr)
            {
                outError->clear();
            }
            return true;
        }

        const sc::ErrorCode rc = db_->Commit(pendingEdit_.Get());
        if (sc::Failed(rc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(rc);
            }
            return false;
        }

        pendingEdit_.Reset();
        emit RecordsChanged();
        return true;
    }

    bool SCDatabaseSession::DiscardPendingChanges(QString* outError)
    {
        if (!db_)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("未打开数据库。");
            }
            return false;
        }

        if (!pendingEdit_)
        {
            if (outError != nullptr)
            {
                outError->clear();
            }
            return true;
        }

        const sc::ErrorCode rc = db_->Rollback(pendingEdit_.Get());
        if (sc::Failed(rc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(rc);
            }
            return false;
        }

        pendingEdit_.Reset();
        emit RecordsChanged();
        return true;
    }

    bool SCDatabaseSession::Undo(QString* outError)
    {
        if (!IsOpen())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("未打开数据库。");
            }
            return false;
        }

        if (HasPendingEdit())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral(
                    "Save or discard pending changes before undoing.");
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

        sc::SCChangeSet observedChangeSet;
        bool hasObservedChangeSet = false;
        if (const auto observedIt = ObservedChangeSets().find(this);
            observedIt != ObservedChangeSets().end())
        {
            observedChangeSet = observedIt.value();
            ObservedChangeSets().erase(observedIt);
            hasObservedChangeSet = true;
        }

        if (!LoadTableNames(outError))
        {
            return false;
        }

        if (hasObservedChangeSet)
        {
            ApplyObservedRenameChangesToComputedColumns(
                &sessionComputedColumnsByTable_, observedChangeSet);
            currentTableName_ = RewriteTableNameForObservedRenames(
                currentTableName_, observedChangeSet);
        }

        if (currentTable_)
        {
            const QString liveTableName =
                CanonicalizeTableName(tableNames_, currentTableName_);
            if (!liveTableName.isEmpty() && tableNames_.contains(liveTableName))
            {
                currentTableName_ = liveTableName;
                if (!RebuildCurrentTableView(outError))
                {
                    return false;
                }
            }
            else if (!tableNames_.isEmpty())
            {
                const bool selected = SelectTable(tableNames_.front(), outError);
                if (selected)
                {
                    emit TablesChanged();
                }
                return selected;
            }
            else
            {
                currentTable_.Reset();
                currentTableView_.Reset();
                currentTableName_.clear();
            }
        }

        emit TablesChanged();
        emit CurrentTableChanged();
        emit RecordsChanged();
        return true;
    }

    bool SCDatabaseSession::Redo(QString* outError)
    {
        if (!IsOpen())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("未打开数据库。");
            }
            return false;
        }

        if (HasPendingEdit())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral(
                    "Save or discard pending changes before redoing.");
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

        sc::SCChangeSet observedChangeSet;
        bool hasObservedChangeSet = false;
        if (const auto observedIt = ObservedChangeSets().find(this);
            observedIt != ObservedChangeSets().end())
        {
            observedChangeSet = observedIt.value();
            ObservedChangeSets().erase(observedIt);
            hasObservedChangeSet = true;
        }

        if (!LoadTableNames(outError))
        {
            return false;
        }

        if (hasObservedChangeSet)
        {
            ApplyObservedRenameChangesToComputedColumns(
                &sessionComputedColumnsByTable_, observedChangeSet);
            currentTableName_ = RewriteTableNameForObservedRenames(
                currentTableName_, observedChangeSet);
        }

        if (currentTable_)
        {
            const QString liveTableName =
                CanonicalizeTableName(tableNames_, currentTableName_);
            if (!liveTableName.isEmpty() && tableNames_.contains(liveTableName))
            {
                currentTableName_ = liveTableName;
                if (!RebuildCurrentTableView(outError))
                {
                    return false;
                }
            }
            else if (!tableNames_.isEmpty())
            {
                const bool selected = SelectTable(tableNames_.front(), outError);
                if (selected)
                {
                    emit TablesChanged();
                }
                return selected;
            }
            else
            {
                currentTable_.Reset();
                currentTableView_.Reset();
                currentTableName_.clear();
            }
        }

        emit TablesChanged();
        emit CurrentTableChanged();
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
        QString convertError;
        rc = ConvertVariantToValue(column, SCValue, &storageValue,
                                   &convertError);
        if (sc::Failed(rc))
        {
            if (outError != nullptr)
            {
                *outError = convertError.isEmpty() ? ErrorToString(rc)
                                                   : convertError;
            }
            return false;
        }

        auto applyValue = [&]() {
            sc::SCRecordPtr record;
            sc::ErrorCode getRc = currentTable_->GetRecord(recordId, record);
            if (sc::Failed(getRc))
            {
                return getRc;
            }
            return record->SetValue(columnName.toStdWString().c_str(),
                                    storageValue);
        };

        if (HasPendingEdit())
        {
            const sc::ErrorCode applyRc = applyValue();
            if (sc::Failed(applyRc))
            {
                if (outError != nullptr)
                {
                    *outError = ErrorToString(applyRc);
                }
                return false;
            }
            emit RecordsChanged();
            return true;
        }

        const bool ok = BeginAndCommitSingleAction(
            L"Edit Cell", [&]() { return applyValue(); }, outError);

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
        const sc::SCColumnDef& relationColumn,
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
                *outError = QStringLiteral("未打开数据库。");
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

        sc::SCRecordPtr record;
        while (cursor->Next(record) == sc::SC_OK && record)
        {
            RelationCandidate candidate;
            candidate.recordId = record->GetId();
            candidate.previewFields.push_back(qMakePair(
                QStringLiteral("RecordId"),
                QString::number(candidate.recordId)));

            const QString storageColumnName =
                relationColumn.referenceStorageColumn.empty()
                    ? QString()
                    : ToQString(relationColumn.referenceStorageColumn);
            const QString displayColumnName =
                relationColumn.referenceDisplayColumn.empty()
                    ? storageColumnName
                    : ToQString(relationColumn.referenceDisplayColumn);

            if (storageColumnName.isEmpty())
            {
                candidate.previewFields.push_back(qMakePair(
                    QStringLiteral("StoredValue"),
                    QString::number(candidate.recordId)));
            } else
            {
                sc::SCValue storageValue;
                if (record->GetValue(storageColumnName.toStdWString().c_str(),
                                     &storageValue) == sc::SC_OK)
                {
                    candidate.previewFields.push_back(qMakePair(
                        storageColumnName, ValueToDisplayString(storageValue)));
                }
            }

            if (!displayColumnName.isEmpty() &&
                displayColumnName.compare(storageColumnName, Qt::CaseInsensitive) != 0)
            {
                sc::SCValue displayValue;
                if (record->GetValue(displayColumnName.toStdWString().c_str(),
                                     &displayValue) == sc::SC_OK)
                {
                    candidate.previewFields.push_back(qMakePair(
                        displayColumnName, ValueToDisplayString(displayValue)));
                }
            }

            for (const sc::SCColumnDef& column : columns)
            {
                const QString columnName = ToQString(column.name);
                if (columnName.compare(storageColumnName, Qt::CaseInsensitive) == 0 ||
                    columnName.compare(displayColumnName, Qt::CaseInsensitive) == 0)
                {
                    continue;
                }
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
                    ToQString(column.displayName.empty() ? column.name : column.displayName),
                    ValueToDisplayString(SCValue)));
            }

            candidate.label = PickRecordLabel(candidate.previewFields, candidate.recordId);
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
                        "同名计算字段已存在。");
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
                        "同名计算字段已存在。");
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

    bool SCDatabaseSession::GetTableColumnNames(const QString& tableName,
                                                QStringList* outColumns,
                                                QString* outError) const
    {
        if (outColumns == nullptr)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Output columns is null.");
            }
            return false;
        }
        outColumns->clear();

        if (!db_)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("未打开数据库。");
            }
            return false;
        }

        sc::SCTablePtr table;
        const sc::ErrorCode tableRc =
            db_->GetTable(tableName.toStdWString().c_str(), table);
        if (sc::Failed(tableRc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(tableRc);
            }
            return false;
        }

        sc::SCSchemaPtr schema;
        const sc::ErrorCode schemaRc = table->GetSchema(schema);
        if (sc::Failed(schemaRc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(schemaRc);
            }
            return false;
        }

        std::int32_t columnCount = 0;
        const sc::ErrorCode countRc = schema->GetColumnCount(&columnCount);
        if (sc::Failed(countRc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(countRc);
            }
            return false;
        }

        for (std::int32_t index = 0; index < columnCount; ++index)
        {
            sc::SCColumnDef column;
            const sc::ErrorCode columnRc = schema->GetColumn(index, &column);
            if (sc::Failed(columnRc))
            {
                if (outError != nullptr)
                {
                    *outError = ErrorToString(columnRc);
                }
                return false;
            }
            if (!column.name.empty())
            {
                outColumns->push_back(ToQString(column.name));
            }
        }

        return true;
    }

    bool SCDatabaseSession::GetTableSchemaSnapshot(
        const QString& tableName, sc::SCTableSchemaSnapshot* outSnapshot,
        QString* outError) const
    {
        if (outSnapshot == nullptr)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Output snapshot is null.");
            }
            return false;
        }

        outSnapshot->columns.clear();
        outSnapshot->constraints.clear();
        outSnapshot->indexes.clear();
        outSnapshot->table = sc::SCTableDef{};

        if (!db_)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("未打开数据库。");
            }
            return false;
        }

        sc::SCTablePtr table;
        const sc::ErrorCode tableRc =
            db_->GetTable(tableName.toStdWString().c_str(), table);
        if (sc::Failed(tableRc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(tableRc);
            }
            return false;
        }

        sc::SCSchemaPtr schema;
        const sc::ErrorCode schemaRc = table->GetSchema(schema);
        if (sc::Failed(schemaRc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(schemaRc);
            }
            return false;
        }

        const sc::ErrorCode snapshotRc = schema->GetSchemaSnapshot(outSnapshot);
        if (sc::Failed(snapshotRc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(snapshotRc);
            }
            return false;
        }

        if (outSnapshot->table.name.empty())
        {
            outSnapshot->table.name = tableName.toStdWString();
        }

        return true;
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

    bool SCDatabaseSession::AddIndex(const sc::SCIndexDef& index,
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

        if (index.name.empty() || index.columns.empty())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Index name and columns are required.");
            }
            return false;
        }

        return BeginAndCommitSingleAction(
            L"Add Index",
            [this, index]() {
                sc::SCSchemaPtr schema;
                sc::ErrorCode rc = currentTable_->GetSchema(schema);
                if (sc::Failed(rc))
                {
                    return rc;
                }
                return schema->AddIndex(index);
            },
            outError);
    }

    bool SCDatabaseSession::RemoveIndex(const QString& indexName,
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

        const QString name = indexName.trimmed();
        if (name.isEmpty())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Index name is required.");
            }
            return false;
        }

        return BeginAndCommitSingleAction(
            L"Remove Index",
            [this, name]() {
                sc::SCSchemaPtr schema;
                sc::ErrorCode rc = currentTable_->GetSchema(schema);
                if (sc::Failed(rc))
                {
                    return rc;
                }
                return schema->RemoveIndex(name.toStdWString().c_str());
            },
            outError);
    }

    bool SCDatabaseSession::UpdateIndex(const QString& originalName,
                                        const sc::SCIndexDef& newIndex,
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

        const QString original = originalName.trimmed();
        if (original.isEmpty() || newIndex.name.empty())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Index name is required.");
            }
            return false;
        }

        if (newIndex.columns.empty())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Index must have at least one column.");
            }
            return false;
        }

        return BeginAndCommitSingleAction(
            L"Update Index",
            [this, original, newIndex]() {
                sc::SCSchemaPtr schema;
                sc::ErrorCode rc = currentTable_->GetSchema(schema);
                if (sc::Failed(rc))
                {
                    return rc;
                }

                rc = schema->RemoveIndex(original.toStdWString().c_str());
                if (sc::Failed(rc))
                {
                    return rc;
                }

                return schema->AddIndex(newIndex);
            },
            outError);
    }

    bool SCDatabaseSession::AddConstraint(
        const sc::SCConstraintDef& constraint, QString* outError)
    {
        if (!currentTable_)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("No table is selected.");
            }
            return false;
        }

        if (constraint.name.empty() || constraint.columns.empty())
        {
            if (outError != nullptr)
            {
                *outError =
                    QStringLiteral("Constraint name and columns are required.");
            }
            return false;
        }

        return BeginAndCommitSingleAction(
            L"Add Constraint",
            [this, constraint]() {
                sc::SCSchemaPtr schema;
                sc::ErrorCode rc = currentTable_->GetSchema(schema);
                if (sc::Failed(rc))
                {
                    return rc;
                }
                return schema->AddConstraint(constraint);
            },
            outError);
    }

    bool SCDatabaseSession::RemoveConstraint(const QString& constraintName,
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

        const QString name = constraintName.trimmed();
        if (name.isEmpty())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Constraint name is required.");
            }
            return false;
        }

        return BeginAndCommitSingleAction(
            L"Remove Constraint",
            [this, name]() {
                sc::SCSchemaPtr schema;
                sc::ErrorCode rc = currentTable_->GetSchema(schema);
                if (sc::Failed(rc))
                {
                    return rc;
                }
                return schema->RemoveConstraint(name.toStdWString().c_str());
            },
            outError);
    }

    bool SCDatabaseSession::UpdateConstraint(
        const QString& originalName, const sc::SCConstraintDef& newConstraint,
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

        const QString original = originalName.trimmed();
        if (original.isEmpty() || newConstraint.name.empty())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Constraint name is required.");
            }
            return false;
        }

        if (newConstraint.columns.empty())
        {
            if (outError != nullptr)
            {
                *outError =
                    QStringLiteral("Constraint must have at least one column.");
            }
            return false;
        }

        return BeginAndCommitSingleAction(
            L"Update Constraint",
            [this, original, newConstraint]() {
                sc::SCSchemaPtr schema;
                sc::ErrorCode rc = currentTable_->GetSchema(schema);
                if (sc::Failed(rc))
                {
                    return rc;
                }

                rc = schema->RemoveConstraint(original.toStdWString().c_str());
                if (sc::Failed(rc))
                {
                    return rc;
                }

                return schema->AddConstraint(newConstraint);
            },
            outError);
    }

    QString SCDatabaseSession::BuildHealthSummary() const
    {
        if (!IsOpen())
        {
            return QStringLiteral("未打开数据库。");
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

    bool SCDatabaseSession::BuildSchemaSnapshot(
        sc::SCSchemaSnapshot* outSnapshot, QString* outError) const
    {
        if (outSnapshot == nullptr)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Output container is null.");
            }
            return false;
        }

        outSnapshot->schemaVersion = db_ ? db_->GetSchemaVersion() : 0;
        outSnapshot->tables.clear();

        if (!currentTable_ || currentTableName_.isEmpty())
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

        sc::SCTableSchemaSnapshot tableSnapshot;
        rc = schema->GetSchemaSnapshot(&tableSnapshot);
        if (sc::Failed(rc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(rc);
            }
            return false;
        }

        if (tableSnapshot.table.name.empty())
        {
            tableSnapshot.table.name = currentTableName_.toStdWString();
        }

        const bool isLegacySchema = db_ && db_->GetSchemaVersion() < 3;
        if (isLegacySchema)
        {
            const auto hasPrimaryKey = std::any_of(
                tableSnapshot.constraints.begin(),
                tableSnapshot.constraints.end(),
                [](const sc::SCConstraintDef& constraint) {
                    return constraint.kind == sc::SCConstraintKind::PrimaryKey;
                });
            if (!hasPrimaryKey)
            {
                const auto idColumn = std::find_if(
                    tableSnapshot.columns.begin(), tableSnapshot.columns.end(),
                    [](const sc::SCColumnDef& column) {
                        return ToQString(column.name)
                                   .compare(QStringLiteral("Id"),
                                            Qt::CaseInsensitive) == 0;
                    });
                if (idColumn != tableSnapshot.columns.end())
                {
                    sc::SCConstraintDef constraint;
                    constraint.kind = sc::SCConstraintKind::PrimaryKey;
                    constraint.name =
                        QStringLiteral("pk_legacy").toStdWString();
                    constraint.columns.push_back(idColumn->name);
                    constraint.sourceKind =
                        sc::SCSchemaSourceKind::MigratedConvention;
                    tableSnapshot.constraints.push_back(std::move(constraint));
                }
            }

            const auto hasIndexForColumn = [&tableSnapshot](
                                               const std::wstring& columnName) {
                for (const sc::SCIndexDef& index : tableSnapshot.indexes)
                {
                    const auto columnIt = std::find_if(
                        index.columns.begin(), index.columns.end(),
                        [&columnName](const sc::SCIndexColumnDef& indexColumn) {
                            return indexColumn.columnName == columnName;
                        });
                    if (columnIt != index.columns.end())
                    {
                        return true;
                    }
                }
                return false;
            };

            for (const sc::SCColumnDef& column : tableSnapshot.columns)
            {
                if (!column.indexed || hasIndexForColumn(column.name))
                {
                    continue;
                }

                sc::SCIndexDef index;
                index.name =
                    (QStringLiteral("idx_legacy_") + ToQString(column.name))
                        .toStdWString();
                index.sourceKind = sc::SCSchemaSourceKind::LegacyHint;
                index.columns.push_back(
                    sc::SCIndexColumnDef{column.name, false});
                tableSnapshot.indexes.push_back(std::move(index));
            }
        }

        outSnapshot->tables.push_back(std::move(tableSnapshot));
        return true;
    }

    bool SCDatabaseSession::CurrentTableHasRecords(bool* outHasRecords,
                                                   QString* outError) const
    {
        if (outHasRecords == nullptr)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Output flag is null.");
            }
            return false;
        }

        *outHasRecords = false;
        if (!currentTable_)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("No table is selected.");
            }
            return false;
        }

        sc::SCRecordCursorPtr cursor;
        const sc::ErrorCode rc = currentTable_->EnumerateRecords(cursor);
        if (sc::Failed(rc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(rc);
            }
            return false;
        }
        if (!cursor)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Failed to enumerate records.");
            }
            return false;
        }

        sc::SCRecordPtr record;
        const sc::ErrorCode nextRc = cursor->Next(record);
        if (sc::Failed(nextRc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(nextRc);
            }
            return false;
        }

        *outHasRecords = static_cast<bool>(record);
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
            QVariant displayValue;
            if (!GetCellDisplayValue(recordId, ToQString(column.name),
                                     &displayValue, outError))
            {
                return false;
            }
            outFields->push_back(
                qMakePair(ToQString(column.name), displayValue.toString()));
        }

        return true;
    }

    bool SCDatabaseSession::GetRelationStoredValue(
        sc::RecordId recordId, const sc::SCColumnDef& relationColumn,
        QVariant* outValue, QString* outError) const
    {
        if (outValue == nullptr)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Output value is null.");
            }
            return false;
        }
        outValue->clear();

        if (relationColumn.referenceTable.empty())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("关系字段缺少引用表。");
            }
            return false;
        }

        sc::SCTablePtr table;
        const sc::ErrorCode tableRc =
            db_->GetTable(relationColumn.referenceTable.c_str(), table);
        if (sc::Failed(tableRc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(tableRc);
            }
            return false;
        }

        sc::SCRecordPtr record;
        const sc::ErrorCode recordRc = table->GetRecord(recordId, record);
        if (sc::Failed(recordRc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(recordRc);
            }
            return false;
        }

        if (relationColumn.referenceStorageColumn.empty())
        {
            *outValue = QVariant::fromValue<qlonglong>(recordId);
            return true;
        }

        sc::SCValue value;
        const sc::ErrorCode valueRc =
            record->GetValue(relationColumn.referenceStorageColumn.c_str(), &value);
        if (valueRc == sc::SC_E_VALUE_IS_NULL)
        {
            return true;
        }
        if (sc::Failed(valueRc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(valueRc);
            }
            return false;
        }

        *outValue = ValueToVariant(value);
        return true;
    }

    bool SCDatabaseSession::GetCellDisplayValue(
        sc::RecordId recordId, const QString& columnName, QVariant* outValue,
        QString* outError) const
    {
        if (outValue == nullptr)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Output value is null.");
            }
            return false;
        }
        outValue->clear();

        if (!currentTable_)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("No table is selected.");
            }
            return false;
        }

        sc::SCSchemaPtr schema;
        const sc::ErrorCode schemaRc = currentTable_->GetSchema(schema);
        if (sc::Failed(schemaRc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(schemaRc);
            }
            return false;
        }

        sc::SCColumnDef column;
        const sc::ErrorCode columnRc =
            schema->FindColumn(columnName.toStdWString().c_str(), &column);
        if (sc::Failed(columnRc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(columnRc);
            }
            return false;
        }

        sc::SCRecordPtr record;
        const sc::ErrorCode recordRc = currentTable_->GetRecord(recordId, record);
        if (sc::Failed(recordRc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(recordRc);
            }
            return false;
        }

        if (column.columnKind != sc::ColumnKind::Relation ||
            column.referenceTable.empty() ||
            column.referenceStorageColumn.empty())
        {
            sc::SCValue value;
            const sc::ErrorCode valueRc =
                record->GetValue(column.name.c_str(), &value);
            if (valueRc == sc::SC_E_VALUE_IS_NULL)
            {
                return true;
            }
            if (sc::Failed(valueRc))
            {
                if (outError != nullptr)
                {
                    *outError = ErrorToString(valueRc);
                }
                return false;
            }

            *outValue = ValueToVariant(value);
            return true;
        }

        sc::SCValue storedValue;
        const sc::ErrorCode storedRc =
            record->GetValue(column.name.c_str(), &storedValue);
        if (storedRc == sc::SC_E_VALUE_IS_NULL)
        {
            return true;
        }
        if (sc::Failed(storedRc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(storedRc);
            }
            return false;
        }

        const QString storedText = ValueToDisplayString(storedValue);

        sc::SCTablePtr targetTable;
        const sc::ErrorCode tableRc =
            db_->GetTable(column.referenceTable.c_str(), targetTable);
        if (sc::Failed(tableRc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(tableRc);
            }
            return false;
        }

        const QString storageColumnName =
            QString::fromStdWString(column.referenceStorageColumn);
        const QString displayColumnName = column.referenceDisplayColumn.empty()
                                              ? storageColumnName
                                              : QString::fromStdWString(column.referenceDisplayColumn);

        bool found = false;
        sc::SCRecordPtr targetRecord;
        sc::SCRecordCursorPtr cursor;
        const sc::ErrorCode cursorRc = targetTable->EnumerateRecords(cursor);
        if (sc::Failed(cursorRc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(cursorRc);
            }
            return false;
        }

        while (cursor->Next(targetRecord) == sc::SC_OK && targetRecord)
        {
            sc::SCValue candidateStoredValue;
            const sc::ErrorCode candidateStoredRc =
                targetRecord->GetValue(storageColumnName.toStdWString().c_str(),
                                       &candidateStoredValue);
            if (candidateStoredRc == sc::SC_E_VALUE_IS_NULL)
            {
                continue;
            }
            if (sc::Failed(candidateStoredRc))
            {
                continue;
            }

            if (candidateStoredValue != storedValue)
            {
                continue;
            }

            found = true;
            break;
        }

        if (!found)
        {
            return false;
        }

        if (displayColumnName.compare(storageColumnName, Qt::CaseInsensitive) == 0)
        {
            *outValue = ValueToVariant(storedValue);
            return true;
        }

        sc::SCValue displayValue;
        const sc::ErrorCode displayRc =
            targetRecord->GetValue(displayColumnName.toStdWString().c_str(), &displayValue);
        if (displayRc == sc::SC_E_VALUE_IS_NULL)
        {
            *outValue = storedText;
            return true;
        }
        if (sc::Failed(displayRc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(displayRc);
            }
            return false;
        }

        const QString displayText = ValueToDisplayString(displayValue);
        if (displayText.isEmpty())
        {
            *outValue = storedText;
            return true;
        }
        if (storedText.isEmpty())
        {
            *outValue = displayText;
            return true;
        }
        *outValue = displayText + QStringLiteral(" (") + storedText +
                    QStringLiteral(")");
        return true;
    }

    bool SCDatabaseSession::GetCellStoredValue(
        sc::RecordId recordId, const QString& columnName, QVariant* outValue,
        QString* outError) const
    {
        if (outValue == nullptr)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Output value is null.");
            }
            return false;
        }
        outValue->clear();

        if (!currentTable_)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("No table is selected.");
            }
            return false;
        }

        sc::SCSchemaPtr schema;
        const sc::ErrorCode schemaRc = currentTable_->GetSchema(schema);
        if (sc::Failed(schemaRc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(schemaRc);
            }
            return false;
        }

        sc::SCColumnDef column;
        const sc::ErrorCode columnRc =
            schema->FindColumn(columnName.toStdWString().c_str(), &column);
        if (sc::Failed(columnRc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(columnRc);
            }
            return false;
        }

        sc::SCRecordPtr record;
        const sc::ErrorCode recordRc = currentTable_->GetRecord(recordId, record);
        if (sc::Failed(recordRc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(recordRc);
            }
            return false;
        }

        sc::SCValue value;
        const sc::ErrorCode valueRc = record->GetValue(column.name.c_str(), &value);
        if (valueRc == sc::SC_E_VALUE_IS_NULL)
        {
            return true;
        }
        if (sc::Failed(valueRc))
        {
            if (outError != nullptr)
            {
                *outError = ErrorToString(valueRc);
            }
            return false;
        }

        *outValue = ValueToVariant(value);
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
        sc::SCComputedTableViewPtr view;
        if (!BuildCurrentTableViewPreview(&view, outError))
        {
            return false;
        }

        currentTableView_ = view;
        return true;
    }

    bool SCDatabaseSession::BuildCurrentTableViewPreview(
        sc::SCComputedTableViewPtr* outView, QString* outError) const
    {
        return BuildComputedTableViewForState(
            db_, currentTable_, currentTableName_,
            sessionComputedColumnsByTable_,
            forceRebuildCurrentTableViewFailureForTest_,
            [this](sc::ErrorCode error) { return ErrorToString(error); },
            outView, outError);
    }

    bool SCDatabaseSession::ApplyColumnMutation(
        const wchar_t* actionName,
        const std::function<sc::ErrorCode(
            sc::SCSchemaPtr& schema, sc::SCComputedTableViewPtr* outPreviewView,
            QString* outError)>& mutation,
        const std::function<void()>& rollbackState, QString* outError)
    {
        if (outError != nullptr)
        {
            outError->clear();
        }

        if (HasPendingEdit())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral(
                    "Save or discard pending changes before changing schema.");
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

        sc::SCSchemaPtr schema;
        rc = currentTable_->GetSchema(schema);
        if (sc::Failed(rc))
        {
            RollbackEditAndReport(
                db_, edit.Get(), ErrorToString(rc),
                QStringLiteral("Column mutation"),
                [this](sc::ErrorCode error) { return ErrorToString(error); },
                outError);
            return false;
        }

        const sc::SCComputedTableViewPtr previousTableView = currentTableView_;
        sc::SCComputedTableViewPtr previewView;
        rc = mutation(schema, &previewView, outError);
        if (sc::Failed(rc))
        {
            const QString primaryError =
                (outError != nullptr && !outError->isEmpty())
                    ? *outError
                    : ErrorToString(rc);
            RollbackEditAndReport(
                db_, edit.Get(), primaryError,
                QStringLiteral("Column mutation"),
                [this](sc::ErrorCode error) { return ErrorToString(error); },
                outError);
            rollbackState();
            currentTableView_ = previousTableView;
            return false;
        }

        if (!previewView)
        {
            RollbackEditAndReport(
                db_, edit.Get(),
                QStringLiteral("Column mutation did not build a preview view."),
                QStringLiteral("Column mutation"),
                [this](sc::ErrorCode error) { return ErrorToString(error); },
                outError);
            rollbackState();
            currentTableView_ = previousTableView;
            return false;
        }

        rc = db_->Commit(edit.Get());
        if (sc::Failed(rc))
        {
            RollbackEditAndReport(
                db_, edit.Get(), ErrorToString(rc),
                QStringLiteral("Column commit"),
                [this](sc::ErrorCode error) { return ErrorToString(error); },
                outError);
            rollbackState();
            currentTableView_ = previousTableView;
            return false;
        }

        if (previewView)
        {
            currentTableView_ = previewView;
        }

        emit CurrentTableChanged();
        emit RecordsChanged();
        return true;
    }

    bool SCDatabaseSession::BeginAndCommitSingleAction(
        const wchar_t* actionName, const std::function<sc::ErrorCode()>& action,
        QString* outError)
    {
        if (HasPendingEdit())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral(
                    "A pending edit is active. Save or discard it before "
                    "starting a new action.");
            }
            return false;
        }

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
            RollbackEditAndReport(
                db_, edit.Get(), ErrorToString(rc),
                QStringLiteral("Edit action"),
                [this](sc::ErrorCode error) { return ErrorToString(error); },
                outError);
            return false;
        }

        rc = db_->Commit(edit.Get());
        if (sc::Failed(rc))
        {
            RollbackEditAndReport(
                db_, edit.Get(), ErrorToString(rc),
                QStringLiteral("Edit commit"),
                [this](sc::ErrorCode error) { return ErrorToString(error); },
                outError);
            return false;
        }

        return true;
    }

    bool SCDatabaseSession::HasPendingEdit() const noexcept
    {
        return pendingEdit_.Get() != nullptr;
    }

    bool SCDatabaseSession::ExportDebugPackage(
        const QString& filePath, const sc::SCExportRequest& request,
        QString* outError) const
    {
        if (!IsOpen())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("未打开数据库。");
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
        sc::SCValue* outValue, QString* outError) const
    {
        if (outValue == nullptr)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Output value is null.");
            }
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
            case sc::ValueKind::Binary: {
                std::vector<std::uint8_t> bytes;
                if (input.metaType().id() == qMetaTypeId<QByteArray>())
                {
                    const QByteArray raw = input.toByteArray();
                    if (!raw.isEmpty())
                    {
                        bytes.assign(
                            reinterpret_cast<const std::uint8_t*>(
                                raw.constData()),
                            reinterpret_cast<const std::uint8_t*>(
                                raw.constData()) + raw.size());
                    }
                    *outValue = sc::SCValue::FromBinary(std::move(bytes));
                    return sc::SC_OK;
                }

                if (!ParseBinaryHex(input.toString(), &bytes, outError))
                {
                    return sc::SC_E_INVALIDARG;
                }
                *outValue = sc::SCValue::FromBinary(std::move(bytes));
                return sc::SC_OK;
            }
            case sc::ValueKind::Null:
            default:
                return sc::SC_E_TYPE_MISMATCH;
        }
    }

    QString SCDatabaseSession::ErrorToString(sc::ErrorCode error) const
    {
        if (error == sc::SC_E_NOTIMPL)
        {
            return QStringLiteral("当前版本尚未支持该数据库格式的自动升级。");
        }
        if (error == sc::SC_E_UPGRADE_PATH_NOT_FOUND)
        {
            return QStringLiteral("数据库版本过旧，且没有可用的升级路径。");
        }
        if (error == sc::SC_E_JOURNAL_TABLE_MISSING)
        {
            return QStringLiteral("数据库缺少必要的 journal 表，无法继续打开。");
        }
        return QStringLiteral("Storage error: 0x") +
               QString::number(static_cast<qulonglong>(error), 16);
    }

}  // namespace StableCore::Storage::Editor
