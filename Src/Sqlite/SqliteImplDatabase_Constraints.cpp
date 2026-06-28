#include "Sqlite/SqliteImplCore.h"
#include "Sqlite/SqliteImplDatabase.h"

namespace StableCore::Storage
{
        void SqliteDatabase::CollectTouchedTableNames(
            const JournalTransaction& tx,
            std::vector<std::wstring>* outTableNames,
            bool reverseRenameResolution) const
        {
            if (outTableNames == nullptr)
            {
                return;
            }

            outTableNames->clear();
            const bool hasRenameTable =
                JournalTransactionContainsRenameTable(tx);
            for (const auto& entry : tx.entries)
            {
                const std::wstring resolvedName =
                    hasRenameTable
                        ? ResolveJournalTableNameToReplayState(
                              tx,
                              entry.tableName,
                              reverseRenameResolution)
                        : entry.tableName;
                if (resolvedName.empty())
                {
                    continue;
                }
                if (std::find(outTableNames->begin(),
                              outTableNames->end(),
                              resolvedName) == outTableNames->end())
                {
                    outTableNames->push_back(resolvedName);
                }
            }
        }

        ErrorCode SqliteDatabase::CaptureDeferredRenameState(
            const std::wstring& oldName,
            const std::wstring& newName,
            DeferredRenameState* outState) const
        {
            if (outState == nullptr)
            {
                return SC_E_POINTER;
            }
            if (SCCommon::EqualsIgnoreCase(oldName, newName))
            {
                return SC_E_INVALIDARG;
            }

            auto tableIt = tables_.find(oldName);
            if (tableIt == tables_.end())
            {
                for (auto it = tables_.begin(); it != tables_.end(); ++it)
                {
                    if (SCCommon::EqualsIgnoreCase(it->first, oldName))
                    {
                        tableIt = it;
                        break;
                    }
                }
            }
            if (tableIt == tables_.end())
            {
                return SC_E_TABLE_NOT_FOUND;
            }

            for (const auto& [existingName, existingTable] : tables_)
            {
                (void)existingTable;
                if (SCCommon::EqualsIgnoreCase(existingName, newName) &&
                    !SCCommon::EqualsIgnoreCase(existingName, tableIt->first))
                {
                    return SC_E_SCHEMA_VIOLATION;
                }
            }

            DeferredRenameState capturedState;
            capturedState.tableRef = tableIt->second;
            capturedState.oldName = tableIt->first;
            capturedState.newName = newName;

            for (const auto& [otherName, otherTableRef] : tables_)
            {
                (void)otherName;
                auto* otherTable = static_cast<SqliteTable*>(otherTableRef.Get());
                if (otherTable == nullptr)
                {
                    continue;
                }

                SCSchemaPtr otherSchema;
                const ErrorCode schemaRc = otherTable->GetSchema(otherSchema);
                if (Failed(schemaRc) || !otherSchema)
                {
                    return Failed(schemaRc) ? schemaRc : SC_E_FAIL;
                }

                auto* sqliteOtherSchema =
                    static_cast<SqliteSchema*>(otherSchema.Get());
                if (sqliteOtherSchema == nullptr)
                {
                    return SC_E_FAIL;
                }

                std::int32_t columnCount = 0;
                const ErrorCode columnCountRc =
                    otherSchema->GetColumnCount(&columnCount);
                if (Failed(columnCountRc))
                {
                    return columnCountRc;
                }

                for (std::int32_t columnIndex = 0; columnIndex < columnCount;
                     ++columnIndex)
                {
                    SCColumnDef column;
                    const ErrorCode columnRc =
                        otherSchema->GetColumn(columnIndex, &column);
                    if (Failed(columnRc))
                    {
                        return columnRc;
                    }

                    if (column.columnKind == ColumnKind::Relation &&
                        SCCommon::EqualsIgnoreCase(column.referenceTable,
                                         capturedState.oldName))
                    {
                        capturedState.relationColumns.push_back(
                            DeferredRenameColumnSnapshot{sqliteOtherSchema, column});
                    }
                }

                std::int32_t constraintCount = 0;
                const ErrorCode constraintCountRc =
                    otherSchema->GetConstraintCount(&constraintCount);
                if (Failed(constraintCountRc))
                {
                    return constraintCountRc;
                }

                for (std::int32_t constraintIndex = 0;
                     constraintIndex < constraintCount; ++constraintIndex)
                {
                    SCConstraintDef constraint;
                    const ErrorCode constraintRc =
                        otherSchema->GetConstraint(constraintIndex, &constraint);
                    if (Failed(constraintRc))
                    {
                        return constraintRc;
                    }

                    if (constraint.kind == SCConstraintKind::ForeignKey &&
                        SCCommon::EqualsIgnoreCase(constraint.referencedTable,
                                         capturedState.oldName))
                    {
                        capturedState.foreignKeyConstraints.push_back(
                            DeferredRenameConstraintSnapshot{
                                sqliteOtherSchema, constraint});
                    }
                }
            }

            *outState = std::move(capturedState);
            return SC_OK;
        }

        void SqliteDatabase::ApplyDeferredRenameWorkingState(
            const DeferredRenameState& state)
        {
            auto eraseCurrentEntry = [&]() {
                for (auto it = tables_.begin(); it != tables_.end(); ++it)
                {
                    if (it->second.Get() == state.tableRef.Get())
                    {
                        tables_.erase(it);
                        return;
                    }
                }
            };

            eraseCurrentEntry();
            tables_.emplace(state.newName, state.tableRef);

            auto* table = static_cast<SqliteTable*>(state.tableRef.Get());
            if (table != nullptr)
            {
                table->SetName(state.newName);
                if (auto* schema = table->Schema())
                {
                    schema->SetTableName(state.newName);
                }
            }

            for (const auto& snapshot : state.relationColumns)
            {
                if (snapshot.schema == nullptr)
                {
                    continue;
                }
                SCColumnDef updatedColumn = snapshot.column;
                updatedColumn.referenceTable = state.newName;
                snapshot.schema->ReplaceColumn(updatedColumn);
            }

            for (const auto& snapshot : state.foreignKeyConstraints)
            {
                if (snapshot.schema == nullptr)
                {
                    continue;
                }
                SCConstraintDef updatedConstraint = snapshot.constraint;
                updatedConstraint.referencedTable = state.newName;
                snapshot.schema->ReplaceConstraint(updatedConstraint);
            }

            MarkReferenceIndexDirty();
            MarkForeignKeyReferenceCacheDirty();
        }

        void SqliteDatabase::RollbackDeferredRenameWorkingState(
            const DeferredRenameState& state)
        {
            auto eraseCurrentEntry = [&]() {
                for (auto it = tables_.begin(); it != tables_.end(); ++it)
                {
                    if (it->second.Get() == state.tableRef.Get())
                    {
                        tables_.erase(it);
                        return;
                    }
                }
            };

            eraseCurrentEntry();
            tables_.emplace(state.oldName, state.tableRef);

            auto* table = static_cast<SqliteTable*>(state.tableRef.Get());
            if (table != nullptr)
            {
                table->SetName(state.oldName);
                if (auto* schema = table->Schema())
                {
                    schema->SetTableName(state.oldName);
                }
            }

            for (const auto& snapshot : state.relationColumns)
            {
                if (snapshot.schema != nullptr)
                {
                    snapshot.schema->ReplaceColumn(snapshot.column);
                }
            }

            for (const auto& snapshot : state.foreignKeyConstraints)
            {
                if (snapshot.schema != nullptr)
                {
                    snapshot.schema->ReplaceConstraint(snapshot.constraint);
                }
            }

            MarkReferenceIndexDirty();
            MarkForeignKeyReferenceCacheDirty();
        }

        DeferredRenameState* SqliteDatabase::FindDeferredRenameState(
            const std::wstring& oldName,
            const std::wstring& newName)
        {
            for (auto it = activeSchemaOps_.rbegin();
                 it != activeSchemaOps_.rend(); ++it)
            {
                if (it->kind != DeferredSchemaOp::Kind::RenameTable)
                {
                    continue;
                }
                if (SCCommon::EqualsIgnoreCase(it->rename.oldName, oldName) &&
                    SCCommon::EqualsIgnoreCase(it->rename.newName, newName))
                {
                    return &it->rename;
                }
            }
            return nullptr;
        }

        const DeferredRenameState* SqliteDatabase::FindDeferredRenameState(
            const std::wstring& oldName,
            const std::wstring& newName) const
        {
            for (auto it = activeSchemaOps_.rbegin();
                 it != activeSchemaOps_.rend(); ++it)
            {
                if (it->kind != DeferredSchemaOp::Kind::RenameTable)
                {
                    continue;
                }
                if (SCCommon::EqualsIgnoreCase(it->rename.oldName, oldName) &&
                    SCCommon::EqualsIgnoreCase(it->rename.newName, newName))
                {
                    return &it->rename;
                }
            }
            return nullptr;
        }

        ErrorCode SqliteDatabase::RecordDeferredRenameTable(
            const std::wstring& oldName,
            const std::wstring& newName)
        {
            DeferredRenameState capturedState;
            const ErrorCode captureRc =
                CaptureDeferredRenameState(oldName, newName, &capturedState);
            if (Failed(captureRc))
            {
                return captureRc;
            }

            ApplyDeferredRenameWorkingState(capturedState);

            DeferredSchemaOp op;
            op.kind = DeferredSchemaOp::Kind::RenameTable;
            op.rename = std::move(capturedState);
            activeSchemaOps_.push_back(std::move(op));
            return SC_OK;
        }

        bool SqliteDatabase::JournalTransactionContainsRenameTable(
            const JournalTransaction& tx) const
        {
            return std::any_of(
                tx.entries.begin(),
                tx.entries.end(),
                [](const JournalEntry& entry) {
                    return entry.op == JournalOp::RenameTable;
                });
        }

        std::wstring SqliteDatabase::ResolveJournalTableNameToReplayState(
            const JournalTransaction& tx,
            const std::wstring& tableName,
            bool reverseRenameResolution) const
        {
            std::wstring resolvedName = tableName;
            if (resolvedName.empty())
            {
                return resolvedName;
            }

            auto applyRename = [&resolvedName](const JournalEntry& entry,
                                               bool reverseDirection) {
                if (entry.op != JournalOp::RenameTable)
                {
                    return;
                }

                std::wstring oldName;
                std::wstring newName;
                if (entry.oldValue.AsStringCopy(&oldName) != SC_OK ||
                    entry.newValue.AsStringCopy(&newName) != SC_OK)
                {
                    return;
                }

                const std::wstring& fromName = reverseDirection ? newName : oldName;
                const std::wstring& toName = reverseDirection ? oldName : newName;
                if (SCCommon::EqualsIgnoreCase(resolvedName, fromName))
                {
                    resolvedName = toName;
                }
            };

            if (reverseRenameResolution)
            {
                for (auto it = tx.entries.rbegin(); it != tx.entries.rend(); ++it)
                {
                    applyRename(*it, true);
                }
            }
            else
            {
                for (const auto& entry : tx.entries)
                {
                    applyRename(entry, false);
                }
            }

            return resolvedName;
        }

        bool SqliteDatabase::JournalEntryMatchesCurrentTableName(
            const JournalEntry& entry,
            const std::wstring& tableName) const
        {
            if (SCCommon::EqualsIgnoreCase(entry.tableName, tableName))
            {
                return true;
            }
            if (!JournalTransactionContainsRenameTable(activeJournal_))
            {
                return false;
            }
            return SCCommon::EqualsIgnoreCase(
                ResolveJournalTableNameToReplayState(activeJournal_,
                                                     entry.tableName,
                                                     false),
                tableName);
        }

        ErrorCode SqliteDatabase::ReadConstraintValue(SqliteTable* table,
                                                      const SqliteRecordData& recordData,
                                                      const std::wstring& columnName,
                                                      const std::wstring* overrideFieldName,
                                                      const SCValue* overrideValue,
                                                      SCValue* outValue,
                                                      bool* outIsNull) const
        {
            if (table == nullptr || outValue == nullptr || outIsNull == nullptr)
            {
                return SC_E_POINTER;
            }

            SCSchemaPtr schema;
            const ErrorCode schemaRc = table->GetSchema(schema);
            if (Failed(schemaRc) || !schema)
            {
                return Failed(schemaRc) ? schemaRc : SC_E_FAIL;
            }

            SCColumnDef column;
            const ErrorCode columnRc = schema->FindColumn(columnName.c_str(), &column);
            if (Failed(columnRc))
            {
                return columnRc;
            }

            if (overrideFieldName != nullptr && overrideValue != nullptr && SCCommon::EqualsIgnoreCase(*overrideFieldName, column.name))
            {
                *outValue = *overrideValue;
            } else
            {
                const auto valueIt = recordData.values.find(column.name);
                *outValue = (valueIt != recordData.values.end()) ? valueIt->second : column.defaultValue;
            }

            *outIsNull = outValue->IsNull();
            return SC_OK;
        }

        ErrorCode SqliteDatabase::ValidateConstraintUniqueness(SqliteTable* table,
                                                               const SCConstraintDef& constraint,
                                                               const std::shared_ptr<SqliteRecordData>& candidateData,
                                                               const std::wstring* overrideFieldName,
                                                               const SCValue* overrideValue) const
        {
            if (table == nullptr)
            {
                return SC_E_POINTER;
            }
            if (constraint.kind != SCConstraintKind::PrimaryKey && constraint.kind != SCConstraintKind::Unique)
            {
                return SC_OK;
            }

            auto buildKeySignature = [&](const std::shared_ptr<SqliteRecordData>& recordData,
                                         const std::wstring* fieldOverrideName,
                                         const SCValue* fieldOverrideValue,
                                         std::wstring* outSignature,
                                         bool* outHasNull) -> ErrorCode {
                if (recordData == nullptr || outSignature == nullptr || outHasNull == nullptr)
                {
                    return SC_E_POINTER;
                }

                outSignature->clear();
                *outHasNull = false;

                for (const std::wstring& columnName : constraint.columns)
                {
                    SCValue value;
                    bool columnIsNull = false;
                    const ErrorCode valueRc = ReadConstraintValue(table,
                                                                  *recordData,
                                                                  columnName,
                                                                  fieldOverrideName,
                                                                  fieldOverrideValue,
                                                                  &value,
                                                                  &columnIsNull);
                    if (Failed(valueRc))
                    {
                        return valueRc;
                    }
                    if (columnIsNull)
                    {
                        *outHasNull = true;
                    }
                    SCCommon::AppendToken(outSignature, ImportSerializer::SerializeConstraintKeyValue(value));
                }

                return SC_OK;
            };

            if (candidateData != nullptr)
            {
                std::wstring candidateSignature;
                bool candidateHasNull = false;
                const ErrorCode candidateKeyRc =
                    buildKeySignature(candidateData, overrideFieldName, overrideValue, &candidateSignature, &candidateHasNull);
                if (Failed(candidateKeyRc))
                {
                    return candidateKeyRc;
                }

                if (candidateHasNull)
                {
                    if (constraint.kind == SCConstraintKind::PrimaryKey)
                    {
                        SetConstraintViolation(SCConstraintViolationInfo{
                            table->Name(),
                            constraint.name,
                            constraint.kind,
                            constraint.columns,
                            candidateData->id,
                            0,
                            L"Primary key columns cannot be null.",
                        });
                        return SC_E_CONSTRAINT_VIOLATION;
                    }
                    return SC_OK;
                }

                for (const auto& [recordId, recordData] : table->Records())
                {
                    if (recordData == nullptr || recordData->state == RecordState::Deleted ||
                        recordData.get() == candidateData.get())
                    {
                        continue;
                    }

                    std::wstring otherSignature;
                    bool otherHasNull = false;
                    const ErrorCode otherKeyRc =
                        buildKeySignature(recordData, nullptr, nullptr, &otherSignature, &otherHasNull);
                    if (Failed(otherKeyRc))
                    {
                        return otherKeyRc;
                    }
                    if (otherHasNull || otherSignature != candidateSignature)
                    {
                        continue;
                    }

                    SetConstraintViolation(SCConstraintViolationInfo{
                        table->Name(),
                        constraint.name,
                        constraint.kind,
                        constraint.columns,
                        candidateData->id,
                        recordData->id,
                        L"Unique key already exists.",
                    });
                    return SC_E_CONSTRAINT_VIOLATION;
                }
                return SC_OK;
            }

            std::unordered_set<std::wstring> seenKeySignatures;
            seenKeySignatures.reserve(table->Records().size());

            for (const auto& [recordId, recordData] : table->Records())
            {
                if (recordData == nullptr || recordData->state == RecordState::Deleted)
                {
                    continue;
                }

                std::wstring keySignature;
                bool hasNull = false;
                const ErrorCode keyRc = buildKeySignature(recordData, nullptr, nullptr, &keySignature, &hasNull);
                if (Failed(keyRc))
                {
                    return keyRc;
                }

                if (hasNull)
                {
                    if (constraint.kind == SCConstraintKind::PrimaryKey)
                    {
                        SetConstraintViolation(SCConstraintViolationInfo{
                            table->Name(),
                            constraint.name,
                            constraint.kind,
                            constraint.columns,
                            recordData->id,
                            0,
                            L"Primary key columns cannot be null.",
                        });
                        return SC_E_CONSTRAINT_VIOLATION;
                    }
                    continue;
                }

                if (!seenKeySignatures.insert(keySignature).second)
                {
                    SetConstraintViolation(SCConstraintViolationInfo{
                        table->Name(),
                        constraint.name,
                        constraint.kind,
                        constraint.columns,
                        recordData->id,
                        0,
                        L"Unique key already exists.",
                    });
                    return SC_E_CONSTRAINT_VIOLATION;
                }
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::ValidateCheckConstraint(SqliteTable* table,
                                                          const SCConstraintDef& constraint,
                                                          const std::shared_ptr<SqliteRecordData>& candidateData,
                                                          const std::wstring* overrideFieldName,
                                                          const SCValue* overrideValue) const
        {
            if (table == nullptr)
            {
                return SC_E_POINTER;
            }
            if (constraint.kind != SCConstraintKind::Check)
            {
                return SC_OK;
            }
            if (overrideFieldName != nullptr)
            {
                const auto usesField = std::any_of(constraint.columns.begin(),
                                                   constraint.columns.end(),
                                                   [overrideFieldName](const std::wstring& columnName) {
                                                       return SCCommon::EqualsIgnoreCase(columnName, *overrideFieldName);
                                                   });
                if (!usesField)
                {
                    return SC_OK;
                }
            }

            if (candidateData == nullptr)
            {
                for (const auto& [_, recordData] : table->Records())
                {
                    if (recordData == nullptr || recordData->state == RecordState::Deleted)
                    {
                        continue;
                    }
                    const ErrorCode rc =
                        ValidateCheckConstraint(table, constraint, recordData, overrideFieldName, overrideValue);
                    if (Failed(rc))
                    {
                        return rc;
                    }
                }
                return SC_OK;
            }

            SCSchemaPtr schema;
            const ErrorCode schemaRc = table->GetSchema(schema);
            if (Failed(schemaRc) || !schema)
            {
                return Failed(schemaRc) ? schemaRc : SC_E_FAIL;
            }

            std::set<std::wstring> allowedColumns(constraint.columns.begin(), constraint.columns.end());
            ConstraintExpressionParser parser(constraint.checkExpression, std::move(allowedColumns));
            ConstraintExpressionAst ast;
            const ErrorCode parseRc = parser.Parse(&ast);
            if (Failed(parseRc))
            {
                return parseRc;
            }

            std::unordered_map<std::wstring, SCValue> values;
            values.reserve(constraint.columns.size());
            for (const std::wstring& columnName : constraint.columns)
            {
                SCValue value;
                bool isNull = false;
                const ErrorCode valueRc = ReadConstraintValue(table,
                                                              *candidateData,
                                                              columnName,
                                                              overrideFieldName,
                                                              overrideValue,
                                                              &value,
                                                              &isNull);
                if (Failed(valueRc))
                {
                    return valueRc;
                }
                values[columnName] = value;
            }

            SCValue result;
            const ErrorCode evalRc = EvaluateConstraintExpressionNode(*ast.root, values, &result);
            if (Failed(evalRc))
            {
                return evalRc;
            }

            if (result.IsNull())
            {
                return SC_OK;
            }
            if (ValueComparator::IsTruthy(result))
            {
                return SC_OK;
            }

            SetConstraintViolation(SCConstraintViolationInfo{
                table->Name(),
                constraint.name,
                constraint.kind,
                constraint.columns,
                candidateData != nullptr ? candidateData->id : 0,
                0,
                L"Check constraint evaluated to false.",
            });
            return SC_E_CONSTRAINT_VIOLATION;
        }

        ErrorCode SqliteDatabase::ValidateForeignKeyConstraint(SqliteTable* table,
                                                               const SCConstraintDef& constraint,
                                                               const std::shared_ptr<SqliteRecordData>& candidateData,
                                                               const std::wstring* overrideFieldName,
                                                               const SCValue* overrideValue) const
        {
            if (table == nullptr)
            {
                return SC_E_POINTER;
            }
            if (constraint.kind != SCConstraintKind::ForeignKey)
            {
                return SC_OK;
            }
            if (constraint.referencedTable.empty())
            {
                return SC_E_SCHEMA_VIOLATION;
            }
            if (overrideFieldName != nullptr)
            {
                const auto usesField = std::any_of(constraint.columns.begin(),
                                                   constraint.columns.end(),
                                                   [overrideFieldName](const std::wstring& columnName) {
                                                       return SCCommon::EqualsIgnoreCase(columnName, *overrideFieldName);
                                                   });
                if (!usesField)
                {
                    return SC_OK;
                }
            }

            if (candidateData == nullptr)
            {
                for (const auto& [_, recordData] : table->Records())
                {
                    if (recordData == nullptr || recordData->state == RecordState::Deleted)
                    {
                        continue;
                    }
                    const ErrorCode rc =
                        ValidateForeignKeyConstraint(table, constraint, recordData, overrideFieldName, overrideValue);
                    if (Failed(rc))
                    {
                        return rc;
                    }
                }
                return SC_OK;
            }

            SCSchemaPtr schema;
            const ErrorCode schemaRc = table->GetSchema(schema);
            if (Failed(schemaRc) || !schema)
            {
                return Failed(schemaRc) ? schemaRc : SC_E_FAIL;
            }

            auto targetIt = tables_.find(constraint.referencedTable);
            if (targetIt == tables_.end())
            {
                return SC_E_TABLE_NOT_FOUND;
            }

            auto* targetTable = static_cast<SqliteTable*>(targetIt->second.Get());
            if (targetTable == nullptr)
            {
                return SC_E_FAIL;
            }

            SCSchemaPtr targetSchema;
            const ErrorCode targetSchemaRc = targetTable->GetSchema(targetSchema);
            if (Failed(targetSchemaRc) || !targetSchema)
            {
                return Failed(targetSchemaRc) ? targetSchemaRc : SC_E_FAIL;
            }

            std::vector<SCValue> sourceValues;
            sourceValues.reserve(constraint.columns.size());
            for (std::size_t i = 0; i < constraint.columns.size(); ++i)
            {
                SCValue value;
                bool isNull = false;
                const ErrorCode valueRc = ReadConstraintValue(table,
                                                              *candidateData,
                                                              constraint.columns[i],
                                                              overrideFieldName,
                                                              overrideValue,
                                                              &value,
                                                              &isNull);
                if (Failed(valueRc))
                {
                    return valueRc;
                }
                if (isNull)
                {
                    return SC_OK;
                }
                sourceValues.push_back(value);
            }

            bool matched = false;
            for (const auto& [candidateId, candidateDataIt] : targetTable->Records())
            {
                if (candidateDataIt == nullptr || candidateDataIt->state == RecordState::Deleted)
                {
                    continue;
                }

                bool rowMatches = true;
                for (std::size_t i = 0; i < constraint.columns.size(); ++i)
                {
                    const std::wstring targetColumnName =
                        (i < constraint.referencedColumns.size() && !constraint.referencedColumns[i].empty())
                            ? constraint.referencedColumns[i]
                            : constraint.columns[i];

                    SCValue targetValue;
                    bool isNull = false;
                    const ErrorCode valueRc = ReadConstraintValue(targetTable,
                                                                  *candidateDataIt,
                                                                  targetColumnName,
                                                                  nullptr,
                                                                  nullptr,
                                                                  &targetValue,
                                                                  &isNull);
                    if (Failed(valueRc))
                    {
                        return valueRc;
                    }
                    if (isNull || targetValue != sourceValues[i])
                    {
                        rowMatches = false;
                        break;
                    }
                }

                if (!rowMatches)
                {
                    continue;
                }

                if (matched)
                {
                    SetConstraintViolation(SCConstraintViolationInfo{
                        table->Name(),
                        constraint.name,
                        constraint.kind,
                        constraint.columns,
                        candidateData != nullptr ? candidateData->id : 0,
                        candidateId,
                        L"Referenced key is not unique.",
                    });
                    return SC_E_CONSTRAINT_VIOLATION;
                }
                matched = true;
            }

            if (!matched)
            {
                SetConstraintViolation(SCConstraintViolationInfo{
                    table->Name(),
                    constraint.name,
                    constraint.kind,
                    constraint.columns,
                    candidateData != nullptr ? candidateData->id : 0,
                    0,
                    L"Referenced key does not exist.",
                });
                return SC_E_CONSTRAINT_VIOLATION;
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::ApplyForeignKeyActionsForTableDelete(
            SqliteTable* table, const std::shared_ptr<SqliteRecordData>& candidateData)
        {
            if (table == nullptr)
            {
                return SC_E_POINTER;
            }
            if (candidateData == nullptr)
            {
                return SC_E_POINTER;
            }

            SCSchemaPtr schema;
            const ErrorCode schemaRc = table->GetSchema(schema);
            if (Failed(schemaRc) || !schema)
            {
                return Failed(schemaRc) ? schemaRc : SC_E_FAIL;
            }
            auto* sourceSchema = table->Schema();
            if (sourceSchema == nullptr)
            {
                return SC_E_FAIL;
            }

            struct Scope
            {
                std::unordered_set<std::wstring>* set{nullptr};
                std::wstring key;
                ~Scope()
                {
                    if (set != nullptr)
                    {
                        set->erase(key);
                    }
                }
            };

            const std::wstring parentScopeKey = table->Name() + L"|" + std::to_wstring(candidateData->id);
            if (!activeConstraintPropagationKeys_.insert(parentScopeKey).second)
            {
                return SC_OK;
            }
            Scope parentScope{&activeConstraintPropagationKeys_, parentScopeKey};

            for (const auto& [_, tableRef] : tables_)
            {
                auto* sourceTable = static_cast<SqliteTable*>(tableRef.Get());
                if (sourceTable == nullptr)
                {
                    continue;
                }

                SCSchemaPtr sourceSchema;
                if (Failed(sourceTable->GetSchema(sourceSchema)) || !sourceSchema)
                {
                    continue;
                }
                auto* sourceSchemaImpl = sourceTable->Schema();
                if (sourceSchemaImpl == nullptr)
                {
                    continue;
                }

                std::int32_t constraintCount = 0;
                if (Failed(sourceSchema->GetConstraintCount(&constraintCount)))
                {
                    continue;
                }

                for (std::int32_t constraintIndex = 0; constraintIndex < constraintCount; ++constraintIndex)
                {
                    SCConstraintDef constraint;
                    if (Failed(sourceSchema->GetConstraint(constraintIndex, &constraint)))
                    {
                        continue;
                    }
                    if (constraint.kind != SCConstraintKind::ForeignKey ||
                        !SCCommon::EqualsIgnoreCase(constraint.referencedTable, table->Name()))
                    {
                        continue;
                    }

                    std::vector<SCValue> parentValues;
                    parentValues.reserve(constraint.columns.size());
                    bool anyNull = false;
                    for (std::size_t i = 0; i < constraint.columns.size(); ++i)
                    {
                        SCValue value;
                        bool isNull = false;
                        const ErrorCode valueRc = ReadConstraintValue(table,
                                                                      *candidateData,
                                                                      ResolveForeignKeyReferencedColumn(constraint, i),
                                                                      nullptr,
                                                                      nullptr,
                                                                      &value,
                                                                      &isNull);
                        if (Failed(valueRc))
                        {
                            return valueRc;
                        }
                        if (isNull)
                        {
                            anyNull = true;
                        }
                        parentValues.push_back(value);
                    }
                    if (anyNull)
                    {
                        continue;
                    }

                    for (const auto& [childId, childData] : sourceTable->Records())
                    {
                        if (childData == nullptr || childData->state == RecordState::Deleted)
                        {
                            continue;
                        }

                        bool matches = true;
                        for (std::size_t i = 0; i < constraint.columns.size(); ++i)
                        {
                            SCValue childValue;
                            bool isNull = false;
                            const ErrorCode childRc = ReadConstraintValue(sourceTable,
                                                                           *childData,
                                                                           constraint.columns[i],
                                                                           nullptr,
                                                                           nullptr,
                                                                           &childValue,
                                                                           &isNull);
                            if (Failed(childRc))
                            {
                                return childRc;
                            }
                            if (isNull || childValue != parentValues[i])
                            {
                                matches = false;
                                break;
                            }
                        }

                        if (!matches)
                        {
                            continue;
                        }

                        const std::wstring scopeKey = sourceTable->Name() + L"|" + std::to_wstring(childId);
                        if (!activeConstraintPropagationKeys_.insert(scopeKey).second)
                        {
                            continue;
                        }
                        Scope scope{&activeConstraintPropagationKeys_, scopeKey};

                        switch (constraint.onDelete)
                        {
                            case SCForeignKeyAction::Restrict:
                            case SCForeignKeyAction::NoAction:
                                SetConstraintViolation(SCConstraintViolationInfo{
                                    sourceTable->Name(),
                                    constraint.name,
                                    constraint.kind,
                                    constraint.columns,
                                    childId,
                                    candidateData->id,
                                    L"Foreign key delete/update would leave dependent rows.",
                                });
                                return SC_E_CONSTRAINT_VIOLATION;
                            case SCForeignKeyAction::Cascade:
                                if (const ErrorCode deleteRc = sourceTable->DeleteRecord(childId);
                                    Failed(deleteRc) && deleteRc != SC_E_RECORD_DELETED)
                                {
                                    return deleteRc;
                                }
                                break;
                            case SCForeignKeyAction::SetNull:
                            case SCForeignKeyAction::SetDefault: {
                                for (std::size_t i = 0; i < constraint.columns.size(); ++i)
                                {
                                    const std::wstring childColumnName = constraint.columns[i];
                                    SCValue nextValue = SCValue::Null();
                                    if (constraint.onDelete == SCForeignKeyAction::SetDefault)
                                    {
                                        const SCColumnDef* childColumn = sourceSchemaImpl->FindColumnDef(childColumnName);
                                        if (childColumn == nullptr)
                                        {
                                            return SC_E_COLUMN_NOT_FOUND;
                                        }
                                        nextValue = childColumn->defaultValue;
                                    }
                                    const ErrorCode writeRc = WriteValue(sourceTable, childData, childColumnName, nextValue);
                                    if (Failed(writeRc) && writeRc != SC_E_RECORD_DELETED)
                                    {
                                        return writeRc;
                                    }
                                }
                                break;
                            }
                        }
                    }
                }
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::ApplyForeignKeyActionsForColumnUpdate(
            SqliteTable* table,
            const std::shared_ptr<SqliteRecordData>& candidateData,
            const std::wstring& fieldName,
            const SCValue& oldValue,
            const SCValue& newValue)
        {
            if (table == nullptr)
            {
                return SC_E_POINTER;
            }
            if (candidateData == nullptr)
            {
                return SC_E_POINTER;
            }

            if (oldValue == newValue)
            {
                return SC_OK;
            }

            struct Scope
            {
                std::unordered_set<std::wstring>* set{nullptr};
                std::wstring key;
                ~Scope()
                {
                    if (set != nullptr)
                    {
                        set->erase(key);
                    }
                }
            };

            const std::wstring parentScopeKey = table->Name() + L"|" + std::to_wstring(candidateData->id);
            if (!activeConstraintPropagationKeys_.insert(parentScopeKey).second)
            {
                return SC_OK;
            }
            Scope parentScope{&activeConstraintPropagationKeys_, parentScopeKey};

            const auto* referenceEntries = GetForeignKeyReferenceEntries(table->Name(), fieldName);
            if (referenceEntries == nullptr || referenceEntries->empty())
            {
                return SC_OK;
            }

            for (const auto& entry : *referenceEntries)
            {
                auto sourceIt = tables_.find(entry.sourceTableName);
                if (sourceIt == tables_.end())
                {
                    continue;
                }

                auto* sourceTable = static_cast<SqliteTable*>(sourceIt->second.Get());
                if (sourceTable == nullptr)
                {
                    continue;
                }

                SCSchemaPtr sourceSchema;
                if (Failed(sourceTable->GetSchema(sourceSchema)) || !sourceSchema)
                {
                    continue;
                }
                auto* sourceSchemaImpl = sourceTable->Schema();
                if (sourceSchemaImpl == nullptr)
                {
                    continue;
                }

                std::vector<SCValue> parentOldValues;
                std::vector<SCValue> parentNewValues;
                parentOldValues.reserve(entry.constraint.columns.size());
                parentNewValues.reserve(entry.constraint.columns.size());
                bool anyNull = false;
                for (std::size_t i = 0; i < entry.constraint.columns.size(); ++i)
                {
                    const std::wstring referencedColumn = ResolveForeignKeyReferencedColumn(entry.constraint, i);

                    SCValue oldTupleValue;
                    bool oldIsNull = false;
                    const ErrorCode oldRc = ReadConstraintValue(table,
                                                                 *candidateData,
                                                                 referencedColumn,
                                                                 nullptr,
                                                                 nullptr,
                                                                 &oldTupleValue,
                                                                 &oldIsNull);
                    if (Failed(oldRc))
                    {
                        return oldRc;
                    }
                    parentOldValues.push_back(oldTupleValue);
                    anyNull = anyNull || oldIsNull;

                    SCValue newTupleValue;
                    bool newIsNull = false;
                    const ErrorCode newRc = ReadConstraintValue(table,
                                                                 *candidateData,
                                                                 referencedColumn,
                                                                 &fieldName,
                                                                 &newValue,
                                                                 &newTupleValue,
                                                                 &newIsNull);
                    if (Failed(newRc))
                    {
                        return newRc;
                    }
                    parentNewValues.push_back(newTupleValue);
                }

                if (anyNull)
                {
                    continue;
                }

                for (const auto& [childId, childData] : sourceTable->Records())
                {
                    if (childData == nullptr || childData->state == RecordState::Deleted)
                    {
                        continue;
                    }

                    bool matches = true;
                    for (std::size_t i = 0; i < entry.constraint.columns.size(); ++i)
                    {
                        SCValue childValue;
                        bool isNull = false;
                        const ErrorCode childRc = ReadConstraintValue(sourceTable,
                                                                       *childData,
                                                                       entry.constraint.columns[i],
                                                                       nullptr,
                                                                       nullptr,
                                                                       &childValue,
                                                                       &isNull);
                        if (Failed(childRc))
                        {
                            return childRc;
                        }
                        if (isNull || childValue != parentOldValues[i])
                        {
                            matches = false;
                            break;
                        }
                    }

                    if (!matches)
                    {
                        continue;
                    }

                    const std::wstring scopeKey = sourceTable->Name() + L"|" + std::to_wstring(childId);
                    if (!activeConstraintPropagationKeys_.insert(scopeKey).second)
                    {
                        continue;
                    }
                    Scope scope{&activeConstraintPropagationKeys_, scopeKey};

                    switch (entry.constraint.onUpdate)
                    {
                        case SCForeignKeyAction::Restrict:
                        case SCForeignKeyAction::NoAction:
                            SetConstraintViolation(SCConstraintViolationInfo{
                                sourceTable->Name(),
                                entry.constraint.name,
                                entry.constraint.kind,
                                entry.constraint.columns,
                                childId,
                                candidateData->id,
                                L"Foreign key update would leave dependent rows.",
                            });
                            return SC_E_CONSTRAINT_VIOLATION;
                        case SCForeignKeyAction::Cascade:
                            for (std::size_t i = 0; i < entry.constraint.columns.size(); ++i)
                            {
                                const ErrorCode writeRc = WriteValue(sourceTable,
                                                                     childData,
                                                                     entry.constraint.columns[i],
                                                                     parentNewValues[i]);
                                if (Failed(writeRc) && writeRc != SC_E_RECORD_DELETED)
                                {
                                    return writeRc;
                                }
                            }
                            break;
                        case SCForeignKeyAction::SetNull:
                        case SCForeignKeyAction::SetDefault: {
                            for (std::size_t i = 0; i < entry.constraint.columns.size(); ++i)
                            {
                                SCValue nextValue = SCValue::Null();
                                if (entry.constraint.onUpdate == SCForeignKeyAction::SetDefault)
                                {
                                    const SCColumnDef* childColumn = sourceSchemaImpl->FindColumnDef(entry.constraint.columns[i]);
                                    if (childColumn == nullptr)
                                    {
                                        return SC_E_COLUMN_NOT_FOUND;
                                    }
                                    nextValue = childColumn->defaultValue;
                                }
                                const ErrorCode writeRc =
                                    WriteValue(sourceTable, childData, entry.constraint.columns[i], nextValue);
                                if (Failed(writeRc) && writeRc != SC_E_RECORD_DELETED)
                                {
                                    return writeRc;
                                }
                            }
                            break;
                        }
                    }
                }
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::ValidateTableConstraints(SqliteTable* table,
                                                           const std::shared_ptr<SqliteRecordData>& candidateData,
                                                           const std::wstring* overrideFieldName,
                                                           const SCValue* overrideValue,
                                                           const SCConstraintDef* specificConstraint) const
        {
            if (table == nullptr)
            {
                return SC_E_POINTER;
            }

            SCSchemaPtr schema;
            const ErrorCode schemaRc = table->GetSchema(schema);
            if (Failed(schemaRc) || !schema)
            {
                return Failed(schemaRc) ? schemaRc : SC_E_FAIL;
            }

            std::int32_t constraintCount = 0;
            const ErrorCode countRc = schema->GetConstraintCount(&constraintCount);
            if (Failed(countRc))
            {
                return countRc;
            }

            auto validateRecord = [&](const std::shared_ptr<SqliteRecordData>& recordData) -> ErrorCode {
                for (std::int32_t index = 0; index < constraintCount; ++index)
                {
                    SCConstraintDef constraint;
                    const ErrorCode constraintRc = schema->GetConstraint(index, &constraint);
                    if (Failed(constraintRc))
                    {
                        return constraintRc;
                    }

                    if (constraint.kind != SCConstraintKind::PrimaryKey && constraint.kind != SCConstraintKind::Unique &&
                        constraint.kind != SCConstraintKind::Check && constraint.kind != SCConstraintKind::ForeignKey)
                    {
                        continue;
                    }

                    if (specificConstraint != nullptr && !SCCommon::EqualsIgnoreCase(constraint.name, specificConstraint->name))
                    {
                        continue;
                    }

                    if (overrideFieldName != nullptr)
                    {
                        const auto usesField = std::any_of(constraint.columns.begin(),
                                                           constraint.columns.end(),
                                                           [overrideFieldName](const std::wstring& columnName) {
                                                               return SCCommon::EqualsIgnoreCase(columnName, *overrideFieldName);
                                                           });
                        if (!usesField && constraint.kind != SCConstraintKind::ForeignKey)
                        {
                            continue;
                        }
                    }

                    ErrorCode validateRc = SC_OK;
                    switch (constraint.kind)
                    {
                        case SCConstraintKind::PrimaryKey:
                        case SCConstraintKind::Unique:
                            validateRc = ValidateConstraintUniqueness(
                                table, constraint, recordData, overrideFieldName, overrideValue);
                            break;
                        case SCConstraintKind::Check:
                            validateRc =
                                ValidateCheckConstraint(table, constraint, recordData, overrideFieldName, overrideValue);
                            break;
                        case SCConstraintKind::ForeignKey:
                            validateRc = ValidateForeignKeyConstraint(
                                table, constraint, recordData, overrideFieldName, overrideValue);
                            break;
                        default:
                            break;
                    }

                    if (Failed(validateRc))
                    {
                        return validateRc;
                    }
                }
                return SC_OK;
            };

            if (candidateData != nullptr)
            {
                return validateRecord(candidateData);
            }

            for (const auto& [_, recordData] : table->Records())
            {
                if (recordData == nullptr || recordData->state == RecordState::Deleted)
                {
                    continue;
                }
                const ErrorCode validateRc = validateRecord(recordData);
                if (Failed(validateRc))
                {
                    return validateRc;
                }
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::ValidateForeignKeyReferencesToTable(SqliteTable* table,
                                                                       const std::shared_ptr<SqliteRecordData>& candidateData,
                                                                       const std::wstring* overrideFieldName,
                                                                       const SCValue* overrideValue) const
        {
            if (table == nullptr)
            {
                return SC_E_POINTER;
            }

            SCSchemaPtr schema;
            const ErrorCode schemaRc = table->GetSchema(schema);
            if (Failed(schemaRc) || !schema)
            {
                return Failed(schemaRc) ? schemaRc : SC_E_FAIL;
            }

            const auto* referenceEntries = GetForeignKeyReferenceEntries(table->Name());
            if (referenceEntries == nullptr || referenceEntries->empty())
            {
                return SC_OK;
            }

            std::int32_t columnCount = 0;
            const ErrorCode countRc = schema->GetColumnCount(&columnCount);
            if (Failed(countRc))
            {
                return countRc;
            }

            for (std::int32_t columnIndex = 0; columnIndex < columnCount; ++columnIndex)
            {
                SCColumnDef column;
                if (Failed(schema->GetColumn(columnIndex, &column)))
                {
                    continue;
                }

                const auto* columnEntries = GetForeignKeyReferenceEntries(table->Name(), column.name);
                if (columnEntries == nullptr || columnEntries->empty())
                {
                    continue;
                }

                for (const auto& entry : *columnEntries)
                {
                    auto sourceIt = tables_.find(entry.sourceTableName);
                    if (sourceIt == tables_.end())
                    {
                        continue;
                    }

                    auto* sourceTable = static_cast<SqliteTable*>(sourceIt->second.Get());
                    if (sourceTable == nullptr)
                    {
                        continue;
                    }

                    SCSchemaPtr sourceSchema;
                    if (Failed(sourceTable->GetSchema(sourceSchema)) || !sourceSchema)
                    {
                        continue;
                    }

                    std::vector<SCValue> targetValues;
                    targetValues.reserve(entry.constraint.columns.size());
                    bool anyNull = false;
                    for (std::size_t i = 0; i < entry.constraint.columns.size(); ++i)
                    {
                        const std::wstring referencedColumnName = ResolveForeignKeyReferencedColumn(entry.constraint, i);
                        SCValue value;
                        bool isNull = false;
                        const ErrorCode valueRc = ReadConstraintValue(table,
                                                                      *candidateData,
                                                                      referencedColumnName,
                                                                      overrideFieldName,
                                                                      overrideValue,
                                                                      &value,
                                                                      &isNull);
                        if (Failed(valueRc))
                        {
                            return valueRc;
                        }
                        if (isNull)
                        {
                            anyNull = true;
                        }
                        targetValues.push_back(value);
                    }

                    if (anyNull)
                    {
                        continue;
                    }

                    for (const auto& [candidateId, candidateDataIt] : sourceTable->Records())
                    {
                        if (candidateDataIt == nullptr || candidateDataIt->state == RecordState::Deleted)
                        {
                            continue;
                        }

                        bool match = true;
                        for (std::size_t i = 0; i < entry.constraint.columns.size(); ++i)
                        {
                            SCValue sourceValue;
                            bool isNull = false;
                            const ErrorCode valueRc = ReadConstraintValue(sourceTable,
                                                                          *candidateDataIt,
                                                                          entry.constraint.columns[i],
                                                                          nullptr,
                                                                          nullptr,
                                                                          &sourceValue,
                                                                          &isNull);
                            if (Failed(valueRc))
                            {
                                return valueRc;
                            }
                            if (isNull || sourceValue != targetValues[i])
                            {
                                match = false;
                                break;
                            }
                        }

                        if (match)
                        {
                            return SC_E_CONSTRAINT_VIOLATION;
                        }
                    }
                }
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::ValidateUniqueAndPrimaryKeyConstraints(
            SqliteTable* table,
            const std::shared_ptr<SqliteRecordData>& candidateData,
            const std::wstring* overrideFieldName,
            const SCValue* overrideValue,
            const SCConstraintDef* specificConstraint) const
        {
            return ValidateTableConstraints(table, candidateData, overrideFieldName, overrideValue, specificConstraint);
        }

        ErrorCode SqliteDatabase::ValidateForeignKeyReferencesForTouchedTables(
            const JournalTransaction& tx,
            bool reverseRenameResolution) const
        {
            std::vector<std::wstring> touchedTableNames;
            CollectTouchedTableNames(tx, &touchedTableNames, reverseRenameResolution);

            for (const std::wstring& tableName : touchedTableNames)
            {
                const auto* referenceEntries = GetForeignKeyReferenceEntries(tableName);
                if (referenceEntries == nullptr || referenceEntries->empty())
                {
                    continue;
                }

                for (const auto& entry : *referenceEntries)
                {
                    auto sourceIt = tables_.find(entry.sourceTableName);
                    if (sourceIt == tables_.end())
                    {
                        continue;
                    }

                    auto* sourceTable = static_cast<SqliteTable*>(sourceIt->second.Get());
                    if (sourceTable == nullptr)
                    {
                        continue;
                    }

                    const ErrorCode validateRc =
                        ValidateForeignKeyConstraint(sourceTable, entry.constraint, std::shared_ptr<SqliteRecordData>{});
                    if (Failed(validateRc))
                    {
                        return validateRc;
                    }
                }
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::ValidateRequiredValuesForCommit() const
        {
            for (const auto& [_, tableRef] : tables_)
            {
                auto* table = static_cast<SqliteTable*>(tableRef.Get());
                if (table == nullptr)
                {
                    continue;
                }

                SCSchemaPtr schema;
                const ErrorCode schemaRc = table->GetSchema(schema);
                if (Failed(schemaRc) || !schema)
                {
                    return Failed(schemaRc) ? schemaRc : SC_E_FAIL;
                }

                std::int32_t columnCount = 0;
                const ErrorCode countRc = schema->GetColumnCount(&columnCount);
                if (Failed(countRc))
                {
                    return countRc;
                }

                for (std::int32_t columnIndex = 0; columnIndex < columnCount; ++columnIndex)
                {
                    SCColumnDef column;
                    const ErrorCode columnRc = schema->GetColumn(columnIndex, &column);
                    if (Failed(columnRc))
                    {
                        return columnRc;
                    }

                    if (column.nullable || !column.defaultValue.IsNull())
                    {
                        continue;
                    }

                    for (const auto& [recordId, data] : table->Records())
                    {
                        (void)recordId;
                        if (data == nullptr || data->state == RecordState::Deleted)
                        {
                            continue;
                        }

                        const auto valueIt = data->values.find(column.name);
                        if (valueIt == data->values.end() || valueIt->second.IsNull())
                        {
                            return SC_E_SCHEMA_VIOLATION;
                        }
                    }
                }
            }

            return SC_OK;
        }

        SqliteDatabase::JournalLookup SqliteDatabase::LookupRecordJournalState(const std::wstring& tableName,
                                                                               RecordId recordId) const
        {
            JournalLookup lookup;
            for (const auto& entry : activeJournal_.entries)
            {
                if (!JournalEntryMatchesCurrentTableName(entry, tableName) ||
                    entry.recordId != recordId)
                {
                    continue;
                }
                if (entry.op == JournalOp::CreateRecord)
                {
                    lookup.createdInActiveEdit = true;
                } else if (entry.op == JournalOp::DeleteRecord)
                {
                    lookup.deletedInActiveEdit = true;
                }
            }
            return lookup;
        }

        bool SqliteDatabase::HasForeignKeyReferencesToTable(const std::wstring& tableName) const
        {
            const auto* entries = GetForeignKeyReferenceEntries(tableName);
            return entries != nullptr && !entries->empty();
        }

        ErrorCode SqliteDatabase::ValidateUniqueAndPrimaryKeyConstraintsForTouchedTables(
            const JournalTransaction& tx,
            bool reverseRenameResolution) const
        {
            std::vector<std::wstring> touchedTableNames;
            CollectTouchedTableNames(tx, &touchedTableNames, reverseRenameResolution);

            for (const std::wstring& tableName : touchedTableNames)
            {
                auto tableIt = tables_.find(tableName);
                if (tableIt == tables_.end())
                {
                    continue;
                }

                auto* table = static_cast<SqliteTable*>(tableIt->second.Get());
                if (table == nullptr)
                {
                    continue;
                }

                const ErrorCode validateRc = ValidateTableConstraints(table, std::shared_ptr<SqliteRecordData>{});
                if (Failed(validateRc))
                {
                    return validateRc;
                }
            }

            return SC_OK;
        }

} // namespace StableCore::Storage