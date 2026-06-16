#include "SCSchemaEdit.h"

#include <algorithm>
#include <cwctype>

#include "SCErrors.h"

namespace StableCore::Storage
{
    namespace
    {
        std::wstring ToUpperCopy(const std::wstring& text)
        {
            std::wstring normalized = text;
            std::transform(normalized.begin(), normalized.end(), normalized.begin(), [](wchar_t ch) {
                return static_cast<wchar_t>(std::towupper(ch));
            });
            return normalized;
        }

        bool ContainsName(const std::vector<std::wstring>& names, const std::wstring& candidate)
        {
            const std::wstring normalizedCandidate = ToUpperCopy(candidate);
            return std::any_of(names.begin(), names.end(), [&normalizedCandidate](const std::wstring& existing) {
                return ToUpperCopy(existing) == normalizedCandidate;
            });
        }

        template <typename TName>
        bool EqualsNormalized(const TName& left, const std::wstring& right)
        {
            return ToUpperCopy(left) == ToUpperCopy(right);
        }

        ErrorCode ValidateColumnDefShape(const SCColumnDef& def)
        {
            if (def.name.empty())
            {
                return SC_E_INVALIDARG;
            }

            if (def.columnKind == ColumnKind::Relation)
            {
                if (def.referenceTable.empty())
                {
                    return SC_E_SCHEMA_VIOLATION;
                }

                if (def.referenceStorageColumn.empty() &&
                    !def.referenceDisplayColumn.empty())
                {
                    return SC_E_SCHEMA_VIOLATION;
                }

                if (def.referenceStorageColumn.empty())
                {
                    if (def.valueKind != ValueKind::RecordId)
                    {
                        return SC_E_SCHEMA_VIOLATION;
                    }
                    if (!def.defaultValue.IsNull() &&
                        def.defaultValue.GetKind() != ValueKind::RecordId)
                    {
                        return SC_E_SCHEMA_VIOLATION;
                    }
                } else
                {
                    if (!def.defaultValue.IsNull() &&
                        def.defaultValue.GetKind() != def.valueKind)
                    {
                        return SC_E_SCHEMA_VIOLATION;
                    }
                }
            } else
            {
                if (!def.referenceTable.empty())
                {
                    return SC_E_SCHEMA_VIOLATION;
                }
                if (!def.referenceStorageColumn.empty() ||
                    !def.referenceDisplayColumn.empty())
                {
                    return SC_E_SCHEMA_VIOLATION;
                }
                if (!def.defaultValue.IsNull() && def.defaultValue.GetKind() != def.valueKind)
                {
                    return SC_E_SCHEMA_VIOLATION;
                }
            }

            return SC_OK;
        }

        ErrorCode ValidateUniqueColumnNames(const std::vector<SCColumnDef>& columns)
        {
            std::vector<std::wstring> seenNames;
            seenNames.reserve(columns.size());

            for (const SCColumnDef& column : columns)
            {
                if (column.name.empty())
                {
                    return SC_E_INVALIDARG;
                }
                const ErrorCode shapeRc = ValidateColumnDefShape(column);
                if (Failed(shapeRc))
                {
                    return shapeRc;
                }
                if (ContainsName(seenNames, column.name))
                {
                    return SC_E_VALIDATION_FAILED;
                }
                seenNames.push_back(column.name);
            }

            return SC_OK;
        }

        ErrorCode ValidateUnsupportedSchemaMembers(const SCTableSchemaSnapshot& schema)
        {
            (void)schema;
            return SC_OK;
        }

        ErrorCode ValidateUniqueConstraintNames(const std::vector<SCConstraintDef>& constraints)
        {
            std::vector<std::wstring> seenNames;
            seenNames.reserve(constraints.size());

            for (const SCConstraintDef& constraint : constraints)
            {
                if (constraint.name.empty())
                {
                    return SC_E_INVALIDARG;
                }
                if (ContainsName(seenNames, constraint.name))
                {
                    return SC_E_VALIDATION_FAILED;
                }
                seenNames.push_back(constraint.name);
            }

            return SC_OK;
        }

        ErrorCode ValidateUniqueIndexNames(const std::vector<SCIndexDef>& indexes)
        {
            std::vector<std::wstring> seenNames;
            seenNames.reserve(indexes.size());

            for (const SCIndexDef& index : indexes)
            {
                if (index.name.empty())
                {
                    return SC_E_INVALIDARG;
                }
                if (ContainsName(seenNames, index.name))
                {
                    return SC_E_VALIDATION_FAILED;
                }
                seenNames.push_back(index.name);
            }

            return SC_OK;
        }

        bool SchemaHasColumn(const std::vector<SCColumnDef>& columns, const std::wstring& candidate)
        {
            return std::any_of(columns.begin(), columns.end(), [&candidate](const SCColumnDef& column) {
                return ToUpperCopy(column.name) == ToUpperCopy(candidate);
            });
        }

        ErrorCode ValidateConstraintDefs(ISCDatabase* database, const SCTableSchemaSnapshot& schema)
        {
            for (const SCConstraintDef& constraint : schema.constraints)
            {
                if (constraint.columns.empty())
                {
                    return SC_E_INVALIDARG;
                }

                for (const std::wstring& columnName : constraint.columns)
                {
                    if (!SchemaHasColumn(schema.columns, columnName))
                    {
                        return SC_E_COLUMN_NOT_FOUND;
                    }
                }

                if (constraint.kind == SCConstraintKind::ForeignKey)
                {
                    if (database == nullptr)
                    {
                        return SC_E_POINTER;
                    }
                    if (constraint.referencedTable.empty())
                    {
                        return SC_E_SCHEMA_VIOLATION;
                    }

                    const bool selfReference =
                        ToUpperCopy(constraint.referencedTable) == ToUpperCopy(schema.table.name);

                    SCTablePtr referencedTable;
                    if (!selfReference)
                    {
                        const ErrorCode tableRc =
                            database->GetTable(constraint.referencedTable.c_str(), referencedTable);
                        if (Failed(tableRc))
                        {
                            return tableRc;
                        }
                    }

                    if (!constraint.referencedColumns.empty())
                    {
                        SCSchemaPtr referencedSchema;
                        if (!selfReference)
                        {
                            const ErrorCode schemaRc = referencedTable->GetSchema(referencedSchema);
                            if (Failed(schemaRc))
                            {
                                return schemaRc;
                            }
                        }

                        for (const std::wstring& columnName : constraint.referencedColumns)
                        {
                            if (selfReference)
                            {
                                if (!SchemaHasColumn(schema.columns, columnName))
                                {
                                    return SC_E_COLUMN_NOT_FOUND;
                                }
                            } else
                            {
                                SCColumnDef ignored;
                                const ErrorCode columnRc = referencedSchema->FindColumn(columnName.c_str(), &ignored);
                                if (Failed(columnRc))
                                {
                                    return columnRc;
                                }
                            }
                        }
                    }
                }
            }

            return SC_OK;
        }

        ErrorCode ValidateRelationColumnMetadata(const ISCDatabase* database, const SCColumnDef& def)
        {
            if (database == nullptr)
            {
                return SC_E_POINTER;
            }

            if (def.columnKind != ColumnKind::Relation || def.referenceTable.empty())
            {
                return SC_OK;
            }

            auto* mutableDatabase = const_cast<ISCDatabase*>(database);

            SCTablePtr targetTable;
            const ErrorCode tableRc = mutableDatabase->GetTable(def.referenceTable.c_str(), targetTable);
            if (Failed(tableRc))
            {
                return tableRc;
            }

            if (def.referenceStorageColumn.empty())
            {
                return SC_OK;
            }

            SCSchemaPtr targetSchema;
            const ErrorCode schemaRc = targetTable->GetSchema(targetSchema);
            if (Failed(schemaRc))
            {
                return schemaRc;
            }

            SCColumnDef storageColumn;
            const ErrorCode storageRc = targetSchema->FindColumn(def.referenceStorageColumn.c_str(), &storageColumn);
            if (Failed(storageRc))
            {
                return storageRc;
            }

            if (storageColumn.valueKind != def.valueKind || storageColumn.nullable)
            {
                return SC_E_SCHEMA_VIOLATION;
            }

            bool uniqueStorageColumn = false;
            std::int32_t constraintCount = 0;
            const ErrorCode constraintCountRc = targetSchema->GetConstraintCount(&constraintCount);
            if (Failed(constraintCountRc))
            {
                return constraintCountRc;
            }

            for (std::int32_t index = 0; index < constraintCount; ++index)
            {
                SCConstraintDef constraint;
                const ErrorCode constraintRc = targetSchema->GetConstraint(index, &constraint);
                if (Failed(constraintRc))
                {
                    return constraintRc;
                }

                if (constraint.columns.size() != 1)
                {
                    continue;
                }
                if (constraint.kind != SCConstraintKind::PrimaryKey &&
                    constraint.kind != SCConstraintKind::Unique)
                {
                    continue;
                }
                if (EqualsNormalized(constraint.columns.front(), def.referenceStorageColumn))
                {
                    uniqueStorageColumn = true;
                    break;
                }
            }

            if (!uniqueStorageColumn)
            {
                return SC_E_SCHEMA_VIOLATION;
            }

            if (!def.referenceDisplayColumn.empty())
            {
                SCColumnDef displayColumn;
                const ErrorCode displayRc =
                    targetSchema->FindColumn(def.referenceDisplayColumn.c_str(), &displayColumn);
                if (Failed(displayRc))
                {
                    return displayRc;
                }
            }

            return SC_OK;
        }

        ErrorCode ValidateIndexDefs(const SCTableSchemaSnapshot& schema)
        {
            for (const SCIndexDef& index : schema.indexes)
            {
                if (index.columns.empty())
                {
                    return SC_E_INVALIDARG;
                }

                for (const SCIndexColumnDef& column : index.columns)
                {
                    if (!SchemaHasColumn(schema.columns, column.columnName))
                    {
                        return SC_E_COLUMN_NOT_FOUND;
                    }
                }
            }

            return SC_OK;
        }

        ErrorCode ValidateCreateSchema(ISCDatabase* database, const SCTableSchemaSnapshot& schema)
        {
            if (schema.table.name.empty())
            {
                return SC_E_INVALIDARG;
            }
            if (schema.columns.empty())
            {
                return SC_E_INVALIDARG;
            }

            ErrorCode rc = ValidateUnsupportedSchemaMembers(schema);
            if (Failed(rc))
            {
                return rc;
            }

            rc = ValidateUniqueColumnNames(schema.columns);
            if (Failed(rc))
            {
                return rc;
            }

            rc = ValidateUniqueConstraintNames(schema.constraints);
            if (Failed(rc))
            {
                return rc;
            }

            rc = ValidateUniqueIndexNames(schema.indexes);
            if (Failed(rc))
            {
                return rc;
            }

            rc = ValidateConstraintDefs(database, schema);
            if (Failed(rc))
            {
                return rc;
            }

            for (const SCColumnDef& column : schema.columns)
            {
                rc = ValidateRelationColumnMetadata(database, column);
                if (Failed(rc))
                {
                    return rc;
                }
            }

            return ValidateIndexDefs(schema);
        }

        ErrorCode ValidatePatchNameConflicts(const SCTableSchemaPatch& patch)
        {
            std::vector<std::wstring> removedNames;
            removedNames.reserve(patch.removeColumns.size());
            for (const std::wstring& columnName : patch.removeColumns)
            {
                if (columnName.empty())
                {
                    return SC_E_INVALIDARG;
                }
                if (ContainsName(removedNames, columnName))
                {
                    return SC_E_VALIDATION_FAILED;
                }
                removedNames.push_back(columnName);
            }

            std::vector<std::wstring> addedNames;
            addedNames.reserve(patch.addColumns.size());
            for (const SCColumnDef& column : patch.addColumns)
            {
                if (column.name.empty())
                {
                    return SC_E_INVALIDARG;
                }
                if (ContainsName(removedNames, column.name) || ContainsName(addedNames, column.name))
                {
                    return SC_E_VALIDATION_FAILED;
                }
                addedNames.push_back(column.name);
            }

            std::vector<std::wstring> updatedNames;
            updatedNames.reserve(patch.updateColumns.size());
            for (const SCColumnDef& column : patch.updateColumns)
            {
                if (column.name.empty())
                {
                    return SC_E_INVALIDARG;
                }
                if (ContainsName(removedNames, column.name) || ContainsName(addedNames, column.name) ||
                    ContainsName(updatedNames, column.name))
                {
                    return SC_E_VALIDATION_FAILED;
                }
                updatedNames.push_back(column.name);
            }

            std::vector<std::wstring> removedConstraintNames;
            removedConstraintNames.reserve(patch.removeConstraints.size());
            for (const std::wstring& constraintName : patch.removeConstraints)
            {
                if (constraintName.empty())
                {
                    return SC_E_INVALIDARG;
                }
                if (ContainsName(removedConstraintNames, constraintName))
                {
                    return SC_E_VALIDATION_FAILED;
                }
                removedConstraintNames.push_back(constraintName);
            }

            std::vector<std::wstring> addedConstraintNames;
            addedConstraintNames.reserve(patch.addConstraints.size());
            for (const SCConstraintDef& constraint : patch.addConstraints)
            {
                if (constraint.name.empty())
                {
                    return SC_E_INVALIDARG;
                }
                if (ContainsName(removedConstraintNames, constraint.name) ||
                    ContainsName(addedConstraintNames, constraint.name))
                {
                    return SC_E_VALIDATION_FAILED;
                }
                addedConstraintNames.push_back(constraint.name);
            }

            std::vector<std::wstring> removedIndexNames;
            removedIndexNames.reserve(patch.removeIndexes.size());
            for (const std::wstring& indexName : patch.removeIndexes)
            {
                if (indexName.empty())
                {
                    return SC_E_INVALIDARG;
                }
                if (ContainsName(removedIndexNames, indexName))
                {
                    return SC_E_VALIDATION_FAILED;
                }
                removedIndexNames.push_back(indexName);
            }

            std::vector<std::wstring> addedIndexNames;
            addedIndexNames.reserve(patch.addIndexes.size());
            for (const SCIndexDef& index : patch.addIndexes)
            {
                if (index.name.empty())
                {
                    return SC_E_INVALIDARG;
                }
                if (ContainsName(removedIndexNames, index.name) || ContainsName(addedIndexNames, index.name))
                {
                    return SC_E_VALIDATION_FAILED;
                }
                addedIndexNames.push_back(index.name);
            }

            return SC_OK;
        }

        ErrorCode BuildProjectedSchemaSnapshot(const SCTableSchemaSnapshot& current,
                                               const SCTableSchemaPatch& patch,
                                               SCTableSchemaSnapshot* outProjected)
        {
            if (outProjected == nullptr)
            {
                return SC_E_POINTER;
            }

            SCTableSchemaSnapshot projected = current;

            for (const std::wstring& indexName : patch.removeIndexes)
            {
                const auto it = std::find_if(
                    projected.indexes.begin(), projected.indexes.end(), [&indexName](const SCIndexDef& index) {
                        return EqualsNormalized(index.name, indexName);
                    });
                if (it == projected.indexes.end())
                {
                    return SC_E_INDEX_NOT_FOUND;
                }
                projected.indexes.erase(it);
            }

            for (const std::wstring& constraintName : patch.removeConstraints)
            {
                const auto it = std::find_if(projected.constraints.begin(),
                                             projected.constraints.end(),
                                             [&constraintName](const SCConstraintDef& constraint) {
                                                 return EqualsNormalized(constraint.name, constraintName);
                                             });
                if (it == projected.constraints.end())
                {
                    return SC_E_CONSTRAINT_NOT_FOUND;
                }
                projected.constraints.erase(it);
            }

            for (const std::wstring& columnName : patch.removeColumns)
            {
                const auto it = std::find_if(
                    projected.columns.begin(), projected.columns.end(), [&columnName](const SCColumnDef& column) {
                        return EqualsNormalized(column.name, columnName);
                    });
                if (it == projected.columns.end())
                {
                    return SC_E_COLUMN_NOT_FOUND;
                }
                projected.columns.erase(it);
            }

            for (const SCColumnDef& column : patch.updateColumns)
            {
                const auto it = std::find_if(
                    projected.columns.begin(), projected.columns.end(), [&column](const SCColumnDef& existing) {
                        return EqualsNormalized(existing.name, column.name);
                    });
                if (it == projected.columns.end())
                {
                    return SC_E_COLUMN_NOT_FOUND;
                }
                *it = column;
            }

            for (const SCColumnDef& column : patch.addColumns)
            {
                const auto it = std::find_if(
                    projected.columns.begin(), projected.columns.end(), [&column](const SCColumnDef& existing) {
                        return EqualsNormalized(existing.name, column.name);
                    });
                if (it != projected.columns.end())
                {
                    return SC_E_COLUMN_EXISTS;
                }
                projected.columns.push_back(column);
            }

            for (const SCConstraintDef& constraint : patch.addConstraints)
            {
                const auto it = std::find_if(projected.constraints.begin(),
                                             projected.constraints.end(),
                                             [&constraint](const SCConstraintDef& existing) {
                                                 return EqualsNormalized(existing.name, constraint.name);
                                             });
                if (it != projected.constraints.end())
                {
                    return SC_E_CONSTRAINT_VIOLATION;
                }
                projected.constraints.push_back(constraint);
            }

            for (const SCIndexDef& index : patch.addIndexes)
            {
                const auto it = std::find_if(
                    projected.indexes.begin(), projected.indexes.end(), [&index](const SCIndexDef& existing) {
                        return EqualsNormalized(existing.name, index.name);
                    });
                if (it != projected.indexes.end())
                {
                    return SC_E_CONSTRAINT_VIOLATION;
                }
                projected.indexes.push_back(index);
            }

            *outProjected = std::move(projected);
            return SC_OK;
        }

        ErrorCode ValidatePatch(ISCDatabase* database,
                                const SCTableSchemaPatch& patch,
                                const SCTableSchemaSnapshot& current)
        {
            if (patch.tableName.empty())
            {
                return SC_E_INVALIDARG;
            }

            ErrorCode rc = ValidateUniqueColumnNames(patch.addColumns);
            if (Failed(rc))
            {
                return rc;
            }

            rc = ValidateUniqueColumnNames(patch.updateColumns);
            if (Failed(rc))
            {
                return rc;
            }

            rc = ValidateUniqueConstraintNames(patch.addConstraints);
            if (Failed(rc))
            {
                return rc;
            }

            rc = ValidateUniqueIndexNames(patch.addIndexes);
            if (Failed(rc))
            {
                return rc;
            }

            rc = ValidatePatchNameConflicts(patch);
            if (Failed(rc))
            {
                return rc;
            }

            SCTableSchemaSnapshot projected;
            rc = BuildProjectedSchemaSnapshot(current, patch, &projected);
            if (Failed(rc))
            {
                return rc;
            }

            rc = ValidateCreateSchema(database, projected);
            return rc;
        }

        void ResetResult(SCSchemaEditResult* outResult)
        {
            if (outResult != nullptr)
            {
                *outResult = SCSchemaEditResult{};
            }
        }

        ErrorCode RollbackActiveEdit(ISCDatabase* database, const SCEditPtr& edit, ErrorCode originalError)
        {
            if (database == nullptr || !edit)
            {
                return originalError;
            }

            const ErrorCode rollbackRc = database->Rollback(edit.Get());
            return Failed(rollbackRc) ? rollbackRc : originalError;
        }

        ErrorCode CleanupCreatedTableAfterFailure(ISCDatabase* database,
                                                  const SCEditPtr& edit,
                                                  const std::wstring& tableName,
                                                  ErrorCode originalError)
        {
            ErrorCode rc = RollbackActiveEdit(database, edit, originalError);
            if (Failed(rc))
            {
                return rc;
            }

            if (database == nullptr || tableName.empty())
            {
                return originalError;
            }

            const ErrorCode deleteRc = database->DeleteTable(tableName.c_str());
            return Failed(deleteRc) ? deleteRc : originalError;
        }
    }  // namespace

    ErrorCode CreateTableFromSchema(ISCDatabase* database,
                                    const SCTableSchemaSnapshot& schema,
                                    SCSchemaEditResult* outResult)
    {
        ResetResult(outResult);

        if (database == nullptr)
        {
            return SC_E_POINTER;
        }

        ErrorCode rc = ValidateCreateSchema(database, schema);
        if (Failed(rc))
        {
            return rc;
        }

        SCEditPtr edit;
        rc = database->BeginEdit(L"CreateTableFromSchema", edit);
        if (Failed(rc))
        {
            return rc;
        }

        SCTablePtr table;
        bool tableCreated = false;
        rc = database->CreateTable(schema.table.name.c_str(), table);
        if (Failed(rc))
        {
            return RollbackActiveEdit(database, edit, rc);
        }
        tableCreated = true;

        SCSchemaPtr tableSchema;
        rc = table->GetSchema(tableSchema);
        if (Failed(rc))
        {
            return tableCreated ? CleanupCreatedTableAfterFailure(database, edit, schema.table.name, rc)
                                : RollbackActiveEdit(database, edit, rc);
        }

        for (const SCColumnDef& column : schema.columns)
        {
            rc = tableSchema->AddColumn(column);
            if (Failed(rc))
            {
                return tableCreated ? CleanupCreatedTableAfterFailure(database, edit, schema.table.name, rc)
                                    : RollbackActiveEdit(database, edit, rc);
            }
        }

        for (const SCConstraintDef& constraint : schema.constraints)
        {
            rc = tableSchema->AddConstraint(constraint);
            if (Failed(rc))
            {
                return tableCreated ? CleanupCreatedTableAfterFailure(database, edit, schema.table.name, rc)
                                    : RollbackActiveEdit(database, edit, rc);
            }
        }

        for (const SCIndexDef& index : schema.indexes)
        {
            rc = tableSchema->AddIndex(index);
            if (Failed(rc))
            {
                return tableCreated ? CleanupCreatedTableAfterFailure(database, edit, schema.table.name, rc)
                                    : RollbackActiveEdit(database, edit, rc);
            }
        }

        rc = database->Commit(edit.Get());
        if (Failed(rc))
        {
            return tableCreated ? CleanupCreatedTableAfterFailure(database, edit, schema.table.name, rc)
                                : RollbackActiveEdit(database, edit, rc);
        }

        if (outResult != nullptr)
        {
            outResult->applied = true;
            outResult->committedVersion = database->GetCurrentVersion();
            outResult->tableName = schema.table.name;
            outResult->addedColumns.reserve(schema.columns.size());
            for (const SCColumnDef& column : schema.columns)
            {
                outResult->addedColumns.push_back(column.name);
            }
        }

        return SC_OK;
    }

    ErrorCode ApplyTableSchemaPatch(ISCDatabase* database,
                                    const SCTableSchemaPatch& patch,
                                    SCSchemaEditResult* outResult)
    {
        ResetResult(outResult);

        if (database == nullptr)
        {
            return SC_E_POINTER;
        }

        if (patch.tableName.empty())
        {
            return SC_E_INVALIDARG;
        }

        SCTablePtr table;
        ErrorCode rc = database->GetTable(patch.tableName.c_str(), table);
        if (Failed(rc))
        {
            return rc;
        }

        SCSchemaPtr schema;
        rc = table->GetSchema(schema);
        if (Failed(rc))
        {
            return rc;
        }

        SCTableSchemaSnapshot current;
        rc = schema->GetSchemaSnapshot(&current);
        if (Failed(rc))
        {
            return rc;
        }

        rc = ValidatePatch(database, patch, current);
        if (Failed(rc))
        {
            return rc;
        }

        SCEditPtr edit;
        rc = database->BeginEdit(L"ApplyTableSchemaPatch", edit);
        if (Failed(rc))
        {
            return rc;
        }

        for (const std::wstring& indexName : patch.removeIndexes)
        {
            rc = schema->RemoveIndex(indexName.c_str());
            if (Failed(rc))
            {
                return RollbackActiveEdit(database, edit, rc);
            }
        }

        for (const std::wstring& constraintName : patch.removeConstraints)
        {
            rc = schema->RemoveConstraint(constraintName.c_str());
            if (Failed(rc))
            {
                return RollbackActiveEdit(database, edit, rc);
            }
        }

        for (const std::wstring& columnName : patch.removeColumns)
        {
            rc = schema->RemoveColumn(columnName.c_str());
            if (Failed(rc))
            {
                return RollbackActiveEdit(database, edit, rc);
            }
        }

        for (const SCColumnDef& column : patch.updateColumns)
        {
            rc = schema->UpdateColumn(column);
            if (Failed(rc))
            {
                return RollbackActiveEdit(database, edit, rc);
            }
        }

        for (const SCColumnDef& column : patch.addColumns)
        {
            rc = schema->AddColumn(column);
            if (Failed(rc))
            {
                return RollbackActiveEdit(database, edit, rc);
            }
        }

        for (const SCConstraintDef& constraint : patch.addConstraints)
        {
            rc = schema->AddConstraint(constraint);
            if (Failed(rc))
            {
                return RollbackActiveEdit(database, edit, rc);
            }
        }

        for (const SCIndexDef& index : patch.addIndexes)
        {
            rc = schema->AddIndex(index);
            if (Failed(rc))
            {
                return RollbackActiveEdit(database, edit, rc);
            }
        }

        rc = database->Commit(edit.Get());
        if (Failed(rc))
        {
            return RollbackActiveEdit(database, edit, rc);
        }

        if (outResult != nullptr)
        {
            outResult->applied = true;
            outResult->committedVersion = database->GetCurrentVersion();
            outResult->tableName = patch.tableName;
            outResult->removedColumns = patch.removeColumns;
            outResult->removedConstraints = patch.removeConstraints;
            outResult->removedIndexes = patch.removeIndexes;
            outResult->updatedColumns.reserve(patch.updateColumns.size());
            outResult->addedColumns.reserve(patch.addColumns.size());
            outResult->addedConstraints.reserve(patch.addConstraints.size());
            outResult->addedIndexes.reserve(patch.addIndexes.size());
            for (const SCColumnDef& column : patch.updateColumns)
            {
                outResult->updatedColumns.push_back(column.name);
            }
            for (const SCColumnDef& column : patch.addColumns)
            {
                outResult->addedColumns.push_back(column.name);
            }
            for (const SCConstraintDef& constraint : patch.addConstraints)
            {
                outResult->addedConstraints.push_back(constraint.name);
            }
            for (const SCIndexDef& index : patch.addIndexes)
            {
                outResult->addedIndexes.push_back(index.name);
            }
        }

        return SC_OK;
    }

    ErrorCode ValidateRelationColumnDef(const ISCDatabase* database, const SCColumnDef& def)
    {
        return ValidateRelationColumnMetadata(database, def);
    }

}  // namespace StableCore::Storage
