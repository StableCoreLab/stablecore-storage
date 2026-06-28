#include "Sqlite/SqliteImplCore.h"
#include "Sqlite/SqliteImplDatabase.h"

namespace StableCore::Storage
{
        std::int64_t SqliteDatabase::FindQueryIndexStorageRowId(std::int64_t tableRowId,
                                                                const std::wstring& indexName) const
        {
            const auto it = queryIndexRowIdsByTableAndName_.find(BuildQueryIndexStorageKey(tableRowId, indexName));
            return it == queryIndexRowIdsByTableAndName_.end() ? -1 : it->second;
        }

        ErrorCode SqliteDatabase::EncodeIndexColumnValue(const SCValue& value,
                                                         ValueKind valueKind,
                                                         bool descending,
                                                         std::vector<std::uint8_t>* outBytes) const
        {
            if (outBytes == nullptr)
            {
                return SC_E_POINTER;
            }

            outBytes->clear();
            auto appendByte = [outBytes](std::uint8_t byte) { outBytes->push_back(byte); };
            auto appendU64 = [outBytes](std::uint64_t value) {
                for (int shift = 56; shift >= 0; shift -= 8)
                {
                    outBytes->push_back(static_cast<std::uint8_t>((value >> shift) & 0xffu));
                }
            };

            if (value.IsNull())
            {
                appendByte(0x00);
            } else
            {
                appendByte(0x01);
                switch (valueKind)
                {
                    case ValueKind::Int64:
                    case ValueKind::RecordId: {
                        std::int64_t typed = 0;
                        ErrorCode rc = SC_OK;
                        if (valueKind == ValueKind::RecordId)
                        {
                            RecordId recordId = 0;
                            rc = value.AsRecordId(&recordId);
                            typed = static_cast<std::int64_t>(recordId);
                        } else
                        {
                            rc = value.AsInt64(&typed);
                        }
                        if (Failed(rc))
                        {
                            return rc;
                        }
                        appendByte(valueKind == ValueKind::RecordId ? 0x14 : 0x11);
                        appendU64(static_cast<std::uint64_t>(typed) ^ 0x8000000000000000ull);
                        break;
                    }
                    case ValueKind::Double: {
                        double typed = 0.0;
                        const ErrorCode rc = value.AsDouble(&typed);
                        if (Failed(rc))
                        {
                            return rc;
                        }
                        appendByte(0x12);
                        std::uint64_t bits = 0;
                        static_assert(sizeof(bits) == sizeof(typed));
                        std::memcpy(&bits, &typed, sizeof(bits));
                        bits = (bits & 0x8000000000000000ull) != 0 ? ~bits : (bits ^ 0x8000000000000000ull);
                        appendU64(bits);
                        break;
                    }
                    case ValueKind::Bool: {
                        bool typed = false;
                        const ErrorCode rc = value.AsBool(&typed);
                        if (Failed(rc))
                        {
                            return rc;
                        }
                        appendByte(0x13);
                        appendByte(typed ? 1 : 0);
                        break;
                    }
                    case ValueKind::String:
                    case ValueKind::Enum: {
                        std::wstring text;
                        const ErrorCode rc = value.AsStringCopy(&text);
                        if (Failed(rc))
                        {
                            return rc;
                        }
                        appendByte(valueKind == ValueKind::Enum ? 0x16 : 0x15);
                        const std::string utf8 = SCCommon::ToUtf8(text);
                        for (unsigned char byte : utf8)
                        {
                            if (byte == 0x00u)
                            {
                                outBytes->push_back(0x00u);
                                outBytes->push_back(0xffu);
                            } else
                            {
                                outBytes->push_back(static_cast<std::uint8_t>(byte));
                            }
                        }
                        outBytes->push_back(0x00u);
                        outBytes->push_back(0x00u);
                        break;
                    }
                    case ValueKind::Binary: {
                        const auto* bytes = value.TryGet<std::vector<std::uint8_t>>();
                        if (bytes == nullptr)
                        {
                            return SC_E_TYPE_MISMATCH;
                        }
                        appendByte(0x17);
                        appendU64(static_cast<std::uint64_t>(bytes->size()));
                        outBytes->insert(outBytes->end(), bytes->begin(), bytes->end());
                        break;
                    }
                    case ValueKind::Null:
                    default:
                        appendByte(0x10);
                        break;
                }
            }

            if (descending)
            {
                for (std::uint8_t& byte : *outBytes)
                {
                    byte = static_cast<std::uint8_t>(0xffu - byte);
                }
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::EncodeIndexColumnPrefixValue(const SCValue& value,
                                                               ValueKind valueKind,
                                                               bool descending,
                                                               std::vector<std::uint8_t>* outBytes) const
        {
            if (outBytes == nullptr)
            {
                return SC_E_POINTER;
            }
            outBytes->clear();

            if (valueKind != ValueKind::String && valueKind != ValueKind::Enum)
            {
                return EncodeIndexColumnValue(value, valueKind, descending, outBytes);
            }

            std::wstring text;
            const ErrorCode rc = value.AsStringCopy(&text);
            if (Failed(rc))
            {
                return rc;
            }

            // Keep the prefix shape aligned with EncodeIndexColumnValue() for
            // non-null strings, but omit the trailing terminator so the caller
            // can build a prefix range over the composite full key.
            outBytes->push_back(0x01u);
            outBytes->push_back(valueKind == ValueKind::Enum ? 0x16 : 0x15);
            const std::string utf8 = SCCommon::ToUtf8(text);
            for (unsigned char byte : utf8)
            {
                if (byte == 0x00u)
                {
                    outBytes->push_back(0x00u);
                    outBytes->push_back(0xffu);
                } else
                {
                    outBytes->push_back(static_cast<std::uint8_t>(byte));
                }
            }

            if (descending)
            {
                for (std::uint8_t& byte : *outBytes)
                {
                    byte = static_cast<std::uint8_t>(0xffu - byte);
                }
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::BuildCompositeIndexKey(const SqliteSchema* schema,
                                                         const SCIndexDef& indexDef,
                                                         const SqliteRecordData& recordData,
                                                         CompositeIndexEncodedKey* outKey) const
        {
            if (schema == nullptr || outKey == nullptr)
            {
                return schema == nullptr ? SC_E_POINTER : SC_E_POINTER;
            }

            outKey->prefix1.clear();
            outKey->prefix2.clear();
            outKey->prefix3.clear();
            outKey->full.clear();

            std::vector<std::uint8_t> encoded;
            for (std::size_t index = 0; index < indexDef.columns.size(); ++index)
            {
                const SCIndexColumnDef& indexColumn = indexDef.columns[index];
                const SCColumnDef* columnDef = schema->FindColumnDef(indexColumn.columnName);
                if (columnDef == nullptr)
                {
                    return SC_E_COLUMN_NOT_FOUND;
                }

                const auto valueIt = recordData.values.find(indexColumn.columnName);
                const SCValue& value = valueIt != recordData.values.end() ? valueIt->second : columnDef->defaultValue;
                const ErrorCode encodeRc =
                    EncodeIndexColumnValue(value, columnDef->valueKind, indexColumn.descending, &encoded);
                if (Failed(encodeRc))
                {
                    return encodeRc;
                }

                outKey->full.push_back(0xfeu);
                outKey->full.insert(outKey->full.end(), encoded.begin(), encoded.end());
                if (index == 0)
                {
                    outKey->prefix1 = outKey->full;
                } else if (index == 1)
                {
                    outKey->prefix2 = outKey->full;
                } else if (index == 2)
                {
                    outKey->prefix3 = outKey->full;
                }
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::BuildCompositeLookupBounds(const SqliteSchema* schema,
                                                             const QueryPlan& analyzedPlan,
                                                             CompositeIndexLookupBounds* outBounds) const
        {
            if (schema == nullptr || outBounds == nullptr || !analyzedPlan.matchedIndex.has_value())
            {
                return SC_E_POINTER;
            }

            *outBounds = CompositeIndexLookupBounds{};

            const QueryMatchedIndexSpec& matchedIndex = analyzedPlan.matchedIndex.value();
            const SCIndexDef* indexDef = schema->FindIndexDef(matchedIndex.indexName);
            if (indexDef == nullptr)
            {
                return SC_E_INDEX_NOT_FOUND;
            }

            auto appendExactSegment =
                [this](const SCValue& value,
                       const SCColumnDef& column,
                       bool descending,
                       std::vector<std::uint8_t>* outBytes) -> ErrorCode {
                std::vector<std::uint8_t> encoded;
                const ErrorCode encodeRc = EncodeIndexColumnValue(value, column.valueKind, descending, &encoded);
                if (Failed(encodeRc))
                {
                    return encodeRc;
                }
                outBytes->push_back(0xfeu);
                outBytes->insert(outBytes->end(), encoded.begin(), encoded.end());
                return SC_OK;
            };

            for (std::size_t index = 0; index < matchedIndex.equalityPrefixLength; ++index)
            {
                const std::wstring& columnName = matchedIndex.keyColumns[index];
                const auto it =
                    std::find_if(analyzedPlan.pushdown.pushdownConditions.begin(),
                                 analyzedPlan.pushdown.pushdownConditions.end(),
                                 [&columnName](const QueryCondition& condition) {
                                     return condition.fieldName == columnName &&
                                            IsEqualityIndexOperator(condition.op) && condition.values.size() == 1;
                                 });
                if (it == analyzedPlan.pushdown.pushdownConditions.end())
                {
                    break;
                }

                SCColumnDef column;
                const ErrorCode columnRc = const_cast<SqliteSchema*>(schema)->FindColumn(columnName.c_str(), &column);
                if (Failed(columnRc))
                {
                    return columnRc;
                }
                const ErrorCode appendRc = appendExactSegment(
                    it->values.front(), column, (*indexDef).columns[index].descending, &outBounds->equalityPrefixKey);
                if (Failed(appendRc))
                {
                    return appendRc;
                }
                outBounds->equalityPrefixLength = static_cast<std::uint32_t>(index + 1);
            }

            if (!matchedIndex.hasRangeCondition)
            {
                outBounds->exactMatch = true;
                outBounds->exactPrefixLength = matchedIndex.equalityPrefixLength;
                outBounds->exactPrefixKey = outBounds->equalityPrefixKey;
                return SC_OK;
            }

            const std::size_t rangeColumnIndex = matchedIndex.equalityPrefixLength;
            if (rangeColumnIndex >= matchedIndex.keyColumns.size())
            {
                return SC_E_INVALIDARG;
            }

            const std::wstring& rangeColumnName = matchedIndex.keyColumns[rangeColumnIndex];
            const auto rangeIt =
                std::find_if(analyzedPlan.pushdown.pushdownConditions.begin(),
                             analyzedPlan.pushdown.pushdownConditions.end(),
                             [&rangeColumnName](const QueryCondition& condition) {
                                 return condition.fieldName == rangeColumnName && IsRangeIndexOperator(condition.op);
                             });
            if (rangeIt == analyzedPlan.pushdown.pushdownConditions.end())
            {
                return SC_E_INVALIDARG;
            }

            SCColumnDef rangeColumn;
            const ErrorCode rangeColumnRc =
                const_cast<SqliteSchema*>(schema)->FindColumn(rangeColumnName.c_str(), &rangeColumn);
            if (Failed(rangeColumnRc))
            {
                return rangeColumnRc;
            }

            const bool descending = (*indexDef).columns[rangeColumnIndex].descending;
            std::vector<std::uint8_t> baseKey = outBounds->equalityPrefixKey;
            switch (rangeIt->op)
            {
                case QueryConditionOperator::GreaterThan:
                case QueryConditionOperator::GreaterThanOrEqual: {
                    if (rangeIt->values.size() != 1)
                    {
                        return SC_E_INVALIDARG;
                    }
                    std::vector<std::uint8_t> bound = baseKey;
                    const ErrorCode appendRc =
                        appendExactSegment(rangeIt->values.front(), rangeColumn, descending, &bound);
                    if (Failed(appendRc))
                    {
                        return appendRc;
                    }
                    if (descending)
                    {
                        outBounds->hasUpperBound = true;
                        outBounds->includeUpperBound = rangeIt->op == QueryConditionOperator::GreaterThanOrEqual;
                        outBounds->upperBound = std::move(bound);
                        if (outBounds->includeUpperBound)
                        {
                            outBounds->upperBound.push_back(0xffu);
                        }
                    } else
                    {
                        outBounds->hasLowerBound = true;
                        outBounds->includeLowerBound = rangeIt->op == QueryConditionOperator::GreaterThanOrEqual;
                        outBounds->lowerBound = std::move(bound);
                        if (!outBounds->includeLowerBound)
                        {
                            outBounds->lowerBound.push_back(0xffu);
                        }
                    }
                    break;
                }
                case QueryConditionOperator::LessThan:
                case QueryConditionOperator::LessThanOrEqual: {
                    if (rangeIt->values.size() != 1)
                    {
                        return SC_E_INVALIDARG;
                    }
                    std::vector<std::uint8_t> bound = baseKey;
                    const ErrorCode appendRc =
                        appendExactSegment(rangeIt->values.front(), rangeColumn, descending, &bound);
                    if (Failed(appendRc))
                    {
                        return appendRc;
                    }
                    if (descending)
                    {
                        outBounds->hasLowerBound = true;
                        outBounds->includeLowerBound = rangeIt->op == QueryConditionOperator::LessThanOrEqual;
                        outBounds->lowerBound = std::move(bound);
                        if (!outBounds->includeLowerBound)
                        {
                            outBounds->lowerBound.push_back(0xffu);
                        }
                    } else
                    {
                        outBounds->hasUpperBound = true;
                        outBounds->includeUpperBound = rangeIt->op == QueryConditionOperator::LessThanOrEqual;
                        outBounds->upperBound = std::move(bound);
                        if (outBounds->includeUpperBound)
                        {
                            outBounds->upperBound.push_back(0xffu);
                        }
                    }
                    break;
                }
                case QueryConditionOperator::Between: {
                    if (rangeIt->values.size() != 2)
                    {
                        return SC_E_INVALIDARG;
                    }
                    std::vector<std::uint8_t> firstBound = baseKey;
                    {
                        const ErrorCode appendRc =
                            appendExactSegment(rangeIt->values[0], rangeColumn, descending, &firstBound);
                        if (Failed(appendRc))
                        {
                            return appendRc;
                        }
                    }
                    std::vector<std::uint8_t> secondBound = baseKey;
                    {
                        const ErrorCode appendRc =
                            appendExactSegment(rangeIt->values[1], rangeColumn, descending, &secondBound);
                        if (Failed(appendRc))
                        {
                            return appendRc;
                        }
                    }
                    if (descending)
                    {
                        outBounds->hasLowerBound = true;
                        outBounds->includeLowerBound = true;
                        outBounds->lowerBound = std::move(secondBound);
                        outBounds->hasUpperBound = true;
                        outBounds->includeUpperBound = true;
                        outBounds->upperBound = std::move(firstBound);
                        outBounds->upperBound.push_back(0xffu);
                    } else
                    {
                        outBounds->hasLowerBound = true;
                        outBounds->includeLowerBound = true;
                        outBounds->lowerBound = std::move(firstBound);
                        outBounds->hasUpperBound = true;
                        outBounds->includeUpperBound = true;
                        outBounds->upperBound = std::move(secondBound);
                        outBounds->upperBound.push_back(0xffu);
                    }
                    break;
                }
                case QueryConditionOperator::StartsWith: {
                    if (rangeIt->values.size() != 1)
                    {
                        return SC_E_INVALIDARG;
                    }
                    if (rangeColumn.valueKind != ValueKind::String && rangeColumn.valueKind != ValueKind::Enum)
                    {
                        return SC_E_NOTIMPL;
                    }

                    std::vector<std::uint8_t> encodedPrefix;
                    const ErrorCode encodePrefixRc = EncodeIndexColumnPrefixValue(
                        rangeIt->values.front(), rangeColumn.valueKind, descending, &encodedPrefix);
                    if (Failed(encodePrefixRc))
                    {
                        return encodePrefixRc;
                    }

                    outBounds->hasLowerBound = true;
                    outBounds->includeLowerBound = true;
                    outBounds->lowerBound = baseKey;
                    outBounds->lowerBound.push_back(0xfeu);
                    outBounds->lowerBound.insert(
                        outBounds->lowerBound.end(), encodedPrefix.begin(), encodedPrefix.end());

                    outBounds->hasUpperBound = true;
                    outBounds->includeUpperBound = false;
                    outBounds->upperBound = outBounds->lowerBound;
                    outBounds->upperBound.push_back(0xffu);
                    break;
                }
                default:
                    return SC_E_NOTIMPL;
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::BuildCompositeEqualityPrefixBounds(const SqliteSchema* schema,
                                                                     const QueryPlan& analyzedPlan,
                                                                     CompositeIndexLookupBounds* outBounds) const
        {
            if (schema == nullptr || outBounds == nullptr || !analyzedPlan.matchedIndex.has_value())
            {
                return SC_E_POINTER;
            }

            *outBounds = CompositeIndexLookupBounds{};

            const QueryMatchedIndexSpec& matchedIndex = analyzedPlan.matchedIndex.value();
            const SCIndexDef* indexDef = schema->FindIndexDef(matchedIndex.indexName);
            if (indexDef == nullptr)
            {
                return SC_E_INDEX_NOT_FOUND;
            }

            auto appendExactSegment =
                [this](const SCValue& value,
                       const SCColumnDef& column,
                       bool descending,
                       std::vector<std::uint8_t>* outBytes) -> ErrorCode {
                std::vector<std::uint8_t> encoded;
                const ErrorCode encodeRc = EncodeIndexColumnValue(value, column.valueKind, descending, &encoded);
                if (Failed(encodeRc))
                {
                    return encodeRc;
                }
                outBytes->push_back(0xfeu);
                outBytes->insert(outBytes->end(), encoded.begin(), encoded.end());
                return SC_OK;
            };

            for (std::size_t index = 0; index < matchedIndex.equalityPrefixLength; ++index)
            {
                const std::wstring& columnName = matchedIndex.keyColumns[index];
                const auto it =
                    std::find_if(analyzedPlan.pushdown.pushdownConditions.begin(),
                                 analyzedPlan.pushdown.pushdownConditions.end(),
                                 [&columnName](const QueryCondition& condition) {
                                     return condition.fieldName == columnName &&
                                            IsEqualityIndexOperator(condition.op) && condition.values.size() == 1;
                                 });
                if (it == analyzedPlan.pushdown.pushdownConditions.end())
                {
                    break;
                }

                SCColumnDef column;
                const ErrorCode columnRc = const_cast<SqliteSchema*>(schema)->FindColumn(columnName.c_str(), &column);
                if (Failed(columnRc))
                {
                    return columnRc;
                }
                const ErrorCode appendRc = appendExactSegment(
                    it->values.front(), column, indexDef->columns[index].descending, &outBounds->equalityPrefixKey);
                if (Failed(appendRc))
                {
                    return appendRc;
                }
                outBounds->equalityPrefixLength = static_cast<std::uint32_t>(index + 1);
            }

            if (outBounds->equalityPrefixLength == 0)
            {
                return SC_E_INVALIDARG;
            }

            outBounds->exactMatch = true;
            outBounds->exactPrefixLength = outBounds->equalityPrefixLength;
            outBounds->exactPrefixKey = outBounds->equalityPrefixKey;
            return SC_OK;
        }

        ErrorCode SqliteDatabase::CheckCompositeQueryIndexConsistency(QueryIndexCheckResult* outResult) const
        {
            if (outResult == nullptr)
            {
                return SC_E_POINTER;
            }

            outResult->state = QueryIndexHealthState::Healthy;
            outResult->indexVersion = static_cast<std::int32_t>(version_);
            outResult->expectedVersion = static_cast<std::int32_t>(version_);
            outResult->message = L"query-index-current";

            if (activeEdit_ && !activeJournal_.entries.empty())
            {
                outResult->state = QueryIndexHealthState::OutOfDate;
                outResult->message = L"query-index-rebuild-required";
                return SC_OK;
            }

            auto countEntriesForIndex = [this](std::int64_t schemaIndexRowId, std::int64_t* outCount) -> ErrorCode {
                if (outCount == nullptr)
                {
                    return SC_E_POINTER;
                }

                sqlite3_stmt* stmt = nullptr;
                const int prepareRc = sqlite3_prepare_v2(
                    db_.Raw(),
                    "SELECT COUNT(*) FROM query_index_entries WHERE schema_index_id = ? AND alive_flag = 1;",
                    -1,
                    &stmt,
                    nullptr);
                if (prepareRc != SQLITE_OK)
                {
                    if (stmt != nullptr)
                    {
                        sqlite3_finalize(stmt);
                    }
                    return MapSqliteError(prepareRc);
                }

                sqlite3_bind_int64(stmt, 1, schemaIndexRowId);
                const int stepRc = sqlite3_step(stmt);
                if (stepRc != SQLITE_ROW)
                {
                    sqlite3_finalize(stmt);
                    return MapSqliteError(stepRc);
                }

                *outCount = sqlite3_column_int64(stmt, 0);
                sqlite3_finalize(stmt);
                return SC_OK;
            };

            auto hasExactEntry = [this](std::int64_t schemaIndexRowId,
                                        RecordId recordId,
                                        const CompositeIndexEncodedKey& key,
                                        bool* outFound) -> ErrorCode {
                if (outFound == nullptr)
                {
                    return SC_E_POINTER;
                }

                sqlite3_stmt* stmt = nullptr;
                const int prepareRc = sqlite3_prepare_v2(
                    db_.Raw(),
                    "SELECT 1 FROM query_index_entries WHERE schema_index_id = ? AND record_id = ? AND alive_flag = 1 "
                    "AND full_key = ? LIMIT 1;",
                    -1,
                    &stmt,
                    nullptr);
                if (prepareRc != SQLITE_OK)
                {
                    if (stmt != nullptr)
                    {
                        sqlite3_finalize(stmt);
                    }
                    return MapSqliteError(prepareRc);
                }

                sqlite3_bind_int64(stmt, 1, schemaIndexRowId);
                sqlite3_bind_int64(stmt, 2, recordId);
                sqlite3_bind_blob(stmt,
                                  3,
                                  key.full.data(),
                                  static_cast<int>(key.full.size()),
                                  SQLITE_TRANSIENT);
                const int stepRc = sqlite3_step(stmt);
                *outFound = stepRc == SQLITE_ROW;
                sqlite3_finalize(stmt);
                return (stepRc == SQLITE_ROW || stepRc == SQLITE_DONE) ? SC_OK : MapSqliteError(stepRc);
            };

            for (const auto& [_, tableRef] : tables_)
            {
                auto* table = static_cast<SqliteTable*>(tableRef.Get());
                if (table == nullptr)
                {
                    continue;
                }

                SCTableSchemaSnapshot snapshot;
                const ErrorCode snapshotRc = table->Schema()->GetSchemaSnapshot(&snapshot);
                if (Failed(snapshotRc))
                {
                    return snapshotRc;
                }

                for (const SCIndexDef& indexDef : snapshot.indexes)
                {
                    if (!IsCompositeIndexExplicit(indexDef))
                    {
                        continue;
                    }

                    const std::int64_t schemaIndexRowId = FindQueryIndexStorageRowId(table->TableRowId(), indexDef.name);
                    if (schemaIndexRowId <= 0)
                    {
                        outResult->state = QueryIndexHealthState::Missing;
                        outResult->message = L"query-index-definition-missing:" + indexDef.name;
                        return SC_OK;
                    }

                    std::int64_t expectedAliveCount = 0;
                    for (const auto& [recordId, recordData] : table->Records())
                    {
                        (void)recordId;
                        if (recordData == nullptr || recordData->state != RecordState::Alive)
                        {
                            continue;
                        }

                        ++expectedAliveCount;
                        CompositeIndexEncodedKey key;
                        const ErrorCode keyRc = BuildCompositeIndexKey(table->Schema(), indexDef, *recordData, &key);
                        if (Failed(keyRc))
                        {
                            outResult->state = QueryIndexHealthState::Corrupted;
                            outResult->message = L"query-index-key-build-failed:" + indexDef.name;
                            return SC_OK;
                        }

                        bool found = false;
                        const ErrorCode entryRc = hasExactEntry(schemaIndexRowId, recordData->id, key, &found);
                        if (Failed(entryRc))
                        {
                            return entryRc;
                        }
                        if (!found)
                        {
                            outResult->state = QueryIndexHealthState::Corrupted;
                            outResult->message = L"query-index-entry-missing:" + indexDef.name;
                            return SC_OK;
                        }
                    }

                    std::int64_t actualAliveCount = 0;
                    const ErrorCode countRc = countEntriesForIndex(schemaIndexRowId, &actualAliveCount);
                    if (Failed(countRc))
                    {
                        return countRc;
                    }

                    if (actualAliveCount != expectedAliveCount)
                    {
                        outResult->state = QueryIndexHealthState::Corrupted;
                        outResult->message = L"query-index-entry-count-mismatch:" + indexDef.name;
                        return SC_OK;
                    }
                }
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::PersistQueryIndexDefinition(SqliteSchema* schema,
                                                              const SCIndexDef& def,
                                                              std::int64_t schemaIndexRowId,
                                                              bool updateCache)
        {
            if (schema == nullptr || schemaIndexRowId <= 0)
            {
                return SC_E_INVALIDARG;
            }
            if (!IsCompositeIndexExplicit(def))
            {
                return SC_OK;
            }

            SqliteStmt stmt = db_.Prepare(
                "INSERT OR REPLACE INTO query_indexes(schema_index_id, table_id, index_name, key_arity) "
                "VALUES(?, ?, ?, ?);");
            stmt.BindInt64(1, schemaIndexRowId);
            stmt.BindInt64(2, schema->TableRowId());
            stmt.BindText(3, def.name);
            stmt.BindInt64(4, static_cast<std::int64_t>(def.columns.size()));
            const ErrorCode rc = stmt.Step();
            if (Failed(rc))
            {
                return rc;
            }

            if (updateCache)
            {
                queryIndexRowIdsByTableAndName_[BuildQueryIndexStorageKey(schema->TableRowId(), def.name)] =
                    schemaIndexRowId;
            }
            return SC_OK;
        }

        ErrorCode SqliteDatabase::RemoveQueryIndexDefinition(SqliteSchema* schema, const wchar_t* name, bool updateCache)
        {
            if (schema == nullptr || name == nullptr)
            {
                return SC_E_POINTER;
            }

            {
                SqliteStmt deleteEntries = db_.Prepare("DELETE FROM query_index_entries WHERE schema_index_id = ?;");
                deleteEntries.BindInt64(1, schema->FindIndexRowId(name));
                const ErrorCode rc = deleteEntries.Step();
                if (Failed(rc))
                {
                    return rc;
                }
            }

            SqliteStmt deleteDef = db_.Prepare("DELETE FROM query_indexes WHERE schema_index_id = ?;");
            deleteDef.BindInt64(1, schema->FindIndexRowId(name));
            const ErrorCode rc = deleteDef.Step();
            if (Failed(rc))
            {
                return rc;
            }

            if (updateCache)
            {
                queryIndexRowIdsByTableAndName_.erase(BuildQueryIndexStorageKey(schema->TableRowId(), name));
            }
            return SC_OK;
        }

        ErrorCode SqliteDatabase::RebuildCompositeIndexEntriesForRecord(SqliteTable* table,
                                                                        const SCIndexDef& indexDef,
                                                                        const SqliteRecordData& recordData,
                                                                        std::int64_t schemaIndexRowId)
        {
            if (table == nullptr || !IsCompositeIndexExplicit(indexDef))
            {
                return table == nullptr ? SC_E_POINTER : SC_OK;
            }

            const std::int64_t effectiveSchemaIndexRowId =
                schemaIndexRowId > 0 ? schemaIndexRowId : table->Schema()->FindIndexRowId(indexDef.name.c_str());
            if (effectiveSchemaIndexRowId <= 0)
            {
                return SC_E_INDEX_NOT_FOUND;
            }

            {
                SqliteStmt deleteStmt =
                    db_.Prepare("DELETE FROM query_index_entries WHERE schema_index_id = ? AND record_id = ?;");
                deleteStmt.BindInt64(1, effectiveSchemaIndexRowId);
                deleteStmt.BindInt64(2, recordData.id);
                const ErrorCode deleteRc = deleteStmt.Step();
                if (Failed(deleteRc))
                {
                    return deleteRc;
                }
            }

            if (recordData.state != RecordState::Alive)
            {
                return SC_OK;
            }

            CompositeIndexEncodedKey key;
            const ErrorCode keyRc = BuildCompositeIndexKey(table->Schema(), indexDef, recordData, &key);
            if (Failed(keyRc))
            {
                return keyRc;
            }

            SqliteStmt insertStmt = db_.Prepare(
                "INSERT INTO query_index_entries(schema_index_id, record_id, alive_flag, key_prefix_1, "
                "key_prefix_2, key_prefix_3, full_key) VALUES(?, ?, 1, ?, ?, ?, ?);");
            insertStmt.BindInt64(1, effectiveSchemaIndexRowId);
            insertStmt.BindInt64(2, recordData.id);
            key.prefix1.empty() ? insertStmt.BindNull(3) : insertStmt.BindBlob(3, key.prefix1);
            key.prefix2.empty() ? insertStmt.BindNull(4) : insertStmt.BindBlob(4, key.prefix2);
            key.prefix3.empty() ? insertStmt.BindNull(5) : insertStmt.BindBlob(5, key.prefix3);
            insertStmt.BindBlob(6, key.full);
            return insertStmt.Step();
        }

        ErrorCode SqliteDatabase::RebuildCompositeIndexEntriesForRecord(SqliteTable* table,
                                                                        const SqliteRecordData& recordData)
        {
            if (table == nullptr)
            {
                return SC_E_POINTER;
            }

            SCTableSchemaSnapshot snapshot;
            const ErrorCode snapshotRc = table->Schema()->GetSchemaSnapshot(&snapshot);
            if (Failed(snapshotRc))
            {
                return snapshotRc;
            }

            for (const SCIndexDef& indexDef : snapshot.indexes)
            {
                const ErrorCode rc = RebuildCompositeIndexEntriesForRecord(table, indexDef, recordData);
                if (Failed(rc))
                {
                    return rc;
                }
            }
            return SC_OK;
        }

        ErrorCode SqliteDatabase::RebuildCompositeIndexEntriesForTable(SqliteTable* table,
                                                                       const SCIndexDef& indexDef,
                                                                       std::int64_t schemaIndexRowId)
        {
            if (table == nullptr)
            {
                return SC_E_POINTER;
            }
            if (!IsCompositeIndexExplicit(indexDef))
            {
                return SC_OK;
            }

            const std::int64_t effectiveSchemaIndexRowId =
                schemaIndexRowId > 0 ? schemaIndexRowId : table->Schema()->FindIndexRowId(indexDef.name.c_str());
            if (effectiveSchemaIndexRowId <= 0)
            {
                return SC_E_INDEX_NOT_FOUND;
            }

            {
                SqliteStmt deleteStmt = db_.Prepare("DELETE FROM query_index_entries WHERE schema_index_id = ?;");
                deleteStmt.BindInt64(1, effectiveSchemaIndexRowId);
                const ErrorCode deleteRc = deleteStmt.Step();
                if (Failed(deleteRc))
                {
                    return deleteRc;
                }
            }

            for (const auto& [_, recordData] : table->Records())
            {
                if (!recordData)
                {
                    continue;
                }
                const ErrorCode rc =
                    RebuildCompositeIndexEntriesForRecord(table, indexDef, *recordData, effectiveSchemaIndexRowId);
                if (Failed(rc))
                {
                    return rc;
                }
            }
            return SC_OK;
        }

        ErrorCode SqliteDatabase::RebuildCompositeIndexesForTable(SqliteTable* table)
        {
            if (table == nullptr)
            {
                return SC_E_POINTER;
            }

            SCTableSchemaSnapshot snapshot;
            const ErrorCode snapshotRc = table->Schema()->GetSchemaSnapshot(&snapshot);
            if (Failed(snapshotRc))
            {
                return snapshotRc;
            }

            for (const SCIndexDef& indexDef : snapshot.indexes)
            {
                const std::int64_t schemaIndexRowId = table->Schema()->FindIndexRowId(indexDef.name.c_str());
                const ErrorCode persistRc = PersistQueryIndexDefinition(table->Schema(), indexDef, schemaIndexRowId);
                if (Failed(persistRc))
                {
                    return persistRc;
                }

                const ErrorCode rebuildRc = RebuildCompositeIndexEntriesForTable(table, indexDef);
                if (Failed(rebuildRc))
                {
                    return rebuildRc;
                }
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::AnalyzeCompositeIndexPlan(const QueryPlan& inputPlan, QueryPlan* outPlan)
        {
            if (outPlan == nullptr)
            {
                return SC_E_POINTER;
            }

            QueryPlan analyzed = inputPlan;
            analyzed.matchedIndex.reset();
            analyzed.pushdown.pushdownConditions.clear();
            analyzed.pushdown.residualConditions.clear();

            if (inputPlan.target.type != QueryTargetType::Table || inputPlan.conditionGroups.size() != 1 ||
                inputPlan.conditionGroupLogic != QueryLogicOperator::And || inputPlan.conditionGroups.front().logic != QueryLogicOperator::And)
            {
                *outPlan = std::move(analyzed);
                return SC_OK;
            }

            const auto tableIt = tables_.find(inputPlan.target.name);
            if (tableIt == tables_.end())
            {
                return SC_E_TABLE_NOT_FOUND;
            }
            auto* table = static_cast<SqliteTable*>(tableIt->second.Get());
            auto* schema = table->Schema();

            SCTableSchemaSnapshot snapshot;
            const ErrorCode snapshotRc = schema->GetSchemaSnapshot(&snapshot);
            if (Failed(snapshotRc))
            {
                return snapshotRc;
            }

            const QueryConditionGroup& group = inputPlan.conditionGroups.front();
            std::optional<QueryMatchedIndexSpec> bestMatch;
            std::vector<QueryCondition> bestPushdown;
            std::vector<QueryCondition> bestResidual;
            auto isConditionConsumedByPushdown = [](const QueryCondition& condition,
                                                    const std::vector<QueryCondition>& usedPushdown) {
                return std::find_if(usedPushdown.begin(),
                                    usedPushdown.end(),
                                    [&condition](const QueryCondition& used) {
                                        return used.fieldName == condition.fieldName && used.op == condition.op &&
                                               used.values == condition.values;
                                    }) != usedPushdown.end();
            };
            auto isConditionImpliedByPushdown = [](const QueryCondition& condition,
                                                   const std::vector<QueryCondition>& usedPushdown) {
                if (condition.op != QueryConditionOperator::IsNotNull)
                {
                    return false;
                }

                return std::find_if(
                           usedPushdown.begin(),
                           usedPushdown.end(),
                           [&condition](const QueryCondition& used) {
                               return used.fieldName == condition.fieldName &&
                                      used.op == QueryConditionOperator::Equal && used.values.size() == 1 &&
                                      !used.values.front().IsNull();
                           }) != usedPushdown.end();
            };
            auto isBetterCandidate = [](const QueryMatchedIndexSpec& candidate,
                                        const std::vector<QueryCondition>& candidateResidual,
                                        const QueryMatchedIndexSpec& best,
                                        const std::vector<QueryCondition>& bestResidual) {
                if (candidate.matchedPrefixLength != best.matchedPrefixLength)
                {
                    return candidate.matchedPrefixLength > best.matchedPrefixLength;
                }
                if (candidate.equalityPrefixLength != best.equalityPrefixLength)
                {
                    return candidate.equalityPrefixLength > best.equalityPrefixLength;
                }
                if (candidate.exactOrderCovered != best.exactOrderCovered)
                {
                    return candidate.exactOrderCovered && !best.exactOrderCovered;
                }
                if (candidate.orderCovered != best.orderCovered)
                {
                    return candidate.orderCovered && !best.orderCovered;
                }
                if (candidateResidual.size() != bestResidual.size())
                {
                    return candidateResidual.size() < bestResidual.size();
                }
                if (candidate.hasRangeCondition != best.hasRangeCondition)
                {
                    return !candidate.hasRangeCondition && best.hasRangeCondition;
                }
                return candidate.indexName < best.indexName;
            };

            for (const SCIndexDef& indexDef : snapshot.indexes)
            {
                if (!IsCompositeIndexExplicit(indexDef))
                {
                    continue;
                }

                bool hasBinaryColumn = false;
                for (const SCIndexColumnDef& indexColumn : indexDef.columns)
                {
                    const SCColumnDef* columnDef = schema->FindColumnDef(indexColumn.columnName);
                    if (columnDef == nullptr)
                    {
                        return SC_E_COLUMN_NOT_FOUND;
                    }
                    if (columnDef->valueKind == ValueKind::Binary)
                    {
                        hasBinaryColumn = true;
                        break;
                    }
                }
                if (hasBinaryColumn)
                {
                    continue;
                }

                QueryMatchedIndexSpec candidate;
                candidate.indexName = indexDef.name;
                for (const SCIndexColumnDef& column : indexDef.columns)
                {
                    candidate.keyColumns.push_back(column.columnName);
                }

                std::vector<QueryCondition> usedPushdown;
                std::vector<QueryCondition> residual = group.conditions;
                bool encounteredRange = false;
                for (const SCIndexColumnDef& column : indexDef.columns)
                {
                    if (encounteredRange)
                    {
                        break;
                    }

                    const auto equalityIt =
                        std::find_if(group.conditions.begin(), group.conditions.end(), [&column](const QueryCondition& condition) {
                            return condition.fieldName == column.columnName && IsEqualityIndexOperator(condition.op) &&
                                   condition.values.size() == 1;
                        });
                    if (equalityIt != group.conditions.end())
                    {
                        ++candidate.matchedPrefixLength;
                        ++candidate.equalityPrefixLength;
                        usedPushdown.push_back(*equalityIt);
                        continue;
                    }

                    const auto rangeIt =
                        std::find_if(group.conditions.begin(), group.conditions.end(), [&column](const QueryCondition& condition) {
                            return condition.fieldName == column.columnName && IsRangeIndexOperator(condition.op);
                        });
                    if (rangeIt == group.conditions.end())
                    {
                        break;
                    }

                    ++candidate.matchedPrefixLength;
                    candidate.hasRangeCondition = true;
                    usedPushdown.push_back(*rangeIt);
                    encounteredRange = true;
                    break;
                }

                if (candidate.matchedPrefixLength == 0)
                {
                    continue;
                }

                residual.erase(std::remove_if(residual.begin(),
                                              residual.end(),
                                              [&usedPushdown, &isConditionConsumedByPushdown, &isConditionImpliedByPushdown](
                                                  const QueryCondition& condition) {
                                                  return isConditionConsumedByPushdown(condition, usedPushdown) ||
                                                         isConditionImpliedByPushdown(condition, usedPushdown);
                                              }),
                               residual.end());

                candidate.orderCovered = !inputPlan.orderBy.empty() && !candidate.hasRangeCondition;
                candidate.exactOrderCovered = candidate.orderCovered;
                if (candidate.orderCovered)
                {
                    std::size_t orderColumnIndex = candidate.equalityPrefixLength;
                    for (const SortSpec& sort : inputPlan.orderBy)
                    {
                        if (orderColumnIndex >= indexDef.columns.size() ||
                            indexDef.columns[orderColumnIndex].columnName != sort.fieldName)
                        {
                            candidate.orderCovered = false;
                            candidate.exactOrderCovered = false;
                            break;
                        }

                        const bool indexDescending = indexDef.columns[orderColumnIndex].descending;
                        const bool sortDescending = sort.direction == QueryOrderDirection::Descending;
                        if (indexDescending != sortDescending)
                        {
                            candidate.orderCovered = false;
                            candidate.exactOrderCovered = false;
                            break;
                        }
                        ++orderColumnIndex;
                    }
                }

                if (!bestMatch.has_value() || isBetterCandidate(candidate, residual, *bestMatch, bestResidual))
                {
                    bestMatch = candidate;
                    bestPushdown = std::move(usedPushdown);
                    bestResidual = std::move(residual);
                }
            }

            if (bestMatch.has_value())
            {
                analyzed.matchedIndex = bestMatch;
                analyzed.pushdown.pushdownConditions = std::move(bestPushdown);
                analyzed.pushdown.residualConditions = std::move(bestResidual);
                analyzed.pushdownConditionCount =
                    static_cast<std::uint32_t>(analyzed.pushdown.pushdownConditions.size());
                analyzed.fallbackConditionCount =
                    static_cast<std::uint32_t>(analyzed.pushdown.residualConditions.size());
                analyzed.state =
                    analyzed.pushdown.residualConditions.empty() ? QueryPlanState::DirectIndex : QueryPlanState::PartialIndex;
                analyzed.fallbackReason.clear();
            } else if (analyzed.constraints.requireIndex)
            {
                analyzed.state = QueryPlanState::Unsupported;
                analyzed.fallbackReason = L"index-required";
            }

            *outPlan = std::move(analyzed);
            return SC_OK;
        }

        ErrorCode SqliteDatabase::CollectCompositeIndexRecordIds(const QueryPlan& analyzedPlan,
                                                                 std::vector<RecordId>* outRecordIds,
                                                                 std::uint64_t* outScannedEntries)
        {
            if (outRecordIds == nullptr)
            {
                return SC_E_POINTER;
            }
            outRecordIds->clear();
            if (outScannedEntries != nullptr)
            {
                *outScannedEntries = 0;
            }
            if (!analyzedPlan.matchedIndex.has_value())
            {
                return SC_E_INVALIDARG;
            }

            const auto tableIt = tables_.find(analyzedPlan.target.name);
            if (tableIt == tables_.end())
            {
                return SC_E_TABLE_NOT_FOUND;
            }

            auto* table = static_cast<SqliteTable*>(tableIt->second.Get());
            const std::int64_t schemaIndexRowId =
                FindQueryIndexStorageRowId(table->TableRowId(), analyzedPlan.matchedIndex->indexName);
            if (schemaIndexRowId <= 0)
            {
                return SC_E_INDEX_NOT_FOUND;
            }

            std::string sql = "SELECT record_id FROM query_index_entries WHERE schema_index_id = ? AND alive_flag = 1";
            CompositeIndexLookupBounds bounds;
            bool useRangeFullVerificationScan = analyzedPlan.matchedIndex->hasRangeCondition;
            if (!useRangeFullVerificationScan)
            {
                const ErrorCode boundsRc = BuildCompositeLookupBounds(table->Schema(), analyzedPlan, &bounds);
                if (Failed(boundsRc))
                {
                    return boundsRc;
                }
                if (bounds.exactMatch && bounds.exactPrefixLength == 0)
                {
                    return SC_E_INVALIDARG;
                }
                if (!bounds.exactMatch && !bounds.hasLowerBound && !bounds.hasUpperBound)
                {
                    return SC_E_INVALIDARG;
                }

                if (bounds.exactMatch)
                {
                    const char* keyColumn = bounds.exactPrefixLength == 1
                                                ? "key_prefix_1"
                                                : (bounds.exactPrefixLength == 2 ? "key_prefix_2" : "key_prefix_3");
                    sql += " AND ";
                    sql += keyColumn;
                    sql += " = ?";
                } else
                {
                    if (bounds.equalityPrefixLength > 0)
                    {
                        const char* keyColumn = bounds.equalityPrefixLength == 1
                                                    ? "key_prefix_1"
                                                    : (bounds.equalityPrefixLength == 2 ? "key_prefix_2" : "key_prefix_3");
                        sql += " AND ";
                        sql += keyColumn;
                        sql += " = ?";
                    }
                    if (bounds.hasLowerBound)
                    {
                        sql += bounds.includeLowerBound ? " AND full_key >= ?" : " AND full_key > ?";
                    }
                    if (bounds.hasUpperBound)
                    {
                        sql += bounds.includeUpperBound ? " AND full_key <= ?" : " AND full_key < ?";
                    }
                }
            }
            else if (analyzedPlan.matchedIndex->equalityPrefixLength > 0)
            {
                const ErrorCode boundsRc = BuildCompositeEqualityPrefixBounds(table->Schema(), analyzedPlan, &bounds);
                if (Succeeded(boundsRc) && bounds.exactPrefixLength > 0)
                {
                    const char* keyColumn = bounds.exactPrefixLength == 1
                                                ? "key_prefix_1"
                                                : (bounds.exactPrefixLength == 2 ? "key_prefix_2" : "key_prefix_3");
                    sql += " AND ";
                    sql += keyColumn;
                    sql += " = ?";
                }
            }
            sql += " ORDER BY full_key, record_id;";
            SqliteStmt stmt = db_.Prepare(sql.c_str());
            stmt.BindInt64(1, schemaIndexRowId);
            int bindIndex = 2;
            if (!useRangeFullVerificationScan && bounds.exactMatch)
            {
                stmt.BindBlob(bindIndex++, bounds.exactPrefixKey);
            } else if (!useRangeFullVerificationScan)
            {
                if (bounds.equalityPrefixLength > 0)
                {
                    stmt.BindBlob(bindIndex++, bounds.equalityPrefixKey);
                }
                if (bounds.hasLowerBound)
                {
                    stmt.BindBlob(bindIndex++, bounds.lowerBound);
                }
                if (bounds.hasUpperBound)
                {
                    stmt.BindBlob(bindIndex++, bounds.upperBound);
                }
            } else if (bounds.exactPrefixLength > 0)
            {
                stmt.BindBlob(bindIndex++, bounds.exactPrefixKey);
            }
            bool hasRow = false;
            while (stmt.Step(&hasRow) == SC_OK && hasRow)
            {
                outRecordIds->push_back(stmt.ColumnInt64(0));
                if (outScannedEntries != nullptr)
                {
                    ++(*outScannedEntries);
                }
            }
            return SC_OK;
        }

        void SqliteDatabase::RemoveFieldJournalEntries(const std::wstring& tableName, RecordId recordId)
        {
            activeJournal_.entries.erase(
                std::remove_if(activeJournal_.entries.begin(),
                               activeJournal_.entries.end(),
                               [&](const JournalEntry& entry) {
                                   return JournalEntryMatchesCurrentTableName(entry, tableName) &&
                                          entry.recordId == recordId &&
                                          (entry.op == JournalOp::SetValue || entry.op == JournalOp::SetRelation);
                               }),
                activeJournal_.entries.end());
        }

        void SqliteDatabase::RemoveAllJournalEntriesForRecord(const std::wstring& tableName, RecordId recordId)
        {
            activeJournal_.entries.erase(std::remove_if(activeJournal_.entries.begin(),
                                                        activeJournal_.entries.end(),
                                                        [&](const JournalEntry& entry) {
                                                            return JournalEntryMatchesCurrentTableName(entry, tableName) &&
                                                                   entry.recordId == recordId;
                                                        }),
                                         activeJournal_.entries.end());
        }

        ErrorCode SqliteDatabase::WriteValue(SqliteTable* table,
                                             const std::shared_ptr<SqliteRecordData>& data,
                                             const std::wstring& fieldName,
                                             const SCValue& value)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            ClearConstraintViolation();
            const ErrorCode validate = ValidateWrite(table, data, fieldName, value);
            if (Failed(validate))
            {
                return validate;
            }

            const JournalLookup lookup = LookupRecordJournalState(table->Name(), data->id);
            if (lookup.deletedInActiveEdit)
            {
                return SC_E_RECORD_DELETED;
            }

            const SCColumnDef* column = table->Schema()->FindColumnDef(fieldName);
            SCValue storedValue = value;
            if (column != nullptr && column->columnKind == ColumnKind::Relation &&
                !column->referenceStorageColumn.empty() && !value.IsNull())
            {
                const ErrorCode relationRc = ResolveRelationWriteValue(*column, value, &storedValue);
                if (Failed(relationRc))
                {
                    return relationRc;
                }
            }

            SCValue oldValue = column->defaultValue;
            const auto existing = data->values.find(fieldName);
            if (existing != data->values.end())
            {
                oldValue = existing->second;
            }

            if (oldValue == storedValue)
            {
                return SC_OK;
            }

            const ErrorCode constraintRc =
                ValidateTableConstraints(table, data, &fieldName, &storedValue);
            if (Failed(constraintRc))
            {
                return constraintRc;
            }

            const bool hasForeignKeyReferences = HasForeignKeyReferencesToColumn(table->Name(), fieldName);
            if (hasForeignKeyReferences)
            {
                const ErrorCode foreignKeyActionRc =
                    ApplyForeignKeyActionsForColumnUpdate(table, data, fieldName, oldValue, storedValue);
                if (Failed(foreignKeyActionRc))
                {
                    return foreignKeyActionRc;
                }
            }

            data->values[fieldName] = storedValue;
            const JournalOp op = (column != nullptr && column->columnKind == ColumnKind::Relation)
                                     ? JournalOp::SetRelation
                                     : JournalOp::SetValue;
            RecordJournal(table->Name(), data->id, fieldName, oldValue, storedValue, false, false, op);
            MarkReferenceIndexDirty();
            return SC_OK;
        }

        ErrorCode SqliteDatabase::DeleteRecord(SqliteTable* table, const std::shared_ptr<SqliteRecordData>& data)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            ClearConstraintViolation();
            if (!activeEdit_)
            {
                return SC_E_NO_ACTIVE_EDIT;
            }
            if (data->state == RecordState::Deleted)
            {
                return SC_E_RECORD_DELETED;
            }
            if (HasForeignKeyReferencesToTable(table->Name()))
            {
                const ErrorCode foreignKeyActionRc = ApplyForeignKeyActionsForTableDelete(table, data);
                if (Failed(foreignKeyActionRc))
                {
                    return foreignKeyActionRc;
                }
            }
            if (IsRecordReferenced(table->Name(), data->id))
            {
                return SC_E_CONSTRAINT_VIOLATION;
            }

            const JournalLookup lookup = LookupRecordJournalState(table->Name(), data->id);
            if (lookup.deletedInActiveEdit)
            {
                return SC_E_RECORD_DELETED;
            }

            data->state = RecordState::Deleted;
            if (lookup.createdInActiveEdit)
            {
                RemoveAllJournalEntriesForRecord(table->Name(), data->id);
                data->values.clear();
                table->Records().erase(data->id);
                MarkReferenceIndexDirty();
                return SC_OK;
            }

            RemoveFieldJournalEntries(table->Name(), data->id);
            RecordJournal(
                table->Name(), data->id, L"", SCValue::Null(), SCValue::Null(), false, true, JournalOp::DeleteRecord);
            MarkReferenceIndexDirty();
            return SC_OK;
        }

        ErrorCode SqliteDatabase::GetReferencesBySource(const std::wstring& sourceTable,
                                                        RecordId sourceRecordId,
                                                        std::vector<ReferenceRecord>* outRecords) const
        {
            if (outRecords == nullptr)
            {
                return SC_E_POINTER;
            }
            outRecords->clear();

            const auto tableIt = tables_.find(sourceTable);
            if (tableIt == tables_.end())
            {
                return SC_OK;
            }

            auto* table = static_cast<SqliteTable*>(tableIt->second.Get());
            if (table == nullptr)
            {
                return SC_OK;
            }

            const auto recordIt = table->Records().find(sourceRecordId);
            if (recordIt == table->Records().end() || recordIt->second == nullptr ||
                recordIt->second->state == RecordState::Deleted)
            {
                return SC_OK;
            }

            SCSchemaPtr schema;
            const ErrorCode schemaRc = table->GetSchema(schema);
            if (Failed(schemaRc) || !schema)
            {
                return schemaRc;
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
                if (column.columnKind != ColumnKind::Relation)
                {
                    continue;
                }

                SCRecordPtr record;
                if (Failed(table->GetRecord(sourceRecordId, record)) || !record)
                {
                    continue;
                }

                SCValue relationValue;
                if (Failed(record->GetValue(column.name.c_str(), &relationValue)))
                {
                    continue;
                }

                RecordId targetRecordId = 0;
                if (Failed(ResolveRelationTargetRecordId(column, relationValue, &targetRecordId)))
                {
                    continue;
                }

                outRecords->push_back(ReferenceRecord{sourceTable,
                                                      sourceRecordId,
                                                      column.name,
                                                      column.referenceTable,
                                                      targetRecordId,
                                                      version_,
                                                      0,
                                                      std::nullopt});
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::GetReferencesByTarget(const std::wstring& targetTable,
                                                        RecordId targetRecordId,
                                                        std::vector<ReverseReferenceRecord>* outRecords) const
        {
            if (outRecords == nullptr)
            {
                return SC_E_POINTER;
            }
            outRecords->clear();

            for (const auto& [_, tableRef] : tables_)
            {
                auto* table = static_cast<SqliteTable*>(tableRef.Get());
                if (table == nullptr)
                {
                    continue;
                }

                SCSchemaPtr schema;
                if (Failed(table->GetSchema(schema)) || !schema)
                {
                    continue;
                }

                std::int32_t columnCount = 0;
                if (Failed(schema->GetColumnCount(&columnCount)))
                {
                    continue;
                }

                for (std::int32_t columnIndex = 0; columnIndex < columnCount; ++columnIndex)
                {
                    SCColumnDef column;
                    if (Failed(schema->GetColumn(columnIndex, &column)))
                    {
                        continue;
                    }
                    if (column.columnKind != ColumnKind::Relation || column.referenceTable != targetTable)
                    {
                        continue;
                    }

                    for (const auto& [candidateId, candidateData] : table->Records())
                    {
                        if (candidateData == nullptr || candidateData->state == RecordState::Deleted)
                        {
                            continue;
                        }

                        SCRecordPtr record;
                        if (Failed(table->GetRecord(candidateId, record)) || !record)
                        {
                            continue;
                        }

                        SCValue relationValue;
                        if (Failed(record->GetValue(column.name.c_str(), &relationValue)))
                        {
                            continue;
                        }

                        RecordId referencedId = 0;
                        if (Succeeded(ResolveRelationTargetRecordId(column, relationValue, &referencedId)) &&
                            referencedId == targetRecordId)
                        {
                            outRecords->push_back(ReverseReferenceRecord{targetTable,
                                                                         targetRecordId,
                                                                         table->Name(),
                                                                         candidateId,
                                                                         column.name,
                                                                         version_,
                                                                         0,
                                                                         std::nullopt});
                        }
                    }
                }
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::CheckReferenceIndex(ReferenceIndexCheckResult* outResult) const
        {
            if (outResult == nullptr)
            {
                return SC_E_POINTER;
            }

            if (!referenceIndexBuilt_)
            {
                outResult->state = ReferenceIndexHealthState::Missing;
                outResult->message = L"reference-index-not-built";
            } else if (referenceIndexDirty_)
            {
                outResult->state = ReferenceIndexHealthState::OutOfDate;
                outResult->message = L"reference-index-rebuild-required";
            } else
            {
                outResult->state = ReferenceIndexHealthState::Healthy;
                outResult->message = L"reference-index-current";
            }
            outResult->indexVersion = static_cast<std::int32_t>(referenceIndexVersion_);
            outResult->expectedVersion = static_cast<std::int32_t>(version_);
            return SC_OK;
        }

        ErrorCode SqliteDatabase::GetAllReferencesDiagnosticOnly(ReferenceIndex* outIndex) const
        {
            if (outIndex == nullptr)
            {
                return SC_E_POINTER;
            }

            outIndex->records.clear();
            for (const auto& [_, tableRef] : tables_)
            {
                auto* table = static_cast<SqliteTable*>(tableRef.Get());
                if (table == nullptr)
                {
                    continue;
                }

                SCSchemaPtr schema;
                if (Failed(table->GetSchema(schema)) || !schema)
                {
                    continue;
                }

                std::int32_t columnCount = 0;
                if (Failed(schema->GetColumnCount(&columnCount)))
                {
                    continue;
                }

                for (const auto& [recordId, recordData] : table->Records())
                {
                    if (recordData == nullptr || recordData->state == RecordState::Deleted)
                    {
                        continue;
                    }

                    for (std::int32_t columnIndex = 0; columnIndex < columnCount; ++columnIndex)
                    {
                        SCColumnDef column;
                        if (Failed(schema->GetColumn(columnIndex, &column)))
                        {
                            continue;
                        }
                        if (column.columnKind != ColumnKind::Relation)
                        {
                            continue;
                        }

                        SCRecordPtr record;
                        if (Failed(table->GetRecord(recordId, record)) || !record)
                        {
                            continue;
                        }

                        SCValue relationValue;
                        if (Failed(record->GetValue(column.name.c_str(), &relationValue)))
                        {
                            continue;
                        }

                        RecordId targetId = 0;
                        if (Failed(ResolveRelationTargetRecordId(column, relationValue, &targetId)))
                        {
                            continue;
                        }

                        outIndex->records.push_back(ReferenceRecord{table->Name(),
                                                                    recordId,
                                                                    column.name,
                                                                    column.referenceTable,
                                                                    targetId,
                                                                    version_,
                                                                    0,
                                                                    std::nullopt});
                    }
                }
            }

            return SC_OK;
        }

        ErrorCode SqliteDatabase::RebuildReferenceIndexes()
        {
            if (readOnly_)
            {
                return SC_E_READ_ONLY_DATABASE;
            }

            ReferenceIndex index;
            const ErrorCode rc = GetAllReferencesDiagnosticOnly(&index);
            if (Failed(rc))
            {
                return rc;
            }

            referenceIndexBuilt_ = true;
            referenceIndexDirty_ = false;
            referenceIndexVersion_ = version_;
            return SC_OK;
        }

        ErrorCode SqliteDatabase::CommitReferenceDelta(const ReferenceIndex& forwardDelta,
                                                       const ReverseReferenceIndex& reverseDelta)
        {
            if (forwardDelta.records.size() != reverseDelta.records.size())
            {
                return SC_E_INVALIDARG;
            }

            return RebuildReferenceIndexes();
        }

        void SqliteDatabase::MarkReferenceIndexDirty() noexcept
        {
            referenceIndexDirty_ = true;
        }

        void SqliteDatabase::MarkForeignKeyReferenceCacheDirty() noexcept
        {
            foreignKeyReferenceCacheDirty_ = true;
        }

        ErrorCode SqliteDatabase::RefreshForeignKeyReferenceCache() const
        {
            foreignKeyReferenceCacheByTable_.clear();
            foreignKeyReferenceCacheByTableAndColumn_.clear();

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

                std::int32_t constraintCount = 0;
                const ErrorCode countRc = sourceSchema->GetConstraintCount(&constraintCount);
                if (Failed(countRc))
                {
                    return countRc;
                }

                for (std::int32_t index = 0; index < constraintCount; ++index)
                {
                    SCConstraintDef constraint;
                    const ErrorCode constraintRc = sourceSchema->GetConstraint(index, &constraint);
                    if (Failed(constraintRc))
                    {
                        return constraintRc;
                    }

                    if (constraint.kind != SCConstraintKind::ForeignKey || constraint.referencedTable.empty())
                    {
                        continue;
                    }

                    foreignKeyReferenceCacheByTable_[SCCommon::ToUpper(constraint.referencedTable)].push_back(
                        ForeignKeyReferenceEntry{sourceTable->Name(), constraint});

                    for (std::size_t index = 0; index < constraint.columns.size(); ++index)
                    {
                        const std::wstring resolvedReferencedColumn = ResolveForeignKeyReferencedColumn(constraint, index);
                        if (resolvedReferencedColumn.empty())
                        {
                            continue;
                        }
                        foreignKeyReferenceCacheByTableAndColumn_[MakeForeignKeyReferenceCacheKey(constraint.referencedTable,
                                                                                                  resolvedReferencedColumn)]
                            .push_back(ForeignKeyReferenceEntry{sourceTable->Name(), constraint});
                    }
                }
            }

            foreignKeyReferenceCacheDirty_ = false;
            return SC_OK;
        }

        const std::vector<ForeignKeyReferenceEntry>*
        SqliteDatabase::GetForeignKeyReferenceEntries(const std::wstring& tableName) const
        {
            if (foreignKeyReferenceCacheDirty_)
            {
                const ErrorCode rc = RefreshForeignKeyReferenceCache();
                if (Failed(rc))
                {
                    return nullptr;
                }
            }

            const auto it = foreignKeyReferenceCacheByTable_.find(SCCommon::ToUpper(tableName));
            if (it == foreignKeyReferenceCacheByTable_.end())
            {
                return nullptr;
            }
            return &it->second;
        }

        const std::vector<ForeignKeyReferenceEntry>*
        SqliteDatabase::GetForeignKeyReferenceEntries(const std::wstring& tableName, const std::wstring& columnName) const
        {
            if (foreignKeyReferenceCacheDirty_)
            {
                const ErrorCode rc = RefreshForeignKeyReferenceCache();
                if (Failed(rc))
                {
                    return nullptr;
                }
            }

            const auto it = foreignKeyReferenceCacheByTableAndColumn_.find(
                MakeForeignKeyReferenceCacheKey(tableName, columnName));
            if (it == foreignKeyReferenceCacheByTableAndColumn_.end())
            {
                return nullptr;
            }
            return &it->second;
        }

        bool SqliteDatabase::HasForeignKeyReferencesToColumn(const std::wstring& tableName,
                                                             const std::wstring& columnName) const
        {
            const auto* entries = GetForeignKeyReferenceEntries(tableName, columnName);
            if (entries == nullptr)
            {
                return false;
            }
            return !entries->empty();
        }

        ErrorCode SqliteDatabase::SyncLegacyIndexMetadata(SqliteSchema* schema,
                                                          const std::wstring& columnName,
                                                          bool indexed)
        {
            if (schema == nullptr)
            {
                return SC_E_POINTER;
            }

            const std::wstring indexName = schema->LegacyIndexName(columnName);
            SCIndexDef index;
            index.name = indexName;
            index.sourceKind = SCSchemaSourceKind::Explicit;
            index.columns.push_back(SCIndexColumnDef{columnName, false});

            {
                SqliteStmt deleteColumnsStmt = db_.Prepare(
                    "DELETE FROM schema_index_columns WHERE index_id IN ("
                    "SELECT index_id FROM schema_indexes WHERE table_id = ? "
                    "AND name = ?);");
                deleteColumnsStmt.BindInt64(1, schema->TableRowId());
                deleteColumnsStmt.BindText(2, indexName);
                const ErrorCode rc = deleteColumnsStmt.Step();
                if (Failed(rc))
                {
                    return rc;
                }
            }

            {
                SqliteStmt deleteIndexStmt = db_.Prepare(
                    "DELETE FROM schema_indexes WHERE table_id = ? AND "
                    "name = ?;");
                deleteIndexStmt.BindInt64(1, schema->TableRowId());
                deleteIndexStmt.BindText(2, indexName);
                const ErrorCode rc = deleteIndexStmt.Step();
                if (Failed(rc))
                {
                    return rc;
                }
            }

            {
                const ErrorCode removeQueryIndexRc =
                    RemoveQueryIndexDefinition(schema, indexName.c_str(), false);
                if (Failed(removeQueryIndexRc))
                {
                    return removeQueryIndexRc;
                }
            }

            if (!indexed)
            {
                schema->UnloadIndex(indexName.c_str());
                queryIndexRowIdsByTableAndName_.erase(
                    BuildQueryIndexStorageKey(schema->TableRowId(), indexName));
                return SC_OK;
            }

            SqliteStmt insertIndexStmt = db_.Prepare(
                "INSERT INTO schema_indexes(table_id, name, source_kind) "
                "VALUES(?, ?, ?);");
            insertIndexStmt.BindInt64(1, schema->TableRowId());
            insertIndexStmt.BindText(2, indexName);
            insertIndexStmt.BindInt(3, ToSqliteSchemaSourceKind(SCSchemaSourceKind::Explicit));
            const ErrorCode insertRc = insertIndexStmt.Step();
            if (Failed(insertRc))
            {
                return insertRc;
            }

            const std::int64_t indexId = db_.LastInsertRowId();
            SqliteStmt insertColumnStmt = db_.Prepare(
                "INSERT INTO schema_index_columns(index_id, column_ordinal, "
                "column_name, descending_flag) VALUES(?, 0, ?, 0);");
            insertColumnStmt.BindInt64(1, indexId);
            insertColumnStmt.BindText(2, columnName);
            const ErrorCode columnRc = insertColumnStmt.Step();
            if (Failed(columnRc))
            {
                return columnRc;
            }

            const ErrorCode queryIndexRc = PersistQueryIndexDefinition(schema, index, indexId);
            if (Failed(queryIndexRc))
            {
                return queryIndexRc;
            }

            // Keep the legacy indexed-column path aligned with query-index storage so the executor can resolve it.
            const auto tableIt = std::find_if(
                tables_.begin(),
                tables_.end(),
                [tableRowId = schema->TableRowId()](const std::pair<const std::wstring, SCTablePtr>& entry) {
                    const auto* table = static_cast<const SqliteTable*>(entry.second.Get());
                    return table != nullptr && table->TableRowId() == tableRowId;
                });
            if (tableIt != tables_.end())
            {
                auto* table = static_cast<SqliteTable*>(tableIt->second.Get());
                const ErrorCode rebuildRc = RebuildCompositeIndexEntriesForTable(table, index, indexId);
                if (Failed(rebuildRc))
                {
                    return rebuildRc;
                }
            }

            schema->UnloadIndex(indexName.c_str());
            schema->LoadIndex(index, indexId);
            return SC_OK;
        }

        ErrorCode SqliteDatabase::RemoveLegacyPrimaryKeyMetadata(SqliteSchema* schema, const std::wstring& columnName)
        {
            if (schema == nullptr)
            {
                return SC_E_POINTER;
            }

            if (!SCCommon::EqualsIgnoreCase(columnName, L"Id"))
            {
                return SC_OK;
            }

            const std::wstring pkName = schema->LegacyPrimaryKeyName();

            {
                SqliteStmt deleteColumnsStmt = db_.Prepare(
                    "DELETE FROM schema_constraint_columns WHERE constraint_id "
                    "IN (SELECT constraint_id FROM schema_constraints WHERE "
                    "table_id = ? AND kind = ? AND name = ?);");
                deleteColumnsStmt.BindInt64(1, schema->TableRowId());
                deleteColumnsStmt.BindInt(2, ToSqliteConstraintKind(SCConstraintKind::PrimaryKey));
                deleteColumnsStmt.BindText(3, pkName);
                const ErrorCode rc = deleteColumnsStmt.Step();
                if (Failed(rc))
                {
                    return rc;
                }
            }

            SqliteStmt deleteConstraintStmt = db_.Prepare(
                "DELETE FROM schema_constraints WHERE table_id = ? AND kind = ? "
                "AND name = ?;");
            deleteConstraintStmt.BindInt64(1, schema->TableRowId());
            deleteConstraintStmt.BindInt(2, ToSqliteConstraintKind(SCConstraintKind::PrimaryKey));
            deleteConstraintStmt.BindText(3, pkName);
            const ErrorCode deleteRc = deleteConstraintStmt.Step();
            if (Failed(deleteRc))
            {
                return deleteRc;
            }

            schema->RemovePrimaryKeyIfColumnMatches(columnName);
            return SC_OK;
        }

        void SqliteDatabase::RefreshReferenceIndexState()
        {
            const ErrorCode rc = RebuildReferenceIndexes();
            if (Failed(rc))
            {
                referenceIndexBuilt_ = false;
                referenceIndexVersion_ = 0;
            }
        }

        bool SqliteDatabase::IsRecordReferenced(const std::wstring& tableName, RecordId recordId) const
        {
            ReferenceIndexCheckResult check;
            if (Succeeded(CheckReferenceIndex(&check)) && check.state == ReferenceIndexHealthState::Healthy)
            {
                std::vector<ReverseReferenceRecord> refs;
                if (Succeeded(GetReferencesByTarget(tableName, recordId, &refs)))
                {
                    return !refs.empty();
                }
            }

            for (const auto& [_, tableRef] : tables_)
            {
                auto* table = static_cast<SqliteTable*>(tableRef.Get());
                if (table == nullptr)
                {
                    continue;
                }

                SCSchemaPtr schema;
                if (Failed(table->GetSchema(schema)) || !schema)
                {
                    continue;
                }

                std::int32_t columnCount = 0;
                if (Failed(schema->GetColumnCount(&columnCount)))
                {
                    continue;
                }

                for (std::int32_t columnIndex = 0; columnIndex < columnCount; ++columnIndex)
                {
                    SCColumnDef column;
                    if (Failed(schema->GetColumn(columnIndex, &column)))
                    {
                        continue;
                    }
                    if (column.columnKind != ColumnKind::Relation || column.referenceTable != tableName)
                    {
                        continue;
                    }

                    SCValue targetStoredValue;
                    if (Failed(ResolveRelationStoredValue(column, recordId, &targetStoredValue)))
                    {
                        continue;
                    }

                    for (const auto& [candidateId, candidateData] : table->Records())
                    {
                        if (candidateData == nullptr || candidateData->state == RecordState::Deleted)
                        {
                            continue;
                        }

                        SCRecordPtr record;
                        if (Failed(table->GetRecord(candidateId, record)) || !record)
                        {
                            continue;
                        }

                        SCValue relationValue;
                        if (Failed(record->GetValue(column.name.c_str(), &relationValue)))
                        {
                            continue;
                        }

                        if (relationValue == targetStoredValue)
                        {
                            return true;
                        }
                    }
                }
            }

            return false;
        }

        void SqliteDatabase::RecordCreate(SqliteTable* table, const std::shared_ptr<SqliteRecordData>& data)
        {
            RecordJournal(
                table->Name(), data->id, L"", SCValue::Null(), SCValue::Null(), true, false, JournalOp::CreateRecord);
        }

        ErrorCode SqliteDatabase::PersistSchemaAddColumn(SqliteSchema* schema,
                                                         const SCColumnDef& def,
                                                         std::int64_t rowId)
        {
            if (schema == nullptr)
            {
                return SC_E_POINTER;
            }

            SqliteStmt stmt = db_.Prepare(
                "INSERT INTO schema_columns("
                " rowid, table_id, column_name, display_name, value_kind, "
                "column_kind, nullable_flag, editable_flag,"
                " user_defined_flag, indexed_flag, "
                "participates_in_calc_flag, unit, reference_table,"
                " reference_storage_column, reference_display_column,"
                " default_kind, default_int64, default_double, "
                "default_bool, default_text, default_blob)"
                " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                "?, ?, ?);");
            if (rowId > 0)
            {
                stmt.BindInt64(1, rowId);
            } else
            {
                stmt.BindNull(1);
            }
            stmt.BindInt64(2, schema->TableRowId());
            stmt.BindText(3, def.name);
            BindColumnDefForStorage(stmt,
                                    4,
                                    5,
                                    6,
                                    7,
                                    8,
                                    9,
                                    10,
                                    11,
                                    12,
                                    13,
                                    14,
                                    15,
                                    16,
                                    17,
                                    18,
                                    19,
                                    20,
                                    21,
                                    def);
            const ErrorCode rc = stmt.Step();
            if (Failed(rc))
            {
                return rc;
            }

            if (def.indexed)
            {
                const std::wstring indexName =
                    L"idx_fv_" + std::to_wstring(schema->TableRowId()) + L"_" + SCCommon::SanitizeFileName(def.name);
                SqliteStmt createIndexStmt =
                    db_.Prepare((std::string("CREATE INDEX IF NOT EXISTS ") + SCCommon::ToUtf8(indexName) +
                                 " ON field_values(table_id, column_name, "
                                 "int64_value, double_value, bool_value, text_value);")
                                    .c_str());
                const ErrorCode createIndexRc = createIndexStmt.Step();
                if (Failed(createIndexRc))
                {
                    return createIndexRc;
                }
            }

            const std::int64_t insertedRowId = rowId > 0 ? rowId : db_.LastInsertRowId();
            const ErrorCode metadataRc = SyncLegacyIndexMetadata(schema, def.name, def.indexed);
            if (Failed(metadataRc))
            {
                return metadataRc;
            }
            schema->LoadColumn(def, insertedRowId);
            MarkReferenceIndexDirty();
            return SC_OK;
        }

        ErrorCode SqliteDatabase::PersistSchemaUpdateColumn(SqliteSchema* schema, const SCColumnDef& def)
        {
            if (schema == nullptr)
            {
                return SC_E_POINTER;
            }

            const SCColumnDef* previousDef = schema->FindColumnDef(def.name);
            if (previousDef == nullptr)
            {
                return SC_E_COLUMN_NOT_FOUND;
            }

            SqliteStmt stmt = db_.Prepare(
                "UPDATE schema_columns SET "
                " display_name = ?, value_kind = ?, column_kind = ?, "
                "nullable_flag = ?, editable_flag = ?, user_defined_flag = ?, "
                "indexed_flag = ?, participates_in_calc_flag = ?, unit = ?, "
                "reference_table = ?, reference_storage_column = ?, "
                "reference_display_column = ?, default_kind = ?, "
                "default_int64 = ?, default_double = ?, default_bool = ?, "
                "default_text = ?, default_blob = ? "
                "WHERE table_id = ? AND column_name = ?;");
            stmt.BindText(1, def.displayName);
            stmt.BindInt(2, ToSqliteValueKind(def.valueKind));
            stmt.BindInt(3, ToSqliteColumnKind(def.columnKind));
            stmt.BindInt(4, def.nullable ? 1 : 0);
            stmt.BindInt(5, def.editable ? 1 : 0);
            stmt.BindInt(6, def.userDefined ? 1 : 0);
            stmt.BindInt(7, def.indexed ? 1 : 0);
            stmt.BindInt(8, def.participatesInCalc ? 1 : 0);
            stmt.BindText(9, def.unit);
            stmt.BindText(10, def.referenceTable);
            stmt.BindText(11, def.referenceStorageColumn);
            stmt.BindText(12, def.referenceDisplayColumn);
            StorageCodec::BindValue(stmt, 13, 14, 15, 16, 17, 18, def.defaultValue);
            stmt.BindInt64(19, schema->TableRowId());
            stmt.BindText(20, def.name);
            const ErrorCode rc = stmt.Step();
            if (Failed(rc))
            {
                return rc;
            }

            const std::wstring indexName =
                L"idx_fv_" + std::to_wstring(schema->TableRowId()) + L"_" + SCCommon::SanitizeFileName(def.name);
            if (previousDef->indexed != def.indexed)
            {
                if (def.indexed)
                {
                    SqliteStmt createIndexStmt =
                        db_.Prepare((std::string("CREATE INDEX IF NOT EXISTS ") + SCCommon::ToUtf8(indexName) +
                                     " ON field_values(table_id, column_name, "
                                     "int64_value, double_value, bool_value, text_value);")
                                        .c_str());
                    const ErrorCode createIndexRc = createIndexStmt.Step();
                    if (Failed(createIndexRc))
                    {
                        return createIndexRc;
                    }
                } else
                {
                    SqliteStmt dropIndexStmt =
                        db_.Prepare((std::string("DROP INDEX IF EXISTS ") + SCCommon::ToUtf8(indexName) + ";").c_str());
                    const ErrorCode dropIndexRc = dropIndexStmt.Step();
                    if (Failed(dropIndexRc))
                    {
                        return dropIndexRc;
                    }
                }
            }

            const ErrorCode metadataRc = SyncLegacyIndexMetadata(schema, def.name, def.indexed);
            if (Failed(metadataRc))
            {
                return metadataRc;
            }
            schema->ReplaceColumn(def);
            MarkReferenceIndexDirty();
            return SC_OK;
        }

        ErrorCode SqliteDatabase::PersistSchemaRemoveColumn(SqliteSchema* schema, const wchar_t* columnName)
        {
            if (schema == nullptr || columnName == nullptr)
            {
                return SC_E_POINTER;
            }

            const SCColumnDef* previousDef = schema->FindColumnDef(columnName);
            if (previousDef == nullptr)
            {
                return SC_E_COLUMN_NOT_FOUND;
            }

            auto tableIt = std::find_if(
                tables_.begin(),
                tables_.end(),
                [tableRowId = schema->TableRowId()](const std::pair<const std::wstring, SCTablePtr>& entry) {
                    const auto* table = static_cast<SqliteTable*>(entry.second.Get());
                    return table != nullptr && table->TableRowId() == tableRowId;
                });
            if (tableIt == tables_.end())
            {
                return SC_E_FAIL;
            }
            auto* sqliteTable = static_cast<SqliteTable*>(tableIt->second.Get());

            const std::wstring indexName =
                L"idx_fv_" + std::to_wstring(schema->TableRowId()) + L"_" + SCCommon::SanitizeFileName(columnName);
            const std::string dropIndexSql = "DROP INDEX IF EXISTS " + SCCommon::ToUtf8(indexName) + ";";
            SqliteStmt dropIndexStmt = db_.Prepare(dropIndexSql.c_str());
            const ErrorCode dropIndexRc = dropIndexStmt.Step();
            if (Failed(dropIndexRc))
            {
                return dropIndexRc;
            }

            SqliteStmt deleteValuesStmt = db_.Prepare(
                "DELETE FROM field_values WHERE table_id = ? AND "
                "column_name = ?;");
            deleteValuesStmt.BindInt64(1, schema->TableRowId());
            deleteValuesStmt.BindText(2, columnName);
            const ErrorCode deleteValuesRc = deleteValuesStmt.Step();
            if (Failed(deleteValuesRc))
            {
                return deleteValuesRc;
            }

            SqliteStmt stmt = db_.Prepare(
                "DELETE FROM schema_columns WHERE table_id = ? AND "
                "column_name = ?;");
            stmt.BindInt64(1, schema->TableRowId());
            stmt.BindText(2, columnName);
            const ErrorCode rc = stmt.Step();
            if (Failed(rc))
            {
                return rc;
            }

            const ErrorCode metadataRc = SyncLegacyIndexMetadata(schema, columnName, false);
            if (Failed(metadataRc))
            {
                return metadataRc;
            }
            const ErrorCode pkMetadataRc = RemoveLegacyPrimaryKeyMetadata(schema, columnName);
            if (Failed(pkMetadataRc))
            {
                return pkMetadataRc;
            }

            for (const auto& [recordId, data] : sqliteTable->Records())
            {
                (void)recordId;
                if (data != nullptr)
                {
                    data->values.erase(columnName);
                }
            }

            schema->UnloadColumn(columnName);
            MarkReferenceIndexDirty();
            MarkForeignKeyReferenceCacheDirty();
            return SC_OK;
        }

        ErrorCode SqliteDatabase::PersistSchemaAddConstraint(SqliteSchema* schema,
                                                             const SCConstraintDef& def,
                                                             std::int64_t rowId)
        {
            if (schema == nullptr)
            {
                return SC_E_POINTER;
            }

            SqliteStmt stmt = db_.Prepare(
                "INSERT INTO schema_constraints("
                " constraint_id, table_id, kind, name, source_kind, "
                "referenced_table, on_delete_action, on_update_action, "
                "check_expression)"
                " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);");
            if (rowId > 0)
            {
                stmt.BindInt64(1, rowId);
            } else
            {
                stmt.BindNull(1);
            }
            stmt.BindInt64(2, schema->TableRowId());
            stmt.BindInt(3, ToSqliteConstraintKind(def.kind));
            stmt.BindText(4, def.name);
            stmt.BindInt(5, ToSqliteSchemaSourceKind(def.sourceKind));
            stmt.BindText(6, def.referencedTable);
            stmt.BindInt(7, ToSqliteForeignKeyAction(def.onDelete));
            stmt.BindInt(8, ToSqliteForeignKeyAction(def.onUpdate));
            stmt.BindText(9, def.checkExpression);
            const ErrorCode rc = stmt.Step();
            if (Failed(rc))
            {
                return rc;
            }

            const std::int64_t constraintId = rowId > 0 ? rowId : db_.LastInsertRowId();
            for (std::size_t index = 0; index < def.columns.size(); ++index)
            {
                SqliteStmt columnStmt = db_.Prepare(
                    "INSERT INTO schema_constraint_columns("
                    " constraint_id, column_ordinal, column_name, "
                    "referenced_column_name) VALUES(?, ?, ?, ?);");
                columnStmt.BindInt64(1, constraintId);
                columnStmt.BindInt64(2, static_cast<std::int64_t>(index));
                columnStmt.BindText(3, def.columns[index]);
                columnStmt.BindText(
                    4, index < def.referencedColumns.size() ? def.referencedColumns[index] : std::wstring{});
                const ErrorCode columnRc = columnStmt.Step();
                if (Failed(columnRc))
                {
                    return columnRc;
                }
            }

            schema->LoadConstraint(def, constraintId);
            MarkForeignKeyReferenceCacheDirty();
            return SC_OK;
        }

        ErrorCode SqliteDatabase::PersistSchemaRemoveConstraint(SqliteSchema* schema, const wchar_t* name)
        {
            if (schema == nullptr || name == nullptr)
            {
                return SC_E_POINTER;
            }

            if (schema->FindConstraintDef(name) == nullptr)
            {
                return SC_E_CONSTRAINT_NOT_FOUND;
            }

            {
                SqliteStmt deleteColumnsStmt = db_.Prepare(
                    "DELETE FROM schema_constraint_columns WHERE constraint_id "
                    "IN (SELECT constraint_id FROM schema_constraints WHERE "
                    "table_id = ? AND name = ?);");
                deleteColumnsStmt.BindInt64(1, schema->TableRowId());
                deleteColumnsStmt.BindText(2, name);
                const ErrorCode rc = deleteColumnsStmt.Step();
                if (Failed(rc))
                {
                    return rc;
                }
            }

            SqliteStmt deleteConstraintStmt =
                db_.Prepare("DELETE FROM schema_constraints WHERE table_id = ? AND name = ?;");
            deleteConstraintStmt.BindInt64(1, schema->TableRowId());
            deleteConstraintStmt.BindText(2, name);
            const ErrorCode deleteRc = deleteConstraintStmt.Step();
            if (Failed(deleteRc))
            {
                return deleteRc;
            }

            schema->UnloadConstraint(name);
            MarkForeignKeyReferenceCacheDirty();
            return SC_OK;
        }

        ErrorCode SqliteDatabase::PersistSchemaAddIndex(SqliteSchema* schema, const SCIndexDef& def, std::int64_t rowId)
        {
            if (schema == nullptr)
            {
                return SC_E_POINTER;
            }

            SqliteStmt stmt = db_.Prepare(
                "INSERT INTO schema_indexes(index_id, table_id, name, "
                "source_kind) VALUES(?, ?, ?, ?);");
            if (rowId > 0)
            {
                stmt.BindInt64(1, rowId);
            } else
            {
                stmt.BindNull(1);
            }
            stmt.BindInt64(2, schema->TableRowId());
            stmt.BindText(3, def.name);
            stmt.BindInt(4, ToSqliteSchemaSourceKind(def.sourceKind));
            const ErrorCode rc = stmt.Step();
            if (Failed(rc))
            {
                return rc;
            }

            const std::int64_t indexId = rowId > 0 ? rowId : db_.LastInsertRowId();
            const ErrorCode queryIndexDefRc = PersistQueryIndexDefinition(schema, def, indexId, false);
            if (Failed(queryIndexDefRc))
            {
                return queryIndexDefRc;
            }

            for (std::size_t columnIndex = 0; columnIndex < def.columns.size(); ++columnIndex)
            {
                SqliteStmt columnStmt = db_.Prepare(
                    "INSERT INTO schema_index_columns(index_id, "
                    "column_ordinal, column_name, descending_flag) "
                    "VALUES(?, ?, ?, ?);");
                columnStmt.BindInt64(1, indexId);
                columnStmt.BindInt64(2, static_cast<std::int64_t>(columnIndex));
                columnStmt.BindText(3, def.columns[columnIndex].columnName);
                columnStmt.BindInt(4, def.columns[columnIndex].descending ? 1 : 0);
                const ErrorCode columnRc = columnStmt.Step();
                if (Failed(columnRc))
                {
                    return columnRc;
                }
            }

            const bool compositeIndex = IsCompositeIndexExplicit(def);
            const auto tableIt = std::find_if(
                tables_.begin(),
                tables_.end(),
                [tableRowId = schema->TableRowId()](const std::pair<const std::wstring, SCTablePtr>& entry) {
                    const auto* table = static_cast<const SqliteTable*>(entry.second.Get());
                    return table != nullptr && table->TableRowId() == tableRowId;
                });
            if (tableIt != tables_.end())
            {
                auto* table = static_cast<SqliteTable*>(tableIt->second.Get());
                const ErrorCode rebuildRc = RebuildCompositeIndexEntriesForTable(table, def, indexId);
                if (Failed(rebuildRc))
                {
                    return rebuildRc;
                }
            }

            schema->LoadIndex(def, indexId);
            if (compositeIndex)
            {
                queryIndexRowIdsByTableAndName_[BuildQueryIndexStorageKey(schema->TableRowId(), def.name)] = indexId;
            }
            return SC_OK;
        }

        ErrorCode SqliteDatabase::PersistSchemaRemoveIndex(SqliteSchema* schema, const wchar_t* name)
        {
            if (schema == nullptr || name == nullptr)
            {
                return SC_E_POINTER;
            }

            if (schema->FindIndexDef(name) == nullptr)
            {
                return SC_E_INDEX_NOT_FOUND;
            }

            const SCIndexDef* previousDef = schema->FindIndexDef(name);
            const bool compositeIndex = previousDef != nullptr && IsCompositeIndexExplicit(*previousDef);
            if (compositeIndex)
            {
                const ErrorCode removeDefRc = RemoveQueryIndexDefinition(schema, name, false);
                if (Failed(removeDefRc))
                {
                    return removeDefRc;
                }
            }

            {
                SqliteStmt deleteColumnsStmt = db_.Prepare(
                    "DELETE FROM schema_index_columns WHERE index_id IN "
                    "(SELECT index_id FROM schema_indexes WHERE table_id = ? "
                    "AND name = ?);");
                deleteColumnsStmt.BindInt64(1, schema->TableRowId());
                deleteColumnsStmt.BindText(2, name);
                const ErrorCode rc = deleteColumnsStmt.Step();
                if (Failed(rc))
                {
                    return rc;
                }
            }

            SqliteStmt deleteIndexStmt = db_.Prepare("DELETE FROM schema_indexes WHERE table_id = ? AND name = ?;");
            deleteIndexStmt.BindInt64(1, schema->TableRowId());
            deleteIndexStmt.BindText(2, name);
            const ErrorCode deleteRc = deleteIndexStmt.Step();
            if (Failed(deleteRc))
            {
                return deleteRc;
            }

            schema->UnloadIndex(name);
            if (compositeIndex)
            {
                queryIndexRowIdsByTableAndName_.erase(BuildQueryIndexStorageKey(schema->TableRowId(), name));
            }
            return SC_OK;
        }

        ErrorCode SqliteDatabase::PersistAddedColumn(SqliteSchema* schema, const SCColumnDef& def)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            if (schema == nullptr)
            {
                return SC_E_POINTER;
            }

            auto tableIt = std::find_if(
                tables_.begin(),
                tables_.end(),
                [tableRowId = schema->TableRowId()](const std::pair<const std::wstring, SCTablePtr>& entry) {
                    const auto* table = static_cast<SqliteTable*>(entry.second.Get());
                    return table != nullptr && table->TableRowId() == tableRowId;
                });
            if (tableIt == tables_.end())
            {
                return SC_E_FAIL;
            }
            const std::wstring tableName = tableIt->first;

            try
            {
                SqliteTxn txn(db_);
                const ErrorCode rc = PersistSchemaAddColumn(schema, def);
                if (Failed(rc))
                {
                    return rc;
                }
                const ErrorCode commitRc = txn.Commit();
                if (Failed(commitRc))
                {
                    schema->UnloadColumn(def.name.c_str());
                    return commitRc;
                }
            } catch (...)
            {
                schema->UnloadColumn(def.name.c_str());
                return SC_E_FAIL;
            }

            const std::int64_t rowId = schema->FindColumnRowId(def.name.c_str());
            RecordSchemaJournal(tableName, SCColumnDef{}, def, JournalOp::AddColumn, rowId);
            return SC_OK;
        }

        ErrorCode SqliteDatabase::PersistUpdatedColumn(SqliteSchema* schema, const SCColumnDef& def)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            if (schema == nullptr)
            {
                return SC_E_POINTER;
            }

            auto tableIt = std::find_if(
                tables_.begin(),
                tables_.end(),
                [tableRowId = schema->TableRowId()](const std::pair<const std::wstring, SCTablePtr>& entry) {
                    const auto* table = static_cast<SqliteTable*>(entry.second.Get());
                    return table != nullptr && table->TableRowId() == tableRowId;
                });
            if (tableIt == tables_.end())
            {
                return SC_E_FAIL;
            }
            auto* sqliteTable = static_cast<SqliteTable*>(tableIt->second.Get());

            struct ColumnValueUpdate
            {
                std::shared_ptr<SqliteRecordData> data;
                SCValue newValue;
                SCValue oldValue;
            };

            std::vector<ColumnValueUpdate> updates;
            const SCColumnDef* previousDef = schema->FindColumnDef(def.name);
            if (previousDef == nullptr)
            {
                return SC_E_COLUMN_NOT_FOUND;
            }
            const SCColumnDef previousColumn = *previousDef;
            const std::size_t journalCheckpoint = activeJournal_.entries.size();
            const std::int64_t rowId = schema->FindColumnRowId(def.name.c_str());

            const auto restoreUpdatedValues = [&updates, &def]() {
                for (const auto& update : updates)
                {
                    if (update.oldValue.IsNull())
                    {
                        update.data->values.erase(def.name);
                    } else
                    {
                        update.data->values[def.name] = update.oldValue;
                    }
                }
            };
            const auto cleanupFailedUpdate = [&]() {
                restoreUpdatedValues();
                activeJournal_.entries.resize(journalCheckpoint);
                schema->ReplaceColumn(previousColumn);
            };

            if (previousDef->valueKind != def.valueKind)
            {
                updates.reserve(sqliteTable->Records().size());
                for (const auto& [recordId, data] : sqliteTable->Records())
                {
                    if (data == nullptr)
                    {
                        continue;
                    }

                    const auto valueIt = data->values.find(def.name);
                    if (valueIt == data->values.end())
                    {
                        continue;
                    }

                    SCValue converted;
                    const ErrorCode convertRc = ValueComparator::Convert(valueIt->second, def.valueKind, &converted);
                    if (Failed(convertRc))
                    {
                        return convertRc;
                    }

                    updates.push_back(ColumnValueUpdate{data, std::move(converted), valueIt->second});
                }
            }

            try
            {
                SqliteTxn txn(db_);
                const ErrorCode schemaRc = PersistSchemaUpdateColumn(schema, def);
                if (Failed(schemaRc))
                {
                    cleanupFailedUpdate();
                    return schemaRc;
                }

                if (!updates.empty())
                {
                    SqliteStmt deleteValueStmt = db_.Prepare(
                        "DELETE FROM field_values WHERE table_id = ? AND "
                        "record_id = ? AND column_name = ?;");
                    SqliteStmt insertValueStmt = db_.Prepare(
                        "INSERT INTO field_values(table_id, record_id, "
                        "column_name, value_kind, int64_value, double_value, "
                        "bool_value, text_value, blob_value) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);");
                    for (auto& update : updates)
                    {
                        deleteValueStmt.BindInt64(1, schema->TableRowId());
                        deleteValueStmt.BindInt64(2, update.data->id);
                        deleteValueStmt.BindText(3, def.name);
                        const ErrorCode deleteRc = deleteValueStmt.Step();
                        deleteValueStmt.Reset();
                        if (Failed(deleteRc))
                        {
                            cleanupFailedUpdate();
                            return deleteRc;
                        }

                        if (!update.newValue.IsNull())
                        {
                            update.data->values[def.name] = update.newValue;
                            insertValueStmt.BindInt64(1, schema->TableRowId());
                            insertValueStmt.BindInt64(2, update.data->id);
                            insertValueStmt.BindText(3, def.name);
                            StorageCodec::BindValue(insertValueStmt, 4, 5, 6, 7, 8, 9, update.newValue);
                            const ErrorCode insertRc = insertValueStmt.Step();
                            insertValueStmt.Reset();
                            if (Failed(insertRc))
                            {
                                cleanupFailedUpdate();
                                return insertRc;
                            }
                        } else
                        {
                            update.data->values.erase(def.name);
                        }

                        if (HasActiveEdit())
                        {
                            RecordJournal(
                                sqliteTable->Name(),
                                update.data->id,
                                def.name,
                                update.oldValue,
                                update.newValue,
                                false,
                                false,
                                def.columnKind == ColumnKind::Relation ? JournalOp::SetRelation : JournalOp::SetValue);
                        }
                    }
                }

                const ErrorCode finalCommitRc = txn.Commit();
                if (Failed(finalCommitRc))
                {
                    cleanupFailedUpdate();
                    return finalCommitRc;
                }
            } catch (...)
            {
                cleanupFailedUpdate();
                return SC_E_FAIL;
            }

            RecordSchemaJournal(tableIt->first, previousColumn, def, JournalOp::UpdateColumn, rowId);

            MarkReferenceIndexDirty();
            return SC_OK;
        }

        ErrorCode SqliteDatabase::ClearColumnValues(ISCTable* table, const wchar_t* name)
        {
            if (name == nullptr)
            {
                return SC_E_INVALIDARG;
            }
            if (!activeEdit_)
            {
                return SC_E_NO_ACTIVE_EDIT;
            }
            if (table == nullptr)
            {
                return SC_E_POINTER;
            }

            auto* sqliteTable = static_cast<SqliteTable*>(table);
            SCSchemaPtr schema;
            const ErrorCode schemaRc = sqliteTable->GetSchema(schema);
            if (Failed(schemaRc) || !schema)
            {
                return schemaRc;
            }

            SCColumnDef column;
            const ErrorCode columnRc = schema->FindColumn(name, &column);
            if (Failed(columnRc))
            {
                return columnRc;
            }

            const bool relationColumn = column.columnKind == ColumnKind::Relation;
            for (const auto& [recordId, data] : sqliteTable->Records())
            {
                if (data == nullptr)
                {
                    continue;
                }

                const auto valueIt = data->values.find(name);
                if (valueIt == data->values.end())
                {
                    continue;
                }

                const SCValue oldValue = valueIt->second;
                data->values.erase(valueIt);
                RecordJournal(sqliteTable->Name(),
                              recordId,
                              name,
                              oldValue,
                              SCValue::Null(),
                              false,
                              false,
                              relationColumn ? JournalOp::SetRelation : JournalOp::SetValue);
            }

            MarkReferenceIndexDirty();
            return SC_OK;
        }

        ErrorCode SqliteDatabase::PersistRemovedColumn(SqliteSchema* schema, const wchar_t* columnName)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            if (schema == nullptr || columnName == nullptr)
            {
                return SC_E_POINTER;
            }

            const SCColumnDef* previousDef = schema->FindColumnDef(columnName);
            if (previousDef == nullptr)
            {
                return SC_E_COLUMN_NOT_FOUND;
            }
            const SCColumnDef previousColumn = *previousDef;
            const std::int64_t rowId = schema->FindColumnRowId(columnName);
            auto tableIt = std::find_if(
                tables_.begin(),
                tables_.end(),
                [tableRowId = schema->TableRowId()](const std::pair<const std::wstring, SCTablePtr>& entry) {
                    const auto* table = static_cast<SqliteTable*>(entry.second.Get());
                    return table != nullptr && table->TableRowId() == tableRowId;
                });
            if (tableIt == tables_.end())
            {
                return SC_E_FAIL;
            }
            const std::wstring tableName = tableIt->first;
            auto* sqliteTable = static_cast<SqliteTable*>(tableIt->second.Get());
            if (sqliteTable == nullptr)
            {
                return SC_E_FAIL;
            }
            const std::wstring columnKey(columnName);
            struct ColumnRemovalValueSnapshot
            {
                RecordId recordId{0};
                bool hadValue{false};
                SCValue value;
            };
            std::vector<ColumnRemovalValueSnapshot> valueSnapshots;
            valueSnapshots.reserve(sqliteTable->Records().size());
            for (const auto& [recordId, data] : sqliteTable->Records())
            {
                ColumnRemovalValueSnapshot snapshot;
                snapshot.recordId = recordId;
                if (data != nullptr)
                {
                    const auto valueIt = data->values.find(columnKey);
                    if (valueIt != data->values.end())
                    {
                        snapshot.hadValue = true;
                        snapshot.value = valueIt->second;
                    }
                }
                valueSnapshots.push_back(std::move(snapshot));
            }

            const auto restorePreImage = [&]() {
                try
                {
                    const auto schemaColumn = schema->FindColumnDef(columnName);
                    if (schemaColumn != nullptr)
                    {
                        schema->ReplaceColumn(previousColumn);
                    } else
                    {
                        schema->LoadColumn(previousColumn);
                    }

                    for (const auto& snapshot : valueSnapshots)
                    {
                        const auto recordIt = sqliteTable->Records().find(snapshot.recordId);
                        if (recordIt == sqliteTable->Records().end() || recordIt->second == nullptr)
                        {
                            continue;
                        }

                        if (snapshot.hadValue)
                        {
                            recordIt->second->values[columnKey] = snapshot.value;
                        } else
                        {
                            recordIt->second->values.erase(columnKey);
                        }
                    }
                } catch (...)
                {
                }
            };

            try
            {
                SqliteTxn txn(db_);
                const ErrorCode rc = PersistSchemaRemoveColumn(schema, columnName);
                if (Failed(rc))
                {
                    restorePreImage();
                    return rc;
                }
                const ErrorCode commitRc = txn.Commit();
                if (Failed(commitRc))
                {
                    restorePreImage();
                    return commitRc;
                }
            } catch (...)
            {
                restorePreImage();
                return SC_E_FAIL;
            }

            RecordSchemaJournal(tableName, previousColumn, SCColumnDef{}, JournalOp::RemoveColumn, rowId);
            return SC_OK;
        }

        ErrorCode SqliteDatabase::PersistAddedConstraint(SqliteSchema* schema, const SCConstraintDef& def)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            ClearConstraintViolation();
            if (schema == nullptr)
            {
                return SC_E_POINTER;
            }

            auto* table = FindTableByRowId(schema->TableRowId());
            if (table == nullptr)
            {
                return SC_E_FAIL;
            }

            if (def.kind == SCConstraintKind::PrimaryKey || def.kind == SCConstraintKind::Unique ||
                def.kind == SCConstraintKind::ForeignKey || def.kind == SCConstraintKind::Check)
            {
                ErrorCode constraintValidationRc = SC_OK;
                switch (def.kind)
                {
                    case SCConstraintKind::PrimaryKey:
                    case SCConstraintKind::Unique:
                        constraintValidationRc =
                            ValidateConstraintUniqueness(table, def, std::shared_ptr<SqliteRecordData>{});
                        break;
                    case SCConstraintKind::Check:
                        constraintValidationRc = ValidateCheckConstraint(table, def, std::shared_ptr<SqliteRecordData>{});
                        break;
                    case SCConstraintKind::ForeignKey:
                        constraintValidationRc =
                            ValidateForeignKeyConstraint(table, def, std::shared_ptr<SqliteRecordData>{});
                        break;
                    default:
                        break;
                }
                if (Failed(constraintValidationRc))
                {
                    return constraintValidationRc;
                }
            }

            try
            {
                SqliteTxn txn(db_);
                const ErrorCode rc = PersistSchemaAddConstraint(schema, def);
                if (Failed(rc))
                {
                    return rc;
                }
                const ErrorCode commitRc = txn.Commit();
                if (Failed(commitRc))
                {
                    schema->UnloadConstraint(def.name.c_str());
                    return commitRc;
                }
            } catch (...)
            {
                schema->UnloadConstraint(def.name.c_str());
                return SC_E_FAIL;
            }

            const std::int64_t rowId = schema->FindConstraintRowId(def.name.c_str());
            RecordConstraintJournal(table->Name(), SCConstraintDef{}, def, JournalOp::AddConstraint, rowId);
            return SC_OK;
        }

        ErrorCode SqliteDatabase::PersistRemovedConstraint(SqliteSchema* schema, const wchar_t* name)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            if (schema == nullptr || name == nullptr)
            {
                return SC_E_POINTER;
            }

            const SCConstraintDef* previousDef = schema->FindConstraintDef(name);
            if (previousDef == nullptr)
            {
                return SC_E_CONSTRAINT_NOT_FOUND;
            }
            const SCConstraintDef previousConstraint = *previousDef;
            const std::int64_t rowId = schema->FindConstraintRowId(name);

            try
            {
                SqliteTxn txn(db_);
                const ErrorCode rc = PersistSchemaRemoveConstraint(schema, name);
                if (Failed(rc))
                {
                    if (schema->FindConstraintDef(name) == nullptr)
                    {
                        schema->LoadConstraint(previousConstraint, rowId);
                    }
                    return rc;
                }
                const ErrorCode commitRc = txn.Commit();
                if (Failed(commitRc))
                {
                    if (schema->FindConstraintDef(name) == nullptr)
                    {
                        schema->LoadConstraint(previousConstraint, rowId);
                    }
                    return commitRc;
                }
            } catch (...)
            {
                if (schema->FindConstraintDef(name) == nullptr)
                {
                    schema->LoadConstraint(previousConstraint, rowId);
                }
                return SC_E_FAIL;
            }

            auto tableIt = std::find_if(
                tables_.begin(),
                tables_.end(),
                [tableRowId = schema->TableRowId()](const std::pair<const std::wstring, SCTablePtr>& entry) {
                    const auto* table = static_cast<SqliteTable*>(entry.second.Get());
                    return table != nullptr && table->TableRowId() == tableRowId;
                });
            if (tableIt == tables_.end())
            {
                return SC_E_FAIL;
            }

            RecordConstraintJournal(
                tableIt->first, previousConstraint, SCConstraintDef{}, JournalOp::RemoveConstraint, rowId);
            return SC_OK;
        }

        ErrorCode SqliteDatabase::PersistAddedIndex(SqliteSchema* schema, const SCIndexDef& def)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            if (schema == nullptr)
            {
                return SC_E_POINTER;
            }

            auto tableIt = std::find_if(
                tables_.begin(),
                tables_.end(),
                [tableRowId = schema->TableRowId()](const std::pair<const std::wstring, SCTablePtr>& entry) {
                    const auto* table = static_cast<SqliteTable*>(entry.second.Get());
                    return table != nullptr && table->TableRowId() == tableRowId;
                });
            if (tableIt == tables_.end())
            {
                return SC_E_FAIL;
            }

            try
            {
                SqliteTxn txn(db_);
                const ErrorCode rc = PersistSchemaAddIndex(schema, def);
                if (Failed(rc))
                {
                    return rc;
                }
                const ErrorCode commitRc = txn.Commit();
                if (Failed(commitRc))
                {
                    schema->UnloadIndex(def.name.c_str());
                    return commitRc;
                }
            } catch (...)
            {
                schema->UnloadIndex(def.name.c_str());
                return SC_E_FAIL;
            }

            const std::int64_t rowId = schema->FindIndexRowId(def.name.c_str());
            RecordIndexJournal(tableIt->first, SCIndexDef{}, def, JournalOp::AddIndex, rowId);
            return SC_OK;
        }

        ErrorCode SqliteDatabase::PersistRemovedIndex(SqliteSchema* schema, const wchar_t* name)
        {
            const ErrorCode writableRc = EnsureWritable();
            if (Failed(writableRc))
            {
                return writableRc;
            }
            if (schema == nullptr || name == nullptr)
            {
                return SC_E_POINTER;
            }

            const SCIndexDef* previousDef = schema->FindIndexDef(name);
            if (previousDef == nullptr)
            {
                return SC_E_INDEX_NOT_FOUND;
            }
            const SCIndexDef previousIndex = *previousDef;
            const std::int64_t rowId = schema->FindIndexRowId(name);

            try
            {
                SqliteTxn txn(db_);
                const ErrorCode rc = PersistSchemaRemoveIndex(schema, name);
                if (Failed(rc))
                {
                    if (schema->FindIndexDef(name) == nullptr)
                    {
                        schema->LoadIndex(previousIndex, rowId);
                    }
                    return rc;
                }
                const ErrorCode commitRc = txn.Commit();
                if (Failed(commitRc))
                {
                    if (schema->FindIndexDef(name) == nullptr)
                    {
                        schema->LoadIndex(previousIndex, rowId);
                    }
                    return commitRc;
                }
            } catch (...)
            {
                if (schema->FindIndexDef(name) == nullptr)
                {
                    schema->LoadIndex(previousIndex, rowId);
                }
                return SC_E_FAIL;
            }

            auto tableIt = std::find_if(
                tables_.begin(),
                tables_.end(),
                [tableRowId = schema->TableRowId()](const std::pair<const std::wstring, SCTablePtr>& entry) {
                    const auto* table = static_cast<SqliteTable*>(entry.second.Get());
                    return table != nullptr && table->TableRowId() == tableRowId;
                });
            if (tableIt == tables_.end())
            {
                return SC_E_FAIL;
            }

            RecordIndexJournal(tableIt->first, previousIndex, SCIndexDef{}, JournalOp::RemoveIndex, rowId);
            return SC_OK;
        }

        void SqliteDatabase::ReloadColumnValuesFromStorage(SqliteTable* table, const wchar_t* columnName)
        {
            if (table == nullptr || columnName == nullptr)
            {
                return;
            }

            try
            {
                const bool supportsBinaryStorage = schemaVersion_ >= 4;
                SqliteStmt valuesStmt =
                    db_.Prepare(supportsBinaryStorage ? "SELECT record_id, value_kind, int64_value, "
                                                        "double_value, bool_value, text_value, blob_value "
                                                        "FROM field_values WHERE table_id = ? AND "
                                                        "column_name = ?;"
                                                      : "SELECT record_id, value_kind, int64_value, "
                                                        "double_value, bool_value, text_value FROM "
                                                        "field_values WHERE table_id = ? AND column_name = "
                                                        "?;");
                valuesStmt.BindInt64(1, table->TableRowId());
                valuesStmt.BindText(2, columnName);
                bool hasValue = false;
                while (valuesStmt.Step(&hasValue) == SC_OK && hasValue)
                {
                    const RecordId recordId = valuesStmt.ColumnInt64(0);
                    const auto recordIt = table->Records().find(recordId);
                    if (recordIt == table->Records().end() || recordIt->second == nullptr)
                    {
                        continue;
                    }

                    recordIt->second->values[std::wstring(columnName)] =
                        supportsBinaryStorage ? StorageCodec::ReadValue(valuesStmt, 1, 2, 3, 4, 5, 6)
                                              : StorageCodec::ReadValue(valuesStmt, 1, 2, 3, 4, 5, 5);
                }
            } catch (...)
            {
            }
        }

        void SqliteDatabase::RecordJournal(const std::wstring& tableName,
                                           RecordId recordId,
                                           const std::wstring& fieldName,
                                           const SCValue& oldValue,
                                           const SCValue& newValue,
                                           bool oldDeleted,
                                           bool newDeleted,
                                           JournalOp op)
        {
            for (auto& entry : activeJournal_.entries)
            {
                if (entry.op == op &&
                    JournalEntryMatchesCurrentTableName(entry, tableName) &&
                    entry.recordId == recordId &&
                    entry.fieldName == fieldName)
                {
                    entry.newValue = newValue;
                    entry.newDeleted = newDeleted;
                    return;
                }
            }

            activeJournal_.entries.push_back(JournalEntry{
                op,
                tableName,
                recordId,
                fieldName,
                oldValue,
                newValue,
                oldDeleted,
                newDeleted,
            });
        }

        void SqliteDatabase::RecordTableRenameJournal(const std::wstring& originalName,
                                                      const std::wstring& newName)
        {
            if (!HasActiveEdit())
            {
                return;
            }

            JournalEntry entry;
            entry.op = JournalOp::RenameTable;
            entry.tableName = originalName;
            entry.oldValue = SCValue::FromString(originalName);
            entry.newValue = SCValue::FromString(newName);
            activeJournal_.entries.push_back(std::move(entry));
        }

        ErrorCode SqliteDatabase::PersistTableRename(const std::wstring& originalName,
                                                     const std::wstring& newName,
                                                     bool recordJournal,
                                                     bool manageTransaction)
        {
            if (SCCommon::EqualsIgnoreCase(originalName, newName))
            {
                return SC_E_INVALIDARG;
            }

            auto tableIt = tables_.find(originalName);
            if (tableIt == tables_.end())
            {
                for (auto it = tables_.begin(); it != tables_.end(); ++it)
                {
                    if (SCCommon::EqualsIgnoreCase(it->first, originalName))
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

            for (const auto& [existingName, _] : tables_)
            {
                if (SCCommon::EqualsIgnoreCase(existingName, newName))
                {
                    return SC_E_SCHEMA_VIOLATION;
                }
            }

            auto* table = static_cast<SqliteTable*>(tableIt->second.Get());
            if (table == nullptr)
            {
                return SC_E_FAIL;
            }

            struct ColumnSnapshot
            {
                SqliteSchema* schema{nullptr};
                SCColumnDef column;
            };

            struct ConstraintSnapshot
            {
                SqliteSchema* schema{nullptr};
                SCConstraintDef constraint;
            };

            std::vector<ColumnSnapshot> originalColumns;
            std::vector<ConstraintSnapshot> originalConstraints;
            const auto restoreSchemaState = [&]() {
                for (auto it = originalColumns.rbegin(); it != originalColumns.rend();
                     ++it)
                {
                    if (it->schema != nullptr)
                    {
                        it->schema->ReplaceColumn(it->column);
                    }
                }
                for (auto it = originalConstraints.rbegin();
                     it != originalConstraints.rend(); ++it)
                {
                    if (it->schema != nullptr)
                    {
                        it->schema->ReplaceConstraint(it->constraint);
                    }
                }
            };

            std::optional<SqliteTxn> txn;
            std::optional<SqliteSavepoint> savepoint;
            try
            {
                if (manageTransaction)
                {
                    txn.emplace(db_);
                }
                else
                {
                    savepoint.emplace(db_, "rename_table");
                }

                SqliteStmt updateTableStmt =
                    db_.Prepare("UPDATE tables SET name = ? WHERE table_id = ?;");
                updateTableStmt.BindText(1, newName);
                updateTableStmt.BindInt64(2, table->TableRowId());
                const ErrorCode updateRc = updateTableStmt.Step();
                if (Failed(updateRc))
                {
                    return updateRc;
                }

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
                        restoreSchemaState();
                        return schemaRc;
                    }

                    auto* sqliteOtherSchema =
                        static_cast<SqliteSchema*>(otherSchema.Get());
                    if (sqliteOtherSchema == nullptr)
                    {
                        restoreSchemaState();
                        return SC_E_FAIL;
                    }

                    std::int32_t columnCount = 0;
                    const ErrorCode columnCountRc =
                        otherSchema->GetColumnCount(&columnCount);
                    if (Failed(columnCountRc))
                    {
                        restoreSchemaState();
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
                            restoreSchemaState();
                            return columnRc;
                        }

                        if (column.columnKind != ColumnKind::Relation ||
                            !SCCommon::EqualsIgnoreCase(column.referenceTable, originalName))
                        {
                            continue;
                        }

                        originalColumns.push_back(ColumnSnapshot{sqliteOtherSchema, column});
                        column.referenceTable = newName;
                        const ErrorCode updateColumnRc =
                            PersistSchemaUpdateColumn(sqliteOtherSchema, column);
                        if (Failed(updateColumnRc))
                        {
                            restoreSchemaState();
                            return updateColumnRc;
                        }
                    }

                    std::int32_t constraintCount = 0;
                    const ErrorCode constraintCountRc =
                        otherSchema->GetConstraintCount(&constraintCount);
                    if (Failed(constraintCountRc))
                    {
                        restoreSchemaState();
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
                            restoreSchemaState();
                            return constraintRc;
                        }
                        if (constraint.kind != SCConstraintKind::ForeignKey ||
                            !SCCommon::EqualsIgnoreCase(constraint.referencedTable, originalName))
                        {
                            continue;
                        }

                        originalConstraints.push_back(
                            ConstraintSnapshot{sqliteOtherSchema, constraint});
                        constraint.referencedTable = newName;

                        SqliteStmt updateConstraintStmt = db_.Prepare(
                            "UPDATE schema_constraints SET referenced_table = ? "
                            "WHERE table_id = ? AND name = ?;");
                        updateConstraintStmt.BindText(1, constraint.referencedTable);
                        updateConstraintStmt.BindInt64(2, sqliteOtherSchema->TableRowId());
                        updateConstraintStmt.BindText(3, constraint.name);
                        const ErrorCode updateConstraintRc =
                            updateConstraintStmt.Step();
                        if (Failed(updateConstraintRc))
                        {
                            restoreSchemaState();
                            return updateConstraintRc;
                        }

                        sqliteOtherSchema->ReplaceConstraint(constraint);
                    }
                }

                SaveMetadata();
                if (txn.has_value())
                {
                    const ErrorCode commitRc = txn->Commit();
                    if (Failed(commitRc))
                    {
                        restoreSchemaState();
                        return commitRc;
                    }
                }
                if (savepoint.has_value())
                {
                    const ErrorCode commitRc = savepoint->Commit();
                    if (Failed(commitRc))
                    {
                        restoreSchemaState();
                        return commitRc;
                    }
                }
            } catch (...)
            {
                restoreSchemaState();
                return SC_E_FAIL;
            }

            const std::wstring canonicalOriginalName = tableIt->first;
            SCTablePtr tableRef = tableIt->second;
            tables_.erase(tableIt);
            tables_.emplace(newName, tableRef);
            table->SetName(newName);
            if (auto* schema = table->Schema())
            {
                schema->SetTableName(newName);
            }
            if (recordJournal)
            {
                RecordTableRenameJournal(canonicalOriginalName, newName);
            }
            MarkReferenceIndexDirty();
            MarkForeignKeyReferenceCacheDirty();
            return SC_OK;
        }

        ErrorCode SqliteDatabase::PersistDeferredRenameToStorage(
            const DeferredRenameState& state)
        {
            auto* table = static_cast<SqliteTable*>(state.tableRef.Get());
            if (table == nullptr)
            {
                return SC_E_FAIL;
            }

            SqliteStmt updateTableStmt =
                db_.Prepare("UPDATE tables SET name = ? WHERE table_id = ?;");
            updateTableStmt.BindText(1, state.newName);
            updateTableStmt.BindInt64(2, table->TableRowId());
            const ErrorCode updateRc = updateTableStmt.Step();
            if (Failed(updateRc))
            {
                return updateRc;
            }

            for (const auto& snapshot : state.relationColumns)
            {
                if (snapshot.schema == nullptr)
                {
                    continue;
                }

                SqliteStmt updateColumnStmt = db_.Prepare(
                    "UPDATE schema_columns SET reference_table = ? "
                    "WHERE table_id = ? AND column_name = ?;");
                updateColumnStmt.BindText(1, state.newName);
                updateColumnStmt.BindInt64(2, snapshot.schema->TableRowId());
                updateColumnStmt.BindText(3, snapshot.column.name);
                const ErrorCode updateColumnRc = updateColumnStmt.Step();
                if (Failed(updateColumnRc))
                {
                    return updateColumnRc;
                }
            }

            for (const auto& snapshot : state.foreignKeyConstraints)
            {
                if (snapshot.schema == nullptr)
                {
                    continue;
                }

                SqliteStmt updateConstraintStmt = db_.Prepare(
                    "UPDATE schema_constraints SET referenced_table = ? "
                    "WHERE table_id = ? AND name = ?;");
                updateConstraintStmt.BindText(1, state.newName);
                updateConstraintStmt.BindInt64(2, snapshot.schema->TableRowId());
                updateConstraintStmt.BindText(3, snapshot.constraint.name);
                const ErrorCode updateConstraintRc =
                    updateConstraintStmt.Step();
                if (Failed(updateConstraintRc))
                {
                    return updateConstraintRc;
                }
            }

            MarkReferenceIndexDirty();
            MarkForeignKeyReferenceCacheDirty();
            return SC_OK;
        }

        ErrorCode SqliteDatabase::PersistDeferredSchemaOps()
        {
            for (const DeferredSchemaOp& op : activeSchemaOps_)
            {
                switch (op.kind)
                {
                    case DeferredSchemaOp::Kind::RenameTable: {
                        const ErrorCode renameRc =
                            PersistDeferredRenameToStorage(op.rename);
                        if (Failed(renameRc))
                        {
                            return renameRc;
                        }
                        break;
                    }
                }
            }
            return SC_OK;
        }

        void SqliteDatabase::RecordSchemaJournal(const std::wstring& tableName,
                                                 const SCColumnDef& oldColumn,
                                                 const SCColumnDef& newColumn,
                                                 JournalOp op,
                                                 std::int64_t columnRowId)
        {
            if (!HasActiveEdit())
            {
                return;
            }

            const std::wstring& columnName = !newColumn.name.empty() ? newColumn.name : oldColumn.name;
            for (auto& entry : activeJournal_.entries)
            {
                if (entry.op == op &&
                    JournalEntryMatchesCurrentTableName(entry, tableName) &&
                    entry.fieldName == columnName)
                {
                    entry.oldColumn = oldColumn;
                    entry.newColumn = newColumn;
                    entry.columnRowId = columnRowId;
                    return;
                }
            }

            activeJournal_.entries.push_back(JournalEntry{
                op,
                tableName,
                0,
                columnName,
                SCValue::Null(),
                SCValue::Null(),
                false,
                false,
                oldColumn,
                newColumn,
                columnRowId,
            });
        }

        void SqliteDatabase::RecordConstraintJournal(const std::wstring& tableName,
                                                     const SCConstraintDef& oldConstraint,
                                                     const SCConstraintDef& newConstraint,
                                                     JournalOp op,
                                                     std::int64_t constraintRowId)
        {
            if (!HasActiveEdit())
            {
                return;
            }

            const std::wstring& constraintName = !newConstraint.name.empty() ? newConstraint.name : oldConstraint.name;
            for (auto& entry : activeJournal_.entries)
            {
                if (entry.op == op &&
                    JournalEntryMatchesCurrentTableName(entry, tableName) &&
                    entry.fieldName == constraintName)
                {
                    entry.oldConstraint = oldConstraint;
                    entry.newConstraint = newConstraint;
                    entry.constraintRowId = constraintRowId;
                    return;
                }
            }

            JournalEntry entry;
            entry.op = op;
            entry.tableName = tableName;
            entry.fieldName = constraintName;
            entry.oldConstraint = oldConstraint;
            entry.newConstraint = newConstraint;
            entry.constraintRowId = constraintRowId;
            activeJournal_.entries.push_back(std::move(entry));
        }

        void SqliteDatabase::RecordIndexJournal(const std::wstring& tableName,
                                                const SCIndexDef& oldIndex,
                                                const SCIndexDef& newIndex,
                                                JournalOp op,
                                                std::int64_t indexRowId)
        {
            if (!HasActiveEdit())
            {
                return;
            }

            const std::wstring& indexName = !newIndex.name.empty() ? newIndex.name : oldIndex.name;
            for (auto& entry : activeJournal_.entries)
            {
                if (entry.op == op &&
                    JournalEntryMatchesCurrentTableName(entry, tableName) &&
                    entry.fieldName == indexName)
                {
                    entry.oldIndex = oldIndex;
                    entry.newIndex = newIndex;
                    entry.indexRowId = indexRowId;
                    return;
                }
            }

            JournalEntry entry;
            entry.op = op;
            entry.tableName = tableName;
            entry.fieldName = indexName;
            entry.oldIndex = oldIndex;
            entry.newIndex = newIndex;
            entry.indexRowId = indexRowId;
            activeJournal_.entries.push_back(std::move(entry));
        }

        ErrorCode SqliteDatabase::ApplyJournalReverse(const JournalTransaction& tx)
        {
            return ApplyJournalReverse(tx, 0, tx.entries.size(), nullptr);
        }

        ErrorCode SqliteDatabase::ApplyJournalForward(const JournalTransaction& tx)
        {
            return ApplyJournalForward(tx, 0, tx.entries.size(), nullptr);
        }

        ErrorCode SqliteDatabase::ApplyJournalReverse(const JournalTransaction& tx,
                                                      std::size_t beginIndex,
                                                      std::size_t endIndex,
                                                      std::size_t* outAppliedCount)
        {
            if (outAppliedCount != nullptr)
            {
                *outAppliedCount = 0;
            }
            if (beginIndex > endIndex || endIndex > tx.entries.size())
            {
                return SC_E_INVALIDARG;
            }

            for (std::size_t index = endIndex; index > beginIndex; --index)
            {
                const ErrorCode rc = ApplyEntry(tx.entries[index - 1], true);
                if (Failed(rc))
                {
                    return rc;
                }
                if (outAppliedCount != nullptr)
                {
                    ++(*outAppliedCount);
                }
            }
            return SC_OK;
        }

        ErrorCode SqliteDatabase::ApplyJournalForward(const JournalTransaction& tx,
                                                      std::size_t beginIndex,
                                                      std::size_t endIndex,
                                                      std::size_t* outAppliedCount)
        {
            if (outAppliedCount != nullptr)
            {
                *outAppliedCount = 0;
            }
            if (beginIndex > endIndex || endIndex > tx.entries.size())
            {
                return SC_E_INVALIDARG;
            }

            for (std::size_t index = beginIndex; index < endIndex; ++index)
            {
                const ErrorCode rc = ApplyEntry(tx.entries[index], false);
                if (Failed(rc))
                {
                    return rc;
                }
                if (outAppliedCount != nullptr)
                {
                    ++(*outAppliedCount);
                }
            }
            return SC_OK;
        }

        ErrorCode SqliteDatabase::FinalizeReplayFailure(ErrorCode primaryRc,
                                                        ErrorCode compensationRc)
        {
            if (Failed(compensationRc))
            {
                replayCompensationFailureDetected_ = true;
                return compensationRc;
            }

            RefreshReferenceIndexState();
            return primaryRc;
        }

        void SqliteDatabase::ClearReplayCompensationFailure() noexcept
        {
            replayCompensationFailureDetected_ = false;
        }

        ErrorCode SqliteDatabase::ApplyEntry(const JournalEntry& entry, bool reverse)
        {
            if (entry.op == JournalOp::RenameTable)
            {
                std::wstring oldName;
                std::wstring newName;
                if (entry.oldValue.AsStringCopy(&oldName) != SC_OK ||
                    entry.newValue.AsStringCopy(&newName) != SC_OK ||
                    oldName.empty() || newName.empty())
                {
                    return SC_E_FAIL;
                }

                if (HasActiveEdit())
                {
                    DeferredRenameState* state =
                        FindDeferredRenameState(oldName, newName);
                    if (state == nullptr)
                    {
                        return SC_E_FAIL;
                    }

                    if (reverse)
                    {
                        RollbackDeferredRenameWorkingState(*state);
                    }
                    else
                    {
                        ApplyDeferredRenameWorkingState(*state);
                    }
                    return SC_OK;
                }

                const std::wstring& fromName = reverse ? newName : oldName;
                const std::wstring& toName = reverse ? oldName : newName;
                return PersistTableRename(fromName, toName, false, false);
            }

            const auto tableIt = tables_.find(entry.tableName);
            if (tableIt == tables_.end())
            {
                return SC_OK;
            }

            auto* table = static_cast<SqliteTable*>(tableIt->second.Get());
            switch (entry.op)
            {
                case JournalOp::AddColumn:
                case JournalOp::UpdateColumn:
                case JournalOp::RemoveColumn:
                case JournalOp::AddConstraint:
                case JournalOp::RemoveConstraint:
                case JournalOp::AddIndex:
                case JournalOp::RemoveIndex:
                    return ApplySchemaEntry(entry, reverse);
                case JournalOp::CreateRecord:
                case JournalOp::DeleteRecord:
                case JournalOp::SetRelation:
                case JournalOp::SetValue: {
                    auto data = table->FindRecordData(entry.recordId);
                    if (!data)
                    {
                        data = std::make_shared<SqliteRecordData>(entry.recordId);
                        table->Records().emplace(entry.recordId, data);
                    }

                    switch (entry.op)
                    {
                        case JournalOp::CreateRecord:
                        case JournalOp::DeleteRecord:
                            data->state = reverse ? (entry.oldDeleted ? RecordState::Deleted : RecordState::Alive)
                                                  : (entry.newDeleted ? RecordState::Deleted : RecordState::Alive);
                            if (reverse && entry.op == JournalOp::CreateRecord)
                            {
                                data->values.clear();
                            }
                            break;
                        case JournalOp::SetRelation:
                        case JournalOp::SetValue:
                            if (reverse)
                            {
                                if (entry.oldValue.IsNull())
                                {
                                    data->values.erase(entry.fieldName);
                                } else
                                {
                                    data->values[entry.fieldName] = entry.oldValue;
                                }
                            } else if (entry.newValue.IsNull())
                            {
                                data->values.erase(entry.fieldName);
                            } else
                            {
                                data->values[entry.fieldName] = entry.newValue;
                            }
                            break;
                        default:
                            break;
                    }
                    break;
                }
                default:
                    break;
            }
            return SC_OK;
        }

        ErrorCode SqliteDatabase::ApplySchemaEntry(const JournalEntry& entry, bool reverse)
        {
            const auto tableIt = tables_.find(entry.tableName);
            if (tableIt == tables_.end())
            {
                return SC_OK;
            }

            auto* table = static_cast<SqliteTable*>(tableIt->second.Get());
            if (table == nullptr)
            {
                return SC_OK;
            }

            auto* schema = table->Schema();
            if (schema == nullptr)
            {
                return SC_OK;
            }

            const SCColumnDef& def = reverse ? entry.oldColumn : entry.newColumn;
            const auto makeReplayColumn = [&]() {
                SCColumnDef replayColumn = def;
                if (replayColumn.name.empty())
                {
                    replayColumn.name = entry.fieldName;
                }
                return replayColumn;
            };

            switch (entry.op)
            {
                case JournalOp::AddColumn:
                    if (reverse)
                    {
                        return PersistSchemaRemoveColumn(schema, entry.fieldName.c_str());
                    } else
                    {
                        return PersistSchemaAddColumn(schema, makeReplayColumn(), entry.columnRowId);
                    }
                case JournalOp::UpdateColumn:
                    return PersistSchemaUpdateColumn(schema, makeReplayColumn());
                case JournalOp::RemoveColumn:
                    if (reverse)
                    {
                        return PersistSchemaAddColumn(schema, makeReplayColumn(), entry.columnRowId);
                    } else
                    {
                        return PersistSchemaRemoveColumn(schema, entry.fieldName.c_str());
                    }
                case JournalOp::AddConstraint:
                    if (reverse)
                    {
                        return PersistSchemaRemoveConstraint(schema, entry.fieldName.c_str());
                    }
                    return PersistSchemaAddConstraint(schema, entry.newConstraint, entry.constraintRowId);
                case JournalOp::RemoveConstraint:
                    if (reverse)
                    {
                        return PersistSchemaAddConstraint(schema, entry.oldConstraint, entry.constraintRowId);
                    }
                    return PersistSchemaRemoveConstraint(schema, entry.fieldName.c_str());
                case JournalOp::AddIndex:
                    if (reverse)
                    {
                        return PersistSchemaRemoveIndex(schema, entry.fieldName.c_str());
                    }
                    return PersistSchemaAddIndex(schema, entry.newIndex, entry.indexRowId);
                case JournalOp::RemoveIndex:
                    if (reverse)
                    {
                        return PersistSchemaAddIndex(schema, entry.oldIndex, entry.indexRowId);
                    }
                    return PersistSchemaRemoveIndex(schema, entry.fieldName.c_str());
                default:
                    break;
            }
            return SC_OK;
        }

        void SqliteDatabase::UpdateTouchedVersions(
            const JournalTransaction& tx,
            VersionId version,
            bool reverseRenameResolution)
        {
            for (const auto& [tableName, recordId] :
                 GetTouchedRecordKeys(tx, reverseRenameResolution))
            {
                const auto tableIt = tables_.find(tableName);
                if (tableIt == tables_.end())
                {
                    continue;
                }
                auto record = static_cast<SqliteTable*>(tableIt->second.Get())->FindRecordData(recordId);
                if (record)
                {
                    record->lastModifiedVersion = version;
                }
            }
        }

        SCChangeSet SqliteDatabase::BuildChangeSet(const JournalTransaction& tx,
                                                   ChangeSource source,
                                                   VersionId version) const
        {
            SCChangeSet SCChangeSet;
            SCChangeSet.actionName = tx.actionName;
            SCChangeSet.source = source;
            SCChangeSet.version = version;

            for (const auto& entry : tx.entries)
            {
                SCDataChange change;
                change.tableName = entry.tableName;
                change.recordId = entry.recordId;
                change.fieldName = entry.fieldName;
                change.oldValue = (source == ChangeSource::Undo) ? entry.newValue : entry.oldValue;
                change.newValue = (source == ChangeSource::Undo) ? entry.oldValue : entry.newValue;
                change.structuralChange =
                    (entry.op == JournalOp::CreateRecord || entry.op == JournalOp::DeleteRecord ||
                     entry.op == JournalOp::AddColumn || entry.op == JournalOp::UpdateColumn ||
                     entry.op == JournalOp::RemoveColumn || entry.op == JournalOp::AddConstraint ||
                     entry.op == JournalOp::RemoveConstraint || entry.op == JournalOp::AddIndex ||
                     entry.op == JournalOp::RemoveIndex || entry.op == JournalOp::RenameTable);
                change.relationChange = (entry.op == JournalOp::SetRelation);

                switch (entry.op)
                {
                    case JournalOp::CreateRecord:
                        change.kind =
                            (source == ChangeSource::Undo) ? ChangeKind::RecordDeleted : ChangeKind::RecordCreated;
                        break;
                    case JournalOp::DeleteRecord:
                        change.kind =
                            (source == ChangeSource::Undo) ? ChangeKind::RecordCreated : ChangeKind::RecordDeleted;
                        break;
                    case JournalOp::SetRelation:
                        change.kind = ChangeKind::RelationUpdated;
                        break;
                    case JournalOp::AddColumn:
                    case JournalOp::UpdateColumn:
                    case JournalOp::RemoveColumn:
                    case JournalOp::AddConstraint:
                    case JournalOp::RemoveConstraint:
                    case JournalOp::AddIndex:
                    case JournalOp::RemoveIndex:
                    case JournalOp::SetValue:
                        change.kind = ChangeKind::FieldUpdated;
                        break;
                    case JournalOp::RenameTable:
                        change.kind = ChangeKind::TableRenamed;
                        break;
                    default:
                        change.kind = ChangeKind::FieldUpdated;
                        break;
                }

                SCChangeSet.changes.push_back(std::move(change));
            }
            return SCChangeSet;
        }

        void SqliteDatabase::NotifyObservers(const SCChangeSet& SCChangeSet)
        {
            std::vector<ISCDatabaseObserver*> snapshot = observers_;
            for (auto* observer : snapshot)
            {
                if (observer != nullptr)
                {
                    observer->OnDatabaseChanged(SCChangeSet);
                }
            }
        }

        std::vector<std::pair<std::wstring, RecordId>>
        SqliteDatabase::GetTouchedRecordKeys(
            const JournalTransaction& tx,
            bool reverseRenameResolution) const
        {
            std::set<std::pair<std::wstring, RecordId>> unique;
            const bool hasRenameTable =
                JournalTransactionContainsRenameTable(tx);
            for (const auto& entry : tx.entries)
            {
                unique.emplace(
                    hasRenameTable
                        ? ResolveJournalTableNameToReplayState(
                              tx,
                              entry.tableName,
                              reverseRenameResolution)
                        : entry.tableName,
                    entry.recordId);
            }
            return std::vector<std::pair<std::wstring, RecordId>>(unique.begin(), unique.end());
        }

        void SqliteDatabase::PersistTouchedRecords(
            const JournalTransaction& tx,
            VersionId version,
            bool reverseRenameResolution)
        {
            SqliteStmt upsertRecord = db_.Prepare(
                "INSERT INTO records(table_id, record_id, state, "
                "last_modified_version) VALUES(?, ?, ?, ?)"
                " ON CONFLICT(table_id, record_id) DO UPDATE SET "
                "state=excluded.state, "
                "last_modified_version=excluded.last_modified_version;");
            SqliteStmt deleteValues = db_.Prepare(
                "DELETE FROM field_values WHERE table_id = ? AND record_id = "
                "?;");
            SqliteStmt insertValue = db_.Prepare(
                "INSERT INTO field_values(table_id, record_id, column_name, "
                "value_kind, int64_value, double_value, bool_value, text_value, blob_value)"
                " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);");

            for (const auto& [tableName, recordId] :
                 GetTouchedRecordKeys(tx, reverseRenameResolution))
            {
                const auto tableIt = tables_.find(tableName);
                if (tableIt == tables_.end())
                {
                    continue;
                }

                auto* table = static_cast<SqliteTable*>(tableIt->second.Get());
                auto data = table->FindRecordData(recordId);
                if (!data)
                {
                    continue;
                }

                upsertRecord.BindInt64(1, table->TableRowId());
                upsertRecord.BindInt64(2, data->id);
                upsertRecord.BindInt(3, ToSqliteRecordState(data->state));
                upsertRecord.BindInt64(4, static_cast<std::int64_t>(version));
                upsertRecord.Step();
                upsertRecord.Reset();

                deleteValues.BindInt64(1, table->TableRowId());
                deleteValues.BindInt64(2, data->id);
                deleteValues.Step();
                deleteValues.Reset();

                if (data->state == RecordState::Deleted)
                {
                    const ErrorCode rebuildDeletedRc = RebuildCompositeIndexEntriesForRecord(table, *data);
                    if (Failed(rebuildDeletedRc))
                    {
                        throw std::runtime_error("failed to rebuild deleted composite index entries");
                    }
                    continue;
                }

                for (const auto& [fieldName, SCValue] : data->values)
                {
                    insertValue.BindInt64(1, table->TableRowId());
                    insertValue.BindInt64(2, data->id);
                    insertValue.BindText(3, fieldName);
                    StorageCodec::BindValue(insertValue, 4, 5, 6, 7, 8, 9, SCValue);
                    insertValue.Step();
                    insertValue.Reset();
                }

                const ErrorCode rebuildRc = RebuildCompositeIndexEntriesForRecord(table, *data);
                if (Failed(rebuildRc))
                {
                    throw std::runtime_error("failed to rebuild composite index entries");
                }
            }
        }

        std::int64_t SqliteDatabase::InsertJournalTransaction(const JournalTransaction& tx,
                                                              int stackKind,
                                                              int stackOrder)
        {
            SqliteStmt stmt = db_.Prepare(
                "INSERT INTO journal_transactions(action_name, "
                "committed_version, stack_kind, stack_order) VALUES(?, ?, ?, "
                "?);");
            stmt.BindText(1, tx.actionName);
            stmt.BindInt64(2, static_cast<std::int64_t>(tx.committedVersion));
            stmt.BindInt(3, stackKind);
            stmt.BindInt(4, stackOrder);
            stmt.Step();
            return db_.LastInsertRowId();
        }

        void SqliteDatabase::PersistJournalEntries(std::int64_t txId, const JournalTransaction& tx)
        {
            SqliteStmt valueStmt = db_.Prepare(
                "INSERT INTO journal_entries("
                " tx_id, sequence_index, op, table_name, record_id, "
                "field_name, old_kind, old_int64, old_double, old_bool, "
                "old_text, old_blob,"
                " new_kind, new_int64, new_double, new_bool, new_text, new_blob, "
                "old_deleted, new_deleted)"
                " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

            int sequence = 0;
            for (const auto& entry : tx.entries)
            {
                if (entry.op == JournalOp::AddColumn || entry.op == JournalOp::UpdateColumn ||
                    entry.op == JournalOp::RemoveColumn)
                {
                    continue;
                }

                valueStmt.BindInt64(1, txId);
                valueStmt.BindInt(2, sequence++);
                valueStmt.BindInt(3, ToSqliteJournalOp(entry.op));
                valueStmt.BindText(4, entry.tableName);
                const bool constraintEntry =
                    entry.op == JournalOp::AddConstraint || entry.op == JournalOp::RemoveConstraint;
                const bool indexEntry = entry.op == JournalOp::AddIndex || entry.op == JournalOp::RemoveIndex;
                valueStmt.BindInt64(
                    5, constraintEntry ? entry.constraintRowId : (indexEntry ? entry.indexRowId : entry.recordId));
                valueStmt.BindText(6, entry.fieldName);
                const SCValue oldPersistedValue =
                    constraintEntry ? (entry.oldConstraint.name.empty()
                                           ? SCValue::Null()
                                           : SCValue::FromString(ImportSerializer::SerializeConstraintDef(entry.oldConstraint)))
                                    : (indexEntry ? (entry.oldIndex.name.empty()
                                                         ? SCValue::Null()
                                                         : SCValue::FromString(ImportSerializer::SerializeIndexDef(entry.oldIndex)))
                                                  : entry.oldValue);
                const SCValue newPersistedValue =
                    constraintEntry ? (entry.newConstraint.name.empty()
                                           ? SCValue::Null()
                                           : SCValue::FromString(ImportSerializer::SerializeConstraintDef(entry.newConstraint)))
                                    : (indexEntry ? (entry.newIndex.name.empty()
                                                         ? SCValue::Null()
                                                         : SCValue::FromString(ImportSerializer::SerializeIndexDef(entry.newIndex)))
                                                  : entry.newValue);
                StorageCodec::BindValue(valueStmt, 7, 8, 9, 10, 11, 12, oldPersistedValue);
                StorageCodec::BindValue(valueStmt, 13, 14, 15, 16, 17, 18, newPersistedValue);
                valueStmt.BindInt(19, entry.oldDeleted ? 1 : 0);
                valueStmt.BindInt(20, entry.newDeleted ? 1 : 0);
                valueStmt.Step();
                valueStmt.Reset();
            }

            PersistSchemaJournalEntries(txId, tx, &sequence);
        }

        void SqliteDatabase::PersistSchemaJournalEntries(std::int64_t txId, const JournalTransaction& tx, int* sequence)
        {
            if (sequence == nullptr)
            {
                return;
            }

            SqliteStmt stmt = db_.Prepare(
                "INSERT INTO journal_schema_entries("
                " tx_id, sequence_index, op, table_name, column_name, "
                "column_rowid, old_display_name, old_value_kind, "
                "old_column_kind, old_nullable, old_editable, "
                "old_user_defined, old_indexed, old_participates_in_calc, "
                "old_unit, old_reference_table, old_reference_storage_column, "
                "old_reference_display_column, old_default_kind, "
                "old_default_int64, old_default_double, old_default_bool, "
                "old_default_text, old_default_blob, new_display_name, new_value_kind, "
                "new_column_kind, new_nullable, new_editable, "
                "new_user_defined, new_indexed, new_participates_in_calc, "
                "new_unit, new_reference_table, new_reference_storage_column, "
                "new_reference_display_column, new_default_kind, "
                "new_default_int64, new_default_double, new_default_bool, "
                "new_default_text, new_default_blob)"
                " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

            for (const auto& entry : tx.entries)
            {
                if (entry.op != JournalOp::AddColumn && entry.op != JournalOp::UpdateColumn &&
                    entry.op != JournalOp::RemoveColumn)
                {
                    continue;
                }

                stmt.BindInt64(1, txId);
                stmt.BindInt(2, (*sequence)++);
                stmt.BindInt(3, ToSqliteJournalOp(entry.op));
                stmt.BindText(4, entry.tableName);
                stmt.BindText(5, entry.fieldName);
                stmt.BindInt64(6, entry.columnRowId);
                BindColumnDefForStorage(
                    stmt, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
                    entry.oldColumn);
                BindColumnDefForStorage(
                    stmt, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42,
                    entry.newColumn);
                stmt.Step();
                stmt.Reset();
            }
        }

        void SqliteDatabase::DeleteRedoJournalRows()
        {
            for (const auto& tx : redoStack_)
            {
                DeleteJournalTransaction(tx.txId);
            }
            redoStack_.clear();
        }

        void SqliteDatabase::UpdateJournalTransactionStack(std::int64_t txId, int stackKind, int stackOrder)
        {
            SqliteStmt stmt = db_.Prepare(
                "UPDATE journal_transactions SET stack_kind = ?, stack_order = "
                "? WHERE tx_id = ?;");
            stmt.BindInt(1, stackKind);
            stmt.BindInt(2, stackOrder);
            stmt.BindInt64(3, txId);
            stmt.Step();
        }

        void SqliteDatabase::DeleteJournalTransaction(std::int64_t txId)
        {
            SqliteStmt valueStmt = db_.Prepare("DELETE FROM journal_entries WHERE tx_id = ?;");
            valueStmt.BindInt64(1, txId);
            valueStmt.Step();

            SqliteStmt schemaStmt = db_.Prepare("DELETE FROM journal_schema_entries WHERE tx_id = ?;");
            schemaStmt.BindInt64(1, txId);
            schemaStmt.Step();

            SqliteStmt stmt = db_.Prepare("DELETE FROM journal_transactions WHERE tx_id = ?;");
            stmt.BindInt64(1, txId);
            stmt.Step();
        }
} // namespace StableCore::Storage
