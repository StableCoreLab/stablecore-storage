#include "Sqlite/SqliteImplCore.h"
#include "Sqlite/SqliteImplDatabase.h"

namespace StableCore::Storage
{

// ========================================================================
// ValueComparator static methods
// (adapted from source lines 3103-3430 and 528-703)
// ========================================================================

bool ValueComparator::IsNumeric(ValueKind kind) noexcept
{
    switch (kind)
    {
        case ValueKind::Int64:
        case ValueKind::Double:
        case ValueKind::Bool:
        case ValueKind::RecordId:
            return true;
        default:
            return false;
    }
}

bool ValueComparator::TryAsDouble(const SCValue& value, double* outValue) noexcept
{
    if (outValue == nullptr)
    {
        return false;
    }

    switch (value.GetKind())
    {
        case ValueKind::Int64: {
            std::int64_t typed = 0;
            if (Failed(value.AsInt64(&typed)))
            {
                return false;
            }
            *outValue = static_cast<double>(typed);
            return true;
        }
        case ValueKind::Double:
            return Succeeded(value.AsDouble(outValue));
        case ValueKind::Bool: {
            bool flag = false;
            if (Failed(value.AsBool(&flag)))
            {
                return false;
            }
            *outValue = flag ? 1.0 : 0.0;
            return true;
        }
        case ValueKind::RecordId: {
            RecordId id = 0;
            if (Failed(value.AsRecordId(&id)))
            {
                return false;
            }
            *outValue = static_cast<double>(id);
            return true;
        }
        default:
            return false;
    }
}

bool ValueComparator::TryAsText(const SCValue& value, std::wstring* outValue) noexcept
{
    if (outValue == nullptr)
    {
        return false;
    }

    switch (value.GetKind())
    {
        case ValueKind::String:
        case ValueKind::Enum:
            return Succeeded(value.AsStringCopy(outValue)) || Succeeded(value.AsEnumCopy(outValue));
        default:
            return false;
    }
}

bool ValueComparator::IsTruthy(const SCValue& value) noexcept
{
    if (value.IsNull())
    {
        return false;
    }

    switch (value.GetKind())
    {
        case ValueKind::Bool: {
            bool flag = false;
            return Succeeded(value.AsBool(&flag)) && flag;
        }
        case ValueKind::Int64: {
            std::int64_t typed = 0;
            return Succeeded(value.AsInt64(&typed)) && typed != 0;
        }
        case ValueKind::Double: {
            double typed = 0.0;
            return Succeeded(value.AsDouble(&typed)) && typed != 0.0;
        }
        case ValueKind::RecordId: {
            RecordId typed = 0;
            return Succeeded(value.AsRecordId(&typed)) && typed != 0;
        }
        case ValueKind::String:
        case ValueKind::Enum: {
            std::wstring text;
            return Succeeded(value.AsStringCopy(&text)) && !text.empty();
        }
        default:
            return false;
    }
}

// ========================================================================
// EvaluateConstraintExpressionNode
// (from source lines 3212-3413)
// ========================================================================

ErrorCode EvaluateConstraintExpressionNode(const ConstraintExpressionNode& node,
                                           const std::unordered_map<std::wstring, SCValue>& values,
                                           SCValue* outValue)
{
    if (outValue == nullptr)
    {
        return SC_E_POINTER;
    }

    switch (node.kind)
    {
        case ConstraintExpressionNode::Kind::Literal:
            *outValue = node.literal;
            return SC_OK;
        case ConstraintExpressionNode::Kind::Column: {
            const auto it = std::find_if(values.begin(),
                                         values.end(),
                                         [&node](const auto& entry) {
                                             return SCCommon::EqualsIgnoreCase(entry.first, node.identifier);
                                         });
            if (it == values.end())
            {
                return SC_E_COLUMN_NOT_FOUND;
            }
            *outValue = it->second;
            return SC_OK;
        }
        case ConstraintExpressionNode::Kind::Unary: {
            SCValue child;
            const ErrorCode rc = EvaluateConstraintExpressionNode(*node.left, values, &child);
            if (Failed(rc))
            {
                return rc;
            }
            if (node.unaryOp == ConstraintExpressionNode::UnaryOp::Not)
            {
                *outValue = SCValue::FromBool(!ValueComparator::IsTruthy(child));
                return SC_OK;
            }
            double numeric = 0.0;
            if (!ValueComparator::TryAsDouble(child, &numeric))
            {
                return SC_E_TYPE_MISMATCH;
            }
            *outValue = SCValue::FromDouble(-numeric);
            return SC_OK;
        }
        case ConstraintExpressionNode::Kind::Binary: {
            SCValue left;
            SCValue right;
            const ErrorCode leftRc = EvaluateConstraintExpressionNode(*node.left, values, &left);
            if (Failed(leftRc))
            {
                return leftRc;
            }
            const ErrorCode rightRc = EvaluateConstraintExpressionNode(*node.right, values, &right);
            if (Failed(rightRc))
            {
                return rightRc;
            }

            switch (node.binaryOp)
            {
                case ConstraintExpressionNode::BinaryOp::Add:
                case ConstraintExpressionNode::BinaryOp::Subtract:
                case ConstraintExpressionNode::BinaryOp::Multiply:
                case ConstraintExpressionNode::BinaryOp::Divide: {
                    double leftNumber = 0.0;
                    double rightNumber = 0.0;
                    if (!ValueComparator::TryAsDouble(left, &leftNumber) ||
                        !ValueComparator::TryAsDouble(right, &rightNumber))
                    {
                        return SC_E_TYPE_MISMATCH;
                    }
                    if (node.binaryOp == ConstraintExpressionNode::BinaryOp::Divide && rightNumber == 0.0)
                    {
                        return SC_E_INVALIDARG;
                    }
                    switch (node.binaryOp)
                    {
                        case ConstraintExpressionNode::BinaryOp::Add:
                            *outValue = SCValue::FromDouble(leftNumber + rightNumber);
                            break;
                        case ConstraintExpressionNode::BinaryOp::Subtract:
                            *outValue = SCValue::FromDouble(leftNumber - rightNumber);
                            break;
                        case ConstraintExpressionNode::BinaryOp::Multiply:
                            *outValue = SCValue::FromDouble(leftNumber * rightNumber);
                            break;
                        case ConstraintExpressionNode::BinaryOp::Divide:
                            *outValue = SCValue::FromDouble(leftNumber / rightNumber);
                            break;
                        default:
                            break;
                    }
                    return SC_OK;
                }
                case ConstraintExpressionNode::BinaryOp::Equal:
                    if (ValueComparator::IsNumeric(left.GetKind()) && ValueComparator::IsNumeric(right.GetKind()))
                    {
                        double leftNumber = 0.0;
                        double rightNumber = 0.0;
                        if (!ValueComparator::TryAsDouble(left, &leftNumber) ||
                            !ValueComparator::TryAsDouble(right, &rightNumber))
                        {
                            return SC_E_TYPE_MISMATCH;
                        }
                        *outValue = SCValue::FromBool(leftNumber == rightNumber);
                    } else
                    {
                        *outValue = SCValue::FromBool(left == right);
                    }
                    return SC_OK;
                case ConstraintExpressionNode::BinaryOp::NotEqual:
                    if (ValueComparator::IsNumeric(left.GetKind()) && ValueComparator::IsNumeric(right.GetKind()))
                    {
                        double leftNumber = 0.0;
                        double rightNumber = 0.0;
                        if (!ValueComparator::TryAsDouble(left, &leftNumber) ||
                            !ValueComparator::TryAsDouble(right, &rightNumber))
                        {
                            return SC_E_TYPE_MISMATCH;
                        }
                        *outValue = SCValue::FromBool(leftNumber != rightNumber);
                    } else
                    {
                        *outValue = SCValue::FromBool(left != right);
                    }
                    return SC_OK;
                case ConstraintExpressionNode::BinaryOp::Less:
                case ConstraintExpressionNode::BinaryOp::LessEqual:
                case ConstraintExpressionNode::BinaryOp::Greater:
                case ConstraintExpressionNode::BinaryOp::GreaterEqual: {
                    if (ValueComparator::IsNumeric(left.GetKind()) && ValueComparator::IsNumeric(right.GetKind()))
                    {
                        double leftNumber = 0.0;
                        double rightNumber = 0.0;
                        if (!ValueComparator::TryAsDouble(left, &leftNumber) ||
                            !ValueComparator::TryAsDouble(right, &rightNumber))
                        {
                            return SC_E_TYPE_MISMATCH;
                        }
                        bool result = false;
                        switch (node.binaryOp)
                        {
                            case ConstraintExpressionNode::BinaryOp::Less:
                                result = leftNumber < rightNumber;
                                break;
                            case ConstraintExpressionNode::BinaryOp::LessEqual:
                                result = leftNumber <= rightNumber;
                                break;
                            case ConstraintExpressionNode::BinaryOp::Greater:
                                result = leftNumber > rightNumber;
                                break;
                            case ConstraintExpressionNode::BinaryOp::GreaterEqual:
                                result = leftNumber >= rightNumber;
                                break;
                            default:
                                break;
                        }
                        *outValue = SCValue::FromBool(result);
                        return SC_OK;
                    }

                    std::wstring leftText;
                    std::wstring rightText;
                    if (!ValueComparator::TryAsText(left, &leftText) ||
                        !ValueComparator::TryAsText(right, &rightText))
                    {
                        return SC_E_TYPE_MISMATCH;
                    }

                    bool result = false;
                    switch (node.binaryOp)
                    {
                        case ConstraintExpressionNode::BinaryOp::Less:
                            result = leftText < rightText;
                            break;
                        case ConstraintExpressionNode::BinaryOp::LessEqual:
                            result = leftText <= rightText;
                            break;
                        case ConstraintExpressionNode::BinaryOp::Greater:
                            result = leftText > rightText;
                            break;
                        case ConstraintExpressionNode::BinaryOp::GreaterEqual:
                            result = leftText >= rightText;
                            break;
                        default:
                            break;
                    }
                    *outValue = SCValue::FromBool(result);
                    return SC_OK;
                }
                case ConstraintExpressionNode::BinaryOp::And:
                    *outValue = SCValue::FromBool(ValueComparator::IsTruthy(left) && ValueComparator::IsTruthy(right));
                    return SC_OK;
                case ConstraintExpressionNode::BinaryOp::Or:
                    *outValue = SCValue::FromBool(ValueComparator::IsTruthy(left) || ValueComparator::IsTruthy(right));
                    return SC_OK;
                default:
                    return SC_E_FAIL;
            }
        }
    }

    return SC_E_FAIL;
}

// ========================================================================
// QuoteSqlIdentifier
// (from source lines 3571-3588)
// ========================================================================

std::wstring QuoteSqlIdentifier(const std::wstring& identifier)
{
    std::wstring quoted;
    quoted.push_back(L'"');
    for (wchar_t ch : identifier)
    {
        if (ch == L'"')
        {
            quoted.push_back(L'"');
            quoted.push_back(L'"');
        } else
        {
            quoted.push_back(ch);
        }
    }
    quoted.push_back(L'"');
    return quoted;
}

// ========================================================================
// StorageCodec static methods
// (adapted from source lines 3432-3705)
// ========================================================================

ErrorCode StorageCodec::BindValue(SqliteStmt& stmt, int columnIndex, ValueKind valueKind, const SCValue& value)
{
    switch (valueKind)
    {
        case ValueKind::Null:
            return stmt.BindNull(columnIndex);
        case ValueKind::Int64: {
            std::int64_t v = 0;
            const ErrorCode rc = value.AsInt64(&v);
            return Failed(rc) ? rc : stmt.BindInt64(columnIndex, v);
        }
        case ValueKind::Double: {
            double v = 0.0;
            const ErrorCode rc = value.AsDouble(&v);
            return Failed(rc) ? rc : stmt.BindDouble(columnIndex, v);
        }
        case ValueKind::Bool: {
            bool v = false;
            const ErrorCode rc = value.AsBool(&v);
            return Failed(rc) ? rc : stmt.BindInt(columnIndex, v ? 1 : 0);
        }
        case ValueKind::String: {
            std::wstring v;
            const ErrorCode rc = value.AsStringCopy(&v);
            return Failed(rc) ? rc : stmt.BindText(columnIndex, v);
        }
        case ValueKind::RecordId: {
            RecordId v = 0;
            const ErrorCode rc = value.AsRecordId(&v);
            return Failed(rc) ? rc : stmt.BindInt64(columnIndex, static_cast<std::int64_t>(v));
        }
        case ValueKind::Enum: {
            std::wstring v;
            const ErrorCode rc = value.AsEnumCopy(&v);
            return Failed(rc) ? rc : stmt.BindText(columnIndex, v);
        }
        case ValueKind::Binary: {
            std::vector<std::uint8_t> v;
            const ErrorCode rc = value.AsBinaryCopy(&v);
            return Failed(rc) ? rc : stmt.BindBlob(columnIndex, v);
        }
        default:
            return SC_E_TYPE_MISMATCH;
    }
}

ErrorCode StorageCodec::ReadValue(SqliteStmt& stmt, int columnIndex, ValueKind valueKind, SCValue* outValue)
{
    if (outValue == nullptr)
    {
        return SC_E_POINTER;
    }

    switch (valueKind)
    {
        case ValueKind::Null:
            *outValue = SCValue::Null();
            return SC_OK;
        case ValueKind::Int64:
            *outValue = SCValue::FromInt64(stmt.ColumnInt64(columnIndex));
            return SC_OK;
        case ValueKind::Double:
            *outValue = SCValue::FromDouble(stmt.ColumnDouble(columnIndex));
            return SC_OK;
        case ValueKind::Bool:
            *outValue = SCValue::FromBool(stmt.ColumnBool(columnIndex));
            return SC_OK;
        case ValueKind::String:
            *outValue = SCValue::FromString(stmt.ColumnText(columnIndex));
            return SC_OK;
        case ValueKind::RecordId:
            *outValue = SCValue::FromRecordId(stmt.ColumnInt64(columnIndex));
            return SC_OK;
        case ValueKind::Enum:
            *outValue = SCValue::FromEnum(stmt.ColumnText(columnIndex));
            return SC_OK;
        case ValueKind::Binary:
            *outValue = SCValue::FromBinary(stmt.ColumnBlob(columnIndex));
            return SC_OK;
        default:
            *outValue = SCValue::Null();
            return SC_OK;
    }
}

ErrorCode StorageCodec::ReadColumnDef(SqliteStmt& stmt, int columnIndex, SCColumnDef* outDef)
{
    SCColumnDef def;
    def.displayName = stmt.ColumnText(columnIndex + 0);
    def.valueKind = static_cast<ValueKind>(stmt.ColumnInt(columnIndex + 1));
    def.columnKind = static_cast<ColumnKind>(stmt.ColumnInt(columnIndex + 2));
    def.nullable = stmt.ColumnBool(columnIndex + 3);
    def.editable = stmt.ColumnBool(columnIndex + 4);
    def.userDefined = stmt.ColumnBool(columnIndex + 5);
    def.indexed = stmt.ColumnBool(columnIndex + 6);
    def.participatesInCalc = stmt.ColumnBool(columnIndex + 7);
    def.unit = stmt.ColumnText(columnIndex + 8);
    def.referenceTable = stmt.ColumnText(columnIndex + 9);
    def.referenceStorageColumn = stmt.ColumnText(columnIndex + 10);
    def.referenceDisplayColumn = stmt.ColumnText(columnIndex + 11);

    // Read default value: column 12 = kind, column 13 = value
    const ValueKind defaultKind = static_cast<ValueKind>(stmt.ColumnInt(columnIndex + 12));
    ErrorCode rc = ReadValue(stmt, columnIndex + 13, defaultKind, &def.defaultValue);
    if (Failed(rc))
    {
        return rc;
    }

    *outDef = def;
    return SC_OK;
}

// ── Multi-index overloads (original per-column-index style) ─────────

void StorageCodec::BindValue(SqliteStmt& stmt,
                             int kindIndex, int intIndex, int doubleIndex,
                             int boolIndex, int textIndex, int blobIndex,
                             const SCValue& value)
{
    stmt.BindInt(kindIndex, ToSqliteValueKind(value.GetKind()));
    switch (value.GetKind())
    {
        case ValueKind::Null:
            stmt.BindNull(intIndex);
            stmt.BindNull(doubleIndex);
            stmt.BindNull(boolIndex);
            stmt.BindNull(textIndex);
            stmt.BindNull(blobIndex);
            break;
        case ValueKind::Int64: {
            std::int64_t v = 0;
            value.AsInt64(&v);
            stmt.BindInt64(intIndex, v);
            stmt.BindNull(doubleIndex);
            stmt.BindNull(boolIndex);
            stmt.BindNull(textIndex);
            stmt.BindNull(blobIndex);
            break;
        }
        case ValueKind::Double: {
            double v = 0.0;
            value.AsDouble(&v);
            stmt.BindNull(intIndex);
            stmt.BindDouble(doubleIndex, v);
            stmt.BindNull(boolIndex);
            stmt.BindNull(textIndex);
            stmt.BindNull(blobIndex);
            break;
        }
        case ValueKind::Bool: {
            bool v = false;
            value.AsBool(&v);
            stmt.BindNull(intIndex);
            stmt.BindNull(doubleIndex);
            stmt.BindInt(boolIndex, v ? 1 : 0);
            stmt.BindNull(textIndex);
            stmt.BindNull(blobIndex);
            break;
        }
        case ValueKind::String: {
            std::wstring v;
            value.AsStringCopy(&v);
            stmt.BindNull(intIndex);
            stmt.BindNull(doubleIndex);
            stmt.BindNull(boolIndex);
            stmt.BindText(textIndex, v);
            stmt.BindNull(blobIndex);
            break;
        }
        case ValueKind::Binary: {
            std::vector<std::uint8_t> v;
            value.AsBinaryCopy(&v);
            stmt.BindNull(intIndex);
            stmt.BindNull(doubleIndex);
            stmt.BindNull(boolIndex);
            stmt.BindNull(textIndex);
            stmt.BindBlob(blobIndex, v);
            break;
        }
        case ValueKind::RecordId: {
            RecordId id = 0;
            value.AsRecordId(&id);
            stmt.BindInt64(intIndex, id);
            stmt.BindNull(doubleIndex);
            stmt.BindNull(boolIndex);
            stmt.BindNull(textIndex);
            stmt.BindNull(blobIndex);
            break;
        }
        case ValueKind::Enum: {
            std::wstring text;
            value.AsEnumCopy(&text);
            stmt.BindNull(intIndex);
            stmt.BindNull(doubleIndex);
            stmt.BindNull(boolIndex);
            stmt.BindText(textIndex, text);
            stmt.BindNull(blobIndex);
            break;
        }
    }
}

SCValue StorageCodec::ReadValue(SqliteStmt& stmt,
                                int kindIndex, int intIndex, int doubleIndex,
                                int boolIndex, int textIndex, int blobIndex)
{
    const int kind = stmt.ColumnInt(kindIndex);
    switch (static_cast<ValueKind>(kind))
    {
        case ValueKind::Null:
            return SCValue::Null();
        case ValueKind::Int64:
            return SCValue::FromInt64(stmt.ColumnInt64(intIndex));
        case ValueKind::Double:
            return SCValue::FromDouble(stmt.ColumnDouble(doubleIndex));
        case ValueKind::Bool:
            return SCValue::FromBool(stmt.ColumnBool(boolIndex));
        case ValueKind::String:
            return SCValue::FromString(stmt.ColumnText(textIndex));
        case ValueKind::RecordId:
            return SCValue::FromRecordId(stmt.ColumnInt64(intIndex));
        case ValueKind::Enum:
            return SCValue::FromEnum(stmt.ColumnText(textIndex));
        case ValueKind::Binary:
            return SCValue::FromBinary(stmt.ColumnBlob(blobIndex));
        default:
            return SCValue::Null();
    }
}

SCColumnDef StorageCodec::ReadColumnDef(SqliteStmt& stmt,
                                        int displayNameIndex, int valueKindIndex, int columnKindIndex,
                                        int nullableIndex, int editableIndex, int userDefinedIndex,
                                        int indexedIndex, int participatesInCalcIndex, int unitIndex,
                                        int referenceTableIndex, int referenceStorageColumnIndex,
                                        int referenceDisplayColumnIndex, int defaultKindIndex,
                                        int defaultInt64Index, int defaultDoubleIndex,
                                        int defaultBoolIndex, int defaultTextIndex,
                                        int defaultBlobIndex)
{
    SCColumnDef def;
    def.displayName = stmt.ColumnText(displayNameIndex);
    def.valueKind = FromSqliteValueKind(stmt.ColumnInt(valueKindIndex));
    def.columnKind = FromSqliteColumnKind(stmt.ColumnInt(columnKindIndex));
    def.nullable = stmt.ColumnBool(nullableIndex);
    def.editable = stmt.ColumnBool(editableIndex);
    def.userDefined = stmt.ColumnBool(userDefinedIndex);
    def.indexed = stmt.ColumnBool(indexedIndex);
    def.participatesInCalc = stmt.ColumnBool(participatesInCalcIndex);
    def.unit = stmt.ColumnText(unitIndex);
    def.referenceTable = stmt.ColumnText(referenceTableIndex);
    def.referenceStorageColumn = stmt.ColumnText(referenceStorageColumnIndex);
    def.referenceDisplayColumn = stmt.ColumnText(referenceDisplayColumnIndex);
    def.defaultValue = ReadValue(stmt,
                                 defaultKindIndex,
                                 defaultInt64Index,
                                 defaultDoubleIndex,
                                 defaultBoolIndex,
                                 defaultTextIndex,
                                 defaultBlobIndex);
    return def;
}

// ========================================================================
// SqliteSchema out-of-line methods
// (from source lines 4651-4917)
// ========================================================================

ErrorCode SqliteSchema::GetColumnCount(std::int32_t* outCount)
{
    if (outCount == nullptr)
    {
        return SC_E_POINTER;
    }
    *outCount = static_cast<std::int32_t>(columns_.size());
    return SC_OK;
}

ErrorCode SqliteSchema::GetColumn(std::int32_t index, SCColumnDef* outDef)
{
    if (outDef == nullptr)
    {
        return SC_E_POINTER;
    }
    if (index < 0 || static_cast<std::size_t>(index) >= columns_.size())
    {
        return SC_E_INVALIDARG;
    }
    *outDef = columns_[static_cast<std::size_t>(index)].def;
    return SC_OK;
}

ErrorCode SqliteSchema::FindColumn(const wchar_t* name, SCColumnDef* outDef)
{
    if (name == nullptr)
    {
        return SC_E_INVALIDARG;
    }
    if (outDef == nullptr)
    {
        return SC_E_POINTER;
    }
    const auto it = columnsByName_.find(name);
    if (it == columnsByName_.end())
    {
        return SC_E_COLUMN_NOT_FOUND;
    }
    *outDef = it->second.def;
    return SC_OK;
}

ErrorCode SqliteSchema::GetSchemaSnapshot(SCTableSchemaSnapshot* outSnapshot)
{
    if (outSnapshot == nullptr)
    {
        return SC_E_POINTER;
    }

    outSnapshot->table.name = tableName_;
    outSnapshot->table.description = description_;
    outSnapshot->columns.clear();
    outSnapshot->constraints.clear();
    outSnapshot->indexes.clear();

    outSnapshot->columns.reserve(columns_.size());
    for (const auto& column : columns_)
    {
        outSnapshot->columns.push_back(column.def);
    }

    outSnapshot->constraints.reserve(constraints_.size());
    for (const auto& constraint : constraints_)
    {
        outSnapshot->constraints.push_back(constraint.def);
    }

    outSnapshot->indexes.reserve(indexes_.size());
    for (const auto& index : indexes_)
    {
        outSnapshot->indexes.push_back(index.def);
    }

    return SC_OK;
}

ErrorCode SqliteSchema::GetConstraintCount(std::int32_t* outCount)
{
    if (outCount == nullptr)
    {
        return SC_E_POINTER;
    }
    *outCount = static_cast<std::int32_t>(constraints_.size());
    return SC_OK;
}

ErrorCode SqliteSchema::GetConstraint(std::int32_t index, SCConstraintDef* outDef)
{
    if (outDef == nullptr)
    {
        return SC_E_POINTER;
    }
    if (index < 0 || static_cast<std::size_t>(index) >= constraints_.size())
    {
        return SC_E_INVALIDARG;
    }

    *outDef = constraints_[static_cast<std::size_t>(index)].def;
    return SC_OK;
}

ErrorCode SqliteSchema::FindConstraint(const wchar_t* name, SCConstraintDef* outDef)
{
    if (name == nullptr)
    {
        return SC_E_INVALIDARG;
    }
    if (outDef == nullptr)
    {
        return SC_E_POINTER;
    }

    const auto it = constraintsByName_.find(name);
    if (it == constraintsByName_.end())
    {
        return SC_E_CONSTRAINT_NOT_FOUND;
    }

    *outDef = it->second.def;
    return SC_OK;
}

ErrorCode SqliteSchema::GetIndexCount(std::int32_t* outCount)
{
    if (outCount == nullptr)
    {
        return SC_E_POINTER;
    }
    *outCount = static_cast<std::int32_t>(indexes_.size());
    return SC_OK;
}

ErrorCode SqliteSchema::GetIndex(std::int32_t index, SCIndexDef* outDef)
{
    if (outDef == nullptr)
    {
        return SC_E_POINTER;
    }
    if (index < 0 || static_cast<std::size_t>(index) >= indexes_.size())
    {
        return SC_E_INVALIDARG;
    }

    *outDef = indexes_[static_cast<std::size_t>(index)].def;
    return SC_OK;
}

ErrorCode SqliteSchema::FindIndex(const wchar_t* name, SCIndexDef* outDef)
{
    if (name == nullptr)
    {
        return SC_E_INVALIDARG;
    }
    if (outDef == nullptr)
    {
        return SC_E_POINTER;
    }

    const auto it = indexesByName_.find(name);
    if (it == indexesByName_.end())
    {
        return SC_E_INDEX_NOT_FOUND;
    }

    *outDef = it->second.def;
    return SC_OK;
}

ErrorCode SqliteSchema::AddColumn(const SCColumnDef& def)
{
    const ErrorCode validate = db_->ValidateColumnDefForSchema(this, def);
    if (Failed(validate))
    {
        return validate;
    }
    if (columnsByName_.contains(def.name))
    {
        return SC_E_COLUMN_EXISTS;
    }
    const ErrorCode persist = db_->PersistAddedColumn(this, def);
    return persist;
}

ErrorCode SqliteSchema::UpdateColumn(const SCColumnDef& def)
{
    const ErrorCode validate = db_->ValidateColumnDefForUpdate(this, def);
    if (Failed(validate))
    {
        return validate;
    }
    if (FindColumnDef(def.name) == nullptr)
    {
        return SC_E_COLUMN_NOT_FOUND;
    }

    const ErrorCode persist = db_->PersistUpdatedColumn(this, def);
    return persist;
}

ErrorCode SqliteSchema::RemoveColumn(const wchar_t* name)
{
    if (name == nullptr)
    {
        return SC_E_INVALIDARG;
    }
    if (FindColumnDef(name) == nullptr)
    {
        return SC_E_COLUMN_NOT_FOUND;
    }
    return db_->PersistRemovedColumn(this, name);
}

ErrorCode SqliteSchema::AddConstraint(const SCConstraintDef& def)
{
    const ErrorCode validate = db_->ValidateConstraintDefForSchema(this, def);
    if (Failed(validate))
    {
        return validate;
    }
    if (constraintsByName_.contains(def.name))
    {
        return SC_E_INVALIDARG;
    }
    return db_->PersistAddedConstraint(this, def);
}

ErrorCode SqliteSchema::RemoveConstraint(const wchar_t* name)
{
    if (name == nullptr)
    {
        return SC_E_INVALIDARG;
    }
    if (FindConstraintDef(name) == nullptr)
    {
        return SC_E_CONSTRAINT_NOT_FOUND;
    }
    return db_->PersistRemovedConstraint(this, name);
}

ErrorCode SqliteSchema::AddIndex(const SCIndexDef& def)
{
    const ErrorCode validate = db_->ValidateIndexDefForSchema(this, def);
    if (Failed(validate))
    {
        return validate;
    }
    if (indexesByName_.contains(def.name))
    {
        return SC_E_CONSTRAINT_VIOLATION;
    }
    return db_->PersistAddedIndex(this, def);
}

ErrorCode SqliteSchema::RemoveIndex(const wchar_t* name)
{
    if (name == nullptr)
    {
        return SC_E_INVALIDARG;
    }
    if (FindIndexDef(name) == nullptr)
    {
        return SC_E_INDEX_NOT_FOUND;
    }
    return db_->PersistRemovedIndex(this, name);
}

// ========================================================================
// SqliteRecord out-of-line methods
// (from source lines 4918-5149)
// ========================================================================

RecordId SqliteRecord::GetId() const noexcept
{
    return data_->id;
}

bool SqliteRecord::IsDeleted() const noexcept
{
    return data_->state == RecordState::Deleted;
}

VersionId SqliteRecord::GetLastModifiedVersion() const noexcept
{
    return data_->lastModifiedVersion;
}

ErrorCode SqliteRecord::ReadTypedValue(const wchar_t* name, SCValue* outValue)
{
    return GetValue(name, outValue);
}

ErrorCode SqliteRecord::ResolveValueStorage(const wchar_t* name, const SCValue** outValue) const
{
    if (name == nullptr)
    {
        return SC_E_INVALIDARG;
    }
    if (outValue == nullptr)
    {
        return SC_E_POINTER;
    }
    if (IsDeleted())
    {
        return SC_E_RECORD_DELETED;
    }

    const SCColumnDef* column = table_->Schema()->FindColumnDef(name);
    if (column == nullptr)
    {
        return SC_E_COLUMN_NOT_FOUND;
    }

    const auto it = data_->values.find(name);
    *outValue = (it != data_->values.end()) ? &it->second : &column->defaultValue;
    return (*outValue)->IsNull() ? SC_E_VALUE_IS_NULL : SC_OK;
}

ErrorCode SqliteRecord::GetValue(const wchar_t* name, SCValue* outValue)
{
    if (outValue == nullptr)
    {
        return SC_E_POINTER;
    }
    const SCValue* storage = nullptr;
    const ErrorCode rc = ResolveValueStorage(name, &storage);
    if (rc == SC_E_VALUE_IS_NULL)
    {
        *outValue = SCValue::Null();
        return rc;
    }
    if (Failed(rc))
    {
        return rc;
    }
    *outValue = *storage;
    return SC_OK;
}

ErrorCode SqliteRecord::SetValue(const wchar_t* name, const SCValue& value)
{
    if (name == nullptr)
    {
        return SC_E_INVALIDARG;
    }
    return db_->WriteValue(table_, data_, name, value);
}

ErrorCode SqliteRecord::GetInt64(const wchar_t* name, std::int64_t* outValue)
{
    SCValue value;
    const ErrorCode rc = ReadTypedValue(name, &value);
    if (Failed(rc))
    {
        return rc;
    }
    return value.AsInt64(outValue);
}

ErrorCode SqliteRecord::SetInt64(const wchar_t* name, std::int64_t value)
{
    return SetValue(name, SCValue::FromInt64(value));
}

ErrorCode SqliteRecord::GetDouble(const wchar_t* name, double* outValue)
{
    SCValue value;
    const ErrorCode rc = ReadTypedValue(name, &value);
    if (Failed(rc))
    {
        return rc;
    }
    return value.AsDouble(outValue);
}

ErrorCode SqliteRecord::SetDouble(const wchar_t* name, double value)
{
    return SetValue(name, SCValue::FromDouble(value));
}

ErrorCode SqliteRecord::GetBool(const wchar_t* name, bool* outValue)
{
    SCValue value;
    const ErrorCode rc = ReadTypedValue(name, &value);
    if (Failed(rc))
    {
        return rc;
    }
    return value.AsBool(outValue);
}

ErrorCode SqliteRecord::SetBool(const wchar_t* name, bool value)
{
    return SetValue(name, SCValue::FromBool(value));
}

ErrorCode SqliteRecord::GetString(const wchar_t* name, const wchar_t** outValue)
{
    const SCValue* storage = nullptr;
    const ErrorCode rc = ResolveValueStorage(name, &storage);
    if (Failed(rc))
    {
        return rc;
    }
    return storage->AsString(outValue);
}

ErrorCode SqliteRecord::GetStringCopy(const wchar_t* name, std::wstring* outValue)
{
    SCValue value;
    const ErrorCode rc = ReadTypedValue(name, &value);
    if (Failed(rc))
    {
        return rc;
    }
    return value.AsStringCopy(outValue);
}

ErrorCode SqliteRecord::SetString(const wchar_t* name, const wchar_t* value)
{
    return SetValue(name, value == nullptr ? SCValue::Null() : SCValue::FromString(value));
}

ErrorCode SqliteRecord::GetBinary(const wchar_t* name, const std::uint8_t** outValue, std::size_t* outSize)
{
    const SCValue* storage = nullptr;
    const ErrorCode rc = ResolveValueStorage(name, &storage);
    if (Failed(rc))
    {
        return rc;
    }
    return storage->AsBinary(outValue, outSize);
}

ErrorCode SqliteRecord::GetBinaryCopy(const wchar_t* name, std::vector<std::uint8_t>* outValue)
{
    SCValue value;
    const ErrorCode rc = ReadTypedValue(name, &value);
    if (Failed(rc))
    {
        return rc;
    }
    return value.AsBinaryCopy(outValue);
}

ErrorCode SqliteRecord::SetBinary(const wchar_t* name, const std::uint8_t* value, std::size_t size)
{
    if (value == nullptr && size > 0)
    {
        return SC_E_POINTER;
    }
    std::vector<std::uint8_t> bytes;
    if (size > 0)
    {
        bytes.assign(value, value + size);
    }
    return SetValue(name, SCValue::FromBinary(std::move(bytes)));
}

ErrorCode SqliteRecord::GetRef(const wchar_t* name, RecordId* outValue)
{
    if (outValue == nullptr)
    {
        return SC_E_POINTER;
    }

    const SCColumnDef* column = table_->Schema()->FindColumnDef(name);
    if (column == nullptr)
    {
        return SC_E_COLUMN_NOT_FOUND;
    }

    SCValue value;
    const ErrorCode rc = ReadTypedValue(name, &value);
    if (Failed(rc))
    {
        return rc;
    }

    return db_->ResolveRelationTargetRecordId(*column, value, outValue);
}

ErrorCode SqliteRecord::SetRef(const wchar_t* name, RecordId value)
{
    if (name == nullptr)
    {
        return SC_E_INVALIDARG;
    }

    const SCColumnDef* column = table_->Schema()->FindColumnDef(name);
    if (column == nullptr)
    {
        return SC_E_COLUMN_NOT_FOUND;
    }

    SCValue storedValue;
    const ErrorCode rc = db_->ResolveRelationStoredValue(*column, value, &storedValue);
    if (Failed(rc))
    {
        return rc;
    }
    return SetValue(name, storedValue);
}

// ========================================================================
// SqliteTable out-of-line methods
// (from source lines 5150-5255)
// ========================================================================

ErrorCode SqliteTable::GetRecord(RecordId id, SCRecordPtr& outRecord)
{
    auto data = FindRecordData(id);
    if (!data)
    {
        return SC_E_RECORD_NOT_FOUND;
    }
    outRecord = MakeRecord(data);
    return SC_OK;
}

ErrorCode SqliteTable::CreateRecord(SCRecordPtr& outRecord)
{
    if (!db_->HasActiveEdit())
    {
        return SC_E_NO_ACTIVE_EDIT;
    }

    std::int32_t columnCount = 0;
    const ErrorCode columnCountRc = schema_->GetColumnCount(&columnCount);
    if (Failed(columnCountRc))
    {
        return columnCountRc;
    }
    if (columnCount <= 0)
    {
        return SC_E_SCHEMA_VIOLATION;
    }

    auto data = std::make_shared<SqliteRecordData>(db_->AllocateRecordId());
    records_.emplace(data->id, data);
    db_->RecordCreate(this, data);
    const ErrorCode constraintRc = db_->ValidateTableConstraints(this, data);
    if (Failed(constraintRc))
    {
        db_->RemoveAllJournalEntriesForRecord(name_, data->id);
        records_.erase(data->id);
        return constraintRc;
    }
    db_->MarkReferenceIndexDirty();
    outRecord = MakeRecord(data);
    return SC_OK;
}

ErrorCode SqliteTable::DeleteRecord(RecordId id)
{
    auto data = FindRecordData(id);
    if (!data)
    {
        return SC_E_RECORD_NOT_FOUND;
    }
    return db_->DeleteRecord(this, data);
}

ErrorCode SqliteTable::EnumerateRecords(SCRecordCursorPtr& outCursor)
{
    std::vector<SCRecordPtr> records;
    for (const auto& [_, data] : records_)
    {
        if (data->state == RecordState::Alive)
        {
            records.push_back(MakeRecord(data));
        }
    }
    outCursor = SCMakeRef<SqliteRecordCursor>(std::move(records));
    return SC_OK;
}

ErrorCode SqliteTable::FindRecords(const SCQueryCondition& condition, SCRecordCursorPtr& outCursor)
{
    QueryPlan legacyPlan;
    const ErrorCode bridgeRc = SCQueryBridge::BuildPlanFromLegacyFindRecords(name_, condition, &legacyPlan);
    if (Failed(bridgeRc))
    {
        return bridgeRc;
    }

    auto planner = CreateDefaultQueryPlanner();
    if (!planner)
    {
        return SC_E_NOTIMPL;
    }

    QueryPlan executablePlan;
    const ErrorCode planRc = planner->BuildPlan(legacyPlan.target,
                                                legacyPlan.conditionGroups,
                                                legacyPlan.conditionGroupLogic,
                                                legacyPlan.orderBy,
                                                legacyPlan.page,
                                                legacyPlan.hints,
                                                legacyPlan.constraints,
                                                &executablePlan);
    if (Failed(planRc))
    {
        return planRc;
    }

    QueryExecutionContext context;
    context.backendKind = QueryBackendKind::SQLite;
    context.database = db_;
    context.backendHandle = db_;
    context.resultCursor = &outCursor;

    QueryExecutionResult executionResult;
    return ExecuteQueryPlan(executablePlan, context, &executionResult);
}

} // namespace StableCore::Storage
