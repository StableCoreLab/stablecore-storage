#include "SCSchemaTableImport.h"

#include <algorithm>
#include <utility>

#include <QRegularExpression>

namespace sc = StableCore::Storage;

namespace StableCore::Storage::Editor
{
    namespace
    {

        QString UnescapeCppString(const QString& text)
        {
            QString result;
            result.reserve(text.size());
            for (qsizetype index = 0; index < text.size(); ++index)
            {
                const QChar ch = text[index];
                if (ch != QLatin1Char('\\') || index + 1 >= text.size())
                {
                    result.push_back(ch);
                    continue;
                }

                const QChar next = text[++index];
                switch (next.unicode())
                {
                    case '\\':
                        result.push_back(QLatin1Char('\\'));
                        break;
                    case '"':
                        result.push_back(QLatin1Char('"'));
                        break;
                    case 'n':
                        result.push_back(QLatin1Char('\n'));
                        break;
                    case 'r':
                        result.push_back(QLatin1Char('\r'));
                        break;
                    case 't':
                        result.push_back(QLatin1Char('\t'));
                        break;
                    default:
                        result.push_back(next);
                        break;
                }
            }
            return result;
        }

        QString EscapeForMessage(const QString& text)
        {
            QString escaped = text;
            escaped.replace(QLatin1Char('\r'), QStringLiteral(" "));
            escaped.replace(QLatin1Char('\n'), QStringLiteral(" "));
            return escaped.trimmed();
        }

        bool ParseQuotedString(const QString& text, QString* outValue)
        {
            if (outValue == nullptr)
            {
                return false;
            }

            const QRegularExpression quotedRegex(
                QStringLiteral("^\\s*\"((?:\\\\.|[^\"])*)\"\\s*$"));
            const auto match = quotedRegex.match(text);
            if (!match.hasMatch())
            {
                return false;
            }

            *outValue = UnescapeCppString(match.captured(1));
            return true;
        }

        bool ParseQuotedList(const QString& text, QStringList* outValues,
                             QString* outError)
        {
            if (outValues == nullptr)
            {
                return false;
            }

            outValues->clear();
            const QRegularExpression quotedRegex(
                QStringLiteral("\"((?:\\\\.|[^\"])*)\""));
            auto iterator = quotedRegex.globalMatch(text);
            while (iterator.hasNext())
            {
                const auto match = iterator.next();
                outValues->push_back(UnescapeCppString(match.captured(1)));
            }

            if (outValues->isEmpty())
            {
                if (outError != nullptr)
                {
                    *outError = QStringLiteral("Index column list is empty.");
                }
                return false;
            }

            return true;
        }

        bool ParseTypeToken(const QString& token, sc::ValueKind* outKind,
                            QStringList* outWarnings, QString* outError)
        {
            if (outKind == nullptr)
            {
                return false;
            }

            if (token == QStringLiteral("Int64"))
            {
                *outKind = sc::ValueKind::Int64;
                return true;
            }
            if (token == QStringLiteral("Double"))
            {
                *outKind = sc::ValueKind::Double;
                return true;
            }
            if (token == QStringLiteral("Bool"))
            {
                *outKind = sc::ValueKind::Bool;
                return true;
            }
            if (token == QStringLiteral("String"))
            {
                *outKind = sc::ValueKind::String;
                return true;
            }
            if (token == QStringLiteral("RecordId"))
            {
                *outKind = sc::ValueKind::RecordId;
                return true;
            }
            if (token == QStringLiteral("Enum"))
            {
                *outKind = sc::ValueKind::Enum;
                return true;
            }
            if (token == QStringLiteral("Int32"))
            {
                *outKind = sc::ValueKind::Int64;
                return true;
            }
            if (token == QStringLiteral("Null"))
            {
                *outKind = sc::ValueKind::Null;
                return true;
            }

            if (outError != nullptr)
            {
                *outError =
                    QStringLiteral("Unsupported SCType token: ") + token;
            }
            return false;
        }

        sc::SCValue NeutralDefaultValue(sc::ValueKind kind)
        {
            switch (kind)
            {
                case sc::ValueKind::Int64:
                    return sc::SCValue::FromInt64(0);
                case sc::ValueKind::Double:
                    return sc::SCValue::FromDouble(0.0);
                case sc::ValueKind::Bool:
                    return sc::SCValue::FromBool(false);
                case sc::ValueKind::String:
                    return sc::SCValue::FromString(L"");
                case sc::ValueKind::RecordId:
                    return sc::SCValue::FromRecordId(0);
                case sc::ValueKind::Enum:
                    return sc::SCValue::FromEnum(L"");
                case sc::ValueKind::Null:
                default:
                    return sc::SCValue::Null();
            }
        }

        bool ParseDefaultValueLiteral(const QString& text, sc::ValueKind kind,
                                      sc::SCValue* outValue, QString* outError)
        {
            if (outValue == nullptr)
            {
                return false;
            }

            const QString trimmed = text.trimmed();
            if (trimmed.isEmpty())
            {
                if (outError != nullptr)
                {
                    *outError = QStringLiteral("Default value is empty.");
                }
                return false;
            }

            switch (kind)
            {
                case sc::ValueKind::Int64: {
                    bool ok = false;
                    const qlonglong value = trimmed.toLongLong(&ok);
                    if (!ok)
                    {
                        if (outError != nullptr)
                        {
                            *outError = QStringLiteral(
                                            "Invalid Int64 default value: ") +
                                        EscapeForMessage(trimmed);
                        }
                        return false;
                    }
                    *outValue = sc::SCValue::FromInt64(
                        static_cast<std::int64_t>(value));
                    return true;
                }
                case sc::ValueKind::Double: {
                    bool ok = false;
                    const double value = trimmed.toDouble(&ok);
                    if (!ok)
                    {
                        if (outError != nullptr)
                        {
                            *outError = QStringLiteral(
                                            "Invalid Double default value: ") +
                                        EscapeForMessage(trimmed);
                        }
                        return false;
                    }
                    *outValue = sc::SCValue::FromDouble(value);
                    return true;
                }
                case sc::ValueKind::Bool: {
                    if (trimmed.compare(QStringLiteral("true"),
                                        Qt::CaseInsensitive) == 0 ||
                        trimmed == QStringLiteral("1"))
                    {
                        *outValue = sc::SCValue::FromBool(true);
                        return true;
                    }
                    if (trimmed.compare(QStringLiteral("false"),
                                        Qt::CaseInsensitive) == 0 ||
                        trimmed == QStringLiteral("0"))
                    {
                        *outValue = sc::SCValue::FromBool(false);
                        return true;
                    }
                    if (outError != nullptr)
                    {
                        *outError =
                            QStringLiteral("Invalid Bool default value: ") +
                            EscapeForMessage(trimmed);
                    }
                    return false;
                }
                case sc::ValueKind::String:
                case sc::ValueKind::Enum: {
                    QString parsed;
                    if (!ParseQuotedString(trimmed, &parsed))
                    {
                        if (outError != nullptr)
                        {
                            *outError =
                                QStringLiteral(
                                    "Text defaults must use quoted text.") +
                                QStringLiteral(" Received: ") +
                                EscapeForMessage(trimmed);
                        }
                        return false;
                    }
                    if (kind == sc::ValueKind::String)
                    {
                        *outValue =
                            sc::SCValue::FromString(parsed.toStdWString());
                    } else
                    {
                        *outValue =
                            sc::SCValue::FromEnum(parsed.toStdWString());
                    }
                    return true;
                }
                case sc::ValueKind::RecordId: {
                    bool ok = false;
                    const qlonglong value = trimmed.toLongLong(&ok);
                    if (!ok)
                    {
                        if (outError != nullptr)
                        {
                            *outError =
                                QStringLiteral(
                                    "Invalid RecordId default value: ") +
                                EscapeForMessage(trimmed);
                        }
                        return false;
                    }
                    *outValue = sc::SCValue::FromRecordId(
                        static_cast<sc::RecordId>(value));
                    return true;
                }
                case sc::ValueKind::Null:
                case sc::ValueKind::Binary:
                default:
                    if (outError != nullptr)
                    {
                        *outError = QStringLiteral(
                            "Default values are not supported "
                            "for this column type.");
                    }
                    return false;
            }
        }

        bool ParseColumnDeclaration(const QString& line,
                                    sc::SCColumnDef* outColumn,
                                    QStringList* outWarnings, QString* outError)
        {
            if (outColumn == nullptr)
            {
                return false;
            }

            const QRegularExpression regex(
                QStringLiteral("^\\s*\\.Column\\(\\s*\"((?:\\\\.|[^\"])*)\"\\s*"
                               ",\\s*SCType::([A-Za-z0-9_]+)\\s*\\)\\s*$"));
            const auto match = regex.match(line);
            if (!match.hasMatch())
            {
                if (outError != nullptr)
                {
                    *outError = QStringLiteral("Invalid column declaration: ") +
                                EscapeForMessage(line);
                }
                return false;
            }

            sc::SCColumnDef column;
            column.name = match.captured(1).toStdWString();
            column.displayName = column.name;
            column.columnKind = sc::ColumnKind::Fact;
            column.editable = true;
            column.userDefined = true;
            column.nullable = true;
            column.indexed = false;
            column.participatesInCalc = false;

            if (!ParseTypeToken(match.captured(2), &column.valueKind,
                                outWarnings, outError))
            {
                return false;
            }

            *outColumn = std::move(column);
            return true;
        }

        bool ParseIndexDeclaration(const QString& line, QString* outName,
                                   QStringList* outColumns, QString* outError)
        {
            if (outName == nullptr || outColumns == nullptr)
            {
                return false;
            }

            const QRegularExpression sameLineRegex(
                QStringLiteral("^\\s*\\.Index\\(\\s*\"((?:\\\\.|[^\"])*)\"\\s*"
                               "\\)\\.Columns\\((.*)\\)\\s*;?\\s*$"));
            const auto sameLineMatch = sameLineRegex.match(line);
            if (sameLineMatch.hasMatch())
            {
                *outName = UnescapeCppString(sameLineMatch.captured(1));
                return ParseQuotedList(sameLineMatch.captured(2), outColumns,
                                       outError);
            }

            const QRegularExpression indexOnlyRegex(QStringLiteral(
                "^\\s*\\.Index\\(\\s*\"((?:\\\\.|[^\"])*)\"\\s*\\)\\s*$"));
            const auto indexOnlyMatch = indexOnlyRegex.match(line);
            if (!indexOnlyMatch.hasMatch())
            {
                return false;
            }

            *outName = UnescapeCppString(indexOnlyMatch.captured(1));
            return true;
        }

        void ApplyNeutralDefaultIfNeeded(sc::SCColumnDef* column)
        {
            if (column == nullptr)
            {
                return;
            }
            if (!column->nullable && column->defaultValue.IsNull())
            {
                column->defaultValue = NeutralDefaultValue(column->valueKind);
            }
        }

    }  // namespace

    bool ParseSchemaTableDescription(const QString& text,
                                     SCSchemaTableImportResult* outResult,
                                     QString* outError)
    {
        if (outResult == nullptr)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Output result is null.");
            }
            return false;
        }

        *outResult = SCSchemaTableImportResult{};
        if (text.trimmed().isEmpty())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Schema text is empty.");
            }
            return false;
        }

        const QRegularExpression macroRegex(QStringLiteral(
            "\\bSC_SCHEMA_TABLE\\(\\s*([A-Za-z_][A-Za-z0-9_]*)\\s*\\)"));
        const auto macroMatch = macroRegex.match(text);
        if (!macroMatch.hasMatch())
        {
            if (outError != nullptr)
            {
                *outError =
                    QStringLiteral("SC_SCHEMA_TABLE(...) macro was not found.");
            }
            return false;
        }

        outResult->tableMacroName = macroMatch.captured(1);
        outResult->tableName = outResult->tableMacroName;

        QStringList lines = text.split(
            QRegularExpression(QStringLiteral("\\r?\\n")), Qt::KeepEmptyParts);

        bool inBlock = false;
        bool haveCurrentColumn = false;
        sc::SCColumnDef currentColumn;
        bool haveCurrentIndex = false;
        SCSchemaTableImportIndex currentIndex;

        const auto flushColumn = [&]() {
            if (!haveCurrentColumn)
            {
                return;
            }
            ApplyNeutralDefaultIfNeeded(&currentColumn);
            outResult->columns.push_back(std::move(currentColumn));
            currentColumn = sc::SCColumnDef{};
            haveCurrentColumn = false;
        };

        const auto flushIndex = [&]() {
            if (!haveCurrentIndex)
            {
                return;
            }
            if (!currentIndex.columns.isEmpty())
            {
                outResult->indexes.push_back(currentIndex);
            }
            currentIndex = SCSchemaTableImportIndex{};
            haveCurrentIndex = false;
        };

        for (QString rawLine : lines)
        {
            QString line = rawLine.trimmed();
            if (line.isEmpty() || line.startsWith(QStringLiteral("//")))
            {
                continue;
            }

            if (!inBlock)
            {
                if (!macroRegex.match(line).hasMatch())
                {
                    continue;
                }
                inBlock = true;
                continue;
            }

            if (line == QStringLiteral("{"))
            {
                continue;
            }

            if (line.startsWith(QStringLiteral("}")))
            {
                break;
            }

            const QRegularExpression tableRegex(QStringLiteral(
                "^\\s*Table\\(\\s*\"((?:\\\\.|[^\"])*)\"\\s*\\)\\s*$"));
            const auto tableMatch = tableRegex.match(line);
            if (tableMatch.hasMatch())
            {
                const QString tableName =
                    UnescapeCppString(tableMatch.captured(1));
                if (tableName.compare(outResult->tableMacroName,
                                      Qt::CaseInsensitive) != 0)
                {
                    outResult->warnings.push_back(QStringLiteral(
                        "Table(\"...\") name differs from the "
                        "SC_SCHEMA_TABLE(...) macro name; the macro name "
                        "will be used as the created table name."));
                }
                continue;
            }

            const QRegularExpression descriptionRegex(
                QStringLiteral("^\\s*\\.Description\\(\\s*\"((?:\\\\.|[^\"])*)"
                               "\"\\s*\\)\\s*;?\\s*$"));
            const auto descriptionMatch = descriptionRegex.match(line);
            if (descriptionMatch.hasMatch())
            {
                const QString description =
                    UnescapeCppString(descriptionMatch.captured(1));
                if (haveCurrentColumn)
                {
                    currentColumn.displayName = description.toStdWString();
                } else
                {
                    outResult->tableDescription = description;
                }
                continue;
            }

            const QRegularExpression primaryKeyRegex(
                QStringLiteral("^\\s*\\.PrimaryKey\\(\\s*\"((?:\\\\.|[^\"])*)"
                               "\"\\s*\\)\\s*;?\\s*$"));
            const auto primaryKeyMatch = primaryKeyRegex.match(line);
            if (primaryKeyMatch.hasMatch())
            {
                outResult->primaryKeyColumnName =
                    UnescapeCppString(primaryKeyMatch.captured(1));
                continue;
            }

            if (line.startsWith(QStringLiteral(".Column(")))
            {
                flushColumn();
                flushIndex();
                if (!ParseColumnDeclaration(line, &currentColumn,
                                            &outResult->warnings, outError))
                {
                    return false;
                }
                haveCurrentColumn = true;
                continue;
            }

            QString indexName;
            QStringList indexColumns;
            if (ParseIndexDeclaration(line, &indexName, &indexColumns,
                                      outError))
            {
                flushColumn();
                if (!indexName.isEmpty())
                {
                    flushIndex();
                    currentIndex.name = indexName;
                    haveCurrentIndex = true;
                    if (!indexColumns.isEmpty())
                    {
                        currentIndex.columns = indexColumns;
                        flushIndex();
                    }
                } else if (!indexColumns.isEmpty())
                {
                    currentIndex.columns = indexColumns;
                    flushIndex();
                }
                continue;
            }

            if (haveCurrentColumn)
            {
                const QRegularExpression defaultRegex(QStringLiteral(
                    "^\\s*\\.Default\\(\\s*(.*)\\s*\\)\\s*;?\\s*$"));
                const auto defaultMatch = defaultRegex.match(line);
                if (defaultMatch.hasMatch())
                {
                    sc::SCValue defaultValue;
                    if (!ParseDefaultValueLiteral(defaultMatch.captured(1),
                                                  currentColumn.valueKind,
                                                  &defaultValue, outError))
                    {
                        return false;
                    }
                    currentColumn.defaultValue = std::move(defaultValue);
                    continue;
                }

                const QRegularExpression notNullRegex(
                    QStringLiteral("^\\s*\\.NotNull\\(\\)\\s*;?\\s*$"));
                if (notNullRegex.match(line).hasMatch())
                {
                    currentColumn.nullable = false;
                    continue;
                }

                const QRegularExpression refRegex(QStringLiteral(
                    "^\\s*\\.Ref\\(\\s*\"((?:\\\\.|[^\"])*)\"\\s*,\\s*\"((?:"
                    "\\\\.|[^\"])*)\"\\s*\\)\\s*;?\\s*$"));
                const auto refMatch = refRegex.match(line);
                if (refMatch.hasMatch())
                {
                    currentColumn.columnKind = sc::ColumnKind::Relation;
                    currentColumn.valueKind = sc::ValueKind::RecordId;
                    currentColumn.referenceTable =
                        UnescapeCppString(refMatch.captured(1)).toStdWString();
                    continue;
                }
            }

            if (haveCurrentIndex &&
                line.startsWith(QStringLiteral(".Columns(")))
            {
                if (!ParseQuotedList(
                        line.mid(QStringLiteral(".Columns").size()),
                        &indexColumns, outError))
                {
                    return false;
                }
                currentIndex.columns = indexColumns;
                flushIndex();
                continue;
            }

            if (line.startsWith(QStringLiteral(".")))
            {
                outResult->warnings.push_back(
                    QStringLiteral("Ignored schema token: ") +
                    EscapeForMessage(line));
                continue;
            }
        }

        flushColumn();
        flushIndex();

        if (outResult->tableName.trimmed().isEmpty())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Table name is missing.");
            }
            return false;
        }

        if (!outResult->tableDescription.trimmed().isEmpty())
        {
            outResult->warnings.push_back(
                QStringLiteral("Table description is parsed but not stored by "
                               "the current create-table flow."));
        }

        if (outResult->primaryKeyColumnName.trimmed().isEmpty())
        {
            outResult->warnings.push_back(
                QStringLiteral("No explicit primary key was imported."));
        } else
        {
            const auto columnIt = std::find_if(
                outResult->columns.begin(), outResult->columns.end(),
                [&outResult](const sc::SCColumnDef& column) {
                    return QString::fromStdWString(column.name)
                               .compare(outResult->primaryKeyColumnName,
                                        Qt::CaseInsensitive) == 0;
                });
            if (columnIt == outResult->columns.end())
            {
                outResult->warnings.push_back(
                    QStringLiteral("Primary key column was not found in the "
                                   "column list."));
            } else
            {
                columnIt->nullable = false;
                ApplyNeutralDefaultIfNeeded(&*columnIt);
                outResult->warnings.push_back(
                    QStringLiteral("Primary key was imported as a non-null "
                                   "column hint only."));
            }
        }

        for (const SCSchemaTableImportIndex& index : outResult->indexes)
        {
            if (index.columns.isEmpty())
            {
                continue;
            }

            if (index.columns.size() > 1)
            {
                outResult->warnings.push_back(
                    QStringLiteral("Composite indexes are imported as "
                                   "per-column indexed hints only."));
            }

            for (const QString& columnName : index.columns)
            {
                const auto columnIt = std::find_if(
                    outResult->columns.begin(), outResult->columns.end(),
                    [&columnName](const sc::SCColumnDef& column) {
                        return QString::fromStdWString(column.name)
                                   .compare(columnName, Qt::CaseInsensitive) ==
                               0;
                    });
                if (columnIt == outResult->columns.end())
                {
                    outResult->warnings.push_back(
                        QStringLiteral("Index column not found: ") +
                        columnName);
                    continue;
                }
                columnIt->indexed = true;
            }
        }

        if (!outResult->indexes.isEmpty())
        {
            outResult->warnings.push_back(
                QStringLiteral("Index names are not stored by the current "
                               "create-table flow; indexed columns were "
                               "imported as single-column hints."));
        }

        if (outResult->columns.isEmpty())
        {
            if (outError != nullptr)
            {
                *outError =
                    QStringLiteral("No column declarations were found.");
            }
            return false;
        }

        return true;
    }

}  // namespace StableCore::Storage::Editor
