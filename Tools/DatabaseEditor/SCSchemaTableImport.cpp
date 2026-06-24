#include "SCSchemaTableImport.h"

#include <cstdint>
#include <algorithm>
#include <utility>
#include <vector>

#include <QRegularExpression>

#include "SCBinaryUtils.h"

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
                    *outError = QStringLiteral("Quoted value list is empty.");
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
            if (token == QStringLiteral("Binary"))
            {
                *outKind = sc::ValueKind::Binary;
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
                case sc::ValueKind::Binary:
                    return sc::SCValue::FromBinary(std::vector<std::uint8_t>{});
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
                case sc::ValueKind::Binary: {
                    std::vector<std::uint8_t> bytes;
                    if (!ParseBinaryHex(trimmed, &bytes, outError))
                    {
                        return false;
                    }
                    *outValue = sc::SCValue::FromBinary(std::move(bytes));
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

        bool ApplyDescendingColumns(
            const QStringList& descendingColumns,
            QVector<sc::SCIndexColumnDef>* columns, QString* outError)
        {
            if (columns == nullptr)
            {
                return false;
            }

            for (const QString& descendingColumn : descendingColumns)
            {
                bool found = false;
                for (sc::SCIndexColumnDef& indexColumn : *columns)
                {
                    if (QString::fromStdWString(indexColumn.columnName)
                            .compare(descendingColumn, Qt::CaseInsensitive) == 0)
                    {
                        indexColumn.descending = true;
                        found = true;
                        break;
                    }
                }

                if (!found)
                {
                    if (outError != nullptr)
                    {
                        *outError = QStringLiteral(
                                        "Descending index column not found: ") +
                                    descendingColumn;
                    }
                    return false;
                }
            }

            return true;
        }

        bool ParseIndexColumns(const QString& text,
                               QVector<sc::SCIndexColumnDef>* outColumns,
                               QString* outError)
        {
            if (outColumns == nullptr)
            {
                return false;
            }

            QStringList columns;
            if (!ParseQuotedList(text, &columns, outError))
            {
                return false;
            }

            outColumns->clear();
            outColumns->reserve(columns.size());
            for (const QString& column : columns)
            {
                outColumns->push_back(
                    sc::SCIndexColumnDef{column.toStdWString(), false});
            }
            return true;
        }

        bool ParseIndexDeclaration(const QString& line, QString* outName,
                                   QVector<sc::SCIndexColumnDef>* outColumns,
                                   QString* outError)
        {
            if (outName == nullptr || outColumns == nullptr)
            {
                return false;
            }

            const QRegularExpression sameLineRegex(
                QStringLiteral("^\\s*\\.Index\\(\\s*\"((?:\\\\.|[^\"])*)\"\\s*"
                               "\\)\\.Columns\\((.*?)\\)"
                               "(?:\\.Desc\\((.*)\\))?\\s*;?\\s*$"));
            const auto sameLineMatch = sameLineRegex.match(line);
            if (sameLineMatch.hasMatch())
            {
                *outName = UnescapeCppString(sameLineMatch.captured(1));
                if (!ParseIndexColumns(sameLineMatch.captured(2), outColumns,
                                       outError))
                {
                    return false;
                }

                if (!sameLineMatch.captured(3).isEmpty())
                {
                    QStringList descendingColumns;
                    if (!ParseQuotedList(sameLineMatch.captured(3),
                                         &descendingColumns, outError))
                    {
                        return false;
                    }
                    return ApplyDescendingColumns(descendingColumns, outColumns,
                                                  outError);
                }
                return true;
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

        bool ParseConstraintKindToken(const QString& token,
                                      sc::SCConstraintKind* outKind,
                                      QString* outError)
        {
            if (outKind == nullptr)
            {
                return false;
            }

            if (token.compare(QStringLiteral("PrimaryKey"),
                              Qt::CaseInsensitive) == 0)
            {
                *outKind = sc::SCConstraintKind::PrimaryKey;
                return true;
            }
            if (token.compare(QStringLiteral("Unique"),
                              Qt::CaseInsensitive) == 0)
            {
                *outKind = sc::SCConstraintKind::Unique;
                return true;
            }
            if (token.compare(QStringLiteral("ForeignKey"),
                              Qt::CaseInsensitive) == 0)
            {
                *outKind = sc::SCConstraintKind::ForeignKey;
                return true;
            }
            if (token.compare(QStringLiteral("Check"),
                              Qt::CaseInsensitive) == 0)
            {
                *outKind = sc::SCConstraintKind::Check;
                return true;
            }

            if (outError != nullptr)
            {
                *outError = QStringLiteral("Unsupported constraint kind: ") +
                            token;
            }
            return false;
        }

        bool ParseConstraintActionToken(const QString& token,
                                        sc::SCForeignKeyAction* outAction,
                                        QString* outError)
        {
            if (outAction == nullptr)
            {
                return false;
            }

            if (token.compare(QStringLiteral("Restrict"),
                              Qt::CaseInsensitive) == 0)
            {
                *outAction = sc::SCForeignKeyAction::Restrict;
                return true;
            }
            if (token.compare(QStringLiteral("NoAction"),
                              Qt::CaseInsensitive) == 0)
            {
                *outAction = sc::SCForeignKeyAction::NoAction;
                return true;
            }
            if (token.compare(QStringLiteral("Cascade"),
                              Qt::CaseInsensitive) == 0)
            {
                *outAction = sc::SCForeignKeyAction::Cascade;
                return true;
            }
            if (token.compare(QStringLiteral("SetNull"),
                              Qt::CaseInsensitive) == 0)
            {
                *outAction = sc::SCForeignKeyAction::SetNull;
                return true;
            }
            if (token.compare(QStringLiteral("SetDefault"),
                              Qt::CaseInsensitive) == 0)
            {
                *outAction = sc::SCForeignKeyAction::SetDefault;
                return true;
            }

            if (outError != nullptr)
            {
                *outError = QStringLiteral("Unsupported foreign key action: ") +
                            token;
            }
            return false;
        }

        bool ParseConstraintCommentStart(const QString& comment,
                                        sc::SCConstraintDef* outConstraint,
                                        QString* outError)
        {
            if (outConstraint == nullptr)
            {
                return false;
            }

            const QRegularExpression regex(QStringLiteral(
                "^\\.Constraint\\(\\s*\"((?:\\\\.|[^\"])*)\"\\s*,\\s*"
                "([A-Za-z_][A-Za-z0-9_]*)\\s*\\)\\s*$"));
            const auto match = regex.match(comment);
            if (!match.hasMatch())
            {
                if (outError != nullptr)
                {
                    *outError = QStringLiteral("Invalid constraint comment: ") +
                                EscapeForMessage(comment);
                }
                return false;
            }

            sc::SCConstraintDef constraint;
            constraint.name = UnescapeCppString(match.captured(1)).toStdWString();
            if (!ParseConstraintKindToken(match.captured(2), &constraint.kind,
                                          outError))
            {
                return false;
            }
            constraint.sourceKind = sc::SCSchemaSourceKind::Explicit;
            *outConstraint = std::move(constraint);
            return true;
        }

        bool ParseConstraintCommentContinuation(
            const QString& comment, sc::SCConstraintDef* constraint,
            QString* outError)
        {
            if (constraint == nullptr)
            {
                return false;
            }

            if (comment.startsWith(QStringLiteral(".Columns(")))
            {
                QStringList columns;
                if (!ParseQuotedList(comment.mid(QStringLiteral(".Columns").size()),
                                     &columns, outError))
                {
                    return false;
                }
                constraint->columns.clear();
                for (const QString& column : columns)
                {
                    constraint->columns.push_back(column.toStdWString());
                }
                return true;
            }

            if (comment.startsWith(QStringLiteral(".Ref(")))
            {
                QStringList refs;
                if (!ParseQuotedList(comment.mid(QStringLiteral(".Ref").size()),
                                     &refs, outError))
                {
                    return false;
                }
                if (refs.isEmpty())
                {
                    if (outError != nullptr)
                    {
                        *outError = QStringLiteral(
                            "Foreign key reference requires at least one quoted value.");
                    }
                    return false;
                }
                constraint->referencedTable = refs.first().toStdWString();
                constraint->referencedColumns.clear();
                for (int index = 1; index < refs.size(); ++index)
                {
                    constraint->referencedColumns.push_back(
                        refs[index].toStdWString());
                }
                return true;
            }

            if (comment.startsWith(QStringLiteral(".OnDelete(")))
            {
                const QRegularExpression tokenRegex(QStringLiteral(
                    "^\\.OnDelete\\(\\s*([A-Za-z_][A-Za-z0-9_]*)\\s*\\)\\s*$"));
                const auto match = tokenRegex.match(comment);
                if (!match.hasMatch())
                {
                    if (outError != nullptr)
                    {
                        *outError = QStringLiteral(
                            "Invalid foreign key action syntax: ") +
                                    EscapeForMessage(comment);
                    }
                    return false;
                }
                return ParseConstraintActionToken(match.captured(1),
                                                  &constraint->onDelete,
                                                  outError);
            }

            if (comment.startsWith(QStringLiteral(".OnUpdate(")))
            {
                const QRegularExpression tokenRegex(QStringLiteral(
                    "^\\.OnUpdate\\(\\s*([A-Za-z_][A-Za-z0-9_]*)\\s*\\)\\s*$"));
                const auto match = tokenRegex.match(comment);
                if (!match.hasMatch())
                {
                    if (outError != nullptr)
                    {
                        *outError = QStringLiteral(
                            "Invalid foreign key action syntax: ") +
                                    EscapeForMessage(comment);
                    }
                    return false;
                }
                return ParseConstraintActionToken(match.captured(1),
                                                  &constraint->onUpdate,
                                                  outError);
            }

            if (comment.startsWith(QStringLiteral(".Expr(")))
            {
                const QRegularExpression exprRegex(QStringLiteral(
                    "^\\.Expr\\(\\s*(\"((?:\\\\.|[^\"])*)\")\\s*\\)\\s*$"));
                const auto exprMatch = exprRegex.match(comment);
                if (!exprMatch.hasMatch())
                {
                    if (outError != nullptr)
                    {
                        *outError = QStringLiteral("Invalid check expression: ") +
                                    EscapeForMessage(comment);
                    }
                    return false;
                }
                constraint->checkExpression =
                    UnescapeCppString(exprMatch.captured(2)).toStdWString();
                return true;
            }

            if (outError != nullptr)
            {
                *outError = QStringLiteral("Unsupported constraint comment token: ") +
                            EscapeForMessage(comment);
            }
            return false;
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
        bool inBlockBody = false;
        bool haveCurrentColumn = false;
        sc::SCColumnDef currentColumn;
        bool haveCurrentIndex = false;
        SCSchemaTableImportIndex currentIndex;
        bool haveCurrentConstraint = false;
        sc::SCConstraintDef currentConstraint;

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

        const auto flushConstraint = [&]() {
            if (!haveCurrentConstraint)
            {
                return;
            }
            outResult->constraints.push_back(currentConstraint);
            currentConstraint = sc::SCConstraintDef{};
            haveCurrentConstraint = false;
        };

        for (QString rawLine : lines)
        {
            QString line = rawLine.trimmed();

            if (!inBlock)
            {
                if (macroRegex.match(line).hasMatch())
                {
                    inBlock = true;
                }
                continue;
            }

            if (!inBlockBody)
            {
                if (line == QStringLiteral("{"))
                {
                    inBlockBody = true;
                }
                continue;
            }

            bool reprocess = true;
            while (reprocess)
            {
                reprocess = false;
                if (line.isEmpty())
                {
                    flushConstraint();
                    break;
                }

                if (line.startsWith(QStringLiteral("//")))
                {
                    QString comment = line.mid(2).trimmed();
                    if (comment.startsWith(QStringLiteral(".Constraint(")))
                    {
                        flushColumn();
                        flushIndex();
                        flushConstraint();
                        if (!ParseConstraintCommentStart(comment,
                                                         &currentConstraint,
                                                         outError))
                        {
                            return false;
                        }
                        haveCurrentConstraint = true;
                        break;
                    }
                    if (haveCurrentConstraint &&
                        (comment.startsWith(QStringLiteral(".Columns(")) ||
                         comment.startsWith(QStringLiteral(".Ref(")) ||
                         comment.startsWith(QStringLiteral(".OnDelete(")) ||
                         comment.startsWith(QStringLiteral(".OnUpdate(")) ||
                         comment.startsWith(QStringLiteral(".Expr("))))
                    {
                        if (ParseConstraintCommentContinuation(
                                comment, &currentConstraint, outError))
                        {
                            break;
                        }
                        return false;
                    }
                    if (haveCurrentConstraint)
                    {
                        flushConstraint();
                        reprocess = true;
                        continue;
                    }
                    break;
                }

                if (haveCurrentConstraint)
                {
                    flushConstraint();
                    reprocess = true;
                    continue;
                }
                break;
            }

            if (line.isEmpty())
            {
                continue;
            }

            if (line.startsWith(QStringLiteral("}")))
            {
                flushConstraint();
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
                flushConstraint();
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
            QVector<sc::SCIndexColumnDef> indexColumns;
            if (ParseIndexDeclaration(line, &indexName, &indexColumns,
                                      outError))
            {
                flushConstraint();
                flushColumn();
                if (!indexName.isEmpty())
                {
                    flushIndex();
                    currentIndex.name = indexName;
                    haveCurrentIndex = true;
                    currentIndex.columns = indexColumns;
                } else if (!indexColumns.isEmpty())
                {
                    currentIndex.columns = indexColumns;
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
                    "^\\s*\\.Ref\\((.*)\\)\\s*;?\\s*$"));
                const auto refMatch = refRegex.match(line);
                if (refMatch.hasMatch())
                {
                    QStringList refParts;
                    if (!ParseQuotedList(refMatch.captured(1), &refParts, outError))
                    {
                        return false;
                    }
                    if (refParts.size() < 1 || refParts.size() > 3)
                    {
                        if (outError != nullptr)
                        {
                            *outError = QStringLiteral(
                                "Relation reference must have 1 to 3 quoted values.");
                        }
                        return false;
                    }
                    currentColumn.columnKind = sc::ColumnKind::Relation;
                    currentColumn.referenceTable = refParts[0].toStdWString();
                    if (refParts.size() >= 2)
                    {
                        currentColumn.referenceStorageColumn =
                            refParts[1].toStdWString();
                    }
                    if (refParts.size() >= 3)
                    {
                        currentColumn.referenceDisplayColumn =
                            refParts[2].toStdWString();
                    } else if (refParts.size() == 2)
                    {
                        currentColumn.referenceDisplayColumn =
                            currentColumn.referenceStorageColumn;
                    }
                    continue;
                }
            }

            if (haveCurrentIndex &&
                line.startsWith(QStringLiteral(".Columns(")))
            {
                if (!ParseIndexColumns(
                        line.mid(QStringLiteral(".Columns").size()),
                        &indexColumns, outError))
                {
                    return false;
                }
                currentIndex.columns = indexColumns;
                continue;
            }

            if (haveCurrentIndex &&
                line.startsWith(QStringLiteral(".Desc(")))
            {
                if (currentIndex.columns.isEmpty())
                {
                    if (outError != nullptr)
                    {
                        *outError = QStringLiteral(
                            "Index descending columns require a preceding .Columns(...).");
                    }
                    return false;
                }

                QStringList descendingColumns;
                if (!ParseQuotedList(
                        line.mid(QStringLiteral(".Desc").size()),
                        &descendingColumns, outError))
                {
                    return false;
                }
                if (!ApplyDescendingColumns(descendingColumns,
                                            &currentIndex.columns, outError))
                {
                    return false;
                }
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
        flushConstraint();

        const auto primaryKeyConstraintIt = std::find_if(
            outResult->constraints.begin(), outResult->constraints.end(),
            [](const sc::SCConstraintDef& constraint) {
                return constraint.kind == sc::SCConstraintKind::PrimaryKey;
            });
        if (primaryKeyConstraintIt == outResult->constraints.end() &&
            !outResult->primaryKeyColumnName.trimmed().isEmpty())
        {
            sc::SCConstraintDef primaryKeyConstraint;
            primaryKeyConstraint.kind = sc::SCConstraintKind::PrimaryKey;
            primaryKeyConstraint.name =
                (QStringLiteral("pk_") + outResult->tableMacroName)
                    .toStdWString();
            primaryKeyConstraint.columns.push_back(
                outResult->primaryKeyColumnName.toStdWString());
            primaryKeyConstraint.sourceKind = sc::SCSchemaSourceKind::Explicit;
            outResult->constraints.insert(outResult->constraints.begin(),
                                          std::move(primaryKeyConstraint));
        }
        if (outResult->primaryKeyColumnName.trimmed().isEmpty())
        {
            for (const sc::SCConstraintDef& constraint : outResult->constraints)
            {
                if (constraint.kind != sc::SCConstraintKind::PrimaryKey ||
                    constraint.columns.empty())
                {
                    continue;
                }
                outResult->primaryKeyColumnName =
                    QString::fromStdWString(constraint.columns.front());
                break;
            }
        }

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

        if (!outResult->primaryKeyColumnName.trimmed().isEmpty())
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
            }
        }

        if (std::none_of(outResult->constraints.begin(),
                         outResult->constraints.end(),
                         [](const sc::SCConstraintDef& constraint) {
                             return constraint.kind ==
                                    sc::SCConstraintKind::PrimaryKey;
                         }))
        {
            outResult->warnings.push_back(
                QStringLiteral("No explicit primary key was imported."));
        }

        for (const SCSchemaTableImportIndex& index : outResult->indexes)
        {
            if (index.columns.isEmpty())
            {
                continue;
            }

            for (const sc::SCIndexColumnDef& indexColumn : index.columns)
            {
                const QString columnName =
                    QString::fromStdWString(indexColumn.columnName);
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
