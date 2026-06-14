#include "SCSchemaTableGenerator.h"

#include <QRegularExpression>
#include <QStringList>

namespace sc = StableCore::Storage;

namespace StableCore::Storage::Editor
{
    namespace
    {
        QString EscapeCppString(const QString& text)
        {
            QString escaped = text;
            escaped.replace(QLatin1Char('\\'), QStringLiteral("\\\\"));
            escaped.replace(QLatin1Char('"'), QStringLiteral("\\\""));
            escaped.replace(QLatin1Char('\r'), QStringLiteral("\\r"));
            escaped.replace(QLatin1Char('\n'), QStringLiteral("\\n"));
            return escaped;
        }

        QString ToQString(const std::wstring& text)
        {
            return QString::fromStdWString(text);
        }

        QString ValueKindToSCType(sc::ValueKind kind)
        {
            switch (kind)
            {
                case sc::ValueKind::Int64:
                    return QStringLiteral("SCType::Int64");
                case sc::ValueKind::Double:
                    return QStringLiteral("SCType::Double");
                case sc::ValueKind::Bool:
                    return QStringLiteral("SCType::Bool");
                case sc::ValueKind::String:
                    return QStringLiteral("SCType::String");
                case sc::ValueKind::RecordId:
                    return QStringLiteral("SCType::RecordId");
                case sc::ValueKind::Enum:
                    return QStringLiteral("SCType::Enum");
                case sc::ValueKind::Null:
                default:
                    return QStringLiteral("SCType::Null");
            }
        }

        QString MakeIndexName(const QString& tableName,
                              const std::vector<sc::SCIndexColumnDef>& columns)
        {
            QStringList parts;
            parts.push_back(QStringLiteral("idx"));
            parts.push_back(tableName);
            for (const sc::SCIndexColumnDef& column : columns)
            {
                parts.push_back(ToQString(column.columnName));
            }

            QString name = parts.join(QLatin1Char('_'));
            name.replace(
                QRegularExpression(QStringLiteral(R"([^A-Za-z0-9_]+)")),
                QStringLiteral("_"));
            return name;
        }

        QString ColumnDescription(const sc::SCColumnDef& column)
        {
            const QString displayName = ToQString(column.displayName);
            const QString name = ToQString(column.name);
            if (displayName.isEmpty() ||
                displayName.compare(name, Qt::CaseInsensitive) == 0)
            {
                return {};
            }
            return displayName;
        }

        QString DefaultValueLiteral(const sc::SCColumnDef& column)
        {
            if (column.defaultValue.IsNull())
            {
                return {};
            }

            switch (column.defaultValue.GetKind())
            {
                case sc::ValueKind::Int64: {
                    std::int64_t value = 0;
                    if (column.defaultValue.AsInt64(&value) == sc::SC_OK)
                    {
                        return QString::number(value);
                    }
                    break;
                }
                case sc::ValueKind::Double: {
                    double value = 0.0;
                    if (column.defaultValue.AsDouble(&value) == sc::SC_OK)
                    {
                        return QString::number(value, 'g', 12);
                    }
                    break;
                }
                case sc::ValueKind::Bool: {
                    bool value = false;
                    if (column.defaultValue.AsBool(&value) == sc::SC_OK)
                    {
                        return value ? QStringLiteral("true")
                                     : QStringLiteral("false");
                    }
                    break;
                }
                case sc::ValueKind::String: {
                    std::wstring value;
                    if (column.defaultValue.AsStringCopy(&value) == sc::SC_OK)
                    {
                        return QStringLiteral("\"") +
                               EscapeCppString(ToQString(value)) +
                               QStringLiteral("\"");
                    }
                    break;
                }
                case sc::ValueKind::RecordId: {
                    sc::RecordId value = 0;
                    if (column.defaultValue.AsRecordId(&value) == sc::SC_OK)
                    {
                        return QString::number(static_cast<qlonglong>(value));
                    }
                    break;
                }
                case sc::ValueKind::Enum: {
                    std::wstring value;
                    if (column.defaultValue.AsEnumCopy(&value) == sc::SC_OK)
                    {
                        return QStringLiteral("\"") +
                               EscapeCppString(ToQString(value)) +
                               QStringLiteral("\"");
                    }
                    break;
                }
                case sc::ValueKind::Binary:
                case sc::ValueKind::Null:
                default:
                    break;
            }

            return {};
        }

        QString PrimaryKeyColumnName(const sc::SCTableSchemaSnapshot& snapshot,
                                     const SCSchemaTableExportOptions& options)
        {
            const QString manualOverride =
                options.primaryKeyColumnName.trimmed();
            if (!manualOverride.isEmpty())
            {
                return manualOverride;
            }

            for (const sc::SCConstraintDef& constraint : snapshot.constraints)
            {
                if (constraint.kind != sc::SCConstraintKind::PrimaryKey ||
                    constraint.columns.empty())
                {
                    continue;
                }
                if (constraint.columns.size() == 1)
                {
                    return ToQString(constraint.columns.front());
                }
            }

            return {};
        }

        QVector<sc::SCIndexDef> FilterIndexes(
            const sc::SCTableSchemaSnapshot& snapshot,
            const SCSchemaTableExportOptions& options)
        {
            QVector<sc::SCIndexDef> indexes;
            for (const sc::SCIndexDef& index : snapshot.indexes)
            {
                if (index.sourceKind == sc::SCSchemaSourceKind::LegacyHint &&
                    !options.includeLegacyIndexes)
                {
                    continue;
                }
                if (index.columns.empty())
                {
                    continue;
                }
                indexes.push_back(index);
            }
            return indexes;
        }

        QString FormatIndexColumns(
            const std::vector<sc::SCIndexColumnDef>& columns,
            QString* outDescColumns)
        {
            QStringList parts;
            QStringList descParts;
            for (const sc::SCIndexColumnDef& column : columns)
            {
                QString colName = QStringLiteral("\"") +
                                  EscapeCppString(ToQString(column.columnName)) +
                                  QStringLiteral("\"");
                parts.push_back(colName);
                if (column.descending)
                {
                    descParts.push_back(colName);
                }
            }
            if (outDescColumns != nullptr && !descParts.isEmpty())
            {
                *outDescColumns = descParts.join(QStringLiteral(", "));
            }
            return parts.join(QStringLiteral(", "));
        }
    }  // namespace

    QString BuildSchemaTableCode(const sc::SCTableSchemaSnapshot& snapshot,
                                 const SCSchemaTableExportOptions& options)
    {
        const QString tableName = ToQString(snapshot.table.name).trimmed();
        if (tableName.isEmpty())
        {
            return QStringLiteral("// Table name is required.");
        }

        QVector<sc::SCColumnDef> factColumns;
        factColumns.reserve(snapshot.columns.size());
        for (const sc::SCColumnDef& column : snapshot.columns)
        {
            if (column.name.empty())
            {
                continue;
            }
            factColumns.push_back(column);
        }

        const QString tableDescription =
            options.tableDescription.trimmed().isEmpty()
                ? ToQString(snapshot.table.description).trimmed()
                : options.tableDescription.trimmed();
        const QString primaryKey = PrimaryKeyColumnName(snapshot, options);
        const QVector<sc::SCIndexDef> indexes =
            FilterIndexes(snapshot, options);

        QString code;
        code += QStringLiteral("SC_SCHEMA_TABLE(") +
                EscapeCppString(tableName) + QStringLiteral(")\n{\n");
        code += QStringLiteral("    Table(\"") + EscapeCppString(tableName) +
                QStringLiteral("\")\n");

        if (!tableDescription.isEmpty())
        {
            code += QStringLiteral("        .Description(\"") +
                    EscapeCppString(tableDescription) + QStringLiteral("\")\n");
        }
        if (!primaryKey.isEmpty())
        {
            code += QStringLiteral("        .PrimaryKey(\"") +
                    EscapeCppString(primaryKey) + QStringLiteral("\")\n");
        }

        for (const sc::SCColumnDef& column : factColumns)
        {
            const QString columnName = ToQString(column.name);
            QString line =
                QStringLiteral("        .Column(\"") +
                EscapeCppString(columnName) + QStringLiteral("\", ") +
                ValueKindToSCType(column.valueKind) + QStringLiteral(")\n");
            if (!column.nullable)
            {
                line += QStringLiteral("            .NotNull()\n");
            }
            const QString defaultValue = DefaultValueLiteral(column);
            if (!defaultValue.isEmpty())
            {
                line += QStringLiteral("            .Default(") + defaultValue +
                        QStringLiteral(")\n");
            }
            if (!ToQString(column.referenceTable).isEmpty())
            {
                line += QStringLiteral("            .Ref(\"") +
                        EscapeCppString(ToQString(column.referenceTable)) +
                        QStringLiteral("\", \"Id\")\n");
            }
            const QString description = ColumnDescription(column);
            if (!description.isEmpty())
            {
                line += QStringLiteral("            .Description(\"") +
                        EscapeCppString(description) + QStringLiteral("\")\n");
            }
            code += line;
        }

        for (const sc::SCIndexDef& index : indexes)
        {
            QString indexName = ToQString(index.name).trimmed();
            if (indexName.isEmpty())
            {
                indexName = MakeIndexName(tableName, index.columns);
            }
            code += QStringLiteral("        .Index(\"") +
                    EscapeCppString(indexName) + QStringLiteral("\")");
            QString descColumns;
            code += QStringLiteral(".Columns(") +
                    FormatIndexColumns(index.columns, &descColumns) +
                    QStringLiteral(")");
            if (!descColumns.isEmpty())
            {
                code += QStringLiteral("\n                .Desc(") +
                        descColumns + QStringLiteral(")");
            }
            code += QStringLiteral("\n");
        }

        if (code.endsWith(QLatin1Char('\n')))
        {
            code.chop(1);
            code += QStringLiteral(";\n");
        } else
        {
            code += QStringLiteral(";\n");
        }
        code += QStringLiteral("}\n");
        return code;
    }

}  // namespace StableCore::Storage::Editor
