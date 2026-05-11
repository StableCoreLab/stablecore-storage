#pragma once

#include <QString>
#include <QStringList>
#include <QVector>

#include "SCTypes.h"

namespace StableCore::Storage::Editor
{

    struct SCSchemaTableImportIndex
    {
        QString name;
        QStringList columns;
    };

    struct SCSchemaTableImportResult
    {
        QString tableMacroName;
        QString tableName;
        QString tableDescription;
        QString primaryKeyColumnName;
        QVector<StableCore::Storage::SCColumnDef> columns;
        QVector<SCSchemaTableImportIndex> indexes;
        QStringList warnings;
    };

    bool ParseSchemaTableDescription(const QString& text,
                                     SCSchemaTableImportResult* outResult,
                                     QString* outError);

}  // namespace StableCore::Storage::Editor
