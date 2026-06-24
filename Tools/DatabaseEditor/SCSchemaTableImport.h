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
        QVector<StableCore::Storage::SCIndexColumnDef> columns;
    };

    struct SCSchemaTableImportResult
    {
        QString tableMacroName;
        QString tableName;
        QString tableDescription;
        QString primaryKeyColumnName;
        QVector<StableCore::Storage::SCColumnDef> columns;
        QVector<StableCore::Storage::SCConstraintDef> constraints;
        QVector<SCSchemaTableImportIndex> indexes;
        QStringList warnings;
    };

    bool ParseSchemaTableDescription(const QString& text,
                                     SCSchemaTableImportResult* outResult,
                                     QString* outError);

}  // namespace StableCore::Storage::Editor
