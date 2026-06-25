#pragma once

#include <QString>
#include <QVector>

#include "SCTypes.h"

namespace StableCore::Storage::Editor
{

    struct SCSchemaTableExportOptions
    {
        QString tableDescription;
        QString primaryKeyColumnName;
        bool includeLegacyIndexes{true};
    };

    QString BuildSchemaTableCode(
        const StableCore::Storage::SCTableSchemaSnapshot& snapshot,
        const SCSchemaTableExportOptions& options = {});

}  // namespace StableCore::Storage::Editor
