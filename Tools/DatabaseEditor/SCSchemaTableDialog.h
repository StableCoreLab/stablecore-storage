#pragma once

#include <QDialog>
#include <QString>
#include <QVector>

#include "SCTypes.h"

class QLabel;
class QLineEdit;
class QPlainTextEdit;
class QCheckBox;

namespace StableCore::Storage::Editor
{

    class SCDatabaseSession;

    class SCSchemaTableDialog final : public QDialog
    {
        Q_OBJECT

    public:
        explicit SCSchemaTableDialog(SCDatabaseSession* session,
                                     QWidget* parent = nullptr);

    private slots:
        void RefreshPreview();
        void CopyOutput();

    private:
        bool ReloadSchema(QString* outError);
        void UpdateOutput();

        SCDatabaseSession* session_{nullptr};
        StableCore::Storage::SCSchemaSnapshot schemaSnapshot_;
        QString tableName_;
        class QLabel* tableNameLabel_{nullptr};
        class QLineEdit* tableDescriptionEdit_{nullptr};
        class QLineEdit* primaryKeyEdit_{nullptr};
        class QCheckBox* includeLegacyIndexesCheck_{nullptr};
        class QLabel* legacyHintLabel_{nullptr};
        class QPlainTextEdit* outputEdit_{nullptr};
        class QLabel* statusLabel_{nullptr};
    };

}  // namespace StableCore::Storage::Editor
