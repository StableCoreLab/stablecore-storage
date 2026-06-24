#pragma once

#include <QDialog>
#include <QString>
#include <QVector>

#include "SCTypes.h"

class QLabel;
class QLineEdit;
class QPlainTextEdit;
class QCheckBox;
class QListWidget;
class QPushButton;

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
        void AddConstraint();
        void EditConstraint();
        void RemoveConstraint();
        void AddIndex();
        void EditIndex();
        void RemoveIndex();
        void UpdateConstraintList();
        void UpdateIndexList();

    private:
        bool ReloadSchema(QString* outError);
        void UpdateOutput();
        std::vector<std::wstring> GetAvailableColumnNames() const;

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
        class QListWidget* constraintList_{nullptr};
        class QPushButton* addConstraintButton_{nullptr};
        class QPushButton* editConstraintButton_{nullptr};
        class QPushButton* removeConstraintButton_{nullptr};
        class QListWidget* indexList_{nullptr};
        class QPushButton* addIndexButton_{nullptr};
        class QPushButton* editIndexButton_{nullptr};
        class QPushButton* removeIndexButton_{nullptr};
    };

}  // namespace StableCore::Storage::Editor
