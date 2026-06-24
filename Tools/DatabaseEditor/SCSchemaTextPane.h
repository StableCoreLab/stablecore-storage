#pragma once

#include <QWidget>

#include "SCTypes.h"

class QCheckBox;
class QLabel;
class QLineEdit;
class QPlainTextEdit;
class QPushButton;

namespace StableCore::Storage::Editor
{

    class SCDatabaseSession;

    class SCSchemaTextPane final : public QWidget
    {
        Q_OBJECT

    public:
        explicit SCSchemaTextPane(SCDatabaseSession* session,
                                  QWidget* parent = nullptr);

        void Refresh();

    signals:
        void TableImported(const QString& tableName);
        void StatusMessage(const QString& text);

    private slots:
        void RefreshExportPreview();
        void CopyExportPreview();
        void ValidateImportText();
        void ImportAsNewTable();

    private:
        void BuildUi();
        void UpdateExportPreview();
        void UpdateMessage(const QString& text);

        SCDatabaseSession* session_{nullptr};
        StableCore::Storage::SCSchemaSnapshot schemaSnapshot_;

        QLabel* exportStatusLabel_{nullptr};
        QLineEdit* exportDescriptionEdit_{nullptr};
        QCheckBox* includeLegacyIndexesCheck_{nullptr};
        QPlainTextEdit* exportPreviewEdit_{nullptr};
        QPlainTextEdit* importTextEdit_{nullptr};
        QPlainTextEdit* messageTextEdit_{nullptr};
        QPushButton* applyCurrentTableButton_{nullptr};
    };

}  // namespace StableCore::Storage::Editor
