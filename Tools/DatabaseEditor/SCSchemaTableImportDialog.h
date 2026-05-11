#pragma once

#include <QDialog>
#include <QLabel>
#include <QPlainTextEdit>
#include <QString>
#include <QWidget>

namespace StableCore::Storage::Editor
{

    class SCDatabaseSession;

    class SCSchemaTableImportDialog final : public QDialog
    {
        Q_OBJECT

    public:
        explicit SCSchemaTableImportDialog(SCDatabaseSession* session,
                                           QWidget* parent = nullptr);

    private slots:
        void CreateFromText();

    private:
        void UpdateMessage(const QString& text);

        SCDatabaseSession* session_{nullptr};
        QPlainTextEdit* schemaTextEdit_{nullptr};
        QLabel* messageLabel_{nullptr};
    };

}  // namespace StableCore::Storage::Editor
