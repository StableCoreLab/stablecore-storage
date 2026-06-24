#include "SCSchemaTableImportDialog.h"

#include <QHBoxLayout>
#include <QLabel>
#include <QPlainTextEdit>
#include <QPushButton>
#include <QVBoxLayout>

#include "SCDatabaseSession.h"
#include "SCSchemaTableImport.h"

namespace StableCore::Storage::Editor
{
    namespace
    {

        QString DefaultImportTemplate()
        {
            return QStringLiteral(R"(SC_SCHEMA_TABLE(ProjectInfo)
{
    Table("ProjectInfo")
        .Column("GUID", SCType::String)
        .Column("ProjectName", SCType::String)
            .Description("Project Name")
        .Column("ProjectCode", SCType::String)
            .Description("Project Code")
        .Column("OwnerUnit", SCType::String)
            .Description("Owner Unit")
        .Column("ConstructorUnit", SCType::String)
            .Description("Constructor Unit")
        .Column("Compiler", SCType::String)
            .Description("Compiler")
        .Column("CompileDate", SCType::String)
            .Description("Compile Date")
        .Column("RegionCode", SCType::String)
            .Description("Region Code")
        .Column("RegionName", SCType::String)
            .Description("Region Name")
        .Column("ProjectAddress", SCType::String)
            .Description("Project Address")
        .Column("Description", SCType::String)
            .Description("Remark")
        .Column("CreatedAt", SCType::String)
            .Description("Created At")
        .Column("UpdatedAt", SCType::String)
            .Description("Updated At");
})");
        }

    }  // namespace

    SCSchemaTableImportDialog::SCSchemaTableImportDialog(
        SCDatabaseSession* session, QWidget* parent)
        : QDialog(parent), session_(session)
    {
        setWindowTitle(QStringLiteral("Create Table From Schema"));
        resize(980, 760);

        auto* layout = new QVBoxLayout(this);

        auto* introLabel = new QLabel(
            QStringLiteral(
                "Paste SC_SCHEMA_TABLE(...) text to create a table from its schema description."
                " Column descriptions become display names."
                " Defaults are imported when present."
                " Primary keys and table descriptions are treated as hints."),
            this);
        introLabel->setWordWrap(true);
        layout->addWidget(introLabel);

        schemaTextEdit_ = new QPlainTextEdit(this);
        schemaTextEdit_->setPlainText(DefaultImportTemplate());
        schemaTextEdit_->setLineWrapMode(QPlainTextEdit::NoWrap);
        layout->addWidget(schemaTextEdit_, 1);

        messageLabel_ = new QLabel(this);
        messageLabel_->setWordWrap(true);
        messageLabel_->setText(QStringLiteral("Ready."));
        layout->addWidget(messageLabel_);

        auto* buttonRow = new QHBoxLayout();
        auto* createButton =
            new QPushButton(QStringLiteral("Create Table"), this);
        auto* closeButton = new QPushButton(QStringLiteral("Close"), this);
        buttonRow->addWidget(createButton);
        buttonRow->addStretch(1);
        buttonRow->addWidget(closeButton);
        layout->addLayout(buttonRow);

        connect(createButton, &QPushButton::clicked, this,
                &SCSchemaTableImportDialog::CreateFromText);
        connect(closeButton, &QPushButton::clicked, this, &QDialog::accept);
    }

    void SCSchemaTableImportDialog::UpdateMessage(const QString& text)
    {
        if (messageLabel_ != nullptr)
        {
            messageLabel_->setText(text);
        }
    }

    void SCSchemaTableImportDialog::CreateFromText()
    {
        if (session_ == nullptr || !session_->IsOpen())
        {
            UpdateMessage(QStringLiteral("No database open."));
            return;
        }

        SCSchemaTableImportResult importResult;
        QString parseError;
        if (!ParseSchemaTableDescription(schemaTextEdit_->toPlainText(),
                                         &importResult, &parseError))
        {
            UpdateMessage(QStringLiteral("Parse failed: ") + parseError);
            return;
        }

        QString createError;
        if (!session_->CreateTableFromSchema(importResult, &createError))
        {
            UpdateMessage(QStringLiteral("Create failed: ") + createError);
            return;
        }

        QString status = QStringLiteral("Created table \"%1\" with %2 column(s).")
                             .arg(importResult.tableName)
                             .arg(importResult.columns.size());
        if (!importResult.warnings.isEmpty())
        {
            status += QStringLiteral(" Warnings: ") +
                      importResult.warnings.join(QStringLiteral(" | "));
        }
        UpdateMessage(status);
        accept();
    }

}  // namespace StableCore::Storage::Editor
