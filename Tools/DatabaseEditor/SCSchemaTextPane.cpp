#include "SCSchemaTextPane.h"

#include <QApplication>
#include <QCheckBox>
#include <QClipboard>
#include <QGroupBox>
#include <QHBoxLayout>
#include <QLabel>
#include <QLineEdit>
#include <QPlainTextEdit>
#include <QPushButton>
#include <QSplitter>
#include <QVBoxLayout>

#include "SCDatabaseSession.h"
#include "SCSchemaTableGenerator.h"
#include "SCSchemaTableImport.h"

namespace sc = StableCore::Storage;

namespace StableCore::Storage::Editor
{
    SCSchemaTextPane::SCSchemaTextPane(SCDatabaseSession* session,
                                       QWidget* parent)
        : QWidget(parent), session_(session)
    {
        BuildUi();
    }

    void SCSchemaTextPane::BuildUi()
    {
        auto* rootLayout = new QVBoxLayout(this);
        rootLayout->setContentsMargins(0, 0, 0, 0);
        rootLayout->setSpacing(8);

        auto* introLabel = new QLabel(
            QStringLiteral(
                "Generate a schema description from the current table, or import a schema as a new table. "
                "Applying schema text to the current table is not implemented yet."),
            this);
        introLabel->setWordWrap(true);
        rootLayout->addWidget(introLabel);

        auto* optionsRow = new QHBoxLayout();
        exportStatusLabel_ = new QLabel(QStringLiteral("Current table: -"), this);
        exportDescriptionEdit_ = new QLineEdit(this);
        exportDescriptionEdit_->setPlaceholderText(QStringLiteral("Optional exported description"));
        includeLegacyIndexesCheck_ = new QCheckBox(QStringLiteral("Include legacy index hints"), this);
        optionsRow->addWidget(exportStatusLabel_, 1);
        optionsRow->addWidget(exportDescriptionEdit_, 1);
        optionsRow->addWidget(includeLegacyIndexesCheck_);
        rootLayout->addLayout(optionsRow);

        auto* splitter = new QSplitter(Qt::Horizontal, this);

        auto* exportPane = new QGroupBox(QStringLiteral("Export Current Table Schema"), splitter);
        auto* exportLayout = new QVBoxLayout(exportPane);
        exportPreviewEdit_ = new QPlainTextEdit(exportPane);
        exportPreviewEdit_->setReadOnly(true);
        exportPreviewEdit_->setLineWrapMode(QPlainTextEdit::NoWrap);
        exportLayout->addWidget(exportPreviewEdit_, 1);
        auto* exportButtons = new QHBoxLayout();
        auto* refreshButton = new QPushButton(QStringLiteral("Refresh"), exportPane);
        auto* copyButton = new QPushButton(QStringLiteral("Copy"), exportPane);
        exportButtons->addWidget(refreshButton);
        exportButtons->addWidget(copyButton);
        exportButtons->addStretch(1);
        exportLayout->addLayout(exportButtons);

        auto* importPane = new QGroupBox(QStringLiteral("Import Schema As New Table"), splitter);
        auto* importLayout = new QVBoxLayout(importPane);
        importTextEdit_ = new QPlainTextEdit(importPane);
        importTextEdit_->setLineWrapMode(QPlainTextEdit::NoWrap);
        importLayout->addWidget(importTextEdit_, 1);
        auto* importButtons = new QHBoxLayout();
        auto* validateButton = new QPushButton(QStringLiteral("Validate"), importPane);
        auto* importButton = new QPushButton(QStringLiteral("Import As New Table"), importPane);
        importButtons->addWidget(validateButton);
        importButtons->addWidget(importButton);
        importButtons->addStretch(1);
        importLayout->addLayout(importButtons);

        applyCurrentTableButton_ =
            new QPushButton(QStringLiteral("Apply to Current Table"), importPane);
        applyCurrentTableButton_->setEnabled(false);
        applyCurrentTableButton_->setToolTip(
            QStringLiteral("Not implemented yet."));
        importLayout->addWidget(applyCurrentTableButton_);

        auto* futureHintLabel = new QLabel(
            QStringLiteral("Apply to current table: not implemented yet."),
            importPane);
        futureHintLabel->setWordWrap(true);
        futureHintLabel->setStyleSheet(QStringLiteral("color: #6a6a6a;"));
        importLayout->addWidget(futureHintLabel);

        splitter->addWidget(exportPane);
        splitter->addWidget(importPane);
        splitter->setStretchFactor(0, 1);
        splitter->setStretchFactor(1, 1);
        rootLayout->addWidget(splitter, 1);

        auto* messageGroup = new QGroupBox(QStringLiteral("Messages"), this);
        auto* messageLayout = new QVBoxLayout(messageGroup);
        messageTextEdit_ = new QPlainTextEdit(messageGroup);
        messageTextEdit_->setReadOnly(true);
        messageLayout->addWidget(messageTextEdit_);
        rootLayout->addWidget(messageGroup);

        connect(exportDescriptionEdit_, &QLineEdit::textChanged, this,
                &SCSchemaTextPane::UpdateExportPreview);
        connect(includeLegacyIndexesCheck_, &QCheckBox::toggled, this,
                &SCSchemaTextPane::UpdateExportPreview);
        connect(refreshButton, &QPushButton::clicked, this,
                &SCSchemaTextPane::RefreshExportPreview);
        connect(copyButton, &QPushButton::clicked, this,
                &SCSchemaTextPane::CopyExportPreview);
        connect(validateButton, &QPushButton::clicked, this,
                &SCSchemaTextPane::ValidateImportText);
        connect(importButton, &QPushButton::clicked, this,
                &SCSchemaTextPane::ImportAsNewTable);
    }

    void SCSchemaTextPane::Refresh()
    {
        schemaSnapshot_.tables.clear();
        UpdateExportPreview();
    }

    void SCSchemaTextPane::UpdateExportPreview()
    {
        if (session_ == nullptr || !session_->IsOpen())
        {
            exportStatusLabel_->setText(QStringLiteral("Current table: -"));
            exportPreviewEdit_->setPlainText(QStringLiteral("// No database open."));
            UpdateMessage(QStringLiteral("Open a database to preview or import schema text."));
            return;
        }

        const QString currentTableName = session_->CurrentTableName();
        exportStatusLabel_->setText(
            currentTableName.isEmpty()
                ? QStringLiteral("Current table: -")
                : QStringLiteral("Current table: %1").arg(currentTableName));

        if (currentTableName.isEmpty())
        {
            exportPreviewEdit_->setPlainText(
                QStringLiteral("// Select a table to generate schema text."));
            UpdateMessage(QStringLiteral("Select a table to generate schema text."));
            return;
        }

        QString error;
        if (!session_->BuildSchemaSnapshot(&schemaSnapshot_, &error) ||
            schemaSnapshot_.tables.empty())
        {
            exportPreviewEdit_->setPlainText(QStringLiteral("// ") + error);
            UpdateMessage(QStringLiteral("Failed to build schema preview: ") + error);
            return;
        }

        SCSchemaTableExportOptions options;
        options.tableDescription = exportDescriptionEdit_->text().trimmed();
        options.includeLegacyIndexes = includeLegacyIndexesCheck_->isChecked();
        exportPreviewEdit_->setPlainText(
            BuildSchemaTableCode(schemaSnapshot_.tables.front(), options));
    }

    void SCSchemaTextPane::RefreshExportPreview()
    {
        Refresh();
        UpdateMessage(QStringLiteral("Schema export preview refreshed."));
    }

    void SCSchemaTextPane::CopyExportPreview()
    {
        if (QApplication::clipboard() != nullptr)
        {
            QApplication::clipboard()->setText(exportPreviewEdit_->toPlainText());
            UpdateMessage(QStringLiteral("Schema export preview copied."));
        }
    }

    void SCSchemaTextPane::ValidateImportText()
    {
        SCSchemaTableImportResult importResult;
        QString parseError;
        if (!ParseSchemaTableDescription(importTextEdit_->toPlainText(),
                                         &importResult, &parseError))
        {
            UpdateMessage(QStringLiteral("Validation failed: ") + parseError);
            return;
        }

        QString message = QStringLiteral("Valid schema text for table \"%1\" with %2 column(s).")
                              .arg(importResult.tableName)
                              .arg(importResult.columns.size());
        if (!importResult.warnings.isEmpty())
        {
            message += QStringLiteral("\nWarnings:\n") +
                       importResult.warnings.join(QStringLiteral("\n"));
        }
        UpdateMessage(message);
    }

    void SCSchemaTextPane::ImportAsNewTable()
    {
        if (session_ == nullptr || !session_->IsOpen())
        {
            UpdateMessage(QStringLiteral("Open a database before importing schema text."));
            return;
        }

        SCSchemaTableImportResult importResult;
        QString parseError;
        if (!ParseSchemaTableDescription(importTextEdit_->toPlainText(),
                                         &importResult, &parseError))
        {
            UpdateMessage(QStringLiteral("Import failed during parse: ") + parseError);
            return;
        }

        QString error;
        if (!session_->CreateTableFromSchema(importResult, &error))
        {
            UpdateMessage(QStringLiteral("Import failed: ") + error);
            return;
        }

        QString message = QStringLiteral("Created table \"%1\" from schema text.")
                              .arg(importResult.tableName);
        if (!importResult.warnings.isEmpty())
        {
            message += QStringLiteral("\nWarnings:\n") +
                       importResult.warnings.join(QStringLiteral("\n"));
        }
        UpdateMessage(message);
        emit TableImported(importResult.tableName);
    }

    void SCSchemaTextPane::UpdateMessage(const QString& text)
    {
        messageTextEdit_->setPlainText(text);
        emit StatusMessage(text);
    }

}  // namespace StableCore::Storage::Editor
