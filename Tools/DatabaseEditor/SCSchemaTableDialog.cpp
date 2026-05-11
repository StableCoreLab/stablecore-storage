#include "SCSchemaTableDialog.h"

#include <algorithm>

#include <QApplication>
#include <QCheckBox>
#include <QClipboard>
#include <QFormLayout>
#include <QHBoxLayout>
#include <QLabel>
#include <QLineEdit>
#include <QPlainTextEdit>
#include <QPushButton>
#include <QVBoxLayout>

#include "SCDatabaseSession.h"
#include "SCSchemaTableGenerator.h"

namespace sc = StableCore::Storage;

namespace StableCore::Storage::Editor
{
    SCSchemaTableDialog::SCSchemaTableDialog(SCDatabaseSession* session,
                                             QWidget* parent)
        : QDialog(parent), session_(session)
    {
        setWindowTitle(QStringLiteral("表结构"));
        resize(1080, 720);

        auto* layout = new QVBoxLayout(this);
        auto* introLabel = new QLabel(
            QStringLiteral(
                "Generate SC_SCHEMA_TABLE code from the current table. "
                "Primary key and indexes are explicit; legacy indexed hints "
                "are not exported unless you confirm them."),
            this);
        introLabel->setWordWrap(true);
        layout->addWidget(introLabel);

        auto* form = new QFormLayout();
        tableNameLabel_ = new QLabel(this);
        tableNameLabel_->setTextInteractionFlags(Qt::TextSelectableByMouse);
        tableNameLabel_->setText(QStringLiteral("-"));
        form->addRow(QStringLiteral("Current Table"), tableNameLabel_);

        tableDescriptionEdit_ = new QLineEdit(this);
        tableDescriptionEdit_->setPlaceholderText(
            QStringLiteral("Optional table description"));
        form->addRow(QStringLiteral("Table Description"),
                     tableDescriptionEdit_);

        primaryKeyEdit_ = new QLineEdit(this);
        primaryKeyEdit_->setPlaceholderText(
            QStringLiteral("Explicit primary key column name"));
        form->addRow(QStringLiteral("Primary Key"), primaryKeyEdit_);

        includeLegacyIndexesCheck_ =
            new QCheckBox(QStringLiteral("Include legacy indexed columns"),
                          this);
        includeLegacyIndexesCheck_->setChecked(false);
        form->addRow(QStringLiteral("Legacy Indexes"),
                     includeLegacyIndexesCheck_);

        legacyHintLabel_ = new QLabel(this);
        legacyHintLabel_->setWordWrap(true);
        legacyHintLabel_->setText(
            QStringLiteral("Legacy index hints will appear here."));
        form->addRow(QStringLiteral("Legacy Hint"), legacyHintLabel_);
        layout->addLayout(form);

        outputEdit_ = new QPlainTextEdit(this);
        outputEdit_->setReadOnly(true);
        outputEdit_->setLineWrapMode(QPlainTextEdit::NoWrap);
        outputEdit_->setPlaceholderText(
            QStringLiteral("Generated code will appear here."));
        layout->addWidget(outputEdit_, 1);

        statusLabel_ = new QLabel(QStringLiteral("Ready."), this);
        layout->addWidget(statusLabel_);

        auto* buttonRow = new QHBoxLayout();
        auto* refreshButton = new QPushButton(QStringLiteral("Refresh"), this);
        auto* copyButton = new QPushButton(QStringLiteral("Copy Output"), this);
        auto* closeButton = new QPushButton(QStringLiteral("Close"), this);
        buttonRow->addWidget(refreshButton);
        buttonRow->addWidget(copyButton);
        buttonRow->addStretch(1);
        buttonRow->addWidget(closeButton);
        layout->addLayout(buttonRow);

        connect(refreshButton, &QPushButton::clicked, this,
                &SCSchemaTableDialog::RefreshPreview);
        connect(copyButton, &QPushButton::clicked, this,
                &SCSchemaTableDialog::CopyOutput);
        connect(closeButton, &QPushButton::clicked, this, &QDialog::accept);
        connect(tableDescriptionEdit_, &QLineEdit::textChanged, this,
                &SCSchemaTableDialog::UpdateOutput);
        connect(primaryKeyEdit_, &QLineEdit::textChanged, this,
                &SCSchemaTableDialog::UpdateOutput);
        connect(includeLegacyIndexesCheck_, &QCheckBox::toggled, this,
                &SCSchemaTableDialog::UpdateOutput);

        QString error;
        if (!ReloadSchema(&error))
        {
            tableNameLabel_->setText(QStringLiteral("-"));
            outputEdit_->setPlainText(QStringLiteral("// ") + error);
            statusLabel_->setText(QStringLiteral("Failed to load current table: ") +
                                  error);
            copyButton->setEnabled(false);
            refreshButton->setEnabled(false);
            includeLegacyIndexesCheck_->setEnabled(false);
            return;
        }

        UpdateOutput();
    }

    bool SCSchemaTableDialog::ReloadSchema(QString* outError)
    {
        tableName_.clear();
        schemaSnapshot_.tables.clear();

        if (session_ == nullptr || !session_->IsOpen())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("No database is open.");
            }
            return false;
        }

        tableName_ = session_->CurrentTableName();
        if (tableName_.isEmpty())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Select a table first.");
            }
            return false;
        }

        if (!session_->BuildSchemaSnapshot(&schemaSnapshot_, outError))
        {
            return false;
        }
        if (schemaSnapshot_.tables.empty())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("No schema snapshot available.");
            }
            return false;
        }

        tableNameLabel_->setText(tableName_);

        const auto& tableSnapshot = schemaSnapshot_.tables.front();
        const int legacyHintCount = static_cast<int>(std::count_if(
            tableSnapshot.indexes.begin(), tableSnapshot.indexes.end(),
            [](const sc::SCIndexDef& index) {
                return index.sourceKind == sc::SCSchemaSourceKind::LegacyHint;
            }));
        legacyHintLabel_->setText(
            legacyHintCount > 0
                ? QStringLiteral("%1 legacy indexed column(s) detected.")
                      .arg(legacyHintCount)
                : QStringLiteral("No legacy indexed columns were detected."));
        return true;
    }

    void SCSchemaTableDialog::UpdateOutput()
    {
        if (tableName_.isEmpty() || schemaSnapshot_.tables.empty())
        {
            outputEdit_->setPlainText(QStringLiteral("// No table selected."));
            statusLabel_->setText(QStringLiteral("No current table available."));
            return;
        }

        const sc::SCTableSchemaSnapshot& tableSnapshot =
            schemaSnapshot_.tables.front();
        SCSchemaTableExportOptions options;
        options.tableDescription = tableDescriptionEdit_->text().trimmed();
        options.primaryKeyColumnName = primaryKeyEdit_->text().trimmed();
        options.includeLegacyIndexes =
            includeLegacyIndexesCheck_ != nullptr &&
            includeLegacyIndexesCheck_->isChecked();

        const QString code = BuildSchemaTableCode(tableSnapshot, options);
        outputEdit_->setPlainText(code);

        int exportedIndexes = 0;
        for (const sc::SCIndexDef& index : tableSnapshot.indexes)
        {
            if (index.sourceKind == sc::SCSchemaSourceKind::LegacyHint &&
                !options.includeLegacyIndexes)
            {
                continue;
            }
            ++exportedIndexes;
        }

        QString status =
            QStringLiteral("Generated %1 columns and %2 indexes.")
                .arg(tableSnapshot.columns.size())
                .arg(exportedIndexes);
        if (options.primaryKeyColumnName.trimmed().isEmpty())
        {
            status += QStringLiteral(" No explicit primary key selected.");
        }
        if (!options.includeLegacyIndexes &&
            std::any_of(tableSnapshot.indexes.begin(),
                        tableSnapshot.indexes.end(),
                        [](const sc::SCIndexDef& index) {
                            return index.sourceKind ==
                                   sc::SCSchemaSourceKind::LegacyHint;
                        }))
        {
            status += QStringLiteral(" Legacy hints are not exported.");
        }
        if (std::any_of(tableSnapshot.constraints.begin(),
                        tableSnapshot.constraints.end(),
                        [](const sc::SCConstraintDef& constraint) {
                            return constraint.kind !=
                                   sc::SCConstraintKind::PrimaryKey;
                        }))
        {
            status += QStringLiteral(
                " Snapshot contains constraints not exported in this DSL.");
        }

        statusLabel_->setText(status);
    }

    void SCSchemaTableDialog::RefreshPreview()
    {
        QString error;
        if (!ReloadSchema(&error))
        {
            outputEdit_->setPlainText(QStringLiteral("// ") + error);
            statusLabel_->setText(QStringLiteral("Failed to load current table: ") +
                                  error);
            return;
        }

        UpdateOutput();
    }

    void SCSchemaTableDialog::CopyOutput()
    {
        if (QApplication::clipboard() != nullptr)
        {
            QApplication::clipboard()->setText(outputEdit_->toPlainText());
            statusLabel_->setText(QStringLiteral("Output copied to clipboard."));
        }
    }

}  // namespace StableCore::Storage::Editor
