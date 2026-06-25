#include "SCSchemaTableDialog.h"

#include <algorithm>

#include <QApplication>
#include <QCheckBox>
#include <QClipboard>
#include <QFormLayout>
#include <QHBoxLayout>
#include <QLabel>
#include <QLineEdit>
#include <QListWidget>
#include <QPlainTextEdit>
#include <QPushButton>
#include <QStringList>
#include <QVBoxLayout>

#include "SCDatabaseSession.h"
#include "SCConstraintEditorDialog.h"
#include "SCSchemaTableGenerator.h"
#include "SCIndexEditorDialog.h"

namespace sc = StableCore::Storage;

namespace StableCore::Storage::Editor
{
    SCSchemaTableDialog::SCSchemaTableDialog(SCDatabaseSession* session,
                                             QWidget* parent)
        : QDialog(parent), session_(session)
    {
        setWindowTitle(QStringLiteral("Table Design"));
        resize(1080, 720);

        auto* layout = new QVBoxLayout(this);
        auto* introLabel = new QLabel(
            QStringLiteral(
                "Generate SC_SCHEMA_TABLE code from the current table."
                " Defaults are exported when present."
                " Primary keys and indexes are exported explicitly;"
                " legacy index hints are included by default and can be hidden."),
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
        form->addRow(QStringLiteral("Description"),
                     tableDescriptionEdit_);

        primaryKeyEdit_ = new QLineEdit(this);
        primaryKeyEdit_->setPlaceholderText(
            QStringLiteral("Explicit primary key column names"));
        form->addRow(QStringLiteral("Primary Key"), primaryKeyEdit_);

        includeLegacyIndexesCheck_ = new QCheckBox(
            QStringLiteral("Include legacy index hints"), this);
        includeLegacyIndexesCheck_->setChecked(true);
        form->addRow(QStringLiteral("Legacy Indexes"),
                     includeLegacyIndexesCheck_);

        legacyHintLabel_ = new QLabel(this);
        legacyHintLabel_->setWordWrap(true);
        legacyHintLabel_->setText(
            QStringLiteral("Legacy index hints will appear here."));
        form->addRow(QStringLiteral("Legacy Status"), legacyHintLabel_);
        layout->addLayout(form);

        auto* constraintGroup = new QHBoxLayout();
        constraintList_ = new QListWidget(this);
        constraintGroup->addWidget(constraintList_, 1);

        auto* constraintButtons = new QVBoxLayout();
        addConstraintButton_ = new QPushButton(QStringLiteral("Add"), this);
        editConstraintButton_ = new QPushButton(QStringLiteral("Edit"), this);
        removeConstraintButton_ =
            new QPushButton(QStringLiteral("Remove"), this);
        constraintButtons->addWidget(addConstraintButton_);
        constraintButtons->addWidget(editConstraintButton_);
        constraintButtons->addWidget(removeConstraintButton_);
        constraintButtons->addStretch(1);
        constraintGroup->addLayout(constraintButtons);
        layout->addLayout(constraintGroup);

        auto* constraintHintLabel = new QLabel(
            QStringLiteral(
                "Primary keys are edited above. Non-PK constraints are exported as comment metadata and can be imported back."),
            this);
        constraintHintLabel->setWordWrap(true);
        constraintHintLabel->setStyleSheet(QStringLiteral("color: #6a6a6a;"));
        layout->addWidget(constraintHintLabel);

        auto* indexGroup = new QHBoxLayout();
        indexList_ = new QListWidget(this);
        indexGroup->addWidget(indexList_, 1);

        auto* indexButtons = new QVBoxLayout();
        addIndexButton_ = new QPushButton(QStringLiteral("Add"), this);
        editIndexButton_ = new QPushButton(QStringLiteral("Edit"), this);
        removeIndexButton_ = new QPushButton(QStringLiteral("Remove"), this);
        indexButtons->addWidget(addIndexButton_);
        indexButtons->addWidget(editIndexButton_);
        indexButtons->addWidget(removeIndexButton_);
        indexButtons->addStretch(1);
        indexGroup->addLayout(indexButtons);
        layout->addLayout(indexGroup);

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
        auto* copyButton = new QPushButton(QStringLiteral("Copy Code"), this);
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
        connect(addConstraintButton_, &QPushButton::clicked, this,
                &SCSchemaTableDialog::AddConstraint);
        connect(editConstraintButton_, &QPushButton::clicked, this,
                &SCSchemaTableDialog::EditConstraint);
        connect(removeConstraintButton_, &QPushButton::clicked, this,
                &SCSchemaTableDialog::RemoveConstraint);
        connect(constraintList_, &QListWidget::itemSelectionChanged, this,
                [this]() {
                    const bool hasSelection =
                        !constraintList_->selectedItems().isEmpty();
                    editConstraintButton_->setEnabled(hasSelection);
                    removeConstraintButton_->setEnabled(hasSelection);
                });
        connect(addIndexButton_, &QPushButton::clicked, this,
                &SCSchemaTableDialog::AddIndex);
        connect(editIndexButton_, &QPushButton::clicked, this,
                &SCSchemaTableDialog::EditIndex);
        connect(removeIndexButton_, &QPushButton::clicked, this,
                &SCSchemaTableDialog::RemoveIndex);
        connect(indexList_, &QListWidget::itemSelectionChanged, this,
                [this]() {
                    bool hasSelection = indexList_->selectedItems().size() > 0;
                    editIndexButton_->setEnabled(hasSelection);
                    removeIndexButton_->setEnabled(hasSelection);
                });

        QString error;
        if (!ReloadSchema(&error))
        {
            tableNameLabel_->setText(QStringLiteral("-"));
            outputEdit_->setPlainText(QStringLiteral("// ") + error);
            statusLabel_->setText(
                QStringLiteral("Failed to load current table: ") + error);
            copyButton->setEnabled(false);
            refreshButton->setEnabled(false);
            constraintList_->setEnabled(false);
            addConstraintButton_->setEnabled(false);
            editConstraintButton_->setEnabled(false);
            removeConstraintButton_->setEnabled(false);
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
                *outError = QStringLiteral("No database open.");
            }
            return false;
        }

        tableName_ = session_->CurrentTableName();
        if (tableName_.isEmpty())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Please select a table first.");
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
                ? QStringLiteral("%1 legacy index hint(s) detected.")
                      .arg(legacyHintCount)
                : QStringLiteral("No legacy index hints detected."));

        UpdateConstraintList();
        UpdateIndexList();
        return true;
    }

    void SCSchemaTableDialog::UpdateOutput()
    {
        if (tableName_.isEmpty() || schemaSnapshot_.tables.empty())
        {
            outputEdit_->setPlainText(QStringLiteral("// No table selected."));
            statusLabel_->setText(
                QStringLiteral("No current table available."));
            return;
        }

        const sc::SCTableSchemaSnapshot& tableSnapshot =
            schemaSnapshot_.tables.front();
        SCSchemaTableExportOptions options;
        options.tableDescription = tableDescriptionEdit_->text().trimmed();
        options.primaryKeyColumnName = primaryKeyEdit_->text().trimmed();
        options.includeLegacyIndexes = includeLegacyIndexesCheck_ != nullptr &&
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

        QString status = QStringLiteral("Generated %1 column(s) and %2 index(es).")
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
            status += QStringLiteral(" Legacy hints were excluded.");
        }
        if (!tableSnapshot.constraints.empty())
        {
            status += QStringLiteral(
                " 约束元数据以注释形式导出。");
        }

        statusLabel_->setText(status);
    }

    void SCSchemaTableDialog::RefreshPreview()
    {
        QString error;
        if (!ReloadSchema(&error))
        {
            outputEdit_->setPlainText(QStringLiteral("// ") + error);
            statusLabel_->setText(
                QStringLiteral("Failed to load current table: ") + error);
            return;
        }

        UpdateOutput();
    }

    void SCSchemaTableDialog::CopyOutput()
    {
        if (QApplication::clipboard() != nullptr)
        {
            QApplication::clipboard()->setText(outputEdit_->toPlainText());
            statusLabel_->setText(
                QStringLiteral("Code copied to clipboard."));
        }
    }

    void SCSchemaTableDialog::AddConstraint()
    {
        if (session_ == nullptr || schemaSnapshot_.tables.empty())
        {
            return;
        }

        sc::SCConstraintDef newConstraint;
        newConstraint.kind = sc::SCConstraintKind::Unique;
        newConstraint.sourceKind = sc::SCSchemaSourceKind::Explicit;

        auto availableColumns = GetAvailableColumnNames();
        SCConstraintEditorDialog dialog(session_, newConstraint,
                                        availableColumns, this);
        if (dialog.exec() == QDialog::Accepted)
        {
            QString error;
            sc::SCConstraintDef constraint = dialog.GetConstraint();
            if (!session_->AddConstraint(constraint, &error))
            {
                statusLabel_->setText(
                    QStringLiteral("Failed to add constraint: ") + error);
                return;
            }

            RefreshPreview();
            statusLabel_->setText(
                QStringLiteral("Constraint added."));
        }
    }

    void SCSchemaTableDialog::EditConstraint()
    {
        QList<QListWidgetItem*> selected = constraintList_->selectedItems();
        if (selected.isEmpty())
        {
            return;
        }

        QString constraintName = selected.first()->data(Qt::UserRole).toString();
        if (constraintName.isEmpty())
        {
            return;
        }

        if (schemaSnapshot_.tables.empty())
        {
            return;
        }

        const auto& tableSnapshot = schemaSnapshot_.tables.front();
        auto it = std::find_if(
            tableSnapshot.constraints.begin(), tableSnapshot.constraints.end(),
            [&constraintName](const sc::SCConstraintDef& def) {
                return QString::fromStdWString(def.name)
                           .compare(constraintName, Qt::CaseInsensitive) == 0;
            });
        if (it == tableSnapshot.constraints.end())
        {
            statusLabel_->setText(
                QStringLiteral("Constraint not found: ") + constraintName);
            return;
        }

        if (it->kind == sc::SCConstraintKind::PrimaryKey)
        {
            statusLabel_->setText(
                QStringLiteral("Primary keys are edited in the field above."));
            return;
        }

        auto availableColumns = GetAvailableColumnNames();
        SCConstraintEditorDialog dialog(session_, *it, availableColumns, this);
        if (dialog.exec() == QDialog::Accepted)
        {
            QString error;
            sc::SCConstraintDef updatedConstraint = dialog.GetConstraint();
            if (!session_->UpdateConstraint(constraintName, updatedConstraint,
                                            &error))
            {
                statusLabel_->setText(
                    QStringLiteral("Failed to update constraint: ") + error);
                return;
            }

            RefreshPreview();
            statusLabel_->setText(
                QStringLiteral("Constraint updated."));
        }
    }

    void SCSchemaTableDialog::RemoveConstraint()
    {
        QList<QListWidgetItem*> selected = constraintList_->selectedItems();
        if (selected.isEmpty())
        {
            return;
        }

        QString constraintName = selected.first()->data(Qt::UserRole).toString();
        if (constraintName.isEmpty())
        {
            return;
        }

        QString error;
        if (!session_->RemoveConstraint(constraintName, &error))
        {
            statusLabel_->setText(
                QStringLiteral("Failed to remove constraint: ") + error);
            return;
        }

        RefreshPreview();
        statusLabel_->setText(
            QStringLiteral("Constraint removed."));
    }

    void SCSchemaTableDialog::UpdateConstraintList()
    {
        constraintList_->clear();

        if (schemaSnapshot_.tables.empty())
        {
            return;
        }

        const auto& tableSnapshot = schemaSnapshot_.tables.front();
        for (const sc::SCConstraintDef& constraint : tableSnapshot.constraints)
        {
            if (constraint.kind == sc::SCConstraintKind::PrimaryKey)
            {
                continue;
            }

            QString displayName = QString::fromStdWString(constraint.name);
            QString itemText;
            switch (constraint.kind)
            {
                case sc::SCConstraintKind::Unique:
                    itemText = QStringLiteral("Unique %1").arg(displayName);
                    break;
                case sc::SCConstraintKind::ForeignKey: {
                    itemText = QStringLiteral("Foreign Key %1").arg(displayName);
                    if (!constraint.referencedTable.empty())
                    {
                        itemText += QStringLiteral(" -> ") +
                                    QString::fromStdWString(
                                        constraint.referencedTable);
                    }
                    auto actionToText = [](sc::SCForeignKeyAction action) {
                        switch (action)
                        {
                            case sc::SCForeignKeyAction::NoAction:
                                return QStringLiteral("No Action");
                            case sc::SCForeignKeyAction::Cascade:
                                return QStringLiteral("Cascade");
                            case sc::SCForeignKeyAction::SetNull:
                                return QStringLiteral("Set Null");
                            case sc::SCForeignKeyAction::SetDefault:
                                return QStringLiteral("Set Default");
                            case sc::SCForeignKeyAction::Restrict:
                            default:
                                return QStringLiteral("Restrict");
                        }
                    };
                    itemText += QStringLiteral(" onDelete=") +
                                actionToText(constraint.onDelete) +
                                QStringLiteral(" onUpdate=") +
                                actionToText(constraint.onUpdate);
                    break;
                }
                case sc::SCConstraintKind::Check:
                    itemText = QStringLiteral("Check %1").arg(displayName);
                    if (!constraint.checkExpression.empty())
                    {
                        itemText += QStringLiteral(" expr=") +
                                    QString::fromStdWString(
                                        constraint.checkExpression).trimmed()
                                        .left(48);
                    }
                    break;
                case sc::SCConstraintKind::PrimaryKey:
                default:
                    continue;
            }

            if (!constraint.columns.empty())
            {
                QStringList columns;
                for (const std::wstring& column : constraint.columns)
                {
                    columns.push_back(QString::fromStdWString(column));
                }
                itemText += QStringLiteral(" [") +
                            columns.join(QStringLiteral(", ")) +
                            QStringLiteral("]");
            }

            if (constraint.kind == sc::SCConstraintKind::ForeignKey &&
                !constraint.referencedColumns.empty())
            {
                QStringList columns;
                for (const std::wstring& column : constraint.referencedColumns)
                {
                    columns.push_back(QString::fromStdWString(column));
                }
                itemText += QStringLiteral(" (") +
                            columns.join(QStringLiteral(", ")) +
                            QStringLiteral(")");
            }

            auto* item = new QListWidgetItem(itemText);
            item->setData(Qt::UserRole, displayName);
            constraintList_->addItem(item);
        }

        editConstraintButton_->setEnabled(false);
        removeConstraintButton_->setEnabled(false);
    }

    void SCSchemaTableDialog::AddIndex()
    {
        if (session_ == nullptr || !session_->IsOpen())
        {
            statusLabel_->setText(
                QStringLiteral("Open a database before adding indexes."));
            return;
        }
        if (schemaSnapshot_.tables.empty())
        {
            statusLabel_->setText(
                QStringLiteral("Please select a table before adding indexes."));
            return;
        }
        if (session_->HasPendingEdit())
        {
            statusLabel_->setText(
                QStringLiteral("Save or discard pending changes before editing the schema."));
            return;
        }

        sc::SCIndexDef newIndex;
        auto availableColumns = GetAvailableColumnNames();

        SCIndexEditorDialog dialog(newIndex, availableColumns, this);
        if (dialog.exec() == QDialog::Accepted)
        {
            QString error;
            sc::SCIndexDef index = dialog.GetIndex();
            if (!session_->AddIndex(index, &error))
            {
                statusLabel_->setText(
                    QStringLiteral("Failed to add index: ") + error);
                return;
            }

            RefreshPreview();
            statusLabel_->setText(
                QStringLiteral("Index added."));
        }
    }

    void SCSchemaTableDialog::EditIndex()
    {
        QList<QListWidgetItem*> selected = indexList_->selectedItems();
        if (selected.isEmpty())
        {
            return;
        }

        QString indexName = selected.first()->data(Qt::UserRole).toString();
        if (indexName.isEmpty())
        {
            return;
        }

        const auto& tableSnapshot = schemaSnapshot_.tables.front();
        auto it = std::find_if(tableSnapshot.indexes.begin(),
                               tableSnapshot.indexes.end(),
                               [&indexName](const sc::SCIndexDef& idx) {
                                   return QString::fromStdWString(idx.name)
                                              .compare(indexName,
                                                       Qt::CaseInsensitive) == 0;
                               });

        if (it == tableSnapshot.indexes.end())
        {
            statusLabel_->setText(
                QStringLiteral("Index not found: ") + indexName);
            return;
        }

        sc::SCIndexDef index = *it;
        auto availableColumns = GetAvailableColumnNames();

        SCIndexEditorDialog dialog(index, availableColumns, this);
        if (dialog.exec() == QDialog::Accepted)
        {
            QString error;
            sc::SCIndexDef updatedIndex = dialog.GetIndex();
            if (!session_->UpdateIndex(indexName, updatedIndex, &error))
            {
                statusLabel_->setText(
                    QStringLiteral("Failed to update index: ") + error);
                return;
            }

            RefreshPreview();
            statusLabel_->setText(
                QStringLiteral("Index updated."));
        }
    }

    void SCSchemaTableDialog::RemoveIndex()
    {
        QList<QListWidgetItem*> selected = indexList_->selectedItems();
        if (selected.isEmpty())
        {
            return;
        }

        QString indexName = selected.first()->data(Qt::UserRole).toString();
        if (indexName.isEmpty())
        {
            return;
        }

        QString error;
        if (!session_->RemoveIndex(indexName, &error))
        {
            statusLabel_->setText(
                QStringLiteral("Failed to remove index: ") + error);
            return;
        }

        RefreshPreview();
        statusLabel_->setText(
            QStringLiteral("Index removed."));
    }

    void SCSchemaTableDialog::UpdateIndexList()
    {
        indexList_->clear();

        if (schemaSnapshot_.tables.empty())
        {
            return;
        }

        const auto& tableSnapshot = schemaSnapshot_.tables.front();
        for (const sc::SCIndexDef& index : tableSnapshot.indexes)
        {
            QString displayName = QString::fromStdWString(index.name);
            QString columns;
            for (size_t i = 0; i < index.columns.size(); ++i)
            {
                if (i > 0)
                {
                    columns += QStringLiteral(", ");
                }
                columns += QString::fromStdWString(index.columns[i].columnName);
                if (index.columns[i].descending)
                {
                    columns += QStringLiteral(" (descending)");
                }
            }

            QString itemText = QStringLiteral("%1 - %2")
                                   .arg(displayName)
                                   .arg(columns);
            auto* item = new QListWidgetItem(itemText);
            item->setData(Qt::UserRole, displayName);
            indexList_->addItem(item);
        }

        editIndexButton_->setEnabled(false);
        removeIndexButton_->setEnabled(false);
    }

    std::vector<std::wstring> SCSchemaTableDialog::GetAvailableColumnNames() const
    {
        std::vector<std::wstring> names;
        if (schemaSnapshot_.tables.empty())
        {
            return names;
        }

        const auto& tableSnapshot = schemaSnapshot_.tables.front();
        for (const sc::SCColumnDef& column : tableSnapshot.columns)
        {
            names.push_back(column.name);
        }
        return names;
    }

}  // namespace StableCore::Storage::Editor
