#include "SCTableDesignPane.h"

#include <algorithm>

#include <QAction>
#include <QApplication>
#include <QCheckBox>
#include <QClipboard>
#include <QFormLayout>
#include <QGroupBox>
#include <QHeaderView>
#include <QHBoxLayout>
#include <QLabel>
#include <QLineEdit>
#include <QMessageBox>
#include <QMenu>
#include <QPoint>
#include <QPlainTextEdit>
#include <QPushButton>
#include <QSignalBlocker>
#include <QSplitter>
#include <QTabWidget>
#include <QTreeWidget>
#include <QVBoxLayout>

#include "SCAddColumnDialog.h"
#include "SCConstraintEditorDialog.h"
#include "SCDatabaseSession.h"
#include "SCIndexEditorDialog.h"
#include "SCSchemaTableGenerator.h"

namespace sc = StableCore::Storage;

namespace StableCore::Storage::Editor
{
    namespace
    {
        QString ToQString(const std::wstring& text)
        {
            return QString::fromStdWString(text);
        }

        QString ConstraintKindToText(sc::SCConstraintKind kind)
        {
            switch (kind)
            {
                case sc::SCConstraintKind::PrimaryKey:
                    return QStringLiteral("Primary Key");
                case sc::SCConstraintKind::Unique:
                    return QStringLiteral("Unique");
                case sc::SCConstraintKind::ForeignKey:
                    return QStringLiteral("Foreign Key");
                case sc::SCConstraintKind::Check:
                    return QStringLiteral("Check");
                default:
                    return QStringLiteral("Unknown");
            }
        }

        QString ForeignKeyActionToText(sc::SCForeignKeyAction action)
        {
            switch (action)
            {
                case sc::SCForeignKeyAction::NoAction:
                    return QStringLiteral("NoAction");
                case sc::SCForeignKeyAction::Cascade:
                    return QStringLiteral("Cascade");
                case sc::SCForeignKeyAction::SetNull:
                    return QStringLiteral("SetNull");
                case sc::SCForeignKeyAction::SetDefault:
                    return QStringLiteral("SetDefault");
                case sc::SCForeignKeyAction::Restrict:
                default:
                    return QStringLiteral("Restrict");
            }
        }

        QString SourceKindToText(sc::SCSchemaSourceKind kind)
        {
            switch (kind)
            {
                case sc::SCSchemaSourceKind::LegacyHint:
                    return QStringLiteral("LegacyHint");
                case sc::SCSchemaSourceKind::Explicit:
                    return QStringLiteral("Explicit");
                default:
                    return QStringLiteral("Unknown");
            }
        }
    }  // namespace

    SCTableDesignPane::SCTableDesignPane(SCDatabaseSession* session,
                                         QWidget* parent)
        : QWidget(parent), session_(session)
    {
        BuildUi();
    }

    void SCTableDesignPane::BuildUi()
    {
        auto* rootLayout = new QVBoxLayout(this);
        rootLayout->setContentsMargins(0, 0, 0, 0);
        rootLayout->setSpacing(8);

        auto* overviewGroup = new QGroupBox(QStringLiteral("Table Overview"), this);
        auto* overviewLayout = new QFormLayout(overviewGroup);

        tableNameValueLabel_ = new QLabel(QStringLiteral("-"), overviewGroup);
        primaryKeyValueLabel_ = new QLabel(QStringLiteral("-"), overviewGroup);
        statsValueLabel_ = new QLabel(QStringLiteral("Columns: 0 | Constraints: 0 | Indexes: 0"),
                                      overviewGroup);
        legacyHintValueLabel_ = new QLabel(QStringLiteral("No legacy index hints detected."),
                                           overviewGroup);
        tableDescriptionEdit_ = new QLineEdit(overviewGroup);
        tableDescriptionEdit_->setPlaceholderText(QStringLiteral("Optional schema text description"));
        includeLegacyIndexesCheck_ = new QCheckBox(QStringLiteral("Include legacy index hints in generated schema"),
                                                   overviewGroup);
        includeLegacyIndexesCheck_->setChecked(true);

        overviewLayout->addRow(QStringLiteral("Table"), tableNameValueLabel_);
        overviewLayout->addRow(QStringLiteral("Primary Key"), primaryKeyValueLabel_);
        overviewLayout->addRow(QStringLiteral("Description"), tableDescriptionEdit_);
        overviewLayout->addRow(QStringLiteral("Legacy Indexes"), includeLegacyIndexesCheck_);
        overviewLayout->addRow(QStringLiteral("Stats"), statsValueLabel_);
        overviewLayout->addRow(QStringLiteral("Legacy Status"), legacyHintValueLabel_);
        rootLayout->addWidget(overviewGroup);

        auto* splitter = new QSplitter(Qt::Horizontal, this);

        auto* columnsPanel = new QWidget(splitter);
        auto* columnsLayout = new QVBoxLayout(columnsPanel);
        columnsLayout->setContentsMargins(0, 0, 0, 0);
        columnsLayout->setSpacing(6);
        columnsLayout->addWidget(new QLabel(QStringLiteral("Columns"), columnsPanel));

        columnsTree_ = new QTreeWidget(columnsPanel);
        columnsTree_->setHeaderLabels({QStringLiteral("Name"),
                                       QStringLiteral("Display"),
                                       QStringLiteral("Type"),
                                       QStringLiteral("Nullable"),
                                       QStringLiteral("Default"),
                                       QStringLiteral("Kind")});
        columnsTree_->setAlternatingRowColors(true);
        columnsTree_->setRootIsDecorated(false);
        columnsTree_->header()->setStretchLastSection(false);
        columnsTree_->header()->setSectionResizeMode(QHeaderView::ResizeToContents);
        columnsTree_->setContextMenuPolicy(Qt::CustomContextMenu);
        columnsLayout->addWidget(columnsTree_, 1);

        auto* columnButtons = new QHBoxLayout();
        addColumnButton_ = new QPushButton(QStringLiteral("Add Column"), columnsPanel);
        editColumnButton_ = new QPushButton(QStringLiteral("Edit Column"), columnsPanel);
        deleteColumnButton_ = new QPushButton(QStringLiteral("Delete Column"), columnsPanel);
        columnButtons->addWidget(addColumnButton_);
        columnButtons->addWidget(editColumnButton_);
        columnButtons->addWidget(deleteColumnButton_);
        columnButtons->addStretch(1);
        columnsLayout->addLayout(columnButtons);

        auto* structurePanel = new QWidget(splitter);
        auto* structureLayout = new QVBoxLayout(structurePanel);
        structureLayout->setContentsMargins(0, 0, 0, 0);
        structureLayout->setSpacing(6);
        structureLayout->addWidget(new QLabel(QStringLiteral("Structure"), structurePanel));

        structureTabs_ = new QTabWidget(structurePanel);

        auto* constraintsPage = new QWidget(structureTabs_);
        auto* constraintsLayout = new QVBoxLayout(constraintsPage);
        constraintsLayout->setContentsMargins(0, 0, 0, 0);
        constraintsLayout->setSpacing(6);
        constraintsTree_ = new QTreeWidget(constraintsPage);
        constraintsTree_->setHeaderLabels({QStringLiteral("Name"),
                                           QStringLiteral("Type"),
                                           QStringLiteral("Columns"),
                                           QStringLiteral("Target"),
                                           QStringLiteral("Actions"),
                                           QStringLiteral("Source")});
        constraintsTree_->setAlternatingRowColors(true);
        constraintsTree_->setRootIsDecorated(false);
        constraintsTree_->header()->setStretchLastSection(false);
        constraintsTree_->header()->setSectionResizeMode(QHeaderView::ResizeToContents);
        constraintsLayout->addWidget(constraintsTree_, 1);

        auto* constraintButtons = new QHBoxLayout();
        auto* addConstraintButton = new QPushButton(QStringLiteral("Add Constraint"), constraintsPage);
        auto* editConstraintButton = new QPushButton(QStringLiteral("Edit Constraint"), constraintsPage);
        auto* removeConstraintButton = new QPushButton(QStringLiteral("Remove Constraint"), constraintsPage);
        constraintButtons->addWidget(addConstraintButton);
        constraintButtons->addWidget(editConstraintButton);
        constraintButtons->addWidget(removeConstraintButton);
        constraintButtons->addStretch(1);
        constraintsLayout->addLayout(constraintButtons);
        structureTabs_->addTab(constraintsPage, QStringLiteral("Constraints"));

        auto* indexesPage = new QWidget(structureTabs_);
        auto* indexesLayout = new QVBoxLayout(indexesPage);
        indexesLayout->setContentsMargins(0, 0, 0, 0);
        indexesLayout->setSpacing(6);
        indexesTree_ = new QTreeWidget(indexesPage);
        indexesTree_->setHeaderLabels({QStringLiteral("Name"),
                                       QStringLiteral("Columns"),
                                       QStringLiteral("Descending"),
                                       QStringLiteral("Source"),
                                       QStringLiteral("Badge")});
        indexesTree_->setAlternatingRowColors(true);
        indexesTree_->setRootIsDecorated(false);
        indexesTree_->header()->setStretchLastSection(false);
        indexesTree_->header()->setSectionResizeMode(QHeaderView::ResizeToContents);
        indexesLayout->addWidget(indexesTree_, 1);

        auto* indexButtons = new QHBoxLayout();
        auto* addIndexButton = new QPushButton(QStringLiteral("Add Index"), indexesPage);
        auto* editIndexButton = new QPushButton(QStringLiteral("Edit Index"), indexesPage);
        auto* removeIndexButton = new QPushButton(QStringLiteral("Remove Index"), indexesPage);
        indexButtons->addWidget(addIndexButton);
        indexButtons->addWidget(editIndexButton);
        indexButtons->addWidget(removeIndexButton);
        indexButtons->addStretch(1);
        indexesLayout->addLayout(indexButtons);
        structureTabs_->addTab(indexesPage, QStringLiteral("Indexes"));

        structureLayout->addWidget(structureTabs_, 1);
        splitter->addWidget(columnsPanel);
        splitter->addWidget(structurePanel);
        splitter->setStretchFactor(0, 3);
        splitter->setStretchFactor(1, 4);
        rootLayout->addWidget(splitter, 1);

        auto* previewGroup = new QGroupBox(QStringLiteral("Generated Schema"), this);
        auto* previewLayout = new QVBoxLayout(previewGroup);
        previewEdit_ = new QPlainTextEdit(previewGroup);
        previewEdit_->setReadOnly(true);
        previewEdit_->setLineWrapMode(QPlainTextEdit::NoWrap);
        previewLayout->addWidget(previewEdit_, 1);

        auto* previewButtons = new QHBoxLayout();
        auto* refreshPreviewButton = new QPushButton(QStringLiteral("Refresh Preview"), previewGroup);
        auto* copyPreviewButton = new QPushButton(QStringLiteral("Copy"), previewGroup);
        previewButtons->addWidget(refreshPreviewButton);
        previewButtons->addWidget(copyPreviewButton);
        previewButtons->addStretch(1);
        previewLayout->addLayout(previewButtons);
        rootLayout->addWidget(previewGroup, 1);

        statusLabel_ = new QLabel(QStringLiteral("Ready"), this);
        rootLayout->addWidget(statusLabel_);

        connect(tableDescriptionEdit_, &QLineEdit::textChanged, this,
                &SCTableDesignPane::UpdatePreview);
        connect(includeLegacyIndexesCheck_, &QCheckBox::toggled, this,
                &SCTableDesignPane::UpdatePreview);
        connect(refreshPreviewButton, &QPushButton::clicked, this,
                &SCTableDesignPane::RefreshPreview);
        connect(copyPreviewButton, &QPushButton::clicked, this,
                &SCTableDesignPane::CopyPreview);

        connect(addColumnButton_, &QPushButton::clicked, this,
                &SCTableDesignPane::AddColumn);
        connect(editColumnButton_, &QPushButton::clicked, this,
                &SCTableDesignPane::EditColumn);
        connect(deleteColumnButton_, &QPushButton::clicked, this,
                &SCTableDesignPane::RemoveColumn);

        connect(columnsTree_, &QTreeWidget::itemSelectionChanged, this, [this]() {
            UpdateColumnActionState();
        });
        connect(columnsTree_, &QWidget::customContextMenuRequested, this,
                &SCTableDesignPane::OnColumnsContextMenuRequested);

        connect(addConstraintButton, &QPushButton::clicked, this,
                &SCTableDesignPane::AddConstraint);
        connect(editConstraintButton, &QPushButton::clicked, this,
                &SCTableDesignPane::EditConstraint);
        connect(removeConstraintButton, &QPushButton::clicked, this,
                &SCTableDesignPane::RemoveConstraint);
        connect(addIndexButton, &QPushButton::clicked, this,
                &SCTableDesignPane::AddIndex);
        connect(editIndexButton, &QPushButton::clicked, this,
                &SCTableDesignPane::EditIndex);
        connect(removeIndexButton, &QPushButton::clicked, this,
                &SCTableDesignPane::RemoveIndex);

        UpdateColumnActionState();
    }

    void SCTableDesignPane::Refresh()
    {
        tableName_.clear();
        schemaSnapshot_.tables.clear();

        if (session_ == nullptr || !session_->IsOpen())
        {
            tableNameValueLabel_->setText(QStringLiteral("-"));
            primaryKeyValueLabel_->setText(QStringLiteral("-"));
            statsValueLabel_->setText(QStringLiteral("Columns: 0 | Constraints: 0 | Indexes: 0"));
            legacyHintValueLabel_->setText(QStringLiteral("No database open."));
            columnsTree_->clear();
            constraintsTree_->clear();
            indexesTree_->clear();
            previewEdit_->setPlainText(QStringLiteral("// No database open."));
            UpdateColumnActionState();
            SetStatus(QStringLiteral("Open a database to start designing a table."));
            return;
        }

        tableName_ = session_->CurrentTableName();
        if (tableName_.isEmpty())
        {
            tableNameValueLabel_->setText(QStringLiteral("-"));
            primaryKeyValueLabel_->setText(QStringLiteral("-"));
            statsValueLabel_->setText(QStringLiteral("Columns: 0 | Constraints: 0 | Indexes: 0"));
            legacyHintValueLabel_->setText(QStringLiteral("No table selected."));
            columnsTree_->clear();
            constraintsTree_->clear();
            indexesTree_->clear();
            previewEdit_->setPlainText(QStringLiteral("// Select a table to open the design workspace."));
            UpdateColumnActionState();
            SetStatus(QStringLiteral("No table selected."));
            return;
        }

        QString error;
        if (!session_->BuildSchemaSnapshot(&schemaSnapshot_, &error) ||
            schemaSnapshot_.tables.empty())
        {
            columnsTree_->clear();
            constraintsTree_->clear();
            indexesTree_->clear();
            previewEdit_->setPlainText(QStringLiteral("// ") + error);
            UpdateColumnActionState();
            SetStatus(QStringLiteral("Failed to load schema: ") + error);
            return;
        }

        UpdateOverview();
        UpdateColumns();
        UpdateConstraints();
        UpdateIndexes();
        UpdatePreview();
        UpdateColumnActionState();
    }

    QString SCTableDesignPane::CurrentColumnName() const
    {
        if (columnsTree_ == nullptr)
        {
            return {};
        }

        QTreeWidgetItem* current = columnsTree_->currentItem();
        if (current == nullptr)
        {
            return {};
        }

        return current->text(0);
    }

    void SCTableDesignPane::SelectColumnByName(const QString& columnName)
    {
        if (columnsTree_ == nullptr || columnName.isEmpty())
        {
            return;
        }

        const QSignalBlocker blocker(columnsTree_);
        for (int index = 0; index < columnsTree_->topLevelItemCount(); ++index)
        {
            QTreeWidgetItem* item = columnsTree_->topLevelItem(index);
            if (item != nullptr &&
                item->text(0).compare(columnName, Qt::CaseInsensitive) == 0)
            {
                columnsTree_->setCurrentItem(item);
                return;
            }
        }
    }

    void SCTableDesignPane::AddColumn()
    {
        if (session_ == nullptr || !session_->IsOpen())
        {
            SetStatus(QStringLiteral("Open a database to add columns."));
            return;
        }
        if (tableName_.isEmpty())
        {
            SetStatus(QStringLiteral("Select a table before adding columns."));
            return;
        }
        if (session_->HasPendingEdit())
        {
            SetStatus(QStringLiteral(
                "Save or discard pending changes before editing the schema."));
            return;
        }

        bool tableHasRecords = false;
        QString error;
        if (!session_->CurrentTableHasRecords(&tableHasRecords, &error))
        {
            SetStatus(QStringLiteral("Failed to inspect table: ") + error);
            return;
        }

        SCAddColumnDialog dialog(session_, this);
        dialog.SetCurrentTableHasRecords(tableHasRecords);
        if (dialog.exec() != QDialog::Accepted)
        {
            return;
        }

        const sc::SCColumnDef column = dialog.BuildColumnDef();
        if (column.name.empty())
        {
            SetStatus(QStringLiteral("Column name is required."));
            return;
        }

        if (!session_->AddColumn(column, &error))
        {
            SetStatus(QStringLiteral("Add column failed: ") + error);
            return;
        }

        const QString newColumnName = ToQString(column.name);
        Refresh();
        SelectColumnByName(newColumnName);
        SetStatus(QStringLiteral("Column added: ") + newColumnName);
    }

    void SCTableDesignPane::EditColumn()
    {
        const QString columnName = CurrentColumnName();
        if (columnName.isEmpty())
        {
            SetStatus(QStringLiteral("Select a column before editing."));
            return;
        }
        if (session_ == nullptr || !session_->IsOpen())
        {
            SetStatus(QStringLiteral("Open a database before editing columns."));
            return;
        }
        if (session_->HasPendingEdit())
        {
            SetStatus(QStringLiteral(
                "Save or discard pending changes before editing the schema."));
            return;
        }

        sc::SCColumnDef existing;
        QString error;
        if (!session_->GetColumnDef(columnName, &existing, &error))
        {
            SetStatus(QStringLiteral("Failed to load column: ") + error);
            return;
        }

        bool tableHasRecords = false;
        if (!session_->CurrentTableHasRecords(&tableHasRecords, &error))
        {
            SetStatus(QStringLiteral("Failed to inspect table: ") + error);
            return;
        }

        SCAddColumnDialog dialog(session_, existing, this);
        dialog.SetCurrentTableHasRecords(tableHasRecords);
        if (dialog.exec() != QDialog::Accepted)
        {
            return;
        }

        const sc::SCColumnDef updated = dialog.BuildColumnDef();
        if (updated.name.empty())
        {
            SetStatus(QStringLiteral("Column name is required."));
            return;
        }

        if (!session_->UpdateColumn(columnName, updated, &error))
        {
            SetStatus(QStringLiteral("Update column failed: ") + error);
            return;
        }

        const QString updatedColumnName = ToQString(updated.name);
        Refresh();
        SelectColumnByName(updatedColumnName);
        SetStatus(QStringLiteral("Column updated: ") + updatedColumnName);
    }

    void SCTableDesignPane::RemoveColumn()
    {
        const QString columnName = CurrentColumnName();
        if (columnName.isEmpty())
        {
            SetStatus(QStringLiteral("Select a column before deleting."));
            return;
        }
        if (session_ == nullptr || !session_->IsOpen())
        {
            SetStatus(QStringLiteral("Open a database before deleting columns."));
            return;
        }

        const QMessageBox::StandardButton answer = QMessageBox::question(
            this, QStringLiteral("Delete Column"),
            QStringLiteral("Delete column \"%1\"?").arg(columnName));
        if (answer != QMessageBox::Yes)
        {
            return;
        }

        QString error;
        if (!session_->RemoveColumn(columnName, &error))
        {
            SetStatus(QStringLiteral("Remove column failed: ") + error);
            return;
        }

        Refresh();
        SetStatus(QStringLiteral("Column removed: ") + columnName);
    }

    void SCTableDesignPane::UpdateOverview()
    {
        if (schemaSnapshot_.tables.empty())
        {
            return;
        }

        const sc::SCTableSchemaSnapshot& tableSnapshot = schemaSnapshot_.tables.front();
        tableNameValueLabel_->setText(tableName_);

        QString primaryKeyText = QStringLiteral("-");
        int primaryKeyCount = 0;
        int uniqueCount = 0;
        int foreignKeyCount = 0;
        int checkCount = 0;
        int legacyHintCount = 0;

        for (const sc::SCConstraintDef& constraint : tableSnapshot.constraints)
        {
            switch (constraint.kind)
            {
                case sc::SCConstraintKind::PrimaryKey:
                    ++primaryKeyCount;
                    if (!constraint.columns.empty())
                    {
                        QStringList columns;
                        for (const std::wstring& column : constraint.columns)
                        {
                            columns.push_back(ToQString(column));
                        }
                        primaryKeyText = columns.join(QStringLiteral(", "));
                    }
                    break;
                case sc::SCConstraintKind::Unique:
                    ++uniqueCount;
                    break;
                case sc::SCConstraintKind::ForeignKey:
                    ++foreignKeyCount;
                    break;
                case sc::SCConstraintKind::Check:
                    ++checkCount;
                    break;
                default:
                    break;
            }
        }

        for (const sc::SCIndexDef& index : tableSnapshot.indexes)
        {
            if (index.sourceKind == sc::SCSchemaSourceKind::LegacyHint)
            {
                ++legacyHintCount;
            }
        }

        primaryKeyValueLabel_->setText(primaryKeyText);
        statsValueLabel_->setText(
            QStringLiteral("Columns: %1 | Constraints: PK %2 / Unique %3 / FK %4 / Check %5 | Indexes: %6")
                .arg(tableSnapshot.columns.size())
                .arg(primaryKeyCount)
                .arg(uniqueCount)
                .arg(foreignKeyCount)
                .arg(checkCount)
                .arg(tableSnapshot.indexes.size()));
        legacyHintValueLabel_->setText(
            legacyHintCount > 0
                ? QStringLiteral("%1 legacy index hint(s) detected.").arg(legacyHintCount)
                : QStringLiteral("No legacy index hints detected."));
    }

    void SCTableDesignPane::UpdateColumns()
    {
        columnsTree_->clear();
        if (schemaSnapshot_.tables.empty())
        {
            return;
        }

        const sc::SCTableSchemaSnapshot& tableSnapshot = schemaSnapshot_.tables.front();
        for (const sc::SCColumnDef& column : tableSnapshot.columns)
        {
            auto* item = new QTreeWidgetItem(columnsTree_);
            item->setText(0, ToQString(column.name));
            item->setText(1, ToQString(column.displayName));
            item->setText(2, QString::number(static_cast<int>(column.valueKind)));
            item->setText(3, column.nullable ? QStringLiteral("Yes") : QStringLiteral("No"));
            item->setText(4, column.defaultValue.IsNull() ? QStringLiteral("-")
                                                          : QStringLiteral("(set)"));
            item->setText(5, column.columnKind == sc::ColumnKind::Relation
                                   ? QStringLiteral("Relation")
                                   : QStringLiteral("Fact"));
        }
    }

    void SCTableDesignPane::UpdateConstraints()
    {
        constraintsTree_->clear();
        if (schemaSnapshot_.tables.empty())
        {
            return;
        }

        const sc::SCTableSchemaSnapshot& tableSnapshot = schemaSnapshot_.tables.front();
        for (const sc::SCConstraintDef& constraint : tableSnapshot.constraints)
        {
            auto* item = new QTreeWidgetItem(constraintsTree_);
            const QString name = ToQString(constraint.name);
            QStringList columns;
            for (const std::wstring& column : constraint.columns)
            {
                columns.push_back(ToQString(column));
            }

            QString target;
            QString actions;
            if (constraint.kind == sc::SCConstraintKind::ForeignKey)
            {
                target = ToQString(constraint.referencedTable);
                if (!constraint.referencedColumns.empty())
                {
                    QStringList referencedColumns;
                    for (const std::wstring& referencedColumn : constraint.referencedColumns)
                    {
                        referencedColumns.push_back(ToQString(referencedColumn));
                    }
                    if (!target.isEmpty())
                    {
                        target += QStringLiteral(" (") +
                                  referencedColumns.join(QStringLiteral(", ")) +
                                  QStringLiteral(")");
                    }
                }
                actions = QStringLiteral("Delete=%1 | Update=%2")
                              .arg(ForeignKeyActionToText(constraint.onDelete))
                              .arg(ForeignKeyActionToText(constraint.onUpdate));
            }
            else if (constraint.kind == sc::SCConstraintKind::Check)
            {
                actions = ToQString(constraint.checkExpression).trimmed().left(64);
            }

            item->setText(0, name);
            item->setText(1, ConstraintKindToText(constraint.kind));
            item->setText(2, columns.join(QStringLiteral(", ")));
            item->setText(3, target);
            item->setText(4, actions);
            item->setText(5, SourceKindToText(constraint.sourceKind));
            item->setData(0, Qt::UserRole, name);
        }
    }

    void SCTableDesignPane::UpdateIndexes()
    {
        indexesTree_->clear();
        if (schemaSnapshot_.tables.empty())
        {
            return;
        }

        const sc::SCTableSchemaSnapshot& tableSnapshot = schemaSnapshot_.tables.front();
        for (const sc::SCIndexDef& index : tableSnapshot.indexes)
        {
            auto* item = new QTreeWidgetItem(indexesTree_);
            QStringList columns;
            QStringList descendingColumns;
            for (const sc::SCIndexColumnDef& column : index.columns)
            {
                const QString columnName = ToQString(column.columnName);
                columns.push_back(columnName);
                if (column.descending)
                {
                    descendingColumns.push_back(columnName);
                }
            }

            const QString name = ToQString(index.name);
            item->setText(0, name);
            item->setText(1, columns.join(QStringLiteral(", ")));
            item->setText(2, descendingColumns.isEmpty()
                                 ? QStringLiteral("-")
                                 : descendingColumns.join(QStringLiteral(", ")));
            item->setText(3, SourceKindToText(index.sourceKind));
            item->setText(4, index.sourceKind == sc::SCSchemaSourceKind::LegacyHint
                                   ? QStringLiteral("Legacy")
                                   : QStringLiteral("Explicit"));
            item->setData(0, Qt::UserRole, name);
        }
    }

    void SCTableDesignPane::UpdatePreview()
    {
        if (schemaSnapshot_.tables.empty())
        {
            previewEdit_->setPlainText(QStringLiteral("// No table selected."));
            return;
        }

        SCSchemaTableExportOptions options;
        options.tableDescription = tableDescriptionEdit_->text().trimmed();
        options.includeLegacyIndexes = includeLegacyIndexesCheck_->isChecked();

        const sc::SCTableSchemaSnapshot& tableSnapshot = schemaSnapshot_.tables.front();
        previewEdit_->setPlainText(BuildSchemaTableCode(tableSnapshot, options));
        SetStatus(QStringLiteral("Design workspace refreshed."));
    }

    void SCTableDesignPane::RefreshPreview()
    {
        Refresh();
    }

    void SCTableDesignPane::CopyPreview()
    {
        if (QApplication::clipboard() != nullptr)
        {
            QApplication::clipboard()->setText(previewEdit_->toPlainText());
            SetStatus(QStringLiteral("Generated schema copied."));
        }
    }

    void SCTableDesignPane::AddConstraint()
    {
        if (schemaSnapshot_.tables.empty())
        {
            return;
        }

        sc::SCConstraintDef newConstraint;
        newConstraint.kind = sc::SCConstraintKind::Unique;
        newConstraint.sourceKind = sc::SCSchemaSourceKind::Explicit;

        SCConstraintEditorDialog dialog(session_, newConstraint,
                                        GetAvailableColumnNames(), this);
        if (dialog.exec() != QDialog::Accepted)
        {
            return;
        }

        QString error;
        if (!session_->AddConstraint(dialog.GetConstraint(), &error))
        {
            SetStatus(QStringLiteral("Add constraint failed: ") + error);
            return;
        }

        Refresh();
        SetStatus(QStringLiteral("Constraint added."));
    }

    void SCTableDesignPane::EditConstraint()
    {
        const QString constraintName = CurrentConstraintName();
        if (constraintName.isEmpty() || schemaSnapshot_.tables.empty())
        {
            return;
        }

        const sc::SCTableSchemaSnapshot& tableSnapshot = schemaSnapshot_.tables.front();
        const auto it = std::find_if(
            tableSnapshot.constraints.begin(), tableSnapshot.constraints.end(),
            [&constraintName](const sc::SCConstraintDef& constraint) {
                return QString::fromStdWString(constraint.name)
                           .compare(constraintName, Qt::CaseInsensitive) == 0;
            });
        if (it == tableSnapshot.constraints.end())
        {
            SetStatus(QStringLiteral("Constraint not found: ") + constraintName);
            return;
        }

        SCConstraintEditorDialog dialog(session_, *it, GetAvailableColumnNames(), this);
        if (dialog.exec() != QDialog::Accepted)
        {
            return;
        }

        QString error;
        if (!session_->UpdateConstraint(constraintName, dialog.GetConstraint(), &error))
        {
            SetStatus(QStringLiteral("Update constraint failed: ") + error);
            return;
        }

        Refresh();
        SetStatus(QStringLiteral("Constraint updated."));
    }

    void SCTableDesignPane::RemoveConstraint()
    {
        const QString constraintName = CurrentConstraintName();
        if (constraintName.isEmpty())
        {
            return;
        }

        QString error;
        if (!session_->RemoveConstraint(constraintName, &error))
        {
            SetStatus(QStringLiteral("Remove constraint failed: ") + error);
            return;
        }

        Refresh();
        SetStatus(QStringLiteral("Constraint removed."));
    }

    void SCTableDesignPane::AddIndex()
    {
        if (session_ == nullptr || !session_->IsOpen())
        {
            SetStatus(QStringLiteral("Open a database to add indexes."));
            return;
        }
        if (tableName_.isEmpty())
        {
            SetStatus(QStringLiteral("Select a table before adding indexes."));
            return;
        }
        if (session_->HasPendingEdit())
        {
            SetStatus(QStringLiteral(
                "Save or discard pending changes before editing the schema."));
            return;
        }

        sc::SCIndexDef index;
        SCIndexEditorDialog dialog(index, GetAvailableColumnNames(), this);
        if (dialog.exec() != QDialog::Accepted)
        {
            return;
        }

        QString error;
        if (!session_->AddIndex(dialog.GetIndex(), &error))
        {
            SetStatus(QStringLiteral("Add index failed: ") + error);
            return;
        }

        Refresh();
        SetStatus(QStringLiteral("Index added."));
    }

    void SCTableDesignPane::EditIndex()
    {
        const QString indexName = CurrentIndexName();
        if (indexName.isEmpty() || schemaSnapshot_.tables.empty())
        {
            return;
        }

        const sc::SCTableSchemaSnapshot& tableSnapshot = schemaSnapshot_.tables.front();
        const auto it = std::find_if(
            tableSnapshot.indexes.begin(), tableSnapshot.indexes.end(),
            [&indexName](const sc::SCIndexDef& index) {
                return QString::fromStdWString(index.name)
                           .compare(indexName, Qt::CaseInsensitive) == 0;
            });
        if (it == tableSnapshot.indexes.end())
        {
            SetStatus(QStringLiteral("Index not found: ") + indexName);
            return;
        }

        SCIndexEditorDialog dialog(*it, GetAvailableColumnNames(), this);
        if (dialog.exec() != QDialog::Accepted)
        {
            return;
        }

        QString error;
        if (!session_->UpdateIndex(indexName, dialog.GetIndex(), &error))
        {
            SetStatus(QStringLiteral("Update index failed: ") + error);
            return;
        }

        Refresh();
        SetStatus(QStringLiteral("Index updated."));
    }

    void SCTableDesignPane::RemoveIndex()
    {
        const QString indexName = CurrentIndexName();
        if (indexName.isEmpty())
        {
            return;
        }

        QString error;
        if (!session_->RemoveIndex(indexName, &error))
        {
            SetStatus(QStringLiteral("Remove index failed: ") + error);
            return;
        }

        Refresh();
        SetStatus(QStringLiteral("Index removed."));
    }

    void SCTableDesignPane::OnColumnsContextMenuRequested(const QPoint& pos)
    {
        if (columnsTree_ == nullptr)
        {
            return;
        }

        if (QTreeWidgetItem* item = columnsTree_->itemAt(pos); item != nullptr)
        {
            columnsTree_->setCurrentItem(item);
        }

        QMenu menu(columnsTree_);
        menu.addAction(QStringLiteral("Add Column"), this,
                       &SCTableDesignPane::AddColumn);
        QAction* editAction =
            menu.addAction(QStringLiteral("Edit Column"), this,
                           &SCTableDesignPane::EditColumn);
        QAction* removeAction =
            menu.addAction(QStringLiteral("Delete Column"), this,
                           &SCTableDesignPane::RemoveColumn);
        const bool hasSelection = !CurrentColumnName().isEmpty();
        editAction->setEnabled(hasSelection);
        removeAction->setEnabled(hasSelection);
        menu.exec(columnsTree_->mapToGlobal(pos));
    }

    void SCTableDesignPane::UpdateColumnActionState()
    {
        const bool hasSelection = !CurrentColumnName().isEmpty();
        if (editColumnButton_ != nullptr)
        {
            editColumnButton_->setEnabled(hasSelection);
        }
        if (deleteColumnButton_ != nullptr)
        {
            deleteColumnButton_->setEnabled(hasSelection);
        }
    }

    QString SCTableDesignPane::CurrentConstraintName() const
    {
        const QTreeWidgetItem* item = constraintsTree_->currentItem();
        return item != nullptr ? item->data(0, Qt::UserRole).toString() : QString();
    }

    QString SCTableDesignPane::CurrentIndexName() const
    {
        const QTreeWidgetItem* item = indexesTree_->currentItem();
        return item != nullptr ? item->data(0, Qt::UserRole).toString() : QString();
    }

    std::vector<std::wstring> SCTableDesignPane::GetAvailableColumnNames() const
    {
        std::vector<std::wstring> names;
        if (schemaSnapshot_.tables.empty())
        {
            return names;
        }

        const sc::SCTableSchemaSnapshot& tableSnapshot = schemaSnapshot_.tables.front();
        for (const sc::SCColumnDef& column : tableSnapshot.columns)
        {
            names.push_back(column.name);
        }
        return names;
    }

    void SCTableDesignPane::SetStatus(const QString& text)
    {
        statusLabel_->setText(text);
        emit StatusMessage(text);
    }

}  // namespace StableCore::Storage::Editor
