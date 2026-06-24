#include "SCConstraintEditorDialog.h"

#include <algorithm>
#include <utility>

#include <QCompleter>
#include <QAbstractItemView>
#include <QDialogButtonBox>
#include <QFormLayout>
#include <QHBoxLayout>
#include <QInputDialog>
#include <QLabel>
#include <QPlainTextEdit>
#include <QPushButton>
#include <QTableWidget>
#include <QTableWidgetItem>
#include <QStringList>
#include <QStringListModel>
#include <QVBoxLayout>

#include "SCDatabaseSession.h"

namespace sc = StableCore::Storage;

namespace StableCore::Storage::Editor
{
    namespace
    {
        sc::SCConstraintKind ConstraintKindFromIndex(int index)
        {
            switch (index)
            {
                case 0:
                    return sc::SCConstraintKind::Unique;
                case 1:
                    return sc::SCConstraintKind::ForeignKey;
                default:
                    return sc::SCConstraintKind::Check;
            }
        }

        sc::SCForeignKeyAction ForeignKeyActionFromIndex(int index)
        {
            switch (index)
            {
                case 1:
                    return sc::SCForeignKeyAction::NoAction;
                case 2:
                    return sc::SCForeignKeyAction::Cascade;
                case 3:
                    return sc::SCForeignKeyAction::SetNull;
                case 4:
                    return sc::SCForeignKeyAction::SetDefault;
                case 0:
                default:
                    return sc::SCForeignKeyAction::Restrict;
            }
        }

        int ForeignKeyActionToIndex(sc::SCForeignKeyAction action)
        {
            switch (action)
            {
                case sc::SCForeignKeyAction::Restrict:
                    return 0;
                case sc::SCForeignKeyAction::NoAction:
                    return 1;
                case sc::SCForeignKeyAction::Cascade:
                    return 2;
                case sc::SCForeignKeyAction::SetNull:
                    return 3;
                case sc::SCForeignKeyAction::SetDefault:
                default:
                    return 4;
            }
        }

        QString ConstraintKindHelp(sc::SCConstraintKind kind)
        {
            switch (kind)
            {
                case sc::SCConstraintKind::Unique:
                    return QStringLiteral("Unique constraint over one or more columns.");
                case sc::SCConstraintKind::ForeignKey:
                    return QStringLiteral(
                        "Foreign key references another table. Referenced columns are optional.");
                case sc::SCConstraintKind::Check:
                    return QStringLiteral("Check constraint requires an expression.");
                case sc::SCConstraintKind::PrimaryKey:
                default:
                    return QStringLiteral("Primary keys are edited in the field above.");
            }
        }
    }  // namespace

    SCConstraintEditorDialog::SCConstraintEditorDialog(
        SCDatabaseSession* session, sc::SCConstraintDef constraint,
        const std::vector<std::wstring>& availableColumns, QWidget* parent)
        : QDialog(parent),
          session_(session),
          constraint_(std::move(constraint)),
          availableColumns_(availableColumns)
    {
        setWindowTitle(QStringLiteral("Constraint Editor"));
        resize(880, 720);

        auto* layout = new QVBoxLayout(this);
        auto* form = new QFormLayout();

        nameEdit_ = new QLineEdit(this);
        nameEdit_->setText(QString::fromStdWString(constraint_.name));
        form->addRow(QStringLiteral("Name"), nameEdit_);

        kindCombo_ = new QComboBox(this);
        kindCombo_->addItem(QStringLiteral("Unique"),
                            static_cast<int>(sc::SCConstraintKind::Unique));
        kindCombo_->addItem(QStringLiteral("ForeignKey"),
                            static_cast<int>(sc::SCConstraintKind::ForeignKey));
        kindCombo_->addItem(QStringLiteral("Check"),
                            static_cast<int>(sc::SCConstraintKind::Check));
        const int kindIndex = kindCombo_->findData(
            static_cast<int>(constraint_.kind));
        kindCombo_->setCurrentIndex(kindIndex >= 0 ? kindIndex : 0);
        form->addRow(QStringLiteral("Kind"), kindCombo_);

        columnsTable_ = new QTableWidget(this);
        columnsTable_->setColumnCount(1);
        columnsTable_->setHorizontalHeaderLabels({QStringLiteral("Column")});
        columnsTable_->setSelectionMode(QAbstractItemView::SingleSelection);
        columnsTable_->setSelectionBehavior(QAbstractItemView::SelectRows);
        columnsTable_->setEditTriggers(QAbstractItemView::NoEditTriggers);
        form->addRow(QStringLiteral("Columns"), columnsTable_);

        auto* columnsButtons = new QHBoxLayout();
        addColumnButton_ = new QPushButton(QStringLiteral("+ Add"), this);
        removeColumnButton_ = new QPushButton(QStringLiteral("- Remove"), this);
        upColumnButton_ = new QPushButton(QStringLiteral("Up"), this);
        downColumnButton_ = new QPushButton(QStringLiteral("Down"), this);
        columnsButtons->addWidget(addColumnButton_);
        columnsButtons->addWidget(removeColumnButton_);
        columnsButtons->addStretch(1);
        columnsButtons->addWidget(upColumnButton_);
        columnsButtons->addWidget(downColumnButton_);
        form->addRow(QStringLiteral(""), columnsButtons);

        referencedTableEdit_ = new QLineEdit(this);
        referencedTableEdit_->setText(
            QString::fromStdWString(constraint_.referencedTable));
        referencedTableModel_ = new QStringListModel(this);
        referencedTableCompleter_ =
            new QCompleter(referencedTableModel_, this);
        referencedTableCompleter_->setCaseSensitivity(Qt::CaseInsensitive);
        referencedTableEdit_->setCompleter(referencedTableCompleter_);
        form->addRow(QStringLiteral("Referenced table"),
                     referencedTableEdit_);

        referencedColumnsTable_ = new QTableWidget(this);
        referencedColumnsTable_->setColumnCount(1);
        referencedColumnsTable_->setHorizontalHeaderLabels(
            {QStringLiteral("Referenced column")});
        referencedColumnsTable_->setSelectionMode(
            QAbstractItemView::SingleSelection);
        referencedColumnsTable_->setSelectionBehavior(
            QAbstractItemView::SelectRows);
        referencedColumnsTable_->setEditTriggers(QAbstractItemView::NoEditTriggers);
        form->addRow(QStringLiteral("Referenced columns"),
                     referencedColumnsTable_);

        auto* referencedButtons = new QHBoxLayout();
        addReferencedColumnButton_ =
            new QPushButton(QStringLiteral("+ Add"), this);
        removeReferencedColumnButton_ =
            new QPushButton(QStringLiteral("- Remove"), this);
        upReferencedColumnButton_ = new QPushButton(QStringLiteral("Up"), this);
        downReferencedColumnButton_ =
            new QPushButton(QStringLiteral("Down"), this);
        referencedButtons->addWidget(addReferencedColumnButton_);
        referencedButtons->addWidget(removeReferencedColumnButton_);
        referencedButtons->addStretch(1);
        referencedButtons->addWidget(upReferencedColumnButton_);
        referencedButtons->addWidget(downReferencedColumnButton_);
        form->addRow(QStringLiteral(""), referencedButtons);

        onDeleteCombo_ = new QComboBox(this);
        onDeleteCombo_->addItems({QStringLiteral("Restrict"),
                                  QStringLiteral("NoAction"),
                                  QStringLiteral("Cascade"),
                                  QStringLiteral("SetNull"),
                                  QStringLiteral("SetDefault")});
        onDeleteCombo_->setCurrentIndex(
            ForeignKeyActionToIndex(constraint_.onDelete));
        form->addRow(QStringLiteral("On delete"), onDeleteCombo_);

        onUpdateCombo_ = new QComboBox(this);
        onUpdateCombo_->addItems({QStringLiteral("Restrict"),
                                  QStringLiteral("NoAction"),
                                  QStringLiteral("Cascade"),
                                  QStringLiteral("SetNull"),
                                  QStringLiteral("SetDefault")});
        onUpdateCombo_->setCurrentIndex(
            ForeignKeyActionToIndex(constraint_.onUpdate));
        form->addRow(QStringLiteral("On update"), onUpdateCombo_);

        checkExpressionEdit_ = new QPlainTextEdit(this);
        checkExpressionEdit_->setPlaceholderText(
            QStringLiteral("Enter the check expression."));
        checkExpressionEdit_->setPlainText(
            QString::fromStdWString(constraint_.checkExpression));
        checkExpressionEdit_->setMinimumHeight(120);
        form->addRow(QStringLiteral("Check expression"),
                     checkExpressionEdit_);

        layout->addLayout(form);

        auto* hintLabel = new QLabel(
            QStringLiteral(
                "Fields that do not apply to the current kind are disabled."),
            this);
        hintLabel->setWordWrap(true);
        hintLabel->setStyleSheet(QStringLiteral("color: #6a6a6a;"));
        layout->addWidget(hintLabel);

        validationLabel_ = new QLabel(this);
        validationLabel_->setWordWrap(true);
        layout->addWidget(validationLabel_);

        auto* buttonRow = new QHBoxLayout();
        auto* okCancel = new QDialogButtonBox(
            QDialogButtonBox::Ok | QDialogButtonBox::Cancel, this);
        okButton_ = okCancel->button(QDialogButtonBox::Ok);
        buttonRow->addStretch(1);
        buttonRow->addWidget(okCancel);
        layout->addLayout(buttonRow);

        connect(addColumnButton_, &QPushButton::clicked, this,
                &SCConstraintEditorDialog::AddColumn);
        connect(removeColumnButton_, &QPushButton::clicked, this,
                &SCConstraintEditorDialog::RemoveColumn);
        connect(upColumnButton_, &QPushButton::clicked, this,
                &SCConstraintEditorDialog::MoveColumnUp);
        connect(downColumnButton_, &QPushButton::clicked, this,
                &SCConstraintEditorDialog::MoveColumnDown);
        connect(addReferencedColumnButton_, &QPushButton::clicked, this,
                &SCConstraintEditorDialog::AddReferencedColumn);
        connect(removeReferencedColumnButton_, &QPushButton::clicked, this,
                &SCConstraintEditorDialog::RemoveReferencedColumn);
        connect(upReferencedColumnButton_, &QPushButton::clicked, this,
                &SCConstraintEditorDialog::MoveReferencedColumnUp);
        connect(downReferencedColumnButton_, &QPushButton::clicked, this,
                &SCConstraintEditorDialog::MoveReferencedColumnDown);
        connect(kindCombo_, qOverload<int>(&QComboBox::currentIndexChanged),
                this, &SCConstraintEditorDialog::ValidateInput);
        connect(nameEdit_, &QLineEdit::textChanged, this,
                &SCConstraintEditorDialog::ValidateInput);
        connect(referencedTableEdit_, &QLineEdit::textChanged, this,
                &SCConstraintEditorDialog::ValidateInput);
        connect(checkExpressionEdit_, &QPlainTextEdit::textChanged, this,
                &SCConstraintEditorDialog::ValidateInput);
        connect(onDeleteCombo_, qOverload<int>(&QComboBox::currentIndexChanged),
                this, &SCConstraintEditorDialog::ValidateInput);
        connect(onUpdateCombo_, qOverload<int>(&QComboBox::currentIndexChanged),
                this, &SCConstraintEditorDialog::ValidateInput);
        connect(okCancel, &QDialogButtonBox::accepted, this, &QDialog::accept);
        connect(okCancel, &QDialogButtonBox::rejected, this, &QDialog::reject);

        PopulateColumnList();
        PopulateReferencedColumnList();
        UpdateKindUi();
        ValidateInput();
    }

    void SCConstraintEditorDialog::PopulateColumnList()
    {
        columnsTable_->setRowCount(static_cast<int>(constraint_.columns.size()));
        for (std::size_t index = 0; index < constraint_.columns.size(); ++index)
        {
            auto* item = new QTableWidgetItem(
                QString::fromStdWString(constraint_.columns[index]));
            item->setFlags(item->flags() & ~Qt::ItemIsEditable);
            columnsTable_->setItem(static_cast<int>(index), 0, item);
        }
        columnsTable_->resizeColumnsToContents();
    }

    void SCConstraintEditorDialog::PopulateReferencedColumnList()
    {
        referencedColumnsTable_->setRowCount(
            static_cast<int>(constraint_.referencedColumns.size()));
        for (std::size_t index = 0;
             index < constraint_.referencedColumns.size(); ++index)
        {
            auto* item = new QTableWidgetItem(
                QString::fromStdWString(constraint_.referencedColumns[index]));
            item->setFlags(item->flags() & ~Qt::ItemIsEditable);
            referencedColumnsTable_->setItem(static_cast<int>(index), 0, item);
        }
        referencedColumnsTable_->resizeColumnsToContents();
    }

    void SCConstraintEditorDialog::UpdateKindUi()
    {
        const sc::SCConstraintKind kind =
            ConstraintKindFromIndex(kindCombo_->currentIndex());
        const bool isForeignKey = kind == sc::SCConstraintKind::ForeignKey;
        const bool isCheck = kind == sc::SCConstraintKind::Check;

        referencedTableEdit_->setEnabled(isForeignKey);
        referencedColumnsTable_->setEnabled(isForeignKey);
        addReferencedColumnButton_->setEnabled(isForeignKey);
        removeReferencedColumnButton_->setEnabled(isForeignKey);
        upReferencedColumnButton_->setEnabled(isForeignKey);
        downReferencedColumnButton_->setEnabled(isForeignKey);
        onDeleteCombo_->setEnabled(isForeignKey);
        onUpdateCombo_->setEnabled(isForeignKey);
        checkExpressionEdit_->setEnabled(isCheck);
    }

    void SCConstraintEditorDialog::RefreshReferencedTableHints()
    {
        if (session_ == nullptr || referencedTableModel_ == nullptr)
        {
            return;
        }
        referencedTableModel_->setStringList(session_->TableNames());
    }

    bool SCConstraintEditorDialog::LoadReferencedTableSnapshot(
        sc::SCTableSchemaSnapshot* outSnapshot, QString* outError) const
    {
        if (outSnapshot == nullptr)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Output snapshot is null.");
            }
            return false;
        }

        outSnapshot->columns.clear();
        outSnapshot->constraints.clear();
        outSnapshot->indexes.clear();
        outSnapshot->table = sc::SCTableDef{};

        if (session_ == nullptr)
        {
            return true;
        }

        const QString tableName = referencedTableEdit_->text().trimmed();
        if (tableName.isEmpty())
        {
            return true;
        }

        return session_->GetTableSchemaSnapshot(tableName, outSnapshot,
                                                outError);
    }

    QStringList SCConstraintEditorDialog::GetSelectedColumnNames(
        const QTableWidget* table) const
    {
        QStringList names;
        if (table == nullptr)
        {
            return names;
        }
        for (int row = 0; row < table->rowCount(); ++row)
        {
            if (auto* item = table->item(row, 0); item != nullptr)
            {
                names.push_back(item->text());
            }
        }
        return names;
    }

    QStringList SCConstraintEditorDialog::GetAvailableColumns(
        const std::vector<std::wstring>& sourceColumns,
        const QStringList& selectedColumns) const
    {
        QStringList available;
        for (const std::wstring& column : sourceColumns)
        {
            const QString columnName = QString::fromStdWString(column);
            if (!selectedColumns.contains(columnName, Qt::CaseInsensitive))
            {
                available.push_back(columnName);
            }
        }
        return available;
    }

    void SCConstraintEditorDialog::AddColumn()
    {
        QStringList selected = GetSelectedColumnNames(columnsTable_);
        QStringList available = GetAvailableColumns(availableColumns_, selected);
        if (available.isEmpty())
        {
            return;
        }

        QString choice = QInputDialog::getItem(
            this, QStringLiteral("Add column"), QStringLiteral("Select a column"),
            available);
        if (choice.isEmpty())
        {
            return;
        }

        constraint_.columns.push_back(choice.toStdWString());
        PopulateColumnList();
        ValidateInput();
    }

    void SCConstraintEditorDialog::RemoveColumn()
    {
        const int row = columnsTable_->currentRow();
        if (row < 0 || row >= static_cast<int>(constraint_.columns.size()))
        {
            return;
        }

        constraint_.columns.erase(constraint_.columns.begin() + row);
        PopulateColumnList();
        ValidateInput();
    }

    void SCConstraintEditorDialog::MoveColumnUp()
    {
        const int row = columnsTable_->currentRow();
        if (row <= 0 || row >= static_cast<int>(constraint_.columns.size()))
        {
            return;
        }

        std::swap(constraint_.columns[static_cast<std::size_t>(row)],
                  constraint_.columns[static_cast<std::size_t>(row - 1)]);
        PopulateColumnList();
        columnsTable_->selectRow(row - 1);
    }

    void SCConstraintEditorDialog::MoveColumnDown()
    {
        const int row = columnsTable_->currentRow();
        if (row < 0 ||
            row >= static_cast<int>(constraint_.columns.size()) - 1)
        {
            return;
        }

        std::swap(constraint_.columns[static_cast<std::size_t>(row)],
                  constraint_.columns[static_cast<std::size_t>(row + 1)]);
        PopulateColumnList();
        columnsTable_->selectRow(row + 1);
    }

    void SCConstraintEditorDialog::AddReferencedColumn()
    {
        sc::SCTableSchemaSnapshot snapshot;
        QString error;
        if (!LoadReferencedTableSnapshot(&snapshot, &error))
        {
            validationLabel_->setStyleSheet(QStringLiteral("color: #b00020;"));
            validationLabel_->setText(error);
            ValidateInput();
            return;
        }

        QStringList selected = GetSelectedColumnNames(referencedColumnsTable_);
        QStringList available;
        for (const sc::SCColumnDef& column : snapshot.columns)
        {
            const QString name = QString::fromStdWString(column.name);
            if (!selected.contains(name, Qt::CaseInsensitive))
            {
                available.push_back(name);
            }
        }

        if (available.isEmpty())
        {
            return;
        }

        QString choice = QInputDialog::getItem(
            this, QStringLiteral("Add referenced column"),
            QStringLiteral("Select a referenced column"), available);
        if (choice.isEmpty())
        {
            return;
        }

        constraint_.referencedColumns.push_back(choice.toStdWString());
        PopulateReferencedColumnList();
        ValidateInput();
    }

    void SCConstraintEditorDialog::RemoveReferencedColumn()
    {
        const int row = referencedColumnsTable_->currentRow();
        if (row < 0 ||
            row >= static_cast<int>(constraint_.referencedColumns.size()))
        {
            return;
        }

        constraint_.referencedColumns.erase(
            constraint_.referencedColumns.begin() + row);
        PopulateReferencedColumnList();
        ValidateInput();
    }

    void SCConstraintEditorDialog::MoveReferencedColumnUp()
    {
        const int row = referencedColumnsTable_->currentRow();
        if (row <= 0 ||
            row >= static_cast<int>(constraint_.referencedColumns.size()))
        {
            return;
        }

        std::swap(constraint_.referencedColumns[static_cast<std::size_t>(row)],
                  constraint_.referencedColumns[static_cast<std::size_t>(row - 1)]);
        PopulateReferencedColumnList();
        referencedColumnsTable_->selectRow(row - 1);
    }

    void SCConstraintEditorDialog::MoveReferencedColumnDown()
    {
        const int row = referencedColumnsTable_->currentRow();
        if (row < 0 ||
            row >= static_cast<int>(constraint_.referencedColumns.size()) - 1)
        {
            return;
        }

        std::swap(constraint_.referencedColumns[static_cast<std::size_t>(row)],
                  constraint_.referencedColumns[static_cast<std::size_t>(row + 1)]);
        PopulateReferencedColumnList();
        referencedColumnsTable_->selectRow(row + 1);
    }

    void SCConstraintEditorDialog::ValidateInput()
    {
        const sc::SCConstraintKind kind =
            ConstraintKindFromIndex(kindCombo_->currentIndex());

        constraint_.name = nameEdit_->text().trimmed().toStdWString();
        constraint_.kind = kind;
        constraint_.sourceKind = sc::SCSchemaSourceKind::Explicit;
        constraint_.onDelete =
            ForeignKeyActionFromIndex(onDeleteCombo_->currentIndex());
        constraint_.onUpdate =
            ForeignKeyActionFromIndex(onUpdateCombo_->currentIndex());
        constraint_.checkExpression =
            checkExpressionEdit_->toPlainText().trimmed().toStdWString();
        constraint_.referencedTable =
            referencedTableEdit_->text().trimmed().toStdWString();

        const QStringList sourceColumns = GetSelectedColumnNames(columnsTable_);
        const QStringList referencedColumns =
            GetSelectedColumnNames(referencedColumnsTable_);

        bool valid = true;
        QString message;

        if (constraint_.name.empty())
        {
            valid = false;
            message = QStringLiteral("Constraint name is required.");
        } else if (sourceColumns.isEmpty())
        {
            valid = false;
            message = QStringLiteral("At least one source column is required.");
        }

        if (kind == sc::SCConstraintKind::ForeignKey)
        {
            if (valid && constraint_.referencedTable.empty())
            {
                valid = false;
                message = QStringLiteral("Referenced table is required.");
            } else if (valid && !referencedColumns.isEmpty() &&
                       referencedColumns.size() != sourceColumns.size())
            {
                valid = false;
                message = QStringLiteral(
                    "Referenced columns must be empty or match source column count.");
            }
            constraint_.referencedColumns.clear();
            for (const QString& column : referencedColumns)
            {
                constraint_.referencedColumns.push_back(column.toStdWString());
            }
            constraint_.checkExpression.clear();
        } else if (kind == sc::SCConstraintKind::Check)
        {
            if (valid && constraint_.checkExpression.empty())
            {
                valid = false;
                message = QStringLiteral("Check expression is required.");
            }
            constraint_.referencedTable.clear();
            constraint_.referencedColumns.clear();
            constraint_.onDelete = sc::SCForeignKeyAction::Restrict;
            constraint_.onUpdate = sc::SCForeignKeyAction::Restrict;
        } else
        {
            constraint_.referencedTable.clear();
            constraint_.referencedColumns.clear();
            constraint_.checkExpression.clear();
            constraint_.onDelete = sc::SCForeignKeyAction::Restrict;
            constraint_.onUpdate = sc::SCForeignKeyAction::Restrict;
        }

        constraint_.columns.clear();
        for (const QString& column : sourceColumns)
        {
            constraint_.columns.push_back(column.toStdWString());
        }

        if (valid && kind == sc::SCConstraintKind::ForeignKey)
        {
            sc::SCTableSchemaSnapshot snapshot;
            QString error;
            if (LoadReferencedTableSnapshot(&snapshot, &error))
            {
                for (const QString& column : referencedColumns)
                {
                    const bool exists =
                        std::any_of(snapshot.columns.begin(),
                                    snapshot.columns.end(),
                                    [&column](const sc::SCColumnDef& def) {
                                        return QString::fromStdWString(def.name)
                                                   .compare(column,
                                                            Qt::CaseInsensitive) == 0;
                                    });
                    if (!exists)
                    {
                        valid = false;
                        message = QStringLiteral("Referenced column does not exist.");
                        break;
                    }
                }
            } else
            {
                valid = false;
                message = error;
            }
        }

        if (validationLabel_ != nullptr)
        {
            if (valid)
            {
                validationLabel_->setStyleSheet(QStringLiteral("color: #0b5fff;"));
                validationLabel_->setText(ConstraintKindHelp(kind));
            } else
            {
                validationLabel_->setStyleSheet(QStringLiteral("color: #b00020;"));
                validationLabel_->setText(message);
            }
        }

        const bool kindOk = valid && !constraint_.name.empty() &&
                            !constraint_.columns.empty();
        if (okButton_ != nullptr)
        {
            okButton_->setEnabled(kindOk);
        }

        UpdateKindUi();
        RefreshReferencedTableHints();
    }

}  // namespace StableCore::Storage::Editor
