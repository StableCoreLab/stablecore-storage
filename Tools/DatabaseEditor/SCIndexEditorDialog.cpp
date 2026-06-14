#include "SCIndexEditorDialog.h"

#include <QFormLayout>
#include <QHBoxLayout>
#include <QInputDialog>
#include <QLineEdit>
#include <QPushButton>
#include <QTableWidget>
#include <QTableWidgetItem>
#include <QVBoxLayout>

namespace sc = StableCore::Storage;

namespace StableCore::Storage::Editor
{
    SCIndexEditorDialog::SCIndexEditorDialog(
        StableCore::Storage::SCIndexDef index,
        const std::vector<std::wstring>& availableColumns,
        QWidget* parent)
        : QDialog(parent), index_(index), availableColumns_(availableColumns)
    {
        setWindowTitle(QStringLiteral("索引编辑器"));
        resize(600, 400);

        auto* layout = new QVBoxLayout(this);

        auto* form = new QFormLayout();
        indexNameEdit_ = new QLineEdit(this);
        indexNameEdit_->setText(QString::fromStdWString(index.name));
        form->addRow(QStringLiteral("索引名称"), indexNameEdit_);
        layout->addLayout(form);

        columnsTable_ = new QTableWidget(this);
        columnsTable_->setColumnCount(2);
        columnsTable_->setHorizontalHeaderLabels(
            {QStringLiteral("列名"), QStringLiteral("排序")});
        columnsTable_->setSelectionMode(QAbstractItemView::SingleSelection);
        columnsTable_->setSelectionBehavior(QAbstractItemView::SelectRows);
        layout->addWidget(columnsTable_, 1);

        auto* buttonLayout = new QHBoxLayout();
        addButton_ = new QPushButton(QStringLiteral("+ 添加列"), this);
        removeButton_ = new QPushButton(QStringLiteral("- 删除列"), this);
        upButton_ = new QPushButton(QStringLiteral("↑"), this);
        downButton_ = new QPushButton(QStringLiteral("↓"), this);
        buttonLayout->addWidget(addButton_);
        buttonLayout->addWidget(removeButton_);
        buttonLayout->addStretch(1);
        buttonLayout->addWidget(upButton_);
        buttonLayout->addWidget(downButton_);
        layout->addLayout(buttonLayout);

        auto* okCancelLayout = new QHBoxLayout();
        okButton_ = new QPushButton(QStringLiteral("确定"), this);
        auto* cancelButton = new QPushButton(QStringLiteral("取消"), this);
        okCancelLayout->addStretch(1);
        okCancelLayout->addWidget(okButton_);
        okCancelLayout->addWidget(cancelButton);
        layout->addLayout(okCancelLayout);

        connect(addButton_, &QPushButton::clicked, this,
                &SCIndexEditorDialog::AddColumn);
        connect(removeButton_, &QPushButton::clicked, this,
                &SCIndexEditorDialog::RemoveColumn);
        connect(upButton_, &QPushButton::clicked, this,
                &SCIndexEditorDialog::MoveColumnUp);
        connect(downButton_, &QPushButton::clicked, this,
                &SCIndexEditorDialog::MoveColumnDown);
        connect(columnsTable_, &QTableWidget::cellChanged, this,
                &SCIndexEditorDialog::UpdateColumnOrder);
        connect(indexNameEdit_, &QLineEdit::textChanged, this,
                &SCIndexEditorDialog::ValidateInput);
        connect(okButton_, &QPushButton::clicked, this, &QDialog::accept);
        connect(cancelButton, &QPushButton::clicked, this, &QDialog::reject);

        PopulateColumnList();
        ValidateInput();
    }

    void SCIndexEditorDialog::PopulateColumnList()
    {
        columnsTable_->setRowCount(static_cast<int>(index_.columns.size()));
        for (size_t i = 0; i < index_.columns.size(); ++i)
        {
            auto* columnItem = new QTableWidgetItem(
                QString::fromStdWString(index_.columns[i].columnName));
            columnItem->setFlags(columnItem->flags() & ~Qt::ItemIsEditable);
            columnsTable_->setItem(static_cast<int>(i), 0, columnItem);

            auto* orderItem = new QTableWidgetItem(
                index_.columns[i].descending ? QStringLiteral("DESC")
                                             : QStringLiteral("ASC"));
            orderItem->setFlags(columnItem->flags() | Qt::ItemIsEditable);
            columnsTable_->setItem(static_cast<int>(i), 1, orderItem);
        }
        columnsTable_->resizeColumnsToContents();
    }

    void SCIndexEditorDialog::AddColumn()
    {
        QStringList available;
        QStringList selected = GetSelectedColumnNames();
        for (const auto& col : availableColumns_)
        {
            QString qcol = QString::fromStdWString(col);
            if (!selected.contains(qcol))
            {
                available.append(qcol);
            }
        }

        if (available.isEmpty())
        {
            return;
        }

        QString selectedColumn = QInputDialog::getItem(
            this, QStringLiteral("添加列"), QStringLiteral("选择列:"),
            available);

        if (!selectedColumn.isEmpty())
        {
            sc::SCIndexColumnDef colDef;
            colDef.columnName = selectedColumn.toStdWString();
            colDef.descending = false;
            index_.columns.push_back(colDef);
            PopulateColumnList();
            ValidateInput();
        }
    }

    void SCIndexEditorDialog::RemoveColumn()
    {
        int currentRow = columnsTable_->currentRow();
        if (currentRow >= 0 &&
            currentRow < static_cast<int>(index_.columns.size()))
        {
            index_.columns.erase(index_.columns.begin() + currentRow);
            PopulateColumnList();
            ValidateInput();
        }
    }

    void SCIndexEditorDialog::MoveColumnUp()
    {
        int currentRow = columnsTable_->currentRow();
        if (currentRow > 0 &&
            currentRow < static_cast<int>(index_.columns.size()))
        {
            std::swap(index_.columns[currentRow],
                      index_.columns[currentRow - 1]);
            PopulateColumnList();
            columnsTable_->selectRow(currentRow - 1);
        }
    }

    void SCIndexEditorDialog::MoveColumnDown()
    {
        int currentRow = columnsTable_->currentRow();
        if (currentRow >= 0 &&
            currentRow < static_cast<int>(index_.columns.size()) - 1)
        {
            std::swap(index_.columns[currentRow],
                      index_.columns[currentRow + 1]);
            PopulateColumnList();
            columnsTable_->selectRow(currentRow + 1);
        }
    }

    void SCIndexEditorDialog::UpdateColumnOrder(int row, int /*column*/)
    {
        if (row >= 0 && row < static_cast<int>(index_.columns.size()))
        {
            QTableWidgetItem* item = columnsTable_->item(row, 1);
            if (item)
            {
                index_.columns[row].descending =
                    (item->text().compare(QStringLiteral("DESC"),
                                          Qt::CaseInsensitive) == 0);
            }
        }
    }

    void SCIndexEditorDialog::ValidateInput()
    {
        bool valid = !indexNameEdit_->text().trimmed().isEmpty() &&
                     !index_.columns.empty();
        okButton_->setEnabled(valid);
    }

    QStringList SCIndexEditorDialog::GetSelectedColumnNames() const
    {
        QStringList names;
        for (const auto& col : index_.columns)
        {
            names.append(QString::fromStdWString(col.columnName));
        }
        return names;
    }

}  // namespace StableCore::Storage::Editor