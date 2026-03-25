#include "RelationPickerDialog.h"

#include <QDialogButtonBox>
#include <QHeaderView>
#include <QLabel>
#include <QVBoxLayout>

namespace sc = stablecore::storage;

namespace stablecore::storage::editor
{

RelationPickerDialog::RelationPickerDialog(
    const QString& targetTableName,
    const QVector<DatabaseSession::RelationCandidate>& candidates,
    QWidget* parent)
    : QDialog(parent)
    , targetTableName_(targetTableName)
    , candidates_(candidates)
{
    setWindowTitle(QStringLiteral("Select Relation Record"));
    resize(780, 520);

    auto* layout = new QVBoxLayout(this);
    layout->addWidget(new QLabel(QStringLiteral("Target Table: %1").arg(targetTableName_), this));

    filterEdit_ = new QLineEdit(this);
    filterEdit_->setPlaceholderText(QStringLiteral("Filter by label, id, or preview field"));
    layout->addWidget(filterEdit_);

    tableWidget_ = new QTableWidget(this);
    tableWidget_->setColumnCount(3);
    tableWidget_->setHorizontalHeaderLabels({QStringLiteral("RecordId"), QStringLiteral("Label"), QStringLiteral("Preview")});
    tableWidget_->horizontalHeader()->setStretchLastSection(true);
    tableWidget_->horizontalHeader()->setSectionResizeMode(QHeaderView::ResizeToContents);
    tableWidget_->setSelectionBehavior(QAbstractItemView::SelectRows);
    tableWidget_->setSelectionMode(QAbstractItemView::SingleSelection);
    tableWidget_->setEditTriggers(QAbstractItemView::NoEditTriggers);
    layout->addWidget(tableWidget_, 1);

    auto* buttonBox = new QDialogButtonBox(QDialogButtonBox::Ok | QDialogButtonBox::Cancel, this);
    layout->addWidget(buttonBox);

    connect(filterEdit_, &QLineEdit::textChanged, this, &RelationPickerDialog::ApplyFilter);
    connect(tableWidget_, &QTableWidget::cellDoubleClicked, this, [this](int, int) { AcceptCurrentSelection(); });
    connect(buttonBox, &QDialogButtonBox::accepted, this, &RelationPickerDialog::AcceptCurrentSelection);
    connect(buttonBox, &QDialogButtonBox::rejected, this, &RelationPickerDialog::reject);

    PopulateRows();
}

sc::RecordId RelationPickerDialog::SelectedRecordId() const noexcept
{
    const int row = tableWidget_->currentRow();
    if (row < 0)
    {
        return 0;
    }
    return tableWidget_->item(row, 0)->data(Qt::UserRole).toLongLong();
}

void RelationPickerDialog::ApplyFilter(const QString& text)
{
    for (int row = 0; row < tableWidget_->rowCount(); ++row)
    {
        bool visible = text.trimmed().isEmpty();
        if (!visible)
        {
            for (int column = 0; column < tableWidget_->columnCount(); ++column)
            {
                const QTableWidgetItem* item = tableWidget_->item(row, column);
                if (item != nullptr && item->text().contains(text, Qt::CaseInsensitive))
                {
                    visible = true;
                    break;
                }
            }
        }
        tableWidget_->setRowHidden(row, !visible);
    }
}

void RelationPickerDialog::AcceptCurrentSelection()
{
    if (SelectedRecordId() == 0)
    {
        return;
    }
    accept();
}

void RelationPickerDialog::PopulateRows()
{
    tableWidget_->setRowCount(candidates_.size());
    for (int row = 0; row < candidates_.size(); ++row)
    {
        const DatabaseSession::RelationCandidate& candidate = candidates_[row];
        QStringList preview;
        for (const auto& pair : candidate.previewFields)
        {
            if (pair.first == QStringLiteral("RecordId"))
            {
                continue;
            }
            preview.push_back(pair.first + QStringLiteral("=") + pair.second);
        }

        auto* idItem = new QTableWidgetItem(QString::number(candidate.recordId));
        idItem->setData(Qt::UserRole, QVariant::fromValue<qlonglong>(candidate.recordId));
        tableWidget_->setItem(row, 0, idItem);
        tableWidget_->setItem(row, 1, new QTableWidgetItem(candidate.label));
        tableWidget_->setItem(row, 2, new QTableWidgetItem(preview.join(QStringLiteral(" | "))));
    }

    if (tableWidget_->rowCount() > 0)
    {
        tableWidget_->selectRow(0);
    }
}

}  // namespace stablecore::storage::editor
