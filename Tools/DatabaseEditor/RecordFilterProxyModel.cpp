#include "RecordFilterProxyModel.h"

#include <QAbstractItemModel>

namespace stablecore::storage::editor
{

RecordFilterProxyModel::RecordFilterProxyModel(QObject* parent)
    : QSortFilterProxyModel(parent)
{
    setFilterCaseSensitivity(Qt::CaseInsensitive);
    setSortCaseSensitivity(Qt::CaseInsensitive);
}

bool RecordFilterProxyModel::filterAcceptsRow(int sourceRow, const QModelIndex& sourceParent) const
{
    if (filterRegularExpression().pattern().trimmed().isEmpty())
    {
        return true;
    }

    const QAbstractItemModel* model = sourceModel();
    if (model == nullptr)
    {
        return true;
    }

    for (int column = 0; column < model->columnCount(sourceParent); ++column)
    {
        const QModelIndex index = model->index(sourceRow, column, sourceParent);
        const QString text = model->data(index, Qt::DisplayRole).toString();
        if (text.contains(filterRegularExpression()))
        {
            return true;
        }
    }

    return false;
}

bool RecordFilterProxyModel::lessThan(const QModelIndex& left, const QModelIndex& right) const
{
    const QVariant leftValue = sourceModel()->data(left, Qt::EditRole);
    const QVariant rightValue = sourceModel()->data(right, Qt::EditRole);

    const bool leftIsNumber = leftValue.canConvert<double>();
    const bool rightIsNumber = rightValue.canConvert<double>();
    if (leftIsNumber && rightIsNumber)
    {
        return leftValue.toDouble() < rightValue.toDouble();
    }

    return QString::localeAwareCompare(leftValue.toString(), rightValue.toString()) < 0;
}

}  // namespace stablecore::storage::editor
