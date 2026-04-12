#pragma once

#include <QSortFilterProxyModel>

namespace stablecore::storage::editor
{

class SCRecordFilterProxyModel final : public QSortFilterProxyModel
{
    Q_OBJECT

public:
    explicit SCRecordFilterProxyModel(QObject* parent = nullptr);

protected:
    bool filterAcceptsRow(int sourceRow, const QModelIndex& sourceParent) const override;
    bool lessThan(const QModelIndex& left, const QModelIndex& right) const override;
};

}  // namespace stablecore::storage::editor
