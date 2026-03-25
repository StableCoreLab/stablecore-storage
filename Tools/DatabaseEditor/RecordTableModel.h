#pragma once

#include <QAbstractTableModel>
#include <QVector>

#include "DatabaseSession.h"

namespace stablecore::storage::editor
{

class RecordTableModel final : public QAbstractTableModel
{
    Q_OBJECT

public:
    explicit RecordTableModel(DatabaseSession* session, QObject* parent = nullptr);

    int rowCount(const QModelIndex& parent = QModelIndex()) const override;
    int columnCount(const QModelIndex& parent = QModelIndex()) const override;
    QVariant data(const QModelIndex& index, int role = Qt::DisplayRole) const override;
    QVariant headerData(int section, Qt::Orientation orientation, int role = Qt::DisplayRole) const override;
    Qt::ItemFlags flags(const QModelIndex& index) const override;
    bool setData(const QModelIndex& index, const QVariant& value, int role = Qt::EditRole) override;

    stablecore::storage::RecordId RecordIdAt(int row) const;
    void Refresh();

private:
    struct RowData
    {
        stablecore::storage::RecordId recordId{0};
    };

    DatabaseSession* session_{nullptr};
    QVector<stablecore::storage::TableViewColumnDef> columns_;
    QVector<RowData> rows_;
};

}  // namespace stablecore::storage::editor
