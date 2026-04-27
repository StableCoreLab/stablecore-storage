#pragma once

#include <QAbstractTableModel>
#include <QVector>

#include "SCDatabaseSession.h"

namespace StableCore::Storage::Editor
{

    class SCRecordTableModel final : public QAbstractTableModel
    {
        Q_OBJECT

    public:
        explicit SCRecordTableModel(SCDatabaseSession* session,
                                    QObject* parent = nullptr);

        int rowCount(const QModelIndex& parent = QModelIndex()) const override;
        int columnCount(
            const QModelIndex& parent = QModelIndex()) const override;
        QVariant data(const QModelIndex& index,
                      int role = Qt::DisplayRole) const override;
        QVariant headerData(int section, Qt::Orientation orientation,
                            int role = Qt::DisplayRole) const override;
        Qt::ItemFlags flags(const QModelIndex& index) const override;
        bool setData(const QModelIndex& index, const QVariant& SCValue,
                     int role = Qt::EditRole) override;

        StableCore::Storage::RecordId RecordIdAt(int row) const;
        StableCore::Storage::SCTableViewColumnDef ColumnAt(int column) const;
        int RowCountValue() const noexcept;
        void Refresh();

    private:
        struct RowData
        {
            StableCore::Storage::RecordId recordId{0};
        };

        SCDatabaseSession* session_{nullptr};
        QVector<StableCore::Storage::SCTableViewColumnDef> columns_;
        QVector<RowData> rows_;
    };

}  // namespace StableCore::Storage::Editor
