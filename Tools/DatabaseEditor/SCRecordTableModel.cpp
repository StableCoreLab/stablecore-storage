#include "SCRecordTableModel.h"

namespace sc = StableCore::Storage;

namespace StableCore::Storage::Editor
{
    namespace
    {

        QString ToQString(const std::wstring& text)
        {
            return QString::fromStdWString(text);
        }

        QVariant ValueToVariant(const sc::SCValue& SCValue)
        {
            switch (SCValue.GetKind())
            {
                case sc::ValueKind::Null:
                    return QVariant{};
                case sc::ValueKind::Int64: {
                    std::int64_t v = 0;
                    SCValue.AsInt64(&v);
                    return QVariant::fromValue<qlonglong>(v);
                }
                case sc::ValueKind::Double: {
                    double v = 0.0;
                    SCValue.AsDouble(&v);
                    return v;
                }
                case sc::ValueKind::Bool: {
                    bool v = false;
                    SCValue.AsBool(&v);
                    return v;
                }
                case sc::ValueKind::String: {
                    std::wstring v;
                    SCValue.AsStringCopy(&v);
                    return ToQString(v);
                }
                case sc::ValueKind::RecordId: {
                    sc::RecordId v = 0;
                    SCValue.AsRecordId(&v);
                    return QVariant::fromValue<qlonglong>(v);
                }
                case sc::ValueKind::Enum: {
                    std::wstring v;
                    SCValue.AsEnumCopy(&v);
                    return ToQString(v);
                }
                default:
                    return QStringLiteral("<unsupported>");
            }
        }

    }  // namespace

    SCRecordTableModel::SCRecordTableModel(SCDatabaseSession* session,
                                           QObject* parent)
        : QAbstractTableModel(parent), session_(session)
    {
        connect(session_, &SCDatabaseSession::CurrentTableChanged, this,
                &SCRecordTableModel::Refresh);
        connect(session_, &SCDatabaseSession::RecordsChanged, this,
                &SCRecordTableModel::Refresh);
    }

    int SCRecordTableModel::rowCount(const QModelIndex& parent) const
    {
        return parent.isValid() ? 0 : rows_.size();
    }

    int SCRecordTableModel::columnCount(const QModelIndex& parent) const
    {
        return parent.isValid() ? 0 : columns_.size();
    }

    QVariant SCRecordTableModel::data(const QModelIndex& index, int role) const
    {
        if (!index.isValid() || index.row() < 0 ||
            index.row() >= rows_.size() || index.column() < 0 ||
            index.column() >= columns_.size())
        {
            return QVariant{};
        }

        if (role != Qt::DisplayRole && role != Qt::EditRole &&
            role != Qt::ToolTipRole)
        {
            return QVariant{};
        }

        sc::ISCComputedTableView* view = session_->CurrentTableView();
        if (view == nullptr)
        {
            return QVariant{};
        }

        sc::SCValue SCValue;
        const sc::ErrorCode rc =
            view->GetCellValue(rows_[index.row()].recordId,
                               columns_[index.column()].name.c_str(), &SCValue);
        if (rc == sc::SC_E_VALUE_IS_NULL)
        {
            return QVariant{};
        }
        if (sc::Failed(rc))
        {
            return role == Qt::DisplayRole || role == Qt::ToolTipRole
                       ? QStringLiteral("<error>")
                       : QVariant{};
        }
        return ValueToVariant(SCValue);
    }

    QVariant SCRecordTableModel::headerData(int section,
                                            Qt::Orientation orientation,
                                            int role) const
    {
        if (role != Qt::DisplayRole)
        {
            return QVariant{};
        }

        if (orientation == Qt::Vertical)
        {
            return section + 1;
        }

        if (section < 0 || section >= columns_.size())
        {
            return QVariant{};
        }

        return ToQString(columns_[section].displayName.empty()
                             ? columns_[section].name
                             : columns_[section].displayName);
    }

    Qt::ItemFlags SCRecordTableModel::flags(const QModelIndex& index) const
    {
        if (!index.isValid())
        {
            return Qt::NoItemFlags;
        }

        Qt::ItemFlags result = Qt::ItemIsSelectable | Qt::ItemIsEnabled;
        if (columns_[index.column()].layer == sc::TableColumnLayer::Fact &&
            columns_[index.column()].editable)
        {
            result |= Qt::ItemIsEditable;
        }
        return result;
    }

    bool SCRecordTableModel::setData(const QModelIndex& index,
                                     const QVariant& SCValue, int role)
    {
        if (role != Qt::EditRole || !index.isValid())
        {
            return false;
        }

        QString error;
        const bool ok = session_->SetCellValue(
            rows_[index.row()].recordId,
            ToQString(columns_[index.column()].name), SCValue, &error);
        if (ok)
        {
            emit dataChanged(index, index, {Qt::DisplayRole, Qt::EditRole});
        }
        return ok;
    }

    sc::RecordId SCRecordTableModel::RecordIdAt(int row) const
    {
        if (row < 0 || row >= rows_.size())
        {
            return 0;
        }
        return rows_[row].recordId;
    }

    sc::SCTableViewColumnDef SCRecordTableModel::ColumnAt(int column) const
    {
        if (column < 0 || column >= columns_.size())
        {
            return {};
        }
        return columns_[column];
    }

    int SCRecordTableModel::RowCountValue() const noexcept
    {
        return rows_.size();
    }

    void SCRecordTableModel::Refresh()
    {
        beginResetModel();
        columns_.clear();
        rows_.clear();

        sc::ISCComputedTableView* view = session_->CurrentTableView();
        if (view != nullptr)
        {
            std::int32_t columnCount = 0;
            if (view->GetColumnCount(&columnCount) == sc::SC_OK)
            {
                for (std::int32_t index = 0; index < columnCount; ++index)
                {
                    sc::SCTableViewColumnDef column;
                    if (view->GetColumn(index, &column) == sc::SC_OK)
                    {
                        columns_.push_back(column);
                    }
                }
            }

            sc::SCRecordCursorPtr cursor;
            if (view->EnumerateRecords(cursor) == sc::SC_OK)
            {
                bool hasRow = false;
                while (cursor->MoveNext(&hasRow) == sc::SC_OK && hasRow)
                {
                    sc::SCRecordPtr record;
                    if (cursor->GetCurrent(record) == sc::SC_OK)
                    {
                        rows_.push_back(RowData{record->GetId()});
                    }
                }
            }
        }

        endResetModel();
    }

}  // namespace StableCore::Storage::Editor
