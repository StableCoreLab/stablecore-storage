#pragma once

#include <functional>

#include <QString>
#include <QStringList>
#include <QPair>
#include <QObject>
#include <QVariant>
#include <QVector>

#include "StableCore/Storage/Storage.h"

namespace stablecore::storage::editor
{

class DatabaseSession final : public QObject
{
    Q_OBJECT

public:
    explicit DatabaseSession(QObject* parent = nullptr);

    bool IsOpen() const noexcept;
    QString DatabasePath() const;
    QString CurrentTableName() const;
    QStringList TableNames() const;

    stablecore::storage::IDatabase* Database() const noexcept;
    stablecore::storage::ITable* CurrentTable() const noexcept;
    stablecore::storage::IComputedTableView* CurrentTableView() const noexcept;

    bool CreateDatabase(const QString& filePath, QString* outError);
    bool OpenDatabase(const QString& filePath, QString* outError);
    bool Refresh(QString* outError);
    bool CreateTable(const QString& tableName, QString* outError);
    bool SelectTable(const QString& tableName, QString* outError);
    bool AddColumn(const stablecore::storage::ColumnDef& column, QString* outError);
    bool AddRecord(QString* outError);
    bool DeleteRecord(stablecore::storage::RecordId recordId, QString* outError);
    bool Undo(QString* outError);
    bool Redo(QString* outError);
    bool SetCellValue(
        stablecore::storage::RecordId recordId,
        const QString& columnName,
        const QVariant& value,
        QString* outError);

    QString BuildHealthSummary() const;
    bool BuildSchemaSnapshot(QVector<stablecore::storage::ColumnDef>* outColumns, QString* outError) const;
    bool BuildRecordSnapshot(
        stablecore::storage::RecordId recordId,
        QVector<QPair<QString, QString>>* outFields,
        QString* outError) const;

signals:
    void DatabaseOpened();
    void TablesChanged();
    void CurrentTableChanged();
    void RecordsChanged();

private:
    bool LoadTableNames(QString* outError);
    bool BeginAndCommitSingleAction(
        const wchar_t* actionName,
        const std::function<stablecore::storage::ErrorCode()>& action,
        QString* outError);
    stablecore::storage::ErrorCode ConvertVariantToValue(
        const stablecore::storage::ColumnDef& column,
        const QVariant& input,
        stablecore::storage::Value* outValue) const;
    QString ErrorToString(stablecore::storage::ErrorCode error) const;

    stablecore::storage::DbPtr db_;
    stablecore::storage::TablePtr currentTable_;
    stablecore::storage::ComputedTableViewPtr currentTableView_;
    QString databasePath_;
    QString currentTableName_;
    QStringList tableNames_;
};

}  // namespace stablecore::storage::editor
