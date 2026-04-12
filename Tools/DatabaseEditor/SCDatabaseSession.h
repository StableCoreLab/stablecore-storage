#pragma once

#include <functional>

#include <QHash>
#include <QString>
#include <QStringList>
#include <QPair>
#include <QObject>
#include <QVariant>
#include <QVector>

#include "StableCore/Storage/SCStorage.h"

namespace StableCore::Storage::Editor
{

class SCDatabaseSession final : public QObject
{
    Q_OBJECT

public:
    struct RelationCandidate
    {
        StableCore::Storage::RecordId recordId{0};
        QString label;
        QVector<QPair<QString, QString>> previewFields;
    };

    explicit SCDatabaseSession(QObject* parent = nullptr);

    bool IsOpen() const noexcept;
    QString DatabasePath() const;
    QString CurrentTableName() const;
    QStringList TableNames() const;

    StableCore::Storage::ISCDatabase* Database() const noexcept;
    StableCore::Storage::ISCTable* CurrentTable() const noexcept;
    StableCore::Storage::ISCComputedTableView* CurrentTableView() const noexcept;

    bool CreateDatabase(const QString& filePath, QString* outError);
    bool OpenDatabase(const QString& filePath, QString* outError);
    bool Refresh(QString* outError);
    bool CreateTable(const QString& tableName, QString* outError);
    bool SelectTable(const QString& tableName, QString* outError);
    bool AddColumn(const StableCore::Storage::SCColumnDef& column, QString* outError);
    bool AddRecord(QString* outError);
    bool DeleteRecord(StableCore::Storage::RecordId recordId, QString* outError);
    bool Undo(QString* outError);
    bool Redo(QString* outError);
    bool SetCellValue(
        StableCore::Storage::RecordId recordId,
        const QString& columnName,
        const QVariant& SCValue,
        QString* outError);
    bool GetColumnDef(const QString& columnName, StableCore::Storage::SCColumnDef* outColumn, QString* outError) const;
    bool BuildRelationCandidates(
        const QString& targetTableName,
        QVector<RelationCandidate>* outCandidates,
        QString* outError) const;
    bool AddSessionComputedColumn(const StableCore::Storage::SCComputedColumnDef& column, QString* outError);
    bool UpdateSessionComputedColumn(
        const QString& originalName,
        const StableCore::Storage::SCComputedColumnDef& column,
        QString* outError);
    bool RemoveSessionComputedColumn(const QString& name, QString* outError);
    bool GetSessionComputedColumn(
        const QString& name,
        StableCore::Storage::SCComputedColumnDef* outColumn,
        QString* outError) const;
    QVector<StableCore::Storage::SCComputedColumnDef> CurrentSessionComputedColumns() const;

    QString BuildHealthSummary() const;
    bool BuildSchemaSnapshot(QVector<StableCore::Storage::SCColumnDef>* outColumns, QString* outError) const;
    bool BuildRecordSnapshot(
        StableCore::Storage::RecordId recordId,
        QVector<QPair<QString, QString>>* outFields,
        QString* outError) const;

signals:
    void DatabaseOpened();
    void TablesChanged();
    void CurrentTableChanged();
    void RecordsChanged();

private:
    QVector<StableCore::Storage::SCComputedColumnDef>* CurrentSessionComputedColumnsStorage();
    const QVector<StableCore::Storage::SCComputedColumnDef>* CurrentSessionComputedColumnsStorage() const;
    bool LoadTableNames(QString* outError);
    bool RebuildCurrentTableView(QString* outError);
    bool BeginAndCommitSingleAction(
        const wchar_t* actionName,
        const std::function<StableCore::Storage::ErrorCode()>& action,
        QString* outError);
    StableCore::Storage::ErrorCode ConvertVariantToValue(
        const StableCore::Storage::SCColumnDef& column,
        const QVariant& input,
        StableCore::Storage::SCValue* outValue) const;
    QString ErrorToString(StableCore::Storage::ErrorCode error) const;

    StableCore::Storage::SCDbPtr db_;
    StableCore::Storage::SCTablePtr currentTable_;
    StableCore::Storage::SCComputedTableViewPtr currentTableView_;
    QString databasePath_;
    QString currentTableName_;
    QStringList tableNames_;
    QHash<QString, QVector<StableCore::Storage::SCComputedColumnDef>> sessionComputedColumnsByTable_;
};

}  // namespace StableCore::Storage::Editor
