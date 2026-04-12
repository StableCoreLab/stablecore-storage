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

namespace stablecore::storage::editor
{

class SCDatabaseSession final : public QObject
{
    Q_OBJECT

public:
    struct RelationCandidate
    {
        stablecore::storage::RecordId recordId{0};
        QString label;
        QVector<QPair<QString, QString>> previewFields;
    };

    explicit SCDatabaseSession(QObject* parent = nullptr);

    bool IsOpen() const noexcept;
    QString DatabasePath() const;
    QString CurrentTableName() const;
    QStringList TableNames() const;

    stablecore::storage::ISCDatabase* Database() const noexcept;
    stablecore::storage::ISCTable* CurrentTable() const noexcept;
    stablecore::storage::ISCComputedTableView* CurrentTableView() const noexcept;

    bool CreateDatabase(const QString& filePath, QString* outError);
    bool OpenDatabase(const QString& filePath, QString* outError);
    bool Refresh(QString* outError);
    bool CreateTable(const QString& tableName, QString* outError);
    bool SelectTable(const QString& tableName, QString* outError);
    bool AddColumn(const stablecore::storage::SCColumnDef& column, QString* outError);
    bool AddRecord(QString* outError);
    bool DeleteRecord(stablecore::storage::RecordId recordId, QString* outError);
    bool Undo(QString* outError);
    bool Redo(QString* outError);
    bool SetCellValue(
        stablecore::storage::RecordId recordId,
        const QString& columnName,
        const QVariant& SCValue,
        QString* outError);
    bool GetColumnDef(const QString& columnName, stablecore::storage::SCColumnDef* outColumn, QString* outError) const;
    bool BuildRelationCandidates(
        const QString& targetTableName,
        QVector<RelationCandidate>* outCandidates,
        QString* outError) const;
    bool AddSessionComputedColumn(const stablecore::storage::SCComputedColumnDef& column, QString* outError);
    bool UpdateSessionComputedColumn(
        const QString& originalName,
        const stablecore::storage::SCComputedColumnDef& column,
        QString* outError);
    bool RemoveSessionComputedColumn(const QString& name, QString* outError);
    bool GetSessionComputedColumn(
        const QString& name,
        stablecore::storage::SCComputedColumnDef* outColumn,
        QString* outError) const;
    QVector<stablecore::storage::SCComputedColumnDef> CurrentSessionComputedColumns() const;

    QString BuildHealthSummary() const;
    bool BuildSchemaSnapshot(QVector<stablecore::storage::SCColumnDef>* outColumns, QString* outError) const;
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
    QVector<stablecore::storage::SCComputedColumnDef>* CurrentSessionComputedColumnsStorage();
    const QVector<stablecore::storage::SCComputedColumnDef>* CurrentSessionComputedColumnsStorage() const;
    bool LoadTableNames(QString* outError);
    bool RebuildCurrentTableView(QString* outError);
    bool BeginAndCommitSingleAction(
        const wchar_t* actionName,
        const std::function<stablecore::storage::ErrorCode()>& action,
        QString* outError);
    stablecore::storage::ErrorCode ConvertVariantToValue(
        const stablecore::storage::SCColumnDef& column,
        const QVariant& input,
        stablecore::storage::SCValue* outValue) const;
    QString ErrorToString(stablecore::storage::ErrorCode error) const;

    stablecore::storage::SCDbPtr db_;
    stablecore::storage::SCTablePtr currentTable_;
    stablecore::storage::SCComputedTableViewPtr currentTableView_;
    QString databasePath_;
    QString currentTableName_;
    QStringList tableNames_;
    QHash<QString, QVector<stablecore::storage::SCComputedColumnDef>> sessionComputedColumnsByTable_;
};

}  // namespace stablecore::storage::editor
