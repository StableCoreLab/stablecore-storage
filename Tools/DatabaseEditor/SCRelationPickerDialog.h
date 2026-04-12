#pragma once

#include <QDialog>
#include <QLineEdit>
#include <QTableWidget>
#include <QVector>

#include "SCDatabaseSession.h"

namespace stablecore::storage::editor
{

class SCRelationPickerDialog final : public QDialog
{
    Q_OBJECT

public:
    SCRelationPickerDialog(
        const QString& targetTableName,
        const QVector<SCDatabaseSession::RelationCandidate>& candidates,
        QWidget* parent = nullptr);

    stablecore::storage::RecordId SelectedRecordId() const noexcept;

private slots:
    void ApplyFilter(const QString& text);
    void AcceptCurrentSelection();

private:
    void PopulateRows();

    QString targetTableName_;
    QVector<SCDatabaseSession::RelationCandidate> candidates_;
    QLineEdit* filterEdit_{nullptr};
    QTableWidget* tableWidget_{nullptr};
};

}  // namespace stablecore::storage::editor
