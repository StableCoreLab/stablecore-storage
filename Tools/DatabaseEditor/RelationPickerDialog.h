#pragma once

#include <QDialog>
#include <QLineEdit>
#include <QTableWidget>
#include <QVector>

#include "DatabaseSession.h"

namespace stablecore::storage::editor
{

class RelationPickerDialog final : public QDialog
{
    Q_OBJECT

public:
    RelationPickerDialog(
        const QString& targetTableName,
        const QVector<DatabaseSession::RelationCandidate>& candidates,
        QWidget* parent = nullptr);

    stablecore::storage::RecordId SelectedRecordId() const noexcept;

private slots:
    void ApplyFilter(const QString& text);
    void AcceptCurrentSelection();

private:
    void PopulateRows();

    QString targetTableName_;
    QVector<DatabaseSession::RelationCandidate> candidates_;
    QLineEdit* filterEdit_{nullptr};
    QTableWidget* tableWidget_{nullptr};
};

}  // namespace stablecore::storage::editor
