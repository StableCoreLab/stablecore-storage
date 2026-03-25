#pragma once

#include <QCheckBox>
#include <QComboBox>
#include <QDialog>
#include <QLineEdit>
#include <QPlainTextEdit>

#include "StableCore/Storage/Types.h"

namespace stablecore::storage::editor
{

class ComputedColumnDialog final : public QDialog
{
    Q_OBJECT

public:
    explicit ComputedColumnDialog(const QString& currentTableName, QWidget* parent = nullptr);

    bool BuildDefinition(stablecore::storage::ComputedColumnDef* outColumn, QString* outError) const;

private slots:
    void UpdateModeVisibility();

private:
    stablecore::storage::ValueKind CurrentValueKind() const;
    stablecore::storage::ComputedFieldKind CurrentComputedKind() const;
    stablecore::storage::AggregateKind CurrentAggregateKind() const;

    QString currentTableName_;
    QLineEdit* nameEdit_{nullptr};
    QLineEdit* displayNameEdit_{nullptr};
    QComboBox* valueKindCombo_{nullptr};
    QComboBox* kindCombo_{nullptr};
    QPlainTextEdit* expressionEdit_{nullptr};
    QLineEdit* ruleIdEdit_{nullptr};
    QComboBox* aggregateKindCombo_{nullptr};
    QLineEdit* aggregateRelationEdit_{nullptr};
    QLineEdit* aggregateFieldEdit_{nullptr};
    QLineEdit* factDepsEdit_{nullptr};
    QLineEdit* relationDepsEdit_{nullptr};
    QCheckBox* cacheableCheck_{nullptr};
};

}  // namespace stablecore::storage::editor
