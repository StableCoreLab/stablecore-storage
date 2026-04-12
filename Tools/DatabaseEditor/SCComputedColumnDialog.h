#pragma once

#include <QCheckBox>
#include <QComboBox>
#include <QDialog>
#include <QLineEdit>
#include <QPlainTextEdit>

#include "StableCore/Storage/SCTypes.h"

namespace stablecore::storage::editor
{

class SCComputedColumnDialog final : public QDialog
{
    Q_OBJECT

public:
    explicit SCComputedColumnDialog(const QString& currentTableName, QWidget* parent = nullptr);
    SCComputedColumnDialog(
        const QString& currentTableName,
        const stablecore::storage::SCComputedColumnDef& initialValue,
        QWidget* parent = nullptr);

    bool BuildDefinition(stablecore::storage::SCComputedColumnDef* outColumn, QString* outError) const;

private slots:
    void UpdateModeVisibility();

private:
    void BuildForm();
    void ApplyInitialValue(const stablecore::storage::SCComputedColumnDef& initialValue);
    stablecore::storage::ValueKind CurrentValueKind() const;
    stablecore::storage::ComputedFieldKind CurrentComputedKind() const;
    stablecore::storage::SCAggregateKind CurrentAggregateKind() const;

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
