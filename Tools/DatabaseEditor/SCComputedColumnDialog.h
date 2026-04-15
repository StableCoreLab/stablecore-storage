#pragma once

#include <QCheckBox>
#include <QComboBox>
#include <QDialog>
#include <QLineEdit>
#include <QPlainTextEdit>

#include "SCTypes.h"

namespace StableCore::Storage::Editor
{

class SCComputedColumnDialog final : public QDialog
{
    Q_OBJECT

public:
    explicit SCComputedColumnDialog(const QString& currentTableName, QWidget* parent = nullptr);
    SCComputedColumnDialog(
        const QString& currentTableName,
        const StableCore::Storage::SCComputedColumnDef& initialValue,
        QWidget* parent = nullptr);

    bool BuildDefinition(StableCore::Storage::SCComputedColumnDef* outColumn, QString* outError) const;

private slots:
    void UpdateModeVisibility();

private:
    void BuildForm();
    void ApplyInitialValue(const StableCore::Storage::SCComputedColumnDef& initialValue);
    StableCore::Storage::ValueKind CurrentValueKind() const;
    StableCore::Storage::ComputedFieldKind CurrentComputedKind() const;
    StableCore::Storage::SCAggregateKind CurrentAggregateKind() const;

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

}  // namespace StableCore::Storage::Editor
