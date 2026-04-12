#pragma once

#include <QCheckBox>
#include <QComboBox>
#include <QDialog>
#include <QDialogButtonBox>
#include <QFormLayout>
#include <QLineEdit>

#include "StableCore/Storage/SCStorage.h"

namespace stablecore::storage::editor
{

class AddColumnDialog final : public QDialog
{
    Q_OBJECT

public:
    explicit AddColumnDialog(QWidget* parent = nullptr);

    stablecore::storage::SCColumnDef BuildColumnDef() const;

private:
    QLineEdit* nameEdit_{nullptr};
    QLineEdit* displayNameEdit_{nullptr};
    QComboBox* valueKindCombo_{nullptr};
    QCheckBox* relationCheck_{nullptr};
    QCheckBox* nullableCheck_{nullptr};
    QCheckBox* editableCheck_{nullptr};
    QCheckBox* userDefinedCheck_{nullptr};
    QCheckBox* indexedCheck_{nullptr};
    QCheckBox* participatesInCalcCheck_{nullptr};
    QLineEdit* unitEdit_{nullptr};
    QLineEdit* referenceTableEdit_{nullptr};
    QLineEdit* defaultValueEdit_{nullptr};
};

}  // namespace stablecore::storage::editor
