#pragma once

#include <QCheckBox>
#include <QComboBox>
#include <QDialog>
#include <QDialogButtonBox>
#include <QFormLayout>
#include <QLineEdit>

#include "SCStorage.h"

namespace StableCore::Storage::Editor
{

    class SCAddColumnDialog final : public QDialog
    {
        Q_OBJECT

    public:
        explicit SCAddColumnDialog(QWidget* parent = nullptr);

        StableCore::Storage::SCColumnDef BuildColumnDef() const;

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

}  // namespace StableCore::Storage::Editor
