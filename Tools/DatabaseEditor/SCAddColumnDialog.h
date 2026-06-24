#pragma once

#include <QCheckBox>
#include <QComboBox>
#include <QDialog>
#include <QDialogButtonBox>
#include <QFormLayout>
#include <QLabel>
#include <QLineEdit>
#include <QCompleter>
#include <QStringList>
#include <QStringListModel>
#include <QPushButton>

#include "SCStorage.h"

namespace StableCore::Storage::Editor
{
    class SCDatabaseSession;

    class SCAddColumnDialog final : public QDialog
    {
        Q_OBJECT

    public:
        explicit SCAddColumnDialog(SCDatabaseSession* session = nullptr,
                                   QWidget* parent = nullptr);
        explicit SCAddColumnDialog(SCDatabaseSession* session,
                                   const StableCore::Storage::SCColumnDef& initialValue,
                                   QWidget* parent = nullptr);

        StableCore::Storage::SCColumnDef BuildColumnDef() const;
        void SetCurrentTableHasRecords(bool hasRecords);

    private:
        void ApplyInitialValue(const StableCore::Storage::SCColumnDef& value);
        void UpdateValidationState();
        void UpdateRelationHints();
        bool LoadReferenceTableSnapshot(
            StableCore::Storage::SCTableSchemaSnapshot* outSnapshot,
            QString* outError) const;
        QLineEdit* nameEdit_{nullptr};
        QLineEdit* displayNameEdit_{nullptr};
        QComboBox* valueKindCombo_{nullptr};
        QCheckBox* relationCheck_{nullptr};
        QComboBox* relationBindingCombo_{nullptr};
        QCheckBox* nullableCheck_{nullptr};
        QCheckBox* editableCheck_{nullptr};
        QCheckBox* userDefinedCheck_{nullptr};
        QCheckBox* indexedCheck_{nullptr};
        QCheckBox* participatesInCalcCheck_{nullptr};
        QLineEdit* unitEdit_{nullptr};
        QLineEdit* referenceTableEdit_{nullptr};
        QLabel* referenceTableLabel_{nullptr};
        QLineEdit* referenceStorageColumnEdit_{nullptr};
        QLabel* referenceStorageColumnLabel_{nullptr};
        QLineEdit* referenceDisplayColumnEdit_{nullptr};
        QLabel* referenceDisplayColumnLabel_{nullptr};
        QLineEdit* defaultValueEdit_{nullptr};
        QLabel* validationLabel_{nullptr};
        QLabel* relationHintLabel_{nullptr};
        QCompleter* referenceTableCompleter_{nullptr};
        QCompleter* referenceStorageCompleter_{nullptr};
        QCompleter* referenceDisplayCompleter_{nullptr};
        QStringListModel* referenceTableModel_{nullptr};
        QStringListModel* referenceStorageModel_{nullptr};
        QStringListModel* referenceDisplayModel_{nullptr};
        QDialogButtonBox* buttonBox_{nullptr};
        QPushButton* okButton_{nullptr};
        bool currentTableHasRecords_{false};
        SCDatabaseSession* session_{nullptr};
    };

}  // namespace StableCore::Storage::Editor
