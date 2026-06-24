#pragma once

#include <QComboBox>
#include <QDialog>
#include <QLineEdit>
#include <QStringList>
#include <QVector>

#include "SCStorage.h"

class QCompleter;
class QLabel;
class QPlainTextEdit;
class QPushButton;
class QTableWidget;
class QStringListModel;

namespace StableCore::Storage::Editor
{

    class SCDatabaseSession;

    class SCConstraintEditorDialog final : public QDialog
    {
        Q_OBJECT

    public:
        SCConstraintEditorDialog(
            SCDatabaseSession* session,
            StableCore::Storage::SCConstraintDef constraint,
            const std::vector<std::wstring>& availableColumns,
            QWidget* parent = nullptr);

        StableCore::Storage::SCConstraintDef GetConstraint() const
        {
            return constraint_;
        }

    private slots:
        void AddColumn();
        void RemoveColumn();
        void MoveColumnUp();
        void MoveColumnDown();
        void AddReferencedColumn();
        void RemoveReferencedColumn();
        void MoveReferencedColumnUp();
        void MoveReferencedColumnDown();
        void ValidateInput();

    private:
        void PopulateColumnList();
        void PopulateReferencedColumnList();
        void UpdateKindUi();
        void RefreshReferencedTableHints();
        bool LoadReferencedTableSnapshot(
            StableCore::Storage::SCTableSchemaSnapshot* outSnapshot,
            QString* outError) const;
        QStringList GetSelectedColumnNames(const QTableWidget* table) const;
        QStringList GetAvailableColumns(
            const std::vector<std::wstring>& sourceColumns,
            const QStringList& selectedColumns) const;

        SCDatabaseSession* session_{nullptr};
        StableCore::Storage::SCConstraintDef constraint_;
        std::vector<std::wstring> availableColumns_;

        QLineEdit* nameEdit_{nullptr};
        QComboBox* kindCombo_{nullptr};
        QTableWidget* columnsTable_{nullptr};
        QPushButton* addColumnButton_{nullptr};
        QPushButton* removeColumnButton_{nullptr};
        QPushButton* upColumnButton_{nullptr};
        QPushButton* downColumnButton_{nullptr};
        QLineEdit* referencedTableEdit_{nullptr};
        QCompleter* referencedTableCompleter_{nullptr};
        QStringListModel* referencedTableModel_{nullptr};
        QTableWidget* referencedColumnsTable_{nullptr};
        QPushButton* addReferencedColumnButton_{nullptr};
        QPushButton* removeReferencedColumnButton_{nullptr};
        QPushButton* upReferencedColumnButton_{nullptr};
        QPushButton* downReferencedColumnButton_{nullptr};
        QComboBox* onDeleteCombo_{nullptr};
        QComboBox* onUpdateCombo_{nullptr};
        QPlainTextEdit* checkExpressionEdit_{nullptr};
        QLabel* validationLabel_{nullptr};
        QPushButton* okButton_{nullptr};
    };

}  // namespace StableCore::Storage::Editor
