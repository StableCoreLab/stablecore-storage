#pragma once

#include <QDialog>
#include <QString>
#include <QVector>
#include <QStringList>

#include "SCStorage.h"

class QLineEdit;
class QTableWidget;
class QPushButton;

namespace StableCore::Storage::Editor
{

    class SCIndexEditorDialog final : public QDialog
    {
        Q_OBJECT

    public:
        SCIndexEditorDialog(StableCore::Storage::SCIndexDef index,
                            const std::vector<std::wstring>& availableColumns,
                            QWidget* parent = nullptr);

        StableCore::Storage::SCIndexDef GetIndex() const { return index_; }

    protected:
        void accept() override;

    private slots:
        void AddColumn();
        void RemoveColumn();
        void MoveColumnUp();
        void MoveColumnDown();
        void UpdateColumnOrder(int row, int column);
        void ValidateInput();

    private:
        void PopulateColumnList();
        QStringList GetSelectedColumnNames() const;

        StableCore::Storage::SCIndexDef index_;
        std::vector<std::wstring> availableColumns_;

        QLineEdit* indexNameEdit_{nullptr};
        QTableWidget* columnsTable_{nullptr};
        QPushButton* addButton_{nullptr};
        QPushButton* removeButton_{nullptr};
        QPushButton* upButton_{nullptr};
        QPushButton* downButton_{nullptr};
        QPushButton* okButton_{nullptr};
    };

}  // namespace StableCore::Storage::Editor
