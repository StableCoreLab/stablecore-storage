#pragma once

#include <string>
#include <vector>

#include <QWidget>

#include "SCTypes.h"

class QCheckBox;
class QGroupBox;
class QLabel;
class QLineEdit;
class QMenu;
class QPoint;
class QPlainTextEdit;
class QTabWidget;
class QTreeWidget;
class QPushButton;

namespace StableCore::Storage::Editor
{

    class SCDatabaseSession;

    class SCTableDesignPane final : public QWidget
    {
        Q_OBJECT

    public:
        explicit SCTableDesignPane(SCDatabaseSession* session,
                                   QWidget* parent = nullptr);

        void Refresh();
        QString CurrentColumnName() const;
        void SelectColumnByName(const QString& columnName);

    signals:
        void StatusMessage(const QString& text);

    private slots:
        void AddColumn();
        void EditColumn();
        void RemoveColumn();
        void RefreshPreview();
        void CopyPreview();
        void AddConstraint();
        void EditConstraint();
        void RemoveConstraint();
        void AddIndex();
        void EditIndex();
        void RemoveIndex();
        void OnColumnsContextMenuRequested(const QPoint& pos);
        void UpdateColumnActionState();

    private:
        QString CurrentConstraintName() const;
        QString CurrentIndexName() const;
        std::vector<std::wstring> GetAvailableColumnNames() const;
        void BuildUi();
        void UpdateOverview();
        void UpdateColumns();
        void UpdateConstraints();
        void UpdateIndexes();
        void UpdatePreview();
        void SetStatus(const QString& text);

        SCDatabaseSession* session_{nullptr};
        StableCore::Storage::SCSchemaSnapshot schemaSnapshot_;
        QString tableName_;

        QLabel* tableNameValueLabel_{nullptr};
        QLabel* primaryKeyValueLabel_{nullptr};
        QLabel* statsValueLabel_{nullptr};
        QLabel* legacyHintValueLabel_{nullptr};
        QLineEdit* tableDescriptionEdit_{nullptr};
        QCheckBox* includeLegacyIndexesCheck_{nullptr};
        QTreeWidget* columnsTree_{nullptr};
        QPushButton* addColumnButton_{nullptr};
        QPushButton* editColumnButton_{nullptr};
        QPushButton* deleteColumnButton_{nullptr};
        QTabWidget* structureTabs_{nullptr};
        QTreeWidget* constraintsTree_{nullptr};
        QTreeWidget* indexesTree_{nullptr};
        QPlainTextEdit* previewEdit_{nullptr};
        QLabel* statusLabel_{nullptr};
    };

}  // namespace StableCore::Storage::Editor
