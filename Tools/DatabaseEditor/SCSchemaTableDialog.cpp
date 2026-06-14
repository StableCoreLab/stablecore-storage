#include "SCSchemaTableDialog.h"

#include <algorithm>

#include <QApplication>
#include <QCheckBox>
#include <QClipboard>
#include <QFormLayout>
#include <QHBoxLayout>
#include <QLabel>
#include <QLineEdit>
#include <QListWidget>
#include <QPlainTextEdit>
#include <QPushButton>
#include <QVBoxLayout>

#include "SCDatabaseSession.h"
#include "SCSchemaTableGenerator.h"
#include "SCIndexEditorDialog.h"

namespace sc = StableCore::Storage;

namespace StableCore::Storage::Editor
{
    SCSchemaTableDialog::SCSchemaTableDialog(SCDatabaseSession* session,
                                             QWidget* parent)
        : QDialog(parent), session_(session)
    {
        setWindowTitle(QStringLiteral("表结构"));
        resize(1080, 720);

        auto* layout = new QVBoxLayout(this);
        auto* introLabel = new QLabel(
            QStringLiteral(
                "从当前表生成 SC_SCHEMA_TABLE 代码。"
                "默认值会被导出（如果存在）。"
                "主键和索引会被显式导出；"
                "旧式索引提示除非确认，否则不会被导出。"),
            this);
        introLabel->setWordWrap(true);
        layout->addWidget(introLabel);

        auto* form = new QFormLayout();
        tableNameLabel_ = new QLabel(this);
        tableNameLabel_->setTextInteractionFlags(Qt::TextSelectableByMouse);
        tableNameLabel_->setText(QStringLiteral("-"));
        form->addRow(QStringLiteral("当前表"), tableNameLabel_);

        tableDescriptionEdit_ = new QLineEdit(this);
        tableDescriptionEdit_->setPlaceholderText(
            QStringLiteral("可选的表描述"));
        form->addRow(QStringLiteral("表描述"),
                     tableDescriptionEdit_);

        primaryKeyEdit_ = new QLineEdit(this);
        primaryKeyEdit_->setPlaceholderText(
            QStringLiteral("显式主键列名"));
        form->addRow(QStringLiteral("主键"), primaryKeyEdit_);

        includeLegacyIndexesCheck_ = new QCheckBox(
            QStringLiteral("包含旧式索引列"), this);
        includeLegacyIndexesCheck_->setChecked(false);
        form->addRow(QStringLiteral("旧式索引"),
                     includeLegacyIndexesCheck_);

        legacyHintLabel_ = new QLabel(this);
        legacyHintLabel_->setWordWrap(true);
        legacyHintLabel_->setText(
            QStringLiteral("旧式索引提示将显示在这里。"));
        form->addRow(QStringLiteral("旧式提示"), legacyHintLabel_);
        layout->addLayout(form);

        auto* indexGroup = new QHBoxLayout();
        indexList_ = new QListWidget(this);
        indexGroup->addWidget(indexList_, 1);

        auto* indexButtons = new QVBoxLayout();
        addIndexButton_ = new QPushButton(QStringLiteral("+ 新建"), this);
        editIndexButton_ = new QPushButton(QStringLiteral("编辑"), this);
        removeIndexButton_ = new QPushButton(QStringLiteral("- 删除"), this);
        indexButtons->addWidget(addIndexButton_);
        indexButtons->addWidget(editIndexButton_);
        indexButtons->addWidget(removeIndexButton_);
        indexButtons->addStretch(1);
        indexGroup->addLayout(indexButtons);
        layout->addLayout(indexGroup);

        outputEdit_ = new QPlainTextEdit(this);
        outputEdit_->setReadOnly(true);
        outputEdit_->setLineWrapMode(QPlainTextEdit::NoWrap);
        outputEdit_->setPlaceholderText(
            QStringLiteral("生成的代码将显示在这里。"));
        layout->addWidget(outputEdit_, 1);

        statusLabel_ = new QLabel(QStringLiteral("就绪。"), this);
        layout->addWidget(statusLabel_);

        auto* buttonRow = new QHBoxLayout();
        auto* refreshButton = new QPushButton(QStringLiteral("刷新"), this);
        auto* copyButton = new QPushButton(QStringLiteral("复制代码"), this);
        auto* closeButton = new QPushButton(QStringLiteral("关闭"), this);
        buttonRow->addWidget(refreshButton);
        buttonRow->addWidget(copyButton);
        buttonRow->addStretch(1);
        buttonRow->addWidget(closeButton);
        layout->addLayout(buttonRow);

        connect(refreshButton, &QPushButton::clicked, this,
                &SCSchemaTableDialog::RefreshPreview);
        connect(copyButton, &QPushButton::clicked, this,
                &SCSchemaTableDialog::CopyOutput);
        connect(closeButton, &QPushButton::clicked, this, &QDialog::accept);
        connect(tableDescriptionEdit_, &QLineEdit::textChanged, this,
                &SCSchemaTableDialog::UpdateOutput);
        connect(primaryKeyEdit_, &QLineEdit::textChanged, this,
                &SCSchemaTableDialog::UpdateOutput);
        connect(includeLegacyIndexesCheck_, &QCheckBox::toggled, this,
                &SCSchemaTableDialog::UpdateOutput);
        connect(addIndexButton_, &QPushButton::clicked, this,
                &SCSchemaTableDialog::AddIndex);
        connect(editIndexButton_, &QPushButton::clicked, this,
                &SCSchemaTableDialog::EditIndex);
        connect(removeIndexButton_, &QPushButton::clicked, this,
                &SCSchemaTableDialog::RemoveIndex);
        connect(indexList_, &QListWidget::itemSelectionChanged, this,
                [this]() {
                    bool hasSelection = indexList_->selectedItems().size() > 0;
                    editIndexButton_->setEnabled(hasSelection);
                    removeIndexButton_->setEnabled(hasSelection);
                });

        QString error;
        if (!ReloadSchema(&error))
        {
            tableNameLabel_->setText(QStringLiteral("-"));
            outputEdit_->setPlainText(QStringLiteral("// ") + error);
            statusLabel_->setText(
                QStringLiteral("加载当前表失败: ") + error);
            copyButton->setEnabled(false);
            refreshButton->setEnabled(false);
            includeLegacyIndexesCheck_->setEnabled(false);
            return;
        }

        UpdateOutput();
    }

    bool SCSchemaTableDialog::ReloadSchema(QString* outError)
    {
        tableName_.clear();
        schemaSnapshot_.tables.clear();

        if (session_ == nullptr || !session_->IsOpen())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("没有打开的数据库。");
            }
            return false;
        }

        tableName_ = session_->CurrentTableName();
        if (tableName_.isEmpty())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("请先选择一个表。");
            }
            return false;
        }

        if (!session_->BuildSchemaSnapshot(&schemaSnapshot_, outError))
        {
            return false;
        }
        if (schemaSnapshot_.tables.empty())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("没有可用的模式快照。");
            }
            return false;
        }

        tableNameLabel_->setText(tableName_);

        const auto& tableSnapshot = schemaSnapshot_.tables.front();
        const int legacyHintCount = static_cast<int>(std::count_if(
            tableSnapshot.indexes.begin(), tableSnapshot.indexes.end(),
            [](const sc::SCIndexDef& index) {
                return index.sourceKind == sc::SCSchemaSourceKind::LegacyHint;
            }));
        legacyHintLabel_->setText(
            legacyHintCount > 0
                ? QStringLiteral("检测到 %1 个旧式索引列。")
                      .arg(legacyHintCount)
                : QStringLiteral("未检测到旧式索引列。"));

        UpdateIndexList();
        return true;
    }

    void SCSchemaTableDialog::UpdateOutput()
    {
        if (tableName_.isEmpty() || schemaSnapshot_.tables.empty())
        {
            outputEdit_->setPlainText(QStringLiteral("// 未选择表。"));
            statusLabel_->setText(
                QStringLiteral("没有当前可用的表。"));
            return;
        }

        const sc::SCTableSchemaSnapshot& tableSnapshot =
            schemaSnapshot_.tables.front();
        SCSchemaTableExportOptions options;
        options.tableDescription = tableDescriptionEdit_->text().trimmed();
        options.primaryKeyColumnName = primaryKeyEdit_->text().trimmed();
        options.includeLegacyIndexes = includeLegacyIndexesCheck_ != nullptr &&
                                       includeLegacyIndexesCheck_->isChecked();

        const QString code = BuildSchemaTableCode(tableSnapshot, options);
        outputEdit_->setPlainText(code);

        int exportedIndexes = 0;
        for (const sc::SCIndexDef& index : tableSnapshot.indexes)
        {
            if (index.sourceKind == sc::SCSchemaSourceKind::LegacyHint &&
                !options.includeLegacyIndexes)
            {
                continue;
            }
            ++exportedIndexes;
        }

        QString status = QStringLiteral("已生成 %1 列和 %2 个索引。")
                             .arg(tableSnapshot.columns.size())
                             .arg(exportedIndexes);
        if (options.primaryKeyColumnName.trimmed().isEmpty())
        {
            status += QStringLiteral(" 未选择显式主键。");
        }
        if (!options.includeLegacyIndexes &&
            std::any_of(tableSnapshot.indexes.begin(),
                        tableSnapshot.indexes.end(),
                        [](const sc::SCIndexDef& index) {
                            return index.sourceKind ==
                                   sc::SCSchemaSourceKind::LegacyHint;
                        }))
        {
            status += QStringLiteral(" 旧式提示未被导出。");
        }
        if (std::any_of(tableSnapshot.constraints.begin(),
                        tableSnapshot.constraints.end(),
                        [](const sc::SCConstraintDef& constraint) {
                            return constraint.kind !=
                                   sc::SCConstraintKind::PrimaryKey;
                        }))
        {
            status += QStringLiteral(
                " 快照包含此 DSL 中未导出的约束。");
        }

        statusLabel_->setText(status);
    }

    void SCSchemaTableDialog::RefreshPreview()
    {
        QString error;
        if (!ReloadSchema(&error))
        {
            outputEdit_->setPlainText(QStringLiteral("// ") + error);
            statusLabel_->setText(
                QStringLiteral("加载当前表失败: ") + error);
            return;
        }

        UpdateOutput();
    }

    void SCSchemaTableDialog::CopyOutput()
    {
        if (QApplication::clipboard() != nullptr)
        {
            QApplication::clipboard()->setText(outputEdit_->toPlainText());
            statusLabel_->setText(
                QStringLiteral("代码已复制到剪贴板。"));
        }
    }

    void SCSchemaTableDialog::AddIndex()
    {
        sc::SCIndexDef newIndex;
        auto availableColumns = GetAvailableColumnNames();
        
        SCIndexEditorDialog dialog(newIndex, availableColumns, this);
        if (dialog.exec() == QDialog::Accepted)
        {
            QString error;
            sc::SCIndexDef index = dialog.GetIndex();
            if (!session_->AddIndex(index, &error))
            {
                statusLabel_->setText(
                    QStringLiteral("添加索引失败: ") + error);
                return;
            }

            RefreshPreview();
            statusLabel_->setText(
                QStringLiteral("索引添加成功。"));
        }
    }

    void SCSchemaTableDialog::EditIndex()
    {
        QList<QListWidgetItem*> selected = indexList_->selectedItems();
        if (selected.isEmpty())
        {
            return;
        }

        QString indexName = selected.first()->data(Qt::UserRole).toString();
        if (indexName.isEmpty())
        {
            return;
        }

        const auto& tableSnapshot = schemaSnapshot_.tables.front();
        auto it = std::find_if(tableSnapshot.indexes.begin(),
                               tableSnapshot.indexes.end(),
                               [&indexName](const sc::SCIndexDef& idx) {
                                   return QString::fromStdWString(idx.name)
                                              .compare(indexName,
                                                       Qt::CaseInsensitive) == 0;
                               });

        if (it == tableSnapshot.indexes.end())
        {
            statusLabel_->setText(
                QStringLiteral("未找到索引: ") + indexName);
            return;
        }

        sc::SCIndexDef index = *it;
        auto availableColumns = GetAvailableColumnNames();

        SCIndexEditorDialog dialog(index, availableColumns, this);
        if (dialog.exec() == QDialog::Accepted)
        {
            QString error;
            sc::SCIndexDef updatedIndex = dialog.GetIndex();
            if (!session_->UpdateIndex(indexName, updatedIndex, &error))
            {
                statusLabel_->setText(
                    QStringLiteral("更新索引失败: ") + error);
                return;
            }

            RefreshPreview();
            statusLabel_->setText(
                QStringLiteral("索引更新成功。"));
        }
    }

    void SCSchemaTableDialog::RemoveIndex()
    {
        QList<QListWidgetItem*> selected = indexList_->selectedItems();
        if (selected.isEmpty())
        {
            return;
        }

        QString indexName = selected.first()->data(Qt::UserRole).toString();
        if (indexName.isEmpty())
        {
            return;
        }

        QString error;
        if (!session_->RemoveIndex(indexName, &error))
        {
            statusLabel_->setText(
                QStringLiteral("删除索引失败: ") + error);
            return;
        }

        RefreshPreview();
        statusLabel_->setText(
            QStringLiteral("索引删除成功。"));
    }

    void SCSchemaTableDialog::UpdateIndexList()
    {
        indexList_->clear();

        if (schemaSnapshot_.tables.empty())
        {
            return;
        }

        const auto& tableSnapshot = schemaSnapshot_.tables.front();
        for (const sc::SCIndexDef& index : tableSnapshot.indexes)
        {
            QString displayName = QString::fromStdWString(index.name);
            QString columns;
            for (size_t i = 0; i < index.columns.size(); ++i)
            {
                if (i > 0)
                {
                    columns += QStringLiteral(", ");
                }
                columns += QString::fromStdWString(index.columns[i].columnName);
                if (index.columns[i].descending)
                {
                    columns += QStringLiteral(" (降序)");
                }
            }

            QString itemText = QStringLiteral("%1 - %2")
                                   .arg(displayName)
                                   .arg(columns);
            auto* item = new QListWidgetItem(itemText);
            item->setData(Qt::UserRole, displayName);
            indexList_->addItem(item);
        }

        editIndexButton_->setEnabled(false);
        removeIndexButton_->setEnabled(false);
    }

    std::vector<std::wstring> SCSchemaTableDialog::GetAvailableColumnNames() const
    {
        std::vector<std::wstring> names;
        if (schemaSnapshot_.tables.empty())
        {
            return names;
        }

        const auto& tableSnapshot = schemaSnapshot_.tables.front();
        for (const sc::SCColumnDef& column : tableSnapshot.columns)
        {
            names.push_back(column.name);
        }
        return names;
    }

}  // namespace StableCore::Storage::Editor
