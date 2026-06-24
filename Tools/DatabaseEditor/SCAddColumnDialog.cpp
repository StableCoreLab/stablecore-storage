#include "SCAddColumnDialog.h"

#include "SCBinaryUtils.h"
#include "SCDatabaseSession.h"

#include <cstdint>
#include <algorithm>
#include <vector>

#include <QDialogButtonBox>
#include <QVBoxLayout>

namespace sc = StableCore::Storage;

namespace StableCore::Storage::Editor
{
    namespace
    {
        enum class RelationBindingMode
        {
            RecordId = 0,
            BusinessKey = 1,
        };

        bool IsRelationStorageColumnUsable(const sc::SCTableSchemaSnapshot& snapshot,
                                           const QString& columnName)
        {
            for (const sc::SCColumnDef& column : snapshot.columns)
            {
                if (QString::fromStdWString(column.name).compare(columnName,
                                                                 Qt::CaseInsensitive) != 0)
                {
                    continue;
                }

                if (column.nullable)
                {
                    return false;
                }

                break;
            }

            for (const sc::SCConstraintDef& constraint : snapshot.constraints)
            {
                if (constraint.columns.size() != 1)
                {
                    continue;
                }
                if (constraint.kind != sc::SCConstraintKind::PrimaryKey &&
                    constraint.kind != sc::SCConstraintKind::Unique)
                {
                    continue;
                }

                if (QString::fromStdWString(constraint.columns.front()).compare(
                        columnName, Qt::CaseInsensitive) == 0)
                {
                    return true;
                }
            }

            return false;
        }

        bool ParseDefaultValue(sc::ValueKind kind, const QString& text,
                               sc::SCValue* outValue, QString* outError)
        {
            if (outValue == nullptr)
            {
                if (outError != nullptr)
                {
                    *outError = QStringLiteral("Output value is null.");
                }
                return false;
            }

            if (text.trimmed().isEmpty())
            {
                *outValue = sc::SCValue::Null();
                return true;
            }

            switch (kind)
            {
                case sc::ValueKind::Int64:
                    *outValue = sc::SCValue::FromInt64(text.toLongLong());
                    return true;
                case sc::ValueKind::Double:
                    *outValue = sc::SCValue::FromDouble(text.toDouble());
                    return true;
                case sc::ValueKind::Bool:
                    *outValue = sc::SCValue::FromBool(
                        text.compare(QStringLiteral("true"),
                                     Qt::CaseInsensitive) == 0 ||
                        text == QStringLiteral("1"));
                    return true;
                case sc::ValueKind::String:
                    *outValue = sc::SCValue::FromString(text.toStdWString());
                    return true;
                case sc::ValueKind::RecordId:
                    *outValue = sc::SCValue::FromRecordId(text.toLongLong());
                    return true;
                case sc::ValueKind::Enum:
                    *outValue = sc::SCValue::FromEnum(text.toStdWString());
                    return true;
                case sc::ValueKind::Binary: {
                    std::vector<std::uint8_t> bytes;
                    if (!ParseBinaryHex(text, &bytes, outError))
                    {
                        return false;
                    }
                    *outValue = sc::SCValue::FromBinary(std::move(bytes));
                    return true;
                }
                case sc::ValueKind::Null:
                default:
                    *outValue = sc::SCValue::Null();
                    return true;
            }
        }
    }  // namespace

    SCAddColumnDialog::SCAddColumnDialog(SCDatabaseSession* session, QWidget* parent)
        : QDialog(parent), session_(session)
    {
        setWindowTitle(QStringLiteral("新增字段"));

        auto* layout = new QVBoxLayout(this);
        auto* form = new QFormLayout();

        nameEdit_ = new QLineEdit(this);
        displayNameEdit_ = new QLineEdit(this);
        valueKindCombo_ = new QComboBox(this);
        relationCheck_ = new QCheckBox(QStringLiteral("关系字段"), this);
        relationBindingCombo_ = new QComboBox(this);
        nullableCheck_ = new QCheckBox(this);
        editableCheck_ = new QCheckBox(this);
        userDefinedCheck_ = new QCheckBox(this);
        indexedCheck_ = new QCheckBox(this);
        participatesInCalcCheck_ = new QCheckBox(this);
        unitEdit_ = new QLineEdit(this);
        referenceTableEdit_ = new QLineEdit(this);
        referenceTableLabel_ = new QLabel(QStringLiteral("引用表"), this);
        referenceStorageColumnEdit_ = new QLineEdit(this);
        referenceStorageColumnLabel_ = new QLabel(QStringLiteral("存储列"), this);
        referenceDisplayColumnEdit_ = new QLineEdit(this);
        referenceDisplayColumnLabel_ = new QLabel(QStringLiteral("显示列"), this);
        defaultValueEdit_ = new QLineEdit(this);
        defaultValueEdit_->setPlaceholderText(
            QStringLiteral("仅在表中已有数据时需要填写"));

        referenceTableEdit_->setPlaceholderText(QStringLiteral("请输入已存在的表名"));
        referenceStorageColumnEdit_->setPlaceholderText(
            QStringLiteral("用于保存关联值的业务列"));
        referenceDisplayColumnEdit_->setPlaceholderText(
            QStringLiteral("可选，不填则默认与存储列一致"));

        referenceTableModel_ = new QStringListModel(this);
        referenceStorageModel_ = new QStringListModel(this);
        referenceDisplayModel_ = new QStringListModel(this);
        referenceTableCompleter_ = new QCompleter(referenceTableModel_, this);
        referenceStorageCompleter_ = new QCompleter(referenceStorageModel_, this);
        referenceDisplayCompleter_ = new QCompleter(referenceDisplayModel_, this);
        referenceTableCompleter_->setCaseSensitivity(Qt::CaseInsensitive);
        referenceStorageCompleter_->setCaseSensitivity(Qt::CaseInsensitive);
        referenceDisplayCompleter_->setCaseSensitivity(Qt::CaseInsensitive);
        referenceTableEdit_->setCompleter(referenceTableCompleter_);
        referenceStorageColumnEdit_->setCompleter(referenceStorageCompleter_);
        referenceDisplayColumnEdit_->setCompleter(referenceDisplayCompleter_);
        relationBindingCombo_->addItem(
            QStringLiteral("按记录ID关联"),
            static_cast<int>(RelationBindingMode::RecordId));
        relationBindingCombo_->addItem(
            QStringLiteral("按业务键关联"),
            static_cast<int>(RelationBindingMode::BusinessKey));
        relationBindingCombo_->setCurrentIndex(
            relationBindingCombo_->findData(
                static_cast<int>(RelationBindingMode::BusinessKey)));

        valueKindCombo_->addItem(QStringLiteral("Integer (Int64)"),
                                 static_cast<int>(sc::ValueKind::Int64));
        valueKindCombo_->addItem(QStringLiteral("Decimal (Double)"),
                                 static_cast<int>(sc::ValueKind::Double));
        valueKindCombo_->addItem(QStringLiteral("Boolean (Bool)"),
                                 static_cast<int>(sc::ValueKind::Bool));
        valueKindCombo_->addItem(QStringLiteral("String"),
                                 static_cast<int>(sc::ValueKind::String));
        valueKindCombo_->addItem(QStringLiteral("RecordId"),
                                 static_cast<int>(sc::ValueKind::RecordId));
        valueKindCombo_->addItem(QStringLiteral("Enum"),
                                 static_cast<int>(sc::ValueKind::Enum));
        valueKindCombo_->addItem(QStringLiteral("Binary (Blob)"),
                                 static_cast<int>(sc::ValueKind::Binary));

        nullableCheck_->setChecked(true);
        editableCheck_->setChecked(true);

        form->addRow(QStringLiteral("字段名"), nameEdit_);
        form->addRow(QStringLiteral("显示名"), displayNameEdit_);
        form->addRow(QStringLiteral("值类型"), valueKindCombo_);
        form->addRow(QStringLiteral("字段类型"), relationCheck_);
        form->addRow(QStringLiteral("关联方式"), relationBindingCombo_);
        form->addRow(QStringLiteral("可空"), nullableCheck_);
        form->addRow(QStringLiteral("可编辑"), editableCheck_);
        form->addRow(QStringLiteral("用户定义"), userDefinedCheck_);
        form->addRow(QStringLiteral("已建索引"), indexedCheck_);
        form->addRow(QStringLiteral("参与计算"), participatesInCalcCheck_);
        form->addRow(QStringLiteral("单位"), unitEdit_);
        form->addRow(referenceTableLabel_, referenceTableEdit_);
        form->addRow(referenceStorageColumnLabel_, referenceStorageColumnEdit_);
        form->addRow(referenceDisplayColumnLabel_, referenceDisplayColumnEdit_);
        form->addRow(QStringLiteral("默认值"), defaultValueEdit_);
        layout->addLayout(form);

        relationHintLabel_ = new QLabel(this);
        relationHintLabel_->setWordWrap(true);
        relationHintLabel_->setStyleSheet(QStringLiteral("color: #6a6a6a;"));
        layout->addWidget(relationHintLabel_);

        validationLabel_ = new QLabel(this);
        validationLabel_->setWordWrap(true);
        layout->addWidget(validationLabel_);

        buttonBox_ = new QDialogButtonBox(
            QDialogButtonBox::Ok | QDialogButtonBox::Cancel, this);
        okButton_ = buttonBox_->button(QDialogButtonBox::Ok);
        connect(buttonBox_, &QDialogButtonBox::accepted, this, &QDialog::accept);
        connect(buttonBox_, &QDialogButtonBox::rejected, this, &QDialog::reject);
        layout->addWidget(buttonBox_);

        connect(nullableCheck_, &QCheckBox::toggled, this,
                &SCAddColumnDialog::UpdateValidationState);
        connect(relationCheck_, &QCheckBox::toggled, this,
                &SCAddColumnDialog::UpdateValidationState);
        connect(relationBindingCombo_, qOverload<int>(&QComboBox::currentIndexChanged), this,
                &SCAddColumnDialog::UpdateValidationState);
        connect(referenceTableEdit_, &QLineEdit::textChanged, this,
                &SCAddColumnDialog::UpdateValidationState);
        connect(referenceStorageColumnEdit_, &QLineEdit::textChanged, this,
                &SCAddColumnDialog::UpdateValidationState);
        connect(referenceDisplayColumnEdit_, &QLineEdit::textChanged, this,
                &SCAddColumnDialog::UpdateValidationState);
        connect(valueKindCombo_, qOverload<int>(&QComboBox::currentIndexChanged), this,
                &SCAddColumnDialog::UpdateValidationState);
        connect(defaultValueEdit_, &QLineEdit::textChanged, this,
                &SCAddColumnDialog::UpdateValidationState);

        UpdateRelationHints();
        UpdateValidationState();
    }

    SCAddColumnDialog::SCAddColumnDialog(SCDatabaseSession* session,
                                         const sc::SCColumnDef& initialValue,
                                         QWidget* parent)
        : SCAddColumnDialog(session, parent)
    {
        setWindowTitle(QStringLiteral("编辑字段"));
        nameEdit_->setReadOnly(true);
        ApplyInitialValue(initialValue);
    }

    void SCAddColumnDialog::ApplyInitialValue(const sc::SCColumnDef& value)
    {
        nameEdit_->setText(QString::fromStdWString(value.name));
        displayNameEdit_->setText(QString::fromStdWString(value.displayName));
        const int valueKindIndex =
            valueKindCombo_->findData(static_cast<int>(value.valueKind));
        if (valueKindIndex >= 0)
        {
            valueKindCombo_->setCurrentIndex(valueKindIndex);
        }
        relationCheck_->setChecked(value.columnKind == sc::ColumnKind::Relation);
        nullableCheck_->setChecked(value.nullable);
        editableCheck_->setChecked(value.editable);
        userDefinedCheck_->setChecked(value.userDefined);
        indexedCheck_->setChecked(value.indexed);
        participatesInCalcCheck_->setChecked(value.participatesInCalc);
        unitEdit_->setText(QString::fromStdWString(value.unit));
        referenceTableEdit_->setText(QString::fromStdWString(value.referenceTable));
        const bool recordIdBinding = value.referenceStorageColumn.empty();
        relationBindingCombo_->setCurrentIndex(
            relationBindingCombo_->findData(static_cast<int>(
                recordIdBinding ? RelationBindingMode::RecordId
                                : RelationBindingMode::BusinessKey)));
        referenceStorageColumnEdit_->setText(
            QString::fromStdWString(value.referenceStorageColumn));
        referenceDisplayColumnEdit_->setText(
            QString::fromStdWString(value.referenceDisplayColumn));

        if (value.defaultValue.IsNull())
        {
            defaultValueEdit_->clear();
        } else
        {
            switch (value.defaultValue.GetKind())
            {
                case sc::ValueKind::Int64: {
                    std::int64_t result = 0;
                    value.defaultValue.AsInt64(&result);
                    defaultValueEdit_->setText(QString::number(result));
                    break;
                }
                case sc::ValueKind::Double: {
                    double result = 0.0;
                    value.defaultValue.AsDouble(&result);
                    defaultValueEdit_->setText(QString::number(result, 'g', 12));
                    break;
                }
                case sc::ValueKind::Bool: {
                    bool result = false;
                    value.defaultValue.AsBool(&result);
                    defaultValueEdit_->setText(result ? QStringLiteral("true")
                                                      : QStringLiteral("false"));
                    break;
                }
                case sc::ValueKind::String: {
                    std::wstring result;
                    value.defaultValue.AsStringCopy(&result);
                    defaultValueEdit_->setText(QString::fromStdWString(result));
                    break;
                }
                case sc::ValueKind::RecordId: {
                    sc::RecordId result = 0;
                    value.defaultValue.AsRecordId(&result);
                    defaultValueEdit_->setText(QString::number(result));
                    break;
                }
                case sc::ValueKind::Enum: {
                    std::wstring result;
                    value.defaultValue.AsEnumCopy(&result);
                    defaultValueEdit_->setText(QString::fromStdWString(result));
                    break;
                }
                case sc::ValueKind::Binary: {
                    std::vector<std::uint8_t> result;
                    value.defaultValue.AsBinaryCopy(&result);
                    defaultValueEdit_->setText(BinaryToHex(result));
                    break;
                }
                case sc::ValueKind::Null:
                default:
                    defaultValueEdit_->clear();
                    break;
            }
        }

        UpdateRelationHints();
        UpdateValidationState();
    }

    sc::SCColumnDef SCAddColumnDialog::BuildColumnDef() const
    {
        sc::SCColumnDef column;
        column.name = nameEdit_->text().toStdWString();
        column.displayName = displayNameEdit_->text().isEmpty()
                                 ? column.name
                                 : displayNameEdit_->text().toStdWString();
        column.valueKind =
            static_cast<sc::ValueKind>(valueKindCombo_->currentData().toInt());
        const RelationBindingMode bindingMode =
            static_cast<RelationBindingMode>(
                relationBindingCombo_->currentData().toInt());
        if (relationCheck_->isChecked() &&
            bindingMode == RelationBindingMode::RecordId)
        {
            column.valueKind = sc::ValueKind::RecordId;
        }
        column.columnKind = relationCheck_->isChecked()
                                ? sc::ColumnKind::Relation
                                : sc::ColumnKind::Fact;
        column.nullable = nullableCheck_->isChecked();
        column.editable = editableCheck_->isChecked();
        column.userDefined = userDefinedCheck_->isChecked();
        column.indexed = indexedCheck_->isChecked();
        column.participatesInCalc = participatesInCalcCheck_->isChecked();
        column.unit = unitEdit_->text().toStdWString();
        sc::SCValue defaultValue;
        if (ParseDefaultValue(column.valueKind, defaultValueEdit_->text(),
                              &defaultValue, nullptr))
        {
            column.defaultValue = std::move(defaultValue);
        }
        if (column.columnKind == sc::ColumnKind::Relation)
        {
            column.referenceTable = referenceTableEdit_->text().toStdWString();
            if (bindingMode == RelationBindingMode::RecordId)
            {
                column.referenceStorageColumn.clear();
                column.referenceDisplayColumn.clear();
            } else
            {
                column.referenceStorageColumn =
                    referenceStorageColumnEdit_->text().toStdWString();
                column.referenceDisplayColumn =
                    referenceDisplayColumnEdit_->text().toStdWString();
            }
        } else
        {
            column.referenceTable.clear();
            column.referenceStorageColumn.clear();
            column.referenceDisplayColumn.clear();
        }
        return column;
    }

    void SCAddColumnDialog::SetCurrentTableHasRecords(bool hasRecords)
    {
        currentTableHasRecords_ = hasRecords;
        UpdateValidationState();
    }

    bool SCAddColumnDialog::LoadReferenceTableSnapshot(
        sc::SCTableSchemaSnapshot* outSnapshot, QString* outError) const
    {
        if (outSnapshot == nullptr)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Output snapshot is null.");
            }
            return false;
        }

        outSnapshot->columns.clear();
        outSnapshot->constraints.clear();
        outSnapshot->indexes.clear();
        outSnapshot->table = sc::SCTableDef{};

        if (session_ == nullptr)
        {
            return true;
        }

        const QString tableName = referenceTableEdit_->text().trimmed();
        if (tableName.isEmpty())
        {
            return true;
        }

        return session_->GetTableSchemaSnapshot(tableName, outSnapshot, outError);
    }

    void SCAddColumnDialog::UpdateRelationHints()
    {
        if (referenceTableModel_ != nullptr && session_ != nullptr)
        {
            referenceTableModel_->setStringList(session_->TableNames());
        }

        sc::SCTableSchemaSnapshot snapshot;
        QString error;
        const bool loadedSnapshot = LoadReferenceTableSnapshot(&snapshot, &error);

        QStringList storageColumns;
        QStringList displayColumns;
        if (loadedSnapshot)
        {
            for (const sc::SCColumnDef& column : snapshot.columns)
            {
                const QString columnName = QString::fromStdWString(column.name);
                displayColumns.push_back(columnName);
                if (IsRelationStorageColumnUsable(snapshot, columnName))
                {
                    storageColumns.push_back(columnName);
                }
            }
        }

        if (referenceStorageModel_ != nullptr)
        {
            referenceStorageModel_->setStringList(storageColumns);
        }
        if (referenceDisplayModel_ != nullptr)
        {
            referenceDisplayModel_->setStringList(displayColumns);
        }

        if (relationHintLabel_ == nullptr)
        {
            return;
        }

        if (!relationCheck_->isChecked())
        {
            relationHintLabel_->clear();
            return;
        }

        const QString tableName = referenceTableEdit_->text().trimmed();
        const QString storageColumn = referenceStorageColumnEdit_->text().trimmed();
        const QString displayColumn = referenceDisplayColumnEdit_->text().trimmed();
        const RelationBindingMode bindingMode =
            static_cast<RelationBindingMode>(
                relationBindingCombo_->currentData().toInt());

        if (tableName.isEmpty())
        {
            relationHintLabel_->setText(QStringLiteral("请输入引用表名称。"));
            return;
        }
        if (!loadedSnapshot)
        {
            relationHintLabel_->setText(QStringLiteral("引用表不存在或无法读取。"));
            return;
        }
        if (bindingMode == RelationBindingMode::RecordId)
        {
            relationHintLabel_->setText(
                QStringLiteral("当前模式：按记录ID关联。系统直接保存目标记录的记录ID，") +
                QStringLiteral("无需填写存储列或显示列。"));
            return;
        }
        if (storageColumn.isEmpty())
        {
            relationHintLabel_->setText(QStringLiteral("关系字段必须指定存储列。"));
            return;
        }

        const bool storageColumnExists =
            std::any_of(snapshot.columns.begin(), snapshot.columns.end(),
                        [&storageColumn](const sc::SCColumnDef& column) {
                            return QString::fromStdWString(column.name).compare(
                                       storageColumn, Qt::CaseInsensitive) == 0;
                        });
        if (!storageColumnExists)
        {
            relationHintLabel_->setText(QStringLiteral("存储列必须存在于引用表中。"));
            return;
        }

        if (!IsRelationStorageColumnUsable(snapshot, storageColumn))
        {
            relationHintLabel_->setText(QStringLiteral("存储列必须非空且唯一。"));
            return;
        }

        if (!displayColumn.isEmpty())
        {
            const bool displayColumnExists =
                std::any_of(snapshot.columns.begin(), snapshot.columns.end(),
                            [&displayColumn](const sc::SCColumnDef& column) {
                                return QString::fromStdWString(column.name).compare(
                                           displayColumn, Qt::CaseInsensitive) == 0;
                            });
            if (!displayColumnExists)
            {
                relationHintLabel_->setText(QStringLiteral("显示列必须存在于引用表中。"));
                return;
            }
        }

        relationHintLabel_->setText(QStringLiteral("存储列决定实际写入内容，显示列仅用于界面展示。"));
    }

    void SCAddColumnDialog::UpdateValidationState()
    {
        const bool hasDefaultValue =
            !defaultValueEdit_->text().trimmed().isEmpty();
        const sc::ValueKind selectedKind = static_cast<sc::ValueKind>(
            valueKindCombo_->currentData().toInt());
        bool defaultValid = true;
        QString defaultError;
        if (hasDefaultValue)
        {
            sc::SCValue parsedDefault;
            defaultValid = ParseDefaultValue(selectedKind,
                                             defaultValueEdit_->text(),
                                             &parsedDefault, &defaultError);
        }
        const bool nonNullableWithoutDefault =
            !nullableCheck_->isChecked() && !hasDefaultValue;
        const bool requiresDefault =
            nonNullableWithoutDefault && currentTableHasRecords_;

        UpdateRelationHints();

        bool relationValid = true;
        QString relationError;
        if (relationCheck_->isChecked())
        {
            sc::SCTableSchemaSnapshot snapshot;
            QString error;
            const bool loadedSnapshot = LoadReferenceTableSnapshot(&snapshot, &error);
            const QString tableName = referenceTableEdit_->text().trimmed();
            const QString storageColumn = referenceStorageColumnEdit_->text().trimmed();
            const QString displayColumn = referenceDisplayColumnEdit_->text().trimmed();
            const RelationBindingMode bindingMode =
                static_cast<RelationBindingMode>(
                    relationBindingCombo_->currentData().toInt());

            if (tableName.isEmpty())
            {
                relationValid = false;
                relationError = QStringLiteral("关系字段必须指定引用表。");
            }
            else if (!loadedSnapshot)
            {
                relationValid = false;
                relationError = QStringLiteral("引用表不存在或无法读取。");
            }
            else if (bindingMode == RelationBindingMode::RecordId)
            {
                if (selectedKind != sc::ValueKind::RecordId)
                {
                    relationValid = false;
                    relationError = QStringLiteral("按记录ID关联时，值类型必须为 RecordId。");
                }
            }
            else if (storageColumn.isEmpty())
            {
                relationValid = false;
                relationError = QStringLiteral("关系字段必须指定存储列。");
            }
            else if (std::none_of(
                           snapshot.columns.begin(), snapshot.columns.end(),
                           [&storageColumn](const sc::SCColumnDef& column) {
                               return QString::fromStdWString(column.name).compare(
                                          storageColumn, Qt::CaseInsensitive) == 0;
                           }))
            {
                relationValid = false;
                relationError = QStringLiteral("存储列必须存在于引用表中。");
            }
            else if (!IsRelationStorageColumnUsable(snapshot, storageColumn))
            {
                relationValid = false;
                relationError = QStringLiteral("存储列必须非空且唯一。");
            }
            else if (!displayColumn.isEmpty() &&
                       std::none_of(snapshot.columns.begin(), snapshot.columns.end(),
                                    [&displayColumn](const sc::SCColumnDef& column) {
                                        return QString::fromStdWString(column.name).compare(
                                                   displayColumn, Qt::CaseInsensitive) == 0;
                                    }))
            {
                relationValid = false;
                relationError = QStringLiteral("显示列必须存在于引用表中。");
            }
        }

        if (validationLabel_ != nullptr)
        {
            if (!relationValid)
            {
                validationLabel_->setStyleSheet(QStringLiteral("color: #b00020;"));
                validationLabel_->setText(relationError);
                validationLabel_->setVisible(true);
            } else if (!defaultValid)
            {
                validationLabel_->setStyleSheet(QStringLiteral("color: #b00020;"));
                validationLabel_->setText(defaultError);
                validationLabel_->setVisible(true);
            } else if (nonNullableWithoutDefault)
            {
                if (currentTableHasRecords_)
                {
                    validationLabel_->setStyleSheet(QStringLiteral("color: #b00020;"));
                    validationLabel_->setText(
                        QStringLiteral("The current table already has data, so a non-null column needs a default value."));
                } else
                {
                    validationLabel_->setStyleSheet(QStringLiteral("color: #0b5fff;"));
                    validationLabel_->setText(
                        QStringLiteral("The current table is empty, so you can create a non-null column first and fill values later."));
                }
                validationLabel_->setVisible(true);
            } else
            {
                validationLabel_->clear();
                validationLabel_->setVisible(false);
            }
        }

        const bool relationVisible = relationCheck_->isChecked();
        const bool businessKeyBinding =
            relationVisible &&
            static_cast<RelationBindingMode>(
                relationBindingCombo_->currentData().toInt()) ==
                RelationBindingMode::BusinessKey;
        if (referenceTableLabel_ != nullptr)
        {
            referenceTableLabel_->setVisible(relationVisible);
        }
        if (referenceTableEdit_ != nullptr)
        {
            referenceTableEdit_->setVisible(relationVisible);
        }
        if (relationBindingCombo_ != nullptr)
        {
            relationBindingCombo_->setVisible(relationVisible);
        }
        if (referenceStorageColumnLabel_ != nullptr)
        {
            referenceStorageColumnLabel_->setVisible(businessKeyBinding);
        }
        if (referenceStorageColumnEdit_ != nullptr)
        {
            referenceStorageColumnEdit_->setVisible(businessKeyBinding);
        }
        if (referenceDisplayColumnLabel_ != nullptr)
        {
            referenceDisplayColumnLabel_->setVisible(businessKeyBinding);
        }
        if (referenceDisplayColumnEdit_ != nullptr)
        {
            referenceDisplayColumnEdit_->setVisible(businessKeyBinding);
        }

        if (okButton_ != nullptr)
        {
            okButton_->setEnabled(relationValid && defaultValid && !requiresDefault);
        }
    }

}  // namespace StableCore::Storage::Editor
