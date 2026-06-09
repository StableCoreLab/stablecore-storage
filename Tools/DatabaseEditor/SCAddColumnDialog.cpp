#include "SCAddColumnDialog.h"

#include <QDialogButtonBox>
#include <QVBoxLayout>

namespace sc = StableCore::Storage;

namespace StableCore::Storage::Editor
{
    namespace
    {

        sc::SCValue ParseDefaultValue(sc::ValueKind kind, const QString& text)
        {
            if (text.trimmed().isEmpty())
            {
                return sc::SCValue::Null();
            }

            switch (kind)
            {
                case sc::ValueKind::Int64:
                    return sc::SCValue::FromInt64(text.toLongLong());
                case sc::ValueKind::Double:
                    return sc::SCValue::FromDouble(text.toDouble());
                case sc::ValueKind::Bool:
                    return sc::SCValue::FromBool(
                        text.compare(QStringLiteral("true"),
                                     Qt::CaseInsensitive) == 0 ||
                        text == QStringLiteral("1"));
                case sc::ValueKind::String:
                    return sc::SCValue::FromString(text.toStdWString());
                case sc::ValueKind::RecordId:
                    return sc::SCValue::FromRecordId(text.toLongLong());
                case sc::ValueKind::Enum:
                    return sc::SCValue::FromEnum(text.toStdWString());
                case sc::ValueKind::Null:
                default:
                    return sc::SCValue::Null();
            }
        }

    }  // namespace

    SCAddColumnDialog::SCAddColumnDialog(QWidget* parent) : QDialog(parent)
    {
        setWindowTitle(QStringLiteral("Add Column"));

        auto* layout = new QVBoxLayout(this);
        auto* form = new QFormLayout();

        nameEdit_ = new QLineEdit(this);
        displayNameEdit_ = new QLineEdit(this);
        valueKindCombo_ = new QComboBox(this);
        relationCheck_ = new QCheckBox(QStringLiteral("Relation Field"), this);
        nullableCheck_ = new QCheckBox(this);
        editableCheck_ = new QCheckBox(this);
        userDefinedCheck_ = new QCheckBox(this);
        indexedCheck_ = new QCheckBox(this);
        participatesInCalcCheck_ = new QCheckBox(this);
        unitEdit_ = new QLineEdit(this);
        referenceTableEdit_ = new QLineEdit(this);
        defaultValueEdit_ = new QLineEdit(this);
        defaultValueEdit_->setPlaceholderText(
            QStringLiteral("Required only when the table already has rows"));

        valueKindCombo_->addItem(QStringLiteral("Int64"),
                                 static_cast<int>(sc::ValueKind::Int64));
        valueKindCombo_->addItem(QStringLiteral("Double"),
                                 static_cast<int>(sc::ValueKind::Double));
        valueKindCombo_->addItem(QStringLiteral("Bool"),
                                 static_cast<int>(sc::ValueKind::Bool));
        valueKindCombo_->addItem(QStringLiteral("String"),
                                 static_cast<int>(sc::ValueKind::String));
        valueKindCombo_->addItem(QStringLiteral("RecordId"),
                                 static_cast<int>(sc::ValueKind::RecordId));
        valueKindCombo_->addItem(QStringLiteral("Enum"),
                                 static_cast<int>(sc::ValueKind::Enum));

        nullableCheck_->setChecked(true);
        editableCheck_->setChecked(true);

        form->addRow(QStringLiteral("Name"), nameEdit_);
        form->addRow(QStringLiteral("Display Name"), displayNameEdit_);
        form->addRow(QStringLiteral("SCValue Kind"), valueKindCombo_);
        form->addRow(QStringLiteral("Column Kind"), relationCheck_);
        form->addRow(QStringLiteral("Nullable"), nullableCheck_);
        form->addRow(QStringLiteral("Editable"), editableCheck_);
        form->addRow(QStringLiteral("User Defined"), userDefinedCheck_);
        form->addRow(QStringLiteral("Indexed"), indexedCheck_);
        form->addRow(QStringLiteral("Participates In Calc"),
                     participatesInCalcCheck_);
        form->addRow(QStringLiteral("Unit"), unitEdit_);
        form->addRow(QStringLiteral("Reference Table"), referenceTableEdit_);
        form->addRow(QStringLiteral("Default SCValue"), defaultValueEdit_);
        layout->addLayout(form);

        validationLabel_ = new QLabel(this);
        validationLabel_->setWordWrap(true);
        layout->addWidget(validationLabel_);

        buttonBox_ = new QDialogButtonBox(
            QDialogButtonBox::Ok | QDialogButtonBox::Cancel, this);
        okButton_ = buttonBox_->button(QDialogButtonBox::Ok);
        connect(buttonBox_, &QDialogButtonBox::accepted, this,
                &QDialog::accept);
        connect(buttonBox_, &QDialogButtonBox::rejected, this,
                &QDialog::reject);
        layout->addWidget(buttonBox_);

        connect(nullableCheck_, &QCheckBox::toggled, this,
                &SCAddColumnDialog::UpdateValidationState);
        connect(relationCheck_, &QCheckBox::toggled, this,
                &SCAddColumnDialog::UpdateValidationState);
        connect(valueKindCombo_,
                qOverload<int>(&QComboBox::currentIndexChanged), this,
                &SCAddColumnDialog::UpdateValidationState);
        connect(defaultValueEdit_, &QLineEdit::textChanged, this,
                &SCAddColumnDialog::UpdateValidationState);

        UpdateValidationState();
    }

    SCAddColumnDialog::SCAddColumnDialog(const sc::SCColumnDef& initialValue,
                                         QWidget* parent)
        : SCAddColumnDialog(parent)
    {
        setWindowTitle(QStringLiteral("Edit Column"));
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
        relationCheck_->setChecked(value.columnKind ==
                                   sc::ColumnKind::Relation);
        nullableCheck_->setChecked(value.nullable);
        editableCheck_->setChecked(value.editable);
        userDefinedCheck_->setChecked(value.userDefined);
        indexedCheck_->setChecked(value.indexed);
        participatesInCalcCheck_->setChecked(value.participatesInCalc);
        unitEdit_->setText(QString::fromStdWString(value.unit));
        referenceTableEdit_->setText(
            QString::fromStdWString(value.referenceTable));

        if (value.defaultValue.IsNull())
        {
            defaultValueEdit_->clear();
            return;
        }

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
            case sc::ValueKind::Null:
            default:
                defaultValueEdit_->clear();
                break;
        }

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
        column.columnKind = relationCheck_->isChecked()
                                ? sc::ColumnKind::Relation
                                : sc::ColumnKind::Fact;
        if (column.columnKind == sc::ColumnKind::Relation)
        {
            column.valueKind = sc::ValueKind::RecordId;
        }
        column.nullable = nullableCheck_->isChecked();
        column.editable = editableCheck_->isChecked();
        column.userDefined = userDefinedCheck_->isChecked();
        column.indexed = indexedCheck_->isChecked();
        column.participatesInCalc = participatesInCalcCheck_->isChecked();
        column.unit = unitEdit_->text().toStdWString();
        column.referenceTable = referenceTableEdit_->text().toStdWString();
        column.defaultValue =
            ParseDefaultValue(column.valueKind, defaultValueEdit_->text());
        return column;
    }

    void SCAddColumnDialog::SetCurrentTableHasRecords(bool hasRecords)
    {
        currentTableHasRecords_ = hasRecords;
        UpdateValidationState();
    }

    void SCAddColumnDialog::UpdateValidationState()
    {
        const bool hasDefaultValue =
            !defaultValueEdit_->text().trimmed().isEmpty();
        const bool nonNullableWithoutDefault =
            !nullableCheck_->isChecked() && !hasDefaultValue;
        const bool requiresDefault =
            nonNullableWithoutDefault && currentTableHasRecords_;

        if (validationLabel_ != nullptr)
        {
            if (nonNullableWithoutDefault)
            {
                if (currentTableHasRecords_)
                {
                    validationLabel_->setStyleSheet(
                        QStringLiteral("color: #b00020;"));
                    validationLabel_->setText(QStringLiteral(
                        "This table already has records. Non-null columns "
                        "need a default value."));
                } else
                {
                    validationLabel_->setStyleSheet(
                        QStringLiteral("color: #0b5fff;"));
                    validationLabel_->setText(QStringLiteral(
                        "This table is empty. You can create a non-null "
                        "column without a default, but future inserts must "
                        "set a value explicitly."));
                }
            } else
            {
                validationLabel_->clear();
            }
            validationLabel_->setVisible(nonNullableWithoutDefault);
        }

        if (okButton_ != nullptr)
        {
            okButton_->setEnabled(!requiresDefault);
        }
    }

}  // namespace StableCore::Storage::Editor
