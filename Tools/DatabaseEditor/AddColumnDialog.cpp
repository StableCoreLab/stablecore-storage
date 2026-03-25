#include "AddColumnDialog.h"

namespace sc = stablecore::storage;

namespace stablecore::storage::editor
{
namespace
{

sc::Value ParseDefaultValue(sc::ValueKind kind, const QString& text)
{
    if (text.trimmed().isEmpty())
    {
        return sc::Value::Null();
    }

    switch (kind)
    {
    case sc::ValueKind::Int64:
        return sc::Value::FromInt64(text.toLongLong());
    case sc::ValueKind::Double:
        return sc::Value::FromDouble(text.toDouble());
    case sc::ValueKind::Bool:
        return sc::Value::FromBool(text.compare(QStringLiteral("true"), Qt::CaseInsensitive) == 0 || text == QStringLiteral("1"));
    case sc::ValueKind::String:
        return sc::Value::FromString(text.toStdWString());
    case sc::ValueKind::RecordId:
        return sc::Value::FromRecordId(text.toLongLong());
    case sc::ValueKind::Enum:
        return sc::Value::FromEnum(text.toStdWString());
    case sc::ValueKind::Null:
    default:
        return sc::Value::Null();
    }
}

}  // namespace

AddColumnDialog::AddColumnDialog(QWidget* parent)
    : QDialog(parent)
{
    setWindowTitle(QStringLiteral("Add Column"));

    auto* layout = new QFormLayout(this);

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

    valueKindCombo_->addItem(QStringLiteral("Int64"), static_cast<int>(sc::ValueKind::Int64));
    valueKindCombo_->addItem(QStringLiteral("Double"), static_cast<int>(sc::ValueKind::Double));
    valueKindCombo_->addItem(QStringLiteral("Bool"), static_cast<int>(sc::ValueKind::Bool));
    valueKindCombo_->addItem(QStringLiteral("String"), static_cast<int>(sc::ValueKind::String));
    valueKindCombo_->addItem(QStringLiteral("RecordId"), static_cast<int>(sc::ValueKind::RecordId));
    valueKindCombo_->addItem(QStringLiteral("Enum"), static_cast<int>(sc::ValueKind::Enum));

    nullableCheck_->setChecked(true);
    editableCheck_->setChecked(true);

    layout->addRow(QStringLiteral("Name"), nameEdit_);
    layout->addRow(QStringLiteral("Display Name"), displayNameEdit_);
    layout->addRow(QStringLiteral("Value Kind"), valueKindCombo_);
    layout->addRow(QStringLiteral("Column Kind"), relationCheck_);
    layout->addRow(QStringLiteral("Nullable"), nullableCheck_);
    layout->addRow(QStringLiteral("Editable"), editableCheck_);
    layout->addRow(QStringLiteral("User Defined"), userDefinedCheck_);
    layout->addRow(QStringLiteral("Indexed"), indexedCheck_);
    layout->addRow(QStringLiteral("Participates In Calc"), participatesInCalcCheck_);
    layout->addRow(QStringLiteral("Unit"), unitEdit_);
    layout->addRow(QStringLiteral("Reference Table"), referenceTableEdit_);
    layout->addRow(QStringLiteral("Default Value"), defaultValueEdit_);

    auto* buttons = new QDialogButtonBox(QDialogButtonBox::Ok | QDialogButtonBox::Cancel, this);
    connect(buttons, &QDialogButtonBox::accepted, this, &QDialog::accept);
    connect(buttons, &QDialogButtonBox::rejected, this, &QDialog::reject);
    layout->addRow(buttons);
}

sc::ColumnDef AddColumnDialog::BuildColumnDef() const
{
    sc::ColumnDef column;
    column.name = nameEdit_->text().toStdWString();
    column.displayName = displayNameEdit_->text().isEmpty() ? column.name : displayNameEdit_->text().toStdWString();
    column.valueKind = static_cast<sc::ValueKind>(valueKindCombo_->currentData().toInt());
    column.columnKind = relationCheck_->isChecked() ? sc::ColumnKind::Relation : sc::ColumnKind::Fact;
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
    column.defaultValue = ParseDefaultValue(column.valueKind, defaultValueEdit_->text());
    return column;
}

}  // namespace stablecore::storage::editor
