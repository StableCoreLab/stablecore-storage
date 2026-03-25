#include "ComputedColumnDialog.h"

#include <QDialogButtonBox>
#include <QFormLayout>
#include <QLabel>
#include <QStringList>
#include <QVBoxLayout>

namespace sc = stablecore::storage;

namespace stablecore::storage::editor
{
namespace
{

std::vector<sc::FieldDependency> ParseDependencies(const QString& text, const QString& currentTableName)
{
    std::vector<sc::FieldDependency> result;
    const QStringList parts = text.split(',', Qt::SkipEmptyParts);
    for (const QString& rawPart : parts)
    {
        const QString token = rawPart.trimmed();
        if (token.isEmpty())
        {
            continue;
        }

        const int dot = token.indexOf('.');
        if (dot >= 0)
        {
            result.push_back(sc::FieldDependency{
                token.left(dot).trimmed().toStdWString(),
                token.mid(dot + 1).trimmed().toStdWString()});
            continue;
        }

        result.push_back(sc::FieldDependency{
            currentTableName.toStdWString(),
            token.toStdWString()});
    }
    return result;
}

QString JoinDependencies(
    const std::vector<sc::FieldDependency>& dependencies,
    const QString& currentTableName)
{
    QStringList parts;
    for (const sc::FieldDependency& dependency : dependencies)
    {
        const QString tableName = QString::fromStdWString(dependency.tableName);
        const QString fieldName = QString::fromStdWString(dependency.fieldName);
        parts.push_back(
            tableName.compare(currentTableName, Qt::CaseInsensitive) == 0
                ? fieldName
                : tableName + QStringLiteral(".") + fieldName);
    }
    return parts.join(QStringLiteral(", "));
}

}  // namespace

ComputedColumnDialog::ComputedColumnDialog(const QString& currentTableName, QWidget* parent)
    : QDialog(parent)
    , currentTableName_(currentTableName)
{
    setWindowTitle(QStringLiteral("Add Session Computed Column"));
    resize(620, 540);
    BuildForm();
}

ComputedColumnDialog::ComputedColumnDialog(
    const QString& currentTableName,
    const sc::ComputedColumnDef& initialValue,
    QWidget* parent)
    : QDialog(parent)
    , currentTableName_(currentTableName)
{
    setWindowTitle(QStringLiteral("Edit Session Computed Column"));
    resize(620, 540);
    BuildForm();
    ApplyInitialValue(initialValue);
}

void ComputedColumnDialog::BuildForm()
{
    auto* layout = new QVBoxLayout(this);
    layout->addWidget(new QLabel(
        QStringLiteral("Computed columns are session-only and will disappear after the editor closes."),
        this));

    auto* form = new QFormLayout();
    nameEdit_ = new QLineEdit(this);
    displayNameEdit_ = new QLineEdit(this);

    valueKindCombo_ = new QComboBox(this);
    valueKindCombo_->addItem(QStringLiteral("Int64"), static_cast<int>(sc::ValueKind::Int64));
    valueKindCombo_->addItem(QStringLiteral("Double"), static_cast<int>(sc::ValueKind::Double));
    valueKindCombo_->addItem(QStringLiteral("Bool"), static_cast<int>(sc::ValueKind::Bool));
    valueKindCombo_->addItem(QStringLiteral("String"), static_cast<int>(sc::ValueKind::String));
    valueKindCombo_->addItem(QStringLiteral("RecordId"), static_cast<int>(sc::ValueKind::RecordId));
    valueKindCombo_->addItem(QStringLiteral("Enum"), static_cast<int>(sc::ValueKind::Enum));

    kindCombo_ = new QComboBox(this);
    kindCombo_->addItem(QStringLiteral("Expression"), static_cast<int>(sc::ComputedFieldKind::Expression));
    kindCombo_->addItem(QStringLiteral("Rule"), static_cast<int>(sc::ComputedFieldKind::Rule));
    kindCombo_->addItem(QStringLiteral("Aggregate"), static_cast<int>(sc::ComputedFieldKind::Aggregate));

    expressionEdit_ = new QPlainTextEdit(this);
    expressionEdit_->setPlaceholderText(QStringLiteral("Example: Length * Width"));
    expressionEdit_->setFixedHeight(100);

    ruleIdEdit_ = new QLineEdit(this);
    ruleIdEdit_->setPlaceholderText(QStringLiteral("Rule registry id"));

    aggregateKindCombo_ = new QComboBox(this);
    aggregateKindCombo_->addItem(QStringLiteral("Count"), static_cast<int>(sc::AggregateKind::Count));
    aggregateKindCombo_->addItem(QStringLiteral("Sum"), static_cast<int>(sc::AggregateKind::Sum));
    aggregateKindCombo_->addItem(QStringLiteral("Min"), static_cast<int>(sc::AggregateKind::Min));
    aggregateKindCombo_->addItem(QStringLiteral("Max"), static_cast<int>(sc::AggregateKind::Max));

    aggregateRelationEdit_ = new QLineEdit(this);
    aggregateRelationEdit_->setPlaceholderText(QStringLiteral("Example: Beam.FloorRef"));
    aggregateFieldEdit_ = new QLineEdit(this);
    aggregateFieldEdit_->setPlaceholderText(QStringLiteral("Field used by Sum/Min/Max"));

    factDepsEdit_ = new QLineEdit(this);
    factDepsEdit_->setPlaceholderText(QStringLiteral("Comma-separated, supports Table.Field"));
    relationDepsEdit_ = new QLineEdit(this);
    relationDepsEdit_->setPlaceholderText(QStringLiteral("Comma-separated, supports Table.Field"));

    cacheableCheck_ = new QCheckBox(QStringLiteral("Cacheable"), this);
    cacheableCheck_->setChecked(true);

    form->addRow(QStringLiteral("Name"), nameEdit_);
    form->addRow(QStringLiteral("Display Name"), displayNameEdit_);
    form->addRow(QStringLiteral("Value Kind"), valueKindCombo_);
    form->addRow(QStringLiteral("Kind"), kindCombo_);
    form->addRow(QStringLiteral("Expression"), expressionEdit_);
    form->addRow(QStringLiteral("Rule Id"), ruleIdEdit_);
    form->addRow(QStringLiteral("Aggregate Kind"), aggregateKindCombo_);
    form->addRow(QStringLiteral("Aggregate Relation"), aggregateRelationEdit_);
    form->addRow(QStringLiteral("Aggregate Field"), aggregateFieldEdit_);
    form->addRow(QStringLiteral("Fact Dependencies"), factDepsEdit_);
    form->addRow(QStringLiteral("Relation Dependencies"), relationDepsEdit_);
    form->addRow(QString(), cacheableCheck_);
    layout->addLayout(form);

    auto* buttons = new QDialogButtonBox(QDialogButtonBox::Ok | QDialogButtonBox::Cancel, this);
    layout->addWidget(buttons);

    connect(kindCombo_, qOverload<int>(&QComboBox::currentIndexChanged), this, &ComputedColumnDialog::UpdateModeVisibility);
    connect(buttons, &QDialogButtonBox::accepted, this, &QDialog::accept);
    connect(buttons, &QDialogButtonBox::rejected, this, &QDialog::reject);

    UpdateModeVisibility();
}

void ComputedColumnDialog::ApplyInitialValue(const sc::ComputedColumnDef& initialValue)
{
    nameEdit_->setText(QString::fromStdWString(initialValue.name));
    displayNameEdit_->setText(QString::fromStdWString(initialValue.displayName));
    valueKindCombo_->setCurrentIndex(valueKindCombo_->findData(static_cast<int>(initialValue.valueKind)));
    kindCombo_->setCurrentIndex(kindCombo_->findData(static_cast<int>(initialValue.kind)));
    expressionEdit_->setPlainText(QString::fromStdWString(initialValue.expression));
    ruleIdEdit_->setText(QString::fromStdWString(initialValue.ruleId));
    aggregateKindCombo_->setCurrentIndex(aggregateKindCombo_->findData(static_cast<int>(initialValue.aggregateKind)));
    aggregateRelationEdit_->setText(QString::fromStdWString(initialValue.aggregateRelation));
    aggregateFieldEdit_->setText(QString::fromStdWString(initialValue.aggregateField));
    factDepsEdit_->setText(JoinDependencies(initialValue.dependencies.factFields, currentTableName_));
    relationDepsEdit_->setText(JoinDependencies(initialValue.dependencies.relationFields, currentTableName_));
    cacheableCheck_->setChecked(initialValue.cacheable);
    UpdateModeVisibility();
}

bool ComputedColumnDialog::BuildDefinition(sc::ComputedColumnDef* outColumn, QString* outError) const
{
    if (outColumn == nullptr)
    {
        if (outError != nullptr)
        {
            *outError = QStringLiteral("Output column is null.");
        }
        return false;
    }

    const QString name = nameEdit_->text().trimmed();
    if (name.isEmpty())
    {
        if (outError != nullptr)
        {
            *outError = QStringLiteral("Computed column name is required.");
        }
        return false;
    }

    sc::ComputedColumnDef column;
    column.name = name.toStdWString();
    column.displayName = displayNameEdit_->text().trimmed().toStdWString();
    column.valueKind = CurrentValueKind();
    column.kind = CurrentComputedKind();
    column.cacheable = cacheableCheck_->isChecked();
    column.editable = false;

    if (column.kind == sc::ComputedFieldKind::Expression)
    {
        const QString expression = expressionEdit_->toPlainText().trimmed();
        if (expression.isEmpty())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Expression is required for expression computed columns.");
            }
            return false;
        }
        column.expression = expression.toStdWString();
    }
    else if (column.kind == sc::ComputedFieldKind::Rule)
    {
        const QString ruleId = ruleIdEdit_->text().trimmed();
        if (ruleId.isEmpty())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Rule id is required for rule computed columns.");
            }
            return false;
        }
        column.ruleId = ruleId.toStdWString();
    }
    else
    {
        const QString relation = aggregateRelationEdit_->text().trimmed();
        if (relation.isEmpty())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Aggregate relation is required.");
            }
            return false;
        }

        column.aggregateKind = CurrentAggregateKind();
        column.aggregateRelation = relation.toStdWString();
        if (column.aggregateKind != sc::AggregateKind::Count)
        {
            const QString field = aggregateFieldEdit_->text().trimmed();
            if (field.isEmpty())
            {
                if (outError != nullptr)
                {
                    *outError = QStringLiteral("Aggregate field is required for Sum/Min/Max.");
                }
                return false;
            }
            column.aggregateField = field.toStdWString();
        }
    }

    column.dependencies.factFields = ParseDependencies(factDepsEdit_->text(), currentTableName_);
    column.dependencies.relationFields = ParseDependencies(relationDepsEdit_->text(), currentTableName_);
    *outColumn = column;
    return true;
}

void ComputedColumnDialog::UpdateModeVisibility()
{
    const sc::ComputedFieldKind kind = CurrentComputedKind();
    expressionEdit_->setVisible(kind == sc::ComputedFieldKind::Expression);
    ruleIdEdit_->setVisible(kind == sc::ComputedFieldKind::Rule);
    aggregateKindCombo_->setVisible(kind == sc::ComputedFieldKind::Aggregate);
    aggregateRelationEdit_->setVisible(kind == sc::ComputedFieldKind::Aggregate);
    aggregateFieldEdit_->setVisible(kind == sc::ComputedFieldKind::Aggregate);
}

sc::ValueKind ComputedColumnDialog::CurrentValueKind() const
{
    return static_cast<sc::ValueKind>(valueKindCombo_->currentData().toInt());
}

sc::ComputedFieldKind ComputedColumnDialog::CurrentComputedKind() const
{
    return static_cast<sc::ComputedFieldKind>(kindCombo_->currentData().toInt());
}

sc::AggregateKind ComputedColumnDialog::CurrentAggregateKind() const
{
    return static_cast<sc::AggregateKind>(aggregateKindCombo_->currentData().toInt());
}

}  // namespace stablecore::storage::editor
