#include "SCComputedColumnDialog.h"

#include <QDialogButtonBox>
#include <QFormLayout>
#include <QLabel>
#include <QStringList>
#include <QVBoxLayout>

namespace sc = StableCore::Storage;

namespace StableCore::Storage::Editor
{
namespace
{

bool ParseDependencies(
    const QString& text,
    const QString& currentTableName,
    std::vector<sc::SCFieldDependency>* outDependencies,
    QString* outError)
{
    if (outDependencies == nullptr)
    {
        if (outError != nullptr)
        {
            *outError = QStringLiteral("Output dependency container is null.");
        }
        return false;
    }

    outDependencies->clear();
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
            const QString tableName = token.left(dot).trimmed();
            const QString fieldName = token.mid(dot + 1).trimmed();
            if (tableName.isEmpty() || fieldName.isEmpty() || token.indexOf('.', dot + 1) >= 0)
            {
                if (outError != nullptr)
                {
                    *outError = QStringLiteral("Dependencies must use Table.Field format.");
                }
                return false;
            }

            outDependencies->push_back(sc::SCFieldDependency{tableName.toStdWString(), fieldName.toStdWString()});
            continue;
        }

        outDependencies->push_back(sc::SCFieldDependency{
            currentTableName.toStdWString(),
            token.toStdWString()});
    }
    return true;
}

QString JoinDependencies(
    const std::vector<sc::SCFieldDependency>& dependencies,
    const QString& currentTableName)
{
    QStringList parts;
    for (const sc::SCFieldDependency& dependency : dependencies)
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

SCComputedColumnDialog::SCComputedColumnDialog(const QString& currentTableName, QWidget* parent)
    : QDialog(parent)
    , currentTableName_(currentTableName)
{
    setWindowTitle(QStringLiteral("Add Session Computed Column"));
    resize(620, 540);
    BuildForm();
}

SCComputedColumnDialog::SCComputedColumnDialog(
    const QString& currentTableName,
    const sc::SCComputedColumnDef& initialValue,
    QWidget* parent)
    : QDialog(parent)
    , currentTableName_(currentTableName)
{
    setWindowTitle(QStringLiteral("Edit Session Computed Column"));
    resize(620, 540);
    BuildForm();
    ApplyInitialValue(initialValue);
}

void SCComputedColumnDialog::BuildForm()
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
    aggregateKindCombo_->addItem(QStringLiteral("Count"), static_cast<int>(sc::SCAggregateKind::Count));
    aggregateKindCombo_->addItem(QStringLiteral("Sum"), static_cast<int>(sc::SCAggregateKind::Sum));
    aggregateKindCombo_->addItem(QStringLiteral("Min"), static_cast<int>(sc::SCAggregateKind::Min));
    aggregateKindCombo_->addItem(QStringLiteral("Max"), static_cast<int>(sc::SCAggregateKind::Max));

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
    form->addRow(QStringLiteral("SCValue Kind"), valueKindCombo_);
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

    connect(kindCombo_, qOverload<int>(&QComboBox::currentIndexChanged), this, &SCComputedColumnDialog::UpdateModeVisibility);
    connect(buttons, &QDialogButtonBox::accepted, this, &QDialog::accept);
    connect(buttons, &QDialogButtonBox::rejected, this, &QDialog::reject);

    UpdateModeVisibility();
}

void SCComputedColumnDialog::ApplyInitialValue(const sc::SCComputedColumnDef& initialValue)
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

bool SCComputedColumnDialog::BuildDefinition(sc::SCComputedColumnDef* outColumn, QString* outError) const
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

    sc::SCComputedColumnDef column;
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
        if (column.aggregateKind != sc::SCAggregateKind::Count)
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

    if (!ParseDependencies(factDepsEdit_->text(), currentTableName_, &column.dependencies.factFields, outError))
    {
        return false;
    }
    if (!ParseDependencies(relationDepsEdit_->text(), currentTableName_, &column.dependencies.relationFields, outError))
    {
        return false;
    }

    if (column.dependencies.factFields.empty() && column.dependencies.relationFields.empty())
    {
        if (outError != nullptr)
        {
            *outError = QStringLiteral("At least one dependency is required.");
        }
        return false;
    }

    *outColumn = column;
    return true;
}

void SCComputedColumnDialog::UpdateModeVisibility()
{
    const sc::ComputedFieldKind kind = CurrentComputedKind();
    expressionEdit_->setVisible(kind == sc::ComputedFieldKind::Expression);
    ruleIdEdit_->setVisible(kind == sc::ComputedFieldKind::Rule);
    aggregateKindCombo_->setVisible(kind == sc::ComputedFieldKind::Aggregate);
    aggregateRelationEdit_->setVisible(kind == sc::ComputedFieldKind::Aggregate);
    aggregateFieldEdit_->setVisible(kind == sc::ComputedFieldKind::Aggregate);
}

sc::ValueKind SCComputedColumnDialog::CurrentValueKind() const
{
    return static_cast<sc::ValueKind>(valueKindCombo_->currentData().toInt());
}

sc::ComputedFieldKind SCComputedColumnDialog::CurrentComputedKind() const
{
    return static_cast<sc::ComputedFieldKind>(kindCombo_->currentData().toInt());
}

sc::SCAggregateKind SCComputedColumnDialog::CurrentAggregateKind() const
{
    return static_cast<sc::SCAggregateKind>(aggregateKindCombo_->currentData().toInt());
}

}  // namespace StableCore::Storage::Editor
