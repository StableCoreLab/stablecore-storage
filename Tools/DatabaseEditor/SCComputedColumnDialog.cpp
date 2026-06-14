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
            const QString& text, const QString& currentTableName,
            std::vector<sc::SCFieldDependency>* outDependencies,
            QString* outError)
        {
            if (outDependencies == nullptr)
        {
            if (outError != nullptr)
            {
                *outError =
                    QStringLiteral("输出依赖容器为空。");
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
                if (tableName.isEmpty() || fieldName.isEmpty() ||
                    token.indexOf('.', dot + 1) >= 0)
                {
                    if (outError != nullptr)
                    {
                        *outError = QStringLiteral(
                            "依赖项必须使用 表.字段 格式。");
                    }
                    return false;
                }

                outDependencies->push_back(sc::SCFieldDependency{
                    tableName.toStdWString(), fieldName.toStdWString()});
                continue;
            }

            outDependencies->push_back(sc::SCFieldDependency{
                currentTableName.toStdWString(), token.toStdWString()});
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
                const QString tableName =
                    QString::fromStdWString(dependency.tableName);
                const QString fieldName =
                    QString::fromStdWString(dependency.fieldName);
                parts.push_back(
                    tableName.compare(currentTableName, Qt::CaseInsensitive) ==
                            0
                        ? fieldName
                        : tableName + QStringLiteral(".") + fieldName);
            }
            return parts.join(QStringLiteral(", "));
        }

    }  // namespace

    SCComputedColumnDialog::SCComputedColumnDialog(
        const QString& currentTableName, QWidget* parent)
        : SCComputedColumnDialog(currentTableName, sc::SCComputedColumnDef{},
                                 false, parent)
    {
    }

    SCComputedColumnDialog::SCComputedColumnDialog(
        const QString& currentTableName,
        const sc::SCComputedColumnDef& initialValue, QWidget* parent)
        : SCComputedColumnDialog(currentTableName, initialValue, false, parent)
    {
    }

    SCComputedColumnDialog::SCComputedColumnDialog(
        const QString& currentTableName,
        const sc::SCComputedColumnDef& initialValue, bool lockName,
        QWidget* parent)
        : QDialog(parent),
          currentTableName_(currentTableName),
          lockName_(lockName)
    {
        setWindowTitle(initialValue.name.empty()
                           ? QStringLiteral("添加会话计算列")
                           : QStringLiteral("编辑会话计算列"));
        resize(620, 540);
        BuildForm();
        nameEdit_->setReadOnly(lockName_);
        if (lockName_)
        {
            nameEdit_->setEnabled(true);
        }
        if (!initialValue.name.empty())
        {
            ApplyInitialValue(initialValue);
        }
    }

    void SCComputedColumnDialog::BuildForm()
    {
        auto* layout = new QVBoxLayout(this);
        layout->addWidget(new QLabel(
            QStringLiteral("计算列仅在会话期间有效，编辑器关闭后将消失。"),
            this));

        auto* form = new QFormLayout();
        nameEdit_ = new QLineEdit(this);
        displayNameEdit_ = new QLineEdit(this);

        valueKindCombo_ = new QComboBox(this);
        valueKindCombo_->addItem(QStringLiteral("整数 (Int64)"),
                                 static_cast<int>(sc::ValueKind::Int64));
        valueKindCombo_->addItem(QStringLiteral("浮点数 (Double)"),
                                 static_cast<int>(sc::ValueKind::Double));
        valueKindCombo_->addItem(QStringLiteral("布尔 (Bool)"),
                                 static_cast<int>(sc::ValueKind::Bool));
        valueKindCombo_->addItem(QStringLiteral("字符串 (String)"),
                                 static_cast<int>(sc::ValueKind::String));
        valueKindCombo_->addItem(QStringLiteral("记录ID (RecordId)"),
                                 static_cast<int>(sc::ValueKind::RecordId));
        valueKindCombo_->addItem(QStringLiteral("枚举 (Enum)"),
                                 static_cast<int>(sc::ValueKind::Enum));

        kindCombo_ = new QComboBox(this);
        kindCombo_->addItem(
            QStringLiteral("表达式"),
            static_cast<int>(sc::ComputedFieldKind::Expression));
        kindCombo_->addItem(QStringLiteral("规则"),
                            static_cast<int>(sc::ComputedFieldKind::Rule));
        kindCombo_->addItem(QStringLiteral("聚合"),
                            static_cast<int>(sc::ComputedFieldKind::Aggregate));

        expressionEdit_ = new QPlainTextEdit(this);
        expressionEdit_->setPlaceholderText(
            QStringLiteral("示例: Length * Width"));
        expressionEdit_->setFixedHeight(100);

        ruleIdEdit_ = new QLineEdit(this);
        ruleIdEdit_->setPlaceholderText(QStringLiteral("规则注册ID"));

        aggregateKindCombo_ = new QComboBox(this);
        aggregateKindCombo_->addItem(
            QStringLiteral("计数"),
            static_cast<int>(sc::SCAggregateKind::Count));
        aggregateKindCombo_->addItem(
            QStringLiteral("求和"), static_cast<int>(sc::SCAggregateKind::Sum));
        aggregateKindCombo_->addItem(
            QStringLiteral("最小值"), static_cast<int>(sc::SCAggregateKind::Min));
        aggregateKindCombo_->addItem(
            QStringLiteral("最大值"), static_cast<int>(sc::SCAggregateKind::Max));

        aggregateRelationEdit_ = new QLineEdit(this);
        aggregateRelationEdit_->setPlaceholderText(
            QStringLiteral("示例: Beam.FloorRef"));
        aggregateFieldEdit_ = new QLineEdit(this);
        aggregateFieldEdit_->setPlaceholderText(
            QStringLiteral("Sum/Min/Max 使用的字段"));

        factDepsEdit_ = new QLineEdit(this);
        factDepsEdit_->setPlaceholderText(
            QStringLiteral("逗号分隔，支持 表.字段 格式"));
        relationDepsEdit_ = new QLineEdit(this);
        relationDepsEdit_->setPlaceholderText(
            QStringLiteral("逗号分隔，支持 表.字段 格式"));

        cacheableCheck_ = new QCheckBox(QStringLiteral("可缓存"), this);
        cacheableCheck_->setChecked(true);

        form->addRow(QStringLiteral("名称"), nameEdit_);
        form->addRow(QStringLiteral("显示名称"), displayNameEdit_);
        form->addRow(QStringLiteral("数据类型"), valueKindCombo_);
        form->addRow(QStringLiteral("类型"), kindCombo_);
        form->addRow(QStringLiteral("表达式"), expressionEdit_);
        form->addRow(QStringLiteral("规则ID"), ruleIdEdit_);
        form->addRow(QStringLiteral("聚合类型"), aggregateKindCombo_);
        form->addRow(QStringLiteral("聚合关联表"),
                     aggregateRelationEdit_);
        form->addRow(QStringLiteral("聚合字段"), aggregateFieldEdit_);
        form->addRow(QStringLiteral("事实依赖"), factDepsEdit_);
        form->addRow(QStringLiteral("关联依赖"),
                     relationDepsEdit_);
        form->addRow(QString(), cacheableCheck_);
        layout->addLayout(form);

        auto* buttons = new QDialogButtonBox(
            QDialogButtonBox::Ok | QDialogButtonBox::Cancel, this);
        layout->addWidget(buttons);

        connect(kindCombo_, qOverload<int>(&QComboBox::currentIndexChanged),
                this, &SCComputedColumnDialog::UpdateModeVisibility);
        connect(buttons, &QDialogButtonBox::accepted, this, &QDialog::accept);
        connect(buttons, &QDialogButtonBox::rejected, this, &QDialog::reject);

        UpdateModeVisibility();
    }

    void SCComputedColumnDialog::ApplyInitialValue(
        const sc::SCComputedColumnDef& initialValue)
    {
        nameEdit_->setText(QString::fromStdWString(initialValue.name));
        displayNameEdit_->setText(
            QString::fromStdWString(initialValue.displayName));
        valueKindCombo_->setCurrentIndex(valueKindCombo_->findData(
            static_cast<int>(initialValue.valueKind)));
        kindCombo_->setCurrentIndex(
            kindCombo_->findData(static_cast<int>(initialValue.kind)));
        expressionEdit_->setPlainText(
            QString::fromStdWString(initialValue.expression));
        ruleIdEdit_->setText(QString::fromStdWString(initialValue.ruleId));
        aggregateKindCombo_->setCurrentIndex(aggregateKindCombo_->findData(
            static_cast<int>(initialValue.aggregateKind)));
        aggregateRelationEdit_->setText(
            QString::fromStdWString(initialValue.aggregateRelation));
        aggregateFieldEdit_->setText(
            QString::fromStdWString(initialValue.aggregateField));
        factDepsEdit_->setText(JoinDependencies(
            initialValue.dependencies.factFields, currentTableName_));
        relationDepsEdit_->setText(JoinDependencies(
            initialValue.dependencies.relationFields, currentTableName_));
        cacheableCheck_->setChecked(initialValue.cacheable);
        UpdateModeVisibility();
    }

    bool SCComputedColumnDialog::BuildDefinition(
        sc::SCComputedColumnDef* outColumn, QString* outError) const
    {
        if (outColumn == nullptr)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("输出列为空。");
            }
            return false;
        }

        const QString name = nameEdit_->text().trimmed();
        if (name.isEmpty())
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("计算列名称为必填项。");
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
                    *outError = QStringLiteral(
                        "表达式计算列需要填写表达式。");
                }
                return false;
            }
            column.expression = expression.toStdWString();
        } else if (column.kind == sc::ComputedFieldKind::Rule)
        {
            const QString ruleId = ruleIdEdit_->text().trimmed();
            if (ruleId.isEmpty())
            {
                if (outError != nullptr)
                {
                    *outError = QStringLiteral(
                        "规则计算列需要填写规则ID。");
                }
                return false;
            }
            column.ruleId = ruleId.toStdWString();
        } else
        {
            const QString relation = aggregateRelationEdit_->text().trimmed();
            if (relation.isEmpty())
            {
                if (outError != nullptr)
                {
                    *outError =
                        QStringLiteral("聚合计算需要填写关联表。");
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
                        *outError = QStringLiteral(
                            "Sum/Min/Max 聚合需要填写聚合字段。");
                    }
                    return false;
                }
                column.aggregateField = field.toStdWString();
            }
        }

        if (!ParseDependencies(factDepsEdit_->text(), currentTableName_,
                               &column.dependencies.factFields, outError))
        {
            return false;
        }
        if (!ParseDependencies(relationDepsEdit_->text(), currentTableName_,
                               &column.dependencies.relationFields, outError))
        {
            return false;
        }

        if (column.dependencies.factFields.empty() &&
            column.dependencies.relationFields.empty())
        {
            if (outError != nullptr)
            {
                *outError =
                    QStringLiteral("至少需要一个依赖项。");
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
        aggregateKindCombo_->setVisible(kind ==
                                        sc::ComputedFieldKind::Aggregate);
        aggregateRelationEdit_->setVisible(kind ==
                                           sc::ComputedFieldKind::Aggregate);
        aggregateFieldEdit_->setVisible(kind ==
                                        sc::ComputedFieldKind::Aggregate);
    }

    sc::ValueKind SCComputedColumnDialog::CurrentValueKind() const
    {
        return static_cast<sc::ValueKind>(
            valueKindCombo_->currentData().toInt());
    }

    sc::ComputedFieldKind SCComputedColumnDialog::CurrentComputedKind() const
    {
        return static_cast<sc::ComputedFieldKind>(
            kindCombo_->currentData().toInt());
    }

    sc::SCAggregateKind SCComputedColumnDialog::CurrentAggregateKind() const
    {
        return static_cast<sc::SCAggregateKind>(
            aggregateKindCombo_->currentData().toInt());
    }

}  // namespace StableCore::Storage::Editor
