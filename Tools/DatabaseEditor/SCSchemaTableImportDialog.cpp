#include "SCSchemaTableImportDialog.h"

#include <QHBoxLayout>
#include <QLabel>
#include <QPlainTextEdit>
#include <QPushButton>
#include <QVBoxLayout>

#include "SCDatabaseSession.h"
#include "SCSchemaTableImport.h"

namespace StableCore::Storage::Editor
{
    namespace
    {

        QString DefaultImportTemplate()
        {
            return QStringLiteral(R"(SC_SCHEMA_TABLE(ProjectInfo)
{
    Table("ProjectInfo")
        .Column("GUID", SCType::String)
        .Column("ProjectName", SCType::String)
            .Description("Project Name")
        .Column("ProjectCode", SCType::String)
            .Description("Project Code")
        .Column("OwnerUnit", SCType::String)
            .Description("Owner Unit")
        .Column("ConstructorUnit", SCType::String)
            .Description("Constructor Unit")
        .Column("Compiler", SCType::String)
            .Description("Compiler")
        .Column("CompileDate", SCType::String)
            .Description("Compile Date")
        .Column("RegionCode", SCType::String)
            .Description("Region Code")
        .Column("RegionName", SCType::String)
            .Description("Region Name")
        .Column("ProjectAddress", SCType::String)
            .Description("Project Address")
        .Column("Description", SCType::String)
            .Description("Remark")
        .Column("CreatedAt", SCType::String)
            .Description("Created At")
        .Column("UpdatedAt", SCType::String)
            .Description("Updated At");
})");
        }

    }  // namespace

    SCSchemaTableImportDialog::SCSchemaTableImportDialog(
        SCDatabaseSession* session, QWidget* parent)
        : QDialog(parent), session_(session)
    {
        setWindowTitle(QStringLiteral("从模式创建表"));
        resize(980, 760);

        auto* layout = new QVBoxLayout(this);

        auto* introLabel = new QLabel(
            QStringLiteral(
                "粘贴 SC_SCHEMA_TABLE(...) 文本以从其模式描述创建表。"
                "列描述将成为显示名称。"
                "默认值会被导入（如果存在）。"
                "主键和表描述会被视为提示。"),
            this);
        introLabel->setWordWrap(true);
        layout->addWidget(introLabel);

        schemaTextEdit_ = new QPlainTextEdit(this);
        schemaTextEdit_->setPlainText(DefaultImportTemplate());
        schemaTextEdit_->setLineWrapMode(QPlainTextEdit::NoWrap);
        layout->addWidget(schemaTextEdit_, 1);

        messageLabel_ = new QLabel(this);
        messageLabel_->setWordWrap(true);
        messageLabel_->setText(QStringLiteral("就绪。"));
        layout->addWidget(messageLabel_);

        auto* buttonRow = new QHBoxLayout();
        auto* createButton =
            new QPushButton(QStringLiteral("创建表"), this);
        auto* closeButton = new QPushButton(QStringLiteral("关闭"), this);
        buttonRow->addWidget(createButton);
        buttonRow->addStretch(1);
        buttonRow->addWidget(closeButton);
        layout->addLayout(buttonRow);

        connect(createButton, &QPushButton::clicked, this,
                &SCSchemaTableImportDialog::CreateFromText);
        connect(closeButton, &QPushButton::clicked, this, &QDialog::accept);
    }

    void SCSchemaTableImportDialog::UpdateMessage(const QString& text)
    {
        if (messageLabel_ != nullptr)
        {
            messageLabel_->setText(text);
        }
    }

    void SCSchemaTableImportDialog::CreateFromText()
    {
        if (session_ == nullptr || !session_->IsOpen())
        {
            UpdateMessage(QStringLiteral("没有打开的数据库。"));
            return;
        }

        SCSchemaTableImportResult importResult;
        QString parseError;
        if (!ParseSchemaTableDescription(schemaTextEdit_->toPlainText(),
                                         &importResult, &parseError))
        {
            UpdateMessage(QStringLiteral("解析失败: ") + parseError);
            return;
        }

        QString createError;
        if (!session_->CreateTableFromSchema(importResult, &createError))
        {
            UpdateMessage(QStringLiteral("创建失败: ") + createError);
            return;
        }

        QString status = QStringLiteral("已创建表 \"%1\"，包含 %2 列。")
                             .arg(importResult.tableName)
                             .arg(importResult.columns.size());
        if (!importResult.warnings.isEmpty())
        {
            status += QStringLiteral(" 警告: ") +
                      importResult.warnings.join(QStringLiteral(" | "));
        }
        UpdateMessage(status);
    }

}  // namespace StableCore::Storage::Editor
