#include <QApplication>

#include "SCDatabaseEditorMainWindow.h"

int main(int argc, char* argv[])
{
    QApplication app(argc, argv);
    QApplication::setApplicationName(
        QStringLiteral("StableCore 数据库编辑器"));
    QApplication::setOrganizationName(QStringLiteral("StableCore"));

    StableCore::Storage::Editor::SCDatabaseEditorMainWindow window;
    window.show();
    return app.exec();
}
