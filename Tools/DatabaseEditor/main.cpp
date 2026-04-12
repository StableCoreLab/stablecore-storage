#include <QApplication>

#include "SCDatabaseEditorMainWindow.h"

int main(int argc, char* argv[])
{
    QApplication app(argc, argv);
    QApplication::setApplicationName(QStringLiteral("StableCore Database Editor"));
    QApplication::setOrganizationName(QStringLiteral("StableCore"));

    StableCore::Storage::Editor::SCDatabaseEditorMainWindow window;
    window.show();
    return app.exec();
}
