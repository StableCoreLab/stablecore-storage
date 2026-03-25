#include <QApplication>

#include "DatabaseEditorMainWindow.h"

int main(int argc, char* argv[])
{
    QApplication app(argc, argv);
    QApplication::setApplicationName(QStringLiteral("StableCore Database Editor"));
    QApplication::setOrganizationName(QStringLiteral("StableCore"));

    stablecore::storage::editor::DatabaseEditorMainWindow window;
    window.show();
    return app.exec();
}
