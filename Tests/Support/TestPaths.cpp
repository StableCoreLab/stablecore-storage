#include "TestPaths.h"

namespace
{
    void RemoveIfExists(const fs::path& path)
    {
        std::error_code ec;
        fs::remove(path, ec);
    }
}

fs::path MakeTempDbPath(const wchar_t* fileName)
{
    fs::path path = fs::temp_directory_path() / fileName;
    RemoveTempDbArtifacts(path);
    return path;
}

void RemoveTempDbArtifacts(const fs::path& path)
{
    RemoveIfExists(path);

    fs::path walPath = path;
    walPath += L"-wal";
    RemoveIfExists(walPath);

    fs::path shmPath = path;
    shmPath += L"-shm";
    RemoveIfExists(shmPath);

    fs::path journalPath = path;
    journalPath += L"-journal";
    RemoveIfExists(journalPath);
}
