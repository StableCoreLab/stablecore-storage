#pragma once

#include <filesystem>

namespace fs = std::filesystem;

// Helper function to create a temporary database path and remove any existing file at that location
fs::path MakeTempDbPath(const wchar_t* fileName);

// Remove the database file and any SQLite sidecar files created alongside it.
void RemoveTempDbArtifacts(const fs::path& path);
