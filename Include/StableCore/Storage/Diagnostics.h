#pragma once

#include <vector>

#include "StableCore/Storage/Interfaces.h"

namespace stablecore::storage
{

enum class DiagnosticSeverity
{
    Info,
    Warning,
    Error,
};

struct DiagnosticEntry
{
    DiagnosticSeverity severity{DiagnosticSeverity::Info};
    std::wstring category;
    std::wstring message;
};

struct StorageHealthReport
{
    std::wstring backendName;
    VersionId currentVersion{0};
    std::vector<DiagnosticEntry> diagnostics;
};

class IDatabaseDiagnosticsProvider
{
public:
    virtual ~IDatabaseDiagnosticsProvider() = default;
    virtual ErrorCode CollectDiagnostics(StorageHealthReport* outReport) const = 0;
};

ErrorCode BuildStorageHealthReport(
    IDatabase* database,
    const wchar_t* backendName,
    StorageHealthReport* outReport);

ErrorCode DescribeChangeSet(const ChangeSet& changeSet, std::wstring* outText);

}  // namespace stablecore::storage
