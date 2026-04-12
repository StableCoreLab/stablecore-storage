#pragma once

#include <vector>

#include "StableCore/Storage/ISCInterfaces.h"

namespace stablecore::storage
{

enum class SCDiagnosticSeverity
{
    Info,
    Warning,
    Error,
};

struct SCDiagnosticEntry
{
    SCDiagnosticSeverity severity{SCDiagnosticSeverity::Info};
    std::wstring category;
    std::wstring message;
};

struct SCStorageHealthReport
{
    std::wstring backendName;
    VersionId currentVersion{0};
    std::vector<SCDiagnosticEntry> diagnostics;
};

class ISCDatabaseDiagnosticsProvider
{
public:
    virtual ~ISCDatabaseDiagnosticsProvider() = default;
    virtual ErrorCode CollectDiagnostics(SCStorageHealthReport* outReport) const = 0;
};

ErrorCode BuildStorageHealthReport(
    ISCDatabase* database,
    const wchar_t* backendName,
    SCStorageHealthReport* outReport);

ErrorCode DescribeChangeSet(const SCChangeSet& SCChangeSet, std::wstring* outText);

}  // namespace stablecore::storage
