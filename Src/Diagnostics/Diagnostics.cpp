#include "StableCore/Storage/Diagnostics.h"

#include <sstream>

namespace stablecore::storage
{

ErrorCode BuildStorageHealthReport(
    IDatabase* database,
    const wchar_t* backendName,
    StorageHealthReport* outReport)
{
    if (database == nullptr || outReport == nullptr)
    {
        return SC_E_POINTER;
    }

    StorageHealthReport report;
    report.backendName = (backendName != nullptr) ? backendName : L"unknown";
    report.currentVersion = database->GetCurrentVersion();

    if (const auto* provider = dynamic_cast<const IDatabaseDiagnosticsProvider*>(database))
    {
        const ErrorCode rc = provider->CollectDiagnostics(&report);
        if (Failed(rc))
        {
            return rc;
        }
    }
    else
    {
        report.diagnostics.push_back(DiagnosticEntry{
            DiagnosticSeverity::Info,
            L"version",
            L"Database opened and version metadata is available.",
        });
        report.diagnostics.push_back(DiagnosticEntry{
            DiagnosticSeverity::Info,
            L"recovery",
            L"Undo/redo journal should be replay-safe before product startup proceeds.",
        });
    }

    *outReport = std::move(report);
    return SC_OK;
}

ErrorCode DescribeChangeSet(const ChangeSet& changeSet, std::wstring* outText)
{
    if (outText == nullptr)
    {
        return SC_E_POINTER;
    }

    std::wostringstream stream;
    stream << L"Action=" << changeSet.actionName
           << L", Version=" << changeSet.version
           << L", Changes=" << changeSet.changes.size();

    for (const auto& change : changeSet.changes)
    {
        stream << L"\n - " << change.tableName << L"#" << change.recordId;
        if (!change.fieldName.empty())
        {
            stream << L"." << change.fieldName;
        }
        switch (change.kind)
        {
        case ChangeKind::FieldUpdated: stream << L" FieldUpdated"; break;
        case ChangeKind::RecordCreated: stream << L" RecordCreated"; break;
        case ChangeKind::RecordDeleted: stream << L" RecordDeleted"; break;
        case ChangeKind::RelationUpdated: stream << L" RelationUpdated"; break;
        }
    }

    *outText = stream.str();
    return SC_OK;
}

}  // namespace stablecore::storage
