#include "StableCore/Storage/ISCDiagnostics.h"

#include <sstream>

namespace stablecore::storage
{

ErrorCode BuildStorageHealthReport(
    ISCDatabase* database,
    const wchar_t* backendName,
    SCStorageHealthReport* outReport)
{
    if (database == nullptr || outReport == nullptr)
    {
        return SC_E_POINTER;
    }

    SCStorageHealthReport report;
    report.backendName = (backendName != nullptr) ? backendName : L"unknown";
    report.currentVersion = database->GetCurrentVersion();

    if (const auto* provider = dynamic_cast<const ISCDatabaseDiagnosticsProvider*>(database))
    {
        const ErrorCode rc = provider->CollectDiagnostics(&report);
        if (Failed(rc))
        {
            return rc;
        }
    }
    else
    {
        report.diagnostics.push_back(SCDiagnosticEntry{
            SCDiagnosticSeverity::Info,
            L"version",
            L"Database opened and version metadata is available.",
        });
        report.diagnostics.push_back(SCDiagnosticEntry{
            SCDiagnosticSeverity::Info,
            L"recovery",
            L"Undo/redo journal should be replay-safe before product startup proceeds.",
        });
    }

    *outReport = std::move(report);
    return SC_OK;
}

ErrorCode DescribeChangeSet(const SCChangeSet& SCChangeSet, std::wstring* outText)
{
    if (outText == nullptr)
    {
        return SC_E_POINTER;
    }

    std::wostringstream stream;
    stream << L"Action=" << SCChangeSet.actionName
           << L", Version=" << SCChangeSet.version
           << L", Changes=" << SCChangeSet.changes.size();

    for (const auto& change : SCChangeSet.changes)
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
