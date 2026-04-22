#pragma once

#include "SCErrors.h"

#include <cstddef>
#include <cstdint>
#include <string>

namespace StableCore::Storage
{

enum class SCExportMode
{
    Normal,
    DebugPackage,
};

enum class SCExportOverflowPolicy
{
    Fail,
    Truncate,
};

struct SCAssetSelection
{
    bool includeProject{true};
    bool includeSystemConfig{false};
    bool includeUserConfig{false};
    bool includeReplayJournal{false};
    bool includeReplaySnapshot{false};
    bool includeReplaySession{false};
    bool includeDiagnostics{false};
    bool includeLog{false};
};

struct SCRedactionPolicy
{
    bool redactPaths{true};
    bool redactUserNames{true};
    bool redactSensitiveText{true};
    bool redactReplayPayloads{true};
};

struct SCPackageSizePolicy
{
    std::uint64_t maxBytes{64ULL * 1024ULL * 1024ULL};
    SCExportOverflowPolicy overflowPolicy{SCExportOverflowPolicy::Fail};
};

using SCExportWriteCallback =
    ErrorCode (*)(void* userData, const void* data, std::size_t size, std::size_t* bytesWritten);

struct SCStreamingExportContext
{
    SCExportWriteCallback write{nullptr};
    void* userData{nullptr};
    std::uint64_t bytesWritten{0};
    bool cancelled{false};
};

struct SCExportRequest
{
    SCExportMode mode{SCExportMode::Normal};
    SCAssetSelection assets{};
    SCRedactionPolicy redaction{};
    SCPackageSizePolicy packageSize{};
    SCStreamingExportContext* stream{nullptr};
};

enum class SCExportStatus
{
    Completed,
    Truncated,
    Cancelled,
    Failed,
};

struct SCExportResult
{
    SCExportStatus status{SCExportStatus::Failed};
    std::uint64_t bytesWritten{0};
    std::wstring failureReason;
};

SCAssetSelection BuildDefaultAssetSelection(SCExportMode mode);
SCRedactionPolicy BuildDefaultRedactionPolicy(SCExportMode mode);
SCPackageSizePolicy BuildDefaultPackageSizePolicy(SCExportMode mode);

ErrorCode WriteExportText(
    SCStreamingExportContext* context,
    const std::string& text,
    const SCPackageSizePolicy& sizePolicy);

ErrorCode WriteExportLine(
    SCStreamingExportContext* context,
    const std::string& text,
    const SCPackageSizePolicy& sizePolicy);

std::string Utf8Encode(const std::wstring& text);
std::wstring ApplyRedaction(const std::wstring& text, const SCRedactionPolicy& policy);

}  // namespace StableCore::Storage
