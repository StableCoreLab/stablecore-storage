#include "ISCDebugPackage.h"

#include <codecvt>
#include <locale>

namespace StableCore::Storage
{
namespace
{

ErrorCode WriteUtf8Buffer(SCStreamingExportContext* context, const std::string& text, const SCPackageSizePolicy& sizePolicy)
{
    if (context == nullptr || context->write == nullptr)
    {
        return SC_E_EXPORT_INVALID_STATE;
    }
    if (context->cancelled)
    {
        return SC_E_EXPORT_CANCELLED;
    }

    const std::uint64_t currentBytes = context->bytesWritten;
    const std::uint64_t requestBytes = static_cast<std::uint64_t>(text.size());
    if (sizePolicy.maxBytes > 0 && currentBytes + requestBytes > sizePolicy.maxBytes)
    {
        if (sizePolicy.overflowPolicy == SCExportOverflowPolicy::Truncate)
        {
            const std::uint64_t remain = sizePolicy.maxBytes > currentBytes ? sizePolicy.maxBytes - currentBytes : 0;
            if (remain == 0)
            {
                context->cancelled = true;
                return SC_E_EXPORT_TOO_LARGE;
            }

            std::size_t written = 0;
            const ErrorCode rc = context->write(context->userData, text.data(), static_cast<std::size_t>(remain), &written);
            if (Failed(rc))
            {
                return rc;
            }
            context->bytesWritten += static_cast<std::uint64_t>(written);
            context->cancelled = true;
            return SC_E_EXPORT_TOO_LARGE;
        }

        context->cancelled = true;
        return SC_E_EXPORT_TOO_LARGE;
    }

    std::size_t written = 0;
    const ErrorCode rc = context->write(context->userData, text.data(), text.size(), &written);
    if (Failed(rc))
    {
        return rc;
    }
    context->bytesWritten += static_cast<std::uint64_t>(written);
    if (written != text.size())
    {
        context->cancelled = true;
        return SC_E_EXPORT_WRITE_FAILED;
    }
    return SC_OK;
}

std::wstring RedactSensitiveText(const std::wstring& text)
{
    if (text.empty())
    {
        return text;
    }
    return std::wstring(text.size(), L'*');
}

}  // namespace

SCAssetSelection BuildDefaultAssetSelection(SCExportMode mode)
{
    SCAssetSelection selection;
    if (mode == SCExportMode::Normal)
    {
        return selection;
    }

    selection.includeDiagnostics = true;
    return selection;
}

SCRedactionPolicy BuildDefaultRedactionPolicy(SCExportMode mode)
{
    SCRedactionPolicy policy;
    if (mode == SCExportMode::Normal)
    {
        policy.redactPaths = false;
        policy.redactUserNames = false;
        policy.redactSensitiveText = false;
        policy.redactReplayPayloads = true;
        return policy;
    }

    policy.redactPaths = true;
    policy.redactUserNames = true;
    policy.redactSensitiveText = true;
    policy.redactReplayPayloads = true;
    return policy;
}

SCPackageSizePolicy BuildDefaultPackageSizePolicy(SCExportMode mode)
{
    SCPackageSizePolicy policy;
    policy.maxBytes = mode == SCExportMode::Normal ? 16ULL * 1024ULL * 1024ULL : 64ULL * 1024ULL * 1024ULL;
    policy.overflowPolicy = SCExportOverflowPolicy::Fail;
    return policy;
}

ErrorCode WriteExportText(
    SCStreamingExportContext* context,
    const std::string& text,
    const SCPackageSizePolicy& sizePolicy)
{
    return WriteUtf8Buffer(context, text, sizePolicy);
}

ErrorCode WriteExportLine(
    SCStreamingExportContext* context,
    const std::string& text,
    const SCPackageSizePolicy& sizePolicy)
{
    ErrorCode rc = WriteUtf8Buffer(context, text, sizePolicy);
    if (Failed(rc))
    {
        return rc;
    }
    return WriteUtf8Buffer(context, std::string("\n"), sizePolicy);
}

std::string Utf8Encode(const std::wstring& text)
{
    if (text.empty())
    {
        return {};
    }

    std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> convert;
    return convert.to_bytes(text);
}

std::wstring ApplyRedaction(const std::wstring& text, const SCRedactionPolicy& policy)
{
    if (!policy.redactSensitiveText)
    {
        return text;
    }

    return RedactSensitiveText(text);
}

}  // namespace StableCore::Storage
