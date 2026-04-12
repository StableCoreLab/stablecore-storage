#pragma once

#include <vector>

#include "StableCore/Storage/ISCInterfaces.h"

namespace stablecore::storage
{

class ISCComputedContext;
class ISCComputedEvaluator;
class ISCRuleRegistry;
class ISCComputedCache;

using SCComputedEvaluatorPtr = SCRefPtr<ISCComputedEvaluator>;
using SCRuleRegistryPtr = SCRefPtr<ISCRuleRegistry>;
using SCComputedCachePtr = SCRefPtr<ISCComputedCache>;

class ISCComputedContext : public virtual ISCRefObject
{
public:
    virtual ErrorCode GetValue(const wchar_t* fieldName, SCValue* outValue) = 0;
    virtual ErrorCode GetRef(const wchar_t* fieldName, RecordId* outValue) = 0;
    virtual ErrorCode GetRelated(const wchar_t* relationName, SCRecordCursorPtr& outCursor) = 0;
};

class ISCComputedEvaluator : public virtual ISCRefObject
{
public:
    virtual ErrorCode Evaluate(
        const SCComputedColumnDef& column,
        ISCComputedContext* context,
        SCValue* outValue) = 0;
};

class ISCRuleRegistry : public virtual ISCRefObject
{
public:
    virtual ErrorCode Register(const wchar_t* ruleId, ISCComputedEvaluator* evaluator) = 0;
    virtual ErrorCode Find(const wchar_t* ruleId, SCComputedEvaluatorPtr& outEvaluator) = 0;
};

struct SCComputedCacheKey
{
    RecordId recordId{0};
    std::wstring columnName;
    VersionId version{0};

    bool operator==(const SCComputedCacheKey& other) const noexcept
    {
        return recordId == other.recordId
            && columnName == other.columnName
            && version == other.version;
    }
};

struct SCComputedCacheEntry
{
    SCComputedCacheKey key;
    SCComputedDependencySet dependencies;
    SCValue SCValue;
};

class ISCComputedCache : public virtual ISCRefObject
{
public:
    virtual ErrorCode TryGet(const SCComputedCacheKey& key, SCValue* outValue) = 0;
    virtual ErrorCode Put(const SCComputedCacheEntry& entry) = 0;
    virtual ErrorCode Invalidate(const SCChangeSet& SCChangeSet, const std::vector<SCComputedColumnDef>& computedColumns) = 0;
    virtual ErrorCode Clear() = 0;
};

ErrorCode CreateDefaultExpressionEvaluator(SCComputedEvaluatorPtr& outEvaluator);
ErrorCode CreateDefaultRuleRegistry(SCRuleRegistryPtr& outRegistry);
ErrorCode CreateComputedCache(SCComputedCachePtr& outCache);

ErrorCode EvaluateComputedColumn(
    const SCComputedColumnDef& column,
    ISCComputedContext* context,
    ISCRuleRegistry* ruleRegistry,
    SCValue* outValue);

bool DoesDependencySetIntersect(const SCComputedDependencySet& dependencies, const SCChangeSet& SCChangeSet) noexcept;

}  // namespace stablecore::storage
