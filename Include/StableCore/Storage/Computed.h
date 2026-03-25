#pragma once

#include <vector>

#include "StableCore/Storage/Interfaces.h"

namespace stablecore::storage
{

class IComputedContext;
class IComputedEvaluator;
class IRuleRegistry;
class IComputedCache;

using ComputedEvaluatorPtr = RefPtr<IComputedEvaluator>;
using RuleRegistryPtr = RefPtr<IRuleRegistry>;
using ComputedCachePtr = RefPtr<IComputedCache>;

class IComputedContext : public virtual IRefObject
{
public:
    virtual ErrorCode GetValue(const wchar_t* fieldName, Value* outValue) = 0;
    virtual ErrorCode GetRef(const wchar_t* fieldName, RecordId* outValue) = 0;
    virtual ErrorCode GetRelated(const wchar_t* relationName, RecordCursorPtr& outCursor) = 0;
};

class IComputedEvaluator : public virtual IRefObject
{
public:
    virtual ErrorCode Evaluate(
        const ComputedColumnDef& column,
        IComputedContext* context,
        Value* outValue) = 0;
};

class IRuleRegistry : public virtual IRefObject
{
public:
    virtual ErrorCode Register(const wchar_t* ruleId, IComputedEvaluator* evaluator) = 0;
    virtual ErrorCode Find(const wchar_t* ruleId, ComputedEvaluatorPtr& outEvaluator) = 0;
};

struct ComputedCacheKey
{
    RecordId recordId{0};
    std::wstring columnName;
    VersionId version{0};
};

struct ComputedCacheEntry
{
    ComputedCacheKey key;
    ComputedDependencySet dependencies;
    Value value;
};

class IComputedCache : public virtual IRefObject
{
public:
    virtual ErrorCode TryGet(const ComputedCacheKey& key, Value* outValue) = 0;
    virtual ErrorCode Put(const ComputedCacheEntry& entry) = 0;
    virtual ErrorCode Invalidate(const ChangeSet& changeSet, const std::vector<ComputedColumnDef>& computedColumns) = 0;
    virtual ErrorCode Clear() = 0;
};

ErrorCode CreateDefaultExpressionEvaluator(ComputedEvaluatorPtr& outEvaluator);
ErrorCode CreateDefaultRuleRegistry(RuleRegistryPtr& outRegistry);
ErrorCode CreateComputedCache(ComputedCachePtr& outCache);

ErrorCode EvaluateComputedColumn(
    const ComputedColumnDef& column,
    IComputedContext* context,
    IRuleRegistry* ruleRegistry,
    Value* outValue);

bool DoesDependencySetIntersect(const ComputedDependencySet& dependencies, const ChangeSet& changeSet) noexcept;

}  // namespace stablecore::storage
