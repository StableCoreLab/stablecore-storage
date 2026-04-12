#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "StableCore/Storage/SCStorage.h"

namespace sc = StableCore::Storage;

namespace
{

class TestComputedContext final : public sc::ISCComputedContext, public sc::SCRefCountedObject
{
public:
    std::unordered_map<std::wstring, sc::SCValue> values;

    sc::ErrorCode GetValue(const wchar_t* fieldName, sc::SCValue* outValue) override
    {
        if (fieldName == nullptr || outValue == nullptr)
        {
            return sc::SC_E_POINTER;
        }
        const auto it = values.find(fieldName);
        if (it == values.end())
        {
            return sc::SC_E_COLUMN_NOT_FOUND;
        }
        *outValue = it->second;
        return it->second.IsNull() ? sc::SC_E_VALUE_IS_NULL : sc::SC_OK;
    }

    sc::ErrorCode GetRef(const wchar_t*, sc::RecordId*) override
    {
        return sc::SC_E_NOTIMPL;
    }

    sc::ErrorCode GetRelated(const wchar_t*, sc::SCRecordCursorPtr&) override
    {
        return sc::SC_E_NOTIMPL;
    }
};

class ConstantRuleEvaluator final : public sc::ISCComputedEvaluator, public sc::SCRefCountedObject
{
public:
    explicit ConstantRuleEvaluator(double SCValue)
        : value_(SCValue)
    {
    }

    sc::ErrorCode Evaluate(const sc::SCComputedColumnDef&, sc::ISCComputedContext*, sc::SCValue* outValue) override
    {
        if (outValue == nullptr)
        {
            return sc::SC_E_POINTER;
        }
        *outValue = sc::SCValue::FromDouble(value_);
        return sc::SC_OK;
    }

private:
    double value_{0.0};
};

}  // namespace

TEST(StorageComputed, ExpressionEvaluatorSupportsArithmeticAndFunctions)
{
    sc::SCComputedColumnDef column;
    column.name = L"Volume";
    column.valueKind = sc::ValueKind::Double;
    column.kind = sc::ComputedFieldKind::Expression;
    column.expression = L"max(Length * Width * Height, 0)";

    sc::SCRefPtr<TestComputedContext> context = sc::SCMakeRef<TestComputedContext>();
    context->values[L"Length"] = sc::SCValue::FromDouble(6.0);
    context->values[L"Width"] = sc::SCValue::FromDouble(0.3);
    context->values[L"Height"] = sc::SCValue::FromDouble(0.5);

    sc::SCValue SCValue;
    EXPECT_EQ(sc::EvaluateComputedColumn(column, context.Get(), nullptr, &SCValue), sc::SC_OK);

    double result = 0.0;
    EXPECT_EQ(SCValue.AsDouble(&result), sc::SC_OK);
    EXPECT_DOUBLE_EQ(result, 0.9);
}

TEST(StorageComputed, RuleRegistryResolvesRuleEvaluator)
{
    sc::SCRuleRegistryPtr registry;
    EXPECT_EQ(sc::CreateDefaultRuleRegistry(registry), sc::SC_OK);

    sc::SCComputedEvaluatorPtr evaluator = sc::SCMakeRef<ConstantRuleEvaluator>(42.5);
    EXPECT_EQ(registry->Register(L"beam.volume.v1", evaluator.Get()), sc::SC_OK);

    sc::SCComputedColumnDef column;
    column.name = L"Volume";
    column.valueKind = sc::ValueKind::Double;
    column.kind = sc::ComputedFieldKind::Rule;
    column.ruleId = L"beam.volume.v1";

    sc::SCRefPtr<TestComputedContext> context = sc::SCMakeRef<TestComputedContext>();
    sc::SCValue SCValue;
    EXPECT_EQ(sc::EvaluateComputedColumn(column, context.Get(), registry.Get(), &SCValue), sc::SC_OK);

    double result = 0.0;
    EXPECT_EQ(SCValue.AsDouble(&result), sc::SC_OK);
    EXPECT_DOUBLE_EQ(result, 42.5);
}

TEST(StorageComputed, CacheInvalidatesOnDependencyChanges)
{
    sc::SCComputedCachePtr cache;
    EXPECT_EQ(sc::CreateComputedCache(cache), sc::SC_OK);

    sc::SCComputedCacheEntry entry;
    entry.key.recordId = 1001;
    entry.key.columnName = L"Volume";
    entry.key.version = 8;
    entry.dependencies.factFields = {{L"Beam", L"Length"}};
    entry.SCValue = sc::SCValue::FromDouble(9.0);
    EXPECT_EQ(cache->Put(entry), sc::SC_OK);

    sc::SCComputedColumnDef column;
    column.name = L"Volume";
    column.dependencies.factFields = {{L"Beam", L"Length"}};

    sc::SCChangeSet SCChangeSet;
    SCChangeSet.version = 9;
    SCChangeSet.changes.push_back(sc::SCDataChange{
        sc::ChangeKind::FieldUpdated,
        L"Beam",
        1001,
        L"Length",
        sc::SCValue::FromDouble(3.0),
        sc::SCValue::FromDouble(6.0),
        false,
        false,
    });

    EXPECT_EQ(cache->Invalidate(SCChangeSet, {column}), sc::SC_OK);

    sc::SCValue cached;
    EXPECT_EQ(cache->TryGet(entry.key, &cached), sc::SC_FALSE_RESULT);
}
