#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "StableCore/Storage/Storage.h"

namespace sc = stablecore::storage;

namespace
{

class TestComputedContext final : public sc::IComputedContext, public sc::RefCountedObject
{
public:
    std::unordered_map<std::wstring, sc::Value> values;

    sc::ErrorCode GetValue(const wchar_t* fieldName, sc::Value* outValue) override
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

    sc::ErrorCode GetRelated(const wchar_t*, sc::RecordCursorPtr&) override
    {
        return sc::SC_E_NOTIMPL;
    }
};

class ConstantRuleEvaluator final : public sc::IComputedEvaluator, public sc::RefCountedObject
{
public:
    explicit ConstantRuleEvaluator(double value)
        : value_(value)
    {
    }

    sc::ErrorCode Evaluate(const sc::ComputedColumnDef&, sc::IComputedContext*, sc::Value* outValue) override
    {
        if (outValue == nullptr)
        {
            return sc::SC_E_POINTER;
        }
        *outValue = sc::Value::FromDouble(value_);
        return sc::SC_OK;
    }

private:
    double value_{0.0};
};

}  // namespace

TEST(StorageComputed, ExpressionEvaluatorSupportsArithmeticAndFunctions)
{
    sc::ComputedColumnDef column;
    column.name = L"Volume";
    column.valueKind = sc::ValueKind::Double;
    column.kind = sc::ComputedFieldKind::Expression;
    column.expression = L"max(Length * Width * Height, 0)";

    sc::RefPtr<TestComputedContext> context = sc::MakeRef<TestComputedContext>();
    context->values[L"Length"] = sc::Value::FromDouble(6.0);
    context->values[L"Width"] = sc::Value::FromDouble(0.3);
    context->values[L"Height"] = sc::Value::FromDouble(0.5);

    sc::Value value;
    EXPECT_EQ(sc::EvaluateComputedColumn(column, context.Get(), nullptr, &value), sc::SC_OK);

    double result = 0.0;
    EXPECT_EQ(value.AsDouble(&result), sc::SC_OK);
    EXPECT_DOUBLE_EQ(result, 0.9);
}

TEST(StorageComputed, RuleRegistryResolvesRuleEvaluator)
{
    sc::RuleRegistryPtr registry;
    EXPECT_EQ(sc::CreateDefaultRuleRegistry(registry), sc::SC_OK);

    sc::ComputedEvaluatorPtr evaluator = sc::MakeRef<ConstantRuleEvaluator>(42.5);
    EXPECT_EQ(registry->Register(L"beam.volume.v1", evaluator.Get()), sc::SC_OK);

    sc::ComputedColumnDef column;
    column.name = L"Volume";
    column.valueKind = sc::ValueKind::Double;
    column.kind = sc::ComputedFieldKind::Rule;
    column.ruleId = L"beam.volume.v1";

    sc::RefPtr<TestComputedContext> context = sc::MakeRef<TestComputedContext>();
    sc::Value value;
    EXPECT_EQ(sc::EvaluateComputedColumn(column, context.Get(), registry.Get(), &value), sc::SC_OK);

    double result = 0.0;
    EXPECT_EQ(value.AsDouble(&result), sc::SC_OK);
    EXPECT_DOUBLE_EQ(result, 42.5);
}

TEST(StorageComputed, CacheInvalidatesOnDependencyChanges)
{
    sc::ComputedCachePtr cache;
    EXPECT_EQ(sc::CreateComputedCache(cache), sc::SC_OK);

    sc::ComputedCacheEntry entry;
    entry.key.recordId = 1001;
    entry.key.columnName = L"Volume";
    entry.key.version = 8;
    entry.dependencies.factFields = {{L"Beam", L"Length"}};
    entry.value = sc::Value::FromDouble(9.0);
    EXPECT_EQ(cache->Put(entry), sc::SC_OK);

    sc::ComputedColumnDef column;
    column.name = L"Volume";
    column.dependencies.factFields = {{L"Beam", L"Length"}};

    sc::ChangeSet changeSet;
    changeSet.version = 9;
    changeSet.changes.push_back(sc::DataChange{
        sc::ChangeKind::FieldUpdated,
        L"Beam",
        1001,
        L"Length",
        sc::Value::FromDouble(3.0),
        sc::Value::FromDouble(6.0),
        false,
        false,
    });

    EXPECT_EQ(cache->Invalidate(changeSet, {column}), sc::SC_OK);

    sc::Value cached;
    EXPECT_EQ(cache->TryGet(entry.key, &cached), sc::SC_FALSE_RESULT);
}
