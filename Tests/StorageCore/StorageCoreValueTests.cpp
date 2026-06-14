#include <gtest/gtest.h>

#include "SCStorage.h"

#include "Support/TestFixtures.h"

namespace sc = StableCore::Storage;

// 迁移自 M1Tests.cpp - ValueTypedAccess
// 测试值类型的基本访问功能
TEST(StorageCoreValue, ValueTypedAccess)
{
    sc::SCValue value = sc::SCValue::FromRecordId(42);
    sc::RecordId id = 0;
    EXPECT_EQ(value.AsRecordId(&id), sc::SC_OK);
    EXPECT_EQ(id, 42);

    std::wstring text;
    EXPECT_EQ(sc::SCValue::Null().AsStringCopy(&text), sc::SC_E_VALUE_IS_NULL);
}

// 迁移自 M1Tests.cpp - GetStringCopyAndDefaultValue
// 测试字符串值的获取和默认值处理
// 使用 CompositeBeamQueryTest 夹具，它提供 Width 和 Name 列
TEST_F(CompositeBeamQueryTest, GetStringCopyAndDefaultValue)
{
    sc::SCEditPtr edit;
    EXPECT_EQ(db()->BeginEdit(L"seed", edit), sc::SC_OK);
    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable()->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(db()->Commit(edit.Get()), sc::SC_OK);

    std::int64_t widthValue = 0;
    EXPECT_EQ(beam->GetInt64(L"Width", &widthValue), sc::SC_OK);
    EXPECT_EQ(widthValue, 0);

    std::wstring nameValue;
    EXPECT_EQ(beam->GetStringCopy(L"Name", &nameValue), sc::SC_OK);
    EXPECT_EQ(nameValue, L"");
}

// 新增测试：整数值类型的边界情况
TEST(StorageCoreValue, Int64ValueBoundary)
{
    sc::SCValue minValue = sc::SCValue::FromInt64(INT64_MIN);
    sc::SCValue maxValue = sc::SCValue::FromInt64(INT64_MAX);

    std::int64_t minResult = 0;
    std::int64_t maxResult = 0;

    EXPECT_EQ(minValue.AsInt64(&minResult), sc::SC_OK);
    EXPECT_EQ(maxValue.AsInt64(&maxResult), sc::SC_OK);

    EXPECT_EQ(minResult, INT64_MIN);
    EXPECT_EQ(maxResult, INT64_MAX);
}

// 新增测试：布尔值类型
TEST(StorageCoreValue, BoolValueAccess)
{
    sc::SCValue trueValue = sc::SCValue::FromBool(true);
    sc::SCValue falseValue = sc::SCValue::FromBool(false);

    bool trueResult = false;
    bool falseResult = true;

    EXPECT_EQ(trueValue.AsBool(&trueResult), sc::SC_OK);
    EXPECT_EQ(falseValue.AsBool(&falseResult), sc::SC_OK);

    EXPECT_TRUE(trueResult);
    EXPECT_FALSE(falseResult);
}

// 新增测试：浮点数值类型
TEST(StorageCoreValue, DoubleValueAccess)
{
    const double testValue = 3.14159265358979;
    sc::SCValue value = sc::SCValue::FromDouble(testValue);

    double result = 0.0;
    EXPECT_EQ(value.AsDouble(&result), sc::SC_OK);

    EXPECT_NEAR(result, testValue, 1e-15);
}

// 新增测试：空值的类型检查
TEST(StorageCoreValue, NullValueHandling)
{
    sc::SCValue nullValue = sc::SCValue::Null();

    EXPECT_TRUE(nullValue.IsNull());

    std::int64_t intResult = 0;
    EXPECT_EQ(nullValue.AsInt64(&intResult), sc::SC_E_VALUE_IS_NULL);

    double doubleResult = 0.0;
    EXPECT_EQ(nullValue.AsDouble(&doubleResult), sc::SC_E_VALUE_IS_NULL);

    bool boolResult = false;
    EXPECT_EQ(nullValue.AsBool(&boolResult), sc::SC_E_VALUE_IS_NULL);

    std::wstring stringResult;
    EXPECT_EQ(nullValue.AsStringCopy(&stringResult), sc::SC_E_VALUE_IS_NULL);

    sc::RecordId recordIdResult = 0;
    EXPECT_EQ(nullValue.AsRecordId(&recordIdResult), sc::SC_E_VALUE_IS_NULL);
}