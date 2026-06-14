#include <vector>

#include <gtest/gtest.h>

#include "SCStorage.h"

#include "Support/TestFixtures.h"

namespace sc = StableCore::Storage;

namespace
{
    struct RecordingObserver final : sc::ISCDatabaseObserver
    {
        void OnDatabaseChanged(const sc::SCChangeSet& changeSet) override
        {
            seen.push_back(changeSet);
        }

        std::vector<sc::SCChangeSet> seen;
    };
}

// 迁移自 M1Tests.cpp - RelationFieldValidationAndChangeSet
// 测试关系字段验证和变更集
TEST_F(TwoTableTest, RelationFieldValidationAndChangeSet)
{
    RecordingObserver observer;
    EXPECT_EQ(db()->AddObserver(&observer), sc::SC_OK);

    sc::SCEditPtr createEdit;
    EXPECT_EQ(db()->BeginEdit(L"seed", createEdit), sc::SC_OK);
    sc::SCRecordPtr floor;
    EXPECT_EQ(floorTable()->CreateRecord(floor), sc::SC_OK);
    EXPECT_EQ(floor->SetString(L"Name", L"2F"), sc::SC_OK);

    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable()->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(beam->SetValue(L"FloorRef", sc::SCValue::FromRecordId(floor->GetId())), sc::SC_OK);
    EXPECT_EQ(db()->Commit(createEdit.Get()), sc::SC_OK);

    EXPECT_FALSE(observer.seen.empty());
    const sc::SCChangeSet& changeSet = observer.seen.back();

    bool sawRelationUpdate = false;
    for (const auto& change : changeSet.changes)
    {
        if (change.kind == sc::ChangeKind::RelationUpdated && change.fieldName == L"FloorRef")
        {
            sawRelationUpdate = true;
        }
    }
    EXPECT_TRUE(sawRelationUpdate);

    EXPECT_EQ(db()->RemoveObserver(&observer), sc::SC_OK);
}

// 新增测试：关系字段可以设置为空
TEST_F(TwoTableTest, RelationFieldCanBeSetToNull)
{
    sc::SCEditPtr createEdit;
    EXPECT_EQ(db()->BeginEdit(L"seed", createEdit), sc::SC_OK);

    sc::SCRecordPtr floor;
    EXPECT_EQ(floorTable()->CreateRecord(floor), sc::SC_OK);
    EXPECT_EQ(floor->SetString(L"Name", L"1F"), sc::SC_OK);

    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable()->CreateRecord(beam), sc::SC_OK);
    EXPECT_EQ(beam->SetRef(L"FloorRef", floor->GetId()), sc::SC_OK);

    EXPECT_EQ(db()->Commit(createEdit.Get()), sc::SC_OK);

    // 验证关系已设置
    sc::RecordId refId = 0;
    EXPECT_EQ(beam->GetRef(L"FloorRef", &refId), sc::SC_OK);
    EXPECT_EQ(refId, floor->GetId());

    // 设置为空（在新的编辑上下文中）
    sc::SCEditPtr clearEdit;
    EXPECT_EQ(db()->BeginEdit(L"clear", clearEdit), sc::SC_OK);
    // 需要重新获取记录引用
    sc::SCRecordPtr beamReloaded;
    EXPECT_EQ(beamTable()->GetRecord(beam->GetId(), beamReloaded), sc::SC_OK);
    EXPECT_EQ(beamReloaded->SetValue(L"FloorRef", sc::SCValue::Null()), sc::SC_OK);
    EXPECT_EQ(db()->Commit(clearEdit.Get()), sc::SC_OK);

    // 验证关系已清除
    EXPECT_EQ(beamReloaded->GetRef(L"FloorRef", &refId), sc::SC_E_VALUE_IS_NULL);
}

// 新增测试：关系字段引用不存在的记录
TEST_F(TwoTableTest, RelationFieldRejectsInvalidReference)
{
    sc::SCEditPtr createEdit;
    EXPECT_EQ(db()->BeginEdit(L"seed", createEdit), sc::SC_OK);

    sc::SCRecordPtr beam;
    EXPECT_EQ(beamTable()->CreateRecord(beam), sc::SC_OK);

    // 尝试设置一个不存在的记录 ID
    EXPECT_EQ(beam->SetValue(L"FloorRef", sc::SCValue::FromRecordId(999)), sc::SC_E_REFERENCE_INVALID);

    EXPECT_EQ(db()->Rollback(createEdit.Get()), sc::SC_OK);
}

// 新增测试：关系字段的查询
TEST_F(TwoTableTest, RelationFieldQuery)
{
    sc::SCEditPtr createEdit;
    EXPECT_EQ(db()->BeginEdit(L"seed", createEdit), sc::SC_OK);

    sc::SCRecordPtr floor1;
    EXPECT_EQ(floorTable()->CreateRecord(floor1), sc::SC_OK);
    EXPECT_EQ(floor1->SetString(L"Name", L"1F"), sc::SC_OK);

    sc::SCRecordPtr floor2;
    EXPECT_EQ(floorTable()->CreateRecord(floor2), sc::SC_OK);
    EXPECT_EQ(floor2->SetString(L"Name", L"2F"), sc::SC_OK);

    sc::SCRecordPtr beam1;
    EXPECT_EQ(beamTable()->CreateRecord(beam1), sc::SC_OK);
    EXPECT_EQ(beam1->SetValue(L"FloorRef", sc::SCValue::FromRecordId(floor1->GetId())), sc::SC_OK);

    sc::SCRecordPtr beam2;
    EXPECT_EQ(beamTable()->CreateRecord(beam2), sc::SC_OK);
    EXPECT_EQ(beam2->SetValue(L"FloorRef", sc::SCValue::FromRecordId(floor1->GetId())), sc::SC_OK);

    sc::SCRecordPtr beam3;
    EXPECT_EQ(beamTable()->CreateRecord(beam3), sc::SC_OK);
    EXPECT_EQ(beam3->SetValue(L"FloorRef", sc::SCValue::FromRecordId(floor2->GetId())), sc::SC_OK);

    EXPECT_EQ(db()->Commit(createEdit.Get()), sc::SC_OK);

    // 查询属于 floor1 的梁
    sc::SCRecordCursorPtr cursor;
    sc::SCQueryCondition condition{L"FloorRef", sc::SCValue::FromRecordId(floor1->GetId())};
    EXPECT_EQ(beamTable()->FindRecords(condition, cursor), sc::SC_OK);

    int count = 0;
    sc::SCRecordPtr record;
    while (cursor->Next(record) == sc::SC_OK && static_cast<bool>(record))
    {
        count++;
    }
    EXPECT_EQ(count, 2);
}