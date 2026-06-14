#include "TestSeedData.h"
#include <gtest/gtest.h>

void SeedQueryableBeamRows(const sc::SCTablePtr& table, sc::SCDbPtr& db)
{
    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpha"), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 200), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Beta"), sc::SC_OK);

    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", 300), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", L"Alpine"), sc::SC_OK);

    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);
}

void SeedSingleBeam(const sc::SCTablePtr& table, sc::SCDbPtr& db, const wchar_t* name, std::int64_t width)
{
    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"seed", edit), sc::SC_OK);

    sc::SCRecordPtr row;
    EXPECT_EQ(table->CreateRecord(row), sc::SC_OK);
    EXPECT_EQ(row->SetInt64(L"Width", width), sc::SC_OK);
    EXPECT_EQ(row->SetString(L"Name", name), sc::SC_OK);

    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);
}
