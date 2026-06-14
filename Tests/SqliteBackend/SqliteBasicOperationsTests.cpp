#include <filesystem>
#include <vector>

#include <gtest/gtest.h>

#include "SCStorage.h"

#include "Support/TestFixtures.h"

namespace sc = StableCore::Storage;
namespace fs = std::filesystem;

TEST_F(BeamTableTest, PersistedRecordSurvivesReopen)
{
    {
        sc::SCEditPtr edit;
        EXPECT_EQ(db()->BeginEdit(L"seed", edit), sc::SC_OK);

        sc::SCRecordPtr beam;
        EXPECT_EQ(beamTable()->CreateRecord(beam), sc::SC_OK);
        EXPECT_EQ(beam->SetInt64(L"Width", 320), sc::SC_OK);
        EXPECT_EQ(db()->Commit(edit.Get()), sc::SC_OK);
    }

    {
        sc::SCDbPtr reopened;
        EXPECT_EQ(sc::CreateFileDatabase(dbPath().c_str(), sc::SCOpenDatabaseOptions{}, reopened), sc::SC_OK);

        sc::SCTablePtr beamTable;
        EXPECT_EQ(reopened->GetTable(L"Beam", beamTable), sc::SC_OK);

        sc::SCRecordCursorPtr cursor;
        EXPECT_EQ(beamTable->EnumerateRecords(cursor), sc::SC_OK);

        sc::SCRecordPtr beam;
        EXPECT_EQ(cursor->Next(beam), sc::SC_OK);
        EXPECT_TRUE(static_cast<bool>(beam));

        std::int64_t width = 0;
        EXPECT_EQ(beam->GetInt64(L"Width", &width), sc::SC_OK);
        EXPECT_EQ(width, 320);
    }
}

TEST_F(BeamTableTest, PersistedQueryAndDelete)
{
    {
        sc::SCEditPtr edit;
        EXPECT_EQ(db()->BeginEdit(L"seed", edit), sc::SC_OK);

        sc::SCRecordPtr beamA;
        EXPECT_EQ(beamTable()->CreateRecord(beamA), sc::SC_OK);
        EXPECT_EQ(beamA->SetInt64(L"Width", 200), sc::SC_OK);

        sc::SCRecordPtr beamB;
        EXPECT_EQ(beamTable()->CreateRecord(beamB), sc::SC_OK);
        EXPECT_EQ(beamB->SetInt64(L"Width", 500), sc::SC_OK);

        EXPECT_EQ(db()->Commit(edit.Get()), sc::SC_OK);

        sc::SCEditPtr deleteEdit;
        EXPECT_EQ(db()->BeginEdit(L"delete", deleteEdit), sc::SC_OK);
        EXPECT_EQ(beamTable()->DeleteRecord(beamA->GetId()), sc::SC_OK);
        EXPECT_EQ(db()->Commit(deleteEdit.Get()), sc::SC_OK);
    }

    {
        sc::SCDbPtr reopened;
        EXPECT_EQ(sc::CreateFileDatabase(dbPath().c_str(), sc::SCOpenDatabaseOptions{}, reopened), sc::SC_OK);
        sc::SCTablePtr beamTable;
        EXPECT_EQ(reopened->GetTable(L"Beam", beamTable), sc::SC_OK);

        sc::SCRecordCursorPtr cursor;
        sc::SCQueryCondition condition{L"Width", sc::SCValue::FromInt64(500)};
        EXPECT_EQ(beamTable->FindRecords(condition, cursor), sc::SC_OK);

        sc::SCRecordPtr beam;
        EXPECT_EQ(cursor->Next(beam), sc::SC_OK);
        EXPECT_TRUE(static_cast<bool>(beam));

        std::int64_t width = 0;
        EXPECT_EQ(beam->GetInt64(L"Width", &width), sc::SC_OK);
        EXPECT_EQ(width, 500);
    }
}

TEST_F(BeamTableTest, PersistedEmptyQueryIsNotError)
{
    sc::SCRecordCursorPtr cursor;
    EXPECT_EQ(beamTable()->FindRecords({L"Width", sc::SCValue::FromInt64(12345)}, cursor), sc::SC_OK);

    sc::SCRecordPtr beam;
    EXPECT_EQ(cursor->Next(beam), sc::SC_OK);
    EXPECT_FALSE(static_cast<bool>(beam));
}

TEST_F(BeamTableTest, BinaryFieldPersistsAcrossReopen)
{
    sc::RecordId beamId = 0;
    const std::vector<std::uint8_t> payload{0x00, 0x10, 0x7F, 0xFF};

    {
        sc::SCSchemaPtr schema;
        EXPECT_EQ(beamTable()->GetSchema(schema), sc::SC_OK);

        sc::SCColumnDef attachment;
        attachment.name = L"Attachment";
        attachment.displayName = L"Attachment";
        attachment.valueKind = sc::ValueKind::Binary;
        attachment.nullable = true;
        EXPECT_EQ(schema->AddColumn(attachment), sc::SC_OK);

        sc::SCEditPtr edit;
        EXPECT_EQ(db()->BeginEdit(L"seed binary", edit), sc::SC_OK);

        sc::SCRecordPtr beam;
        EXPECT_EQ(beamTable()->CreateRecord(beam), sc::SC_OK);
        beamId = beam->GetId();
        EXPECT_EQ(beam->SetBinary(L"Attachment", payload.data(), payload.size()), sc::SC_OK);
        EXPECT_EQ(db()->Commit(edit.Get()), sc::SC_OK);
    }

    {
        sc::SCDbPtr db;
        EXPECT_EQ(sc::CreateFileDatabase(dbPath().c_str(), sc::SCOpenDatabaseOptions{}, db), sc::SC_OK);

        sc::SCTablePtr table;
        EXPECT_EQ(db->GetTable(L"Beam", table), sc::SC_OK);

        sc::SCRecordPtr record;
        EXPECT_EQ(table->GetRecord(beamId, record), sc::SC_OK);

        std::vector<std::uint8_t> loaded;
        EXPECT_EQ(record->GetBinaryCopy(L"Attachment", &loaded), sc::SC_OK);
        EXPECT_EQ(loaded, payload);
    }
}

TEST_F(BeamTableTest, StringAndBinaryGettersExposeStableBackingStorage)
{
    const std::vector<std::uint8_t> explicitBinary{0xAA, 0xBB, 0xCC};
    const std::vector<std::uint8_t> defaultBinary{0x10, 0x20};

    sc::SCSchemaPtr schema;
    EXPECT_EQ(beamTable()->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef name;
    name.name = L"Name";
    name.displayName = L"Name";
    name.valueKind = sc::ValueKind::String;
    name.nullable = false;
    name.defaultValue = sc::SCValue::FromString(L"default-name");
    EXPECT_EQ(schema->AddColumn(name), sc::SC_OK);

    sc::SCColumnDef attachment;
    attachment.name = L"Attachment";
    attachment.displayName = L"Attachment";
    attachment.valueKind = sc::ValueKind::Binary;
    attachment.nullable = false;
    attachment.defaultValue = sc::SCValue::FromBinary(defaultBinary);
    EXPECT_EQ(schema->AddColumn(attachment), sc::SC_OK);

    sc::RecordId explicitRecordId = 0;
    sc::RecordId defaultRecordId = 0;
    {
        sc::SCEditPtr edit;
        EXPECT_EQ(db()->BeginEdit(L"seed", edit), sc::SC_OK);

        sc::SCRecordPtr explicitRecord;
        EXPECT_EQ(beamTable()->CreateRecord(explicitRecord), sc::SC_OK);
        explicitRecordId = explicitRecord->GetId();
        EXPECT_EQ(explicitRecord->SetString(L"Name", L"explicit-name"), sc::SC_OK);
        EXPECT_EQ(explicitRecord->SetBinary(L"Attachment", explicitBinary.data(), explicitBinary.size()), sc::SC_OK);

        sc::SCRecordPtr defaultRecord;
        EXPECT_EQ(beamTable()->CreateRecord(defaultRecord), sc::SC_OK);
        defaultRecordId = defaultRecord->GetId();

        EXPECT_EQ(db()->Commit(edit.Get()), sc::SC_OK);
    }

    const auto verifyRecord = [&](sc::RecordId recordId,
                                  const wchar_t* expectedName,
                                  const std::vector<std::uint8_t>& expectedBinary) {
        sc::SCRecordPtr record;
        ASSERT_EQ(beamTable()->GetRecord(recordId, record), sc::SC_OK);

        const wchar_t* namePtr = nullptr;
        ASSERT_EQ(record->GetString(L"Name", &namePtr), sc::SC_OK);
        ASSERT_NE(namePtr, nullptr);

        const std::uint8_t* binaryPtr = nullptr;
        std::size_t binarySize = 0;
        ASSERT_EQ(record->GetBinary(L"Attachment", &binaryPtr, &binarySize), sc::SC_OK);
        ASSERT_EQ(binarySize, expectedBinary.size());
        ASSERT_TRUE(binaryPtr != nullptr || expectedBinary.empty());

        std::wstring copiedName;
        EXPECT_EQ(record->GetStringCopy(L"Name", &copiedName), sc::SC_OK);
        EXPECT_EQ(copiedName, expectedName);
        EXPECT_EQ(std::wstring(namePtr), expectedName);

        std::vector<std::uint8_t> copiedBinary;
        EXPECT_EQ(record->GetBinaryCopy(L"Attachment", &copiedBinary), sc::SC_OK);
        EXPECT_EQ(copiedBinary, expectedBinary);
        if (expectedBinary.empty())
        {
            EXPECT_EQ(binaryPtr, nullptr);
        } else
        {
            EXPECT_TRUE(std::equal(binaryPtr, binaryPtr + binarySize, expectedBinary.begin(), expectedBinary.end()));
        }
    };

    verifyRecord(explicitRecordId, L"explicit-name", explicitBinary);
    verifyRecord(defaultRecordId, L"default-name", defaultBinary);
}

TEST_F(FileDbTest, PersistedSchemaRejectsInvalidReferenceTableUsage)
{
    sc::SCTablePtr beamTable;
    EXPECT_EQ(db()->CreateTable(L"Beam", beamTable), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(beamTable->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef factWithRef;
    factWithRef.name = L"Width";
    factWithRef.valueKind = sc::ValueKind::Int64;
    factWithRef.referenceTable = L"Floor";
    EXPECT_EQ(schema->AddColumn(factWithRef), sc::SC_E_SCHEMA_VIOLATION);

    sc::SCColumnDef relationWithoutRef;
    relationWithoutRef.name = L"FloorRef";
    relationWithoutRef.valueKind = sc::ValueKind::RecordId;
    relationWithoutRef.columnKind = sc::ColumnKind::Relation;
    EXPECT_EQ(schema->AddColumn(relationWithoutRef), sc::SC_E_SCHEMA_VIOLATION);
}