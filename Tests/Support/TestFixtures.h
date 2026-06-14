#pragma once

#include <filesystem>

#include <gtest/gtest.h>

#include "SCStorage.h"
#include "TestPaths.h"

namespace sc = StableCore::Storage;
namespace fs = std::filesystem;

class FileDbTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        dbPath_ = MakeTempDbPath(L"StableCoreStorage_TestFixture.sqlite");
        ASSERT_EQ(sc::CreateFileDatabase(dbPath_.c_str(), sc::SCOpenDatabaseOptions{}, db_), sc::SC_OK);
    }

    void TearDown() override
    {
        db_.Reset();
        RemoveTempDbArtifacts(dbPath_);
    }

    sc::SCDbPtr db() { return db_; }
    const fs::path& dbPath() const { return dbPath_; }

private:
    sc::SCDbPtr db_;
    fs::path dbPath_;
};

class BeamTableTest : public FileDbTest
{
protected:
    void SetUp() override
    {
        FileDbTest::SetUp();
        
        ASSERT_EQ(db()->CreateTable(L"Beam", beamTable_), sc::SC_OK);

        sc::SCSchemaPtr schema;
        ASSERT_EQ(beamTable_->GetSchema(schema), sc::SC_OK);

        sc::SCColumnDef width;
        width.name = L"Width";
        width.displayName = L"Width";
        width.valueKind = sc::ValueKind::Int64;
        width.defaultValue = sc::SCValue::FromInt64(0);
        ASSERT_EQ(schema->AddColumn(width), sc::SC_OK);
    }

    sc::SCTablePtr beamTable() { return beamTable_; }

private:
    sc::SCTablePtr beamTable_;
};

class CompositeBeamQueryTest : public BeamTableTest
{
protected:
    void SetUp() override
    {
        BeamTableTest::SetUp();

        sc::SCSchemaPtr schema;
        ASSERT_EQ(beamTable()->GetSchema(schema), sc::SC_OK);

        sc::SCColumnDef name;
        name.name = L"Name";
        name.displayName = L"Name";
        name.valueKind = sc::ValueKind::String;
        name.nullable = false;
        name.defaultValue = sc::SCValue::FromString(L"");
        ASSERT_EQ(schema->AddColumn(name), sc::SC_OK);

        sc::SCColumnDef height;
        height.name = L"Height";
        height.displayName = L"Height";
        height.valueKind = sc::ValueKind::Int64;
        height.defaultValue = sc::SCValue::FromInt64(0);
        ASSERT_EQ(schema->AddColumn(height), sc::SC_OK);

        sc::SCEditPtr edit;
        ASSERT_EQ(db()->BeginEdit(L"seed composite", edit), sc::SC_OK);

        for (int i = 0; i < 5; ++i)
        {
            sc::SCRecordPtr record;
            ASSERT_EQ(beamTable()->CreateRecord(record), sc::SC_OK);
            ASSERT_EQ(record->SetInt64(L"Width", 100 + i * 50), sc::SC_OK);
            ASSERT_EQ(record->SetString(L"Name", std::to_wstring(i).c_str()), sc::SC_OK);
            ASSERT_EQ(record->SetInt64(L"Height", 200 + i * 30), sc::SC_OK);
        }

        ASSERT_EQ(db()->Commit(edit.Get()), sc::SC_OK);
    }
};

class TwoTableTest : public FileDbTest
{
protected:
    void SetUp() override
    {
        FileDbTest::SetUp();

        ASSERT_EQ(db()->CreateTable(L"Beam", beamTable_), sc::SC_OK);
        ASSERT_EQ(db()->CreateTable(L"Floor", floorTable_), sc::SC_OK);

        sc::SCSchemaPtr beamSchema;
        ASSERT_EQ(beamTable_->GetSchema(beamSchema), sc::SC_OK);

        sc::SCColumnDef width;
        width.name = L"Width";
        width.displayName = L"Width";
        width.valueKind = sc::ValueKind::Int64;
        width.defaultValue = sc::SCValue::FromInt64(0);
        ASSERT_EQ(beamSchema->AddColumn(width), sc::SC_OK);

        sc::SCColumnDef floorRef;
        floorRef.name = L"FloorRef";
        floorRef.displayName = L"FloorRef";
        floorRef.valueKind = sc::ValueKind::RecordId;
        floorRef.columnKind = sc::ColumnKind::Relation;
        floorRef.referenceTable = L"Floor";
        ASSERT_EQ(beamSchema->AddColumn(floorRef), sc::SC_OK);

        sc::SCSchemaPtr floorSchema;
        ASSERT_EQ(floorTable_->GetSchema(floorSchema), sc::SC_OK);

        sc::SCColumnDef floorName;
        floorName.name = L"Name";
        floorName.displayName = L"Name";
        floorName.valueKind = sc::ValueKind::String;
        floorName.nullable = false;
        floorName.defaultValue = sc::SCValue::FromString(L"");
        ASSERT_EQ(floorSchema->AddColumn(floorName), sc::SC_OK);
    }

    sc::SCTablePtr beamTable() { return beamTable_; }
    sc::SCTablePtr floorTable() { return floorTable_; }

private:
    sc::SCTablePtr beamTable_;
    sc::SCTablePtr floorTable_;
};

class ReadOnlyFileDbTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        dbPath_ = MakeTempDbPath(L"StableCoreStorage_TestFixture_ReadOnly.sqlite");
        
        sc::SCDbPtr writableDb;
        ASSERT_EQ(sc::CreateFileDatabase(dbPath_.c_str(), sc::SCOpenDatabaseOptions{}, writableDb), sc::SC_OK);

        sc::SCTablePtr table;
        ASSERT_EQ(writableDb->CreateTable(L"Beam", table), sc::SC_OK);

        sc::SCSchemaPtr schema;
        ASSERT_EQ(table->GetSchema(schema), sc::SC_OK);

        sc::SCColumnDef width;
        width.name = L"Width";
        width.displayName = L"Width";
        width.valueKind = sc::ValueKind::Int64;
        width.defaultValue = sc::SCValue::FromInt64(0);
        ASSERT_EQ(schema->AddColumn(width), sc::SC_OK);

        sc::SCEditPtr edit;
        ASSERT_EQ(writableDb->BeginEdit(L"seed", edit), sc::SC_OK);

        sc::SCRecordPtr record;
        ASSERT_EQ(table->CreateRecord(record), sc::SC_OK);
        ASSERT_EQ(record->SetInt64(L"Width", 100), sc::SC_OK);

        ASSERT_EQ(writableDb->Commit(edit.Get()), sc::SC_OK);
        writableDb.Reset();

        sc::SCOpenDatabaseOptions options;
        options.openMode = sc::SCDatabaseOpenMode::ReadOnly;
        ASSERT_EQ(sc::CreateFileDatabase(dbPath_.c_str(), options, db_), sc::SC_OK);
    }

    void TearDown() override
    {
        db_.Reset();
        RemoveTempDbArtifacts(dbPath_);
    }

    sc::SCDbPtr db() { return db_; }
    const fs::path& dbPath() const { return dbPath_; }

private:
    sc::SCDbPtr db_;
    fs::path dbPath_;
};
