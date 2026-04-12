#include <filesystem>

#include <gtest/gtest.h>

#include "StableCore/Storage/SCStorage.h"

namespace sc = stablecore::storage;
namespace fs = std::filesystem;

namespace
{

sc::SCTablePtr CreateFloorTable(sc::SCDbPtr& db)
{
    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Floor", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef name;
    name.name = L"Name";
    name.displayName = L"Name";
    name.valueKind = sc::ValueKind::String;
    name.defaultValue = sc::SCValue::FromString(L"");
    EXPECT_EQ(schema->AddColumn(name), sc::SC_OK);
    return table;
}

sc::SCTablePtr CreateBeamTable(sc::SCDbPtr& db)
{
    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);

    sc::SCColumnDef length;
    length.name = L"Length";
    length.displayName = L"Length";
    length.valueKind = sc::ValueKind::Double;
    length.defaultValue = sc::SCValue::FromDouble(0.0);
    length.indexed = true;
    EXPECT_EQ(schema->AddColumn(length), sc::SC_OK);

    sc::SCColumnDef width = length;
    width.name = L"Width";
    EXPECT_EQ(schema->AddColumn(width), sc::SC_OK);

    sc::SCColumnDef height = length;
    height.name = L"Height";
    EXPECT_EQ(schema->AddColumn(height), sc::SC_OK);

    sc::SCColumnDef floorRef;
    floorRef.name = L"FloorRef";
    floorRef.displayName = L"FloorRef";
    floorRef.valueKind = sc::ValueKind::RecordId;
    floorRef.columnKind = sc::ColumnKind::Relation;
    floorRef.referenceTable = L"Floor";
    floorRef.indexed = true;
    EXPECT_EQ(schema->AddColumn(floorRef), sc::SC_OK);

    return table;
}

fs::path MakeTempDbPath(const wchar_t* fileName)
{
    fs::path path = fs::temp_directory_path() / fileName;
    std::error_code ec;
    fs::remove(path, ec);
    return path;
}

}  // namespace

TEST(StorageTableView, CombinesFactAndComputedColumns)
{
    sc::SCDbPtr db;
    ASSERT_EQ(sc::CreateInMemoryDatabase(db), sc::SC_OK);

    sc::SCTablePtr floorTable = CreateFloorTable(db);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    sc::SCEditPtr edit;
    ASSERT_EQ(db->BeginEdit(L"seed", edit), sc::SC_OK);

    sc::SCRecordPtr floor;
    ASSERT_EQ(floorTable->CreateRecord(floor), sc::SC_OK);
    ASSERT_EQ(floor->SetString(L"Name", L"2F"), sc::SC_OK);

    sc::SCRecordPtr beam;
    ASSERT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    ASSERT_EQ(beam->SetDouble(L"Length", 6.0), sc::SC_OK);
    ASSERT_EQ(beam->SetDouble(L"Width", 0.3), sc::SC_OK);
    ASSERT_EQ(beam->SetDouble(L"Height", 0.5), sc::SC_OK);
    ASSERT_EQ(beam->SetRef(L"FloorRef", floor->GetId()), sc::SC_OK);
    ASSERT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    sc::SCComputedTableViewPtr view;
    ASSERT_EQ(sc::CreateComputedTableView(db.Get(), L"Beam", nullptr, view), sc::SC_OK);

    sc::SCComputedColumnDef volume;
    volume.name = L"Volume";
    volume.displayName = L"Volume";
    volume.valueKind = sc::ValueKind::Double;
    volume.kind = sc::ComputedFieldKind::Expression;
    volume.expression = L"Length * Width * Height";
    volume.dependencies.factFields = {
        {L"Beam", L"Length"},
        {L"Beam", L"Width"},
        {L"Beam", L"Height"},
    };
    ASSERT_EQ(view->AddComputedColumn(volume), sc::SC_OK);

    std::int32_t columnCount = 0;
    ASSERT_EQ(view->GetColumnCount(&columnCount), sc::SC_OK);
    EXPECT_EQ(columnCount, 5);

    sc::SCValue cell;
    ASSERT_EQ(view->GetCellValue(beam->GetId(), L"Volume", &cell), sc::SC_OK);
    double volumeValue = 0.0;
    ASSERT_EQ(cell.AsDouble(&volumeValue), sc::SC_OK);
    EXPECT_DOUBLE_EQ(volumeValue, 0.9);
}

TEST(StorageTableView, AggregateColumnTracksRelatedRecords)
{
    sc::SCDbPtr db;
    ASSERT_EQ(sc::CreateInMemoryDatabase(db), sc::SC_OK);

    sc::SCTablePtr floorTable = CreateFloorTable(db);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    sc::SCEditPtr edit;
    ASSERT_EQ(db->BeginEdit(L"seed", edit), sc::SC_OK);

    sc::SCRecordPtr floor;
    ASSERT_EQ(floorTable->CreateRecord(floor), sc::SC_OK);
    ASSERT_EQ(floor->SetString(L"Name", L"2F"), sc::SC_OK);

    for (int i = 0; i < 3; ++i)
    {
        sc::SCRecordPtr beam;
        ASSERT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
        ASSERT_EQ(beam->SetDouble(L"Length", 6.0 + i), sc::SC_OK);
        ASSERT_EQ(beam->SetDouble(L"Width", 0.3), sc::SC_OK);
        ASSERT_EQ(beam->SetDouble(L"Height", 0.5), sc::SC_OK);
        ASSERT_EQ(beam->SetRef(L"FloorRef", floor->GetId()), sc::SC_OK);
    }
    ASSERT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    sc::SCComputedTableViewPtr floorView;
    ASSERT_EQ(sc::CreateComputedTableView(db.Get(), L"Floor", nullptr, floorView), sc::SC_OK);

    sc::SCComputedColumnDef beamCount;
    beamCount.name = L"BeamCount";
    beamCount.displayName = L"BeamCount";
    beamCount.valueKind = sc::ValueKind::Int64;
    beamCount.kind = sc::ComputedFieldKind::Aggregate;
    beamCount.aggregateKind = sc::SCAggregateKind::Count;
    beamCount.aggregateRelation = L"Beam.FloorRef";
    beamCount.dependencies.relationFields = {{L"Beam", L"FloorRef"}};
    ASSERT_EQ(floorView->AddComputedColumn(beamCount), sc::SC_OK);

    sc::SCValue cell;
    ASSERT_EQ(floorView->GetCellValue(floor->GetId(), L"BeamCount", &cell), sc::SC_OK);
    std::int64_t count = 0;
    ASSERT_EQ(cell.AsInt64(&count), sc::SC_OK);
    EXPECT_EQ(count, 3);
}
