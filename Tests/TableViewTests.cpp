#include <filesystem>

#include <gtest/gtest.h>

#include "SCStorage.h"

namespace sc = StableCore::Storage;
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
    ASSERT_EQ(sc::CreateComputedTableView(db.Get(), L"Beam", nullptr, view),
              sc::SC_OK);

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
    ASSERT_EQ(
        sc::CreateComputedTableView(db.Get(), L"Floor", nullptr, floorView),
        sc::SC_OK);

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
    ASSERT_EQ(floorView->GetCellValue(floor->GetId(), L"BeamCount", &cell),
              sc::SC_OK);
    std::int64_t count = 0;
    ASSERT_EQ(cell.AsInt64(&count), sc::SC_OK);
    EXPECT_EQ(count, 3);
}

TEST(StorageTableView, RejectsInvalidComputedColumnDefinitions)
{
    sc::SCDbPtr db;
    ASSERT_EQ(sc::CreateInMemoryDatabase(db), sc::SC_OK);

    sc::SCTablePtr floorTable = CreateFloorTable(db);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    sc::SCComputedTableViewPtr view;
    ASSERT_EQ(sc::CreateComputedTableView(db.Get(), L"Beam", nullptr, view),
              sc::SC_OK);

    sc::SCComputedColumnDef invalidExpression;
    invalidExpression.name = L"InvalidExpression";
    invalidExpression.valueKind = sc::ValueKind::Double;
    invalidExpression.kind = sc::ComputedFieldKind::Expression;
    invalidExpression.dependencies.factFields = {{L"Beam", L"Length"}};
    EXPECT_EQ(view->AddComputedColumn(invalidExpression), sc::SC_E_INVALIDARG);

    sc::SCComputedColumnDef invalidRule;
    invalidRule.name = L"InvalidRule";
    invalidRule.valueKind = sc::ValueKind::Double;
    invalidRule.kind = sc::ComputedFieldKind::Rule;
    invalidRule.dependencies.factFields = {{L"Beam", L"Length"}};
    EXPECT_EQ(view->AddComputedColumn(invalidRule), sc::SC_E_INVALIDARG);

    sc::SCComputedColumnDef invalidAggregate;
    invalidAggregate.name = L"InvalidAggregate";
    invalidAggregate.valueKind = sc::ValueKind::Int64;
    invalidAggregate.kind = sc::ComputedFieldKind::Aggregate;
    invalidAggregate.aggregateKind = sc::SCAggregateKind::Count;
    invalidAggregate.aggregateField = L"FloorRef";
    invalidAggregate.dependencies.relationFields = {{L"Beam", L"FloorRef"}};
    EXPECT_EQ(view->AddComputedColumn(invalidAggregate), sc::SC_E_INVALIDARG);

    sc::SCComputedColumnDef validAggregate;
    validAggregate.name = L"BeamCount";
    validAggregate.valueKind = sc::ValueKind::Int64;
    validAggregate.kind = sc::ComputedFieldKind::Aggregate;
    validAggregate.aggregateKind = sc::SCAggregateKind::Count;
    validAggregate.aggregateRelation = L"Beam.FloorRef";
    validAggregate.aggregateField = L"FloorRef";
    validAggregate.dependencies.relationFields = {{L"Beam", L"FloorRef"}};
    EXPECT_EQ(view->AddComputedColumn(validAggregate), sc::SC_OK);
    EXPECT_EQ(view->AddComputedColumn(validAggregate), sc::SC_E_COLUMN_EXISTS);
}

TEST(StorageTableView, ComputedColumnTracksEditUndoRedo)
{
    sc::SCDbPtr db;
    ASSERT_EQ(sc::CreateInMemoryDatabase(db), sc::SC_OK);

    sc::SCTablePtr floorTable = CreateFloorTable(db);
    sc::SCTablePtr beamTable = CreateBeamTable(db);

    sc::SCEditPtr seedEdit;
    ASSERT_EQ(db->BeginEdit(L"seed", seedEdit), sc::SC_OK);

    sc::SCRecordPtr floor;
    ASSERT_EQ(floorTable->CreateRecord(floor), sc::SC_OK);
    ASSERT_EQ(floor->SetString(L"Name", L"2F"), sc::SC_OK);

    sc::SCRecordPtr beam;
    ASSERT_EQ(beamTable->CreateRecord(beam), sc::SC_OK);
    ASSERT_EQ(beam->SetDouble(L"Length", 4.0), sc::SC_OK);
    ASSERT_EQ(beam->SetDouble(L"Width", 0.5), sc::SC_OK);
    ASSERT_EQ(beam->SetDouble(L"Height", 1.0), sc::SC_OK);
    ASSERT_EQ(beam->SetRef(L"FloorRef", floor->GetId()), sc::SC_OK);
    ASSERT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    sc::SCComputedTableViewPtr view;
    ASSERT_EQ(sc::CreateComputedTableView(db.Get(), L"Beam", nullptr, view),
              sc::SC_OK);

    sc::SCComputedColumnDef doubledWidth;
    doubledWidth.name = L"DoubledWidth";
    doubledWidth.displayName = L"DoubledWidth";
    doubledWidth.valueKind = sc::ValueKind::Double;
    doubledWidth.kind = sc::ComputedFieldKind::Expression;
    doubledWidth.expression = L"Width * 2";
    doubledWidth.dependencies.factFields = {{L"Beam", L"Width"}};
    ASSERT_EQ(view->AddComputedColumn(doubledWidth), sc::SC_OK);

    sc::SCValue cell;
    ASSERT_EQ(view->GetCellValue(beam->GetId(), L"DoubledWidth", &cell),
              sc::SC_OK);
    double doubled = 0.0;
    ASSERT_EQ(cell.AsDouble(&doubled), sc::SC_OK);
    EXPECT_DOUBLE_EQ(doubled, 1.0);

    sc::SCEditPtr edit;
    ASSERT_EQ(db->BeginEdit(L"modify beam width", edit), sc::SC_OK);
    ASSERT_EQ(beam->SetDouble(L"Width", 1.5), sc::SC_OK);
    ASSERT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    ASSERT_EQ(view->GetCellValue(beam->GetId(), L"DoubledWidth", &cell),
              sc::SC_OK);
    ASSERT_EQ(cell.AsDouble(&doubled), sc::SC_OK);
    EXPECT_DOUBLE_EQ(doubled, 3.0);

    ASSERT_EQ(db->Undo(), sc::SC_OK);
    ASSERT_EQ(view->GetCellValue(beam->GetId(), L"DoubledWidth", &cell),
              sc::SC_OK);
    ASSERT_EQ(cell.AsDouble(&doubled), sc::SC_OK);
    EXPECT_DOUBLE_EQ(doubled, 1.0);

    ASSERT_EQ(db->Redo(), sc::SC_OK);
    ASSERT_EQ(view->GetCellValue(beam->GetId(), L"DoubledWidth", &cell),
              sc::SC_OK);
    ASSERT_EQ(cell.AsDouble(&doubled), sc::SC_OK);
    EXPECT_DOUBLE_EQ(doubled, 3.0);
}
