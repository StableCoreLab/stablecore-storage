#include <iostream>
#include <vector>

#include "SCStorage.h"

namespace sc = StableCore::Storage;

namespace
{

    class QuantityObserver final : public sc::ISCDatabaseObserver
    {
    public:
        void OnDatabaseChanged(const sc::SCChangeSet& SCChangeSet) override
        {
            std::wstring text;
            sc::DescribeChangeSet(SCChangeSet, &text);
            std::wcout << L"[SCChangeSet]\n" << text << L"\n";
        }
    };

}  // namespace

int main()
{
    sc::SCDbPtr db;
    if (sc::Failed(sc::CreateInMemoryDatabase(db)))
    {
        return 1;
    }

    QuantityObserver observer;
    db->AddObserver(&observer);

    sc::SCTablePtr floorTable;
    sc::SCTablePtr beamTable;
    db->CreateTable(L"Floor", floorTable);
    db->CreateTable(L"Beam", beamTable);

    sc::SCSchemaPtr floorSchema;
    floorTable->GetSchema(floorSchema);
    floorSchema->AddColumn(sc::SCColumnDef{
        L"Name", L"Name", sc::ValueKind::String, sc::ColumnKind::Fact, false,
        true, false, false, false, L"", L"", sc::SCValue::FromString(L"")});

    sc::SCSchemaPtr beamSchema;
    beamTable->GetSchema(beamSchema);
    beamSchema->AddColumn(
        sc::SCColumnDef{L"Length", L"Length", sc::ValueKind::Double,
                        sc::ColumnKind::Fact, false, true, false, false, true,
                        L"mm", L"", sc::SCValue::FromDouble(0.0)});
    beamSchema->AddColumn(sc::SCColumnDef{
        L"Width", L"Width", sc::ValueKind::Double, sc::ColumnKind::Fact, false,
        true, false, false, true, L"mm", L"", sc::SCValue::FromDouble(0.0)});
    beamSchema->AddColumn(
        sc::SCColumnDef{L"Height", L"Height", sc::ValueKind::Double,
                        sc::ColumnKind::Fact, false, true, false, false, true,
                        L"mm", L"", sc::SCValue::FromDouble(0.0)});

    sc::SCColumnDef floorRef;
    floorRef.name = L"FloorRef";
    floorRef.displayName = L"FloorRef";
    floorRef.valueKind = sc::ValueKind::RecordId;
    floorRef.columnKind = sc::ColumnKind::Relation;
    floorRef.referenceTable = L"Floor";
    beamSchema->AddColumn(floorRef);

    std::vector<sc::SCBatchTableRequest> importRequests;
    sc::SCBatchTableRequest floorImport;
    floorImport.tableName = L"Floor";
    floorImport.creates.push_back(
        {{{L"Name", sc::SCValue::FromString(L"2F")}}});
    importRequests.push_back(floorImport);

    sc::ExecuteImport(db.Get(), importRequests,
                      sc::ISCmportOptions{L"Import Floors"}, nullptr);

    sc::SCRecordCursorPtr floorCursor;
    floorTable->EnumerateRecords(floorCursor);
    bool hasFloor = false;
    floorCursor->MoveNext(&hasFloor);
    if (!hasFloor)
    {
        return 1;
    }

    sc::SCRecordPtr floor;
    floorCursor->GetCurrent(floor);

    std::vector<sc::SCBatchTableRequest> beamImport;
    sc::SCBatchTableRequest beamRequest;
    beamRequest.tableName = L"Beam";
    beamRequest.creates.push_back({{
        {L"Length", sc::SCValue::FromDouble(6000.0)},
        {L"Width", sc::SCValue::FromDouble(300.0)},
        {L"Height", sc::SCValue::FromDouble(500.0)},
        {L"FloorRef", sc::SCValue::FromRecordId(floor->GetId())},
    }});
    beamImport.push_back(beamRequest);

    sc::SCBatchExecutionResult ISCmportResult;
    sc::ExecuteImport(db.Get(), beamImport,
                      sc::ISCmportOptions{L"Import Beams"}, &ISCmportResult);
    std::wcout << L"Imported beams, version=" << ISCmportResult.committedVersion
               << L"\n";

    sc::SCComputedTableViewPtr beamView;
    if (sc::Failed(
            sc::CreateComputedTableView(db.Get(), L"Beam", nullptr, beamView)))
    {
        return 1;
    }

    sc::SCComputedColumnDef volumeColumn;
    volumeColumn.name = L"Volume";
    volumeColumn.displayName = L"Volume";
    volumeColumn.valueKind = sc::ValueKind::Double;
    volumeColumn.kind = sc::ComputedFieldKind::Expression;
    volumeColumn.expression = L"Length * Width * Height";
    volumeColumn.dependencies.factFields = {
        {L"Beam", L"Length"},
        {L"Beam", L"Width"},
        {L"Beam", L"Height"},
    };
    beamView->AddComputedColumn(volumeColumn);

    sc::SCRecordCursorPtr beamCursor;
    beamView->EnumerateRecords(beamCursor);
    bool hasBeam = false;
    beamCursor->MoveNext(&hasBeam);
    if (!hasBeam)
    {
        return 1;
    }

    sc::SCRecordPtr beam;
    beamCursor->GetCurrent(beam);

    sc::SCValue volume;
    if (sc::Failed(beamView->GetCellValue(beam->GetId(), L"Volume", &volume)))
    {
        return 1;
    }

    double volumeValue = 0.0;
    volume.AsDouble(&volumeValue);
    std::wcout << L"Computed beam volume = " << volumeValue << L"\n";

    sc::SCStorageHealthReport report;
    sc::BuildStorageHealthReport(db.Get(), L"InMemory", &report);
    std::wcout << L"Health report diagnostics = " << report.diagnostics.size()
               << L"\n";
    return 0;
}
