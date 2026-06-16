#include <filesystem>
#include <iostream>
#include <vector>

#include "SCStorage.h"

namespace sc = StableCore::Storage;
namespace fs = std::filesystem;

namespace
{

    fs::path MakeTempDbPath(const wchar_t* fileName)
    {
        fs::path path = fs::temp_directory_path() / fileName;
        std::error_code ec;
        fs::remove(path, ec);
        return path;
    }

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
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_ProductIntegrationExample.sqlite");

    sc::SCDbPtr db;
    if (sc::Failed(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db)))
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
    sc::SCColumnDef floorName;
    floorName.name = L"Name";
    floorName.displayName = L"Name";
    floorName.valueKind = sc::ValueKind::String;
    floorName.columnKind = sc::ColumnKind::Fact;
    floorName.nullable = false;
    floorName.editable = true;
    floorName.userDefined = false;
    floorName.indexed = false;
    floorName.participatesInCalc = false;
    floorName.unit = L"";
    floorName.referenceTable = L"";
    floorName.referenceStorageColumn = L"";
    floorName.referenceDisplayColumn = L"";
    floorName.defaultValue = sc::SCValue::FromString(L"");
    floorSchema->AddColumn(floorName);

    sc::SCSchemaPtr beamSchema;
    beamTable->GetSchema(beamSchema);
    sc::SCColumnDef length;
    length.name = L"Length";
    length.displayName = L"Length";
    length.valueKind = sc::ValueKind::Double;
    length.columnKind = sc::ColumnKind::Fact;
    length.nullable = false;
    length.editable = true;
    length.userDefined = false;
    length.indexed = false;
    length.participatesInCalc = true;
    length.unit = L"mm";
    length.defaultValue = sc::SCValue::FromDouble(0.0);
    beamSchema->AddColumn(length);

    sc::SCColumnDef width;
    width.name = L"Width";
    width.displayName = L"Width";
    width.valueKind = sc::ValueKind::Double;
    width.columnKind = sc::ColumnKind::Fact;
    width.nullable = false;
    width.editable = true;
    width.userDefined = false;
    width.indexed = false;
    width.participatesInCalc = true;
    width.unit = L"mm";
    width.defaultValue = sc::SCValue::FromDouble(0.0);
    beamSchema->AddColumn(width);

    sc::SCColumnDef height;
    height.name = L"Height";
    height.displayName = L"Height";
    height.valueKind = sc::ValueKind::Double;
    height.columnKind = sc::ColumnKind::Fact;
    height.nullable = false;
    height.editable = true;
    height.userDefined = false;
    height.indexed = false;
    height.participatesInCalc = true;
    height.unit = L"mm";
    height.defaultValue = sc::SCValue::FromDouble(0.0);
    beamSchema->AddColumn(height);

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
    floorImport.creates.push_back({{{L"Name", sc::SCValue::FromString(L"2F")}}});
    importRequests.push_back(floorImport);

    sc::ExecuteImport(db.Get(), importRequests, sc::ISCmportOptions{L"Import Floors"}, nullptr);

    sc::SCRecordCursorPtr floorCursor;
    floorTable->EnumerateRecords(floorCursor);
    sc::SCRecordPtr floor;
    if (sc::Failed(floorCursor->Next(floor)) || !floor)
    {
        return 1;
    }

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
    sc::ExecuteImport(db.Get(), beamImport, sc::ISCmportOptions{L"Import Beams"}, &ISCmportResult);
    std::wcout << L"Imported beams, version=" << ISCmportResult.committedVersion << L"\n";

    sc::SCComputedTableViewPtr beamView;
    if (sc::Failed(sc::CreateComputedTableView(db.Get(), L"Beam", nullptr, beamView)))
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
    sc::SCRecordPtr beam;
    if (sc::Failed(beamCursor->Next(beam)) || !beam)
    {
        return 1;
    }

    sc::SCValue volume;
    if (sc::Failed(beamView->GetCellValue(beam->GetId(), L"Volume", &volume)))
    {
        return 1;
    }

    double volumeValue = 0.0;
    volume.AsDouble(&volumeValue);
    std::wcout << L"Computed beam volume = " << volumeValue << L"\n";

    sc::SCStorageHealthReport report;
    sc::BuildStorageHealthReport(db.Get(), L"SQLite", &report);
    std::wcout << L"Health report diagnostics = " << report.diagnostics.size() << L"\n";
    return 0;
}
