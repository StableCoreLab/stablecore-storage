#include <iostream>
#include <vector>

#include "StableCore/Storage/Storage.h"

namespace sc = stablecore::storage;

namespace
{

class BeamComputedContext final : public sc::IComputedContext, public sc::RefCountedObject
{
public:
    explicit BeamComputedContext(sc::RecordPtr record)
        : record_(std::move(record))
    {
    }

    sc::ErrorCode GetValue(const wchar_t* fieldName, sc::Value* outValue) override
    {
        return record_->GetValue(fieldName, outValue);
    }

    sc::ErrorCode GetRef(const wchar_t* fieldName, sc::RecordId* outValue) override
    {
        return record_->GetRef(fieldName, outValue);
    }

    sc::ErrorCode GetRelated(const wchar_t*, sc::RecordCursorPtr&) override
    {
        return sc::SC_E_NOTIMPL;
    }

private:
    sc::RecordPtr record_;
};

class QuantityObserver final : public sc::IDatabaseObserver
{
public:
    void OnDatabaseChanged(const sc::ChangeSet& changeSet) override
    {
        std::wstring text;
        sc::DescribeChangeSet(changeSet, &text);
        std::wcout << L"[ChangeSet]\n" << text << L"\n";
    }
};

}  // namespace

int main()
{
    sc::DbPtr db;
    if (sc::Failed(sc::CreateInMemoryDatabase(db)))
    {
        return 1;
    }

    QuantityObserver observer;
    db->AddObserver(&observer);

    sc::TablePtr floorTable;
    sc::TablePtr beamTable;
    db->CreateTable(L"Floor", floorTable);
    db->CreateTable(L"Beam", beamTable);

    sc::SchemaPtr floorSchema;
    floorTable->GetSchema(floorSchema);
    floorSchema->AddColumn(sc::ColumnDef{L"Name", L"Name", sc::ValueKind::String, sc::ColumnKind::Fact, false, true, false, false, false, L"", L"", sc::Value::FromString(L"")});

    sc::SchemaPtr beamSchema;
    beamTable->GetSchema(beamSchema);
    beamSchema->AddColumn(sc::ColumnDef{L"Length", L"Length", sc::ValueKind::Double, sc::ColumnKind::Fact, false, true, false, false, true, L"mm", L"", sc::Value::FromDouble(0.0)});
    beamSchema->AddColumn(sc::ColumnDef{L"Width", L"Width", sc::ValueKind::Double, sc::ColumnKind::Fact, false, true, false, false, true, L"mm", L"", sc::Value::FromDouble(0.0)});
    beamSchema->AddColumn(sc::ColumnDef{L"Height", L"Height", sc::ValueKind::Double, sc::ColumnKind::Fact, false, true, false, false, true, L"mm", L"", sc::Value::FromDouble(0.0)});

    sc::ColumnDef floorRef;
    floorRef.name = L"FloorRef";
    floorRef.displayName = L"FloorRef";
    floorRef.valueKind = sc::ValueKind::RecordId;
    floorRef.columnKind = sc::ColumnKind::Relation;
    floorRef.referenceTable = L"Floor";
    beamSchema->AddColumn(floorRef);

    std::vector<sc::BatchTableRequest> importRequests;
    sc::BatchTableRequest floorImport;
    floorImport.tableName = L"Floor";
    floorImport.creates.push_back({{{L"Name", sc::Value::FromString(L"2F")}}});
    importRequests.push_back(floorImport);

    sc::ExecuteImport(db.Get(), importRequests, sc::ImportOptions{L"Import Floors"}, nullptr);

    sc::RecordCursorPtr floorCursor;
    floorTable->EnumerateRecords(floorCursor);
    bool hasFloor = false;
    floorCursor->MoveNext(&hasFloor);
    if (!hasFloor)
    {
        return 1;
    }

    sc::RecordPtr floor;
    floorCursor->GetCurrent(floor);

    std::vector<sc::BatchTableRequest> beamImport;
    sc::BatchTableRequest beamRequest;
    beamRequest.tableName = L"Beam";
    beamRequest.creates.push_back({{
        {L"Length", sc::Value::FromDouble(6000.0)},
        {L"Width", sc::Value::FromDouble(300.0)},
        {L"Height", sc::Value::FromDouble(500.0)},
        {L"FloorRef", sc::Value::FromRecordId(floor->GetId())},
    }});
    beamImport.push_back(beamRequest);

    sc::BatchExecutionResult importResult;
    sc::ExecuteImport(db.Get(), beamImport, sc::ImportOptions{L"Import Beams"}, &importResult);
    std::wcout << L"Imported beams, version=" << importResult.committedVersion << L"\n";

    sc::ComputedColumnDef volumeColumn;
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

    sc::RecordCursorPtr beamCursor;
    beamTable->EnumerateRecords(beamCursor);
    bool hasBeam = false;
    beamCursor->MoveNext(&hasBeam);
    if (!hasBeam)
    {
        return 1;
    }

    sc::RecordPtr beam;
    beamCursor->GetCurrent(beam);

    sc::RefPtr<BeamComputedContext> context = sc::MakeRef<BeamComputedContext>(beam);
    sc::Value volume;
    if (sc::Failed(sc::EvaluateComputedColumn(volumeColumn, context.Get(), nullptr, &volume)))
    {
        return 1;
    }

    double volumeValue = 0.0;
    volume.AsDouble(&volumeValue);
    std::wcout << L"Computed beam volume = " << volumeValue << L"\n";

    sc::StorageHealthReport report;
    sc::BuildStorageHealthReport(db.Get(), L"InMemory", &report);
    std::wcout << L"Health report diagnostics = " << report.diagnostics.size() << L"\n";
    return 0;
}
