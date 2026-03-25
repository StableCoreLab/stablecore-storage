#include <iostream>

#include "StableCore/Storage/Storage.h"

namespace sc = stablecore::storage;

int main()
{
    sc::DbPtr db;
    if (sc::Failed(sc::CreateInMemoryDatabase(db)))
    {
        return 1;
    }

    sc::TablePtr beamTable;
    if (sc::Failed(db->CreateTable(L"Beam", beamTable)))
    {
        return 1;
    }

    sc::SchemaPtr schema;
    if (sc::Failed(beamTable->GetSchema(schema)))
    {
        return 1;
    }

    sc::ColumnDef width;
    width.name = L"Width";
    width.displayName = L"Width";
    width.valueKind = sc::ValueKind::Int64;
    width.defaultValue = sc::Value::FromInt64(0);
    if (sc::Failed(schema->AddColumn(width)))
    {
        return 1;
    }

    sc::EditPtr edit;
    if (sc::Failed(db->BeginEdit(L"Create Beam", edit)))
    {
        return 1;
    }

    sc::RecordPtr beam;
    if (sc::Failed(beamTable->CreateRecord(beam)))
    {
        return 1;
    }
    if (sc::Failed(beam->SetInt64(L"Width", 300)))
    {
        return 1;
    }
    if (sc::Failed(db->Commit(edit.Get())))
    {
        return 1;
    }

    std::int64_t widthValue = 0;
    if (sc::Failed(beam->GetInt64(L"Width", &widthValue)))
    {
        return 1;
    }

    std::wcout << L"Beam " << beam->GetId() << L" width = " << widthValue << std::endl;
    return 0;
}
