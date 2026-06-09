#include <filesystem>
#include <iostream>

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

    sc::SCColumnDef MakeIntColumn(const wchar_t* name, bool nullable = false)
    {
        sc::SCColumnDef column;
        column.name = name;
        column.displayName = name;
        column.valueKind = sc::ValueKind::Int64;
        column.nullable = nullable;
        column.defaultValue = nullable ? sc::SCValue::Null() : sc::SCValue::FromInt64(0);
        return column;
    }

    sc::SCColumnDef MakeStringColumn(const wchar_t* name, bool nullable = false)
    {
        sc::SCColumnDef column;
        column.name = name;
        column.displayName = name;
        column.valueKind = sc::ValueKind::String;
        column.nullable = nullable;
        column.defaultValue = nullable ? sc::SCValue::Null() : sc::SCValue::FromString(L"");
        return column;
    }

}  // namespace

int main()
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_SchemaEditExample.sqlite");

    sc::SCDbPtr db;
    if (sc::Failed(sc::CreateFileDatabase(dbPath.c_str(), sc::SCOpenDatabaseOptions{}, db)))
    {
        return 1;
    }

    sc::SCTableSchemaSnapshot createSchema;
    createSchema.table.name = L"Beam";
    createSchema.columns.push_back(MakeIntColumn(L"Width"));
    createSchema.columns.push_back(MakeStringColumn(L"Name", true));

    sc::SCIndexDef nameIndex;
    nameIndex.name = L"idx_Beam_Name";
    nameIndex.columns.push_back(sc::SCIndexColumnDef{L"Name", false});
    createSchema.indexes.push_back(nameIndex);

    sc::SCSchemaEditResult createResult;
    sc::ErrorCode rc = sc::CreateTableFromSchema(db.Get(), createSchema, &createResult);
    if (sc::Failed(rc))
    {
        std::wcerr << L"CreateTableFromSchema failed: " << rc << L"\n";
        return 1;
    }

    std::wcout << L"Created table " << createResult.tableName << L", version=" << createResult.committedVersion
               << L"\n";

    sc::SCTableSchemaPatch patch;
    patch.tableName = L"Beam";

    sc::SCColumnDef updatedWidth = MakeStringColumn(L"Width");
    updatedWidth.displayName = L"Width Label";
    updatedWidth.defaultValue = sc::SCValue::FromString(L"0");
    patch.updateColumns.push_back(updatedWidth);
    patch.addColumns.push_back(MakeIntColumn(L"Height"));
    patch.removeIndexes.push_back(L"idx_Beam_Name");

    sc::SCConstraintDef widthUnique;
    widthUnique.kind = sc::SCConstraintKind::Unique;
    widthUnique.name = L"uq_Beam_Width";
    widthUnique.columns.push_back(L"Width");
    patch.addConstraints.push_back(widthUnique);

    sc::SCIndexDef heightIndex;
    heightIndex.name = L"idx_Beam_Height";
    heightIndex.columns.push_back(sc::SCIndexColumnDef{L"Height", false});
    patch.addIndexes.push_back(heightIndex);

    sc::SCSchemaEditResult patchResult;
    rc = sc::ApplyTableSchemaPatch(db.Get(), patch, &patchResult);
    if (sc::Failed(rc))
    {
        std::wcerr << L"ApplyTableSchemaPatch failed: " << rc << L"\n";
        return 1;
    }

    std::wcout << L"Patched table " << patchResult.tableName << L", version=" << patchResult.committedVersion << L"\n";
    std::wcout << L"  Added constraints: " << patchResult.addedConstraints.size() << L"\n";
    std::wcout << L"  Removed indexes: " << patchResult.removedIndexes.size() << L"\n";
    std::wcout << L"  Added indexes: " << patchResult.addedIndexes.size() << L"\n";

    sc::SCTablePtr table;
    if (sc::Failed(db->GetTable(L"Beam", table)))
    {
        return 1;
    }

    sc::SCSchemaPtr schema;
    if (sc::Failed(table->GetSchema(schema)))
    {
        return 1;
    }

    std::int32_t columnCount = 0;
    if (sc::Failed(schema->GetColumnCount(&columnCount)))
    {
        return 1;
    }

    std::wcout << L"Final schema columns:\n";
    for (std::int32_t index = 0; index < columnCount; ++index)
    {
        sc::SCColumnDef column;
        if (sc::Failed(schema->GetColumn(index, &column)))
        {
            return 1;
        }

        std::wcout << L"  - " << column.name << L" (display=" << column.displayName << L")\n";
    }

    std::int32_t constraintCount = 0;
    if (sc::Failed(schema->GetConstraintCount(&constraintCount)))
    {
        return 1;
    }
    std::wcout << L"Constraint count: " << constraintCount << L"\n";

    std::int32_t indexCount = 0;
    if (sc::Failed(schema->GetIndexCount(&indexCount)))
    {
        return 1;
    }
    std::wcout << L"Index count: " << indexCount << L"\n";

    sc::SCConstraintDef constraint;
    if (sc::Failed(schema->FindConstraint(L"uq_Beam_Width", &constraint)))
    {
        return 1;
    }
    std::wcout << L"Constraint: " << constraint.name << L"\n";

    sc::SCIndexDef index;
    if (sc::Failed(schema->FindIndex(L"idx_Beam_Height", &index)))
    {
        return 1;
    }
    std::wcout << L"Index: " << index.name << L"\n";

    return 0;
}
