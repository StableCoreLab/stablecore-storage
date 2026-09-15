#include <filesystem>
#include <string>

#include <gtest/gtest.h>

#include "SCStorage.h"

#include "Support/TestPaths.h"

namespace sc = StableCore::Storage;
namespace fs = std::filesystem;

namespace
{
    sc::ErrorCode CreateFileDb(const wchar_t* path, sc::SCDbPtr& db)
    {
        return sc::CreateFileDatabase(path, sc::SCOpenDatabaseOptions{}, db);
    }

    // A column that participates in a primary key and has no default value, so
    // the commit time required value scan reports it.
    sc::SCColumnDef MakeRequiredStringColumn(const wchar_t* name)
    {
        sc::SCColumnDef column;
        column.name = name;
        column.displayName = name;
        column.valueKind = sc::ValueKind::String;
        column.nullable = false;
        return column;
    }

    sc::SCColumnDef MakeWidthColumn(std::int64_t defaultValue)
    {
        sc::SCColumnDef column;
        column.name = L"Width";
        column.displayName = L"Width";
        column.valueKind = sc::ValueKind::Int64;
        column.nullable = false;
        column.defaultValue = sc::SCValue::FromInt64(defaultValue);
        return column;
    }

    sc::SCConstraintDef MakeUniqueConstraint(const wchar_t* name, const wchar_t* columnName)
    {
        sc::SCConstraintDef constraint;
        constraint.kind = sc::SCConstraintKind::Unique;
        constraint.name = name;
        constraint.columns.push_back(columnName);
        return constraint;
    }

    sc::SCConstraintDef MakePrimaryKeyConstraint(const wchar_t* name, const wchar_t* columnName)
    {
        sc::SCConstraintDef constraint;
        constraint.kind = sc::SCConstraintKind::PrimaryKey;
        constraint.name = name;
        constraint.columns.push_back(columnName);
        return constraint;
    }

    sc::SCConstraintDef MakeCheckConstraint(const wchar_t* name, const wchar_t* columnName, const wchar_t* expression)
    {
        sc::SCConstraintDef constraint;
        constraint.kind = sc::SCConstraintKind::Check;
        constraint.name = name;
        constraint.columns.push_back(columnName);
        constraint.checkExpression = expression;
        return constraint;
    }

    std::size_t CountAliveRecords(sc::SCTablePtr& table)
    {
        sc::SCRecordCursorPtr cursor;
        if (sc::Failed(table->EnumerateRecords(cursor)) || !cursor)
        {
            return 0;
        }

        std::size_t count = 0;
        sc::SCRecordPtr record;
        while (sc::Succeeded(cursor->Next(record)) && record)
        {
            ++count;
        }
        return count;
    }
}  // namespace

// 8.1 Normal create flow: create the draft, fill the required values, commit.
TEST(SqliteCreateRecordLifecycle, DraftRecordIsFilledThenCommitted)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_Lifecycle_DraftCommit.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);
    EXPECT_EQ(schema->AddColumn(MakeRequiredStringColumn(L"Code")), sc::SC_OK);
    EXPECT_EQ(schema->AddColumn(MakeWidthColumn(0)), sc::SC_OK);
    EXPECT_EQ(schema->AddConstraint(MakePrimaryKeyConstraint(L"pk_Beam_Code", L"Code")), sc::SC_OK);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"Add Record", edit), sc::SC_OK);

    sc::SCRecordPtr record;
    EXPECT_EQ(table->CreateRecord(record), sc::SC_OK);
    ASSERT_TRUE(static_cast<bool>(record));
    EXPECT_EQ(record->SetString(L"Code", L"B-1"), sc::SC_OK);
    EXPECT_EQ(record->SetInt64(L"Width", 300), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);
    EXPECT_EQ(edit->GetState(), sc::EditState::Committed);

    std::wstring code;
    EXPECT_EQ(record->GetStringCopy(L"Code", &code), sc::SC_OK);
    EXPECT_EQ(code, L"B-1");
    EXPECT_EQ(CountAliveRecords(table), 1u);
}

// 8.2 An empty primary key fails the commit, the edit stays Active and a fixed
// draft can still be committed.
TEST(SqliteCreateRecordLifecycle, CommitRejectsMissingPrimaryKeyAndKeepsEditActive)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_Lifecycle_EmptyPrimaryKey.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);
    EXPECT_EQ(schema->AddColumn(MakeRequiredStringColumn(L"Code")), sc::SC_OK);
    EXPECT_EQ(schema->AddConstraint(MakePrimaryKeyConstraint(L"pk_Beam_Code", L"Code")), sc::SC_OK);

    const sc::VersionId versionBefore = db->GetCurrentVersion();

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"missing primary key", edit), sc::SC_OK);

    sc::SCRecordPtr record;
    // CreateRecord() succeeds: the draft does not need its primary key yet.
    EXPECT_EQ(table->CreateRecord(record), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_E_SCHEMA_VIOLATION);

    sc::SCConstraintViolationInfo violation;
    EXPECT_EQ(db->GetLastConstraintViolationInfo(&violation), sc::SC_OK);
    EXPECT_EQ(violation.source, sc::SCConstraintViolationSource::RequiredColumnValue);
    EXPECT_EQ(violation.tableName, std::wstring(L"Beam"));
    EXPECT_EQ(violation.recordId, record->GetId());
    EXPECT_TRUE(violation.constraintName.empty());
    // A missing required column value must not be reported as a new constraint
    // kind; SCConstraintKind keeps its schema level meaning only.
    EXPECT_NE(violation.kind, sc::SCConstraintKind::Unique);
    EXPECT_NE(violation.kind, sc::SCConstraintKind::Check);
    ASSERT_EQ(violation.columns.size(), 1u);
    EXPECT_EQ(violation.columns.front(), std::wstring(L"Code"));

    // Nothing was persisted and the edit is still usable.
    EXPECT_EQ(db->GetCurrentVersion(), versionBefore);
    EXPECT_EQ(edit->GetState(), sc::EditState::Active);

    EXPECT_EQ(record->SetString(L"Code", L"B-1"), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);
    EXPECT_EQ(edit->GetState(), sc::EditState::Committed);
}

// 8.3 Several required columns can be filled one by one before the commit.
TEST(SqliteCreateRecordLifecycle, DraftRecordAcceptsRequiredColumnsFilledOneByOne)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_Lifecycle_MultipleRequired.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);
    EXPECT_EQ(schema->AddColumn(MakeRequiredStringColumn(L"Code")), sc::SC_OK);
    EXPECT_EQ(schema->AddColumn(MakeRequiredStringColumn(L"Grade")), sc::SC_OK);
    EXPECT_EQ(schema->AddConstraint(MakePrimaryKeyConstraint(L"pk_Beam_Code", L"Code")), sc::SC_OK);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"fill required columns", edit), sc::SC_OK);

    sc::SCRecordPtr record;
    EXPECT_EQ(table->CreateRecord(record), sc::SC_OK);

    EXPECT_EQ(record->SetString(L"Code", L"B-1"), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_E_SCHEMA_VIOLATION);

    sc::SCConstraintViolationInfo violation;
    EXPECT_EQ(db->GetLastConstraintViolationInfo(&violation), sc::SC_OK);
    EXPECT_EQ(violation.source, sc::SCConstraintViolationSource::RequiredColumnValue);
    ASSERT_EQ(violation.columns.size(), 1u);
    EXPECT_EQ(violation.columns.front(), std::wstring(L"Grade"));

    EXPECT_EQ(record->SetString(L"Grade", L"C30"), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);
    EXPECT_EQ(CountAliveRecords(table), 1u);
}

// 8.4 A unique conflict between a draft and an existing record is detected by
// Commit(), not by SetValue(), and leaves no partial persistence behind.
TEST(SqliteCreateRecordLifecycle, CommitRejectsUniqueConflictOfDraftRecord)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_Lifecycle_UniqueConflict.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);
    EXPECT_EQ(schema->AddColumn(MakeWidthColumn(0)), sc::SC_OK);
    EXPECT_EQ(schema->AddConstraint(MakeUniqueConstraint(L"uq_Beam_Width", L"Width")), sc::SC_OK);

    sc::SCEditPtr seedEdit;
    EXPECT_EQ(db->BeginEdit(L"seed", seedEdit), sc::SC_OK);
    sc::SCRecordPtr seed;
    EXPECT_EQ(table->CreateRecord(seed), sc::SC_OK);
    EXPECT_EQ(seed->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(db->Commit(seedEdit.Get()), sc::SC_OK);

    const sc::VersionId versionBefore = db->GetCurrentVersion();

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"duplicate width", edit), sc::SC_OK);
    sc::SCRecordPtr record;
    EXPECT_EQ(table->CreateRecord(record), sc::SC_OK);
    // The unique conflict is deferred for a record created in this edit, but
    // the value itself is kept so the draft can be inspected and fixed.
    EXPECT_EQ(record->SetInt64(L"Width", 100), sc::SC_OK);
    std::int64_t stagedWidth = 0;
    EXPECT_EQ(record->GetInt64(L"Width", &stagedWidth), sc::SC_OK);
    EXPECT_EQ(stagedWidth, 100);

    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_E_CONSTRAINT_VIOLATION);

    sc::SCConstraintViolationInfo violation;
    EXPECT_EQ(db->GetLastConstraintViolationInfo(&violation), sc::SC_OK);
    EXPECT_EQ(violation.source, sc::SCConstraintViolationSource::SchemaConstraint);
    EXPECT_EQ(violation.kind, sc::SCConstraintKind::Unique);
    EXPECT_EQ(violation.constraintName, std::wstring(L"uq_Beam_Width"));
    EXPECT_EQ(violation.recordId, record->GetId());

    // No partial persistence: the version is unchanged and the draft is gone
    // after the rollback.
    EXPECT_EQ(db->GetCurrentVersion(), versionBefore);
    EXPECT_EQ(edit->GetState(), sc::EditState::Active);
    EXPECT_EQ(record->SetInt64(L"Width", 200), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);
    EXPECT_EQ(CountAliveRecords(table), 2u);
}

// 8.4 Two drafts that conflict with each other are also rejected by Commit().
TEST(SqliteCreateRecordLifecycle, CommitRejectsUniqueConflictBetweenTwoDrafts)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_Lifecycle_TwoDraftConflict.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);
    EXPECT_EQ(schema->AddColumn(MakeWidthColumn(0)), sc::SC_OK);
    EXPECT_EQ(schema->AddConstraint(MakeUniqueConstraint(L"uq_Beam_Width", L"Width")), sc::SC_OK);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"two drafts", edit), sc::SC_OK);

    sc::SCRecordPtr first;
    sc::SCRecordPtr second;
    EXPECT_EQ(table->CreateRecord(first), sc::SC_OK);
    EXPECT_EQ(table->CreateRecord(second), sc::SC_OK);
    EXPECT_EQ(first->SetInt64(L"Width", 100), sc::SC_OK);
    EXPECT_EQ(second->SetInt64(L"Width", 100), sc::SC_OK);

    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_E_CONSTRAINT_VIOLATION);
    EXPECT_EQ(edit->GetState(), sc::EditState::Active);

    EXPECT_EQ(second->SetInt64(L"Width", 200), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);
    EXPECT_EQ(CountAliveRecords(table), 2u);
}

// 8.4.1 A Check violation written into a draft surfaces at Commit() with the
// SchemaConstraint source and the original Check kind.
TEST(SqliteCreateRecordLifecycle, CommitRejectsDraftCheckViolationWithSchemaSource)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_Lifecycle_DraftCheck.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);
    EXPECT_EQ(schema->AddColumn(MakeWidthColumn(1)), sc::SC_OK);
    EXPECT_EQ(schema->AddConstraint(MakeCheckConstraint(L"ck_Beam_Width_Positive", L"Width", L"Width > 0")),
              sc::SC_OK);

    const sc::VersionId versionBefore = db->GetCurrentVersion();

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"invalid draft width", edit), sc::SC_OK);
    sc::SCRecordPtr record;
    EXPECT_EQ(table->CreateRecord(record), sc::SC_OK);
    // SetValue() keeps the value: the check runs at Commit().
    EXPECT_EQ(record->SetInt64(L"Width", -5), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_E_CONSTRAINT_VIOLATION);

    sc::SCConstraintViolationInfo violation;
    EXPECT_EQ(db->GetLastConstraintViolationInfo(&violation), sc::SC_OK);
    EXPECT_EQ(violation.source, sc::SCConstraintViolationSource::SchemaConstraint);
    EXPECT_EQ(violation.kind, sc::SCConstraintKind::Check);
    EXPECT_EQ(violation.constraintName, std::wstring(L"ck_Beam_Width_Positive"));
    EXPECT_EQ(violation.recordId, record->GetId());

    EXPECT_EQ(db->GetCurrentVersion(), versionBefore);
    EXPECT_EQ(edit->GetState(), sc::EditState::Active);

    EXPECT_EQ(record->SetInt64(L"Width", 5), sc::SC_OK);
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);
    EXPECT_EQ(CountAliveRecords(table), 1u);
}

// 8.5 Rollback discards the draft and it must not come back after a reopen.
TEST(SqliteCreateRecordLifecycle, RollbackDiscardsDraftRecord)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_Lifecycle_Rollback.sqlite");
    sc::RecordId draftId = 0;

    {
        sc::SCDbPtr db;
        EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

        sc::SCTablePtr table;
        EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

        sc::SCSchemaPtr schema;
        EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);
        EXPECT_EQ(schema->AddColumn(MakeWidthColumn(0)), sc::SC_OK);
        EXPECT_EQ(schema->AddConstraint(MakeUniqueConstraint(L"uq_Beam_Width", L"Width")), sc::SC_OK);

        sc::SCEditPtr edit;
        EXPECT_EQ(db->BeginEdit(L"draft", edit), sc::SC_OK);
        sc::SCRecordPtr record;
        EXPECT_EQ(table->CreateRecord(record), sc::SC_OK);
        EXPECT_EQ(record->SetInt64(L"Width", 100), sc::SC_OK);
        draftId = record->GetId();
        EXPECT_EQ(db->Rollback(edit.Get()), sc::SC_OK);
        EXPECT_EQ(edit->GetState(), sc::EditState::RolledBack);

        EXPECT_EQ(CountAliveRecords(table), 0u);
        sc::SCRecordPtr discarded;
        if (sc::Succeeded(table->GetRecord(draftId, discarded)))
        {
            EXPECT_TRUE(discarded->IsDeleted());
        }

        // The rolled back draft must not take part in later uniqueness checks.
        sc::SCEditPtr reuseEdit;
        EXPECT_EQ(db->BeginEdit(L"reuse width", reuseEdit), sc::SC_OK);
        sc::SCRecordPtr reuse;
        EXPECT_EQ(table->CreateRecord(reuse), sc::SC_OK);
        EXPECT_EQ(reuse->SetInt64(L"Width", 100), sc::SC_OK);
        EXPECT_EQ(db->Commit(reuseEdit.Get()), sc::SC_OK);
        EXPECT_EQ(CountAliveRecords(table), 1u);
    }

    sc::SCDbPtr reopened;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), reopened), sc::SC_OK);
    sc::SCTablePtr reopenedTable;
    EXPECT_EQ(reopened->GetTable(L"Beam", reopenedTable), sc::SC_OK);
    EXPECT_EQ(CountAliveRecords(reopenedTable), 1u);
    sc::SCRecordPtr revived;
    EXPECT_EQ(reopenedTable->GetRecord(draftId, revived), sc::SC_E_RECORD_NOT_FOUND);
}

// 8.6 Undo removes the committed record and Redo restores the same RecordId.
TEST(SqliteCreateRecordLifecycle, CommittedDraftSupportsUndoRedoWithSameIdentity)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_Lifecycle_UndoRedo.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);
    EXPECT_EQ(schema->AddColumn(MakeWidthColumn(0)), sc::SC_OK);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"draft", edit), sc::SC_OK);
    sc::SCRecordPtr record;
    EXPECT_EQ(table->CreateRecord(record), sc::SC_OK);
    EXPECT_EQ(record->SetInt64(L"Width", 320), sc::SC_OK);
    const sc::RecordId recordId = record->GetId();
    EXPECT_EQ(db->Commit(edit.Get()), sc::SC_OK);

    EXPECT_EQ(db->Undo(), sc::SC_OK);
    sc::SCRecordPtr undone;
    EXPECT_EQ(table->GetRecord(recordId, undone), sc::SC_OK);
    EXPECT_TRUE(undone->IsDeleted());
    EXPECT_EQ(CountAliveRecords(table), 0u);

    EXPECT_EQ(db->Redo(), sc::SC_OK);
    sc::SCRecordPtr redone;
    EXPECT_EQ(table->GetRecord(recordId, redone), sc::SC_OK);
    EXPECT_FALSE(redone->IsDeleted());
    EXPECT_EQ(redone->GetId(), recordId);
    std::int64_t width = 0;
    EXPECT_EQ(redone->GetInt64(L"Width", &width), sc::SC_OK);
    EXPECT_EQ(width, 320);
}

// A table without any column still cannot create records.
TEST(SqliteCreateRecordLifecycle, EmptySchemaTableCannotCreateRecord)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_Lifecycle_EmptySchema.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SCEditPtr edit;
    EXPECT_EQ(db->BeginEdit(L"empty schema", edit), sc::SC_OK);
    sc::SCRecordPtr record;
    EXPECT_EQ(table->CreateRecord(record), sc::SC_E_SCHEMA_VIOLATION);
    EXPECT_FALSE(static_cast<bool>(record));
    EXPECT_EQ(db->Rollback(edit.Get()), sc::SC_OK);
}

// CreateRecord() outside an edit keeps failing with SC_E_NO_ACTIVE_EDIT.
TEST(SqliteCreateRecordLifecycle, CreateRecordRequiresActiveEdit)
{
    const fs::path dbPath = MakeTempDbPath(L"StableCoreStorage_Sqlite_Lifecycle_NoActiveEdit.sqlite");

    sc::SCDbPtr db;
    EXPECT_EQ(CreateFileDb(dbPath.c_str(), db), sc::SC_OK);

    sc::SCTablePtr table;
    EXPECT_EQ(db->CreateTable(L"Beam", table), sc::SC_OK);

    sc::SCSchemaPtr schema;
    EXPECT_EQ(table->GetSchema(schema), sc::SC_OK);
    EXPECT_EQ(schema->AddColumn(MakeWidthColumn(0)), sc::SC_OK);

    sc::SCRecordPtr record;
    EXPECT_EQ(table->CreateRecord(record), sc::SC_E_NO_ACTIVE_EDIT);
}

// The permanent fault code is registered with its name and storage category.
TEST(SqliteCreateRecordLifecycle, DatabaseFaultedErrorCodeHasNameAndCategory)
{
    EXPECT_STREQ(sc::GetErrorCodeName(sc::SC_E_DATABASE_FAULTED), L"SC_E_DATABASE_FAULTED");
    EXPECT_STREQ(sc::GetErrorCodeCategory(sc::SC_E_DATABASE_FAULTED), L"约束与存储错误");
    EXPECT_TRUE(sc::Failed(sc::SC_E_DATABASE_FAULTED));
}
