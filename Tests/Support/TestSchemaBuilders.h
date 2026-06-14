#pragma once

#include "SCStorage.h"

namespace sc = StableCore::Storage;

// Column builder helpers
sc::SCColumnDef MakeIntColumn(const wchar_t* name, bool nullable = false);
sc::SCColumnDef MakeStringColumn(const wchar_t* name, bool nullable = false);

// Constraint builder helpers
sc::SCConstraintDef MakeUniqueConstraint(const wchar_t* name, const wchar_t* columnName);
sc::SCConstraintDef MakeForeignKeyConstraint(const wchar_t* name,
                                             const wchar_t* columnName,
                                             const wchar_t* targetTable,
                                             const wchar_t* targetColumn);

// Index builder helpers
sc::SCIndexDef MakeIndex(const wchar_t* name, const wchar_t* columnName);
sc::SCIndexDef MakeCompositeIndex(const wchar_t* name, const wchar_t* firstColumn, const wchar_t* secondColumn);

// Table builder helpers
sc::SCTablePtr CreateWidthOnlyBeamTable(sc::SCDbPtr& db);
sc::SCTablePtr CreateBeamTable(sc::SCDbPtr& db);
sc::SCTablePtr CreateQueryableBeamTable(sc::SCDbPtr& db);
sc::SCTablePtr CreateCompositeIndexedBeamTable(sc::SCDbPtr& db);
sc::SCTablePtr CreateCompositeIndexedBeamTableWithLegacyWidth(sc::SCDbPtr& db);
sc::SCTablePtr CreateDescendingCompositeIndexedBeamTable(sc::SCDbPtr& db);
sc::SCTablePtr CreateTripleCompositeIndexedBeamTable(sc::SCDbPtr& db);
sc::SCTablePtr CreateDescendingTripleCompositeIndexedBeamTable(sc::SCDbPtr& db);
sc::SCTablePtr CreateCompetingCompositeIndexedBeamTable(sc::SCDbPtr& db);
sc::SCTablePtr CreateNullableCompositeIndexedBeamTable(sc::SCDbPtr& db);
