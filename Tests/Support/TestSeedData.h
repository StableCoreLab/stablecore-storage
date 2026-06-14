#pragma once

#include "SCStorage.h"

namespace sc = StableCore::Storage;

// Seed helper functions for populating test data
void SeedQueryableBeamRows(const sc::SCTablePtr& table, sc::SCDbPtr& db);
void SeedSingleBeam(const sc::SCTablePtr& table, sc::SCDbPtr& db, const wchar_t* name, std::int64_t width);
