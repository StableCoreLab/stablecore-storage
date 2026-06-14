#pragma once

#include "SCQuery.h"

#include <vector>

namespace StableCore::Storage
{

    class ISqliteQueryIndexAccess
    {
    public:
        virtual ~ISqliteQueryIndexAccess() = default;

        virtual ErrorCode AnalyzeCompositeIndexPlan(const QueryPlan& inputPlan, QueryPlan* outPlan) = 0;

        virtual ErrorCode CollectCompositeIndexRecordIds(const QueryPlan& analyzedPlan,
                                                         std::vector<RecordId>* outRecordIds,
                                                         std::uint64_t* outScannedEntries) = 0;
    };

}  // namespace StableCore::Storage
