#ifndef SCC_KVSTORE_ITEM_ANCHOR_
#define SCC_KVSTORE_ITEM_ANCHOR_

#include "kvstore/item_version.h"
#include "common/sys_config.h"
#include "common/sys_logger.h"
#include "common/sys_stats.h"
#include "common/utils.h"
#include "common/types.h"
#include <thread>
#include <list>
#include <utility>
#include <string>
#include <boost/format.hpp>
#include <iostream>

namespace scc {

    class ItemAnchor {
    public:
        ItemAnchor(std::string itemKey, int numReplicas, int replicaId);

        ~ItemAnchor();

        void InsertVersion(ItemVersion *version);

        static int _replicaId;

#if defined(H_CURE) || defined(WREN)

        ItemVersion *LatestSnapshotVersion(const PhysicalTimeSpec &snapshotLDT, const PhysicalTimeSpec &snapshotRST);

#elif defined(CURE)
        ItemVersion *LatestSnapshotVersion(const std::vector<PhysicalTimeSpec> &snapshotVector);

#endif

        void MarkLocalUpdatePersisted(ItemVersion *version);

        std::string ShowItemVersions();

        std::string _itemKey;
    private:

        std::mutex _itemMutex;
        std::vector<ItemVersion *> _latestVersion;
        ItemVersion *_lastAddedVersion;

        void _calculateUserPercievedStalenessTime(ItemVersion *firstNotVisibleItem, int replicaId,
                                                  PhysicalTimeSpec timeGet, std::string stalenessStr);

        bool isHot(std::string basic_string);

        inline ItemVersion *_getLatestItem();

        inline bool _isGreaterOrEqual(std::vector<PhysicalTimeSpec> v1, std::vector<PhysicalTimeSpec> v2);

        inline PhysicalTimeSpec _maxElem(std::vector<PhysicalTimeSpec> v);

        int getNextItemIndex(const std::vector<ItemVersion *> &current);

#if defined(H_CURE) || defined(WREN)
        bool isItemVisible(ItemVersion *next, const PhysicalTimeSpec &snapshotLDT, const PhysicalTimeSpec &snapshotRST);
#elif defined(CURE)
        bool isItemVisible(ItemVersion *next, const std::vector<PhysicalTimeSpec> &dv);
#endif

        bool areItemsRemoteDepLessThanSnapshotVector(const ItemVersion *next,
                                                     const std::vector<PhysicalTimeSpec> &snapshotVector) const;
    };

} // namespace scc

#endif
