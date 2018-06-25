#include "kvstore/item_anchor.h"

#ifdef ATOMIC_LATEST_VERSION
#define LATEST_VERSION(A) {\
                    std::lock_guard<std::mutex> lk(_itemMutex);\
                    A = _latestVersion;\
                        }
#else
#define LATEST_VERSION(A) for(int i=0;i<_latestVersion.size();i++){\
                                A.push_back(_latestVersion[i]);\
                        }
#endif

#ifdef H_CURE
#define IS_LOCAL_UPDATE_CONDITION()  next->SrcReplica == _replicaId && next->UT <= snapshotLDT && next->RST <= snapshotRST
#define IS_REMOTE_UPDATE_CONDITION() next->SrcReplica != _replicaId && next->UT <= snapshotRST

#elif defined(WREN)
#define IS_LOCAL_UPDATE_CONDITION()  next->SrcReplica == _replicaId && next->UT <= snapshotLDT && next->RST <= snapshotRST
#define IS_REMOTE_UPDATE_CONDITION() next->SrcReplica != _replicaId && next->UT <= snapshotRST && next->RST <= snapshotLDT

#elif defined(CURE)

#define IS_LOCAL_UPDATE_CONDITION()  (next->SrcReplica == _replicaId) && (next->UT <= snapshotVector[next->SrcReplica] && areItemsRemoteDepLessThanSnapshotVector(next, snapshotVector))
#define IS_REMOTE_UPDATE_CONDITION() (next->SrcReplica != _replicaId) && (next->UT <= snapshotVector[next->SrcReplica])

#ifdef CURE_AS_WREN_CONDITIONS
#define IS_LOCAL_UPDATE_CONDITION()  next->SrcReplica == _replicaId && next->UT<=dv[next->SrcReplica] && rstItem<=gst
#define IS_REMOTE_UPDATE_CONDITION() next->SrcReplica != _replicaId && next->UT<=gst
#endif

#endif


namespace scc {


    int ItemAnchor::_replicaId(0);

/*
#ifdef NO_LOCAL_WAIT
#define NO_LOCAL() (i!=localReplicaId) &&
#else
#define NO_LOCAL()
#endif

#define GREATER_OR_EQUAL(b, v1, v2) do{ \
                                    for(int i = 0; i < v1.size(); i++)  { \
                                        if (NO_LOCAL() (v1[i] < v2[i])) { \
                                            b = false; \
                                            break; \
                                        }\
                                    }\
                                    }while(0)


#define MAX_ELEM(v, e) do{ \
                    e = v[0]; \
                    for (int i = 1; i < v.size(); i++) { \
                        if (v[i] > e) { \
                            e = v[i]; \
                        } \
                    } \
                    }while(0)
*/

    ItemAnchor::ItemAnchor(std::string itemKey, int numReplicas, int replicaId) {
        _latestVersion.resize(numReplicas);
        _itemKey = itemKey;
    }

    ItemAnchor::~ItemAnchor() {
    }

    void ItemAnchor::InsertVersion(ItemVersion *version) {
        std::lock_guard<std::mutex> lk(_itemMutex);

        ASSERT(version != NULL);
        int replica = version->SrcReplica;
        ItemVersion *lv = _latestVersion.at(replica);

        // head is NULL
        if (lv == NULL) {
            version->Next = NULL;
            _latestVersion[replica] = version;
            _lastAddedVersion = version;
            ASSERT(_latestVersion[replica] != NULL);
            return;
        }

        ASSERT(*version >= *lv);

        // insert to head
        version->Next = lv;
        assert(version->Key != " ");
        _latestVersion[replica] = version;

        if (_lastAddedVersion->UT < version->UT)
            _lastAddedVersion = version;

        ASSERT(_latestVersion[replica] != NULL);
    }

    void ItemAnchor::_calculateUserPercievedStalenessTime(ItemVersion *firstNotVisibleItem, int replicaId,
                                                          PhysicalTimeSpec timeGet, std::string stalenessStr) {

        PhysicalTimeSpec timeNow = Utils::GetCurrentClockTime();

        double resTime = (timeNow - firstNotVisibleItem->RIT).toMilliSeconds();

        {
            std::lock_guard<std::mutex> lk(SysStats::UserPerceivedStalenessTimeMutex);
            SysStats::UserPerceivedStalenessTime.push_back(resTime);
        }

        if (resTime > SysStats::MaxUserPercievedStalenessTime) {
            SysStats::MaxUserPercievedStalenessTime = resTime;
        }

        if (resTime < SysStats::MinUserPercievedStalenessTime) {
            SysStats::MinUserPercievedStalenessTime = resTime;
        }

    }

#if defined(H_CURE) || defined(WREN)

    ItemVersion *
    ItemAnchor::LatestSnapshotVersion(const PhysicalTimeSpec &snapshotLDT, const PhysicalTimeSpec &snapshotRST) {

#elif defined(CURE)

    ItemVersion *ItemAnchor::LatestSnapshotVersion(const std::vector<PhysicalTimeSpec> &snapshotVector) {

#endif

        std::vector<ItemVersion *> current;
        LATEST_VERSION(current);
        ItemVersion *next = NULL;

#ifdef MEASURE_STATISTICS
        std::string stalenessStr = "";
        PhysicalTimeSpec timeGet = Utils::GetCurrentClockTime();
        ItemVersion *firstNotVisibleItem = NULL;
        bool stale = false;
        int numVersionsBehind = 0;
#endif

        while (true) {
            int nextIndex = getNextItemIndex(current);
            next = current[nextIndex];

            assert(next != NULL);


#if defined(H_CURE) || defined(WREN)
            if (isItemVisible(next, snapshotLDT, snapshotRST))

#elif defined(CURE)
            if (isItemVisible(next, snapshotVector))
#endif
            {
                // the item is visible and can be returned
                break;
            }

#ifdef MEASURE_STATISTICS
                if (!stale) {
                    stale = true;
                    firstNotVisibleItem = next;
                }
                numVersionsBehind++;

#endif

            assert(current[nextIndex] == next);
            current[nextIndex] = next->Next;
        }

        assert(next != NULL);

#ifdef MEASURE_STATISTICS

        if (stale) {

            assert(firstNotVisibleItem != NULL);
            _calculateUserPercievedStalenessTime(firstNotVisibleItem, -1, timeGet, stalenessStr);

            SysStats::NumReturnedStaleItemVersions++;

            {
                std::lock_guard<std::mutex> lk(SysStats::NumFresherVersionsInItemChainMutex);
                SysStats::NumFresherVersionsInItemChain.push_back(numVersionsBehind);
            }

            if (next->SrcReplica == _replicaId) {
                SysStats::NumReadLocalStaleItems++;
            } else {
                SysStats::NumReadRemoteStaleItems++;
            }

        } else {
            if (next->SrcReplica == _replicaId) {
                SysStats::NumReadLocalItems++;
            } else {
                SysStats::NumReadRemoteItems++;
            }
        }

        SysStats::NumReturnedItemVersions ++;
#endif
        return next;
    }

#if defined(H_CURE) || defined(WREN)

    bool
    ItemAnchor::isItemVisible(ItemVersion *next, const PhysicalTimeSpec &snapshotLDT,
                              const PhysicalTimeSpec &snapshotRST) {

        return (IS_LOCAL_UPDATE_CONDITION() || IS_REMOTE_UPDATE_CONDITION());
    }

#elif defined(CURE)

    bool
    ItemAnchor::isItemVisible(ItemVersion *next, const std::vector<PhysicalTimeSpec> &snapshotVector) {

        return (IS_LOCAL_UPDATE_CONDITION() || IS_REMOTE_UPDATE_CONDITION());
    }

    bool ItemAnchor::areItemsRemoteDepLessThanSnapshotVector(const ItemVersion *next,
                                                             const std::vector<PhysicalTimeSpec> &snapshotVector) const {

        for (int i = 0; i < next->DV.size(); i++) {
            if (i != next->SrcReplica) {
                if (next->DV[i] > snapshotVector[i]) {
                    return false;
                }
            }
        }

        return true;
    }

#endif

    int ItemAnchor::getNextItemIndex(const std::vector<ItemVersion *> &current) {

        int nextIndex = 0;
        ItemVersion *next = current[0];

        for (int i = 1; i < current.size(); ++i) {
            ItemVersion *cur = current[i];

            if (next == NULL || (cur != NULL && next->UT < cur->UT)) {
                next = cur;
                nextIndex = i;
            }
        }

        return nextIndex;
    }

    void ItemAnchor::MarkLocalUpdatePersisted(ItemVersion *version) {
        std::lock_guard<std::mutex> lk(_itemMutex);
        version->Persisted = true;
    }

    std::string ItemAnchor::ShowItemVersions() {
        std::string itemVersions = (boost::format("[KEY %s]\n") % _itemKey).str();

        std::vector<ItemVersion *> current;
        {
            std::lock_guard<std::mutex> lk(_itemMutex);
            current = _latestVersion;
        }

        ItemVersion *next;

        while (true) {

            int nextIndex = 0;
            next = current[0];

            PhysicalTimeSpec prevTime;

            for (int i = 1; i < current.size(); i++) {
                ItemVersion *cur = current[i];

                if (next == NULL || (cur != NULL && next->UT < cur->UT)) {

                    if (next != NULL) {
                        prevTime = next->UT;
                    }
                    next = cur;
                    nextIndex = i;
                }

            }
            PhysicalTimeSpec timeNow = Utils::GetCurrentClockTime();
            if (next == NULL) break;
#if defined(H_CURE) || defined(WREN)
            itemVersions += (boost::format("->(%s, lut %d, ut %s, rit %s, sr %d, timeNow %s, rst %s)\n")
                             % next->Value
                             % next->LUT
                             % Utils::physicaltime2str(next->UT)
                             % Utils::physicaltime2str(next->RIT)
                             % next->SrcReplica
                             % Utils::physicaltime2str(timeNow)
                             % Utils::physicaltime2str(next->RST)).str();

#elif defined(CURE)
            std::string dvStr = "[";

            for (int i = 0; i < next->DV.size(); i++) {
                dvStr += (boost::format("%s,") % Utils::physicaltime2str(next->DV[i])).str();

            }
            dvStr += "]";

            itemVersions += (boost::format("->(%s, lut %d, ut %s, rit %s, sr %d, timeNow %s, DV = %s )\n")
                             % next->Value
                             % next->LUT
                             % Utils::physicaltime2str(next->UT)
                             % Utils::physicaltime2str(next->RIT)
                             % next->SrcReplica
                             % Utils::physicaltime2str(timeNow)
                             % dvStr.c_str()).str();
#endif

            ASSERT(current[nextIndex] == next);
            current[nextIndex] = next->Next;

        }

        return itemVersions;
    }


    inline ItemVersion *ItemAnchor::_getLatestItem() {

        std::vector<ItemVersion *> current;
        {
            std::lock_guard<std::mutex> lk(_itemMutex);
            current = _latestVersion;
        }

        ItemVersion *next = current[0];

        for (int i = 1; i < current.size(); ++i) {
            ItemVersion *cur = current[i];

            if (next == NULL || (cur != NULL && next < cur)) {
                next = cur;
            }
        }

        ASSERT(next != NULL);
        return next;
    }


    inline bool ItemAnchor::_isGreaterOrEqual(std::vector<PhysicalTimeSpec> v1,
                                              std::vector<PhysicalTimeSpec> v2) {
        for (int i = 0; i < v1.size(); i++) {
            if (v1[i] < v2[i]) {
                return false;
            }
        }

        return true;
    }

    inline PhysicalTimeSpec ItemAnchor::_maxElem(std::vector<PhysicalTimeSpec> v) {

        PhysicalTimeSpec max = v[0];
        for (int i = 1; i < v.size(); i++) {
            if (v[i] > max) {
                max = v[i];
            }
        }
        return max;
    }

} // namespace scc
