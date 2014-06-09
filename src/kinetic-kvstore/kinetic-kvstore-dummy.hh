#ifndef KINETIC_KVSTORE_DUMMY_H
#define KINETIC_KVSTORE_DUMMY_H 1

#ifdef HAVE_LIBKINETIC
#error "This header file should only be included if you don't have libcouchstore"
#endif

#include "kvstore.hh"

class EPStats;

/**
 * THis is a dummy implementation of the kinetickvstore just to satisfy the
 * linker without a too advanced Makefile (for builds without libkinetic)
 */
class KineticKVStore : public KVStore
{
public:
    KineticKVStore(EPStats &stats, Configuration &config, bool read_only = false);
    KineticKVStore(const KineticKVStore &from);
    void reset();
    bool begin();
    bool commit();
    void rollback();
    StorageProperties getStorageProperties();
    void set(const Item &item,
             Callback<mutation_result> &cb);
    void get(const std::string &key, uint64_t rowid,
             uint16_t vb,
             Callback<GetValue> &cb);
    void del(const Item &itm, uint64_t rowid,
             Callback<int> &cb);
    bool delVBucket(uint16_t vbucket, bool recreate);
    vbucket_map_t listPersistedVbuckets(void);
    bool snapshotStats(const std::map<std::string, std::string> &m);
    bool snapshotVBuckets(const vbucket_map_t &m);
    void dump(shared_ptr<Callback<GetValue> > cb);
    void dump(uint16_t vbid, shared_ptr<Callback<GetValue> > cb);
    void destroyInvalidVBuckets(bool destroyOnlyOne = false);
};

#endif
