#ifndef KINETIC_KVSTORE_H
#define KINETIC_KVSTORE_H 1

#include "libmemcached/memcached.h"
#include "kvstore.hh"
#include "item.hh"
#include "histo.hh"
#include "stats.hh"
#include "configuration.hh"
#include "kinetic-kvstore/kinetic-notifier.hh"
#include "kinetic-kvstore/kinetic-stats.hh"

/** Options flags for document iteration */
typedef uint64_t kineticstore_options;

enum {
	KINETICSTORE_NO_OPTIONS = 0,
	/*
	 * Retrieve keys only
	 */
	KINETICSTORE_KEYS_ONLY = 1,
	/**
	 * Retrieve all kvs
	 */
	KINETICSTORE_ALL_DATA = 2,
	/**
	 * Retrieve item count
	 */
	KINETICSTORE_ITEM_COUNT = 3,
	/**
	 * Send or retrieve only deleted items.
	 */
	KINETICSTORE_DELETES_ONLY = 4,
	/**
	 * Send only non-deleted items.
	 */
	KINETICSTORE_NO_DELETES = 5,

};

enum {
	KINETICSTORE_VBUCKET_STATE = 2000,
};

/**
 * Stats and timings for kineticKVStore
 */
class KineticKVStoreStats {

public:
    /**
     * Default constructor
     */
    KineticKVStoreStats() :
      docsCommitted(0), numOpen(0), numClose(0),
      numLoadedVb(0), numGetFailure(0), numSetFailure(0),
      numDelFailure(0), numOpenFailure(0), numVbSetFailure(0),
      readSizeHisto(ExponentialGenerator<size_t>(1, 2), 25),
      writeSizeHisto(ExponentialGenerator<size_t>(1, 2), 25) {
    }

    void reset() {
        docsCommitted.set(0);
        numOpen.set(0);
        numClose.set(0);
        numLoadedVb.set(0);
        numGetFailure.set(0);
        numSetFailure.set(0);
        numDelFailure.set(0);
        numOpenFailure.set(0);
        numVbSetFailure.set(0);
        numCommitRetry.set(0);

        readTimeHisto.reset();
        readSizeHisto.reset();
        writeTimeHisto.reset();
        writeSizeHisto.reset();
        delTimeHisto.reset();
        commitHisto.reset();
        commitRetryHisto.reset();
        saveDocsHisto.reset();
        batchSize.reset();
        //fsStats.reset();
    }

    // the number of docs committed
    Atomic<size_t> docsCommitted;
    // the number of open() calls
    Atomic<size_t> numOpen;
    // the number of close() calls
    Atomic<size_t> numClose;
    // the number of vbuckets loaded
    Atomic<size_t> numLoadedVb;

    //stats tracking failures
    Atomic<size_t> numGetFailure;
    Atomic<size_t> numSetFailure;
    Atomic<size_t> numDelFailure;
    Atomic<size_t> numOpenFailure;
    Atomic<size_t> numVbSetFailure;
    Atomic<size_t> numCommitRetry;

    /* for flush and vb delete, no error handling in CouchKVStore, such
     * failure should be tracked in MC-engine  */

    // How long it takes us to complete a read
    Histogram<hrtime_t> readTimeHisto;
    // How big are our reads?
    Histogram<size_t> readSizeHisto;
    // How long it takes us to complete a write
    Histogram<hrtime_t> writeTimeHisto;
    // How big are our writes?
    Histogram<size_t> writeSizeHisto;
    // Time spent in delete() calls.
    Histogram<hrtime_t> delTimeHisto;
    // Time spent in kinetic drive commit
    Histogram<hrtime_t> commitHisto;
    // Time spent in kinetic drive commit retry
    Histogram<hrtime_t> commitRetryHisto;
    // Time spent in kinetic save documents
    Histogram<hrtime_t> saveDocsHisto;
    // Batch size of saveDocs calls
    Histogram<size_t> batchSize;

    // Stats from the underlying kinetic drive operations.
    KineticstoreStats fsStats;
};

class EventuallyPersistentEngine;
class EPStats;

typedef union {
    Callback <mutation_result> *setCb;
    Callback <int> *delCb;
} KineticRequestCallback;

const size_t KINETICSTORE_METADATA_SIZE(2 * sizeof(uint32_t) + sizeof(uint64_t));

/**
 * Class representing a document to be persisted in kinetic drive.
 */
class KineticRequest
{
public:
    /**
     * Constructor
     *
     * @param it Item instance to be persisted
     * @param rev vbucket database revision number
     * @param cb persistence callback
     * @param del flag indicating if it is an item deletion or not
     */
    KineticRequest(const Item &it, uint64_t rev, KineticRequestCallback &cb, bool del);

    /**
     * Get the vbucket id of a document to be persisted
     *
     * @return vbucket id of a document
     */
    uint16_t getVBucketId(void) {
        return vbucketId;
    }

    /**
     * Get the revision number of the vbucket database file
     * where the document is persisted
     *
     * @return revision number of the corresponding vbucket database file
     */
    uint64_t getRevNum(void) {
        return fileRevNum;
    }

    /**
     * Get the callback instance for SET
     *
     * @return callback instance for SET
     */
    Callback<mutation_result> *getSetCallback(void) {
        return callback.setCb;
    }

    /**
     * Get the callback instance for DELETE
     *
     * @return callback instance for DELETE
     */
    Callback<int> *getDelCallback(void) {
        return callback.delCb;
    }

    /**
     * Get the sequence number of a document to be persisted
     *
     * @return sequence number of a document
     */
    int64_t getItemId(void) {
        return itemId;
    }

    /**
     * Get the time in ns elapsed since the creation of this instance
     *
     * @return time in ns elapsed since the creation of this instance
     */
    hrtime_t getDelta() {
        return (gethrtime() - start) / 1000;
    }

    /**
     * Get the value to be persisted
     */
    const char* getValue(void) const {
    	return value->getData();
    }

    /**
     * Get the length of a document body to be persisted
     *
     * @return length of a document body
     */
    size_t getNBytes() {
        return valuelen;
    }

    /**
     * Return true if the document to be persisted is for DELETE
     *
     * @return true if the document to be persisted is for DELETE
     */
    bool isDelete() {
        return deleteItem;
    };

    /**
     * Get the key of a document to be persisted
     *
     * @return key of a document to be persisted
     */
    const std::string& getKey(void) const {
        return key;
    }

private :
    value_t value;
    size_t valuelen;
    uint8_t meta[KINETICSTORE_METADATA_SIZE];
    uint16_t vbucketId;
    uint64_t fileRevNum;
    std::string key;
    int64_t itemId;
    bool deleteItem;
    KineticRequestCallback callback;

    hrtime_t start;
};

/**
 * KVStore with kinetic as the underlying storage system
 */
class KineticKVStore : public KVStore
{
public:
    /**
     * Constructor
     *
     * @param theEngine EventuallyPersistentEngine instance
     * @param read_only flag indicating if this kvstore instance is for read-only operations
     */
    KineticKVStore(EPStats &stats, Configuration &config, bool read_only = false);

    /**
     * Copy constructor
     *
     * @param from the source kvstore instance
     */
    KineticKVStore(const KineticKVStore &from);

    /**
     * Deconstructor
     */
    virtual ~KineticKVStore() {
        close();
    }

    /**
     * Reset database to a clean state.
     */
    void reset(void);

    /**
     * Begin a transaction (if not already in one).
     *
     * @return true if the transaction is started successfully
     */
    bool begin(void) {
        assert(!isReadOnly());
        intransaction = true;
        return intransaction;
    }

    /**
     * Commit a transaction (unless not currently in one).
     *
     * @return true if the commit is completed successfully.
     */
    bool commit(void);

    /**
     * Rollback a transaction (unless not currently in one).
     */
    void rollback(void) {
        assert(!isReadOnly());
        if (intransaction) {
            intransaction = false;
        }
    }

    /**
     * Query the properties of the underlying storage.
     *
     * @return properties of the underlying storage system
     */
    StorageProperties getStorageProperties(void);

    /**
     * Insert or update a given document.
     *
     * @param itm instance representing the document to be inserted or updated
     * @param cb callback instance for SET
     */
    void set(const Item &itm, Callback<mutation_result> &cb);

    /**
     * Retrieve the document with a given key from the underlying storage system.
     *
     * @param key the key of a document to be retrieved
     * @param rowid the sequence number of a document
     * @param vb vbucket id of a document
     * @param cb callback instance for GET
     */
    void get(const std::string &key, uint64_t rowid,
             uint16_t vb, Callback<GetValue> &cb);

    /**
     * Retrieve the multiple documents from the underlying storage system at once.
     *
     * @param vb vbucket id of a document
     * @param itms list of items whose documents are going to be retrieved
     */
    void getMulti(uint16_t vb, vb_bgfetch_queue_t &itms);

    /**
     * Delete a given document from the underlying storage system.
     *
     * @param itm instance representing the document to be deleted
     * @param rowid the sequence number of a document
     * @param cb callback instance for DELETE
     */
    void del(const Item &itm, uint64_t rowid,
             Callback<int> &cb);

    /**
     * Delete a given vbucket database instance from the underlying storage system
     *
     * @param vbucket vbucket id
     * @param recreate true if we need to create an empty vbucket after deletion
     * @return true if the vbucket deletion is completed successfully.
     */
    bool delVBucket(uint16_t vbucket, bool recreate);

    /**
     * Retrieve the list of persisted vbucket states
     *
     * @return vbucket state map instance where key is vbucket id and
     * value is vbucket state
     */
    vbucket_map_t listPersistedVbuckets(void);

    /**
     * Retrieve ths list of persisted engine stats
     *
     * @param stats map instance where the persisted engine stats will be added
     */
    void getPersistedStats(std::map<std::string, std::string> &stats);

    /**
     * Persist a snapshot of the engine stats in the underlying storage.
     *
     * @param engine_stats map instance that contains all the engine stats
     * @return true if the snapshot is done successfully
     */
    bool snapshotStats(const std::map<std::string, std::string> &engine_stats);

    /**
     * Persist a snapshot of the vbucket states in the underlying storage system.
     *
     * @param vb_stats map instance that contains all the vbucket states
     * @return true if the snapshot is done successfully
     */
    bool snapshotVBuckets(const vbucket_map_t &vb_states);

    /**
     * Retrieve all the documents from the underlying storage system.
     *
     * @param cb callback instance to process each document retrieved
     */
    void dump(shared_ptr<Callback<GetValue> > cb);

    /**
     * Retrieve all the documents for a given vbucket from the storage system.
     *
     * @param vb vbucket id
     * @param cb callback instance to process each document retrieved
     */
    void dump(uint16_t vb, shared_ptr<Callback<GetValue> > cb);

    /**
     * Retrieve all the keys from the underlying storage system.
     *
     * @param vbids list of vbucket ids whose document keys are going to be retrieved
     * @param cb callback instance to process each key retrieved
     */
    void dumpKeys(const std::vector<uint16_t> &vbids,  shared_ptr<Callback<GetValue> > cb);

    /**
     * Retrieve the list of keys and their meta data for a given
     * vbucket, which were deleted.
     * @param vb vbucket id
     * @param cb callback instance to process each key and its meta data
     */
    void dumpDeleted(uint16_t vb,  shared_ptr<Callback<GetValue> > cb);

    /**
     * Does the underlying storage system support key-only retrieval operations?
     *
     * @return true if key-only retrieval is supported
     */
    bool isKeyDumpSupported() {
        return true;
    }

    /**
     * Get the estimated number of items that are going to be loaded during warmup.
     *
     * @param items number of estimated items to be loaded during warmup
     * @return true if the estimation is completed successfully
     */
    bool getEstimatedItemCount(size_t &items);

    /**
     * Get the number of deleted items that are persisted to a vbucket file
     *
     * @param vbid The vbucket if of the file to get the number of deletes for
     */
    size_t getNumPersistedDeletes(uint16_t vbid);

    /**
     * Perform the pre-optimizations before persisting dirty items
     *
     * @param items list of dirty items that can be pre-optimized
     */
    void optimizeWrites(std::vector<queued_item> &items);

    /**
     * Add all the kvstore stats to the stat response
     *
     * @param prefix stat name prefix
     * @param add_stat upstream function that allows us to add a stat to the response
     * @param cookie upstream connection cookie
     */
    void addStats(const std::string &prefix, ADD_STAT add_stat, const void *cookie);

    /**
     * Add all the kvstore timings stats to the stat response
     *
     * @param prefix stat name prefix
     * @param add_stat upstream function that allows us to add a stat to the response
     * @param cookie upstream connection cookie
     */
    void addTimingStats(const std::string &prefix, ADD_STAT add_stat,
                        const void *c);

    /**
     * Resets kinetic stats
     */
    void resetStats() {
        st.reset();
    }

    void processTxnSizeChange(size_t txn_size) {
        (void) txn_size;
    }
    void setVBBatchCount(size_t batch_count) {
        (void) batch_count;
    }
    void destroyInvalidVBuckets(bool destroyOnlyOne = false) {
        (void) destroyOnlyOne;
    }

    ENGINE_ERROR_CODE kineticErr2EngineErr(memcached_return_t errCode);

    KineticKVStoreStats &getCKVStoreStat(void) { return st; }

protected:
    void loadDB(shared_ptr<Callback<GetValue> > cb, bool keysOnly,
                std::vector<uint16_t> *vbids,
                kineticstore_options options=KINETICSTORE_NO_OPTIONS);

    bool setVBucketState(uint16_t vbucketId, vbucket_state &vbstate,
                         uint32_t vb_change_type, bool newfile = false,
                         bool notify = true);
    bool resetVBucket(uint16_t vbucketId, vbucket_state &vbstate) {
        return setVBucketState(vbucketId, vbstate, VB_STATE_CHANGED, true, true);
    }

    template <typename T>
    void addStat(const std::string &prefix, const char *nm, T &val,
                 ADD_STAT add_stat, const void *c);

private:
    void operator=(const KineticKVStore &from);

    void open();
    void close();
    bool commit2kineticstore(void);
    void queueItem(KineticRequest *req);

    void commitCallback(KineticRequest **committedReqs, int numReqs,
    		            memcached_return_t errCode);
private:
    memcached_st *memc;
    memcached_server_st *memc_servers;

    EPStats &epStats;
    Configuration &configuration;
    const std::string dbname;
    KineticNotifier *kineticNotifier;
    std::vector<KineticRequest *> pendingReqsQ;
    size_t pendingCommitCnt;
    bool intransaction;

    /* all stats */
    KineticKVStoreStats   st;
    couch_file_ops statCollectingFileOps;
    /* vbucket state cache*/
    vbucket_map_t cachedVBStates;
    /* deleted docs in each file*/
    std::map<uint16_t, size_t> cachedDeleteCount;
};

#endif /* KINETIC_KVSTORE_H */
