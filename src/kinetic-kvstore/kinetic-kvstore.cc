/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <string.h>
#include <cstdlib>
#include <cctype>
#include <algorithm>
#include <stdio.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <iostream>
#include <fstream>

#include "common.hh"
#include "kinetic-kvstore/kinetic-kvstore.hh"
#include "kinetic-kvstore/dirutils.hh"
#include "warmup.hh"
#include "tools/cJSON.h"
#include "tools/JSON_checker.h"

#define STATWRITER_NAMESPACE KINETICSTORE_engine
#include "statwriter.hh"
#undef STATWRITER_NAMESPACE

using namespace KineticKVStoreDirectoryUtilities;

static const int MUTATION_FAILED = -1;
static const int DOC_NOT_FOUND = 0;
static const int MUTATION_SUCCESS = 1;

static const int MAX_OPEN_DB_RETRY = 10;

extern "C" {
    static std::string getStrError() {
        const size_t max_msg_len = 256;
        char msg[max_msg_len];
        //KINETICSTORE_last_os_error(msg, max_msg_len);
        std::string errorStr(msg);
        return errorStr;
    }
}

static bool isJSON(const value_t &value)
{/*
    const int len = value->length();
    const unsigned char *data = (unsigned char*) value->getData();
    return checkUTF8JSON(data, len);*/
	return false;
}

/*
static const std::string getJSONObjString(const cJSON *i)
{
    if (i == NULL) {
        return "";
    }
    if (i->type != cJSON_String) {
        abort();
    }
    return i->valuestring;
}
*/
static bool endWithCompact(const std::string &filename)
{
    size_t pos = filename.find(".compact");
    if (pos == std::string::npos || (filename.size() - sizeof(".compact")) != pos) {
        return false;
    }
    return true;
}

static int getMutationStatus(memcached_return_t errCode)
{
    switch (errCode) {
    case MEMCACHED_SUCCESS:
        return MUTATION_SUCCESS;
    case MEMCACHED_NOTSTORED:
    case MEMCACHED_NOTFOUND:
        // this return causes ep engine to drop the failed flush
        // of an item since it does not know about the itme any longer
        return DOC_NOT_FOUND;
    default:
        // this return causes ep engine to keep requeuing the failed
        // flush of an item
        return MUTATION_FAILED;
    }
}

static bool allDigit(std::string &input)
{
    size_t numchar = input.length();
    for(size_t i = 0; i < numchar; ++i) {
        if (!isdigit(input[i])) {
            return false;
        }
    }
    return true;
}

KineticRequest::KineticRequest(const Item &it, uint64_t rev, KineticRequestCallback &cb, bool del) :
    value(it.getValue()), valuelen(it.getNBytes()),
    vbucketId(it.getVBucketId()), fileRevNum(rev),
    key(it.getKey()), deleteItem(del)
{
    bool isjson = false;
    uint64_t cas = htonll(it.getCas());
    uint32_t flags = it.getFlags();
    uint32_t exptime = it.getExptime();
    // Save time of deletion in expiry time field of deleted item's metadata.
    if (del) {
        exptime = ep_real_time();
    }
    exptime = htonl(exptime);

    itemId = (it.getId() <= 0) ? 1 : 0;

    start = gethrtime();
}

KineticKVStore::KineticKVStore(EPStats &stats, Configuration &config, bool read_only) :
    KVStore(read_only), epStats(stats), configuration(config),
    dbname(configuration.getDbname()), kineticNotifier(NULL), pendingCommitCnt(0),
    intransaction(false)
{
    open();
    //statCollectingFileOps = getKINETICSTOREStatsOps(&st.fsStats);
}

KineticKVStore::KineticKVStore(const KineticKVStore &copyFrom) :
    KVStore(copyFrom), epStats(copyFrom.epStats),
    configuration(copyFrom.configuration),
    dbname(copyFrom.dbname),
    kineticNotifier(NULL),
    pendingCommitCnt(0), intransaction(false),
    memc(NULL), memc_servers(NULL)
{
    open();
    //§statCollectingFileOps = getKINETICSTOREStatsOps(&st.fsStats);
}

void KineticKVStore::reset()
{
    assert(!isReadOnly());
    // TODO KineticKVStore::flush() when KINETICSTORE api ready
    RememberingCallback<bool> cb;

    kineticNotifier->flush(cb);
    cb.waitForValue();

    vbucket_map_t::iterator itor = cachedVBStates.begin();
    for (; itor != cachedVBStates.end(); ++itor) {
        uint16_t vbucket = itor->first;
        itor->second.checkpointId = 0;
        itor->second.maxDeletedSeqno = 0;
        resetVBucket(vbucket, itor->second);
    }
}

void KineticKVStore::set(const Item &itm, Callback<mutation_result> &cb)
{
    assert(!isReadOnly());
    assert(intransaction);
    KineticRequestCallback requestcb;

    // each req will be de-allocated after commit
    KineticRequest *req = new KineticRequest(itm, 1, requestcb, false);
    queueItem(req);
}

void KineticKVStore::get(const std::string &key, uint64_t, uint16_t vb,
                       Callback<GetValue> &cb)
{
    hrtime_t start = gethrtime();
    GetValue rv;

    size_t value_length;
    uint32_t flags;
    memcached_return rc;
    char* retrieved_value = memcached_get_with_vbucket(memc,
    							         const_cast<char *>(key.c_str()),
    							         key.size(),
    							         vb,
    							         &value_length,
    							         &flags,
    							         &rc);
    if (rc == MEMCACHED_SUCCESS) {
      fprintf(stderr, "The key '%s' returned value '%s'.\n", key.c_str(), retrieved_value);
      Item *it = new Item(key.c_str(), (size_t)key.size(),
                          0, (time_t)0, retrieved_value, value_length,
                          0, -1, vb);

      rv = GetValue(it);
      free(retrieved_value);

      ++epStats.io_num_read;
      epStats.io_read_bytes += value_length;
      // record stats
      st.readTimeHisto.add((gethrtime() - start) / 1000);
      st.readSizeHisto.add(key.length() + rv.getValue()->getNBytes());
    }
    else {
    	++st.numGetFailure;
    	LOG(EXTENSION_LOG_WARNING,
          "Warning: failed to retrieve key value from "
          "key=%s error=%s", key.c_str(), memcached_strerror(memc, rc));
    }

    rv.setStatus(kineticErr2EngineErr(rc));
    cb.callback(rv);
}

void KineticKVStore::getMulti(uint16_t vb, vb_bgfetch_queue_t &itms)
{
    hrtime_t start = gethrtime();

    VBucketBGFetchItem *item2fetch;
    vb_bgfetch_queue_t::iterator itr = itms.begin();
    for (; itr != itms.end(); itr++) {
        std::list<VBucketBGFetchItem *> &fetches = (*itr).second;
        std::list<VBucketBGFetchItem *>::iterator fitr = fetches.begin();
        for (; fitr != fetches.end(); fitr++) {
            size_t value_length;
            uint32_t flags;
            memcached_return rc;
            char* retrieved_value = memcached_get_with_vbucket(memc,
            							         const_cast<char *>((*fitr)->key.c_str()),
            							         (*fitr)->key.size(),
            							         vb,
            							         &value_length,
            							         &flags,
            							         &rc);
            if (rc == MEMCACHED_SUCCESS) {
                Item *it = new Item((*fitr)->key.c_str(),
                					(size_t)(*fitr)->key.size(),
                                    flags,
                                    (time_t)0,
                                    retrieved_value,
                                    value_length,
                                    0, // cas
                                    -1, // seq
                                    vb);

                GetValue returnVal(it, ENGINE_SUCCESS);
                (*fitr)->value = returnVal;
                free(retrieved_value);

                ++epStats.io_num_read;
                epStats.io_read_bytes += value_length;
                // record stats
                st.readTimeHisto.add((gethrtime() - start) / 1000);
                st.readSizeHisto.add((*fitr)->key.size() + value_length);
            } else {
                LOG(EXTENSION_LOG_WARNING, "Warning: failed to read database by"
                    " sequence id = %lld, vBucketId = %d "
                    "key = %s\n",
                    (*fitr)->value.getId(), vb, (*fitr)->key.c_str());
                (*fitr)->value.setStatus(kineticErr2EngineErr(rc));
            }
        }
    }
}

void KineticKVStore::del(const Item &itm,
                       uint64_t,
                       Callback<int> &cb)
{
    assert(!isReadOnly());
    assert(intransaction);
    KineticRequestCallback requestcb;

    requestcb.delCb = &cb;

    // each req will be de-allocated after commit
    KineticRequest *req = new KineticRequest(itm, 1, requestcb, true);
    queueItem(req);
}

bool KineticKVStore::delVBucket(uint16_t vbucket, bool recreate)
{
    assert(!isReadOnly());
    assert(kineticNotifier);
    RememberingCallback<bool> cb;

    kineticNotifier->delVBucket(vbucket, cb);
    cb.waitForValue();

    if (recreate) {
        vbucket_state vbstate(vbucket_state_dead, 0, 0);
        vbucket_map_t::iterator it = cachedVBStates.find(vbucket);
        if (it != cachedVBStates.end()) {
            vbstate.state = it->second.state;
        }
        cachedVBStates[vbucket] = vbstate;
        resetVBucket(vbucket, vbstate);
    } else {
        cachedVBStates.erase(vbucket);
    }
    return cb.val;
}

vbucket_map_t KineticKVStore::listPersistedVbuckets()
{
    if (!cachedVBStates.empty()) {
        cachedVBStates.clear();
    }

    const char* keys[] = {"key1"};
    size_t key_lengths[] = {strlen("key1")};
    uint32_t flags;

    memcached_return_t rc = memcached_mget_with_vbucket(memc, keys, key_lengths, 1, KINETICSTORE_VBUCKET_STATE);
    if (rc == MEMCACHED_SUCCESS) {
        memcached_return_t error;

        for(;;) {
          char key[1024];
          size_t keylen;
          char *value;
          size_t value_length;

          value = memcached_fetch(memc, key, &keylen, &value_length, &flags, &error);
          if (value == NULL) {
            break;
          }

          vbucket_state_t state = (vbucket_state_t)atoi(value);
          vbucket_state vb_state(state, 0, 0);
          uint16_t vbID;
          sscanf(key, "vb%hu", &vbID);

          /* insert populated state to the array to return to the caller */
          cachedVBStates[vbID] = vb_state;
          /* update stat */
          ++st.numLoadedVb;

          printf("vbid:%u) key:%s, klen:%d, vlen:%ld, flags:%d, value:%s\n", vbID, key, keylen, value_length, flags, value);
          free(value);
          value = NULL;
        }
    }

    return cachedVBStates;
}

void KineticKVStore::getPersistedStats(std::map<std::string, std::string> &stats)
{
    char *buffer = NULL;
    std::string fname = dbname + "/stats.json";
    if (access(fname.c_str(), R_OK) == -1) {
        return ;
    }

    std::ifstream session_stats;
    session_stats.exceptions (session_stats.failbit | session_stats.badbit);
    try {
        session_stats.open(fname.c_str(), std::ifstream::binary);
        session_stats.seekg (0, std::ifstream::end);
        int flen = session_stats.tellg();
        if (flen < 0) {
            LOG(EXTENSION_LOG_WARNING,
                "Warning: error in session stats ifstream!!!");
            session_stats.close();
            return;
        }
        session_stats.seekg (0, std::ifstream::beg);
        buffer = new char[flen + 1];
        session_stats.read(buffer, flen);
        session_stats.close();
        buffer[flen] = '\0';

        cJSON *json_obj = cJSON_Parse(buffer);
        if (!json_obj) {
            LOG(EXTENSION_LOG_WARNING,
                "Warning: failed to parse the session stats json doc!!!");
            delete[] buffer;
            return;
        }

        int json_arr_size = 0; //cJSON_GetArraySize(json_obj);
        for (int i = 0; i < json_arr_size; ++i) {
            cJSON *obj = cJSON_GetArrayItem(json_obj, i);
            if (obj) {
                stats[obj->string] = obj->valuestring ? obj->valuestring : "";
            }
        }
        cJSON_Delete(json_obj);

    } catch (const std::ifstream::failure &e) {
        LOG(EXTENSION_LOG_WARNING,
            "Warning: failed to load the engine session stats "
            " due to IO exception \"%s\"", e.what());
    } catch (...) {
        LOG(EXTENSION_LOG_WARNING,
            "Warning: failed to load the engine session stats "
            " due to IO exception");
    }

    delete[] buffer;
}

bool KineticKVStore::snapshotVBuckets(const vbucket_map_t &vbstates)
{
    assert(!isReadOnly());
    bool success = true;

    vbucket_map_t::const_reverse_iterator iter = vbstates.rbegin();
    for (; iter != vbstates.rend(); ++iter) {
        uint16_t vbucketId = iter->first;
        vbucket_state vbstate = iter->second;
        vbucket_map_t::iterator it = cachedVBStates.find(vbucketId);
        uint32_t vb_change_type = VB_NO_CHANGE;
        if (it != cachedVBStates.end()) {
            if (it->second.state != vbstate.state) {
                vb_change_type |= VB_STATE_CHANGED;
            }
            if (it->second.checkpointId != vbstate.checkpointId) {
                vb_change_type |= VB_CHECKPOINT_CHANGED;
            }
            if (vb_change_type == VB_NO_CHANGE) {
                continue; // no changes
            }
            it->second.state = vbstate.state;
            it->second.checkpointId = vbstate.checkpointId;
            // Note that the max deleted seq number is maintained within KineticKVStore
            vbstate.maxDeletedSeqno = it->second.maxDeletedSeqno;
        } else {
            vb_change_type = VB_STATE_CHANGED;
            cachedVBStates[vbucketId] = vbstate;
        }

        success = setVBucketState(vbucketId, vbstate, vb_change_type, false);
        if (!success) {
            LOG(EXTENSION_LOG_WARNING,
                "Warning: failed to set new state, %s, for vbucket %d\n",
                VBucket::toString(vbstate.state), vbucketId);
            break;
        }
    }
    return success;
}

bool KineticKVStore::snapshotStats(const std::map<std::string, std::string> &stats)
{
    assert(!isReadOnly());
    size_t count = 0;
    size_t size = stats.size();
    std::stringstream stats_buf;
    stats_buf << "{";
    std::map<std::string, std::string>::const_iterator it = stats.begin();
    for (; it != stats.end(); ++it) {
        stats_buf << "\"" << it->first << "\": \"" << it->second << "\"";
        ++count;
        if (count < size) {
            stats_buf << ", ";
        }
    }
    stats_buf << "}";

    // TODO: This stats json should be written into the master database. However,
    // we don't support the write synchronization between KineticKVStore in C++ and
    // compaction manager in the erlang side for the master database yet. At this time,
    // we simply log the engine stats into a separate json file. As part of futhre work,
    // we need to get rid of the tight coupling between those two components.
    bool rv = true;
    std::string next_fname = dbname + "/stats.json.new";
    std::ofstream new_stats;
    new_stats.exceptions (new_stats.failbit | new_stats.badbit);
    try {
        new_stats.open(next_fname.c_str());
        new_stats << stats_buf.str().c_str() << std::endl;
        new_stats.flush();
        new_stats.close();
    } catch (const std::ofstream::failure& e) {
        LOG(EXTENSION_LOG_WARNING, "Warning: failed to log the engine stats due"
            " to IO exception \"%s\"", e.what());
        rv = false;
    }

    if (rv) {
        std::string old_fname = dbname + "/stats.json.old";
        std::string stats_fname = dbname + "/stats.json";
        if (access(old_fname.c_str(), F_OK) == 0 && remove(old_fname.c_str()) != 0) {
            LOG(EXTENSION_LOG_WARNING, "FATAL: Failed to remove '%s': %s",
                old_fname.c_str(), strerror(errno));
            remove(next_fname.c_str());
            rv = false;
        } else if (access(stats_fname.c_str(), F_OK) == 0 &&
                   rename(stats_fname.c_str(), old_fname.c_str()) != 0) {
            LOG(EXTENSION_LOG_WARNING,
                "Warning: failed to rename '%s' to '%s': %s",
                stats_fname.c_str(), old_fname.c_str(), strerror(errno));
            remove(next_fname.c_str());
            rv = false;
        } else if (rename(next_fname.c_str(), stats_fname.c_str()) != 0) {
            LOG(EXTENSION_LOG_WARNING,
                "Warning: failed to rename '%s' to '%s': %s",
                next_fname.c_str(), stats_fname.c_str(), strerror(errno));
            remove(next_fname.c_str());
            rv = false;
        }
    }

    return rv;
}

bool KineticKVStore::setVBucketState(uint16_t vbucketId, vbucket_state &vbstate,
                                   uint32_t vb_change_type, bool newfile,
                                   bool notify)
{
	memcached_return_t memcached_set_with_vbucket(memcached_st *ptr, const char *key, size_t key_length,
	                                 const char *value, size_t value_length,
	                                 time_t expiration,
	                                 uint32_t  flags,
	                                 uint16_t  vbucket);

	char key[20];
	sprintf(key, "vb_%d", vbucketId);
	char stateValue;
	stateValue = (char) vbstate.state;

	memcached_return rc =
			memcached_set_with_vbucket(memc,
									   key, strlen(key),
									   &stateValue, 1,
									   (time_t)0, (uint32_t)0,
									   KINETICSTORE_VBUCKET_STATE);
	if (rc != MEMCACHED_SUCCESS) {
        ++st.numVbSetFailure;
        return false;
	}

    return true;
}

void KineticKVStore::dump(shared_ptr<Callback<GetValue> > cb)
{
    loadDB(cb, false, NULL, KINETICSTORE_NO_DELETES);
}

void KineticKVStore::dump(uint16_t vb, shared_ptr<Callback<GetValue> > cb)
{
    std::vector<uint16_t> vbids;
    vbids.push_back(vb);
    loadDB(cb, false, &vbids);
}

void KineticKVStore::dumpKeys(const std::vector<uint16_t> &vbids,  shared_ptr<Callback<GetValue> > cb)
{
    (void)vbids;
    loadDB(cb, true, NULL, KINETICSTORE_NO_DELETES);
}

void KineticKVStore::dumpDeleted(uint16_t vb,  shared_ptr<Callback<GetValue> > cb)
{
    std::vector<uint16_t> vbids;
    vbids.push_back(vb);
    loadDB(cb, true, &vbids, KINETICSTORE_DELETES_ONLY);
}

StorageProperties KineticKVStore::getStorageProperties()
{
    StorageProperties rv(true, true, true, true);
    return rv;
}

bool KineticKVStore::commit(void)
{
    assert(!isReadOnly());
    if (intransaction) {
        intransaction = commit2kineticstore() ? false : true;
    }
    return !intransaction;

}

void KineticKVStore::addStats(const std::string &prefix,
                            ADD_STAT add_stat,
                            const void *c)
{
    const char *prefix_str = prefix.c_str();

    /* stats for both read-only and read-write threads */
    addStat(prefix_str, "backend_type",   "KINETICSTORE",       add_stat, c);
    addStat(prefix_str, "open",           st.numOpen,         add_stat, c);
    addStat(prefix_str, "close",          st.numClose,        add_stat, c);
    addStat(prefix_str, "readTime",       st.readTimeHisto,   add_stat, c);
    addStat(prefix_str, "readSize",       st.readSizeHisto,   add_stat, c);
    addStat(prefix_str, "numLoadedVb",    st.numLoadedVb,     add_stat, c);

    // failure stats
    addStat(prefix_str, "failure_open",   st.numOpenFailure, add_stat, c);
    addStat(prefix_str, "failure_get",    st.numGetFailure,  add_stat, c);

    if (!isReadOnly()) {
        addStat(prefix_str, "failure_set",   st.numSetFailure,   add_stat, c);
        addStat(prefix_str, "failure_del",   st.numDelFailure,   add_stat, c);
        addStat(prefix_str, "failure_vbset", st.numVbSetFailure, add_stat, c);
        addStat(prefix_str, "lastCommDocs",  st.docsCommitted,   add_stat, c);
        addStat(prefix_str, "numCommitRetry", st.numCommitRetry, add_stat, c);

        // stats for CouchNotifier
        kineticNotifier->addStats(prefix, add_stat, c);
    }
}

void KineticKVStore::addTimingStats(const std::string &prefix,
                                  ADD_STAT add_stat, const void *c) {
    if (isReadOnly()) {
        return;
    }
    const char *prefix_str = prefix.c_str();
    addStat(prefix_str, "commit",      st.commitHisto,      add_stat, c);
    addStat(prefix_str, "commitRetry", st.commitRetryHisto, add_stat, c);
    addStat(prefix_str, "delete",      st.delTimeHisto,     add_stat, c);
    addStat(prefix_str, "save_documents", st.saveDocsHisto, add_stat, c);
    addStat(prefix_str, "writeTime",   st.writeTimeHisto,   add_stat, c);
    addStat(prefix_str, "writeSize",   st.writeSizeHisto,   add_stat, c);
    addStat(prefix_str, "bulkSize",    st.batchSize,        add_stat, c);

    // KINETICSTORE file ops stats
    addStat(prefix_str, "fsReadTime",  st.fsStats.readTimeHisto,  add_stat, c);
    addStat(prefix_str, "fsWriteTime", st.fsStats.writeTimeHisto, add_stat, c);
    addStat(prefix_str, "fsSyncTime",  st.fsStats.syncTimeHisto,  add_stat, c);
    addStat(prefix_str, "fsReadSize",  st.fsStats.readSizeHisto,  add_stat, c);
    addStat(prefix_str, "fsWriteSize", st.fsStats.writeSizeHisto, add_stat, c);
    addStat(prefix_str, "fsReadSeek",  st.fsStats.readSeekHisto,  add_stat, c);
}

template <typename T>
void KineticKVStore::addStat(const std::string &prefix, const char *stat, T &val,
                           ADD_STAT add_stat, const void *c)
{
    std::stringstream fullstat;
    fullstat << prefix << ":" << stat;
    add_casted_stat(fullstat.str().c_str(), val, add_stat, c);
}

void KineticKVStore::optimizeWrites(std::vector<queued_item> &items)
{
    assert(!isReadOnly());
    if (items.empty()) {
        return;
    }
    CompareQueuedItemsByVBAndKey cq;
    std::sort(items.begin(), items.end(), cq);
}


void KineticKVStore::loadDB(shared_ptr<Callback<GetValue> > cb, bool keysOnly,
                          std::vector<uint16_t> *vbids,
                          kineticstore_options options)
{
    hrtime_t start = gethrtime();
    std::vector< std::pair<uint16_t, uint64_t> > vbuckets;
    std::vector< std::pair<uint16_t, uint64_t> > replicaVbuckets;
    bool loadingData = !vbids && !keysOnly;
    GetValue rv;

    if (vbids) {
    	char *value;
    	size_t value_length;
    	const char* keys[] = {"key1"};
    	if (keysOnly) {
    		keys[0] = "keys_only";
    	}
    	size_t key_lengths[] = {strlen(keys[0])};
    	uint32_t flags;
        std::vector<uint16_t>::iterator vbidItr;
        for (vbidItr = vbids->begin(); vbidItr != vbids->end(); vbidItr++) {
        	//retrive items under *vbidIter
        	/*
        	 * 1. both key and value
        	 * 2. key only
        	 * 3. delete only?
        	 */

            memcached_return_t rc = memcached_mget_with_vbucket(memc, keys, key_lengths, 1, *vbidItr);
            if (rc == MEMCACHED_SUCCESS) {
            	memcached_return_t error;
            }

			for(;;) {
				char key[1024];
				size_t keylen;
			    memcached_return_t rc;

				char* retrieved_value = memcached_fetch(memc, key, &keylen, &value_length, &flags, &rc);
				if (retrieved_value == NULL) {
					break;
				}

				Item *it = new Item(key, keylen,
									  0, (time_t)0, retrieved_value, value_length,
									  0, -1, *vbidItr);
				rv = GetValue(it);
				free(retrieved_value);

				++epStats.io_num_read;
				epStats.io_read_bytes += value_length;
				// record stats
				st.readTimeHisto.add((gethrtime() - start) / 1000);
				st.readSizeHisto.add(keylen + rv.getValue()->getNBytes());

				rv.setStatus(kineticErr2EngineErr(rc));
				cb->callback(rv);

				free(retrieved_value);
				retrieved_value = NULL;
			}
        }
    }
}

void KineticKVStore::open()
{
    // TODO intransaction, is it needed?
    intransaction = false;
    if (!isReadOnly()) {
        kineticNotifier = KineticNotifier::create(epStats, configuration);
    }

    if (memc == NULL) {
    	 memcached_return rc;

    	 memc = memcached_create(NULL);
    	 memc_servers = memcached_server_list_append(NULL, "127.0.0.1", 11210, &rc);
    	 rc = memcached_server_push(memc, memc_servers);
    	 if (MEMCACHED_SUCCESS != rc) {
             std::stringstream ss;
             ss << "Fail to create kinetic client";
             throw std::runtime_error(ss.str());
    	 }
    }
}

void KineticKVStore::close()
{
    intransaction = false;
    if (!isReadOnly() && memc != NULL) {
    	if (memc_servers != NULL) {
    		memcached_server_list_free(memc_servers);
    		memc_servers = NULL;
    	}
        memcached_free(memc);
        memc = NULL;
    }
    kineticNotifier = NULL;
}

/**
 * recordDbDump()
 *  memcpy(&cas, metadata.buf, 8);
    memcpy(&exptime, (metadata.buf) + 8, 4);
    memcpy(&itemflags, (metadata.buf) + 12, 4);
    itemflags = itemflags;
    exptime = ntohl(exptime);
    cas = ntohll(cas);
 */



bool KineticKVStore::commit2kineticstore(void)
{
    bool success = true;

    if (pendingCommitCnt == 0) {
        return success;
    }

    assert(pendingReqsQ[0]);
    uint16_t vbucket2flush = pendingReqsQ[0]->getVBucketId();

    int reqIndex = 0;
	memcached_return rc;
    for (; pendingCommitCnt > 0; ++reqIndex, --pendingCommitCnt) {
        KineticRequest *req = pendingReqsQ[reqIndex];
        assert(req);
        assert(vbucket2flush == req->getVBucketId());

        const char* key = req->getKey().c_str();
        size_t keySize = req->getKey().length();
        size_t dataSize = req->getNBytes();

        /* update ep stats */
        ++epStats.io_num_write;
        epStats.io_write_bytes += keySize + dataSize;

        if (req->isDelete()) {
        	rc = memcached_delete_with_vbucket(memc, req->getKey().c_str(), keySize, 0, req->getVBucketId());
        	if (rc == MEMCACHED_SUCCESS) {
        		  st.delTimeHisto.add(req->getDelta() / 1000);
        	}
        	else {
        		  fprintf(stderr, "Couldn't delete key: %s\n", memcached_strerror(memc, rc));
        	  	  ++st.numDelFailure;
        	}
            int rv = getMutationStatus(rc);
            req->getDelCallback()->callback(rv);
        } else {
        	rc = memcached_set_with_vbucket(memc,
        								    req->getKey().c_str(),
        								    keySize,
        								    req->getValue(),
        								    dataSize,
        								    (time_t)0,
        								    (uint32_t)0,
        								    req->getVBucketId());
        	if (rc == MEMCACHED_SUCCESS) {
        	    fprintf(stderr, "Key stored successfully\n");
                st.writeTimeHisto.add(req->getDelta() / 1000);
                st.writeSizeHisto.add(dataSize + keySize);
        	}
        	else {
        		  ++st.numSetFailure;
        	    fprintf(stderr, "Couldn't store key: %s\n", memcached_strerror(memc, rc));
        	}
        }
    }

    // clean up
    pendingReqsQ.clear();
    return success;
}


void KineticKVStore::queueItem(KineticRequest *req)
{
    if (pendingCommitCnt &&
        pendingReqsQ.front()->getVBucketId() != req->getVBucketId()) {
        // got new request for a different vb, commit pending
        // pending requests of the current vb firt
    	commit2kineticstore();
    }
    pendingReqsQ.push_back(req);
    pendingCommitCnt++;
}

void KineticKVStore::commitCallback(KineticRequest **committedReqs, int numReqs,
                                  memcached_return_t errCode)
{
    for (int index = 0; index < numReqs; index++) {
        size_t dataSize = committedReqs[index]->getNBytes();
        size_t keySize = committedReqs[index]->getKey().length();
        /* update ep stats */
        ++epStats.io_num_write;
        epStats.io_write_bytes += keySize + dataSize;

        if (committedReqs[index]->isDelete()) {
            int rv = getMutationStatus(errCode);
            if (errCode) {
                ++st.numDelFailure;
            } else {
                st.delTimeHisto.add(committedReqs[index]->getDelta() / 1000);
            }
            committedReqs[index]->getDelCallback()->callback(rv);
        } else {
            int rv = getMutationStatus(errCode);
            int64_t newItemId = 0; //committedReqs[index]->getDbDocInfo()->db_seq;
            if (errCode) {
                ++st.numSetFailure;
                newItemId = 0;
            } else {
                st.writeTimeHisto.add(committedReqs[index]->getDelta() / 1000);
                st.writeSizeHisto.add(dataSize + keySize);
            }
            mutation_result p(rv, newItemId);
            committedReqs[index]->getSetCallback()->callback(p);
        }
    }
}


ENGINE_ERROR_CODE KineticKVStore::kineticErr2EngineErr(memcached_return_t errCode)
{
    switch (errCode) {
    case MEMCACHED_SUCCESS:
        return ENGINE_SUCCESS;
    case MEMCACHED_MEMORY_ALLOCATION_FAILURE:
        return ENGINE_ENOMEM;
    case MEMCACHED_NOTFOUND:
    case MEMCACHED_BAD_KEY_PROVIDED:
        return ENGINE_KEY_ENOENT;
    default:
        // same as the general error return code of
        // EvetuallyPersistentStore::getInternal
        return ENGINE_TMPFAIL;
    }
}

bool KineticKVStore::getEstimatedItemCount(size_t &items)
{
    items = 0;

    const char* keys[] = {"items_count"};
    size_t key_lengths[] = {strlen(keys[0])};

    vbucket_map_t::iterator itor = cachedVBStates.begin();
    for (; itor != cachedVBStates.end(); ++itor) {
        uint16_t vbucket = itor->first;
        memcached_return_t rc = memcached_mget_with_vbucket(memc, keys, key_lengths, 1, vbucket);
        if (rc == MEMCACHED_SUCCESS) {
        	memcached_return_t error;

        	for(;;) {
        		char key[1024];
        		size_t keylen;
        	    uint32_t flags;
        	    char *value;
        	    size_t value_length;
        		value = memcached_fetch(memc, key, &keylen, &value_length, &flags, &error);
        		if (value == NULL) {
        			break;
        		}

        		items += atol(value);
        		free(value);
        		value = NULL;
        	}
        }
    }
    return true;
}

size_t KineticKVStore::getNumPersistedDeletes(uint16_t vbid) {
    std::map<uint16_t, size_t>::iterator itr = cachedDeleteCount.find(vbid);
    if (itr != cachedDeleteCount.end()) {
        return itr->second;
    }
    return 0;
}

/* end of couch-kvstore.cc */
