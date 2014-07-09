/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"

#include <string>
#include <map>

#include "common.hh"
#include "ep_engine.h"
#include "stats.hh"
#include "kvstore.hh"
#include "blackhole-kvstore/blackhole.hh"
#include "warmup.hh"
#ifdef HAVE_LIBCOUCHSTORE
#include "couch-kvstore/couch-kvstore.hh"
#else
#include "couch-kvstore/couch-kvstore-dummy.hh"
#endif
#ifdef HAVE_LIBKINETIC
#include "kinetic-kvstore/kinetic-kvstore.hh"
#else
#include "kinetic-kvstore/kinetic-kvstore-dummy.hh"
#endif

KVStore *KVStoreFactory::create(EPStats &stats, Configuration &config,
                                bool read_only) {

    KVStore *ret = NULL;
    std::string backend = config.getBackend();
    //std::string backend = "couchdb";
    if (backend.compare("couchdb") == 0) {
        ret = new CouchKVStore(stats, config, read_only);
    } else if (backend.compare("blackhole") == 0) {
        ret = new BlackholeKVStore(read_only);
    } else if (backend.compare("kinetic") == 0) {
    	ret = new KineticKVStore(stats, config, read_only);
    } else {
        LOG(EXTENSION_LOG_WARNING, "Unknown backend: [%s]", backend.c_str());
    }

    return ret;
}

bool KVStore::getEstimatedItemCount(size_t &) {
    // Not supported
    return false;
}
