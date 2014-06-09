/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include "kinetic-kvstore/kinetic-kvstore-dummy.hh"

KineticKVStore::KineticKVStore(EPStats &, Configuration &, bool) : KVStore()
{
    throw std::runtime_error("This kvstore should never be used");
}

KineticKVStore::KineticKVStore(const KineticKVStore &) : KVStore()
{
    throw std::runtime_error("This kvstore should never be used");
}

void KineticKVStore::reset()
{
    throw std::runtime_error("This kvstore should never be used");
}

bool KineticKVStore::begin()
{
    throw std::runtime_error("This kvstore should never be used");
}

void KineticKVStore::rollback()
{
    throw std::runtime_error("This kvstore should never be used");
}

void KineticKVStore::destroyInvalidVBuckets(bool)
{
    throw std::runtime_error("This kvstore should never be used");
}

void KineticKVStore::set(const Item &, Callback<mutation_result> &)
{
    throw std::runtime_error("This kvstore should never be used");
}

void KineticKVStore::get(const std::string &, uint64_t, uint16_t,
                       Callback<GetValue> &)
{
    throw std::runtime_error("This kvstore should never be used");
}

void KineticKVStore::del(const Item &, uint64_t, Callback<int> &)
{
    throw std::runtime_error("This kvstore should never be used");
}

bool KineticKVStore::delVBucket(uint16_t vbucket, bool recreate)
{
    throw std::runtime_error("This kvstore should never be used");
}

vbucket_map_t KineticKVStore::listPersistedVbuckets()
{
    throw std::runtime_error("This kvstore should never be used");
}


bool KineticKVStore::snapshotVBuckets(const vbucket_map_t &)
{
    throw std::runtime_error("This kvstore should never be used");
}

bool KineticKVStore::snapshotStats(const std::map<std::string, std::string> &)
{
    // noop, virtual function implementation for abstract base class
    return true;
}

void KineticKVStore::dump(shared_ptr<Callback<GetValue> >)
{
    throw std::runtime_error("This kvstore should never be used");
}

void KineticKVStore::dump(uint16_t, shared_ptr<Callback<GetValue> >)
{
    throw std::runtime_error("This kvstore should never be used");
}

StorageProperties KineticKVStore::getStorageProperties()
{
    throw std::runtime_error("This kvstore should never be used");
}

bool KineticKVStore::commit(void)
{
    throw std::runtime_error("This kvstore should never be used");
}

