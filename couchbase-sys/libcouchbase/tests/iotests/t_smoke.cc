/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012-2020 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#include <gtest/gtest.h>
#include "iotests.h"
#include "internalstructs.h"

using std::string;
using std::vector;

// This file contains the 'migrated' tests from smoke-test.c

static lcb_BOOTSTRAP_TRANSPORT transports[] = {LCB_CONFIG_TRANSPORT_HTTP, LCB_CONFIG_TRANSPORT_LIST_END};
struct rvbuf {
    lcb_STATUS error{};
    lcb_STORE_OPERATION operation{};
    vector<char> bytes;
    vector<char> key;
    uint64_t cas{};
    uint32_t flags{};
    int32_t counter{};
    uint32_t errorCount{};

    template <typename T>
    void setKey(const T *resp)
    {
        const char *ktmp, *kend;
        ktmp = (const char *)resp->key;
        kend = ktmp + resp->nkey;
        key.assign(ktmp, kend);
    }

    void setKey(const lcb_RESPTOUCH *resp)
    {
        const char *ktmp, *kend;
        size_t ntmp;
        lcb_resptouch_key(resp, &ktmp, &ntmp);
        kend = ktmp + ntmp;
        key.assign(ktmp, kend);
    }

    void setKey(const lcb_RESPSTORE *resp)
    {
        const char *ktmp, *kend;
        size_t ntmp;
        lcb_respstore_key(resp, &ktmp, &ntmp);
        kend = ktmp + ntmp;
        key.assign(ktmp, kend);
    }

    void setKey(const lcb_RESPGET *resp)
    {
        const char *ktmp, *kend;
        size_t ntmp;
        lcb_respget_key(resp, &ktmp, &ntmp);
        kend = ktmp + ntmp;
        key.assign(ktmp, kend);
    }

    void setValue(const lcb_RESPGET *resp)
    {
        const char *btmp;
        size_t ntmp;
        lcb_respget_value(resp, &btmp, &ntmp);
        const char *bend = btmp + ntmp;
        bytes.assign(btmp, bend);
    }

    string getKeyString()
    {
        return string(key.begin(), key.end());
    }

    string getValueString()
    {
        return string(bytes.begin(), bytes.end());
    }

    rvbuf()
    {
        reset();
    }

    void reset()
    {
        error = LCB_SUCCESS;
        operation = LCB_STORE_UPSERT;
        cas = 0;
        flags = 0;
        counter = 0;
        errorCount = 0;
        key.clear();
        bytes.clear();
    }

    void setError(lcb_STATUS err)
    {
        EXPECT_GT(counter, 0);
        counter--;
        if (err != LCB_SUCCESS) {
            error = err;
            errorCount++;
        }
    }
    void incRemaining()
    {
        counter++;
    }
};

extern "C" {

static void bootstrap_callback(lcb_INSTANCE * /* instance */, lcb_STATUS err)
{
    EXPECT_TRUE(err == LCB_SUCCESS || err == LCB_ERR_BUCKET_NOT_FOUND || err == LCB_ERR_AUTHENTICATION_FAILURE);
    EXPECT_NE(err, LCB_ERR_NO_MATCHING_SERVER);
}

static void store_callback(lcb_INSTANCE *, lcb_CALLBACK_TYPE, const lcb_RESPSTORE *resp)
{
    rvbuf *rv;
    lcb_respstore_cookie(resp, (void **)&rv);
    rv->setError(lcb_respstore_status(resp));
    rv->setKey(resp);
    lcb_respstore_operation(resp, &rv->operation);
}

static void get_callback(lcb_INSTANCE *, lcb_CALLBACK_TYPE, const lcb_RESPGET *resp)
{
    rvbuf *rv;

    lcb_respget_cookie(resp, (void **)&rv);
    rv->setError(lcb_respget_status(resp));
    rv->setKey(resp);
    if (lcb_respget_status(resp) == LCB_SUCCESS) {
        rv->setValue(resp);
    }
}

static void touch_callback(lcb_INSTANCE *, lcb_CALLBACK_TYPE, const lcb_RESPTOUCH *resp)
{
    rvbuf *rv;
    lcb_resptouch_cookie(resp, (void **)&rv);
    lcb_STATUS rc = lcb_resptouch_status(resp);
    rv->setError(rc);
    rv->setKey(resp);
    if (rc == LCB_ERR_TIMEOUT) {
        fprintf(stderr, "caught timeout\n");
    }
    EXPECT_EQ(LCB_SUCCESS, rc) << std::string(rv->key.begin(), rv->key.end()) << ": " << lcb_strerror_short(rc);
}

} // extern "C"
static void setupCallbacks(lcb_INSTANCE *instance)
{
    lcb_install_callback(instance, LCB_CALLBACK_STORE, (lcb_RESPCALLBACK)store_callback);
    lcb_install_callback(instance, LCB_CALLBACK_GET, (lcb_RESPCALLBACK)get_callback);
    lcb_install_callback(instance, LCB_CALLBACK_TOUCH, (lcb_RESPCALLBACK)touch_callback);
}

class SmokeTest : public ::testing::Test
{
  protected:
    MockEnvironment *mock;
    lcb_INSTANCE *session;
    void SetUp() override
    {
        assert(session == nullptr);
        session = nullptr;
        mock = nullptr;
    }

    void TearDown() override
    {
        if (session != nullptr) {
            lcb_destroy(session);
        }
        delete mock;

        session = nullptr;
        mock = nullptr;
    }
    void destroySession()
    {
        if (session != nullptr) {
            lcb_destroy(session);
            session = nullptr;
        }
    }

    SmokeTest() : mock(nullptr), session(nullptr) {}

  public:
    void testSet1();
    void testSet2();
    void testGet1();
    void testGet2();
    void testTouch1();
    void testSpuriousSaslError();
    lcb_STATUS testMissingBucket();

    // Call to connect instance
    void connectCommon(const char *bucket = nullptr, const char *password = nullptr, lcb_STATUS expected = LCB_SUCCESS);
};

void SmokeTest::testSet1()
{
    rvbuf rv;
    lcb_CMDSTORE *cmd;

    string key("foo");
    string value("bar");

    lcb_cmdstore_create(&cmd, LCB_STORE_UPSERT);
    lcb_cmdstore_key(cmd, key.c_str(), key.size());
    lcb_cmdstore_value(cmd, value.c_str(), value.size());
    EXPECT_EQ(LCB_SUCCESS, lcb_store(session, &rv, cmd));
    lcb_cmdstore_destroy(cmd);
    rv.incRemaining();
    lcb_wait(session, LCB_WAIT_DEFAULT);
    EXPECT_EQ(LCB_SUCCESS, rv.error);
    EXPECT_EQ(LCB_STORE_UPSERT, rv.operation);
    EXPECT_EQ(key, rv.getKeyString());
}

void SmokeTest::testSet2()
{
    struct rvbuf rv;
    lcb_size_t ii;
    lcb_CMDSTORE *cmd;
    string key("foo"), value("bar");

    lcb_cmdstore_create(&cmd, LCB_STORE_UPSERT);
    lcb_cmdstore_key(cmd, key.c_str(), key.size());
    lcb_cmdstore_value(cmd, value.c_str(), value.size());

    rv.errorCount = 0;
    rv.counter = 0;
    for (ii = 0; ii < 10; ++ii, rv.incRemaining()) {
        EXPECT_EQ(LCB_SUCCESS, lcb_store(session, &rv, cmd));
    }
    lcb_cmdstore_destroy(cmd);
    lcb_wait(session, LCB_WAIT_DEFAULT);
    EXPECT_EQ(0, rv.errorCount);
}

void SmokeTest::testGet1()
{
    struct rvbuf rv;
    string key("foo"), value("bar");

    lcb_CMDSTORE *storecmd;
    lcb_cmdstore_create(&storecmd, LCB_STORE_UPSERT);
    lcb_cmdstore_key(storecmd, key.c_str(), key.size());
    lcb_cmdstore_value(storecmd, value.c_str(), value.size());

    EXPECT_EQ(LCB_SUCCESS, lcb_store(session, &rv, storecmd));
    lcb_cmdstore_destroy(storecmd);
    rv.incRemaining();

    lcb_wait(session, LCB_WAIT_DEFAULT);
    EXPECT_EQ(LCB_SUCCESS, rv.error);

    rv.reset();

    lcb_CMDGET *getcmd;
    lcb_cmdget_create(&getcmd);
    lcb_cmdget_key(getcmd, key.c_str(), key.size());
    EXPECT_EQ(LCB_SUCCESS, lcb_get(session, &rv, getcmd));
    rv.incRemaining();
    lcb_wait(session, LCB_WAIT_DEFAULT);
    lcb_cmdget_destroy(getcmd);

    EXPECT_EQ(rv.error, LCB_SUCCESS);
    EXPECT_EQ(key, rv.getKeyString());
    EXPECT_EQ(value, rv.getValueString());
}

static void genAZString(vector<string> &coll)
{
    string base("foo");
    for (size_t ii = 0; ii < 26; ++ii) {
        coll.push_back(base);
        coll.back() += std::to_string('a' + ii);
    }
}

void SmokeTest::testGet2()
{
    struct rvbuf rv;
    string value("bar");
    vector<string> coll;
    genAZString(coll);

    for (auto &curKey : coll) {
        lcb_CMDSTORE *cmd;
        lcb_cmdstore_create(&cmd, LCB_STORE_UPSERT);
        lcb_cmdstore_key(cmd, curKey.c_str(), curKey.size());
        lcb_cmdstore_value(cmd, value.c_str(), value.size());

        EXPECT_EQ(LCB_SUCCESS, lcb_store(session, &rv, cmd));
        lcb_cmdstore_destroy(cmd);
        rv.incRemaining();
        lcb_wait(session, LCB_WAIT_DEFAULT);
        EXPECT_EQ(LCB_SUCCESS, rv.error);

        rv.reset();
    }

    rv.counter = coll.size();

    for (auto &curKey : coll) {
        lcb_CMDGET *cmd;
        lcb_cmdget_create(&cmd);
        lcb_cmdget_key(cmd, curKey.c_str(), curKey.size());
        EXPECT_EQ(LCB_SUCCESS, lcb_get(session, &rv, cmd));
        rv.incRemaining();
        lcb_cmdget_destroy(cmd);
    }
    lcb_wait(session, LCB_WAIT_DEFAULT);
    EXPECT_EQ(LCB_SUCCESS, rv.error);
    EXPECT_EQ(value, rv.getValueString());
}

void SmokeTest::testTouch1()
{
    struct rvbuf rv;
    vector<string> coll;
    string value("bar");
    genAZString(coll);

    for (auto &curKey : coll) {
        lcb_CMDSTORE *cmd;
        lcb_cmdstore_create(&cmd, LCB_STORE_UPSERT);
        lcb_cmdstore_key(cmd, curKey.c_str(), curKey.size());
        lcb_cmdstore_value(cmd, value.c_str(), value.size());

        EXPECT_EQ(LCB_SUCCESS, lcb_store(session, &rv, cmd));
        lcb_cmdstore_destroy(cmd);
        rv.incRemaining();
        lcb_wait(session, LCB_WAIT_DEFAULT);
        EXPECT_EQ(LCB_SUCCESS, rv.error);

        rv.reset();
    }

    rv.counter = coll.size();
    for (auto &curKey : coll) {
        lcb_CMDTOUCH *cmd;
        lcb_cmdtouch_create(&cmd);
        lcb_cmdtouch_key(cmd, curKey.c_str(), curKey.size());
        lcb_cmdtouch_expiry(cmd, 10);
        EXPECT_EQ(LCB_SUCCESS, lcb_touch(session, &rv, cmd));
        lcb_cmdtouch_destroy(cmd);
        rv.incRemaining();
    }

    lcb_wait(session, LCB_WAIT_DEFAULT);
    EXPECT_EQ(LCB_SUCCESS, rv.error);
}

lcb_STATUS SmokeTest::testMissingBucket()
{
    destroySession();
    // create a new session
    lcb_CREATEOPTS *cropts = nullptr;
    mock->makeConnectParams(cropts);
    std::string bucket("nonexist");
    std::string username("nonexist");
    lcb_createopts_bucket(cropts, bucket.c_str(), bucket.size());
    lcb_createopts_credentials(cropts, username.c_str(), username.size(), nullptr, 0);
    lcb_STATUS err;
    err = lcb_create(&session, cropts);
    lcb_createopts_destroy(cropts);
    EXPECT_EQ(LCB_SUCCESS, err);
    mock->postCreate(session);
    lcb_set_bootstrap_callback(session, bootstrap_callback);
    err = lcb_connect(session);
    EXPECT_EQ(LCB_SUCCESS, err);
    lcb_wait(session, LCB_WAIT_DEFAULT);
    err = lcb_get_bootstrap_status(session);
    EXPECT_NE(LCB_SUCCESS, err);
    EXPECT_TRUE(err == LCB_ERR_BUCKET_NOT_FOUND || err == LCB_ERR_AUTHENTICATION_FAILURE);
    destroySession();
    return err;
}

void SmokeTest::testSpuriousSaslError()
{
    int iterations = 50;
    rvbuf rvs[50];
    int i;
    string key("KEY");

    for (i = 0; i < iterations; i++) {
        rvs[i].counter = 999;

        lcb_CMDSTORE *cmd;
        lcb_cmdstore_create(&cmd, LCB_STORE_UPSERT);
        lcb_cmdstore_key(cmd, key.c_str(), key.size());
        lcb_cmdstore_value(cmd, key.c_str(), key.size());
        EXPECT_EQ(LCB_SUCCESS, lcb_store(session, rvs + i, cmd));
        lcb_cmdstore_destroy(cmd);
    }
    lcb_wait(session, LCB_WAIT_DEFAULT);

    for (i = 0; i < iterations; i++) {
        const char *errinfo = nullptr;
        if (rvs[i].errorCount != LCB_SUCCESS) {
            errinfo = "Did not get success response";
        } else if (rvs[i].key.size() != 3) {
            errinfo = "Did not get expected key length";
        } else if (rvs[i].getKeyString() != key) {
            errinfo = "Weird key size";
        }
        if (errinfo) {
            EXPECT_FALSE(true) << errinfo;
        }
    }
}

void SmokeTest::connectCommon(const char *bucket, const char *password, lcb_STATUS expected)
{
    lcb_CREATEOPTS *cropts = nullptr;
    mock->makeConnectParams(cropts, nullptr);

    if (bucket) {
        lcb_createopts_bucket(cropts, bucket, strlen(bucket));
        if (password) {
            lcb_createopts_credentials(cropts, bucket, strlen(bucket), password, strlen(password));
        }
    }
    lcb_STATUS err = lcb_create(&session, cropts);
    lcb_createopts_destroy(cropts);
    EXPECT_EQ(LCB_SUCCESS, err);

    mock->postCreate(session);
    lcb_set_bootstrap_callback(session, bootstrap_callback);
    err = lcb_connect(session);
    EXPECT_EQ(LCB_SUCCESS, err);
    lcb_wait(session, LCB_WAIT_DEFAULT);
    EXPECT_EQ(expected, lcb_get_bootstrap_status(session));
    setupCallbacks(session);
}

TEST_F(SmokeTest, testMemcachedBucket)
{
    SKIP_UNLESS_MOCK()
    const char *args[] = {"--buckets", "default::memcache", nullptr};
    mock = new MockEnvironment(args);
    mock->setCCCP(false);
    connectCommon();
    testSet1();
    testSet2();
    testGet1();
    testGet2();

    // A bit out of place, but check that replica commands fail at schedule-time
    lcb_sched_enter(session);
    lcb_CMDGETREPLICA *cmd;
    lcb_STATUS rc;

    lcb_cmdgetreplica_create(&cmd, LCB_REPLICA_MODE_ANY);
    lcb_cmdgetreplica_key(cmd, "key", 3);
    rc = lcb_getreplica(session, nullptr, cmd);
    ASSERT_EQ(LCB_ERR_NO_MATCHING_SERVER, rc);
    lcb_cmdgetreplica_destroy(cmd);

    lcb_cmdgetreplica_create(&cmd, LCB_REPLICA_MODE_ALL);
    lcb_cmdgetreplica_key(cmd, "key", 3);
    rc = lcb_getreplica(session, nullptr, cmd);
    ASSERT_EQ(LCB_ERR_NO_MATCHING_SERVER, rc);
    lcb_cmdgetreplica_destroy(cmd);

    lcb_cmdgetreplica_create(&cmd, LCB_REPLICA_MODE_IDX0);
    lcb_cmdgetreplica_key(cmd, "key", 3);
    rc = lcb_getreplica(session, nullptr, cmd);
    ASSERT_EQ(LCB_ERR_NO_MATCHING_SERVER, rc);
    lcb_cmdgetreplica_destroy(cmd);

    testMissingBucket();
}

TEST_F(SmokeTest, testCouchbaseBucket)
{
    SKIP_UNLESS_MOCK()
    const char *args[] = {"--buckets", "default::couchbase", "--debug", nullptr};
    mock = new MockEnvironment(args);
    mock->setCCCP(false);
    connectCommon();
    testSet1();
    testSet2();
    testGet1();
    testGet2();
    testMissingBucket();
}

TEST_F(SmokeTest, testSaslBucket)
{
    SKIP_UNLESS_MOCK()
    const char *args[] = {"--buckets", "protected:secret:couchbase", nullptr};
    mock = new MockEnvironment(args, "protected");
    mock->setCCCP(false);

    testMissingBucket();

    connectCommon("protected", "secret");
    testSpuriousSaslError();

    destroySession();
    connectCommon("protected", "incorrect", LCB_ERR_AUTHENTICATION_FAILURE);
    destroySession();
}
