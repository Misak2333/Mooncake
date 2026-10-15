#include <gtest/gtest.h>

#include <chrono>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include "kv_event/kv_event_publisher.h"

#if defined(MOONCAKE_ENABLE_KV_EVENTS) && MOONCAKE_ENABLE_KV_EVENTS
#include <msgpack.hpp>
#include <zmq.h>
#include "master_service_test_fixture.h"
#endif

namespace mooncake {
namespace {

TEST(KvEventPublisherTest, DisabledPublisherIsNoop) {
    KvEventConfig config;
    config.enabled = false;
    KvEventPublisher publisher(config);
    EXPECT_FALSE(publisher.enabled());
    publisher.PublishStored("42", "cpu");
    publisher.PublishRemoved("42", "cpu");
    publisher.PublishCleared("tenant-a");
    publisher.PublishReplicaUpdate("key", {{"memory", "node-a:1"}});
    const auto stats = publisher.GetStats();
    EXPECT_EQ(stats.published_events, 0u);
    EXPECT_EQ(stats.dropped_events, 0u);
}

#if defined(MOONCAKE_ENABLE_KV_EVENTS) && MOONCAKE_ENABLE_KV_EVENTS

namespace {

std::string MakeIpcEndpoint() {
    return "ipc:///tmp/kv_event_test_" + std::to_string(getpid()) + "_" +
           std::to_string(
               std::chrono::steady_clock::now().time_since_epoch().count());
}

bool ReceiveZmqMultipart(void* socket, std::vector<std::string>& frames) {
    frames.clear();
    while (true) {
        zmq_msg_t msg;
        if (zmq_msg_init(&msg) != 0) {
            return false;
        }
        const int rc = zmq_msg_recv(&msg, socket, 0);
        if (rc < 0) {
            zmq_msg_close(&msg);
            return false;
        }
        const char* data = static_cast<const char*>(zmq_msg_data(&msg));
        frames.emplace_back(data, data + zmq_msg_size(&msg));
        const int more = zmq_msg_more(&msg);
        zmq_msg_close(&msg);
        if (!more) {
            break;
        }
    }
    return true;
}

bool ReceivePayload(void* socket, msgpack::object_handle* payload) {
    std::vector<std::string> frames;
    if (!ReceiveZmqMultipart(socket, frames) || frames.size() != 3) {
        return false;
    }
    *payload = msgpack::unpack(frames[2].data(), frames[2].size());
    return true;
}

const msgpack::object* FindMapValue(const msgpack::object& map,
                                    const std::string& field) {
    if (map.type != msgpack::type::MAP) {
        return nullptr;
    }
    for (uint32_t i = 0; i < map.via.map.size; ++i) {
        const auto& key = map.via.map.ptr[i].key;
        if (key.type == msgpack::type::STR &&
            std::string(key.via.str.ptr, key.via.str.size) == field) {
            return &map.via.map.ptr[i].val;
        }
    }
    return nullptr;
}

std::string MsgpackString(const msgpack::object& value) {
    return value.type == msgpack::type::STR
               ? std::string(value.via.str.ptr, value.via.str.size)
               : std::string{};
}

const msgpack::object& FirstEvent(const msgpack::object_handle& payload) {
    return payload.get().via.array.ptr[1].via.array.ptr[0];
}

void ExpectObjectEvent(const msgpack::object& event, const std::string& type,
                       const std::string& object_key) {
    ASSERT_EQ(event.type, msgpack::type::MAP);
    const auto* event_type = FindMapValue(event, "event_type");
    const auto* actual_key = FindMapValue(event, "object_key");
    const auto* seq_hashes = FindMapValue(event, "seq_hashes");
    ASSERT_NE(event_type, nullptr);
    ASSERT_NE(actual_key, nullptr);
    ASSERT_NE(seq_hashes, nullptr);
    EXPECT_EQ(MsgpackString(*event_type), type);
    EXPECT_EQ(MsgpackString(*actual_key), object_key);
    ASSERT_EQ(seq_hashes->type, msgpack::type::ARRAY);
    EXPECT_EQ(seq_hashes->via.array.size, 0u);
    EXPECT_EQ(FindMapValue(event, "connector_block_hash"), nullptr);
    EXPECT_EQ(FindMapValue(event, "cache_prefix"), nullptr);
}

KvEventConfig MakeEnabledConfig(const std::string& endpoint) {
    KvEventConfig config;
    config.enabled = true;
    config.bind_endpoint = endpoint;
    config.backend_id = "mooncake-test";
    config.emit_object_key = true;
    return config;
}

uint32_t EventCount(const msgpack::object_handle& payload) {
    return payload.get().via.array.ptr[1].via.array.size;
}

const msgpack::object& EventAt(const msgpack::object_handle& payload,
                               uint32_t index) {
    return payload.get().via.array.ptr[1].via.array.ptr[index];
}

// Confirms the publisher emitted nothing more, using a short receive timeout so
// negative assertions do not stall the suite.
bool ExpectNoMessage(void* socket) {
    int short_timeout_ms = 200;
    zmq_setsockopt(socket, ZMQ_RCVTIMEO, &short_timeout_ms,
                   sizeof(short_timeout_ms));
    std::vector<std::string> frames;
    const bool received = ReceiveZmqMultipart(socket, frames);
    int restore_timeout_ms = 2000;
    zmq_setsockopt(socket, ZMQ_RCVTIMEO, &restore_timeout_ms,
                   sizeof(restore_timeout_ms));
    return !received;
}

// Owns the SUB socket lifetime so each test body stays focused on assertions.
struct ScopedSubscriber {
    void* ctx{nullptr};
    void* sub{nullptr};

    bool Connect(const std::string& endpoint, int timeout_ms = 2000) {
        ctx = zmq_ctx_new();
        if (ctx == nullptr) {
            return false;
        }
        sub = zmq_socket(ctx, ZMQ_SUB);
        if (sub == nullptr) {
            return false;
        }
        if (zmq_setsockopt(sub, ZMQ_RCVTIMEO, &timeout_ms,
                           sizeof(timeout_ms)) != 0 ||
            zmq_connect(sub, endpoint.c_str()) != 0 ||
            zmq_setsockopt(sub, ZMQ_SUBSCRIBE, "", 0) != 0) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        return true;
    }

    ~ScopedSubscriber() {
        if (sub != nullptr) {
            zmq_close(sub);
        }
        if (ctx != nullptr) {
            zmq_ctx_destroy(ctx);
        }
    }
};

}  // namespace

TEST(KvEventPublisherTest, PublishesSglangObjectKeyOverZmq) {
    const std::string endpoint = MakeIpcEndpoint();
    const std::string object_key =
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855_0_k";
    const std::string group_id =
        "sglang-hicache:"
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    KvEventConfig config;
    config.enabled = true;
    config.bind_endpoint = endpoint;
    config.backend_id = "mooncake-test";
    config.model_name = "configured-model";
    config.block_size = 128;
    config.additional_salt = "salt-a";
    config.lora_name = "adapter-a";
    config.dp_rank = 5;
    config.emit_object_key = true;
    config.emit_legacy_compat_fields = true;
    config.queue_capacity = 64;
    KvEventPublisher publisher(config);
    ASSERT_TRUE(publisher.enabled());

    void* ctx = zmq_ctx_new();
    ASSERT_NE(ctx, nullptr);
    void* sub = zmq_socket(ctx, ZMQ_SUB);
    ASSERT_NE(sub, nullptr);
    const int timeout_ms = 2000;
    ASSERT_EQ(
        zmq_setsockopt(sub, ZMQ_RCVTIMEO, &timeout_ms, sizeof(timeout_ms)), 0);
    ASSERT_EQ(zmq_connect(sub, endpoint.c_str()), 0);
    ASSERT_EQ(zmq_setsockopt(sub, ZMQ_SUBSCRIBE, "", 0), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    publisher.PublishStored(object_key, "cpu", "tenant-a", group_id);
    msgpack::object_handle payload;
    ASSERT_TRUE(ReceivePayload(sub, &payload));
    ASSERT_EQ(payload.get().type, msgpack::type::ARRAY);
    ASSERT_EQ(payload.get().via.array.size, 3u);
    ExpectObjectEvent(FirstEvent(payload), "stored", object_key);
    const auto& event = FirstEvent(payload);
    EXPECT_EQ(MsgpackString(*FindMapValue(event, "group_id")), group_id);
    EXPECT_EQ(MsgpackString(*FindMapValue(event, "backend_id")),
              "mooncake-test");
    EXPECT_EQ(MsgpackString(*FindMapValue(event, "tenant_id")), "tenant-a");
    EXPECT_EQ(FindMapValue(event, "model_name")->as<std::string>(),
              "configured-model");
    EXPECT_EQ(FindMapValue(event, "block_size")->as<uint32_t>(), 128u);
    EXPECT_EQ(FindMapValue(event, "dp_rank")->as<uint32_t>(), 5u);

    publisher.PublishRemoved(object_key, "cpu", "tenant-a", group_id);
    ASSERT_TRUE(ReceivePayload(sub, &payload));
    ExpectObjectEvent(FirstEvent(payload), "removed", object_key);

    publisher.PublishCleared("tenant-a");
    ASSERT_TRUE(ReceivePayload(sub, &payload));
    const auto& cleared = FirstEvent(payload);
    EXPECT_EQ(MsgpackString(*FindMapValue(cleared, "event_type")), "cleared");
    EXPECT_EQ(MsgpackString(*FindMapValue(cleared, "tenant_id")), "tenant-a");
    EXPECT_EQ(FindMapValue(cleared, "object_key"), nullptr);

    for (int i = 0; i < 50; ++i) {
        const auto stats = publisher.GetStats();
        if (stats.published_events == 3 && stats.published_batches == 3) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    const auto stats = publisher.GetStats();
    EXPECT_EQ(stats.published_events, 3u);
    EXPECT_EQ(stats.published_batches, 3u);
    EXPECT_EQ(stats.dropped_events, 0u);

    zmq_close(sub);
    zmq_ctx_destroy(ctx);
}

TEST(KvEventPublisherTest, PublishesStoreMetadataAndMediumDelta) {
    const std::string endpoint = MakeIpcEndpoint();
    KvEventConfig config;
    config.enabled = true;
    config.bind_endpoint = endpoint;
    config.backend_id = "mooncake-test";
    config.model_name = "model-a";
    config.block_size = 64;
    config.additional_salt = "salt-a";
    config.lora_name = "adapter-a";
    config.dp_rank = 4;
    config.emit_object_key = true;
    config.emit_legacy_compat_fields = true;
    KvEventPublisher publisher(config);
    ASSERT_TRUE(publisher.enabled());

    void* ctx = zmq_ctx_new();
    ASSERT_NE(ctx, nullptr);
    void* sub = zmq_socket(ctx, ZMQ_SUB);
    ASSERT_NE(sub, nullptr);
    const int timeout_ms = 2000;
    ASSERT_EQ(
        zmq_setsockopt(sub, ZMQ_RCVTIMEO, &timeout_ms, sizeof(timeout_ms)), 0);
    ASSERT_EQ(zmq_connect(sub, endpoint.c_str()), 0);
    ASSERT_EQ(zmq_setsockopt(sub, ZMQ_SUBSCRIBE, "", 0), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    const std::string object_key =
        "deployment-a@model-a@tp_rank:2@pcp1@dcp0@pp_rank:3@group:7@"
        "0123456789abcdef000000000000002a";
    msgpack::object_handle payload;
    publisher.PublishCommitted(object_key, {"cpu"}, "tenant-a", "group-a");
    ASSERT_TRUE(ReceivePayload(sub, &payload));
    ExpectObjectEvent(FirstEvent(payload), "stored", object_key);

    publisher.PublishCommitted(object_key, {"cpu"}, "tenant-a", "group-a");
    ASSERT_TRUE(ReceivePayload(sub, &payload));
    ExpectObjectEvent(FirstEvent(payload), "stored", object_key);

    publisher.SyncObjectState(object_key, {}, "tenant-a", "group-a", {"cpu"});
    ASSERT_TRUE(ReceivePayload(sub, &payload));
    ExpectObjectEvent(FirstEvent(payload), "removed", object_key);

    publisher.PublishCommitted(object_key, {"cpu"}, "tenant-a", "group-a");
    ASSERT_TRUE(ReceivePayload(sub, &payload));
    ExpectObjectEvent(FirstEvent(payload), "stored", object_key);

    publisher.SyncObjectState(object_key, {"disk"}, "tenant-a", "group-a",
                              {"cpu"});
    ASSERT_TRUE(ReceivePayload(sub, &payload));
    ASSERT_EQ(payload.get().via.array.ptr[1].via.array.size, 2u);
    ExpectObjectEvent(payload.get().via.array.ptr[1].via.array.ptr[0],
                      "removed", object_key);
    ExpectObjectEvent(payload.get().via.array.ptr[1].via.array.ptr[1], "stored",
                      object_key);

    publisher.PublishObjectRemoved(object_key, "tenant-a", "group-a", {"disk"});
    ASSERT_TRUE(ReceivePayload(sub, &payload));
    ExpectObjectEvent(FirstEvent(payload), "removed", object_key);

    zmq_close(sub);
    zmq_ctx_destroy(ctx);
}

TEST(KvEventPublisherTest, ClearIsTenantScoped) {
    const std::string endpoint = MakeIpcEndpoint();
    KvEventConfig config;
    config.enabled = true;
    config.bind_endpoint = endpoint;
    config.backend_id = "mooncake-test";
    config.model_name = "model-a";
    config.block_size = 64;
    KvEventPublisher publisher(config);
    ASSERT_TRUE(publisher.enabled());

    void* ctx = zmq_ctx_new();
    ASSERT_NE(ctx, nullptr);
    void* sub = zmq_socket(ctx, ZMQ_SUB);
    ASSERT_NE(sub, nullptr);
    const int timeout_ms = 2000;
    ASSERT_EQ(
        zmq_setsockopt(sub, ZMQ_RCVTIMEO, &timeout_ms, sizeof(timeout_ms)), 0);
    ASSERT_EQ(zmq_connect(sub, endpoint.c_str()), 0);
    ASSERT_EQ(zmq_setsockopt(sub, ZMQ_SUBSCRIBE, "", 0), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    msgpack::object_handle payload;
    publisher.PublishStored("tenant-a-key", "cpu", "tenant-a");
    ASSERT_TRUE(ReceivePayload(sub, &payload));
    publisher.PublishStored("tenant-b-key", "cpu", "tenant-b");
    ASSERT_TRUE(ReceivePayload(sub, &payload));
    publisher.PublishCleared("tenant-a");
    ASSERT_TRUE(ReceivePayload(sub, &payload));
    const auto& event = FirstEvent(payload);
    EXPECT_EQ(MsgpackString(*FindMapValue(event, "event_type")), "cleared");
    EXPECT_EQ(MsgpackString(*FindMapValue(event, "tenant_id")), "tenant-a");

    zmq_close(sub);
    zmq_ctx_destroy(ctx);
}

TEST(KvEventPublisherTest, DisabledObjectKeySkipsObjectEventsButKeepsClear) {
    KvEventConfig config;
    config.enabled = true;
    config.bind_endpoint = MakeIpcEndpoint();
    config.backend_id = "mooncake-test";
    config.emit_object_key = false;
    KvEventPublisher publisher(config);
    ASSERT_TRUE(publisher.enabled());

    publisher.PublishStored("object-a", "cpu", "tenant-a");
    publisher.PublishRemoved("object-a", "cpu", "tenant-a");
    publisher.PublishCleared("tenant-a");

    for (int i = 0; i < 50; ++i) {
        const auto stats = publisher.GetStats();
        if (stats.skipped_keyless_events == 2 && stats.published_events == 1) {
            EXPECT_EQ(stats.published_batches, 1u);
            EXPECT_EQ(stats.dropped_events, 0u);
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    const auto stats = publisher.GetStats();
    FAIL() << "unexpected publisher stats: skipped="
           << stats.skipped_keyless_events
           << " published_events=" << stats.published_events
           << " published_batches=" << stats.published_batches;
}

TEST(KvEventPublisherTest, DropsOldestWhenQueueFull) {
    const std::string endpoint = MakeIpcEndpoint();
    KvEventConfig config;
    config.enabled = true;
    config.bind_endpoint = endpoint;
    config.backend_id = "mooncake-test";
    config.emit_object_key = true;
    config.queue_capacity = 2;
    KvEventPublisher publisher(config);
    ASSERT_TRUE(publisher.enabled());

    for (int i = 0; i < 100; ++i) {
        publisher.PublishStored(std::to_string(i), "cpu");
    }

    for (int i = 0; i < 50; ++i) {
        const auto stats = publisher.GetStats();
        if (stats.dropped_events >= 1) {
            EXPECT_GE(stats.dropped_events, 1u);
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    FAIL() << "expected dropped_events after queue overflow";
}

// Removal is driven entirely by the caller's previous-media set, in sorted
// order so a multi-medium object produces a deterministic event sequence.
TEST(KvEventPublisherTest, RemovalEmitsEveryMediumFromCaller) {
    const std::string endpoint = MakeIpcEndpoint();
    KvEventPublisher publisher(MakeEnabledConfig(endpoint));
    ASSERT_TRUE(publisher.enabled());
    ScopedSubscriber subscriber;
    ASSERT_TRUE(subscriber.Connect(endpoint));

    msgpack::object_handle payload;
    publisher.PublishObjectRemoved("object-a", "tenant-a", "group-a",
                                   {"disk", "cpu"});
    ASSERT_TRUE(ReceivePayload(subscriber.sub, &payload));
    ASSERT_EQ(EventCount(payload), 2u);
    ExpectObjectEvent(EventAt(payload, 0), "removed", "object-a");
    ExpectObjectEvent(EventAt(payload, 1), "removed", "object-a");
    EXPECT_EQ(MsgpackString(*FindMapValue(EventAt(payload, 0), "medium")),
              "cpu");
    EXPECT_EQ(MsgpackString(*FindMapValue(EventAt(payload, 1), "medium")),
              "disk");
}

// A medium that survives the mutation must not be announced as removed, even
// though the caller lists it as previously present. This is what keeps a
// partial eviction from retracting the tier that still holds a replica.
TEST(KvEventPublisherTest, SurvivingMediumIsNotRemoved) {
    const std::string endpoint = MakeIpcEndpoint();
    KvEventPublisher publisher(MakeEnabledConfig(endpoint));
    ASSERT_TRUE(publisher.enabled());
    ScopedSubscriber subscriber;
    ASSERT_TRUE(subscriber.Connect(endpoint));

    msgpack::object_handle payload;
    publisher.SyncObjectState("object-a", {"cpu"}, "tenant-a", "group-a",
                              {"cpu", "disk"});
    ASSERT_TRUE(ReceivePayload(subscriber.sub, &payload));
    ASSERT_EQ(EventCount(payload), 1u);
    ExpectObjectEvent(EventAt(payload, 0), "removed", "object-a");
    EXPECT_EQ(MsgpackString(*FindMapValue(EventAt(payload, 0), "medium")),
              "disk");
}

TEST(KvEventPublisherTest, RemovesBothMediaAfterStoredOnCpuAndDisk) {
    const std::string endpoint = MakeIpcEndpoint();
    KvEventPublisher publisher(MakeEnabledConfig(endpoint));
    ASSERT_TRUE(publisher.enabled());
    ScopedSubscriber subscriber;
    ASSERT_TRUE(subscriber.Connect(endpoint));

    msgpack::object_handle payload;
    publisher.PublishCommitted("object-a", {"cpu", "disk"}, "tenant-a",
                               "group-a");
    ASSERT_TRUE(ReceivePayload(subscriber.sub, &payload));
    ASSERT_EQ(EventCount(payload), 2u);
    ExpectObjectEvent(EventAt(payload, 0), "stored", "object-a");
    ExpectObjectEvent(EventAt(payload, 1), "stored", "object-a");

    publisher.PublishObjectRemoved("object-a", "tenant-a", "group-a",
                                   {"cpu", "disk"});
    ASSERT_TRUE(ReceivePayload(subscriber.sub, &payload));
    ASSERT_EQ(EventCount(payload), 2u);
    EXPECT_EQ(MsgpackString(*FindMapValue(EventAt(payload, 0), "medium")),
              "cpu");
    EXPECT_EQ(MsgpackString(*FindMapValue(EventAt(payload, 1), "medium")),
              "disk");
}

// Covers restart and snapshot-restore: metadata can name media the publisher
// A mutation that left the medium set alone carries no availability news, so it
// must stay off the wire. This is what separates SyncObjectState from a commit,
// which always re-announces.
TEST(KvEventPublisherTest, SyncWithUnchangedMediaEmitsNothing) {
    const std::string endpoint = MakeIpcEndpoint();
    KvEventPublisher publisher(MakeEnabledConfig(endpoint));
    ASSERT_TRUE(publisher.enabled());
    ScopedSubscriber subscriber;
    ASSERT_TRUE(subscriber.Connect(endpoint));

    publisher.SyncObjectState("object-a", {"cpu", "disk"}, "tenant-a",
                              "group-a", {"disk", "cpu"});
    EXPECT_TRUE(ExpectNoMessage(subscriber.sub));
}

// A medium that appears during an internal mutation, such as an offload landing
// on disk, has to be announced as stored even though no commit ran.
TEST(KvEventPublisherTest, SyncAnnouncesNewlyAppearedMedium) {
    const std::string endpoint = MakeIpcEndpoint();
    KvEventPublisher publisher(MakeEnabledConfig(endpoint));
    ASSERT_TRUE(publisher.enabled());
    ScopedSubscriber subscriber;
    ASSERT_TRUE(subscriber.Connect(endpoint));

    msgpack::object_handle payload;
    publisher.SyncObjectState("object-a", {"disk"}, "tenant-a", "group-a",
                              {"cpu"});
    ASSERT_TRUE(ReceivePayload(subscriber.sub, &payload));
    ASSERT_EQ(EventCount(payload), 2u);
    ExpectObjectEvent(EventAt(payload, 0), "removed", "object-a");
    EXPECT_EQ(MsgpackString(*FindMapValue(EventAt(payload, 0), "medium")),
              "cpu");
    ExpectObjectEvent(EventAt(payload, 1), "stored", "object-a");
    EXPECT_EQ(MsgpackString(*FindMapValue(EventAt(payload, 1), "medium")),
              "disk");
}

TEST(KvEventPublisherTest, DisabledObjectKeyCountsSkippedObjectEvents) {
    const std::string endpoint = MakeIpcEndpoint();
    KvEventConfig config = MakeEnabledConfig(endpoint);
    config.emit_object_key = false;
    KvEventPublisher publisher(config);
    ASSERT_TRUE(publisher.enabled());
    ScopedSubscriber subscriber;
    ASSERT_TRUE(subscriber.Connect(endpoint));

    publisher.PublishCommitted("object-a", {"cpu"}, "tenant-a", "group-a");
    publisher.PublishObjectRemoved("object-a", "tenant-a", "group-a", {"cpu"});
    EXPECT_TRUE(ExpectNoMessage(subscriber.sub));

    // Clear carries no object key, so it survives the filter.
    msgpack::object_handle payload;
    publisher.PublishCleared("tenant-a");
    ASSERT_TRUE(ReceivePayload(subscriber.sub, &payload));
    ASSERT_EQ(EventCount(payload), 1u);
    EXPECT_EQ(MsgpackString(*FindMapValue(EventAt(payload, 0), "event_type")),
              "cleared");

    const auto stats = publisher.GetStats();
    EXPECT_GT(stats.skipped_keyless_events, 0u);
}

TEST(KvEventPublisherTest, ClearedEventIsEnvelopeOnlyWithNilMedium) {
    const std::string endpoint = MakeIpcEndpoint();
    KvEventConfig config = MakeEnabledConfig(endpoint);
    config.model_name = "model-a";
    config.block_size = 64;
    config.additional_salt = "salt-a";
    config.lora_name = "adapter-a";
    config.dp_rank = 3;
    config.emit_legacy_compat_fields = true;
    KvEventPublisher publisher(config);
    ASSERT_TRUE(publisher.enabled());
    ScopedSubscriber subscriber;
    ASSERT_TRUE(subscriber.Connect(endpoint));

    msgpack::object_handle payload;
    publisher.PublishCleared("tenant-a");
    ASSERT_TRUE(ReceivePayload(subscriber.sub, &payload));
    ASSERT_EQ(EventCount(payload), 1u);
    const auto& cleared = EventAt(payload, 0);

    EXPECT_EQ(MsgpackString(*FindMapValue(cleared, "event_type")), "cleared");
    EXPECT_EQ(MsgpackString(*FindMapValue(cleared, "type")),
              "AllBlocksCleared");
    ASSERT_NE(FindMapValue(cleared, "medium"), nullptr);
    EXPECT_EQ(FindMapValue(cleared, "medium")->type, msgpack::type::NIL);

    // A cleared envelope carries no object, hash, token, or group fields.
    EXPECT_EQ(FindMapValue(cleared, "object_key"), nullptr);
    EXPECT_EQ(FindMapValue(cleared, "group_id"), nullptr);
    EXPECT_EQ(FindMapValue(cleared, "seq_hashes"), nullptr);
    EXPECT_EQ(FindMapValue(cleared, "block_hashes"), nullptr);
    EXPECT_EQ(FindMapValue(cleared, "base_block_idx"), nullptr);

    EXPECT_EQ(MsgpackString(*FindMapValue(cleared, "tenant_id")), "tenant-a");
    EXPECT_EQ(MsgpackString(*FindMapValue(cleared, "model_name")), "model-a");
    EXPECT_EQ(MsgpackString(*FindMapValue(cleared, "additional_salt")),
              "salt-a");
    EXPECT_EQ(MsgpackString(*FindMapValue(cleared, "lora_name")), "adapter-a");
    EXPECT_EQ(FindMapValue(cleared, "block_size")->as<uint32_t>(), 64u);
    EXPECT_EQ(FindMapValue(cleared, "dp_rank")->as<uint32_t>(), 3u);
}

// medium is the availability field, so an empty one would describe a replica
// the subscriber cannot place on any tier. Every entry point has to drop it
// rather than publish a nil medium.
TEST(KvEventPublisherTest, EmptyMediumProducesNoEvent) {
    const std::string endpoint = MakeIpcEndpoint();
    KvEventPublisher publisher(MakeEnabledConfig(endpoint));
    ScopedSubscriber subscriber;
    ASSERT_TRUE(subscriber.Connect(endpoint));

    publisher.PublishStored("object-a", "", "tenant-a", "group-a");
    EXPECT_TRUE(ExpectNoMessage(subscriber.sub));

    publisher.PublishRemoved("object-a", "", "tenant-a", "group-a");
    EXPECT_TRUE(ExpectNoMessage(subscriber.sub));

    publisher.PublishCommitted("object-a", {""}, "tenant-a", "group-a");
    EXPECT_TRUE(ExpectNoMessage(subscriber.sub));

    publisher.PublishObjectRemoved("object-a", "tenant-a", "group-a", {""});
    EXPECT_TRUE(ExpectNoMessage(subscriber.sub));

    publisher.SyncObjectState("object-a", {""}, "tenant-a", "group-a", {""});
    EXPECT_TRUE(ExpectNoMessage(subscriber.sub));
}

TEST(KvEventPublisherTest, RejectsInvalidUcsConfiguration) {
    auto config = MakeEnabledConfig(MakeIpcEndpoint());
    config.wire_format = "unknown";
    EXPECT_THROW(
        { KvEventPublisher publisher(config); }, std::invalid_argument);
    config.wire_format = "ucs";
    config.emit_object_key = false;
    EXPECT_THROW(
        { KvEventPublisher publisher(config); }, std::invalid_argument);
}

TEST(KvEventPublisherTest, PublishesUcsReplicaUpdatesOverZmq) {
    const auto endpoint = MakeIpcEndpoint();
    auto config = MakeEnabledConfig(endpoint);
    config.wire_format = "ucs";
    config.backend_id.clear();
    KvEventPublisher publisher(config);
    ASSERT_TRUE(publisher.enabled());
    ScopedSubscriber subscriber;
    ASSERT_TRUE(subscriber.Connect(endpoint));
    const std::string key = "glm-5@tp0@0123456789abcdef";
    publisher.PublishReplicaUpdate(key, {{"memory", "node-a:1234"},
                                         {"memory", "node-b:1234"},
                                         {"disk", "/cache/object"},
                                         {"local_disk", "node-c:1234"}});
    std::vector<std::string> frames;
    ASSERT_TRUE(ReceiveZmqMultipart(subscriber.sub, frames));
    ASSERT_EQ(frames.size(), 3u);
    EXPECT_EQ(frames[0], "mooncake");
    EXPECT_EQ(frames[1].size(), 8u);
    auto payload = msgpack::unpack(frames[2].data(), frames[2].size());
    ASSERT_EQ(payload.get().type, msgpack::type::ARRAY);
    ASSERT_EQ(payload.get().via.array.size, 2u);
    const double now = std::chrono::duration<double>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();
    EXPECT_NEAR(payload.get().via.array.ptr[0].as<double>(), now, 5.0);
    const auto& event = FirstEvent(payload);
    ASSERT_EQ(event.type, msgpack::type::ARRAY);
    ASSERT_EQ(event.via.array.size, 3u);
    EXPECT_EQ(MsgpackString(event.via.array.ptr[0]), "BlockUpdateEvent");
    EXPECT_EQ(MsgpackString(event.via.array.ptr[1]), key);
    const auto& replicas = event.via.array.ptr[2];
    ASSERT_EQ(replicas.via.array.size, 4u);
    EXPECT_EQ(MsgpackString(replicas.via.array.ptr[1].via.array.ptr[1]),
              "node-b:1234");
    EXPECT_EQ(MsgpackString(replicas.via.array.ptr[2].via.array.ptr[0]),
              "disk");
    EXPECT_EQ(MsgpackString(replicas.via.array.ptr[3].via.array.ptr[0]),
              "local_disk");

    publisher.PublishReplicaUpdate(key, {});
    ASSERT_TRUE(ReceivePayload(subscriber.sub, &payload));
    EXPECT_EQ(FirstEvent(payload).via.array.ptr[2].via.array.size, 0u);
    publisher.PublishCleared();
    ASSERT_TRUE(ReceivePayload(subscriber.sub, &payload));
    ASSERT_EQ(FirstEvent(payload).via.array.size, 1u);
    EXPECT_EQ(MsgpackString(FirstEvent(payload).via.array.ptr[0]),
              "RemoveAllEvent");
}

TEST(KvEventPublisherTest, ReplaysBoundedUcsHistoryWhileIdle) {
    auto config = MakeEnabledConfig(MakeIpcEndpoint());
    config.wire_format = "ucs";
    config.replay_endpoint = MakeIpcEndpoint();
    config.replay_buffer_capacity = 2;
    KvEventPublisher publisher(config);
    ASSERT_TRUE(publisher.enabled());
    for (uint64_t index = 1; index <= 3; ++index) {
        publisher.PublishReplicaUpdate("key-" + std::to_string(index),
                                       {{"memory", "node-a:1"}});
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (publisher.GetStats().published_batches < index &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        ASSERT_EQ(publisher.GetStats().published_batches, index);
    }
    ScopedSubscriber replay;
    replay.ctx = zmq_ctx_new();
    ASSERT_NE(replay.ctx, nullptr);
    replay.sub = zmq_socket(replay.ctx, ZMQ_DEALER);
    ASSERT_NE(replay.sub, nullptr);
    int timeout = 2000;
    zmq_setsockopt(replay.sub, ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
    ASSERT_EQ(zmq_connect(replay.sub, config.replay_endpoint.c_str()), 0);
    const unsigned char start[8]{0, 0, 0, 0, 0, 0, 0, 1};
    ASSERT_EQ(zmq_send(replay.sub, "", 0, ZMQ_SNDMORE), 0);
    ASSERT_EQ(zmq_send(replay.sub, start, sizeof(start), 0), 8);
    for (uint64_t expected = 2; expected <= 3; ++expected) {
        std::vector<std::string> frames;
        ASSERT_TRUE(ReceiveZmqMultipart(replay.sub, frames));
        ASSERT_EQ(frames.size(), 3u);
        ASSERT_EQ(frames[1].size(), 8u);
        uint64_t sequence = 0;
        for (unsigned char byte : frames[1]) sequence = (sequence << 8) | byte;
        EXPECT_EQ(sequence, expected);
        auto payload = msgpack::unpack(frames[2].data(), frames[2].size());
        EXPECT_EQ(MsgpackString(FirstEvent(payload).via.array.ptr[1]),
                  "key-" + std::to_string(expected));
    }
    std::vector<std::string> end;
    ASSERT_TRUE(ReceiveZmqMultipart(replay.sub, end));
    ASSERT_EQ(end.size(), 3u);
    EXPECT_EQ(end[1], std::string(8, static_cast<char>(0xff)));
    EXPECT_TRUE(end[2].empty());
}

class UcsStoreEventTest : public test::MasterServiceTest {};

TEST_F(UcsStoreEventTest, RejectsMultiTenantLegacyProtocol) {
    auto config = MasterServiceConfig::builder().build();
    config.enable_kv_events = true;
    config.kv_events_wire_format = "ucs";
    config.enable_multi_tenants = true;
    EXPECT_THROW({ MasterService service(config); }, std::invalid_argument);
}

TEST_F(UcsStoreEventTest, PublishesPositionChangesWithinOneTier) {
    const auto endpoint = MakeIpcEndpoint();
    auto config = MasterServiceConfig::builder().build();
    config.enable_kv_events = true;
    config.kv_events_wire_format = "ucs";
    config.kv_events_bind_endpoint = endpoint;
    MasterService service(config);
    ScopedSubscriber subscriber;
    ASSERT_TRUE(subscriber.Connect(endpoint, 5000));
    const auto first = PrepareSimpleSegment(service, "node-a:1");
    const auto second = PrepareSimpleSegment(
        service, "node-b:1", kDefaultSegmentBase + kDefaultSegmentSize);
    ReplicateConfig placement;
    placement.replica_num = 2;
    placement.preferred_segments = {"node-a:1", "node-b:1"};
    const std::string key = "glm-5@tp0@0123456789abcdef";
    ASSERT_TRUE(service
                    .PutStart(first.client_id, key, TenantId::Default(), 1024,
                              placement)
                    .has_value());
    EXPECT_TRUE(ExpectNoMessage(subscriber.sub));
    ASSERT_TRUE(service
                    .PutEnd(first.client_id, key, TenantId::Default(),
                            ReplicaType::MEMORY)
                    .has_value());
    msgpack::object_handle payload;
    ASSERT_TRUE(ReceivePayload(subscriber.sub, &payload));
    ASSERT_EQ(FirstEvent(payload).via.array.ptr[2].via.array.size, 2u);
    ASSERT_TRUE(
        service.UnmountSegment(first.segment_id, first.client_id).has_value());
    ClearInvalidHandlesForTest(service);
    ASSERT_TRUE(ReceivePayload(subscriber.sub, &payload));
    const auto& remaining = FirstEvent(payload).via.array.ptr[2];
    ASSERT_EQ(remaining.via.array.size, 1u);
    EXPECT_EQ(MsgpackString(remaining.via.array.ptr[0].via.array.ptr[1]),
              "node-b:1");
    ASSERT_TRUE(service.UnmountSegment(second.segment_id, second.client_id)
                    .has_value());
    ClearInvalidHandlesForTest(service);
    ASSERT_TRUE(ReceivePayload(subscriber.sub, &payload));
    EXPECT_EQ(FirstEvent(payload).via.array.ptr[2].via.array.size, 0u);
}

TEST_F(UcsStoreEventTest, RemoveAllRetractsEachDeletedObject) {
    const auto endpoint = MakeIpcEndpoint();
    auto config = MasterServiceConfig::builder().build();
    config.enable_kv_events = true;
    config.kv_events_wire_format = "ucs";
    config.kv_events_bind_endpoint = endpoint;
    MasterService service(config);
    ScopedSubscriber subscriber;
    ASSERT_TRUE(subscriber.Connect(endpoint));
    const auto segment = PrepareSimpleSegment(service, "node-a:1");
    const std::string key = "glm-5@tp0@0123456789abcdef";
    ReplicateConfig placement;
    ASSERT_TRUE(service
                    .PutStart(segment.client_id, key, TenantId::Default(), 1024,
                              placement)
                    .has_value());
    ASSERT_TRUE(service
                    .PutEnd(segment.client_id, key, TenantId::Default(),
                            ReplicaType::MEMORY)
                    .has_value());
    msgpack::object_handle payload;
    ASSERT_TRUE(ReceivePayload(subscriber.sub, &payload));
    ASSERT_EQ(service.RemoveAll(TenantId::Default(), true), 1);
    ASSERT_TRUE(ReceivePayload(subscriber.sub, &payload));
    const auto& removed = FirstEvent(payload);
    ASSERT_EQ(removed.via.array.size, 3u);
    EXPECT_EQ(MsgpackString(removed.via.array.ptr[1]), key);
    EXPECT_EQ(removed.via.array.ptr[2].via.array.size, 0u);
}

#endif  // MOONCAKE_ENABLE_KV_EVENTS

}  // namespace
}  // namespace mooncake
