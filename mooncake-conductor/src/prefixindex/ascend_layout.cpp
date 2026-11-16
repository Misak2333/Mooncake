#include "conductor/prefixindex/cache_layout.h"

#include <json/json.h>

#include <memory>
#include <set>
#include <string>

namespace mooncake::conductor::prefixindex {

std::string ResolveAscendDeployment(const common::ServiceConfig& service,
                                    std::string_view declaration,
                                    SourceLayout* result) {
    const auto* model = FindModelCacheSpec(service.model_name);
    if (!result || !model || !model->checkpoints || model->sliding_window ||
        service.publisher_kind != common::PublisherKind::kVllm ||
        service.hash_profile.strategy != "vllm_v1")
        return "unsupported Ascend deployment/model";
    if (declaration.empty() || declaration.size() > 65536)
        return "cache layout declaration must contain at most 65536 bytes";
    Json::CharReaderBuilder builder;
    builder["rejectDupKeys"] = true;
    builder["failIfExtra"] = true;
    Json::Value root;
    std::string error;
    const std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    if (!reader->parse(declaration.data(),
                       declaration.data() + declaration.size(), &root,
                       &error) ||
        !root.isObject())
        return "invalid cache layout JSON";
    auto positive = [](const Json::Value& value) {
        return value.isInt64() && value.asInt64() > 0;
    };
    if (!root["format"].isString() ||
        root["format"].asString() != "ascend-store-v1" ||
        !root["model_name"].isString() ||
        FindModelCacheSpec(root["model_name"].asString()) != model ||
        !positive(root["hash_unit_tokens"]) ||
        root["hash_unit_tokens"].asInt64() != service.block_size ||
        !positive(root["alignment_tokens"]) ||
        !root["storage_domain"].isString() ||
        root["storage_domain"].asString().empty() ||
        !root["groups"].isArray() || root["groups"].empty() ||
        root["groups"].size() > 128)
        return "invalid deployment identity or geometry";
    auto layout = std::make_shared<CacheLayout>();
    layout->id = std::string(model->name);
    layout->hash_unit_tokens = service.block_size;
    layout->alignment_tokens = root["alignment_tokens"].asInt64();
    // GLM-Next's private Indexer K-pool tail is regenerated at pool-aligned
    // boundaries, not a persisted checkpoint. Never infer this for other rings.
    if (model->token_compression && (root["transient_alignment_tokens"] != 4 ||
                                     layout->alignment_tokens % 4 != 0))
        return "GLM Flash requires verified pool-aligned tail recovery";
    SourceLayout binding;
    binding.source = LayoutSource::kVllmAnnouncement;
    binding.layout = layout;
    binding.storage_domain = root["storage_domain"].asString();
    binding.readable_domains.insert(binding.storage_domain);
    bool checkpoint = false, dense = false;
    std::set<std::string> prefixes;
    for (const auto& item : root["groups"]) {
        if (!item.isObject() || !item["group_idx"].isInt64() ||
            item["group_idx"].asInt64() < 0 || !item["kind"].isString() ||
            !positive(item["block_tokens"]) ||
            !item["key_prefixes"].isArray() || item["key_prefixes"].empty() ||
            item["key_prefixes"].size() > 256)
            return "invalid cache group declaration";
        const auto id = item["group_idx"].asInt64();
        const auto slot = layout->groups.size();
        if (!binding.groups.emplace(id, slot).second)
            return "duplicate deployment group";
        CacheGroupLayout group;
        group.id = std::to_string(id);
        group.event_kind = item["kind"].asString();
        group.block_tokens = item["block_tokens"].asInt64();
        group.alignment_tokens = group.block_tokens;
        if (group.event_kind == "mamba") {
            if (item["mamba_cache_mode"] != "align")
                return "only aligned KDA checkpoints are supported";
            group.match = MatchPolicy::kCheckpoint;
            checkpoint = true;
        } else if (group.event_kind == "full_attention" ||
                   group.event_kind == "mla_attention") {
            group.match = MatchPolicy::kDense;
            dense = true;
        } else {
            return "unsupported group recovery policy";
        }
        group.parts.clear();
        for (const auto& part : item["key_prefixes"]) {
            if (!part.isString()) return "invalid object namespace";
            const auto prefix = part.asString();
            if (prefix.size() > 1024 || !prefix.ends_with('@') ||
                !prefix.starts_with(root["model_name"].asString() + "@") ||
                prefix.find("@group:" + group.id + "@") == std::string::npos ||
                prefix.find("@cache_role:kv@") == std::string::npos ||
                !prefixes.insert(prefix).second)
                return "invalid or duplicate object namespace";
            binding.objects.push_back({.key_prefix = prefix,
                                       .group = slot,
                                       .part = group.parts.size()});
            group.parts.push_back(prefix);
        }
        layout->groups.push_back(std::move(group));
    }
    if (!checkpoint || !dense)
        return "hybrid deployment must declare dense and checkpoint groups";
    if (auto validation = ValidateCacheLayout(*layout); !validation.empty())
        return validation;
    *result = std::move(binding);
    return "";
}

}  // namespace mooncake::conductor::prefixindex
