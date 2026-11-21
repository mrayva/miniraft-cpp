#include <miniraft/durable.h>
#include <algorithm>
#include <charconv>
#include <iostream>
#include <limits>
#include <map>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace {
void Usage(std::ostream& out) {
    out << "Usage: miniraft-migrate-disk-state --input-base PREFIX --output-base PREFIX "
           "--node-id ID --voters ID,ID,... --commit-index INDEX [--dry-run]\n"
        << "The complete original voter set and commit index must come from "
           "independently trusted records.\n"
        << "Only offline, un-compacted TDiskState logs with valid retained membership "
           "history can be migrated.\n"
        << "Usage: miniraft-migrate-disk-state --bind-durable --durable-base PREFIX "
           "--node-id ID --voters ID,ID,... [--commit-index INDEX] [--dry-run]\n"
        << "Omit --commit-index for an unconfigured legacy log or legacy snapshot; "
           "include it to validate a configuration-bearing snapshot history.\n";
}

uint64_t ParseUnsigned(std::string_view text, const char* name) {
    uint64_t value = 0;
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (text.empty() || error != std::errc{} || end != text.data() + text.size())
        throw std::invalid_argument(std::string("Invalid ") + name);
    return value;
}

std::vector<uint32_t> ParseVoters(std::string_view text) {
    if (text.empty() || text.back() == ',')
        throw std::invalid_argument("Invalid voter list");
    std::vector<uint32_t> voters;
    size_t start = 0;
    while (start < text.size()) {
        const auto comma = text.find(',', start);
        const auto end = comma == std::string_view::npos ? text.size() : comma;
        const auto id = ParseUnsigned(text.substr(start, end - start), "voter ID");
        if (!id || id > std::numeric_limits<uint32_t>::max())
            throw std::invalid_argument("Voter IDs must be positive 32-bit integers");
        voters.push_back(uint32_t(id));
        if (comma == std::string_view::npos) break;
        start = comma + 1;
    }
    if (voters.empty()) throw std::invalid_argument("The voter set must not be empty");
    std::sort(voters.begin(), voters.end());
    if (std::adjacent_find(voters.begin(), voters.end()) != voters.end())
        throw std::invalid_argument("The voter set contains duplicate IDs");
    return voters;
}

void PrintInfo(const TLegacyDiskStateMigrationInfo& info) {
    std::cout << "current term: " << info.CurrentTerm << '\n'
              << "voted for: " << info.VotedFor << '\n'
              << "last log index: " << info.LastLogIndex << '\n'
              << "last log term: " << info.LastLogTerm << '\n'
              << "commit index: " << info.CommitIndex << '\n';
}
}

int main(int argc, char** argv) {
    try {
        std::map<std::string, std::string> values;
        bool dryRun = false;
        bool bindDurable = false;
        for (int i = 1; i < argc; ++i) {
            const std::string option = argv[i];
            if (option == "--help" || option == "-h") {
                Usage(std::cout);
                return 0;
            }
            if (option == "--dry-run") {
                if (dryRun) throw std::invalid_argument("Duplicate --dry-run option");
                dryRun = true;
                continue;
            }
            if (option == "--bind-durable") {
                if (bindDurable) throw std::invalid_argument("Duplicate --bind-durable option");
                bindDurable = true;
                continue;
            }
            if (option != "--input-base" && option != "--output-base" && option != "--node-id" &&
                option != "--voters" && option != "--commit-index" && option != "--durable-base")
                throw std::invalid_argument("Unknown option: " + option);
            if (i + 1 == argc) throw std::invalid_argument("Missing value for " + option);
            if (!values.emplace(option, argv[++i]).second)
                throw std::invalid_argument("Duplicate option: " + option);
        }
        if (bindDurable) {
            for (const auto* required : {"--durable-base", "--node-id", "--voters"})
                if (!values.contains(required)) throw std::invalid_argument(std::string("Missing ") + required);
            if (values.contains("--input-base") || values.contains("--output-base"))
                throw std::invalid_argument("--bind-durable cannot be combined with disk migration paths");
            if (values.at("--durable-base").empty())
                throw std::invalid_argument("Durable store base path must not be empty");
        } else {
            for (const auto* required : {"--input-base", "--output-base", "--node-id", "--voters", "--commit-index"})
                if (!values.contains(required)) throw std::invalid_argument(std::string("Missing ") + required);
            if (values.contains("--durable-base"))
                throw std::invalid_argument("--durable-base requires --bind-durable");
            if (values.at("--input-base").empty() || values.at("--output-base").empty())
                throw std::invalid_argument("Store base paths must not be empty");
        }

        const auto rawNodeId = ParseUnsigned(values.at("--node-id"), "node ID");
        if (!rawNodeId || rawNodeId > std::numeric_limits<uint32_t>::max())
            throw std::invalid_argument("Node ID must be a positive 32-bit integer");
        const auto voters = ParseVoters(values.at("--voters"));
        const auto nodeId = uint32_t(rawNodeId);

        TLegacyDiskStateMigrationInfo info;
        if (bindDurable) {
            const bool useTrustedHistory = values.contains("--commit-index");
            const auto commitIndex = useTrustedHistory
                ? ParseUnsigned(values.at("--commit-index"), "commit index") : uint64_t{0};
            uint64_t snapshotIndex = 0;
            uint64_t snapshotTerm = 0;
            {
                TDurableState state(values.at("--durable-base"), nodeId);
                info = {state.CurrentTerm, state.VotedFor, state.LastLogIndex,
                    state.LastLogTerm, state.CommitIndex};
                snapshotIndex = state.SnapshotIndex;
                snapshotTerm = state.SnapshotTerm;
                std::string reason;
                const bool valid = useTrustedHistory
                    ? state.CanBindVotersFromTrustedHistory(voters, commitIndex, &reason)
                    : state.CanBindVotersFromTrustedBaseline(voters, &reason);
                if (!valid) throw std::invalid_argument("Trusted voter binding refused: " + reason);
                if (!dryRun) {
                    if (useTrustedHistory) state.BindVotersFromTrustedHistory(voters, commitIndex);
                    else state.BindVotersFromTrustedBaseline(voters);
                }
            }
            if (!dryRun) {
                TDurableState verified(values.at("--durable-base"), nodeId);
                const auto expectedCommitIndex = useTrustedHistory ? commitIndex : info.CommitIndex;
                if (verified.GetBoundVoters() != voters || verified.CurrentTerm != info.CurrentTerm ||
                    verified.VotedFor != info.VotedFor || verified.LastLogIndex != info.LastLogIndex ||
                    verified.LastLogTerm != info.LastLogTerm || verified.CommitIndex != expectedCommitIndex ||
                    verified.SnapshotIndex != snapshotIndex || verified.SnapshotTerm != snapshotTerm)
                    throw std::runtime_error("Durable voter binding failed reopen verification");
                info = {verified.CurrentTerm, verified.VotedFor, verified.LastLogIndex,
                    verified.LastLogTerm, verified.CommitIndex};
                std::cout << "trusted voter binding completed\n";
            } else {
                std::cout << "dry run passed; no store was written\n";
            }
        } else {
            const auto commitIndex = ParseUnsigned(values.at("--commit-index"), "commit index");
            if (dryRun) {
                info = CheckLegacyDiskStateMigration(values.at("--input-base"), nodeId, voters, commitIndex);
                std::cout << "dry run passed; no store was written\n";
            } else {
                info = MigrateLegacyDiskState(values.at("--input-base"), values.at("--output-base"),
                    nodeId, voters, commitIndex);
                std::cout << "migration completed to " << values.at("--output-base") << "\n";
            }
        }
        PrintInfo(info);
        std::cout << "The tool records the supplied voter set and commit index when provided; "
                     "it cannot verify their historical completeness.\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "Migration refused: " << error.what() << '\n';
        Usage(std::cerr);
        return 2;
    }
}
