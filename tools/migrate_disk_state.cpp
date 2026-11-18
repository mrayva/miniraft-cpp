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
        << "Only offline, un-compacted TDiskState logs without configuration entries "
           "can be migrated.\n";
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
            if (option != "--input-base" && option != "--output-base" && option != "--node-id" &&
                option != "--voters" && option != "--commit-index")
                throw std::invalid_argument("Unknown option: " + option);
            if (i + 1 == argc) throw std::invalid_argument("Missing value for " + option);
            if (!values.emplace(option, argv[++i]).second)
                throw std::invalid_argument("Duplicate option: " + option);
        }
        for (const auto* required : {"--input-base", "--output-base", "--node-id", "--voters", "--commit-index"})
            if (!values.contains(required)) throw std::invalid_argument(std::string("Missing ") + required);
        if (values.at("--input-base").empty() || values.at("--output-base").empty())
            throw std::invalid_argument("Store base paths must not be empty");

        const auto rawNodeId = ParseUnsigned(values.at("--node-id"), "node ID");
        if (!rawNodeId || rawNodeId > std::numeric_limits<uint32_t>::max())
            throw std::invalid_argument("Node ID must be a positive 32-bit integer");
        const auto voters = ParseVoters(values.at("--voters"));
        const auto rawCommitIndex = ParseUnsigned(values.at("--commit-index"), "commit index");
        const auto nodeId = uint32_t(rawNodeId);

        TLegacyDiskStateMigrationInfo info;
        if (dryRun) {
            info = CheckLegacyDiskStateMigration(values.at("--input-base"), nodeId, voters, rawCommitIndex);
            std::cout << "dry run passed; no store was written\n";
        } else {
            info = MigrateLegacyDiskState(values.at("--input-base"), values.at("--output-base"),
                nodeId, voters, rawCommitIndex);
            std::cout << "migration completed to " << values.at("--output-base") << "\n";
        }
        PrintInfo(info);
        std::cout << "The tool records the supplied voter set and commit index; it cannot verify their historical completeness.\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "Migration refused: " << error.what() << '\n';
        Usage(std::cerr);
        return 2;
    }
}
