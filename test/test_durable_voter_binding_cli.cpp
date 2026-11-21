#include <miniraft/durable.h>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {
void Require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

std::string Quote(const std::string& value) {
    std::string result{"'"};
    for (const char ch : value) {
        if (ch == '\'') result += "'\\''";
        else result += ch;
    }
    result += '\'';
    return result;
}

std::string ReadFile(const fs::path& path) {
    std::ifstream input(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

void SeedUnboundDurableStore(const std::string& base, uint32_t vote) {
    TDurableState state(base, 7);
    state.CurrentTerm = 3;
    state.VotedFor = vote;
    auto entry = NewHoldedMessage<TLogEntry>();
    entry->Term = 1;
    state.Append(entry);
}
}

int main(int argc, char** argv) {
    try {
        Require(argc == 2, "expected path to miniraft-migrate-disk-state");
        const auto nonce = std::chrono::steady_clock::now().time_since_epoch().count();
        const auto directory = fs::temp_directory_path() /
            ("miniraft-durable-voter-cli-" + std::to_string(nonce));
        fs::create_directories(directory);
        struct TCleanup {
            fs::path Directory;
            ~TCleanup() { std::error_code ignored; fs::remove_all(Directory, ignored); }
        } cleanup{directory};

        auto invoke = [&](const std::string& base, const std::string& voters, bool dryRun,
                          const std::string& commitIndex = std::string{}) {
            auto command = Quote(argv[1]) + " --bind-durable --durable-base " + Quote(base) +
                " --node-id 7 --voters " + Quote(voters);
            if (!commitIndex.empty()) command += " --commit-index " + Quote(commitIndex);
            if (dryRun) command += " --dry-run";
            command += " > /dev/null 2>&1";
            return std::system(command.c_str());
        };

        const auto validBase = (directory / "valid").string();
        SeedUnboundDurableStore(validBase, 2);
        const auto validImage = validBase + ".durable.7";
        const auto beforeDryRun = ReadFile(validImage);
        Require(invoke(validBase, "1,2,3", true) == 0, "trusted-baseline dry run failed");
        Require(ReadFile(validImage) == beforeDryRun, "dry run changed the durable image");
        {
            TDurableState recovered(validBase, 7);
            Require(recovered.HasUnboundRecoveredState(), "dry run unexpectedly bound voters");
        }
        Require(invoke(validBase, "1,2,3", false) == 0, "trusted-baseline binding failed");
        {
            TDurableState recovered(validBase, 7);
            Require(recovered.GetBoundVoters() == std::vector<uint32_t>({1, 2, 3}),
                "bound voter set did not survive reopen");
            Require(!recovered.HasUnboundRecoveredState(), "bound state still appears unbound");
            Require(recovered.CurrentTerm == 3 && recovered.VotedFor == 2 &&
                recovered.LastLogIndex == 1 && recovered.LastLogTerm == 1,
                "voter binding changed existing Raft state");
        }

        const auto historyBase = (directory / "history").string();
        SeedUnboundDurableStore(historyBase, 2);
        const auto historyImage = historyBase + ".durable.7";
        const auto beforeHistoryDryRun = ReadFile(historyImage);
        Require(invoke(historyBase, "1,2,3", true, "1") == 0,
            "trusted-history dry run failed");
        Require(ReadFile(historyImage) == beforeHistoryDryRun,
            "trusted-history dry run changed the durable image");
        {
            TDurableState recovered(historyBase, 7);
            Require(recovered.HasUnboundRecoveredState(),
                "trusted-history dry run unexpectedly bound voters");
        }
        Require(invoke(historyBase, "1,2,3", false, "1") == 0,
            "trusted-history binding failed");
        {
            TDurableState recovered(historyBase, 7);
            Require(recovered.GetBoundVoters() == std::vector<uint32_t>({1, 2, 3}),
                "trusted-history voter set did not survive reopen");
            Require(recovered.CommitIndex == 1,
                "trusted-history commit boundary did not survive reopen");
        }

        const auto invalidBase = (directory / "invalid").string();
        SeedUnboundDurableStore(invalidBase, 4);
        const auto invalidImage = invalidBase + ".durable.7";
        const auto beforeRejectedBind = ReadFile(invalidImage);
        Require(invoke(invalidBase, "1,2,3", false) != 0,
            "binding accepted a baseline that excludes the stored vote");
        Require(ReadFile(invalidImage) == beforeRejectedBind,
            "rejected binding changed the durable image");
        {
            TDurableState recovered(invalidBase, 7);
            Require(recovered.HasUnboundRecoveredState(), "rejected binding changed voter state");
        }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
