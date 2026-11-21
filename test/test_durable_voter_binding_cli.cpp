#include <miniraft/durable.h>
#include <miniraft/configuration.h>
#include <miniraft/snapshot_codec.h>
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
    if (!input) throw std::runtime_error("cannot open durable fixture for reading");
    std::string bytes{std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
    if (input.bad()) throw std::runtime_error("cannot read durable fixture");
    return bytes;
}

void StoreLittleEndian(std::string& bytes, size_t offset, uint64_t value, unsigned width) {
    for (unsigned i = 0; i < width; ++i) bytes[offset + i] = char(value >> (8 * i));
}

void ReplaceDurableSnapshot(const fs::path& path, const TSnapshot& snapshot) {
    auto bytes = ReadFile(path);
    Require(bytes.substr(0, 8) == "MRFTD004", "unexpected durable snapshot fixture version");
    constexpr size_t imageLengthOffset = 52;
    constexpr size_t imageOffset = 60;
    Require(bytes.size() >= imageOffset, "truncated durable snapshot fixture");
    uint64_t oldLength = 0;
    for (unsigned i = 0; i < 8; ++i)
        oldLength |= uint64_t(uint8_t(bytes[imageLengthOffset + i])) << (8 * i);
    Require(oldLength == bytes.size() - imageOffset, "invalid durable snapshot fixture length");
    const auto image = EncodeSnapshot(snapshot);
    bytes.replace(imageOffset, size_t(oldLength), image);
    StoreLittleEndian(bytes, imageLengthOffset, image.size(), 8);
    StoreLittleEndian(bytes, 8, bytes.size() - 20, 8);
    const auto crc = NSnapshotCodec::Checksum(bytes.substr(0, 8) + bytes.substr(20));
    StoreLittleEndian(bytes, 16, crc, 4);
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    Require(bool(output), "cannot open durable snapshot fixture for replacement");
    output.write(bytes.data(), std::streamsize(bytes.size()));
    Require(bool(output), "cannot write durable snapshot fixture");
}

void SeedUnboundDurableStore(const std::string& base, uint32_t vote) {
    TDurableState state(base, 7);
    state.CurrentTerm = 3;
    state.VotedFor = vote;
    auto entry = NewHoldedMessage<TLogEntry>();
    entry->Term = 1;
    state.Append(entry);
}

TSnapshot SeedConfigurationSnapshotStore(const std::string& base) {
    auto history = TConfigurationHistory::Stable({1, 2, 3}, 0, 0, 0, 0);
    TConfigurationRecord joint{EConfigurationPhase::Joint, 1, 0, 0, {1, 2, 3}, {1, 2, 4}};
    history = history.Advance(1, EncodeConfigurationEntry(joint));
    TConfigurationRecord stable{EConfigurationPhase::Stable, 1, 1, 1, {1, 2, 3}, {1, 2, 4}};
    history = history.Advance(2, EncodeConfigurationEntry(stable));
    auto snapshot = history.MakeSnapshot("configuration snapshot");

    {
        TDurableState state(base, 7);
        state.CurrentTerm = snapshot.Term;
        state.VotedFor = 4; // A historical voter, absent from the original set.
        state.InstallSnapshot(TSnapshot{snapshot.Index, snapshot.Term,
            snapshot.Voters, "legacy snapshot"});
    }
    ReplaceDurableSnapshot(base + ".durable.7", snapshot);
    return snapshot;
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

        const auto snapshotBase = (directory / "configuration-snapshot").string();
        const auto expectedSnapshot = SeedConfigurationSnapshotStore(snapshotBase);
        const auto snapshotImage = snapshotBase + ".durable.7";
        const auto beforeSnapshotDryRun = ReadFile(snapshotImage);
        Require(invoke(snapshotBase, "1,2,3", true, "2") == 0,
            "configuration-snapshot trusted-history dry run failed");
        Require(ReadFile(snapshotImage) == beforeSnapshotDryRun,
            "configuration-snapshot dry run changed the durable image");
        Require(invoke(snapshotBase, "1,2,3", false, "2") == 0,
            "configuration-snapshot trusted-history binding failed");
        {
            TDurableState recovered(snapshotBase, 7);
            Require(recovered.GetBoundVoters() == std::vector<uint32_t>({1, 2, 3}),
                "configuration-snapshot binding changed the original voter set");
            Require(recovered.CommitIndex == 2 && recovered.GetSnapshot() &&
                *recovered.GetSnapshot() == expectedSnapshot,
                "configuration-snapshot binding did not preserve the snapshot and commit boundary");
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
