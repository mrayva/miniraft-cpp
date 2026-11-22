#include <miniraft/durable.h>
#include <miniraft/persist.h>
#include <miniraft/configuration.h>
#include <miniraft/snapshot_codec.h>
#include <chrono>
#include <cstdlib>
#include <cstring>
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

TMessageHolder<TLogEntry> MakeEntry(uint64_t term, const std::string& data) {
    auto entry = NewHoldedMessage<TLogEntry>(sizeof(TLogEntry) + data.size());
    entry->Term = term;
    std::memcpy(entry->Data, data.data(), data.size());
    return entry;
}

void SeedUnboundSplitStore(const std::string& base, uint32_t vote) {
    const std::vector<uint32_t> original{1, 2, 3};
    const std::vector<uint32_t> active{2, 3, 4};
    TDiskState state(base, 7);
    state.CurrentTerm = 5;
    state.VotedFor = vote;
    state.Append(MakeEntry(2, "before joint"));
    state.Append(EncodeConfigurationEntry({EConfigurationPhase::Joint, 3, 0, 0,
        original, active}));
    state.Append(MakeEntry(3, "in joint"));
    state.Append(EncodeConfigurationEntry({EConfigurationPhase::Stable, 4, 2, 3,
        original, active}));
    state.CommitIndex = 2;
    state.Commit();
}

TSnapshot SeedUnboundSplitSnapshotStore(const std::string& base) {
    const std::vector<uint32_t> original{1, 2, 3};
    const std::vector<uint32_t> active{2, 3, 4};
    auto history = TConfigurationHistory::Stable(original, 0, 0, 0, 0);
    history = history.Advance(1, EncodeConfigurationEntry({EConfigurationPhase::Joint,
        2, 0, 0, original, active}));
    history = history.Advance(2, EncodeConfigurationEntry({EConfigurationPhase::Stable,
        3, 1, 2, original, active}));
    const auto expectedSnapshot = history.MakeSnapshot("split configuration snapshot");
    {
        TDiskState state(base, 7);
        state.CurrentTerm = 4;
        state.VotedFor = 4;
        state.InstallSnapshot(TSnapshot{1, 2, original, "legacy split snapshot"});
        state.InstallSnapshot(TSnapshot{2, 3, original, "legacy split snapshot successor"});
    }
    const auto encoded = EncodeSnapshot(expectedSnapshot);
    std::ofstream output(base + ".state.7.snapshot.g2", std::ios::binary | std::ios::trunc);
    Require(bool(output), "cannot open split snapshot fixture for replacement");
    output.write(encoded.data(), std::streamsize(encoded.size()));
    Require(bool(output), "cannot write split snapshot fixture");
    for (const auto& path : {base + ".entries.7.g1", base + ".index.7.g1",
                              base + ".state.7.snapshot.g1"}) {
        std::ofstream orphan(path, std::ios::binary | std::ios::trunc);
        Require(bool(orphan), "cannot create stale split generation fixture");
        orphan << "stale generation";
        Require(bool(orphan), "cannot write stale split generation fixture");
    }
    return expectedSnapshot;
}

TSnapshot SeedCompactedSplitMigrationStore(const std::string& base) {
    const std::vector<uint32_t> original{5, 6, 7};
    const std::vector<uint32_t> intermediate{6, 7, 8};
    const std::vector<uint32_t> finalVoters{7, 8, 9};
    const TSnapshotConfigurationTransition anchor{original, intermediate, 0, 0, 6, 3, 7, 3};
    TSnapshot snapshot{7, 3, intermediate, "compacted CLI snapshot"};
    snapshot.StableConfiguration = true;
    snapshot.ConfigurationIndex = 7;
    snapshot.ConfigurationTerm = 3;
    snapshot.PreviousVoters = original;
    snapshot.PreviousJointIndex = 6;
    snapshot.PreviousJointTerm = 3;
    snapshot.ConfigurationTransitions = {anchor};
    {
        TDiskState state(base, 7);
        state.BindVoters(original);
        state.CurrentTerm = 5;
        state.VotedFor = 9;
        state.Commit();
        for (uint64_t term : {1, 2, 2, 3, 3}) state.Append(MakeEntry(term, "prefix"));
        state.Append(EncodeConfigurationEntry({
            EConfigurationPhase::Joint, 3, 0, 0, original, intermediate}));
        state.Append(EncodeConfigurationEntry({
            EConfigurationPhase::Stable, 3, 6, 3, original, intermediate}));
        state.CommitIndex = 7;
        state.Commit();
        state.InstallSnapshot(snapshot);
        state.Append(EncodeConfigurationEntry({
            EConfigurationPhase::Joint, 4, 7, 3, intermediate, finalVoters}));
        state.Append(EncodeConfigurationEntry({
            EConfigurationPhase::Stable, 4, 8, 4, intermediate, finalVoters}));
        state.Append(MakeEntry(4, "retained CLI suffix"));
        state.CommitIndex = 9;
        state.Commit();
    }
    return snapshot;
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
        auto invokeSplit = [&](const std::string& base, const std::string& voters, bool dryRun,
                               const std::string& commitIndex) {
            auto command = Quote(argv[1]) + " --bind-disk-state --disk-base " + Quote(base) +
                " --node-id 7 --voters " + Quote(voters) + " --commit-index " + Quote(commitIndex);
            if (dryRun) command += " --dry-run";
            command += " > /dev/null 2>&1";
            return std::system(command.c_str());
        };
        auto invokeMigration = [&](const std::string& inputBase, const std::string& outputBase,
                                   bool dryRun) {
            auto command = Quote(argv[1]) + " --input-base " + Quote(inputBase) +
                " --output-base " + Quote(outputBase) + " --node-id 7 --voters 5,6,7 --commit-index 9";
            if (dryRun) command += " --dry-run";
            const auto log = directory / "migration.log";
            command += " > " + Quote(log.string()) + " 2>&1";
            const auto status = std::system(command.c_str());
            if (status) std::cerr << ReadFile(log) << '\n';
            return status;
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

        const auto splitBase = (directory / "split-history").string();
        SeedUnboundSplitStore(splitBase, 4);
        const auto readSplitFiles = [&] {
            return std::vector<std::string>{ReadFile(splitBase + ".entries.7"),
                ReadFile(splitBase + ".index.7"), ReadFile(splitBase + ".state.7")};
        };
        const auto beforeSplitDryRun = readSplitFiles();
        Require(invokeSplit(splitBase, "1,2,3", true, "2") == 0,
            "split-store trusted-history dry run failed");
        Require(readSplitFiles() == beforeSplitDryRun,
            "split-store dry run changed storage bytes");
        {
            TDiskState recovered(splitBase, 7);
            Require(recovered.GetBoundVoters().empty(),
                "split-store dry run unexpectedly bound voters");
        }
        Require(invokeSplit(splitBase, "1,2,3", false, "2") == 0,
            "split-store trusted-history binding failed");
        {
            TDiskState recovered(splitBase, 7);
            Require(recovered.GetBoundVoters() == std::vector<uint32_t>({1, 2, 3}),
                "split-store binding did not preserve the original voter set");
            Require(recovered.CurrentTerm == 5 && recovered.VotedFor == 4 &&
                recovered.CommitIndex == 2 && recovered.LastLogIndex == 4 &&
                recovered.LastLogTerm == 4,
                "split-store binding changed existing Raft state");
            const auto beforeJoint = recovered.Get(1);
            const auto inJoint = recovered.Get(3);
            Require(beforeJoint->Term == 2 && beforeJoint->Len == sizeof(TLogEntry) + 12 &&
                std::string(beforeJoint->Data, 12) == "before joint" &&
                inJoint->Term == 3 && inJoint->Len == sizeof(TLogEntry) + 8 &&
                std::string(inJoint->Data, 8) == "in joint",
                "split-store binding changed retained log entries");
            const auto history = ScanCommittedConfigurationHistory(
                recovered, TConfigurationHistory::Stable({1, 2, 3}, 0, 0, 0, 0));
            Require(history.Committed.IsJoint() &&
                history.Committed.OldVoters() == std::vector<uint32_t>({1, 2, 3}) &&
                history.Committed.NewVoters() == std::vector<uint32_t>({2, 3, 4}),
                "split-store binding did not recover committed joint membership");
        }

        const auto invalidSplitBase = (directory / "invalid-split").string();
        SeedUnboundSplitStore(invalidSplitBase, 5);
        const auto invalidSplitFilesBefore = std::vector<std::string>{
            ReadFile(invalidSplitBase + ".entries.7"), ReadFile(invalidSplitBase + ".index.7"),
            ReadFile(invalidSplitBase + ".state.7")};
        Require(invokeSplit(invalidSplitBase, "1,2,3", false, "2") != 0,
            "split-store binding accepted a vote outside validated membership history");
        Require(std::vector<std::string>{ReadFile(invalidSplitBase + ".entries.7"),
                ReadFile(invalidSplitBase + ".index.7"), ReadFile(invalidSplitBase + ".state.7")} ==
                invalidSplitFilesBefore,
            "rejected split-store binding changed storage bytes");
        {
            TDiskState recovered(invalidSplitBase, 7);
            Require(recovered.GetBoundVoters().empty(),
                "rejected split-store binding changed voter state");
        }

        const auto splitSnapshotBase = (directory / "split-snapshot-history").string();
        const auto expectedSplitSnapshot = SeedUnboundSplitSnapshotStore(splitSnapshotBase);
        const auto readSplitSnapshotFiles = [&] {
            return std::vector<std::string>{ReadFile(splitSnapshotBase + ".entries.7"),
                ReadFile(splitSnapshotBase + ".index.7"), ReadFile(splitSnapshotBase + ".state.7"),
                ReadFile(splitSnapshotBase + ".entries.7.g2"), ReadFile(splitSnapshotBase + ".index.7.g2"),
                ReadFile(splitSnapshotBase + ".state.7.snapshot.g2"),
                ReadFile(splitSnapshotBase + ".entries.7.g1"), ReadFile(splitSnapshotBase + ".index.7.g1"),
                ReadFile(splitSnapshotBase + ".state.7.snapshot.g1")};
        };
        const auto beforeSplitSnapshotDryRun = readSplitSnapshotFiles();
        Require(invokeSplit(splitSnapshotBase, "1,2,3", true, "2") == 0,
            "split snapshot trusted-history dry run failed");
        Require(readSplitSnapshotFiles() == beforeSplitSnapshotDryRun,
            "split snapshot dry run changed storage bytes");
        Require(invokeSplit(splitSnapshotBase, "1,2,3", false, "2") == 0,
            "split snapshot trusted-history binding failed");
        {
            TDiskState recovered(splitSnapshotBase, 7);
            Require(recovered.GetBoundVoters() == std::vector<uint32_t>({1, 2, 3}) &&
                recovered.CurrentTerm == 4 && recovered.VotedFor == 4 &&
                recovered.CommitIndex == 2 && recovered.LastLogIndex == 2 &&
                recovered.LastLogTerm == 3 && recovered.GetSnapshot() &&
                *recovered.GetSnapshot() == expectedSplitSnapshot,
                "split snapshot binding did not preserve state and validated history");
        }

        const auto compactedMigrationBase = (directory / "compacted-migration-source").string();
        const auto compactedMigrationOutput = (directory / "compacted-migration-output").string();
        const auto expectedMigrationSnapshot = SeedCompactedSplitMigrationStore(compactedMigrationBase);
        auto readMigrationFiles = [&](const std::string& base) {
            std::vector<std::pair<std::string, std::string>> result;
            for (const auto* suffix : {".entries.7", ".index.7", ".state.7",
                    ".entries.7.g1", ".index.7.g1", ".state.7.snapshot.g1",
                    ".entries.7.g2", ".index.7.g2", ".state.7.snapshot.g2"}) {
                const auto path = base + suffix;
                if (fs::exists(path)) result.emplace_back(suffix, ReadFile(path));
            }
            return result;
        };
        const auto beforeCompactedMigration = readMigrationFiles(compactedMigrationBase);
        Require(invokeMigration(compactedMigrationBase, compactedMigrationOutput, true) == 0,
            "compacted split-store migration dry run failed");
        Require(readMigrationFiles(compactedMigrationBase) == beforeCompactedMigration,
            "compacted migration dry run changed source files");
        Require(!fs::exists(compactedMigrationOutput + ".durable.7"),
            "compacted migration dry run created a destination");
        Require(invokeMigration(compactedMigrationBase, compactedMigrationOutput, false) == 0,
            "compacted split-store migration failed");
        Require(readMigrationFiles(compactedMigrationBase) == beforeCompactedMigration,
            "compacted migration changed source files");
        {
            TDurableState recovered(compactedMigrationOutput, 7);
            Require(recovered.GetBoundVoters() == std::vector<uint32_t>({5, 6, 7}) &&
                recovered.GetSnapshot() && *recovered.GetSnapshot() == expectedMigrationSnapshot &&
                recovered.SnapshotIndex == 7 && recovered.SnapshotTerm == 3 &&
                recovered.LastLogIndex == 10 && recovered.LastLogTerm == 4 &&
                recovered.CommitIndex == 9 && recovered.VotedFor == 9,
                "compacted migration did not preserve the snapshot and durable boundaries");
            const auto suffix = recovered.Get(10);
            Require(suffix && std::string(suffix->Data, suffix->Len - sizeof(TLogEntry)) ==
                "retained CLI suffix", "compacted migration changed the retained suffix");
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

        const auto missingBase = (directory / "missing-store").string();
        Require(invoke(missingBase, "1,2,3", true) != 0 &&
            !fs::exists(missingBase + ".durable.7"),
            "durable dry run created a missing store");
        Require(invokeSplit(missingBase, "1,2,3", true, "0") != 0 &&
            !fs::exists(missingBase + ".entries.7") && !fs::exists(missingBase + ".index.7") &&
            !fs::exists(missingBase + ".state.7"),
            "split-store dry run created a missing store");
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
