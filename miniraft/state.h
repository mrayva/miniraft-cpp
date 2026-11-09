#pragma once

#include "messages.h"
#include "snapshot.h"

struct TStateFields {
    uint64_t CurrentTerm = 1;
    uint32_t VotedFor = 0;
    uint64_t LastLogIndex = 0;
    uint64_t LastLogTerm = 0;
    uint64_t SnapshotIndex = 0;
    uint64_t SnapshotTerm = 0;

    bool operator==(const TStateFields& other) const = default;
};

struct IState: TStateFields {
    virtual void RemoveLast() = 0;
    virtual void Append(TMessageHolder<TLogEntry>) = 0;
    // Absolute indices; only the suffix after SnapshotIndex has log entries.
    // The boundary term remains available through LogTerm(SnapshotIndex).
    virtual TMessageHolder<TLogEntry> Get(int64_t index) const = 0;
    virtual void Commit() = 0;
    virtual bool SupportsSnapshots() const { return false; }
    virtual std::shared_ptr<const TSnapshot> GetSnapshot() const { return {}; }
    virtual void InstallSnapshot(const TSnapshot&) { throw std::logic_error("Storage snapshots are unsupported"); }
    virtual ~IState() = default;

    uint64_t LogTerm(int64_t index = -1) const {
        if (index < 0) {
            index = LastLogIndex;
        }
        if (index > 0 && uint64_t(index) == SnapshotIndex) return SnapshotTerm;
        if (index < 1 || uint64_t(index) < SnapshotIndex || index > LastLogIndex) {
            return 0;
        } else {
            return Get(index)->Term;
        }
    }
};

struct TState: IState {
    std::vector<TMessageHolder<TLogEntry>> Log;
    std::shared_ptr<const TSnapshot> Snapshot;

    TState() = default;
    TState(uint64_t currentTerm, uint32_t votedFor, const std::vector<TMessageHolder<TLogEntry>>& log)
        : Log(log)
    {
        CurrentTerm = currentTerm;
        VotedFor = votedFor;
        if (!log.empty()) {
            LastLogIndex = log.size();
            LastLogTerm = log.back()->Term;
        }
    }

    void RemoveLast() override {
        if (Log.empty()) return;
        Log.pop_back();
        LastLogIndex = SnapshotIndex + Log.size();
        LastLogTerm = Log.empty() ? SnapshotTerm : Log.back()->Term;
    }

    void Append(TMessageHolder<TLogEntry> entry) override {
        Log.emplace_back(std::move(entry));
        LastLogIndex = SnapshotIndex + Log.size();
        LastLogTerm = Log.back()->Term;
    }

    TMessageHolder<TLogEntry> Get(int64_t index) const override {
        if (index < 1 || uint64_t(index) <= SnapshotIndex || uint64_t(index) > LastLogIndex) return {};
        return Log[index - SnapshotIndex - 1];
    }

    bool SupportsSnapshots() const override { return true; }
    std::shared_ptr<const TSnapshot> GetSnapshot() const override { return Snapshot; }
    void InstallSnapshot(const TSnapshot& snapshot) override {
        EncodeSnapshot(snapshot); // Validate before changing any state.
        if (snapshot.Index <= SnapshotIndex || snapshot.Term > CurrentTerm)
            throw std::invalid_argument("Obsolete or future snapshot");
        auto image = std::make_shared<const TSnapshot>(snapshot);
        std::vector<TMessageHolder<TLogEntry>> suffix;
        if (snapshot.Index <= LastLogIndex && LogTerm(snapshot.Index) == snapshot.Term)
            suffix.assign(Log.begin() + (snapshot.Index - SnapshotIndex), Log.end());
        Log.swap(suffix);
        Snapshot = std::move(image);
        SnapshotIndex = snapshot.Index;
        SnapshotTerm = snapshot.Term;
        LastLogIndex = SnapshotIndex + Log.size();
        LastLogTerm = Log.empty() ? SnapshotTerm : Log.back()->Term;
    }

    void Commit() override { }
};

