#pragma once
#include "state.h"
#include <functional>
#include <memory>
#include <string>

// Linux: atomic full-image replacement, with file and directory fsync.
// O(log size) per update; intended as a conservative durability baseline.
class TDurableState : public IState {
public:
    enum class EStage { TempCreated, Written, FileSynced, Renamed, DirectorySynced };
    // Diagnostics/fault injection. Throwing from this hook during a live
    // operation is fatal, just like an I/O failure.
    using THook = std::function<void(EStage)>;
    TDurableState(const std::string& name, uint32_t id, THook hook = {});
    ~TDurableState() override;
    TDurableState(const TDurableState&) = delete;
    TDurableState& operator=(const TDurableState&) = delete;
    void RemoveLast() override;
    void Append(TMessageHolder<TLogEntry> entry) override;
    TMessageHolder<TLogEntry> Get(int64_t index) const override;
    void Commit() override;
private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;
    void Publish();
};
