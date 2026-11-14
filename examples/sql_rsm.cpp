#include "sql_rsm.h"
#include "payload_validation.h"
#include <iostream>
#include <climits>
#include <charconv>
#include <algorithm>
#include <string_view>

namespace {
int ReadAuthorization(void*, int action, const char*, const char* second, const char*, const char*) {
    if (action == SQLITE_SELECT || action == SQLITE_READ || action == SQLITE_RECURSIVE) return SQLITE_OK;
    if (action == SQLITE_FUNCTION && second) {
        // Keep this allowlist limited to trusted SQLite built-ins. In particular,
        // readonly statements can still call extension functions with side effects.
        for (const char* function : {"abs", "avg", "coalesce", "count", "glob", "hex", "ifnull",
             "instr", "length", "like", "lower", "ltrim", "max", "min", "nullif", "quote",
             "replace", "round", "rtrim", "substr", "substring", "sum", "total", "trim",
             "typeof", "unicode", "upper", "group_concat"})
            if (sqlite3_stricmp(function, second) == 0) return SQLITE_OK;
    }
    return SQLITE_DENY;
}
int WriteAuthorization(void*, int action, const char* first, const char* second, const char* database, const char* context) {
    const auto metadata = [](const char* name) {
        return name && sqlite3_stricmp(name, "raft_metadata_") == 0;
    };
    if (metadata(first) || ((action == SQLITE_CREATE_TRIGGER || action == SQLITE_DROP_TRIGGER ||
        action == SQLITE_ALTER_TABLE || action == SQLITE_CREATE_INDEX || action == SQLITE_DROP_INDEX) && metadata(second))) return SQLITE_DENY;
    if (database && sqlite3_stricmp(database, "main") != 0) return SQLITE_DENY;
    switch (action) {
    case SQLITE_INSERT: case SQLITE_UPDATE: case SQLITE_DELETE:
    case SQLITE_CREATE_TABLE: case SQLITE_CREATE_INDEX: case SQLITE_CREATE_VIEW: case SQLITE_CREATE_TRIGGER:
    case SQLITE_DROP_TABLE: case SQLITE_DROP_INDEX: case SQLITE_DROP_VIEW: case SQLITE_DROP_TRIGGER:
    case SQLITE_REINDEX: case SQLITE_ANALYZE:
        return SQLITE_OK;
    default:
        return ReadAuthorization(nullptr, action, first, second, database, context);
    }
}
using TAuthorizer = int (*)(void*, int, const char*, const char*, const char*, const char*);
struct TSqlPolicy {
    sqlite3* Db;
    explicit TSqlPolicy(sqlite3* db, TAuthorizer authorize) : Db(db) {
        if (sqlite3_set_authorizer(Db, authorize, nullptr) != SQLITE_OK)
            throw TMessageError("Cannot install SQL policy");
    }
    ~TSqlPolicy() { sqlite3_set_authorizer(Db, nullptr, nullptr); }
};
struct TTransactionCleanup {
    sqlite3* Db;
    ~TTransactionCleanup() {
        if (!sqlite3_get_autocommit(Db)) sqlite3_exec(Db, "ROLLBACK", nullptr, nullptr, nullptr);
    }
};
struct TReadBudget {
    sqlite3* Db;
    uint32_t Remaining;
    const int Interval;
    const int PreviousLength;
    TReadBudget(sqlite3* db, uint32_t maxSteps)
        : Db(db), Remaining(maxSteps), Interval(std::min<uint32_t>(maxSteps, 1000)),
          PreviousLength(sqlite3_limit(db, SQLITE_LIMIT_LENGTH, TMessageLimits{}.MaxFrameBytes)) {
        sqlite3_progress_handler(Db, Interval, Progress, this);
    }
    ~TReadBudget() {
        sqlite3_progress_handler(Db, 0, nullptr, nullptr);
        sqlite3_limit(Db, SQLITE_LIMIT_LENGTH, PreviousLength);
    }
    static int Progress(void* context) noexcept {
        auto& budget = *static_cast<TReadBudget*>(context);
        if (budget.Remaining <= uint32_t(budget.Interval)) return 1;
        budget.Remaining -= budget.Interval;
        return 0;
    }
};
using TStatement = std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)>;
constexpr size_t MaxSqlSnapshotBytes = TSnapshot::MaxBytes - 40 - 4 * TSnapshot::MaxVoters;

struct TSnapshotDatabase {
    // Keep the deserialize buffer alive until after the connection closes.
    std::unique_ptr<unsigned char, decltype(&sqlite3_free)> Buffer{nullptr, sqlite3_free};
    std::unique_ptr<sqlite3, decltype(&sqlite3_close_v2)> Db{nullptr, sqlite3_close_v2};
};

TSnapshotDatabase OpenSnapshotDatabase(std::string_view bytes) {
    if (bytes.size() < 100 || bytes.size() > MaxSqlSnapshotBytes)
        throw TMessageError("Invalid SQL snapshot size");
    TSnapshotDatabase result;
    constexpr size_t MalformedImageReadSlack = 20;
    result.Buffer.reset(static_cast<unsigned char*>(sqlite3_malloc64(bytes.size() + MalformedImageReadSlack)));
    if (!result.Buffer) throw std::bad_alloc();
    memcpy(result.Buffer.get(), bytes.data(), bytes.size());
    memset(result.Buffer.get() + bytes.size(), 0, MalformedImageReadSlack);
    if (result.Buffer.get()[18] != 1 || result.Buffer.get()[19] != 1)
        throw TMessageError("SQL snapshot is not a standalone rollback-mode image");

    sqlite3* raw = nullptr;
    const int opened = sqlite3_open_v2(":memory:", &raw,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MEMORY, nullptr);
    result.Db.reset(raw);
    if (opened != SQLITE_OK) throw TMessageError("Cannot open SQL snapshot database");
    const int rc = sqlite3_deserialize(result.Db.get(), "main", result.Buffer.get(),
        sqlite3_int64(bytes.size()), sqlite3_int64(bytes.size()), SQLITE_DESERIALIZE_READONLY);
    if (rc != SQLITE_OK) throw TMessageError("Invalid SQL snapshot database image");
    return result;
}

uint64_t SnapshotAppliedIndex(sqlite3* db) {
    sqlite3_stmt* raw = nullptr;
    const int prepared = sqlite3_prepare_v2(db,
        "SELECT value FROM raft_metadata_ WHERE key='LastAppliedIndex'", -1, &raw, nullptr);
    TStatement statement(raw, sqlite3_finalize);
    if (prepared != SQLITE_OK || !statement) throw TMessageError("SQL snapshot has no progress metadata");
    int rc = sqlite3_step(statement.get());
    if (rc != SQLITE_ROW || sqlite3_column_type(statement.get(), 0) != SQLITE_TEXT)
        throw TMessageError("Invalid SQL snapshot progress metadata");
    const auto* text = reinterpret_cast<const char*>(sqlite3_column_text(statement.get(), 0));
    const auto length = sqlite3_column_bytes(statement.get(), 0);
    if (!text || length <= 0) throw TMessageError("Invalid SQL snapshot progress metadata");
    uint64_t index = 0;
    const auto parsed = std::from_chars(text, text + length, index);
    if (parsed.ec != std::errc{} || parsed.ptr != text + length || !index || index > uint64_t(INT64_MAX))
        throw TMessageError("Invalid SQL snapshot progress metadata");
    if (sqlite3_step(statement.get()) != SQLITE_DONE)
        throw TMessageError("Duplicate SQL snapshot progress metadata");
    return index;
}

void ValidateSnapshotDatabase(sqlite3* db, uint64_t index) {
    if (!index || index > uint64_t(INT64_MAX)) throw TMessageError("Invalid SQL snapshot index");
    sqlite3_stmt* raw = nullptr;
    const int prepared = sqlite3_prepare_v2(db, "PRAGMA quick_check(1)", -1, &raw, nullptr);
    TStatement statement(raw, sqlite3_finalize);
    if (prepared != SQLITE_OK || !statement)
        throw TMessageError(std::string("Cannot run SQL snapshot integrity check: ") + sqlite3_errmsg(db));
    const int check = sqlite3_step(statement.get());
    if (check != SQLITE_ROW || sqlite3_column_type(statement.get(), 0) != SQLITE_TEXT)
        throw TMessageError(std::string("Cannot read SQL snapshot integrity check: ") + sqlite3_errmsg(db));
    const auto* result = reinterpret_cast<const char*>(sqlite3_column_text(statement.get(), 0));
    const auto length = sqlite3_column_bytes(statement.get(), 0);
    if (!result || std::string_view(result, size_t(length)) != "ok")
        throw TMessageError("SQL snapshot integrity check failed: " +
            std::string(result ? result : "<null>", result ? size_t(length) : 6));
    if (sqlite3_step(statement.get()) != SQLITE_DONE)
        throw TMessageError("SQL snapshot integrity check failed");
    if (SnapshotAppliedIndex(db) != index)
        throw TMessageError("SQL snapshot application progress does not match its boundary");
}

uint64_t ReadPragmaInteger(sqlite3* db, const char* pragma) {
    sqlite3_stmt* raw = nullptr;
    const std::string query = std::string("PRAGMA ") + pragma;
    const int prepared = sqlite3_prepare_v2(db, query.c_str(), -1, &raw, nullptr);
    TStatement statement(raw, sqlite3_finalize);
    if (prepared != SQLITE_OK || !statement || sqlite3_step(statement.get()) != SQLITE_ROW ||
        sqlite3_column_type(statement.get(), 0) != SQLITE_INTEGER)
        throw TMessageError("Cannot inspect SQL snapshot size");
    const auto value = sqlite3_column_int64(statement.get(), 0);
    if (value <= 0 || sqlite3_step(statement.get()) != SQLITE_DONE)
        throw TMessageError("Invalid SQL snapshot size metadata");
    return uint64_t(value);
}

void ValidateSnapshotForTarget(sqlite3* snapshot, sqlite3* target, uint64_t index) {
    ValidateSnapshotDatabase(snapshot, index);
    if (!sqlite3_get_autocommit(target))
        throw TMessageError("Cannot replace SQL state during a transaction");
    if (ReadPragmaInteger(snapshot, "page_size") != ReadPragmaInteger(target, "page_size"))
        throw TMessageError("SQL snapshot page size does not match the target database");
}

TSnapshotDatabase CopyDatabaseToMemory(sqlite3* source, uint64_t pageSize) {
    TSnapshotDatabase result;
    sqlite3* raw = nullptr;
    const int opened = sqlite3_open_v2(":memory:", &raw,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MEMORY, nullptr);
    result.Db.reset(raw);
    if (opened != SQLITE_OK) throw TMessageError("Cannot open SQL snapshot database");

    // SQLite's backup API requires matching page sizes for an in-memory
    // destination. Set it before copying any pages.
    const auto setPageSize = std::string("PRAGMA page_size = ") + std::to_string(pageSize);
    if (sqlite3_exec(result.Db.get(), setPageSize.c_str(), nullptr, nullptr, nullptr) != SQLITE_OK)
        throw TMessageError("Cannot configure SQL snapshot page size");

    sqlite3_backup* backup = sqlite3_backup_init(result.Db.get(), "main", source, "main");
    if (!backup) throw TMessageError("Cannot begin SQL snapshot capture");
    const int copied = sqlite3_backup_step(backup, -1);
    const int finished = sqlite3_backup_finish(backup);
    if (copied != SQLITE_DONE || finished != SQLITE_OK)
        throw TMessageError("Cannot complete SQL snapshot capture");
    return result;
}
}

std::string TSql::ExecuteRead(const std::string& q) {
    if (q.size() > INT_MAX || q.find('\0') != std::string::npos)
        throw TMessageError("Invalid SQL read text");
    TReadBudget budget(Db, ReadLimits.MaxVmSteps);
    TSqlPolicy policy(Db, ReadAuthorization); // retained through prepare, stepping and finalization
    sqlite3_stmt* raw = nullptr;
    const char* tail = nullptr;
    int rc = sqlite3_prepare_v2(Db, q.data(), int(q.size()), &raw, &tail);
    TStatement statement(raw, sqlite3_finalize);
    if (rc != SQLITE_OK || !statement || !sqlite3_stmt_readonly(statement.get()))
        throw TMessageError("SQL read must be one permitted read-only statement");
    raw = nullptr;
    rc = sqlite3_prepare_v2(Db, tail, int(q.data() + q.size() - tail), &raw, nullptr);
    TStatement extra(raw, sqlite3_finalize);
    if (rc != SQLITE_OK || extra) throw TMessageError("Multiple SQL read statements are not permitted");
    std::string text;
    const auto append = [&](std::string_view part) {
        if (part.size() > ReadLimits.MaxResultBytes - text.size())
            throw TMessageError("SQL read output budget exceeded");
        text.append(part);
    };
    bool firstRow = true;
    while ((rc = sqlite3_step(statement.get())) == SQLITE_ROW) {
        const int count = sqlite3_column_count(statement.get());
        if (firstRow) {
            for (int i = 0; i < count; ++i) {
                const char* name = sqlite3_column_name(statement.get(), i);
                if (!name) throw TMessageError("Cannot read SQL column name");
                append(name);
                if (i + 1 < count) append(",");
            }
            append("\n");
            firstRow = false;
        }
        for (int i = 0; i < count; ++i) {
            if (sqlite3_column_type(statement.get(), i) == SQLITE_NULL) {
                append("null");
            } else {
                const auto* value = reinterpret_cast<const char*>(sqlite3_column_text(statement.get(), i));
                if (!value) throw TMessageError("Cannot read SQL column value");
                const auto length = sqlite3_column_bytes(statement.get(), i);
                append(std::string_view(value, length));
            }
            if (i + 1 < count) append(",");
        }
        append("\n");
    }
    if (rc != SQLITE_DONE) throw TMessageError("SQL read failed or exceeded its execution budget");
    if (firstRow) append("\n"); // Preserve the legacy empty-result format.
    return text;
}

void TSql::Validate(TMessageHolder<TCommandRequest> message) const {
    auto query = message.Cast<TReadSql>();
    ValidatePayloadRange(message, query->Query, query->QuerySize);
    if (memchr(query->Query, 0, query->QuerySize)) throw TMessageError("Invalid SQL query text");
}

TSql::TSql(const std::string& path, int serverId, TSqlReadLimits readLimits)
    : ReadLimits(readLimits)
{
    if (!ReadLimits.MaxVmSteps ||
        ReadLimits.MaxResultBytes > TMessageLimits{}.MaxFrameBytes - sizeof(TCommandResponse))
        throw std::invalid_argument("Invalid SQL read limits");
    std::string dbPath = path == ":memory:" ? path : path + "." + std::to_string(serverId);
    const int opened = sqlite3_open(dbPath.c_str(), &Db);
    // Constructors that fail must also close the connection and release its locks.
    std::unique_ptr<sqlite3, decltype(&sqlite3_close)> cleanup(Db, sqlite3_close);
    if (opened != SQLITE_OK) throw std::runtime_error("Cannot open SQL database");
    RequireExecute("CREATE TABLE IF NOT EXISTS raft_metadata_ (key TEXT PRIMARY KEY, value TEXT)");
    sqlite3_stmt* raw = nullptr;
    const int prepared = sqlite3_prepare_v2(Db,
        "SELECT value FROM raft_metadata_ WHERE key='LastAppliedIndex'", -1, &raw, nullptr);
    TStatement statement(raw, sqlite3_finalize);
    if (prepared != SQLITE_OK || !statement) throw std::runtime_error("Cannot read SQL application progress");
    int rc = sqlite3_step(statement.get());
    if (rc == SQLITE_ROW) {
        if (sqlite3_column_type(statement.get(), 0) != SQLITE_TEXT)
            throw std::runtime_error("Invalid SQL application progress");
        const auto* text = reinterpret_cast<const char*>(sqlite3_column_text(statement.get(), 0));
        if (!text) throw std::runtime_error("Cannot read SQL application progress");
        const auto length = sqlite3_column_bytes(statement.get(), 0);
        uint64_t index = 0;
        const auto parsed = std::from_chars(text, text + length, index);
        if (parsed.ec != std::errc{} || parsed.ptr != text + length)
            throw std::runtime_error("Invalid SQL application progress");
        LastAppliedIndex = index;
        rc = sqlite3_step(statement.get());
    }
    if (rc != SQLITE_DONE) throw std::runtime_error("Invalid SQL application progress");
    cleanup.release();
    std::cerr << "LastAppliedIndex: " << LastAppliedIndex << std::endl;
}

TSql::~TSql()
{
    if (Db) {
        if (sqlite3_close(Db) != SQLITE_OK) {
            std::cerr << "Failed to close db, error:" << sqlite3_errmsg(Db) << std::endl;
        }
    }
}

int TSql::Execute(const std::string& q) {
    char* err = nullptr;
    LastError.clear();
    const int rc = sqlite3_exec(Db, q.c_str(), nullptr, nullptr, &err);
    if (rc != SQLITE_OK) LastError = err ? err : sqlite3_errmsg(Db);
    sqlite3_free(err);
    return rc;
}

void TSql::RequireExecute(const std::string& q) {
    if (Execute(q) != SQLITE_OK) throw std::runtime_error("SQL state update failed: " + LastError);
}

TMessageHolder<TMessage> TSql::Read(TMessageHolder<TCommandRequest> message, uint64_t index) {
    auto readSql = message.Cast<TReadSql>();
    ValidatePayloadRange(message, readSql->Query, readSql->QuerySize);
    return Reply(ExecuteRead(std::string(readSql->Query, readSql->QuerySize)), index);
}

TMessageHolder<TMessage> TSql::Write(TMessageHolder<TLogEntry> message, uint64_t index) {
    if (index <= LastAppliedIndex) return Reply({}, index);
    auto entry = message.Cast<TSqlLogEntry>();
    ValidatePayloadRange(message, entry->Query, entry->QuerySize);
    const std::string query(entry->Query, entry->QuerySize);
    if (query.find('\0') != std::string::npos) throw TMessageError("Invalid SQL write text");
    if (!sqlite3_get_autocommit(Db)) throw std::runtime_error("SQL application transaction already active");
    TTransactionCleanup transaction{Db};
    RequireExecute("BEGIN IMMEDIATE");
    int rc;
    std::string error;
    {
        // Only the payload runs under this policy. It cannot end the transaction
        // or access the progress table, including through views and triggers.
        TSqlPolicy policy(Db, WriteAuthorization);
        rc = Execute(query);
        error = LastError;
    }
    if (rc != SQLITE_OK) {
        if (!sqlite3_get_autocommit(Db)) RequireExecute("ROLLBACK");
        switch (rc & 0xff) {
        case SQLITE_ERROR: case SQLITE_AUTH: case SQLITE_CONSTRAINT: case SQLITE_MISMATCH:
            break; // SQL/application rejection: consume this entry without its effects.
        default:
            throw std::runtime_error("SQL application execution failed: " + error);
        }
        // Some errors (including OR ROLLBACK) already rolled back the transaction.
        // Start a new one to record the rejected entry; never ignore failure here.
        RequireExecute("BEGIN IMMEDIATE");
    }
    const auto progress = std::to_string(index);
    RequireExecute("INSERT INTO raft_metadata_ (key, value) VALUES ('LastAppliedIndex','" + progress +
        "') ON CONFLICT(key) DO UPDATE SET value='" + progress + "'");
    RequireExecute("COMMIT");
    LastAppliedIndex = index;
    return Reply(error, index);
}

TMessageHolder<TMessage> TSql::Reply(const std::string& ans, uint64_t index)
{
    if (ans.size() > TMessageLimits{}.MaxFrameBytes - sizeof(TCommandResponse))
        throw TMessageError("SQL response exceeds frame limit");
    auto res = NewHoldedMessage<TCommandResponse>(sizeof(TCommandResponse)+ans.size());
    res->Index = index;
    memcpy(res->Data, ans.data(), ans.size());
    return res;
}

TMessageHolder<TLogEntry> TSql::Prepare(TMessageHolder<TCommandRequest> command) {
    auto write = command.Cast<TWriteSql>();
    ValidatePayloadRange(command, write->Query, write->QuerySize);
    if (memchr(write->Query, 0, write->QuerySize)) throw TMessageError("Invalid SQL write text");
    auto dataSize = command->Len - sizeof(TCommandRequest);
    auto entry = NewHoldedMessage<TLogEntry>(sizeof(TLogEntry)+dataSize);
    memcpy(entry->Data, command->Data, dataSize);
    return entry;
}

std::string TSql::SaveSnapshot() const {
    if (!LastAppliedIndex || LastAppliedIndex > uint64_t(INT64_MAX) || !sqlite3_get_autocommit(Db))
        throw TMessageError("SQL snapshot requires committed application progress");
    const auto pages = ReadPragmaInteger(Db, "page_count");
    const auto pageSize = ReadPragmaInteger(Db, "page_size");
    if (pages > MaxSqlSnapshotBytes / pageSize)
        throw TMessageError("SQL snapshot exceeds the maximum image size");
    auto consistent = CopyDatabaseToMemory(Db, pageSize);
    sqlite3_int64 size = 0;
    auto* bytes = sqlite3_serialize(consistent.Db.get(), "main", &size, 0);
    std::unique_ptr<unsigned char, decltype(&sqlite3_free)> image(bytes, sqlite3_free);
    if (!bytes || size <= 0 || uint64_t(size) > MaxSqlSnapshotBytes)
        throw TMessageError("Cannot serialize SQL snapshot within the size limit");
    std::string result(reinterpret_cast<const char*>(bytes), size_t(size));
    if (result.size() < 100 ||
        !((static_cast<unsigned char>(result[18]) == 1 && static_cast<unsigned char>(result[19]) == 1) ||
          (static_cast<unsigned char>(result[18]) == 2 && static_cast<unsigned char>(result[19]) == 2)))
        throw TMessageError("Unsupported SQL snapshot database format");
    // SQLite documents this conversion for deserializing a complete database
    // image. Backup copied all WAL-visible pages above, so rollback mode is now
    // the correct self-contained representation.
    result[18] = 1;
    result[19] = 1;
    return result;
}

void TSql::ValidateSnapshot(std::string_view bytes, uint64_t index) const {
    auto image = OpenSnapshotDatabase(bytes);
    ValidateSnapshotForTarget(image.Db.get(), Db, index);
}

void TSql::LoadSnapshot(std::string_view bytes, uint64_t index) {
    auto image = OpenSnapshotDatabase(bytes);
    ValidateSnapshotForTarget(image.Db.get(), Db, index);
    sqlite3_backup* backup = sqlite3_backup_init(Db, "main", image.Db.get(), "main");
    if (!backup) throw TMessageError("Cannot begin SQL snapshot installation");
    const int copied = sqlite3_backup_step(backup, -1);
    const int finished = sqlite3_backup_finish(backup);
    if (copied != SQLITE_DONE || finished != SQLITE_OK)
        throw TMessageError("Cannot atomically install SQL snapshot");
    LastAppliedIndex = index;
}

