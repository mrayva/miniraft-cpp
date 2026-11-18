# Offline migration from split legacy disk state

`miniraft-migrate-disk-state` copies an offline, un-compacted `TDiskState`
store into a new `TDurableState` store. Build it on Linux with:

```sh
cmake --build build --target miniraft-migrate-disk-state
```

Stop the replica and preserve a backup of all three source files first. Supply
the complete original voter set from an independently trusted deployment
record, and supply a commit index that is consistent with the application's
persisted state. The split `TDiskState` metadata does not contain a commit
boundary, so the utility never guesses one.

```sh
build/tools/miniraft-migrate-disk-state \
  --input-base /var/lib/miniraft/legacy/raft \
  --output-base /var/lib/miniraft/durable/raft \
  --node-id 2 \
  --voters 1,2,3 \
  --commit-index 128 \
  --dry-run
```

If the dry run succeeds and the supplied baselines have been independently
verified, repeat the command without `--dry-run`. The output base must be fresh
and distinct from the source base. The command checks the reopened durable image
before reporting success. It leaves the legacy entries, index and state files
untouched; point the node at the new durable base only after success.

The converter validates the source log and stored vote, preserves the term,
vote and every entry, records the supplied commit index and voter binding, and
publishes the new image through the durable backend. It refuses compacted
stores, configuration entries, unknown entry flags, malformed terms and commit
indices beyond the log. Configuration-bearing history needs a separately
verified migration procedure.

The dry run proves only that the files are internally consistent with the
values supplied. It cannot prove that the voter set is historically complete
or that the commit index matches the application's durable state. If either
fact is unknown, do not migrate. This utility does not upgrade the old files
in place or provide downgrade support.
