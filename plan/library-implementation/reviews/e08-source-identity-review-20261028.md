# Focused E08 source-identity publication review — 2026-10-28

**Disposition: accept this bounded fixture repair. No actionable finding remains. This is not acceptance of the installed public-contracts rerun, the 134-case process envelope, native Linux cells, S2 or E08.**

Reviewed the retained failed installed run, its exact executed fixture, the copied failed case and the atomic-publication diff in `tests/fixtures/e08_process_clients.py`. No maintained source was changed by this reviewer. Only this report and ignored local audit scripts/results were written. No SSH, Docker, service or private environment-file access occurred.

## Actual failure and input identity

The installed run at `/private/tmp/graph-loader-e08-matrix-macos-20261028-v1/` remains failed: **1 failed, 268 passed in 39.70 seconds**. The failing ID is the spawn/source-error/four-worker external-client test. Its client had already handled the intended source error and completed the preceding cleanup assertions; parsing the peer identity files then raised JSONDecodeError. The copied case contains 24 canonical identity files, including zero-byte `source-33.json` with SHA-256 `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`. Thus the retained bytes support interruption during the peer's direct identity write as the fixture failure mechanism.

The retained pre-repair fixture is byte-identical to the failed run's staged fixture, SHA-256 `4c6afc3bf126511c8ade54684d496d0784cbf8a17402aa51b3f957a55c46dbea`. The failed outer command receipt has exit 1, no timeout, no owned leftovers/cleanup errors, and its recorded log hash matches the retained log. The installed result is `status: failed`, and no `process-envelope` directory exists. This attempt supplies no envelope pass.

## Repair assessment

Each source now serializes the same identity fields, exclusively creates a source-order/PID-specific pending file in the destination directory, writes and closes it, then publishes the canonical `.json` with `os.replace`. Interruption before publication can leave an empty, partial or complete pending file, but cannot expose that unfinished file under the canonical identity name. The pending name is distinct for concurrently executing sources and workers; source orders remain unique in this client.

The observation parser still strictly decodes every canonical `source-*.json`. It separately retains and reports pending files with path, byte size and SHA-256 after production children have been checked dead. Successful operations additionally require no pending identity files. Expected source-failure observations can retain unfinished peer identities without pretending those files are committed source identities. No malformed committed receipt is silently skipped.

The diff preserves the original record data, error/cause/failure-marker checks, source-0 exit-37 activation proof, data/chunk/digest checks, caller-selected method checks, source count/worker checks and cleanup timing/resource assertions. The publication change is entirely in the external test fixture; it changes no application behavior or deadline.

## Independent controls

All controls use fresh owned output directories and a separate 30-second process-group supervisor:

- **Interrupted before replacement:** three peers close their complete 205-byte pending identities and then block before replacement. A source sentinel error triggers ordinary production cleanup. The client returns the expected source-error observation, retains and correctly hashes all three pending files, and parses only the one committed identity. No corresponding canonical peer identity exists.
- **Interrupted during writing:** three peers flush only the first byte (`{`) of their pending identities and then block. Ordinary source-error cleanup terminates them. The client preserves the source error and reports all three one-byte pending files with matching hashes; it never attempts to parse those pending bytes as committed JSON.
- **Malformed committed identity:** an audit-only source intentionally corrupts its already committed identity to zero bytes before reporting the source error. The unmodified parser raises JSONDecodeError, proving the repair does not tolerate malformed canonical identities.

The controlled source failure uses source order 63 so it executes in the first scan wave while other workers are held in publication. This changes only the ignored control's arrangement; the maintained fixture's ordinary source-0 behavior remains unchanged. An initial control mistakenly held every worker before source 0 was scheduled; its 30-second supervisor timeout and outputs are retained separately as an incomplete attempt, not a passing check.

All three corrected controls have zero supervisor exits, no timeout, owned leftovers or cleanup errors, and verified command-log hashes. An independent focused `tests/test_public_cancellation.py` run completed **33 passed in 8.49 seconds**, with one pytest cache warning because the isolated `/dev/null` configuration made its cache destination unwritable. No application warning was reported. The run used conftest exclusion and plugin autoload disabled. A guard installed before pytest blocked private root dotenv and external socket access; no blocked attempt occurred. This focused run includes normal spawn/inline success and source/serialization/exit failure cases. It does not replace the failed installed regression or establish installed equality.

Scripts, command receipts, raw logs, observations, the retained-proof audit and access audit are under `var/e08-source-identity-review-20261028/`. The reviewed repaired fixture is **10,304 bytes**, SHA-256 **`e18f49e23c0e6986a4af9787ed3ba4849e0938c8baf24d3c5f5bbd436114b627`**. This supersedes only the external-client fixture identity in the earlier focused implementation review. The frozen installed fixture inventory must be refreshed and the complete requested installed/matrix run retried with new output names; the original failed attempt must remain red.
