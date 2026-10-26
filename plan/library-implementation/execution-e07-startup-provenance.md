# E07 bounded startup provenance repair plan — 2026-10-24

**Independently approved before code.** [Plan review](reviews/e07-startup-plan-review.md). S5 is accepted.
The first S6 run passes all 247 pytest cases, but correctly fails the runner
because three actual children lack startup receipts. [Rejected proof](evidence/e07-s6-rejected.json)
and all raw receipts remain retained. S6 is not accepted.

The missing children belong to `public-gap` (synthetic clock expires immediately
after starting a real worker) and `public-startup` (the second real Process.start
is deliberately followed by failure). Their parents terminate those children
before the existing site-hook finishes auditing/importing startup tools. These
are original material E04 regressions; keep their code, bounds and assertions.
Do not treat an absent receipt as a successful origin check.

Add bounded **E07.F1.S11**, half a day, between S5 and S6. Preserve S1's supported
launchers and fail-closed final checks. After actual child creation, record its
PID and OS creation identity, then wait at most three seconds for its fresh
matching process receipt before returning the creation API to the test caller.
Match a pre-creation monotonic anchor, PID/creation identity, target prefix/site;
the child receipt remains emitted only after the existing full payload audit.
Use only the owned append-only provenance log; short-lived children may already
have exited after writing a valid startup receipt. Missing, stale or foreign
receipts fail, with existing owned cleanup retained. No application code changes,
deadline expansion, new service claim, inherited-provenance waiver or editable
rescue. Final bare/image controls still use their original uninstrumented
application timing.

Apply the synchronization to the already supported Popen, POSIX spawn, fork and
multiprocessing spawn paths. Preserve post-exec freshness checks; a pre-exec
fork receipt still cannot satisfy an exec. Avoid importing application modules
inside the guard. Account explicitly for instrumentation overhead in evidence.

Capture the real monotonic-nanosecond clock and sleep functions before tests can
patch shared modules. Poll complete append records and tolerate only a currently
incomplete final line; malformed complete records fail. A receipt wait failure
must leave a durable violation even if application code catches the raised
startup exception. Ownership is recorded before waiting, so cleanup retains the
actual child identity on every failure. For an already-exited child whose OS
identity cannot still be queried, require its fresh audited receipt to match the
pre-creation anchor, actual parent PID, target prefix/site and child PID, and
retain that receipt's OS creation identity; never accept PID-only evidence.

Before acceptance, prove real spawn/Popen/POSIX/fork children can be terminated
immediately after creation while retaining valid startup identities; absent
startup hooks still fail. Re-run the full accepted S1 positive/negative matrix
(including detached/timeout/ownership and missing metadata/module/resource
controls), mechanism tests, lint and whitespace. Then rerun all original public
tests with exact 250 full/247 selected IDs, original assertions, fault supervisor
and all child receipts. Independently review S11 after implementation, then
independently review S6 as originally required. Preserve all rejected proof.

S6–S10 and F2 remain in their approved order after this inserted repair. All
final gates and user prohibitions remain; E08–E12/publication remain unexecuted.
