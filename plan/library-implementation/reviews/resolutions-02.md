# Author resolution of round-02 R08

Submitted for round-03 independent verification, 2026-10-15. This author
response is not approval; the original round-02 report remains preserved.

R08 is addressed in cancellation-contract.md, backlog E04.F2.S3 and its rendered
epic: process time starts once initial serialization/submission completes and
includes elapsed time between a separate start() and finish(); inline time
starts before the first source iteration so scan time counts. Both use one
absolute monotonic deadline across phases, with no gather/phase reset. Excluded
blocking work does not restart the elapsed clock when it returns. Cleanup has
a separately stated budget and is not a reset of the planning deadline.

Validation now explicitly requires ten individually returning inline records
that advance a fake clock; a short timeout must stop consumption before all ten
are read. Separate tests cover cumulative multi-phase budget, the gap between
process start/finish, a blocked real child, cooperative cancellation and owned
cleanup. D02 must approve the proposed contract before implementation.

The nonblocking E09.F1.S6 wording now says current development-candidate image
IDs; E11 alone freezes and attests release identities. The earlier round-01
draft arithmetic was corrected by the reviewer before final submission; no
outstanding count erratum is asserted.

The hierarchy remains 13 epics, 26 features, 53 stories, with all 48 checklist
IDs covered. Structural, inventory-path and Markdown-link checks pass. No
product source, services, publication or pushes were part of these revisions.
