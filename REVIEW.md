# Review instructions

## Summary shape

Open the review summary with a one-line tally: "1 important, 2 nits" or
"No blocking issues." Lead with the tally before any detail.

## What Important means here

Cork is the shared Trino engine fork powering both Galaxy and SEP. A bug here
ships to every customer on both products. Reserve Important for findings that
would cause:

- Incorrect query results, wrong type coercion, or silent data loss
- Security vulnerabilities: SQL injection via connector pushdown, authentication
  or authorization bypass, credential exposure in logs or error messages,
  insecure deserialization, SSRF via connector HTTP calls
- Engine crashes, OOMKills, or unbounded memory allocation under production
  query patterns (missing limits on buffers, unbounded collections)
- Resource leaks: unclosed JDBC connections, HTTP clients, input streams, or
  executors — especially in connector code that runs per-query
- Backward-incompatible SPI or public API changes that would break Galaxy or
  SEP integration layers without coordinated PRs
- Concurrency bugs: race conditions, deadlocks, or unsafe publication of
  shared mutable state in engine-hot paths
- Regressions in predicate pushdown, partition pruning, or dynamic filtering
  that would silently degrade query performance at scale

Style, naming, Javadoc, and refactoring suggestions are Nit at most.

## Cap the nits

Report at most 3 Nits per review. Cork PRs tend to be engine-critical and
authors need signal, not noise. If you found more nits, say "plus N similar
items" in the summary. If everything you found is a Nit, lead the summary with
"No blocking issues."

## Do not report

- Anything CI already enforces: Checkstyle, Error Prone, SpotBugs, test
  failures, Maven enforcer rules
- Generated code: Antlr parser output, Protobuf stubs, Thrift stubs,
  annotation processor output
- OSS rebase/merge commits from upstream Trino — these are bulk imports and
  should not be reviewed line-by-line
- `pom.xml` version bumps covered by automated tooling unless they introduce
  a known CVE
- IDE configuration, lockfiles, `.gitignore` changes
- Test code that intentionally uses anti-patterns to verify error handling
- Changes under `trino-web-ui/` that are purely cosmetic (CSS, layout)

## Always check

- New or modified connectors handle `null` values correctly across all data
  types — null handling bugs are the most common connector defect class
- Predicate pushdown changes preserve correctness: a pushed-down predicate must
  never filter out rows that the engine would have kept, and must never include
  rows the engine would have filtered
- Memory accounting is correct: new buffers and operator state are tracked by
  the memory context so the engine can enforce memory limits
- Thread safety of any shared state in `Plugin`, `ConnectorFactory`, or
  singleton-scoped objects
- CVE fixes (dependency upgrades) actually resolve the vulnerability — verify
  the fixed version matches the advisory, not just that a bump occurred

## Verification bar

Before claiming a concurrency or resource leak bug, trace the full lifecycle
(allocation → usage → close/release) through the code. Do not post based on a
naming heuristic. Behavior claims must cite `file:line`.

## Re-review convergence

After the first review on a PR, suppress new Nits and post Important findings
only.
