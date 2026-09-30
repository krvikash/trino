# Cork Repository Context

## What is Cork?

Cork (COmmon foRK) is Starburst's continuous fork of Trino OSS. It serves as the engine layer for:
- **SEP (Starburst Enterprise Platform)** - Enterprise Trino distribution
- **Galaxy** - Starburst's cloud-native query engine

Cork maintains compatibility with upstream Trino while adding Starburst-specific features that are not open-sourced.

## Change Classification

Every Cork PR falls into one of two categories (see "Fork Admission" section in PR template):

### 1. Upstream Cherry-Picks
- Changes backported from Trino OSS
- Usually reference upstream Trino PR numbers
- May require conflict resolution with Starburst-specific code
- **Notable if**: The upstream change affects public APIs, configuration, or behavior

### 2. Starburst-Specific Features
- Tracked in [trino-fork-log](https://github.com/starburstdata/trino-fork-log)
- Features not contributed to upstream Trino
- **Always check** the linked fork-log issue for customer context

## High-Impact Areas

### Critical Directories (Almost Always Notable)
- `core/trino-spi/` - Service Provider Interface; **but evaluate carefully** — many SPI changes don't affect SPI consumers (e.g. Pages/Blocks internals are implemented in SPI but are not part of the public contract). Starburst does not ship SPI jars, so only changes that affect connector/plugin compatibility or user-visible behavior are notable.
- `core/trino-main/src/main/java/io/trino/server/` - Server configuration
- `core/trino-main/src/main/java/io/trino/execution/` - Query execution engine

### High-Impact Directories
- `plugin/trino-*/` - Upstream Trino connectors (100+ modules)
- `plugin/starburst-*/` - SEP-enhanced connectors (Hive, Iceberg, Delta Lake)
- `plugin/sep-*/` - SEP-specific connectors (OpenAPI, ObjectStore)
- `starburst-buffer-service/` - SEP-specific service
- `lib/trino-cache/` - Caching layer (performance impact)

### Configuration Patterns (Check Carefully)
- `*Config.java` with `@Config` annotations - configuration property changes
- `*SessionProperties.java` - session-level configuration
- `*.g4` files - ANTLR grammar (notable only for core grammar changes; connector-specific grammars, e.g. in `plugin/trino-delta-lake/`, are less impactful; minor fixes like typos are usually not notable)
- `*Resource.java` - REST API endpoints

## Notable Change Indicators for Cork

### Always Notable
1. Changes to `io.trino.spi.*` packages that affect **connector/plugin compatibility** (e.g. interface additions, removals, or signature changes) — internal SPI classes like Pages and Blocks are not notable to users, and Starburst does not ship SPI jars
2. Configuration property additions, removals, or renames
3. Default value changes for existing configuration
4. Feature flag changes (enabling/disabling features by default)
5. Changes to query planning or execution behavior
6. Security-related changes (authentication, authorization)
7. CVE fixes and security patches

### Likely Notable
1. Connector plugin changes (especially SEP-specific connectors)
2. Changes to table formats (Hive, Iceberg, Delta Lake)
3. Catalog management changes
4. Materialized view behavior
5. Session property changes
6. REST API changes

### Usually Not Notable
1. Test-only changes (unless they reveal behavioral expectations)
2. Documentation updates
3. CI/CD pipeline changes
4. Code refactoring without behavioral changes
5. Dependency version updates (unless CVE-related)

## Release Branch Backports

PRs targeting a `release-**` branch are **backports**. The default classification for backports is **notable**. The bar for classifying a backport as not-notable is higher than for master PRs — only exclude if the change is demonstrably customer-invisible, such as:

- Test-only changes (new tests, fixes to flaky tests, test utility tweaks)
- CI / GitHub Actions / build-script changes with no runtime impact
- Pure refactors with zero behavioral impact (no API, config, or execution changes)

When in doubt, classify as notable. Anything worth the effort of a backport almost always matters for a customer who upgrades to that patch version.

## Functionality Not Yet Publicly Released

This rule is an explicit exception to the `release-**` backport default above and takes precedence
when both rules apply.

A bug fix (or any other change) to functionality that was added **after the most recent public
release** and has not yet shipped in *any* released version should be classified **NOT NOTABLE**,
regardless of the underlying bug's severity — including data-correctness bugs, crashes, or
security-shaped issues. Customers were never exposed to the buggy behavior on a released version,
so there is nothing to describe in that release's notes.

**Example:** GPU support for window functions is added to master shortly after a release cuts. A
couple of days later, before the next release, a bug in that new window-function GPU path is
found and fixed. That fix is NOT NOTABLE — the affected functionality never reached a customer,
because it was never in a released version.

**How to determine "not yet publicly released":**
- **Cork's own `release-**` branches/tags are not a reliable signal for this — do not use git-tag/
  branch reachability against them.** They are stale artifacts of an old process, not a live record
  of what shipped in SEP or Galaxy: e.g. `release-435-e`'s tip commit is literally titled "Move
  release-435-e to starburst-enterprise-trino repo" (2024-04-03), and `release-434-e`, `release-438-e`,
  `release-439-e`, `release-443-e` all stopped advancing in early 2024, years behind current cork
  development. A commit being unreachable from these refs proves nothing about whether the
  corresponding feature shipped. (Reliably tracking this would mean, for SEP, mapping
  starburst-enterprise-trino version tags to starburst-enterprise release branches, and for Galaxy,
  mapping galaxy-trino to stargate branches — cross-repo checks with no unified tooling today; treat
  this as an open problem, not something to approximate with cork's local refs.)
- Primary, practical signals — use these instead:
  - The associated JIRA ticket/epic's "Fix Version(s)"/"released in version X" field (check its full
    fix-version history, not just the latest entry), or release notes already published for the
    feature.
  - The CHANGELOG/release notes for the feature by name, in any released version.
- If neither signal is available or conclusive from the PR context, use judgment from what's given
  (linked JIRA, file history, PR timestamps relative to the last known release) rather than
  requiring exact tooling.
- **When it's genuinely unclear whether the functionality has shipped, default to the normal
  classification rules (don't assume unreleased).** Given that cork has no reliable, unified way to
  answer this across SEP and Galaxy, expect this fallback to apply often — treat it as the common
  outcome, not a last resort, whenever JIRA fix-versions and release notes don't settle the question.

**Boundary case — once functionality HAS shipped, this exclusion no longer applies.** If the
affected functionality shipped in *any* prior public release — even if it is config-gated,
experimental, or limited-GA (for example, GPU acceleration, which is publicly documented at
docs.starburst.io and self-service via `gpu-execution=true`) — a subsequent bug fix to it must be
evaluated under the normal classification rules above, not automatically excluded. This is the
opposite of treating a released-but-gated feature as exempt: "not yet publicly released" means the
feature has never appeared in a released version at all, not that it is hidden, optional, or
config-gated within a version that *has* released.

## Tier 2 Tags for Cork

Use these specific tags when relevant:
- `trino-spi` - SPI changes affecting plugin compatibility
- `query-engine` - Query planning/execution changes
- `catalog-management` - Catalog-related features
- `materialized-views` - MV feature changes
- `hive-connector` - Hive connector changes
- `iceberg-connector` - Iceberg connector changes
- `delta-lake-connector` - Delta Lake connector changes
- `openapi-connector` - OpenAPI connector (SEP-specific)
- `objectstore-connector` - ObjectStore connector (SEP-specific)
- `upstream-backport` - Cherry-pick from Trino OSS
- `fork-specific` - Starburst-only feature

## External Context

Cork PRs often reference:
- **JIRA tickets** - Engineering tasks and customer requests
- **Trino PRs** - Upstream changes being backported
- **trino-fork-log issues** - Proprietary feature tracking

When external context is available, use it to understand:
- Customer impact and urgency
- Original design decisions
- Related changes in other repositories
