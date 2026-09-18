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

## Hidden / Experimental Feature Flags

Cork marks config and session properties that gate unreleased or internal-only features with
`@ConfigHidden` (see `.claude/rules/trino-config-properties.md` for the general property
conventions). A `@ConfigHidden` property is not documented or exposed to customers in any
supported configuration — it exists for internal use ahead of an "officialize" step, often
tracked by a `TODO (ENG-XXXX)` comment right next to the annotation (e.g. `gpu-execution` /
`gpu_execution_enabled`, gated by `@ConfigHidden` in `FeaturesConfig.java` and registered as
hidden in `SystemSessionProperties.java`, with `TODO (ENG-9839) officialize config toggles`).

**When a PR's entire customer-visible effect (a bug fix, behavior change, new error message,
etc.) is reachable only through a code path gated exclusively behind a property that is
`@ConfigHidden` and/or a session property registered as hidden, classify the PR as NOT
NOTABLE — regardless of the underlying bug's severity, including P0 data-correctness fixes.**
No customer running a default or supported configuration can reach the change, so it does not
belong in customer-facing release notes. This takes priority over the general Bug Fix
Classification Guide flowchart above, which has no branch for hidden-feature gating and will
otherwise mark a data-correctness fix or observable-behavior change as notable on its own.

This exclusion is narrower than the general "opt-in feature flag" signal: a **documented,
supported** opt-in feature (not `@ConfigHidden`) is still potentially notable, typically at
`impact.level: low` (see Impact Level Guidelines) — do not extend the hidden-feature exclusion
to config/session properties that are merely disabled by default but are otherwise documented
and supported.

If part of the change is reachable outside the hidden path — e.g. it also changes a default or
behavior on the CPU/non-experimental path, or removes the hidden gate itself — evaluate that
reachable part normally instead of applying this exclusion.

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
