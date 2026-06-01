# starburst-materialization-metastore-api

API and serialization format for the materialization metastore — the store that persists the
information needed to substitute a query subtree with a scan over a materialized view (MV).

A persisted materialization (`RawMaterializationDefinition`) holds the serialized IR computation
plan (`computationPlanRootJson`) plus the version stamps that describe the exact format that JSON
was written in. Because a definition written by one engine/connector build may be read by a
different one, the metastore can hold entries that an older or newer reader does not understand.
The reader must therefore be able to tell whether a stored entry is in a format it can decode, and
**skip** the ones it cannot rather than fail or, worse, decode them wrong. Two independent
versioning schemes provide that signal.

## Two versioning schemes

| Scheme | What it versions | Where the version lives | Stored in |
| --- | --- | --- | --- |
| IR node versioning | The shape of each `Operation` IR node (the computation-plan tree) | `Operation.SUPPORTED` + per-node `version()` | `RawMaterializationDefinition.irVersions` |
| Connector id versioning | The serialization of each connector's table/column identity | `ConnectorIdVersion` returned by `ConnectorTableId`/`ConnectorColumnId` | `RawMaterializationDefinition.catalogIrVersions` |

The IR tree is engine-owned and the same for all installs; connector ids are connector-owned and
vary per catalog. They are versioned separately because they evolve independently and are checked
against different sources of truth.

## IR node versioning (`io.starburst.materialization.ir.Operation`)

The computation plan is a tree of sealed `Operation` nodes (currently `Output` and `TableScan`).
Each node type declares:

- a stable `name()` — the JSON `@type` discriminator (e.g. `"Output"`, `"TableScan"`), and
- an `int version()` — bumped on every backward-incompatible change to that node's serialized form.

`Operation.SUPPORTED` is the engine-side declaration of the `name -> version` pairs the **current
build** understands:

```java
Map<String, Integer> SUPPORTED = ImmutableMap.of(
        Output.NAME, Output.VERSION,
        TableScan.NAME, TableScan.VERSION);
```

**On write** (`VersionAwareMaterializationMetastore.createOrReplace`) the IR tree is walked and every
node's `(name, version)` is collected into `RawMaterializationDefinition.irVersions`, stamped
alongside the serialized JSON.

**On read** (`isCompatible`) a stored entry is compatible iff **every** `(name, version)` in
`irVersions` matches `SUPPORTED` exactly:

- a name not present in `SUPPORTED` → incompatible (reader doesn't know that node type), and
- a version mismatch → incompatible (reader knows the type but not this format).

Incompatible entries are skipped (logged at INFO), never decoded.

### When to bump an IR node version

Bump the node's `VERSION` constant whenever you change its serialized form in a way an older reader
could not decode correctly — adding/removing/renaming a JSON property, changing a property's type
or meaning, etc. Adding a brand-new `Operation` subtype does not require bumping existing nodes; it
just adds a new entry to `SUPPORTED` (and old readers will correctly skip definitions that use it,
since the new name is absent from their `SUPPORTED`).

### Shared leaf types: `Symbol` and `Type`

Not every serialized value is an `Operation`. `io.starburst.materialization.ir.Symbol` is a leaf
carried inside IR nodes (`Output.outputs`, `TableScan.assignments` values) and is **not** versioned
on its own. A `Symbol` holds a `name` plus a `io.trino.spi.type.Type`, so its serialized form
depends on stable `Type` serialization.

Because `Symbol` has no version of its own, a change to `Symbol`'s serialized form — or to the
underlying `Type` serialization — is a backward-incompatible change to **every IR node that uses
`Symbol`**. When that happens you must bump the `VERSION` of all such nodes (today `Output` and
`TableScan`) so old definitions are skipped rather than misread.

## Connector id versioning (`ConnectorIdVersion`)

A `TableScan` references the source table by its connector-specific identity: a `ConnectorTableId`
and, per assigned column, a `ConnectorColumnId`. These types live in
`io.trino.spi.connector.substitution` and are implemented by each connector (e.g. `JdbcTableId`,
`IcebergColumnId`). Their serialized form is owned by the connector, not the engine, so it is
versioned with `ConnectorIdVersion`:

```java
public record ConnectorIdVersion(String key, int id) { ... }
```

- `key` names the identity type — typically the implementation's class simple name (e.g.
  `"JdbcTableId"`). It distinguishes identity types, which matters for connectors that wrap other
  connectors (e.g. ObjectStore, Warp Speed) and can emit several identity implementations.
- `id` is the version number, bumped on every backward-incompatible change to that identity's
  serialized form.

Each implementation returns its version from `version()`:

```java
public final class JdbcTableId implements ConnectorTableId {
    public static final ConnectorIdVersion VERSION = new ConnectorIdVersion("JdbcTableId", 1);
    @Override public ConnectorIdVersion version() { return VERSION; }
    ...
}
```

A connector advertises the full set of formats it can currently read through
`ConnectorSubstitutionMetadata.tableIdVersions()` and `columnIdVersions()`. These are usually
singletons; a wrapping connector returns the **union** of the wrapped connectors' versions.

**On write** the versions of the table id and column ids seen under each catalog are collected into
`RawMaterializationDefinition.catalogIrVersions` (a `CatalogName -> ConnectorIdVersions` map, where
`ConnectorIdVersions` holds the set of table-id and column-id versions used).

**On read** (`isCompatible`) for each catalog in the stored definition:

- the catalog must still exist (`CatalogManager`), otherwise the entry is skipped, and
- the connector's currently supported `tableIdVersions()` / `columnIdVersions()` must **contain
  all** the stored versions; otherwise the entry is skipped.

This lets a connector keep reading old definitions after a format change by continuing to advertise
the old `ConnectorIdVersion` in its supported set (in addition to the new one), and lets the engine
safely skip definitions written by a newer connector build it doesn't yet understand.

### When to bump a connector id version

Bump the `id` of the relevant `ConnectorIdVersion` constant whenever the connector's
`ConnectorTableId` / `ConnectorColumnId` serialized fields change incompatibly.

## Compatibility check summary

A stored `RawMaterializationDefinition` is usable by the current build iff **all** of the following
hold (see `VersionAwareMaterializationMetastore.isCompatible`):

1. every IR `(name, version)` in `irVersions` matches `Operation.SUPPORTED` exactly;
2. every catalog in `catalogIrVersions` still exists; and
3. for each such catalog, the connector's `tableIdVersions()` and `columnIdVersions()` contain all
   the stored table-id and column-id versions.

If any check fails the materialization is skipped for substitution; it is never partially decoded.
