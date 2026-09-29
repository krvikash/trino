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
| IR node versioning | The shape of each `Operation` IR node (the computation-plan tree) | `IrVersions.SUPPORTED` + per-node `versionEntries()` | `RawMaterializationDefinition.irVersions` |
| Connector id versioning | The serialization of each connector's table/column identity | `ConnectorIdVersion` returned by `ConnectorTableId`/`ConnectorColumnId` | `RawMaterializationDefinition.catalogIrVersions` |

The IR tree is engine-owned and the same for all installs; connector ids are connector-owned and
vary per catalog. They are versioned separately because they evolve independently and are checked
against different sources of truth.

## IR node versioning (`io.starburst.materialization.ir.IrVersions`)

The computation plan is a tree of `IrNode`s, currently sealed `Operation` nodes (`Output` and
`TableScan`). Each node type declares:

- a stable `name()` — the JSON `@type` discriminator (e.g. `"Output"`, `"TableScan"`), never renamed;
- an `int version()` — bumped on every backward-incompatible change to that node's serialized form;
- `children()` — the IR nodes directly under it; and
- `versionEntries()` — the version-map entries the node itself stamps. An `Operation` stamps its bare
  `name() -> version()`.

`IrVersions.SUPPORTED` is the engine-side declaration of the `name -> version` pairs the **current
build** understands:

```java
public static final Map<String, Integer> SUPPORTED = ImmutableMap.<String, Integer>builder()
        .putAll(OPERATION_VERSIONS)
        .buildOrThrow();
```

where `OPERATION_VERSIONS` holds `Output.NAME -> Output.VERSION` and `TableScan.NAME -> TableScan.VERSION`.

**On write** (`VersionAwareMaterializationMetastore.createOrReplace`) `IrVersions.collect` walks
`children()` from the root and stores the union of every node's `versionEntries()` in
`RawMaterializationDefinition.irVersions`, stamped alongside the serialized JSON.

**On read** (`isIrCompatible`) a stored entry is compatible iff **every** `(name, version)` in
`irVersions` matches `IrVersions.SUPPORTED` exactly:

- a name not present in `SUPPORTED` → incompatible (reader doesn't know that node type), and
- a version mismatch → incompatible (reader knows the type but not this format).

Incompatible entries are skipped (logged at debug and counted in `skippedCount`), never decoded.

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

### Primitive components are required

A primitive component (`int`, `boolean`, ...) must be present in the IR JSON, and not as `null`. Use
`Optional` or a boxed type for a field that may be absent. Every IR codec is built from
`IrJsonMapper`, which enforces this for every class in the document, connector identities included,
since Jackson otherwise reads a missing or null primitive as zero or `false`, which a validating
constructor cannot tell apart from a real value.

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

**On read** (`toTyped`) the definition is deserialized — a connector that was uninstalled, or whose
identity classes changed shape, fails here and the entry is skipped — and `catalogIrVersions` is
re-collected from the deserialized tree. Because an identity's version is a constant of the connector
code, the re-collected map is what the installed connectors produce today; the entry is skipped unless
it **equals** the stored map. Only the connector-id versions are compared this way: the IR name map is
checked by the gate above, which by design accepts a row stamped with a different set of supported
names.

### When to bump a connector id version

Bump the `id` of the relevant `ConnectorIdVersion` constant whenever the connector's
`ConnectorTableId` / `ConnectorColumnId` serialized fields change incompatibly.

Adding a primitive component is such a change: a row written before it has no value for it, and the
required-primitive rule rejects that row rather than reading it as zero or `false`. To add a field
without a bump, make it `Optional` or boxed.

## Compatibility check summary

A stored `RawMaterializationDefinition` is usable by the current build iff **all** of the following
hold (see `VersionAwareMaterializationMetastore.toTyped`):

1. every `(name, version)` in `irVersions` matches `IrVersions.SUPPORTED` exactly (`isIrCompatible`);
2. `computationPlanRootJson` deserializes with the installed connectors, and every node's constructor
   validation passes; and
3. the `catalogIrVersions` re-collected from the deserialized tree equal the stored ones.

If any check fails the materialization is skipped for substitution — logged at debug and counted in
`skippedCount` — and never partially decoded. The IR JSON is never logged: it can be large and
carries customer schema details.
