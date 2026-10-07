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
| IR versioning | The shape of each IR node, and every function, cast pair and shared leaf form the tree uses | `IrVersions.SUPPORTED` + per-node `versionEntries()` | `RawMaterializationDefinition.irVersions` |
| Connector id versioning | The serialization of each connector's table/column identity | `ConnectorIdVersion` returned by `ConnectorTableId`/`ConnectorColumnId` | `RawMaterializationDefinition.catalogIrVersions` |

The IR tree is engine-owned and the same for all installs; connector ids are connector-owned and
vary per catalog. They are versioned separately because they evolve independently and are checked
against different sources of truth.

## IR versioning (`io.starburst.materialization.ir.IrVersions`)

The computation plan is a tree of `IrNode`s: sealed `Operation` nodes (currently `Output`,
`TableScan` and `Aggregation`) and sealed `Expression` nodes. `Aggregation` holds `Expression`s as its
arguments; `Filter` and `Project` will hold them too. Each node type declares:

- a stable `name()` — the JSON `@type` discriminator (e.g. `"TableScan"`, `"Call"`), never renamed;
- an `int version()` — bumped on every backward-incompatible change to that node's serialized form;
- `children()` — the IR nodes directly under it; and
- `versionEntries()` — the version-map entries the node itself stamps (see below).

**Gate invariant.** A stored definition is readable iff every name it is stamped with is present in
`IrVersions.SUPPORTED` at the same version, so every wire-format decision the decoder makes must have a
name: if the decoder branches on it, it must be stamped.

**On write** (`VersionAwareMaterializationMetastore.createOrReplace`) `IrVersions.collect` walks
`children()` from the root and stores the union of every node's `versionEntries()` in
`RawMaterializationDefinition.irVersions`.

**On read** (`isIrCompatible`) a stored entry is compatible iff **every** `(name, version)` in
`irVersions` matches `IrVersions.SUPPORTED` exactly:

- a name not present in `SUPPORTED` → incompatible (the reader doesn't know that node type, function,
  cast pair or leaf form), and
- a version mismatch → incompatible (the reader knows the name but not this format or meaning).

Incompatible entries are skipped — logged at debug and counted in `skippedCount` — never decoded. A
downlevel reader facing an uplevel writer sees a name it does not know and skips the row; it never
misreads it, which lets the format roll out across a mixed-version fleet sharing one metastore. The
stamping is per document, so an engine lacking one function skips the whole MV, which is correct: the
MV's data reflects the whole defining query.

### Namespaces

The wire is a flat `Map<String, Integer>`; namespaces keep it collision-free.

| Namespace | Versions | Example |
| --- | --- | --- |
| *(bare)* | `Operation` node type — **grandfathered, never rename** | `"TableScan": 1` |
| `expr:<Name>` | `Expression` node type | `"expr:Call": 1` |
| `fn:<catalog>.<schema>.<function>` | one `SupportedFunctions` or `SupportedAggregateFunctions` entry's semantics | `"fn:system.builtin.$operator$equal": 1` |
| `cast:<from>-><to>` | one `SupportedCasts` pair's conversion semantics, by base type name | `"cast:varchar->varchar": 1` |
| `leaf:<Name>` | a shared leaf serialized form, stamped by `Expression` nodes and by `Aggregation` | `"leaf:Symbol": 1`, `"leaf:Type": 1`, `"leaf:BoundSignature": 1` |

An `Operation` stamps its bare name. `Aggregation` also stamps `leaf:Symbol`, `leaf:BoundSignature` when
it has an aggregate call, and the `fn:` key of each aggregate function it persists. An `Output(TableScan)`
tree is therefore stamped exactly `{Output, TableScan}`, so plain MVs stay readable by engines that predate
expressions. An `Expression` stamps `expr:<Name>`;
`Reference` adds `leaf:Symbol`, `Cast` adds `leaf:Type` and its `cast:` key, `Call` adds
`leaf:BoundSignature` and its `fn:` key.

`fn:` keys are `"fn:" + CatalogSchemaFunctionName.toString()`. `SupportedFunctions` spells names as the
engine does (`$operator$EQUAL`), but `CatalogSchemaFunctionName` and `SchemaFunctionName` lowercase
every part on construction, so keys are lowercase with the builtin catalog spelled `system`, exactly as
`BoundSignature.getName()` returns them: `fn:system.builtin.$operator$equal`, never `$operator$EQUAL`. A
disagreement between what `SUPPORTED` declares and what `collect` stamps would make every `Call`-bearing
row incompatible.

### When to bump what

| Change | Action | Effect on stored MVs |
| --- | --- | --- |
| New node type | new name at 1 | none invalidated |
| Change an existing node's payload | bump **that** name | only MVs using it |
| Add a function to `SupportedFunctions` or `SupportedAggregateFunctions` | new `fn:` at 1 | none |
| A function's semantics change | bump that `fn:` | only MVs using it |
| Remove a function | drop the entry | its MVs become incompatible |
| Add a cast pair to `SupportedCasts` | new `cast:` at 1 | none |
| A cast pair's conversion semantics change | bump that `cast:` | only MVs persisting that cast |
| Remove a cast pair | drop the entry | its MVs become incompatible |
| Change `Symbol`'s serialized form | bump `leaf:Symbol` **and** `Output` / `TableScan` (both embed `Symbol`s) | expression and aggregation trees via the leaf key; every MV via the bare names |
| Change `BoundSignature`'s serialized form | bump `leaf:BoundSignature` | only MVs containing a `Call` or an aggregate call |
| Change the `Type` string form | bump `leaf:Type`, `leaf:Symbol`, `leaf:BoundSignature` **and** `Output` / `TableScan` | effectively everything — `Type` strings are pervasive |
| Add `Filter` / `Project` / `Join` | new bare names at 1, plus the leaf keys it embeds (as `Aggregation` does) | none |
| **Rename any existing name** | **never** | silently invalidates stored definitions |

### Shared leaf types: `Symbol`, `Type` and `BoundSignature`

Not every serialized value is an IR node. `Symbol` (a `name` plus an `io.trino.spi.type.Type`) is a
leaf carried by `Output.outputs`, `TableScan.assignments` values, `Aggregation` (grouping keys, outputs,
filters, masks, sort items and group id) and `Reference`; `Type` is also persisted directly by `Cast`;
`BoundSignature` (trino-spi) is persisted by `Call` and by `Aggregation`'s aggregate calls. Their JSON
shapes are pinned by golden fixtures (`TestIrWireFormat`).

The `leaf:` keys deliberately do **not** cover `Output` and `TableScan`: they predate the leaf keys, and
stamping a new key on every existing tree would make every newly written plain MV unreadable to every
already-deployed engine. So a `Symbol` or `Type` format change still bumps `Output` and `TableScan` by
hand, in addition to the leaf keys — see the compound rows in the table above. An `Operation` added after
the leaf keys, such as `Aggregation`, stamps the leaf keys it embeds instead: every reader that knows it
knows them.

## Aggregation

`Aggregation` mirrors the engine's `AggregationNode`: grouping sets (keys, set count, global sets), a
`Step`, an optional group id, and one call per aggregate, each holding a `BoundSignature`, `Reference`
arguments, `distinct`, and an optional filter, mask and ordering. Its outputs are the grouping keys followed
by the aggregation outputs. The constructor enforces the engine's own invariants and no more: the function is
in `SupportedAggregateFunctions`, arity matches the signature for the step, ORDER BY appears only at
`SINGLE`, the group id is a grouping key, and the grouping sets are well formed. Argument and output types
are not checked against the signature, as the engine does not check them.

## Expressions

`Expression` mirrors the supported subset of the engine's `io.trino.sql.ir.Expression` node for node,
so translating either way is a straight recursion and a matcher can hand a stored predicate back to
engine machinery such as `DomainTranslator`. Both sides come out of the same optimizer prefix, so a
shape the stored side cannot express is a missed substitution, not a normalization opportunity.

| Node | Fields | `type()` | Validation |
| --- | --- | --- | --- |
| `Reference` | `Symbol symbol` | `symbol.type()` | — |
| `Call` | `BoundSignature function`, `List<Expression> arguments` | `function.getReturnType()` | name in `SupportedFunctions`; arity and each argument type match the signature |
| `Cast` | `Expression expression`, `Type type` | `type` | `(source base, target base)` in `SupportedCasts` |
| `FieldReference` | `Expression base`, `int field` | the field's type | `base` is a `RowType`; `field` in bounds |
| `Logical` | `Operator operator` (`AND` / `OR`), `List<Expression> terms` | `boolean` | at least two terms, all `boolean` |
| `In` | `Expression value`, `List<Expression> valueList` | `boolean` | non-empty; every item's type equals `value`'s |
| `IsNull` | `Expression value` | `boolean` | — |

`Constant` is the next node to land, with its value codec and the JSON-path functions it enables.
Everything else is deliberately absent:

- **No `Comparison` or `Not` node.** The engine has none: comparisons and `NOT` are `Call`s
  (`$operator$EQUAL`, `$operator$LESS_THAN`, `$not`, … — engine spellings; their `fn:` keys are
  lowercase). `a > b` is already lowered to `b < a` and `a <> b` to `$not($operator$EQUAL(a, b))`.
- **`canSplit` is not persisted on `Logical`.** It is an optimizer hint: two identical predicates can
  differ in it. A matcher must ignore it on the query side.
- **The cast kind is not persisted on `Cast`.** The engine's `CONVERT` / `REINTERPRET` is a function of
  the source and target types alone (`TypeCoercion.isTypeOnlyCoercion`, which the plan's
  `TypeValidator` enforces), so storing it would only let it contradict them. Code that rebuilds an
  engine cast derives it with `IrExpressions.cast`.
- **`Case`, `Coalesce`, `Row`, `Array` — deferred, not rejected.** A new node type costs nothing later
  (new name, old readers skip definitions using it), while a node added now is permanent surface area.
- **`Lambda`, `Bind`, `Let`, `Match` — excluded.** Higher-order or optimizer-internal. `Match` is the
  lowering of `CASE x WHEN v THEN r`; `Let` wraps `BETWEEN` over a non-trivial operand (over a bare
  column it is a plain `Logical(AND, …)`).
- **Aggregate and window functions are not expressions**, here or in the engine. The `Aggregation`
  `Operation` holds scalar `Expression`s as arguments; like the engine's, they are symbol references, and
  lambdas are not persisted yet.

### Symbol names are bindings, not identities

Persisted symbol names are engine-generated (`orderkey_1`) and are **not** stable across engine
versions or plan shapes. They are meaningful only as bindings within one persisted tree. A matcher must
resolve a `Reference` back to whichever node in the same tree bound that symbol — a `TableScan`
assignment, keyed by its `ConnectorColumnId`, or an `Aggregation` aggregation output. It must **never**
compare a symbol name against a query plan; that produces wrong results. Constructors check that every
symbol name is bound once per tree and that every symbol a node uses, `Reference`s included, is an
output of its source.

### Matching contract

- Normalization is a matcher concern, never baked into the persisted format: persist faithfully and, if
  anything is normalized, normalize both sides at match time in trino-main.
- Ignore `canSplit` and the cast kind on the query side.
- The query-to-MV symbol mapping must be accumulated check-and-merge (same key → same value is a match;
  a conflict is no match).
- Representability is not rewrite safety. The registries only answer whether an expression can be
  persisted with a meaning that is stable across sessions and engine versions. Whether a match licenses
  replacing a query subtree (for example, a predicate on the null-supplying side of an outer join) is
  the matcher's to prove.

### Index key invariant

The index bucket key (`MaterializationIndex.computeHash`) is derived from the **set of leaf `TableId`s**
in the tree and from nothing else — not from expressions, not from operator shape. Today the set has
one element and the key is that element's hash. Do not fold predicates into `ComputationHash` and do
not add `hash()` to expression nodes: a query cannot know an MV's predicate before matching it, so a
predicate in the key makes candidates unfindable and rules out subsumption matching. When `Filter` /
`Project` land, `computeHash` must descend past them to the leaves.

## Safety model

**Fail closed.** An expression that cannot be represented must lead to *not indexing the MV*, never to
indexing a tree whose meaning is uncertain. Constructors reject invalid trees, so an invalid tree cannot
be built or deserialized; `SupportedFunctions.contains`, `SupportedAggregateFunctions.contains` and
`SupportedCasts.contains` are side-effect-free predicates a producer consults to bail out first. The
registries report "unsupported" as `false`, never by throwing. Validation messages name the node, the rule and the offending function or
cast pair, never a symbol name or a row field name.

With candidate validation on (config `materialized-view-substitution.candidate-validation.enabled`,
default `true`, overridden per session by `materialized_view_substitution_candidate_validation_enabled`),
an MV with `substitution_enabled = true` whose definition cannot be represented fails
`CREATE MATERIALIZED VIEW`, the `ALTER … SET PROPERTIES` that enables substitution, and `REFRESH`, with
a generic message; with it off, the MV is silently left unindexed. An MV without `substitution_enabled`
is never validated.

**The registries are the contract and the only place to allow a function or cast pair.**

- `SupportedFunctions` — an allowlist of scalar functions. Each entry asserts the function is
  deterministic **and** session-independent. Determinism alone is not enough: the
  `CAST(timestamp AS timestamp with time zone)` operator is deterministic yet reads the session time
  zone, and an MV's storage table holds values computed under the refresh session. Session dependence in
  a query merely fails a match; in an MV definition it returns wrong rows.
- `SupportedAggregateFunctions` — the same allowlist for the aggregate functions an `Aggregation` may
  persist.
- `SupportedCasts` — an allowlist of `(source base type, target base type)` pairs, for the same reason.
  Two families are **permanently excluded** (`TestExpressionValidation` asserts their absence):
  - casts where exactly one side carries a time zone (`timestamp with time zone`,
    `time with time zone`), which the session time zone resolves. Comparing two tz-typed values is
    fine; converting across the boundary is not. Cost: a predicate mixing a `timestamp` column with a
    `timestamp with time zone` column is not indexable.
  - casts between `char` and `varchar`, in either direction, whose implementation and coercion
    direction `legacy_varchar_to_char_coercion` selects per session. Cost: an MV comparing a `char`
    column to a `varchar` column is not indexable.

  Stamping the creating session's time zone and char/varchar coercion mode into the definition would
  lift both exclusions; it needs a new `RawMaterializationDefinition` field.

The persisted format needs no canonical byte form, since nothing hashes or compares stored bytes:
validating constructors and [required primitives](#primitive-components-are-required) make a malformed
document unreadable, and the metastore is a trusted store. Tamper evidence, if ever added, is a
signature over the stored bytes.

### Checklist: adding a node, function or cast pair

- **Node:** a record in `io.starburst.materialization.ir` with static `NAME` / `VERSION`, a validating
  constructor, `children()` and, if it persists a leaf form, function or cast pair, a `versionEntries()`
  override; add it to its interface's `permits` and `@JsonSubTypes`, to `IrVersions`, and a golden
  fixture to `TestIrWireFormat`. `TestIrExhaustiveness` fails until all of those are in place.
- **Function:** measure first — each entry is permanent. Add a scalar function to `SupportedFunctions`,
  or an aggregate function to `SupportedAggregateFunctions`, at version 1 only if it is deterministic and
  session-independent, and pin its name in trino-main's `TestSupportedNames`.
- **Cast pair:** add it to `SupportedCasts` at version 1, never in an excluded family, and pin it in
  trino-main's `TestSupportedNames`.

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
2. `computationPlanRootJson` deserializes with the installed connectors and types, and every node's
   constructor validation passes; and
3. the `catalogIrVersions` re-collected from the deserialized tree equal the stored ones.

If any check fails the materialization is skipped for substitution — logged at debug and counted in
`skippedCount` — and never partially decoded. The IR JSON is never logged: it can be large and
carries customer schema details.
