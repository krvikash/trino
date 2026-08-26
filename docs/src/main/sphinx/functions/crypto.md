# Encryption functions

The encryption functions encrypt and decrypt individual values with AES. Keys are supplied as
function arguments, so the functions can read and write values that other systems encrypted, as
long as the key material is available to the query.

:::{warning}
The encryption functions are provided as a preview. The function signatures, argument order, and
ciphertext format may change in a future release.
:::

## Configuration

The encryption functions are not built in. They are provided by the `starburst_functions`
connector, so a [catalog properties file](catalog-properties) must register them before they can be
used.

This applies to a self-managed Trino cluster, including a self-hosted SEP deployment, where catalog
configuration is under your control.

Create a catalog properties file `etc/catalog/starburst.properties` that references the
`starburst_functions` connector and enables the encryption functions:

```properties
connector.name=starburst_functions
crypto.enabled=true
```

The functions are disabled by default. When `crypto.enabled` is not set, the `crypto` schema is not
listed and the functions do not resolve. No other configuration is required.

:::{note}
In Starburst Galaxy, the `starburst` catalog is provisioned automatically and its properties are not
user-editable, so the preceding steps do not apply. The encryption functions are exposed per account
by Starburst; contact your account team to have them enabled.
:::

The encryption functions are available with the `crypto` schema name. For the preceding example,
the functions use the `starburst.crypto` catalog and schema prefix:

```sql
SELECT starburst.crypto.aes_encrypt(to_utf8('secret'), X'000102030405060708090a0b0c0d0e0f');
```

To avoid needing to reference the functions with their fully qualified name, configure the
`sql.path` [SQL environment property](/admin/properties-sql-environment) in the
`config.properties` file to include the catalog and schema prefix:

```properties
sql.path=starburst.crypto
```

## Keys

Only key lengths defined by AES are accepted. A key that is not exactly 16, 24 or 32 bytes long is
rejected.

Anyone able to read the key material is able to decrypt, so protect the table or secret holding the
key accordingly. Use a column reference rather than a literal: a literal key is recorded in the
query text, and appears in `EXPLAIN` output, query history and event listeners.

## Modes

The supported modes are `GCM`, which is authenticated and the default, and `CBC`, which uses PKCS#7
padding and provides no authentication. Associated data is only supported for `GCM`. PKCS#7 padding
is named `PKCS5Padding` in the JDK, which is the same scheme for the 16 byte block size AES uses.

Because `CBC` provides no authentication, a successfully decrypted value is not evidence that it was
encrypted by a holder of the key. Where the source system stores a message authentication code next
to the ciphertext, verify it with {func}`hmac_sha256` over the initialization vector and ciphertext
before decrypting.

## Failures

Both functions raise an error when a value cannot be encrypted or decrypted, including when
authentication fails. Wrap a call in {func}`try` to obtain a `NULL` instead.

## Functions

:::{function} aes_encrypt(binary, key, mode, initialization_vector, associated_data) -> varbinary
Encrypts `binary` with AES under the given `key`, using `mode` if supplied and `GCM` otherwise. The
`associated_data` is authenticated but not encrypted, and must be supplied again to decrypt.

The trailing arguments are optional, so `aes_encrypt(binary, key)`,
`aes_encrypt(binary, key, mode)` and `aes_encrypt(binary, key, mode, initialization_vector)` are all
valid. Passing `NULL` for `initialization_vector` or `associated_data` is equivalent to omitting it.

The result always begins with the initialization vector, so that {func}`aes_decrypt` can read it
back without it being stored separately: 12 bytes for `GCM`, 16 bytes for `CBC`. `GCM` additionally
appends a 16 byte authentication tag. This is true whether the initialization vector was generated
randomly or supplied explicitly, so decrypt these values with the implicit form, omitting
`initialization_vector` on {func}`aes_decrypt`.

When `initialization_vector` is omitted or `NULL`, it is generated randomly, so the same input
yields a different result on every call. Equal inputs therefore do not produce equal results, and a
join condition, equality predicate, `GROUP BY` or `DISTINCT` over an encrypted column matches on the
encrypted bytes rather than on the underlying value. Grouping by ciphertext as an opaque identifier
is well defined; matching rows that share a plaintext value is not.

Supplying an initialization vector explicitly makes encryption deterministic. Prefer the random
form.

:::{warning}
Never reuse an initialization vector with the same key. In `GCM` mode this is catastrophic: two
values encrypted under the same key and initialization vector can be combined to recover both
plaintexts, and the authentication guarantee is lost, without the key ever being known. In `CBC`
mode it reveals which rows share a value. A constant or column-derived initialization vector
applied across the rows of a table is the most common way this happens.
:::

```sql
SELECT to_base64(starburst.crypto.aes_encrypt(to_utf8('secret'), k.key_bytes, 'GCM', NULL, to_utf8(account_id)))
FROM accounts
JOIN keys k ON k.id = accounts.key_id;
```
:::

:::{function} aes_decrypt(binary, key, mode, initialization_vector, associated_data) -> varbinary
Decrypts `binary` produced by {func}`aes_encrypt`, using the given `key`, `mode` and
`associated_data`. In `GCM` mode, raises an error if the authentication tag does not match, which
happens when the key or the associated data is wrong, or the value was modified after encryption.

As with {func}`aes_encrypt`, the trailing arguments are optional and passing `NULL` for
`initialization_vector` or `associated_data` is equivalent to omitting it.

When `initialization_vector` is omitted or `NULL`, it is read from the start of `binary`, as written
by {func}`aes_encrypt`. When it is supplied explicitly, all of `binary` is treated as ciphertext,
with no leading initialization vector.

The explicit form exists solely to read values encrypted by another system that stores the
initialization vector separately from the ciphertext, such as a JSON envelope with distinct fields
for the two. It is not the counterpart of `aes_encrypt`'s `initialization_vector` argument.

:::{warning}
Do not supply `initialization_vector` when decrypting a value produced by {func}`aes_encrypt`, even
if you supplied one to encrypt it. That value already begins with its initialization vector, and the
explicit form decrypts those leading bytes as though they were data.

In `GCM` mode this fails the authentication check and raises an error. In `CBC` mode there is no
authentication to catch it: the call succeeds and returns a value beginning with 16 bytes of
unrelated data, with the rest of the plaintext intact. Nothing in the query reports a problem.

To decrypt a value produced by {func}`aes_encrypt`, always omit `initialization_vector`.
:::

```sql
SELECT from_utf8(TRY(starburst.crypto.aes_decrypt(from_base64(secret_encrypted), k.key_bytes, 'GCM', NULL, to_utf8(account_id))))
FROM accounts
JOIN keys k ON k.id = accounts.key_id;
```
:::
