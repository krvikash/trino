#!/usr/bin/env python3
"""
Split a trino-server tree into 3 or 4 Docker layers so they can pull in parallel.

Usage: classify-layers.py <src-dir> <base-dir> <data-lake-dir> <unique-dir> [<cudf-dir>]

src-dir is the extracted trino-server tree. Files outside plugin/ land in base-dir;
when cudf-dir is provided, the cudf jar is carved out of lib/ into cudf-dir instead.
Only plugin/ contents are split across data-lake-dir and unique-dir.

Why split at all?  Docker pulls layers in parallel, so carving the ~1 GiB
trino-server tree into a few substantial chunks lets a cold pull fan out instead
of streaming one giant blob.  The hard constraint: hardlinks only survive inside a
single layer, so every path of a given inode must land in the same layer — split
them and Docker silently makes real copies (~7.8 GiB of bloat).  That is why we
classify by *inode* (the set of all its paths), never by individual file.

The four layers, ordered most-shared → least-shared:
  base-dir/       → /usr/lib/trino/          lib/ + bin/ + every jar hardlinked
                                             widely (into lib/, or across unrelated
                                             plugin clusters).  The reused core.
  data-lake-dir/  → /usr/lib/trino/plugin/   jars whose every copy stays inside the
                                             data-lake cluster (galaxy-objectstore,
                                             hive, iceberg, starburst-functions,
                                             warp-speed) — big cluster-local deps
                                             plus those plugins' own files.
  unique-dir/     → /usr/lib/trino/plugin/   everything else: single-path files (and
                                             same-plugin hardlinks) from non-DL plugins.
  cudf-dir/       → /usr/lib/trino/lib/      the one ~340 MiB ai.rapids_cudf jar —
                                             huge and self-contained, worth its own
                                             parallel stream (optional 4th layer).

Hardlinks in the source are re-established within each output layer via os.link(),
so Docker sees them as hardlinks in the final image.
"""

import os
import shutil
import stat
import sys
from collections import defaultdict

DATA_LAKE_PLUGINS = frozenset({
    'galaxy-objectstore', 'hive', 'iceberg', 'starburst-functions', 'warp-speed',
})

CUDF_JAR_PREFIX = 'ai.rapids_cudf'


def _classify_inode(rel_paths, has_cudf_layer=False):
    """Return 'base', 'data-lake', 'unique', or 'cudf' for one inode's set of paths.

    Rules are checked in order and EACH ASSUMES THE EARLIER ONES DID NOT MATCH.
    The ordering is load-bearing: Rule 2 removes every cross-cluster inode first,
    which is the only reason Rule 3 can treat "subset of DL plugins" as
    "lives exclusively in the DL cluster".  Reorder these and the split breaks.
    """
    # Rule 0 – the cudf jar is huge and self-contained → its own layer.
    #   e.g. lib/ai.rapids_cudf-26.02.0-cuda13.jar
    if has_cudf_layer and any(p.startswith('lib/') and os.path.basename(p).startswith(CUDF_JAR_PREFIX) for p in rel_paths):
        return 'cudf'

    # Rule 1 – touches lib/ or bin/ (any path outside plugin/) → base.
    #   Catches the core runtime plus every plugin jar hardlinked back to lib/.
    #   e.g. lib/it.unimi.dsi_fastutil-8.5.18.jar (also linked into ~15 plugins)
    if any(not p.startswith('plugin/') for p in rel_paths):
        return 'base'

    plugins = set()
    for p in rel_paths:
        parts = p.split('/')
        if parts[0] == 'plugin' and len(parts) >= 2:
            plugins.add(parts[1])

    # Rule 2 – shared across 2+ plugins spanning beyond the DL cluster → base.
    #   Cross-cluster reuse belongs with the other widely-shared jars (and keeps
    #   Rule 3's "subset" test sound). e.g. a jar in both hive (DL) and redshift (non-DL).
    if len(plugins) >= 2 and not plugins.issubset(DATA_LAKE_PLUGINS):
        return 'base'

    # Rule 3 – every remaining path is inside the DL cluster → data-lake.
    #   Both DL-exclusive shared jars AND a DL plugin's own single-path files.
    #   e.g. a jar shared only by hive+iceberg, or galaxy-objectstore/foo.jar
    if plugins and plugins.issubset(DATA_LAKE_PLUGINS):
        return 'data-lake'

    # Rule 4 – fallback: single-path files, or hardlinks within one non-DL plugin.
    #   No cross-plugin sharing, so cheap to isolate. e.g. postgresql/postgresql-42.7.4.jar
    return 'unique'


def _dest(rel_path, layer, base_dir, dl_dir, unique_dir, cudf_dir=None):
    if layer == 'cudf':
        if not rel_path.startswith('lib/'):
            raise ValueError(f"cudf path outside lib/: {rel_path!r}")
        return os.path.join(cudf_dir, os.path.basename(rel_path))
    if layer == 'base':
        return os.path.join(base_dir, rel_path)
    if not rel_path.startswith('plugin/'):
        raise ValueError(f"non-base path outside plugin/: {rel_path!r}")
    without_prefix = rel_path[len('plugin/'):]
    return os.path.join(dl_dir if layer == 'data-lake' else unique_dir, without_prefix)


def classify(src_dir, base_dir, dl_dir, unique_dir, cudf_dir=None):
    # Group every file by inode so all of an inode's hardlinked paths are
    # classified — and staged — together. (symlinks aren't present in
    # trino-server tarballs, so we skip them.)
    inode_paths = defaultdict(list)
    for root, _dirs, files in os.walk(src_dir):
        for fname in files:
            fpath = os.path.join(root, fname)
            st = os.lstat(fpath)
            if stat.S_ISLNK(st.st_mode):
                print(f"warning: skipping symlink {os.path.relpath(fpath, src_dir)!r}", file=sys.stderr)
            else:
                inode_paths[st.st_ino].append(fpath)

    counts = defaultdict(int)
    for paths in inode_paths.values():
        rel_paths = sorted(os.path.relpath(p, src_dir) for p in paths)
        layer = _classify_inode(rel_paths, has_cudf_layer=cudf_dir is not None)
        counts[layer] += 1

        # Stage the inode once, then re-establish its other paths as hardlinks.
        canonical = None
        for rel_path in rel_paths:
            dest = _dest(rel_path, layer, base_dir, dl_dir, unique_dir, cudf_dir)
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            if canonical is None:
                shutil.copy2(os.path.join(src_dir, rel_path), dest)
                canonical = dest
            else:
                os.link(canonical, dest)

    for layer in ('base', 'data-lake', 'unique', 'cudf'):
        print(f"  {layer}: {counts[layer]} inodes", file=sys.stderr)


if __name__ == '__main__':
    # 4 positional args (3-layer split) or 5 (with the optional cudf layer).
    args = sys.argv[1:]
    if len(args) not in (4, 5):
        print(__doc__, file=sys.stderr)
        sys.exit(1)
    classify(*args)
