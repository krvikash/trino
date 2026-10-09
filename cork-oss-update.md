# Maintaining cork (Common Starburst Trino Fork)

## The Fork

Trino clusters used by Starburst (SEP/Galaxy) are not based on OSS Trino distribution (which lives
in https://github.com/trinodb/trino).
Instead, we use code which lives in https://github.com/starburstdata/cork.

Overall we operate in following GitHub repositories

* `trino` - https://github.com/trinodb/trino
* `cork` - https://github.com/starburstdata/cork

For brevity, we will use just names below.

## Development in cork

Development in `cork` repo happens on `master` branch.
The `master` branch in `cork` should always be release-ready and production quality.

If possible we are updating `cork` `master` branch from `trino` `master` branch on open-source release boundaries,
but the process below also allows for pulling incremental changes before OSS release happens.

`cork` may contain commits which are cherry-picked from `trino` which are not
yet part of a `trino` release, and commits which are internal to Starburst and to be shared between
Galaxy and SEP.

## Updating cork

This instruction is for updating Cork from OSS.
Any other downstream repositories likely have separate update instructions.

### Prerequisites

1. Make sure you have GNU versions of sed and find installed and in PATH. The PATH is usually displayed at the end of brew install.

```shell
# for sed
brew install gnu-sed
# Modify the PATH to make this executable your default for 'sed'. E.g: 
export PATH="/usr/local/opt/gnu-sed/libexec/gnubin:$PATH"
 
# for find
brew install findutils
# Modify the PATH to make this executable your default for 'find'. E.g: 
export PATH="/usr/local/opt/findutils/libexec/gnubin:$PATH"
```

2. Install xq

```shell
brew install xq
```

### Update your local repository

```shell
# Set remote for `trino` oss repo
git remote add oss https://github.com/trinodb/trino.git

# Fetch recent release tags from OSS 
git fetch --jobs 8 --all --prune --tags

# Make sure the local master branch is up to date
git checkout master && git reset --hard '@{u}'
```

### Prepare

Choose one of the options:

#### Update to next released OSS Trino version

```shell

UPDATE_DATE=$(date +%Y%m%d)
FROM=$(cat trino-base.txt)
FROM_VERSION=$(git show "${FROM}":pom.xml | xq -x /project/version | cut -d '-' -f 1)
TO_VERSION=$(git tag --contains "${FROM}" --sort=version:refname | grep -E '^[0-9]+$' | while read tag; do [[ "$(git rev-parse "${tag}^{}")" != "$(git rev-parse "${FROM}^{}")" ]] && { echo "$tag"; break; }; done)
TO=$(git rev-parse --verify "${TO_VERSION}^{}" 2>/dev/null)
TO_SHORT=$(git rev-parse --short "$TO")
TARGET_BRANCH="update/cork/trino-${TO_VERSION}-${TO_SHORT}"

# verify they got set correctly and without any whitespace
echo "FROM=[${FROM}] FROM_VERSION=[${FROM_VERSION}] TO=[${TO}] TO_VERSION=[${TO_VERSION}] TO_SHORT=[${TO_SHORT}] TARGET_BRANCH=${TARGET_BRANCH}"

# check for common problems
if [[ -z "$TO" ]]; then
  echo "Error: cannot compute revision for target version. Is "$TO_VERSION" ambiguous tag or this version was not released yet?"
fi
```

#### Update to tip of OSS Trino master

```shell

FROM=$(cat trino-base.txt)
FROM_VERSION=$(git show "${FROM}":pom.xml | xq -x /project/version | awk -F '-' '{ if (/SNAPSHOT/) print $1-1; else print $1; }')
TO=$(git rev-parse --verify remotes/oss/master)
TO_VERSION=$(git show "${TO}":pom.xml | xq -x /project/version | awk -F '-' '{ if (/SNAPSHOT/) print $1-1; else print $1; }')
TO_SHORT=$(git rev-parse --short "$TO")
TARGET_BRANCH="update/cork/trino-${TO_VERSION}-${TO_SHORT}"

# verify they got set correctly and without any whitespace
echo "FROM=[${FROM}] FROM_VERSION=[${FROM_VERSION}] TO=[${TO}] TO_VERSION=[${TO_VERSION}] TO_SHORT=[${TO_SHORT}] TARGET_BRANCH=${TARGET_BRANCH}"
```

### Create the update branch and update PR's placeholder

Create `${TARGET_BRANCH}` starting at `master` branch.
Create the update PR before doing actual code import work.
The PR will serve as a place to store Action items that may occur during the code import process.


```shell
git checkout -b "$TARGET_BRANCH" "origin/master" &&
git commit --allow-empty --only -m "Empty placeholder commit for update to ${TO_VERSION}-${TO_SHORT}" `# this commit will be replaced later` &&
git push origin "${TARGET_BRANCH}" -u &&
open "https://github.com/starburstdata/cork/compare/${TARGET_BRANCH}?expand=1&title=Update+to+Trino+${TO_VERSION}-${TO_SHORT}&body=$(
python3 -c 'import urllib.parse, sys; print(urllib.parse.quote(sys.stdin.read()))' <<EOF
## Update cork to ${TO_VERSION}-${TO_SHORT}

Action items:
- [ ] Update project version
- [ ] Squash fixups
- [ ] Check OSS release notes for breaking changes
- [ ] Run benchmarks and verify results
- [ ] No pinned items left on \`#tmp-cork-trino-update-${TO_VERSION}-${TO_SHORT}\` channel if it exists
      (check it only before final merge, as new pinned items can be added)
- [ ] No items relevant to current update remain on \`#tmp-cork-trino-update-next\` channel

\`\`\`
UPDATE_DATE=${UPDATE_DATE}
FROM=${FROM} 
FROM_VERSION=${FROM_VERSION} 
TO=${TO} 
TO_VERSION=${TO_VERSION} 
TO_SHORT=${TO_SHORT} 
TARGET_BRANCH=${TARGET_BRANCH}
\`\`\`     
   
EOF
)&labels=ai,gpu,redshift,salesforce,salesforce-cdp,synapse,snowflake,google-sheets,bigquery,databricks"
```

### Create Slack channel

Create Slack channel for update related communication.
Obtain channel name with this snippet:

```shell
echo "#tmp-cork-trino-update-${TO_VERSION}-${TO_SHORT}"
```


### Rebase OSS commits onto Cork codebase

Rebase all incoming commits from OSS, except for the ones created by maven-release-plugin.
We will update the version manually.

```shell
# The initial cherry pick is just to add "(cherry picked from commit ...)" to the commit messages
git reset --hard "${FROM}" &&
git rev-list --reverse "${FROM}..${TO}" --invert-grep --grep '^\[maven-release-plugin]' | git cherry-pick -x --stdin
```

```shell
# Verify commit messages are unique. If they are not, change them manually adding "(n)" suffix to commit messages.
# This will later help fixups to be squashed correctly.
echo "The following commits have non-unique commit titles and need to be manually updated:" &&
git log "${FROM}.." --format="%H %s" | grep -Ef <(
    git log "${FROM}.." --format="%s" | sort | uniq -c \
        | grep -v '^\s\+1 ' | sed -e 's/^\s\+[0-9]\+ /[0-9a-f]{40} /' -e 's/$/$/')
# TODO automate updating the messages
```

```shell
# Do the actual rebase
git rebase --interactive --empty=drop "${FROM}" --onto master
# Run the rebase, resolving the code conflicts as necessary and using git rebase --continue to continue
```

#### Identify commits already backported to Cork

Configure in `~/.gitconfig`the custom alias `lg` to show the log graphically.

```shell
[alias]
	lg = log --graph --pretty=format:'%Cred%h%Creset - %s %Cgreen(%cr) %C(bold blue)<%an>%Creset %C(yellow)%d%Creset' --abbrev-commit --date=relative --no-merges
```

While interactively rebasing, in case of stumbling into conflicts, verify that the commits are already backported to Cork. If yes, skip them.

```shell
git lg |grep "$(git rebase --show-current-patch |head -5 |tail -1 |sed 's/^[[:space:]]*//')"
```

### Update project version

As the maven-release-plugin commits were skipped during previous step, we need to change the version ourselves

```shell
sed -i "s/<revision>.*<\/revision>/<revision>"${TO_VERSION}-cork-${UPDATE_DATE}-SNAPSHOT"<\/revision>/" pom.xml
git commit -a -m "Bump Cork version after code sync with Trino ${TO_VERSION}-${TO_SHORT}"
echo "${TO}" > trino-base.txt &&
git commit -a -m "Update trino-base.txt after sync with Trino ${TO_VERSION} (${TO})"

```

### Update the update PR

Push the ready `update/cork/trino-${TO_VERSION}-${TO}` branch to `origin` remote.

The branch is shared, so pin the lease to the remote state you inspected. A bare
`--force-with-lease` compares with `origin/<branch>` at push time, and any fetch in between
(an IDE's background fetch too) moves that ref, so the lease passes and the push deletes commits
you never saw:

```shell
git fetch origin "${TARGET_BRANCH}"
EXPECT=$(git rev-parse "origin/${TARGET_BRANCH}")
git log --oneline "HEAD..${EXPECT}"         # must be empty, or include those commits first
git push --force-with-lease="${TARGET_BRANCH}:${EXPECT}" origin "HEAD:${TARGET_BRANCH}"
```

### Test out the PR on CI and iterate

If the CI fails on the Update PR, make relevant fixes as separate commits. Never amend a replayed
OSS commit: reviewers read cork's changes, not the 200-600 imported commits, so each cork-side
change has to be visible on its own until the PR is approved.

| The fix is… | Commit it as |
|---|---|
| needed by exactly one incoming commit | `git commit --fixup=<sha>` (`fixup! <exact subject>`) |
| needed by several commits, or by none in particular | `👷 <subject>` |
| a regression already fixed upstream | cherry-pick the upstream fix with `-x` |
| an upstream change cork should not take | `Revert "<subject>"`, with the reason in the body |

Place each `fixup!` directly after its target. CI's `check-commit` compiles every commit except
one that a later `fixup!` amends, so a fixup that repairs a compile break exempts its target but
not the commits between them: every one of those goes red until the fixup is moved up. Do not
squash until the PR is approved.

In a revert, delete the `This reverts commit <sha>` line `git revert` adds and name the target
by subject. That sha is the replayed commit's and changes on every rebase.

When a problem is deferred rather than fixed in the update (an excluded upstream change, a
revert to undo later, a connector bug for another team), file a Jira for it, link it to the
update's tracking ticket and from the PR.

### Run benchmarks on the Update PR

Benchmarking the update PR is done through **BaaS** (Benchmark-as-a-Service).
The old `benchmarks-gha` `benchmark-pr.yaml` workflow is no longer used for this.

1. Open the **PR Benchmarks** page: https://baas.in.starburst.io/pr-benchmarks
   (every action below - adding the PR, adding configs, running, and posting - requires the
   `baas-admin` Cloudflare Access group; without it you can still view existing entries).
2. Click **Add PR**, pick repository `starburstdata/cork` from the dropdown, enter the update PR
   number, and submit. This adds a tracked entry but doesn't start anything yet.
3. Expand the new row and click **Add default configs** to attach cork's current default benchmark
   config set. The default set is admin-managed (edit it via the **PR Benchmark Defaults** button
   on this same page) and can change over time, so use this button rather than hardcoding specific
   benchmark/test-type names here.
4. Click **Run New Configs** (dispatches every config entry that hasn't run yet) or **Run All**
   (re-runs or starts every entry that isn't currently in flight) and confirm the cost-warning
   dialog. BaaS resolves `refs/pull/<n>/head` fresh at dispatch time, so re-running after a
   force-push to the PR always benchmarks the latest commit - no need to recompute a PR link
   manually.
5. Wait for the run(s) to finish. The row shows live status plus two columns once each run
   completes:
   - **Baseline comparison** - a one-to-one check against a baseline run at the exact commit where
     the PR branched from `master` (the merge-base commit). BaaS reuses an existing matching-config
     run at that commit if one exists, or provisions a fresh one on the spot if not. This is the
     main "is this a regression" signal.
   - **Trend** - an independent second check against the recurring schedule's own historical
     regime for that config (many past scheduled runs, not just one baseline), anchored at the
     commit the PR branched from. Catches a regression even if the single baseline run above
     happened to be noisy or atypical.
6. Click **Post to PR** to post a single sticky comment to the GitHub PR with a
   `Config | Commit | Result | Trend` table. Each `Config` cell links straight into BaaS's own
   comparison page for that config, with the regression verdict already computed, so you can open
   it for the full per-query breakdown instead of eyeballing variance in a separate dashboard; each
   `Commit` cell links the short SHA to the exact GitHub commit that was benchmarked.

We can say there is no regression if the Result/Trend verdict is ✅ (`NO_SIGNIFICANT_CHANGE`,
`IMPROVEMENT`, or `MINOR_IMPROVEMENT`). A ⚠️ (`MINOR_REGRESSION` / `INSUFFICIENT_DATA`) or ❌
(`REGRESSION`) verdict needs investigation before merging the update.

### Check OSS release notes

Read OSS release notes for versions FROM_VERSION..TO_VERSION. Look out for any potential breaking changes. They are not guaranteed to be called out as using
the word "breaking" (or any other particular word), so read and try to understand every release notes bullet point.

```shell
for v in $(seq "$[FROM_VERSION+1]" "${TO_VERSION}"); do
    open "https://trino.io/docs/current/release/release-${v}.html"
done
```

If you are doing incremental update to version a commit which was not yet released in OSS you need to look at individual PRs
pulled into update. There is no `release-NNN.md` for it yet, not even upstream: each upstream PR
body may have a `## Release notes` section, and one PR usually covers several commits.

```shell
gh api "repos/trinodb/trino/commits/${TO}" >/dev/null   # gh needs read access to trinodb/trino
for sha in $(git rev-list --reverse "${FROM}..${TO}" --invert-grep --grep '^\[maven-release-plugin]'); do
    pr=$(gh api "repos/trinodb/trino/commits/${sha}/pulls" --jq '.[0] | "\(.number) \(.title)"' 2>/dev/null) \
      && echo "${pr}" || echo "LOOKUP FAILED: ${sha}"
done | sort -u
```

Then read each PR's description (`gh pr view --repo trinodb/trino <number>`), including its
`## Release notes` section when it has one.
Notes are not enough on their own: a PR's note can skip changes made by its other commits, and a
PR marked "not user-visible" can still change behaviour. Also check the diff itself:

```shell
git diff "${FROM}..${TO}" -- core/trino-spi/src/main/java \
  | grep -E '^-\s+(public|default)\s|^-    [A-Za-z<][^=;{(]* [a-z][A-Za-z0-9_]*\(' \
  | grep -vE '^-\s+private\s'   # removed SPI, including implicitly public interface methods
git diff "${FROM}..${TO}" -- '*/src/main/java/*' | grep -E '^-\s*@Config\(|^\+\s*@(Legacy|Defunct)Config\('
git diff "${FROM}..${TO}" -- '*/src/main/java/*AccessControl*.java' '*/src/main/java/*/security/*.java'   # read in full: a changed checkCan body has no signature to grep for
```

### get the cork PR reviewed and merged

Seek the reviews from `@starburstdata/galaxy-trino-maintainers` on the `update/cork/trino-${TO_VERSION}-${TO_SHORT}` PR.
When PR gets approved squash in the fixup commits and merge.

Squash in place, onto the branch's current base. `FROM` is an OSS commit, not an ancestor of the
branch after the rebase onto master, so squashing onto it would replay cork's history onto
upstream. The squash must not change the tree:

```shell
git checkout "${TARGET_BRANCH}" &&
PRE=$(git rev-parse HEAD) &&
GIT_SEQUENCE_EDITOR=true git rebase --interactive --autosquash "$(git merge-base HEAD origin/master)" &&
{ git diff --quiet "${PRE}" HEAD && echo "squash is tree-neutral" \
  || echo "squash CHANGED the tree: do not push"; }
```

Push it (see *Update the update PR*) and wait for CI: the squash changes every sha.

GitHub's *Rebase and merge* fails on PRs this size, so merge from the command line. A merge is a
fast-forward, so it never needs force, and it must land a sha CI has already passed:

```shell
git fetch origin &&
git checkout "${TARGET_BRANCH}" &&
git rebase origin/master &&                 # no-op if master has not moved
{ [ "$(git rev-parse HEAD)" = "$(git rev-parse "origin/${TARGET_BRANCH}")" ] \
  || { echo "rebase moved the branch: push it and wait for CI first"; false; }; } &&
gh pr checks --repo starburstdata/cork "${TARGET_BRANCH}" &&   # fails unless every check is green on this sha
git push --atomic origin HEAD:master "HEAD:${TARGET_BRANCH}"   # both fast-forwards or neither: fails if the branch moved after the check
```

Run it as one paste: if any step fails, the push does not run.

This marks the PR as merged automatically.
