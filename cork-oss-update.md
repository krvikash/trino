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

FROM=$(cat trino-base.txt)
FROM_VERSION=$(git show "${FROM}":pom.xml | xq -x /project/version | cut -d '-' -f 1)
TO_VERSION=$[FROM_VERSION + 1]
TO=$(git rev-parse --verify $TO_VERSION 2>/dev/null)
TO_SHORT=$(git rev-parse --short "$TO_VERSION")
TARGET_BRANCH="update/cork/trino-${TO_VERSION}-${TO_SHORT}"

# verify they got set correctly and without any whitespace
echo "FROM=[${FROM}] FROM_VERSION=[${FROM_VERSION}] TO=[${TO}] TO_VERSION=[${TO_VERSION}]" TO_SHORT=[${TO_SHORT}] TARGET_BRANCH=${TARGET_BRANCH}"

# check for common problems
if [[ -z "$TO" ]]; then
  echo "Error: cannot compute revision for target version. Is "$TO_VERSION" ambiguous tag or this version was not released yet?"
fi
```

#### Update to tip of OSS Trino master

```shell

FROM=$(cat trino-base.txt)
FROM_VERSION=$(git show "${FROM}":pom.xml | xq -x /project/version | cut -d '-' -f 1)
TO=$(git rev-parse --verify remotes/oss/master)
TO_VERSION=$(git show "${TO}":pom.xml | xq -x /project/version | cut -d '-' -f 1)
if git show "${TO}":pom.xml | xq -x /project/version | grep -q SNAPSHOT; then
    TO_VERSION=$[TO_VERSION - 1]
fi
TO_SHORT=$(git rev-parse --short "$TO_VERSION")
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
EOF
)&labels=salesforce,synapse,snowflake"
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

### Update project version

As the maven-release-plugin commits were skipped during previous step, we need to change the version ourselves

```shell
./mvnw versions:set -DnewVersion="${TO_VERSION}-cork-1-SNAPSHOT" &&
./mvnw -pl :trino-test-jdbc-compatibility-old-driver versions:set-property -Dproperty="dep.presto-jdbc-under-test" -DnewVersion="${TO_VERSION}-cork-1-SNAPSHOT" &&
find -name pom.xml.versionsBackup -delete &&

# only create commit if actually updated any pom files
if ! git diff --quiet; then
  git commit -a -m "Bump Cork version after code sync with Trino ${TO_VERSION}-${TO_SHORT}"
fi
echo "${TO}" > trino-base.txt &&
git commit -a -m "Update trino-base.txt after sync with Trino ${TO_VERSION} (${TO})"

```

### Update the update PR

Push the ready `update/cork/trino-${TO_VERSION}-${TO}` branch to `origin` remote.

```shell
git push origin "${TARGET_BRANCH}" --force-with-lease
```

### Test out the PR on CI and iterate

If the CI fails on the Update PR, make relevant fixes, updating the fixup commits

### Run benchmarks on the Update PR

Run this workflow https://github.com/starburstdata/benchmarks-gha/actions/workflows/benchmark-pr.yaml on update PR
using following benchmarks:
- `iceberg/sf1000_parquet_unpart`
- `iceberg/sf1000_parquet_part_c5`

```shell
PR_LINK=$(gh pr list -H ${TARGET_BRANCH} --json url --jq '.[].url')
gh workflow run --repo starburstdata/benchmarks-gha benchmark-pr.yaml -f PrLink=${PR_LINK} -fTestType="iceberg/sf1000_parquet_unpart" -fUseOnDemandNodes=true
gh workflow run --repo starburstdata/benchmarks-gha benchmark-pr.yaml -f PrLink=${PR_LINK} -fTestType="iceberg/sf1000_parquet_part_c5" -fUseOnDemandNodes=true
```

Above workflow on completion will add a comment to the PR with status of benchmark run, and a link to Tableau
dashboard comparing results to the closest run of same config from the master branch. Tableau dashboard will
have 2 tabs:

1. `Sum` - Here make sure that new results are lower, keep in mind variance in results should be <5%.
2. `Per Query` - Here queries are be sorted from the biggest difference to the baseline. Make sure that the
   biggest difference are in favor of update, or they are lower than 3 seconds and rapidly going down in next
   queries.

We can say that there is no regression if both above points are satisfied.
Slight regression would most likely present as a more than 5 sec increase in one (or more) of the queries.

### Check OSS release notes

Read OSS release notes for versions FROM_VERSION..TO_VERSION. Look out for any potential breaking changes. They are not guaranteed to be called out as using
the word "breaking" (or any other particular word), so read and try to understand every release notes bullet point.

```shell
for v in $(seq "$[FROM_VERSION+1]" "${TO_VERSION}"); do
    open "https://trino.io/docs/current/release/release-${v}.html"
done
```

If you are doing incremental update to version a commit which was not yet released in OSS you need to look at individual PRs
pulled into update.

TODO: provide helper snippet.
