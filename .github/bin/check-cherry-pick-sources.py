#!/usr/bin/env python3

import argparse
import logging
import re
import subprocess
import sys
import time
import unittest

# A cherry-picked commit must already be on master of one of these repositories. A change may be
# implemented first in any of them, so there is no single upstream.
SOURCE_REPOSITORIES = (
    "trinodb/trino",
    "starburstdata/cork",
    "starburstdata/galaxy-trino",
    "starburstdata/starburst-enterprise-trino",
)
SOURCE_BRANCH = "master"
LOOKUP_ATTEMPTS = 3

MARKER = "(cherry picked from commit"
MARKER_PATTERN = re.compile(r"\(cherry picked from commit ([0-9a-f]{40})\)")


def main():
    parser = argparse.ArgumentParser(
        description=f"Verify that every '{MARKER} <sha>)' line in commit messages references a commit on {SOURCE_BRANCH} of a known repository."
    )
    parser.add_argument(
        "revision_range",
        nargs="?",
        help="Commits to check, e.g. origin/master..HEAD",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_const",
        dest="loglevel",
        const=logging.INFO,
        default=logging.WARNING,
        help="Print info level logs",
    )
    parser.add_argument(
        "-t",
        "--test",
        action="store_true",
        help="test this script instead of executing it",
    )

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel)
    if args.test:
        sys.argv = [sys.argv[0]]
        unittest.main()
        return
    if args.revision_range is None:
        parser.error("revision_range is required")

    errors = check(read_commit_messages(args.revision_range), find_source_repository)
    for error in errors:
        print(f"::error::{error}")
    if errors:
        print(
            f"Cherry-pick markers must reference the commit as it landed on {SOURCE_BRANCH} of one of: {', '.join(SOURCE_REPOSITORIES)}. "
            "A SHA from a PR branch differs from the merged one, because PRs are merged by rebasing.",
            file=sys.stderr,
        )
        sys.exit(1)


def read_commit_messages(revision_range):
    output = subprocess.run(
        ["git", "log", "--format=%H%x00%B%x1e", revision_range, "--"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    messages = {}
    for record in output.split("\x1e"):
        record = record.strip("\n")
        if not record:
            continue
        commit, message = record.split("\x00", 1)
        messages[commit] = message
    return messages


def check(messages, source_lookup):
    errors = []
    sources = {}
    for commit, message in messages.items():
        for line in message.splitlines():
            line = line.strip()
            # git writes the marker on its own line; elsewhere it's prose mentioning the marker
            if not line.startswith(MARKER):
                continue
            match = MARKER_PATTERN.fullmatch(line)
            if match is None:
                errors.append(f"Commit {commit} has a malformed cherry-pick marker, expected '{MARKER} <full 40-character SHA>)': {line}")
                continue
            source = match.group(1)
            if source not in sources:
                sources[source] = source_lookup(source)
            if sources[source] is None:
                errors.append(f"Commit {commit} is cherry-picked from {source}, which is not on {SOURCE_BRANCH} of any of: {', '.join(SOURCE_REPOSITORIES)}")
            else:
                logging.info("Commit %s is cherry-picked from %s, found on %s of %s", commit, source, SOURCE_BRANCH, sources[source])
    return errors


def find_source_repository(sha):
    for repository in SOURCE_REPOSITORIES:
        if is_on_branch(repository, sha):
            return repository
    return None


def is_on_branch(repository, sha):
    for attempt in range(1, LOOKUP_ATTEMPTS + 1):
        result = subprocess.run(
            ["gh", "api", f"repos/{repository}/compare/{sha}...{SOURCE_BRANCH}", "--jq", ".status"],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            break
        if "(HTTP 404)" in result.stderr:
            # Commit unknown to the repository
            return False
        if attempt == LOOKUP_ATTEMPTS:
            raise RuntimeError(f"Failed to look up {sha} in {repository}: {result.stderr.strip()}")
        logging.warning("Failed to look up %s in %s, retrying: %s", sha, repository, result.stderr.strip())
        time.sleep(attempt * 5)
    status = result.stdout.strip()
    logging.info("%s in %s: %s", sha, repository, status)
    # "ahead" means the branch is ahead of the commit, i.e. the commit is its ancestor
    return status in ("ahead", "identical")


class TestCheck(unittest.TestCase):
    ON_MASTER = "1" * 40
    NOT_ON_MASTER = "2" * 40

    def lookup(self, sha):
        return "trinodb/trino" if sha == self.ON_MASTER else None

    def test_check(self):
        cases = [
            ("No marker", {"c1": "Subject\n\nBody"}, []),
            ("Marker mentioned in prose", {"c1": f"Subject\n\nA `{MARKER} <sha>)` line"}, []),
            ("Valid marker", {"c1": f"Subject\n\n{MARKER} {self.ON_MASTER})"}, []),
            (
                "Unknown commit",
                {"c1": f"Subject\n\n{MARKER} {self.NOT_ON_MASTER})"},
                [f"Commit c1 is cherry-picked from {self.NOT_ON_MASTER}, which is not on master of any of: {', '.join(SOURCE_REPOSITORIES)}"],
            ),
            (
                "Two markers, one unknown",
                {"c1": f"Subject\n\n{MARKER} {self.ON_MASTER})\n{MARKER} {self.NOT_ON_MASTER})"},
                [f"Commit c1 is cherry-picked from {self.NOT_ON_MASTER}, which is not on master of any of: {', '.join(SOURCE_REPOSITORIES)}"],
            ),
            (
                "Abbreviated SHA",
                {"c1": f"Subject\n\n{MARKER} {self.ON_MASTER[:12]})"},
                [f"Commit c1 has a malformed cherry-pick marker, expected '{MARKER} <full 40-character SHA>)': {MARKER} {self.ON_MASTER[:12]})"],
            ),
        ]
        for test_name, messages, expected in cases:
            with self.subTest(test_name):
                self.assertEqual(check(messages, self.lookup), expected)


if __name__ == "__main__":
    main()
