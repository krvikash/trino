#!/usr/bin/env python3

import argparse
import importlib.util
import json
from pathlib import Path
import sys
import textwrap
import unittest


SUITE_DIR = Path("testing/trino-product-tests/src/test/java/io/trino/tests/product/suite")
LEGACY_SUITE_DIR = Path("testing/trino-product-tests-launcher/src/main/java/io/trino/tests/product/launcher/suite/suites")


def load_schedule_module():
    # the schedule-aware script has a hyphenated name, so it can't be imported with a plain import
    path = Path(__file__).with_name("build-matrix-schedule-aware.py")
    spec = importlib.util.spec_from_file_location("build_matrix_schedule_aware", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def parse_args():
    parser = argparse.ArgumentParser(description="Build the JUnit product test bucket matrix.")
    parser.add_argument(
        "-e",
        "--event-name",
        help="Name of the event which triggered the workflow, i.e. ${{ github.event_name }}. "
        "If unset, buckets are not filtered by their 'when' schedule.",
    )
    parser.add_argument(
        "-s",
        "--event-schedule",
        help="The cron of scheduled workflows, i.e. ${{ github.event.schedule }}",
    )
    parser.add_argument(
        "-L",
        "--pr-labels",
        type=json.loads,
        help="JSON string of PR labels, i.e. ${{ toJson(github.event.pull_request.labels) }}",
    )
    parser.add_argument(
        "-t",
        "--test",
        action="store_true",
        help="test this script instead of executing it",
    )
    return parser.parse_args()


def legacy_removed(*legacy_suites):
    return all(not (LEGACY_SUITE_DIR / f"{suite}.java").exists() for suite in legacy_suites)


def junit_exists(suite):
    return (SUITE_DIR / f"{suite}.java").exists()


AGGREGATE_LEGACY_SUITES = (
    "Suite1",
    "Suite2",
    "Suite3",
    "Suite5",
    "Suite6NonGeneric",
    "Suite7NonGeneric",
)

# Use this when coverage is removed from an aggregate legacy suite that still
# exists for unrelated tests.
LEGACY_COVERAGE_REMOVED = ()

# Buckets are run on every event unless a `when` filter is given. The filter uses the same keys and
# semantics as `when` in .github/schedule-config.yml (see .github/schedule-config-schema.yml), except
# that there is no impact detection: on pull requests only `labeled` can select a filtered bucket.
BUCKETS = [
    ("jdbc-core", [
        ("SuiteMysql", ("SuiteMysql",)),
        ("SuitePostgresql", LEGACY_COVERAGE_REMOVED),
        ("SuiteSqlServer", LEGACY_COVERAGE_REMOVED),
        ("SuiteFunctions", LEGACY_COVERAGE_REMOVED),
        ("SuiteTpch", ("SuiteTpch",)),
        ("SuiteTpcds", ("SuiteTpcds",)),
    ]),
    ("jdbc-external", [
        ("SuiteExasol", LEGACY_COVERAGE_REMOVED),
        # SuiteSnowflake covers the OSS Snowflake connector, which cork does not ship.
        # The Starburst Snowflake suite is run instead.
    ]),
    ("connector-smoke", [
        ("SuiteCassandra", LEGACY_COVERAGE_REMOVED),
        ("SuiteClickhouse", LEGACY_COVERAGE_REMOVED),
        ("SuiteBlackHole", LEGACY_COVERAGE_REMOVED),
        ("SuiteAllConnectorsSmoke", LEGACY_COVERAGE_REMOVED),
        ("SuiteIgnite", LEGACY_COVERAGE_REMOVED),
    ]),
    ("auth-and-clients", [
        ("SuiteKafka", LEGACY_COVERAGE_REMOVED),
        ("SuiteLdap", LEGACY_COVERAGE_REMOVED),
        ("SuiteOauth2", LEGACY_COVERAGE_REMOVED),
        ("SuiteClients", LEGACY_COVERAGE_REMOVED),
        ("SuiteJdbcKerberos", LEGACY_COVERAGE_REMOVED),
        ("SuiteLoki", LEGACY_COVERAGE_REMOVED),
        ("SuiteRanger", LEGACY_COVERAGE_REMOVED),
        ("SuiteTls", LEGACY_COVERAGE_REMOVED),
    ]),
    ("hive-basic", [
        ("SuiteHiveBasic", LEGACY_COVERAGE_REMOVED),
        ("SuiteHmsOnly", LEGACY_COVERAGE_REMOVED),
        ("SuiteHiveStorageFormats", LEGACY_COVERAGE_REMOVED),
        ("SuiteSqlCancel", LEGACY_COVERAGE_REMOVED),
    ]),
    ("hive-kerberos", [
        ("SuiteHdfsImpersonation", LEGACY_COVERAGE_REMOVED),
        ("SuiteTwoHives", LEGACY_COVERAGE_REMOVED),
        ("SuiteHive4", LEGACY_COVERAGE_REMOVED),
        ("SuiteHudi", LEGACY_COVERAGE_REMOVED),
    ]),
    ("hive-transactional", [
        ("SuiteHiveTransactional", LEGACY_COVERAGE_REMOVED),
        ("SuiteAuthorization", LEGACY_COVERAGE_REMOVED),
        ("SuiteFaultTolerant", LEGACY_COVERAGE_REMOVED),
    ]),
    ("hive-storage", [
        ("SuiteHiveSpark", LEGACY_COVERAGE_REMOVED),
        ("SuiteHiveAlluxioCaching", LEGACY_COVERAGE_REMOVED),
        ("SuiteStorageFormatsDetailed", LEGACY_COVERAGE_REMOVED),
        ("SuiteParquet", LEGACY_COVERAGE_REMOVED),
    ]),
    ("iceberg", [
        ("SuiteIceberg", LEGACY_COVERAGE_REMOVED),
    ]),
    ("delta-lake", [
        ("SuiteDeltaLakeOss", LEGACY_COVERAGE_REMOVED),
        ("SuiteCompatibility", LEGACY_COVERAGE_REMOVED),
    ]),
    ("cloud-object-store", [
        ("SuiteGcs", LEGACY_COVERAGE_REMOVED),
        ("SuiteAzure", LEGACY_COVERAGE_REMOVED),
    ]),
    ("databricks-154", [
        ("SuiteDeltaLakeDatabricks154", LEGACY_COVERAGE_REMOVED),
    ], {"scheduled": "nightly", "labeled": ["databricks"]}),
    ("databricks-unity", [
        ("SuiteDeltaLakeDatabricksUnity", LEGACY_COVERAGE_REMOVED),
    ], {"scheduled": "nightly", "labeled": ["databricks"]}),
    ("databricks-18", [
        ("SuiteDeltaLakeDatabricks18", LEGACY_COVERAGE_REMOVED),
    ]),
]


def bucket_runs(schedule, github_context, when):
    if when is None or github_context is None:
        return True
    item = schedule.ScheduleConfigItem(module_sets=[], when=schedule.load_when(when))
    return item.matches_event(github_context)


def build_github_context(event_name, event_schedule, pr_labels):
    if not event_name:
        return None
    return {
        "event_name": event_name,
        "event": {
            "schedule": event_schedule or "",
            "pull_request": {"labels": pr_labels or []},
        },
    }


def build_matrix(buckets, schedule, github_context, suite_selected=None):
    if suite_selected is None:
        def suite_selected(suite, legacy_suites):
            return junit_exists(suite) and legacy_removed(*legacy_suites)

    include = []
    for bucket, suites, *rest in buckets:
        if not bucket_runs(schedule, github_context, rest[0] if rest else None):
            continue
        selected_suites = [suite for suite, legacy_suites in suites if suite_selected(suite, legacy_suites)]
        if selected_suites:
            include.append({
                "bucket": bucket,
                "suites": " ".join(selected_suites),
            })
    return {"include": include} if include else {}


def main():
    args = parse_args()
    if args.test:
        sys.argv = [sys.argv[0]]
        unittest.main()
        return
    schedule = load_schedule_module()
    github_context = build_github_context(args.event_name, args.event_schedule, args.pr_labels)
    print(json.dumps(build_matrix(BUCKETS, schedule, github_context)))


NIGHTLY_CRON = "0 0 * * *"
WEEKLY_CRON = "0 0 * * 6"


def labels(*names):
    return [{"name": name} for name in names]


class TestBuildMatrix(unittest.TestCase):
    TEST_BUCKETS = [
        ("every-event", [("SuiteEveryEvent", ())]),
        ("nightly", [("SuiteNightly", ())], {"scheduled": "nightly"}),
        ("weekly", [("SuiteWeekly", ())], {"scheduled": "weekly"}),
        ("never", [("SuiteNever", ())], {"scheduled": "never"}),
        ("nightly-no-dispatch", [("SuiteNightlyNoDispatch", ())], {"scheduled": "nightly", "dispatches": False}),
        ("nightly-or-label", [("SuiteNightlyOrLabel", ())], {"scheduled": "nightly", "labeled": ["databricks"]}),
        ("on-push", [("SuiteOnPush", ())], {"scheduled": "never", "pushes": True}),
    ]

    @classmethod
    def setUpClass(cls):
        cls.schedule = load_schedule_module()

    def buckets(self, event_name=None, event_schedule="", pr_labels=None, buckets=None, suite_selected=None):
        context = build_github_context(event_name, event_schedule, pr_labels)
        matrix = build_matrix(
            self.TEST_BUCKETS if buckets is None else buckets,
            self.schedule,
            context,
            suite_selected or (lambda suite, legacy_suites: True),
        )
        return [item["bucket"] for item in matrix.get("include", [])]

    def test_no_event_does_not_filter(self):
        self.assertEqual(
            self.buckets(),
            ["every-event", "nightly", "weekly", "never", "nightly-no-dispatch", "nightly-or-label", "on-push"],
        )

    def test_nightly_schedule(self):
        self.assertEqual(
            self.buckets("schedule", NIGHTLY_CRON),
            ["every-event", "nightly", "nightly-no-dispatch", "nightly-or-label"],
        )

    def test_weekly_schedule(self):
        self.assertEqual(self.buckets("schedule", WEEKLY_CRON), ["every-event", "weekly"])

    def test_pull_request(self):
        self.assertEqual(self.buckets("pull_request"), ["every-event"])
        self.assertEqual(self.buckets("pull_request", pr_labels=labels("other")), ["every-event"])

    def test_pull_request_label_forces_bucket(self):
        self.assertEqual(
            self.buckets("pull_request", pr_labels=labels("other", "databricks")),
            ["every-event", "nightly-or-label"],
        )

    def test_push(self):
        self.assertEqual(self.buckets("push"), ["every-event", "on-push"])
        self.assertEqual(self.buckets("merge_group"), ["every-event", "on-push"])

    def test_dispatch(self):
        for event_name in ["workflow_dispatch", "repository_dispatch"]:
            self.assertEqual(
                self.buckets(event_name),
                ["every-event", "nightly", "weekly", "never", "nightly-or-label", "on-push"],
                event_name,
            )

    def test_unselected_suites_are_dropped(self):
        buckets = [
            ("some-suites-dropped", [("SuiteKept", ()), ("SuiteDropped", ()), ("SuiteAlsoKept", ())]),
            ("all-suites-dropped", [("SuiteDropped", ())]),
        ]
        selected = lambda suite, legacy_suites: suite != "SuiteDropped"
        matrix = build_matrix(buckets, self.schedule, None, selected)
        self.assertEqual(matrix, {"include": [{"bucket": "some-suites-dropped", "suites": "SuiteKept SuiteAlsoKept"}]})

    def test_empty_matrix(self):
        self.assertEqual(build_matrix(self.TEST_BUCKETS, self.schedule, None, lambda s, l: False), {})
        self.assertEqual(build_matrix([], self.schedule, None), {})

    def test_suites_are_space_separated(self):
        buckets = [("two-suites", [("SuiteFirst", ()), ("SuiteSecond", ())])]
        self.assertEqual(
            build_matrix(buckets, self.schedule, None, lambda s, l: True),
            {"include": [{"bucket": "two-suites", "suites": "SuiteFirst SuiteSecond"}]},
        )

    def test_load_bucket_when(self):
        schedule = self.schedule
        whens = [
            {"scheduled": "nightly"},
            {"scheduled": "weekly", "labeled": ["l"]},
            {"scheduled": "never", "pushes": True, "dispatches": False, "impacted": False},
            {},
        ]
        self.assertEqual(
            [schedule.load_when(when) for when in whens],
            [
                schedule.ScheduleFilter(scheduled=schedule.Schedule.nightly),
                schedule.ScheduleFilter(scheduled=schedule.Schedule.weekly, labeled=["l"]),
                schedule.ScheduleFilter(
                    scheduled=schedule.Schedule.never, pushes=True, dispatches=False, impacted=False),
                schedule.ScheduleFilter(),
            ],
        )

    def test_build_matrix_full(self):
        buckets = [
            ("every-event", [("SuiteEveryEventOne", ()), ("SuiteEveryEventTwo", ())]),
            ("nightly-only", [("SuiteNightlyOnly", ())], {"scheduled": "nightly"}),
            ("weekly-only", [("SuiteWeeklyOnly", ())], {"scheduled": "weekly"}),
        ]

        def matrix_json(github_context):
            matrix = build_matrix(buckets, self.schedule, github_context, lambda suite, legacy_suites: True)
            return json.dumps(matrix, indent=2)

        self.assertEqual(
            matrix_json(build_github_context("schedule", NIGHTLY_CRON, None)),
            textwrap.dedent("""\
                {
                  "include": [
                    {
                      "bucket": "every-event",
                      "suites": "SuiteEveryEventOne SuiteEveryEventTwo"
                    },
                    {
                      "bucket": "nightly-only",
                      "suites": "SuiteNightlyOnly"
                    }
                  ]
                }"""),
        )
        self.assertEqual(
            matrix_json(build_github_context("schedule", WEEKLY_CRON, None)),
            textwrap.dedent("""\
                {
                  "include": [
                    {
                      "bucket": "every-event",
                      "suites": "SuiteEveryEventOne SuiteEveryEventTwo"
                    },
                    {
                      "bucket": "weekly-only",
                      "suites": "SuiteWeeklyOnly"
                    }
                  ]
                }"""),
        )
        self.assertEqual(
            matrix_json(build_github_context("pull_request", "", labels("x"))),
            textwrap.dedent("""\
                {
                  "include": [
                    {
                      "bucket": "every-event",
                      "suites": "SuiteEveryEventOne SuiteEveryEventTwo"
                    }
                  ]
                }"""),
        )

    def test_matrix_for_events(self):
        def matrix(event_name, event_schedule="", pr_labels=None):
            context = build_github_context(event_name, event_schedule, pr_labels)
            return build_matrix(self.TEST_BUCKETS, self.schedule, context, lambda suite, legacy_suites: True)

        every_event = {"bucket": "every-event", "suites": "SuiteEveryEvent"}
        nightly = {"bucket": "nightly", "suites": "SuiteNightly"}
        weekly = {"bucket": "weekly", "suites": "SuiteWeekly"}
        nightly_no_dispatch = {"bucket": "nightly-no-dispatch", "suites": "SuiteNightlyNoDispatch"}
        nightly_or_label = {"bucket": "nightly-or-label", "suites": "SuiteNightlyOrLabel"}

        self.assertEqual(
            matrix("schedule", NIGHTLY_CRON),
            {"include": [every_event, nightly, nightly_no_dispatch, nightly_or_label]},
        )
        self.assertEqual(matrix("schedule", WEEKLY_CRON), {"include": [every_event, weekly]})
        self.assertEqual(matrix("pull_request"), {"include": [every_event]})
        self.assertEqual(
            matrix("pull_request", pr_labels=labels("databricks")),
            {"include": [every_event, nightly_or_label]},
        )

    def test_matrix_json_as_passed_to_workflow(self):
        def matrix_json(event_name, event_schedule="", selected=True, **dumps_args):
            context = build_github_context(event_name, event_schedule, None)
            matrix = build_matrix(self.TEST_BUCKETS, self.schedule, context, lambda suite, legacy_suites: selected)
            return json.dumps(matrix, **dumps_args)

        self.assertEqual(
            matrix_json("schedule", NIGHTLY_CRON, indent=2),
            textwrap.dedent("""\
                {
                  "include": [
                    {
                      "bucket": "every-event",
                      "suites": "SuiteEveryEvent"
                    },
                    {
                      "bucket": "nightly",
                      "suites": "SuiteNightly"
                    },
                    {
                      "bucket": "nightly-no-dispatch",
                      "suites": "SuiteNightlyNoDispatch"
                    },
                    {
                      "bucket": "nightly-or-label",
                      "suites": "SuiteNightlyOrLabel"
                    }
                  ]
                }"""),
        )
        # the workflow receives the compact form
        self.assertEqual(
            matrix_json("pull_request"),
            '{"include": [{"bucket": "every-event", "suites": "SuiteEveryEvent"}]}',
        )
        # ci.yml skips the pt-junit job when the matrix is exactly '{}'
        self.assertEqual(matrix_json("schedule", NIGHTLY_CRON, selected=False), "{}")

    def test_bucket_definitions_are_valid(self):
        names = [bucket[0] for bucket in BUCKETS]
        self.assertEqual(len(names), len(set(names)), "duplicate bucket names")
        for bucket in BUCKETS:
            self.assertIn(len(bucket), (2, 3), bucket[0])
            if len(bucket) == 3:
                # fails if the `when` is not valid according to the schedule config loader
                self.schedule.load_when(bucket[2])


if __name__ == "__main__":
    main()
