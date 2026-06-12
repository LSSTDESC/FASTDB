#!/usr/bin/env python3

"""
Quiet WAL archive + standby replay verification for FASTDB Helm/Kind.

Default output:

  PASS pods
  PASS archive_settings
  PASS buckets
  PASS primary_write_and_archive
  PASS second_write_and_archive
  PASS standby_replay
  PASS cleanup
  SUMMARY passed=7 failed=0 skipped=0

What this proves:
  1. Required pods exist.
  2. Primary PostgreSQL has WAL archiving enabled.
  3. MinIO WAL bucket(s) exist.
  4. A unique real table is created on the primary.
  5. Row count on primary becomes 1 after the first insert.
  6. WAL object count increases after the first forced WAL switch.
  7. Row count on primary becomes 2 after the second insert.
  8. WAL object count increases again after the second forced WAL switch.
  9. Standby is in recovery.
 10. Standby sees the same unique table with 2 rows.
 11. Test table is dropped from the primary by default.
"""

from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from typing import Callable


@dataclass
class TestResult:
    name: str
    status: str
    message: str = ""


@dataclass
class Context:
    args: argparse.Namespace
    table_name: str
    table_created: bool = False

    pg_pod: str | None = None
    site1_pod: str | None = None
    site2_pod: str | None = None
    standby_pod: str | None = None

    before_archived_count: int | None = None


class CheckError(RuntimeError):
    pass


class SkipTest(RuntimeError):
    pass


def vprint(ctx: Context, message: str) -> None:
    if ctx.args.verbose:
        print(message)


def shell_quote(value: str) -> str:
    return shlex.quote(value)


def run_cmd(ctx: Context, cmd: list[str], *, check: bool = True) -> str:
    if ctx.args.verbose:
        print("+ " + " ".join(cmd), file=sys.stderr)

    try:
        proc = subprocess.run(
            cmd,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=ctx.args.command_timeout,
        )
    except subprocess.TimeoutExpired as exc:
        raise CheckError(
            f"command timed out after {ctx.args.command_timeout}s: {' '.join(cmd)}"
        ) from exc

    if check and proc.returncode != 0:
        detail = proc.stderr.strip() or proc.stdout.strip() or f"exit code {proc.returncode}"
        raise CheckError(detail)

    if ctx.args.verbose and proc.stderr.strip():
        print(proc.stderr.strip(), file=sys.stderr)

    return proc.stdout.strip()


def kubectl(ctx: Context, kubectl_args: list[str], *, check: bool = True) -> str:
    return run_cmd(
        ctx,
        ["kubectl", "-n", ctx.args.namespace, *kubectl_args],
        check=check,
    )


def get_pod(ctx: Context, label: str, *, required: bool = True) -> str | None:
    pod = kubectl(
        ctx,
        ["get", "pods", "-l", label, "-o", "jsonpath={.items[0].metadata.name}"],
        check=required,
    ).strip()

    if required and not pod:
        raise CheckError(f"no pod found for label {label}")

    return pod or None


def psql(
    ctx: Context,
    pod: str,
    sql: str,
    *,
    database: str | None = None,
    user: str | None = None,
) -> str:
    db = database or ctx.args.database
    pg_user = user or ctx.args.postgres_user

    return kubectl(
        ctx,
        [
            "exec",
            pod,
            "--",
            "psql",
            "-v",
            "ON_ERROR_STOP=1",
            "-U",
            pg_user,
            "-d",
            db,
            "-At",
            "-c",
            sql,
        ],
    )


def mc_command(ctx: Context) -> str:
    return "mc --insecure" if ctx.args.insecure else "mc"


def minio_exec(ctx: Context, pod: str, script: str) -> str:
    return kubectl(ctx, ["exec", pod, "--", "sh", "-lc", script])


def minio_alias_script(ctx: Context, alias: str, endpoint: str) -> str:
    mc = mc_command(ctx)

    return f"""
set -eu
export HOME=/tmp
export MC_CONFIG_DIR=/tmp/.mc
mkdir -p /tmp/.mc
{mc} alias set {shell_quote(alias)} {shell_quote(endpoint)} {shell_quote(ctx.args.access_key)} {shell_quote(ctx.args.secret_key)} >/dev/null
"""


def minio_bucket_exists(ctx: Context, pod: str, alias: str, endpoint: str, bucket: str) -> None:
    mc = mc_command(ctx)

    script = (
        minio_alias_script(ctx, alias, endpoint)
        + f"""
{mc} ls {shell_quote(alias + "/" + bucket)} >/dev/null
"""
    )

    minio_exec(ctx, pod, script)


def parse_mc_json_lines(output: str) -> set[str]:
    keys: set[str] = set()

    for line in output.splitlines():
        line = line.strip()
        if not line:
            continue

        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue

        key = record.get("key")
        if key and not key.endswith("/"):
            keys.add(key)

    return keys


def minio_list(
    ctx: Context,
    pod: str,
    alias: str,
    endpoint: str,
    bucket: str,
    prefix: str,
) -> set[str]:
    mc = mc_command(ctx)
    path = f"{alias}/{bucket}/{prefix}"

    script = (
        minio_alias_script(ctx, alias, endpoint)
        + f"""
{mc} ls --json --recursive {shell_quote(path)} 2>/dev/null || true
"""
    )

    output = minio_exec(ctx, pod, script)
    return parse_mc_json_lines(output)


def list_wal_objects(ctx: Context) -> tuple[set[str], set[str]]:
    if not ctx.site1_pod:
        raise CheckError("site1 pod not set")

    site1 = minio_list(
        ctx,
        ctx.site1_pod,
        "site1",
        ctx.args.site1_endpoint,
        ctx.args.bucket,
        ctx.args.repo1_prefix,
    )

    if ctx.args.skip_site2:
        return site1, set()

    if not ctx.site2_pod:
        raise CheckError("site2 pod not set")

    site2 = minio_list(
        ctx,
        ctx.site2_pod,
        "site2",
        ctx.args.site2_endpoint,
        ctx.args.bucket,
        ctx.args.repo2_prefix,
    )

    return site1, site2


def wait_for_wal_object_count_increase(
    ctx: Context,
    before_site1: set[str],
    before_site2: set[str],
) -> tuple[set[str], set[str]]:
    deadline = time.time() + ctx.args.timeout
    last_site1: set[str] = set()
    last_site2: set[str] = set()

    while time.time() < deadline:
        current_site1, current_site2 = list_wal_objects(ctx)

        last_site1 = current_site1
        last_site2 = current_site2

        site1_increased = len(current_site1) > len(before_site1)

        if ctx.args.skip_site2:
            if site1_increased:
                return current_site1, current_site2
        else:
            site2_increased = len(current_site2) > len(before_site2)
            if site1_increased and site2_increased:
                return current_site1, current_site2

        time.sleep(ctx.args.poll)

    if ctx.args.skip_site2:
        raise CheckError(
            f"repo1 WAL object count did not increase within {ctx.args.timeout}s "
            f"(before={len(before_site1)}, after={len(last_site1)})"
        )

    raise CheckError(
        f"WAL object count did not increase within {ctx.args.timeout}s "
        f"(repo1 before={len(before_site1)}, after={len(last_site1)}; "
        f"repo2 before={len(before_site2)}, after={len(last_site2)})"
    )


def get_archiver_status(ctx: Context) -> dict[str, str | int]:
    if not ctx.pg_pod:
        raise CheckError("postgres pod not set")

    sql = """
SELECT archived_count || '|' ||
       failed_count || '|' ||
       COALESCE(last_archived_wal, '') || '|' ||
       COALESCE(last_failed_wal, '')
FROM pg_stat_archiver;
"""

    output = psql(ctx, ctx.pg_pod, sql)
    parts = output.split("|")

    if len(parts) != 4:
        raise CheckError(f"could not parse pg_stat_archiver output: {output!r}")

    return {
        "archived_count": int(parts[0]),
        "failed_count": int(parts[1]),
        "last_archived_wal": parts[2],
        "last_failed_wal": parts[3],
    }


def force_archive_switch(ctx: Context) -> None:
    if not ctx.pg_pod:
        raise CheckError("postgres pod not set")

    psql(ctx, ctx.pg_pod, "SELECT pg_switch_wal();")


def primary_row_count(ctx: Context) -> int:
    if not ctx.pg_pod:
        raise CheckError("postgres pod not set")

    output = psql(ctx, ctx.pg_pod, f"SELECT count(*) FROM public.{ctx.table_name};")

    try:
        return int(output)
    except ValueError as exc:
        raise CheckError(f"could not parse primary row count: {output!r}") from exc


def standby_row_count(ctx: Context) -> int:
    if not ctx.standby_pod:
        raise CheckError("standby pod not set")

    output = psql(ctx, ctx.standby_pod, f"SELECT count(*) FROM public.{ctx.table_name};")

    try:
        return int(output)
    except ValueError as exc:
        raise CheckError(f"could not parse standby row count: {output!r}") from exc


def create_table_and_insert_row(ctx: Context) -> None:
    if not ctx.pg_pod:
        raise CheckError("postgres pod not set")

    sql = f"""
DROP TABLE IF EXISTS public.{ctx.table_name};

CREATE TABLE public.{ctx.table_name} (
    id serial primary key,
    created_at timestamptz default now()
);

INSERT INTO public.{ctx.table_name} DEFAULT VALUES;
"""

    psql(ctx, ctx.pg_pod, sql)
    ctx.table_created = True

    count = primary_row_count(ctx)
    if count != 1:
        raise CheckError(f"primary row count after first insert is {count}, expected 1")

    force_archive_switch(ctx)


def insert_second_row(ctx: Context) -> None:
    if not ctx.pg_pod:
        raise CheckError("postgres pod not set")

    psql(ctx, ctx.pg_pod, f"INSERT INTO public.{ctx.table_name} DEFAULT VALUES;")

    count = primary_row_count(ctx)
    if count != 2:
        raise CheckError(f"primary row count after second insert is {count}, expected 2")

    force_archive_switch(ctx)


def drop_test_table_on_primary(ctx: Context) -> None:
    if not ctx.pg_pod:
        raise CheckError("postgres pod not set")

    psql(ctx, ctx.pg_pod, f"DROP TABLE IF EXISTS public.{ctx.table_name};")
    force_archive_switch(ctx)


def test_pods(ctx: Context) -> None:
    ctx.pg_pod = get_pod(ctx, ctx.args.postgres_label)
    ctx.site1_pod = get_pod(ctx, ctx.args.minio_site1_label)

    if not ctx.args.skip_site2:
        ctx.site2_pod = get_pod(ctx, ctx.args.minio_site2_label)

    if not ctx.args.skip_standby:
        ctx.standby_pod = get_pod(ctx, ctx.args.standby_label, required=False)
        if not ctx.standby_pod:
            raise CheckError("no standby pod found; use --skip-standby to ignore")

    vprint(ctx, f"postgres pod: {ctx.pg_pod}")
    vprint(ctx, f"site1 pod: {ctx.site1_pod}")
    vprint(ctx, f"site2 pod: {ctx.site2_pod}")
    vprint(ctx, f"standby pod: {ctx.standby_pod}")


def test_archive_settings(ctx: Context) -> None:
    if not ctx.pg_pod:
        raise CheckError("postgres pod not set")

    archive_mode = psql(ctx, ctx.pg_pod, "SHOW archive_mode;")
    archive_command = psql(ctx, ctx.pg_pod, "SHOW archive_command;")

    if archive_mode != "on":
        raise CheckError(f"archive_mode is {archive_mode!r}, expected 'on'")

    if not archive_command or archive_command == "(disabled)":
        raise CheckError("archive_command is not set")

    status = get_archiver_status(ctx)
    ctx.before_archived_count = int(status["archived_count"])

    vprint(ctx, f"archive_mode: {archive_mode}")
    vprint(ctx, f"archive_command: {archive_command}")
    vprint(ctx, f"archiver before: {status}")


def test_buckets(ctx: Context) -> None:
    if not ctx.site1_pod:
        raise CheckError("site1 pod not set")

    minio_bucket_exists(
        ctx,
        ctx.site1_pod,
        "site1",
        ctx.args.site1_endpoint,
        ctx.args.bucket,
    )

    if not ctx.args.skip_site2:
        if not ctx.site2_pod:
            raise CheckError("site2 pod not set")

        minio_bucket_exists(
            ctx,
            ctx.site2_pod,
            "site2",
            ctx.args.site2_endpoint,
            ctx.args.bucket,
        )

    site1, site2 = list_wal_objects(ctx)

    vprint(ctx, f"initial repo1 WAL object count: {len(site1)}")
    if not ctx.args.skip_site2:
        vprint(ctx, f"initial repo2 WAL object count: {len(site2)}")


def test_primary_write_and_archive(ctx: Context) -> None:
    before_site1, before_site2 = list_wal_objects(ctx)

    create_table_and_insert_row(ctx)

    after_site1, after_site2 = wait_for_wal_object_count_increase(
        ctx,
        before_site1,
        before_site2,
    )

    vprint(ctx, f"created table: public.{ctx.table_name}")
    vprint(ctx, "primary row count: 1")
    vprint(ctx, f"repo1 WAL objects: {len(before_site1)} -> {len(after_site1)}")
    if not ctx.args.skip_site2:
        vprint(ctx, f"repo2 WAL objects: {len(before_site2)} -> {len(after_site2)}")


def test_second_write_and_archive(ctx: Context) -> None:
    before_site1, before_site2 = list_wal_objects(ctx)

    insert_second_row(ctx)

    after_site1, after_site2 = wait_for_wal_object_count_increase(
        ctx,
        before_site1,
        before_site2,
    )

    after_status = get_archiver_status(ctx)
    after_archived_count = int(after_status["archived_count"])

    if ctx.before_archived_count is not None:
        if after_archived_count <= ctx.before_archived_count:
            raise CheckError("pg_stat_archiver archived_count did not increase")

    vprint(ctx, "primary row count: 2")
    vprint(ctx, f"repo1 WAL objects: {len(before_site1)} -> {len(after_site1)}")
    if not ctx.args.skip_site2:
        vprint(ctx, f"repo2 WAL objects: {len(before_site2)} -> {len(after_site2)}")
    vprint(ctx, f"archiver after: {after_status}")


def test_standby_replay(ctx: Context) -> None:
    if ctx.args.skip_standby:
        raise SkipTest("standby skipped by request")

    if not ctx.standby_pod:
        raise CheckError("standby pod not set")

    in_recovery = psql(ctx, ctx.standby_pod, "SELECT pg_is_in_recovery();")
    if in_recovery != "t":
        raise CheckError(f"standby pg_is_in_recovery() returned {in_recovery!r}")

    deadline = time.time() + ctx.args.timeout
    last_table_exists = "f"
    last_count = 0

    while time.time() < deadline:
        table_exists = psql(
            ctx,
            ctx.standby_pod,
            f"SELECT to_regclass('public.{ctx.table_name}') IS NOT NULL;",
        )
        last_table_exists = table_exists

        if table_exists == "t":
            last_count = standby_row_count(ctx)

            if last_count == 2:
                vprint(ctx, f"standby table: public.{ctx.table_name}")
                vprint(ctx, "standby row count: 2")
                return

            if last_count > 2:
                raise CheckError(f"standby row count is {last_count}, expected 2")

        time.sleep(ctx.args.poll)

    raise CheckError(
        f"standby did not reach expected row count within {ctx.args.timeout}s "
        f"(table_exists={last_table_exists}, row_count={last_count}, expected=2)"
    )


def test_cleanup(ctx: Context) -> None:
    if ctx.args.no_cleanup:
        raise SkipTest("cleanup disabled by --no-cleanup")

    if not ctx.table_created:
        raise SkipTest("test table was not created")

    drop_test_table_on_primary(ctx)

    vprint(ctx, f"dropped table on primary: public.{ctx.table_name}")


def print_result(result: TestResult) -> None:
    if result.message:
        print(f"{result.status} {result.name}: {result.message}")
    else:
        print(f"{result.status} {result.name}")


def run_test(ctx: Context, name: str, func: Callable[[Context], None]) -> TestResult:
    try:
        func(ctx)
        return TestResult(name=name, status="PASS")
    except SkipTest as exc:
        return TestResult(name=name, status="SKIP", message=str(exc))
    except Exception as exc:
        return TestResult(name=name, status="FAIL", message=str(exc))


def make_context(args: argparse.Namespace) -> Context:
    run_id = uuid.uuid4().hex[:12]
    timestamp = time.strftime("%Y%m%d%H%M%S", time.gmtime())
    table_name = f"wal_kind_check_{timestamp}_{run_id}"

    return Context(
        args=args,
        table_name=table_name,
    )


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Quiet WAL archive + standby replay verification for FASTDB Kind/Helm"
    )

    parser.add_argument("-n", "--namespace", default="fastdb-local")

    parser.add_argument("--postgres-label", default="app=postgres")
    parser.add_argument("--standby-label", default="app=postgres-standby")
    parser.add_argument("--minio-site1-label", default="app=minio-site1")
    parser.add_argument("--minio-site2-label", default="app=minio-site2")

    parser.add_argument("--site1-endpoint", default="http://localhost:9000")
    parser.add_argument("--site2-endpoint", default="http://localhost:9000")

    parser.add_argument("--access-key", default="minioadmin")
    parser.add_argument("--secret-key", default="minioadmin")
    parser.add_argument("--bucket", default="fastdb-wal")

    parser.add_argument("--repo1-prefix", default="repo1/archive/fastdb/")
    parser.add_argument("--repo2-prefix", default="repo2/archive/fastdb/")

    parser.add_argument("--database", default="fastdb")
    parser.add_argument("--postgres-user", default="postgres")

    parser.add_argument("--timeout", type=int, default=90)
    parser.add_argument("--poll", type=int, default=2)
    parser.add_argument("--command-timeout", type=int, default=30)

    parser.add_argument("--insecure", action="store_true")
    parser.add_argument("--skip-site2", action="store_true")
    parser.add_argument("--skip-standby", action="store_true")
    parser.add_argument("--no-cleanup", action="store_true")
    parser.add_argument("--verbose", "-v", action="store_true")

    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    ctx = make_context(args)

    main_tests: list[tuple[str, Callable[[Context], None]]] = [
        ("pods", test_pods),
        ("archive_settings", test_archive_settings),
        ("buckets", test_buckets),
        ("primary_write_and_archive", test_primary_write_and_archive),
        ("second_write_and_archive", test_second_write_and_archive),
        ("standby_replay", test_standby_replay),
    ]

    results: list[TestResult] = []

    for name, func in main_tests:
        result = run_test(ctx, name, func)
        results.append(result)
        print_result(result)

        if result.status == "FAIL":
            break

    cleanup_result = run_test(ctx, "cleanup", test_cleanup)
    results.append(cleanup_result)
    print_result(cleanup_result)

    passed = sum(1 for result in results if result.status == "PASS")
    failed = sum(1 for result in results if result.status == "FAIL")
    skipped = sum(1 for result in results if result.status == "SKIP")

    print(f"SUMMARY passed={passed} failed={failed} skipped={skipped}")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))