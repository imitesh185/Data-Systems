"""SparkNerve command line (what the generated Airflow tasks execute).

    python -m sparknerve validate-metadata
    python -m sparknerve plan --pipeline retail_sales
    python -m sparknerve run --pipeline retail_sales --table orders --stage extract --run-id manual__1
    python -m sparknerve run --pipeline retail_sales --table orders --stage validate --run-id manual__1
    python -m sparknerve run ... --stage validate --accept-threshold 1.0   # accept a tripped batch
    python -m sparknerve report --pipeline retail_sales --run-id manual__1
    python -m sparknerve reset --pipeline retail_sales --table orders --yes   # rebuild silver/quarantine from bronze
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone

from sparknerve.metadata import MetadataError, load_pipelines
from sparknerve.planner import EXTRACT, VALIDATE, build_plan, cli_command
from sparknerve.settings import Settings


def _validate_metadata(args) -> int:
    try:
        pipelines = load_pipelines(args.metadata_dir)
    except MetadataError as exc:
        print(f"INVALID metadata:\n{exc}", file=sys.stderr)
        return 1
    for pipeline in pipelines.values():
        rules = sum(len(t.rules) for t in pipeline.tables)
        tasks = len(build_plan(pipeline).tasks)
        print(f"OK  {pipeline.name}: {len(pipeline.tables)} tables, {rules} rules, {tasks} DAG tasks "
              f"({pipeline.source_path})")
    return 0


def _plan(args) -> int:
    pipeline = load_pipelines(args.metadata_dir)[args.pipeline]
    for task in build_plan(pipeline).topological():
        upstream = ", ".join(task.upstream) or "-"
        print(f"{task.task_id:24} <- {upstream}\n    {cli_command(pipeline.name, task, args.run_id)}")
    return 0


def _run(args) -> int:
    from sparknerve.spark.session import build_spark

    pipeline = load_pipelines(args.metadata_dir)[args.pipeline]
    spec = pipeline.table(args.table)
    settings = Settings.from_env()
    spark = build_spark(f"sparknerve.{pipeline.name}.{spec.name}.{args.stage}")
    try:
        if args.stage == EXTRACT:
            from sparknerve.spark.extract import run_extract

            runs = [run_extract(spark, pipeline, spec, args.run_id, settings)]
        else:
            from sparknerve.spark.validate import run_validate

            runs = run_validate(spark, pipeline, spec, args.run_id, settings, threshold=args.accept_threshold)
    finally:
        spark.stop()
    for run in runs:
        print(run.summary())
    return 0 if all(r.succeeded for r in runs) else 1


def _report(args) -> int:
    from sparknerve.spark.report import run_report
    from sparknerve.spark.session import build_spark

    pipeline = load_pipelines(args.metadata_dir)[args.pipeline]
    spark = build_spark(f"sparknerve.{pipeline.name}.report", with_jdbc=False)
    try:
        run = run_report(spark, pipeline, args.run_id, Settings.from_env())
    finally:
        spark.stop()
    print(run.summary())
    return 0 if run.succeeded else 1


def _reset(args) -> int:
    """Drop silver, quarantine and the validate checkpoint of one table, so the next
    run rebuilds them from bronze (bronze and the extract watermark are kept)."""
    if not args.yes:
        print("Refusing to delete without --yes", file=sys.stderr)
        return 2
    from sparknerve.spark.session import build_spark

    pipeline = load_pipelines(args.metadata_dir)[args.pipeline]
    lake = Settings.from_env().lake
    spark = build_spark(f"sparknerve.{pipeline.name}.{args.table}.reset", with_jdbc=False)
    try:
        jvm = spark.sparkContext._jvm
        conf = spark.sparkContext._jsc.hadoopConfiguration()
        for path in (lake.silver(pipeline.name, args.table), lake.quarantine(pipeline.name, args.table),
                     lake.checkpoint(pipeline.name, args.table, VALIDATE)):
            target = jvm.org.apache.hadoop.fs.Path(path)
            deleted = target.getFileSystem(conf).delete(target, True)
            print(f"{'deleted' if deleted else 'absent '} {path}")
    finally:
        spark.stop()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m sparknerve", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--metadata-dir", default=None,
                        help="defaults to $SPARKNERVE_METADATA_DIR or ./metadata/pipelines")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("validate-metadata", help="check every pipeline file (JSON Schema + semantic checks)")
    plan = sub.add_parser("plan", help="print the task graph a pipeline generates")
    plan.add_argument("--pipeline", required=True)
    plan.add_argument("--run-id", default="{{ run_id }}")

    run = sub.add_parser("run", help="run one stage of one table")
    run.add_argument("--pipeline", required=True)
    run.add_argument("--table", required=True)
    run.add_argument("--stage", required=True, choices=[EXTRACT, VALIDATE])
    run.add_argument("--run-id", default=f"manual__{datetime.now(timezone.utc):%Y-%m-%dT%H:%M:%S}")
    run.add_argument("--accept-threshold", type=float, default=None,
                     help="override the quarantine threshold for this run (accept a tripped batch)")

    report = sub.add_parser("report", help="summarise a run and fail if any table is unhealthy")
    report.add_argument("--pipeline", required=True)
    report.add_argument("--run-id", required=True)

    reset = sub.add_parser("reset", help="rebuild one table's silver + quarantine from bronze")
    reset.add_argument("--pipeline", required=True)
    reset.add_argument("--table", required=True)
    reset.add_argument("--yes", action="store_true")

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    handlers = {"validate-metadata": _validate_metadata, "plan": _plan, "run": _run, "report": _report,
                "reset": _reset}
    return handlers[args.command](args)


if __name__ == "__main__":
    raise SystemExit(main())
