import argparse


def main():
    parser = argparse.ArgumentParser(
        description="Аналитика вакансий из RustFS"
    )
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("prepare")
    p.add_argument("--csv", required=True)
    p.add_argument("--archive", required=True)
    p.add_argument("--extra", required=True)
    p = sub.add_parser("count")
    p.add_argument(
        "--engine", choices=["baseline", "combined", "duckdb"], required=True
    )
    p.add_argument("--cap", type=int, required=True)
    p.add_argument("--workers", type=int, default=2)
    p.add_argument("--chunk-size", type=int, default=2000)
    p.add_argument("--inject-failure", action="store_true")
    p.add_argument("--output")
    p.add_argument("--no-upload", action="store_true")
    for name in (
        "verify",
        "analyze",
        "benchmark",
        "evidence",
        "failure-test",
        "audit",
        "load",
        "run",
    ):
        sub.add_parser(name)
    args = parser.parse_args()
    if args.command == "prepare":
        from hh_analytics.processing.prepare import prepare

        prepare(args.csv, args.archive, args.extra)
    elif args.command == "count":
        if args.cap < 0 or args.workers < 1 or args.chunk_size < 1:
            parser.error("cap >= 0, workers >= 1, chunk-size >= 1")
        if args.engine == "duckdb":
            from hh_analytics.processing.duckdb import run

            run(args.cap, output=args.output, publish=not args.no_upload)
        else:
            from hh_analytics.processing.mapreduce import run

            run(
                args.cap,
                mode=args.engine,
                workers=args.workers,
                chunk_size=args.chunk_size,
                inject_failure=args.inject_failure,
                output=args.output,
                publish=not args.no_upload,
            )
    else:
        from importlib import import_module

        commands = {
            "load": ("processing.prepare", "load"),
            "verify": ("validation", "run"),
            "failure-test": ("validation", "check_failure"),
            "analyze": ("analysis.salaries", "run"),
            "audit": ("analysis.validation", "run"),
            "benchmark": ("benchmark", "run"),
            "evidence": ("infrastructure", "run"),
        }
        names = [args.command]

        if args.command == "run":
            names = [
                "benchmark",
                "verify",
                "failure-test",
                "analyze",
                "audit",
                "evidence",
            ]

        for name in names:
            module, function = commands[name]
            getattr(import_module(f"hh_analytics.{module}"), function)()


if __name__ == "__main__":
    main()
