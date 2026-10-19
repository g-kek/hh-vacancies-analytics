import json
import platform
import subprocess
from datetime import datetime
from zoneinfo import ZoneInfo

from hh_analytics.config import ROOT, settings
from hh_analytics.storage import bucket, s3_client, upload
from hh_analytics.utils import save_json


def capture(args):
    process = subprocess.run(args, cwd=ROOT, capture_output=True, text=True)
    if process.returncode:
        raise RuntimeError(process.stderr)
    return process.stdout.strip()


def run():
    client = s3_client()
    cfg = settings()
    pages = client.get_paginator("list_objects_v2").paginate(Bucket=bucket())
    objects = []
    for page in pages:
        for obj in page.get("Contents", []):
            objects.append(
                {
                    "key": obj["Key"],
                    "bytes": obj["Size"],
                    "etag": obj["ETag"],
                    "modified_at": obj["LastModified"].isoformat(),
                }
            )
    evidence = {
        "captured_at": datetime.now(ZoneInfo("Europe/Moscow")).isoformat(),
        "docker_ps": capture(
            [
                "docker",
                "ps",
                "--filter",
                "name=hh-rustfs",
                "--format",
                "{{.Names}} | {{.Image}} | {{.Status}} | {{.Ports}}",
            ]
        ),
        "container_image": capture(
            ["docker", "inspect", "hh-rustfs", "--format", "{{.Config.Image}}"]
        ),
        "docker_resources": capture(
            [
                "docker",
                "info",
                "--format",
                "{{.NCPU}} CPUs, {{.MemTotal}} bytes RAM",
            ]
        ),
        "rustfs_resources_snapshot": capture(
            [
                "docker",
                "stats",
                "--no-stream",
                "--format",
                "{{.Name}} | {{.CPUPerc}} | {{.MemUsage}}",
                "hh-rustfs",
            ]
        ),
        "health": json.loads(
            capture(
                ["curl", "--fail", "--silent", "http://127.0.0.1:9000/health"]
            )
        ),
        "bucket": bucket(),
        "objects": objects,
        "platform": platform.platform(),
    }

    checks = []
    from hh_analytics.validation import compare

    for cap in cfg["stack_limits"]:
        key = f"results/{cfg['variant']}/counts/mapreduce_combined_k{cap}.json"
        if key not in {r["key"] for r in objects}:
            continue
        remote = json.loads(
            client.get_object(Bucket=bucket(), Key=key)["Body"].read()
        )
        local = json.loads(
            (
                ROOT / f"results/counts/mapreduce_combined_k{cap}.json"
            ).read_text()
        )
        checks.append({"key": key, "sha256_counts": compare(local, remote)})
    evidence["roundtrip_checks"] = checks
    save_json(ROOT / "results/infrastructure.json", evidence)
    lines = [
        f"$ docker ps --filter name=hh-rustfs\n{evidence['docker_ps']}",
        f"$ curl -fsS http://127.0.0.1:9000/health\n{json.dumps(evidence['health'])}",
        "$ hh evidence",
        *[f"s3://{bucket()}/{r['key']}  {r['bytes']} bytes" for r in objects],
        f"Roundtrip checks: {len(checks)}",
    ]
    (ROOT / "results/commands.txt").write_text("\n\n".join(lines) + "\n")
    upload(
        ROOT / "results/infrastructure.json",
        f"results/{cfg['variant']}/infrastructure.json",
    )
    print("S3 проверен", flush=True)
