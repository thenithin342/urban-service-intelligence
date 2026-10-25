"""Export Member 1's validated forecast inputs for Member 3's priority engine."""

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.analytics.priority_features import build_handoff
from src.ingestion.downloader import sha256_file
from src.ingestion.history import save_json
from src.modeling.demand import load_demand_config
from src.modeling.demand_plots import write_plots
from src.utils.spark import get_spark


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    cfg = load_demand_config(args.config)
    out = ROOT / cfg["output_dir"]
    model_meta = json.loads((out / "model_manifest.json").read_text())
    coverage = model_meta["coverage"]
    if coverage["source_manifest_sha256"] != sha256_file(ROOT / cfg["source_manifest"]):
        raise ValueError("source provenance changed since training")
    if model_meta["producer_sha256"] != sha256_file(ROOT / "src/modeling/demand.py"):
        raise ValueError("demand producer changed; rebuild features and train before exporting handoff")
    spark = get_spark("member1-priority-handoff")
    try:
        result = build_handoff(spark.read.parquet(str(out / "daily")), spark.read.parquet(str(out / "future")),
                               spark.read.parquet(str(ROOT / cfg["input_parquet"])), coverage)
        result.write.mode("overwrite").parquet(str(out / "priority_features"))
        rows = result.orderBy("borough", "complaint_type").toJSON().collect()
        save_json(out / "handoff_manifest.json", {"rows": len(rows), "source_manifest_sha256": coverage["source_manifest_sha256"],
                  "schema": result.schema.jsonValue(), "scope": "borough totals plus eligible categories; categories overlap ALL; no priority weights or ranks"})
        write_plots(spark, out, json.loads((ROOT / cfg["quality_report"]).read_text()))
        print("\n".join(rows))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
