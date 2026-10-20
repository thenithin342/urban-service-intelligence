"""Member 1 Phase 3 CLI: distributed EDA, geospatial + clustering study.

Reads the Phase 2 cleaned Parquet and writes gitignored artifacts under
``artifacts/member1/analytics`` (tables/, figures/, manifest, metrics).

Usage:
  python scripts/run_member1_analytics.py --config configs/sample.yaml
Optional:
  --input <parquet>   --outdir <dir>   --no-figures   --master local[2]
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pyspark.sql import functions as F

from src.analytics import clustering, figures, geospatial, spark_eda
from src.analytics.member1_config import member1_config
from src.utils.config import load_config, resolve_path
from src.utils.spark import ensure_worker_env, get_spark


def _write_table(sdf, path: Path) -> dict:
    path.parent.mkdir(parents=True, exist_ok=True)
    sdf.coalesce(1).write.mode("overwrite").parquet(str(path))
    return {
        "path": str(path),
        "rows": int(sdf.count()),
        "columns": [f"{f.name}:{f.dataType.simpleString()}" for f in sdf.schema],
    }


def _fig(name: str, result: tuple, notes: dict, meta: dict) -> None:
    path, takeaway = result
    if path is not None:
        meta["figures"][name] = {"path": path, "takeaway": takeaway}
        notes[name] = takeaway


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Member 1 Phase 3 analytics")
    ap.add_argument("--config", required=True, help="Path to YAML config")
    ap.add_argument("--input", default=None, help="Override cleaned Parquet path")
    ap.add_argument("--outdir", default=None, help="Override analytics output dir")
    ap.add_argument("--no-figures", action="store_true", help="Skip figure generation")
    ap.add_argument("--master", default="local[2]")
    args = ap.parse_args(argv)

    ensure_worker_env()
    cfg = load_config(args.config)
    m1 = member1_config(cfg)
    producer = str(m1["producer_version"])
    outdir = (
        Path(args.outdir)
        if args.outdir
        else resolve_path(ROOT, m1["analytics_outdir"])
    )
    tables_dir = outdir / "tables"
    figures_dir = outdir / "figures"
    input_path = (
        Path(args.input)
        if args.input
        else resolve_path(ROOT, cfg.get("etl_output", "data/processed/nyc311_clean"))
    )
    if not input_path.exists():
        print(
            f"[member1] BLOCKER: cleaned Parquet not found at {input_path}. "
            "Run scripts/download_sample.py then scripts/run_etl.py first.",
            flush=True,
        )
        return 3

    spark = get_spark("member1-phase3", master=args.master)
    try:
        print(f"[member1] reading {input_path}", flush=True)
        df = spark_eda.load_clean(spark, str(input_path))
        verify = spark_eda.verify_inputs(df)
        if verify["row_count"] == 0:
            print("[member1] BLOCKER: cleaned table is empty", flush=True)
            return 3

        coverage = spark_eda.time_coverage(df)
        geo_quality = geospatial.geo_quality_summary(df)
        period = {
            "period_start": coverage["period_start"],
            "period_end": coverage["period_end"],
        }

        print("[member1] EDA aggregations", flush=True)
        tables = {
            "complaints_by_borough": spark_eda.complaints_by_borough(df),
            "complaints_by_category": spark_eda.complaints_by_category(df),
            "complaints_by_agency": spark_eda.complaints_by_agency(df),
            "complaints_by_zip": spark_eda.complaints_by_zip(df),
            "borough_category": spark_eda.borough_category(df),
            "complaints_daily": spark_eda.complaints_daily(df),
            "complaints_daily_borough": spark_eda.complaints_daily_borough(df),
            "complaints_weekly": spark_eda.complaints_weekly(df),
            "hour_weekday": spark_eda.hour_weekday_heatmap(df),
            "resolution_by_borough": spark_eda.resolution_by_borough(df),
            "flag_overlap": spark_eda.flag_overlap(df),
        }
        cap = int(m1["max_collect_rows"])
        pand = {k: spark_eda.bounded_pandas(v, cap) for k, v in tables.items()}
        table_meta = {k: _write_table(v, tables_dir / k) for k, v in tables.items()}

        print("[member1] geospatial hotspots", flush=True)
        hot = geospatial.spatial_hotspots(df, m1["grid_cell_deg"], m1["hotspot_min_count"])
        zips = geospatial.zip_hotspots(df, m1["hotspot_min_count"])
        cells = geospatial.cell_features(
            df, m1["grid_cell_deg"], m1["projection_lat0"], m1["projection_lon0"], 1
        )
        table_meta["spatial_hotspots"] = _write_table(hot, tables_dir / "spatial_hotspots")
        table_meta["zip_hotspots"] = _write_table(zips, tables_dir / "zip_hotspots")
        table_meta["spatial_grid_cells"] = _write_table(
            cells.drop("mean_resolution_hours"), tables_dir / "spatial_grid_cells"
        )
        hot_pd = spark_eda.bounded_pandas(hot, cap)
        zips_pd = spark_eda.bounded_pandas(zips, cap)

        print("[member1] clustering study", flush=True)
        n_cells = cells.count()
        ablation = clustering.preprocessing_ablation(
            cells, m1["kmeans_k_values"], m1["kmeans_seed"]
        )
        ablation_sdf = spark.createDataFrame(ablation or [{"variant": "none"}])
        table_meta["kmeans_ablation"] = _write_table(
            ablation_sdf, tables_dir / "kmeans_ablation"
        )
        scored = [a for a in ablation if a.get("silhouette") is not None]
        if scored and n_cells >= 2:
            # Headline variant is the configured, scale-justified one.
            # Absolute silhouette across different feature spaces is not
            # comparable, so the ablation informs the choice rather than
            # overriding it with a numeric maximum.
            best_variant = str(m1["kmeans_variant"])
            use_log, scale = _variant_flags(best_variant)
            frame, feature_col = clustering.build_variant(cells, use_log, scale)
            preds, k_metrics = clustering.select_k(
                frame, feature_col, m1["kmeans_k_values"], m1["kmeans_seed"]
            )
            cluster_pd = clustering.cluster_summary(
                preds, cells, m1["projection_lat0"], m1["projection_lon0"]
            )
            chosen = {
                "variant": best_variant,
                "use_log_count": use_log,
                "scaled": scale,
                "n_cells": int(n_cells),
                "k_metrics": k_metrics,
            }
        else:
            cluster_pd = None
            chosen = {"variant": None, "n_cells": int(n_cells), "k_metrics": []}
        if cluster_pd is not None and len(cluster_pd):
            cs_sdf = spark.createDataFrame(cluster_pd)
            table_meta["cluster_summary"] = _write_table(
                cs_sdf, tables_dir / "cluster_summary"
            )

        print("[member1] density-based comparison (bounded sample)", flush=True)
        sample = geospatial.sample_geo_points(df, int(m1["dbscan_sample_cap"]))
        sample_pd = sample.toPandas()
        dbscan_sensitivity: list[dict] = []
        if len(sample_pd):
            labelled, dbscan_summary = geospatial.dbscan_clusters(
                sample_pd, m1["dbscan_eps_meters"], m1["dbscan_min_pts"]
            )
            for eps in m1["dbscan_eps_grid"]:
                _, sweep = geospatial.dbscan_clusters(
                    sample_pd, eps, m1["dbscan_min_pts"]
                )
                dbscan_sensitivity.append(sweep)
        else:
            labelled, dbscan_summary = None, {"sample_rows": 0}

        notes: dict[str, str] = {}
        meta: dict = {"figures": {}}
        if not args.no_figures:
            print("[member1] figures", flush=True)
            _fig("borough_bar", figures.bar_borough(pand["complaints_by_borough"], figures_dir / "borough_bar.png", period, producer), notes, meta)
            _fig("category_bar", figures.bar_categories(pand["complaints_by_category"], figures_dir / "category_bar.png", period, producer, m1["top_categories"]), notes, meta)
            _fig("daily_timeseries", figures.timeseries_daily(pand["complaints_daily"], figures_dir / "daily_timeseries.png", period, producer), notes, meta)
            _fig("daily_by_borough", figures.daily_lines_by_borough(pand["complaints_daily_borough"], figures_dir / "daily_by_borough.png", period, producer), notes, meta)
            _fig("hour_weekday_heatmap", figures.heatmap_hour_weekday(pand["hour_weekday"], figures_dir / "hour_weekday_heatmap.png", period, producer), notes, meta)
            _fig("daily_kde", figures.kde_daily_volume(pand["complaints_daily"], figures_dir / "daily_kde.png", period, producer), notes, meta)
            _fig("daily_violin", figures.violin_daily_by_borough(pand["complaints_daily_borough"], figures_dir / "daily_violin.png", period, producer), notes, meta)

            # Resolution figures use ALL resolved rows (resolution_hours IS NOT NULL).
            # This is ~98,877 rows — small enough to collect safely (max_collect_rows
            # is 20k, but this frame is a narrow 3-column select, not raw records).
            # Using a 20k deterministic sample here was the bug that caused incorrect
            # mean/p90/p99 values in the Phase 3 manifest and progress reports.
            resolved_sdf = (
                df.filter(F.col("resolution_hours").isNotNull())
                .select("unique_key", "resolution_hours", "borough")
            )
            resolved_n = resolved_sdf.count()
            # Safety guard: if resolved rows exceed 200k, sample. Otherwise collect all.
            _RESOLVED_COLLECT_CAP = 200_000
            if resolved_n > _RESOLVED_COLLECT_CAP:
                resolved = spark_eda.bounded_pandas(
                    geospatial.deterministic_sample(resolved_sdf, _RESOLVED_COLLECT_CAP),
                    _RESOLVED_COLLECT_CAP,
                )
            else:
                resolved = resolved_sdf.toPandas()
            _fig("resolution_hist", figures.histogram_resolution(resolved, figures_dir / "resolution_hist.png", period, producer), notes, meta)
            _fig("resolution_box", figures.box_resolution_by_borough(resolved, figures_dir / "resolution_box.png", period, producer), notes, meta)
            _fig("borough_radar", figures.radar_borough_profile(pand["borough_category"], figures_dir / "borough_radar.png", period, producer), notes, meta)

            if scored:
                sel = [m for m in chosen.get("k_metrics", [])]
                if sel:
                    sel_pd = spark.createDataFrame(sel).toPandas()
                    _fig("kmeans_silhouette", figures.silhouette_curve(sel_pd, figures_dir / "kmeans_silhouette.png", period, producer), notes, meta)
            if cluster_pd is not None:
                _fig("kmeans_clusters", figures.cluster_scatter(cluster_pd, figures_dir / "kmeans_clusters.png", period, producer), notes, meta)
            if labelled is not None:
                _fig("dbscan_clusters", figures.dbscan_scatter(labelled, figures_dir / "dbscan_clusters.png", period, producer), notes, meta)
            if dbscan_sensitivity:
                sweeps = spark.createDataFrame(dbscan_sensitivity).toPandas()
                _fig("dbscan_sensitivity", figures.dbscan_sensitivity(sweeps, figures_dir / "dbscan_sensitivity.png", period, producer), notes, meta)
            _fig("hotspot_map", figures.hotspot_map_plotly(hot_pd, figures_dir / "hotspot_map.html", period, producer), notes, meta)

        metrics = {
            "producer_version": producer,
            "config_path": args.config,
            "input_path": str(input_path),
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),  # noqa: UP017
            "spark_version": spark.version,
            "verify": verify,
            "coverage": coverage,
            "geo_quality": geo_quality,
            "clustering": chosen,
            "preprocessing_ablation": ablation,
            "dbscan": dbscan_summary,
            "dbscan_sensitivity": dbscan_sensitivity,
            "kmeans_seed": m1["kmeans_seed"],
            "grid_cell_deg": m1["grid_cell_deg"],
            "hotspot_min_count": m1["hotspot_min_count"],
            "top_zip_count": len(zips_pd),
        }
        manifest = {
            "producer_version": producer,
            "period": period,
            "snapshot": verify["snapshot_max"],
            "row_count": verify["row_count"],
            "tables": table_meta,
            "figures": meta["figures"],
            "geo_rules": geo_quality["bbox"],
            "grid_cell_deg": m1["grid_cell_deg"],
            "projection": {
                "type": "equirectangular local tangent plane (approx metres)",
                "lat0": m1["projection_lat0"],
                "lon0": m1["projection_lon0"],
            },
            "notes": {
                "counts": (
                    "complaint counts are reported-complaint frequency, not "
                    "population-adjusted rates or infrastructure quality"
                ),
                "invalid_coords": (
                    "invalid/missing coordinates are excluded from spatial "
                    "tables only; non-spatial counts include every row"
                ),
            },
        }
        outdir.mkdir(parents=True, exist_ok=True)
        with open(outdir / "phase3_metrics.json", "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2)
        with open(outdir / "phase3_manifest.json", "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)

        print(json.dumps(
            {
                "row_count": verify["row_count"],
                "period": period,
                "tables": len(table_meta),
                "figures": len(meta["figures"]),
                "kmeans_variant": chosen.get("variant"),
                "kmeans_best_silhouette": (
                    max((m["silhouette"] for m in chosen.get("k_metrics", [])), default=None)
                ),
                "dbscan": dbscan_summary,
                "outdir": str(outdir),
            },
            indent=2,
        ))
        return 0
    finally:
        spark.stop()


def _variant_flags(name: str) -> tuple[bool, bool]:
    mapping = {
        "log_count_scaled": (True, True),
        "log_count_unscaled": (True, False),
        "count_scaled": (False, True),
    }
    if name not in mapping:
        raise ValueError(f"unknown variant {name}")
    return mapping[name]


if __name__ == "__main__":
    raise SystemExit(main())
