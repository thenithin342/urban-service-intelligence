"""Member 2 Phases 5-6 CLI: closure-within-7d baselines, models and cohorts.

Writes contract tables (docs/data_contracts.md 2.2) under
``<root>/artifacts/member2/resolution``:
  tables/resolution_summary, tables/model_comparison,
  predictions/closure_cohorts
plus non-contract extras (calibration, stability_by_borough,
stability_by_category, feature_importance, predictions/test_predictions),
fitted models (models/*.joblib, gitignored) and resolution_metrics.json.

Usage:
  python scripts/run_member2_resolution.py --config configs/sample.yaml
Optional:
  --input <parquet>   --root <dir>   --master local[2]   --no-importance
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import joblib
import pandas as pd
from pyspark.sql import functions as F

from src.evaluation import model_metrics as mm
from src.evaluation.member2_config import (
    MODEL_VERSION_CLOSURE,
    PRODUCER,
    frame_hash,
    member2_config,
    read_yaml,
    resolve,
    utc_now,
    write_extra_table,
    write_json,
)
from src.modeling import resolution as res
from src.utils import contracts
from src.utils.spark import ensure_worker_env, get_spark

SPLITS_REPORTED = ("validation", "train", "test")


def _dates(frame: pd.DataFrame) -> tuple[str, str]:
    d = pd.to_datetime(frame["created_day"])
    return str(d.min().date()), str(d.max().date())


def _importance(model, test: pd.DataFrame, seed: int, cap: int = 20000) -> pd.DataFrame:
    from sklearn.inspection import permutation_importance

    sample = test.sample(n=min(cap, len(test)), random_state=seed).sort_values("unique_key")
    r = permutation_importance(
        model.pipe, sample[res.FEATURE_COLUMNS], sample["closed_within_7d"].astype(int),
        scoring="neg_brier_score", n_repeats=3, random_state=seed)
    out = pd.DataFrame({"feature": res.FEATURE_COLUMNS,
                        "brier_increase_mean": r.importances_mean,
                        "brier_increase_std": r.importances_std,
                        "n_rows": len(sample)})
    return out.sort_values(["brier_increase_mean", "feature"], ascending=[False, True],
                           ignore_index=True)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Member 2 closure-within-7d modeling")
    ap.add_argument("--config", required=True)
    ap.add_argument("--input", default=None, help="Override cleaned Parquet path")
    ap.add_argument("--root", default=None, help="Root for contract paths (default repo)")
    ap.add_argument("--no-importance", action="store_true")
    ap.add_argument("--master", default="local[2]")
    args = ap.parse_args(argv)

    ensure_worker_env()
    cfg = read_yaml(args.config)
    m2 = member2_config(cfg)
    if not m2["train_cutoff"]:
        ap.error("member2.train_cutoff must be set in the config")
    input_path = Path(args.input) if args.input else resolve(ROOT, m2["input"])
    root = Path(args.root) if args.root else resolve(ROOT, m2["artifacts_root"])
    out_dir = root / "artifacts" / "member2" / "resolution"
    if not input_path.exists():
        print(f"[member2] BLOCKER: cleaned Parquet not found at {input_path}", flush=True)
        return 3
    H = m2["closure_horizon_days"]
    alpha, seed = m2["smoothing_alpha"], m2["seed"]

    spark = get_spark("member2-resolution", master=args.master)
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    t0 = time.time()
    try:
        df = spark.read.parquet(str(input_path))
        labelled = res.add_closure_label_spark(df, H).cache()
        n_rows = labelled.count()
        if n_rows == 0:
            print("[member2] BLOCKER: input is empty", flush=True)
            return 3
        labels = res.label_summary_spark(labelled)

        print("[member2] resolution_summary (Spark)", flush=True)
        summary = res.resolution_summary_spark(labelled).toPandas()
        cov_start, cov_end = str(summary["coverage_start"].iloc[0]), str(
            summary["coverage_end"].iloc[0])
        contracts.write_table(summary, root, "resolution_summary", {
            "producer": PRODUCER, "input_path": str(input_path),
            "coverage_start": cov_start, "coverage_end": cov_end,
            "closure_horizon_days": H,
            "label_definition": "closed_at not null and 0 <= closed_at - created_at <= "
                                f"{H} days; eligible if snapshot - created_day >= {H + 1} days",
            "median_p90_definition": "exact interpolated percentiles of resolution_hours "
                                     "(valid closures only)",
        })

        print("[member2] collecting eligible model frame", flush=True)
        elig = labelled.filter(F.col("label_status") == res.ELIGIBLE).select(
            *res.MODEL_INPUT_COLUMNS)
        n_elig = elig.count()
        if n_elig > m2["max_collect_rows"]:
            print(f"[member2] BLOCKER: {n_elig} eligible rows exceed max_collect_rows",
                  flush=True)
            return 3
        frame = elig.toPandas()
        frame["created_at"] = pd.to_datetime(frame["created_at"])
        frame = frame.sort_values(["created_at", "unique_key"], kind="mergesort",
                                  ignore_index=True)
        frame["closed_within_7d"] = frame["closed_within_7d"].astype(int)

        raw_splits = res.chronological_split(frame, m2["train_cutoff"],
                                             m2["label_embargo_days"], m2["validation_days"],
                                             m2["test_end"])
        for name in ("fit", "validation", "train", "test"):
            if raw_splits[name].empty or raw_splits[name]["closed_within_7d"].nunique() < 2:
                print(f"[member2] BLOCKER: split '{name}' empty or single-class", flush=True)
                return 3
        prior = float(raw_splits["train"]["closed_within_7d"].mean())
        feats = res.add_features(frame, H, alpha, prior)
        res.assert_no_leakage(res.FEATURE_COLUMNS)
        splits = {k: feats.loc[v.index] for k, v in raw_splits.items()}

        enc_fit = res.FittedEncoders(splits["fit"], m2["top_categories"])
        enc_train = res.FittedEncoders(splits["train"], m2["top_categories"])
        stage = {
            "validation": (enc_fit.transform(splits["fit"]),
                           enc_fit.transform(splits["validation"])),
        }
        train_x = enc_train.transform(splits["train"])
        test_x = enc_train.transform(splits["test"])

        print("[member2] fitting models", flush=True)
        rows, preds, models, val_brier, timing = [], {}, {}, {}, {}
        for name in res.MODEL_NAMES:
            t1 = time.time()
            m_val = res.make_model(name, alpha, seed).fit(stage["validation"][0])
            pv = m_val.predict_proba(stage["validation"][1])
            m_full = res.make_model(name, alpha, seed).fit(train_x)
            timing[name] = round(time.time() - t1, 2)
            models[name] = m_full
            for split, x, p in (
                ("validation", stage["validation"][1], pv),
                ("train", train_x, m_full.predict_proba(train_x)),
                ("test", test_x, m_full.predict_proba(test_x)),
            ):
                m = mm.binary_metrics(x["closed_within_7d"], p)
                rows.append({"model": name, "split": split, **m,
                             "model_version": MODEL_VERSION_CLOSURE})
                if split == "test":
                    preds[name] = p
                if split == "validation":
                    val_brier[name] = m["brier"]
            print(f"[member2]   {name}: {timing[name]} s", flush=True)
        selected = min(val_brier, key=lambda k: (val_brier[k], k))

        comp = pd.DataFrame(rows)
        test_start, test_end = _dates(splits["test"])
        train_start, train_end = _dates(splits["train"])
        val_start, val_end = _dates(splits["validation"])
        comp_meta = {
            "producer": PRODUCER, "model_version": MODEL_VERSION_CLOSURE,
            "input_path": str(input_path),
            "coverage_start": cov_start, "coverage_end": cov_end,
            "train_cutoff": m2["train_cutoff"],
            "label_embargo_days": m2["label_embargo_days"],
            "train_window": [train_start, train_end],
            "validation_window": [val_start, val_end],
            "test_window": [test_start, test_end],
            "selected_model": selected,
            "selection_rule": "lowest validation Brier (models fitted on train minus the "
                              "validation tail); selected model refitted on full train",
            "features": res.FEATURE_COLUMNS,
            "pr_auc_definition": "average precision (sklearn)",
            "threshold": 0.5,
        }
        contracts.write_table(comp, root, "closure_model_comparison", comp_meta)

        print("[member2] cohorts, calibration, stability", flush=True)
        test = test_x
        p_sel = preds[selected]
        cohorts = res.cohort_predictions(test, p_sel, test_start, test_end,
                                         MODEL_VERSION_CLOSURE, m2["min_cohort_n"])
        contracts.write_table(cohorts, root, "closure_cohort_predictions", {
            "producer": PRODUCER, "model_version": MODEL_VERSION_CLOSURE,
            "model": selected, "coverage_start": test_start, "coverage_end": test_end,
            "train_cutoff": m2["train_cutoff"], "min_cohort_n": m2["min_cohort_n"],
            "note": "expected_open_after_7d = sum(1 - p) over test-window complaints; "
                    "observed_* from mature labels of the same complaints",
        })

        cal = pd.concat([
            mm.calibration_table(test["closed_within_7d"], preds[name]).assign(model=name)
            for name in res.MODEL_NAMES], ignore_index=True)
        meta_t = {"model_version": MODEL_VERSION_CLOSURE, "split": "test",
                  "test_window": [test_start, test_end], "selected_model": selected}
        write_extra_table(cal, out_dir / "tables" / "calibration", meta_t)
        ece = {name: mm.expected_calibration_error(cal[cal["model"] == name])
               for name in res.MODEL_NAMES}

        pred_frame = pd.DataFrame({
            "unique_key": test["unique_key"].to_numpy(),
            "created_day": pd.to_datetime(test["created_day"]).dt.date.to_numpy(),
            "borough": test["borough_cohort"].to_numpy(),
            "complaint_type": test["complaint_type"].to_numpy(),
            "agency": test["agency"].to_numpy(),
            "closed_within_7d": test["closed_within_7d"].to_numpy(),
            **{f"p_{name}": preds[name] for name in res.MODEL_NAMES},
        })
        write_extra_table(pred_frame, out_dir / "predictions" / "test_predictions", meta_t)

        pred_frame["p_selected"] = p_sel
        stab_b = mm.group_metrics(pred_frame.assign(borough=pred_frame["borough"].fillna(
            "UNKNOWN")), "borough", "closed_within_7d", "p_selected", "p_prior_rate",
            m2["min_cohort_n"])
        stab_c = mm.group_metrics(pred_frame, "complaint_type", "closed_within_7d",
                                  "p_selected", "p_prior_rate", m2["min_cohort_n"])
        write_extra_table(stab_b, out_dir / "tables" / "stability_by_borough",
                          {**meta_t, "model": selected})
        write_extra_table(stab_c, out_dir / "tables" / "stability_by_category",
                          {**meta_t, "model": selected})

        importance = None
        if not args.no_importance and selected not in res.BASELINES:
            importance = _importance(models[selected], test, seed)
            write_extra_table(importance, out_dir / "tables" / "feature_importance",
                              {**meta_t, "model": selected,
                               "method": "permutation importance, neg Brier, 3 repeats, "
                                         "<=20000 test rows"})
        model_dir = out_dir / "models"
        model_dir.mkdir(parents=True, exist_ok=True)
        for name, model in models.items():
            joblib.dump(model, model_dir / f"{name}.joblib")

        tr_keys, te_keys = set(splits["train"]["unique_key"]), set(splits["test"]["unique_key"])
        leakage = {
            "forbidden_in_features": sorted(set(res.FEATURE_COLUMNS) & res.FORBIDDEN_FEATURES),
            "train_test_key_overlap": len(tr_keys & te_keys),
            "duplicate_unique_key_in_model_frame": int(frame["unique_key"].duplicated().sum()),
            "max_train_day_before_min_test_day": train_end < test_start,
            "embargo_gap_days": (date.fromisoformat(test_start)
                                 - date.fromisoformat(train_end)).days - 1,
        }
        test_metrics = comp[comp["split"] == "test"].set_index("model")
        metrics = {
            "producer": PRODUCER, "model_version": MODEL_VERSION_CLOSURE,
            "generated_at_utc": utc_now(), "config_path": args.config,
            "input_path": str(input_path), "spark_version": spark.version,
            "rows": n_rows, "eligible_rows": n_elig,
            "label_summary": labels.to_dict(orient="records"),
            "coverage": [cov_start, cov_end],
            "splits": {k: {"rows": len(v), "positive_rate": float(
                v["closed_within_7d"].mean()), "window": list(_dates(v))}
                for k, v in splits.items()},
            **{k: comp_meta[k] for k in ("train_cutoff", "label_embargo_days",
                                         "train_window", "validation_window",
                                         "test_window", "selected_model")},
            "validation_brier": val_brier,
            "fit_seconds": timing,
            "test_metrics": test_metrics.drop(columns=["split", "model_version"]).to_dict(
                orient="index"),
            "test_ece": ece,
            "cohorts": {
                "rows": len(cohorts),
                "small_sample_rows": int(cohorts["small_sample"].sum()),
                "small_sample_complaints": int(
                    cohorts.loc[cohorts["small_sample"], "n_complaints"].sum()),
                "expected_open_total": float(cohorts["expected_open_after_7d"].sum()),
                "observed_open_total": float(cohorts["observed_open_after_7d"].sum()),
            },
            "leakage": leakage,
            "feature_importance": None if importance is None else importance.head(12).to_dict(
                orient="records"),
            "hashes": {
                "model_comparison": frame_hash(comp),
                "closure_cohorts": frame_hash(cohorts),
                "resolution_summary": frame_hash(summary),
                "test_predictions": frame_hash(pred_frame.drop(columns=["p_selected"])),
            },
            "runtime_seconds": round(time.time() - t0, 2),
        }
        write_json(out_dir / "resolution_metrics.json", metrics)
        show = comp[comp["split"].isin(SPLITS_REPORTED)][
            ["model", "split", "n", "positive_rate", "pr_auc", "roc_auc", "brier",
             "precision_at_05", "recall_at_05"]]
        print(show.to_string(index=False), flush=True)
        print(f"[member2] selected={selected} leakage={leakage}", flush=True)
        ok = (not leakage["forbidden_in_features"] and leakage["train_test_key_overlap"] == 0
              and leakage["max_train_day_before_min_test_day"])
        return 0 if ok else 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
