"""End-to-end export/import smoke test: producer -> adapters -> page. ASCII only.

A table is published with contracts.write_table into a temporary artifacts
root (exactly as Member 2 would), read back through dashboard.adapters and
rendered by the real Streamlit page. The page must show the exact exported
values and label them REAL, never the fixture.
"""

from __future__ import annotations

import pandas as pd
from streamlit.testing.v1 import AppTest

from dashboard import adapters as ad
from src.utils import contracts
from tests.app.conftest import REPO

APP = str(REPO / "dashboard" / "app.py")


def _rules() -> pd.DataFrame:
    base = {
        "n_baskets": 777,
        "min_support": 0.01,
        "min_confidence": 0.2,
        "basket_definition": "E2E test basket: ZIP x ISO week",
        "coverage_start": "2023-12-01",
        "coverage_end": "2024-02-29",
        "model_version": "e2e-test-1",
    }
    return pd.DataFrame(
        [
            {"antecedent": ["Alpha Item"], "consequent": ["Beta Item"], "support": 0.4321,
             "confidence": 0.8765, "lift": 2.3456, "rule_baskets": 336, **base},
            {"antecedent": ["Gamma Item", "Alpha Item"], "consequent": ["Delta Item"],
             "support": 0.0123, "confidence": 0.3333, "lift": 1.0101, "rule_baskets": 10,
             **base},
        ]
    )


def test_export_import_render_roundtrip(empty_root):
    df = _rules()
    contracts.write_table(df, empty_root, "pattern_rules", {"producer": "e2e-test"})

    res = ad.load_contract("pattern_rules")
    assert res.source == ad.REAL
    assert res.problems == []
    back = res.df.sort_values("support", ascending=False).reset_index(drop=True)
    assert back["support"].tolist() == [0.4321, 0.0123]
    assert ad.list_text(back.loc[1, "antecedent"]) == "Gamma Item + Alpha Item"

    at = AppTest.from_file(APP, default_timeout=120)
    at.run()
    at.switch_page("views/patterns.py")
    at.run()
    assert not at.exception
    shown = at.dataframe[0].value
    assert sorted(shown["support"].tolist()) == [0.0123, 0.4321]
    assert sorted(shown["lift"].tolist()) == [1.0101, 2.3456]
    assert "Alpha Item" in set(shown["antecedent"])
    text = " ".join(str(m.value) for m in at.markdown) + " ".join(
        str(m.value) for m in at.metric
    )
    assert "E2E test basket" in text and "777" in text and "e2e-test-1" in text
    banner = " ".join(str(w.value) for w in (*at.info, *at.warning))
    assert "REAL: pattern_rules" in banner
    assert "FIXTURE: pattern_rules" not in banner

    # Lift slider filters on the exact values.
    at.sidebar.slider(key="pt_lift").set_value(2.0).run()
    assert at.dataframe[0].value["lift"].tolist() == [2.3456]
    # Item search.
    at.sidebar.slider(key="pt_lift").set_value(1.0101).run()
    at.sidebar.text_input(key="pt_search").input("gamma").run()
    assert at.dataframe[0].value["support"].tolist() == [0.0123]


def test_fixture_rules_are_labelled_on_page(empty_root):
    at = AppTest.from_file(APP, default_timeout=120)
    at.run()
    at.switch_page("views/patterns.py")
    at.run()
    assert not at.exception
    warnings = " ".join(str(w.value) for w in at.warning)
    assert "FIXTURE: pattern_rules" in warnings
    assert "NOT results" in warnings
    infos = " ".join(str(i.value) for i in at.info)
    assert "not causation" in infos
