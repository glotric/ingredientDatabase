from __future__ import annotations

import csv
import json
import tempfile
from pathlib import Path

from ingredient_pipeline.compatibility.curated_import import import_curated_compatibility
from ingredient_pipeline.compatibility.pairwise_tags import infer_pairwise_tags
from ingredient_pipeline.compatibility.priority_scoring import score_priority_ingredients, select_top_ingredients
from ingredient_pipeline.compatibility.role_normalizer import infer_role_tags
from ingredient_pipeline.compatibility.schema import ensure_curated_template, seed_pairwise_rules
from ingredient_pipeline.compatibility.stability_rules import Chem, infer_stability
from ingredient_pipeline.storage.db import Database, utc_now_iso


def _count(db: Database, table: str) -> int:
    with db.connect() as conn:
        return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def test_compatibility_pipeline_smoke() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        db = Database(root / "test.db")
        run = db.start_run("compat-smoke")

        # Canonical layer fixture
        niacinamide_id = db.insert_master("Niacinamide", "niacinamide")
        citric_id = db.insert_master("Citric Acid", "citric acid")

        db.upsert_identifier(niacinamide_id, "CAS", "98-92-0", "98920", "fixture", 1.0)
        db.upsert_identifier(niacinamide_id, "UNII", "25X51I8RD4", "25X51I8RD4", "fixture", 1.0)
        db.upsert_identifier(niacinamide_id, "PUBCHEM_CID", "936", "936", "fixture", 1.0)
        db.upsert_identifier(citric_id, "CAS", "77-92-9", "77929", "fixture", 1.0)

        db.upsert_alias(niacinamide_id, "NICOTINAMIDE", "nicotinamide", "fixture", "fx1")
        db.upsert_alias(citric_id, "2-Hydroxypropane-1,2,3-tricarboxylic acid", "2 hydroxypropane 1 2 3 tricarboxylic acid", "fixture", "fx2")

        db.upsert_cosing_parsed(
            {
                "run_id": run.run_id,
                "source_raw_id": None,
                "record_key": "cosing:fixture:niacinamide",
                "item_type": "ingredient",
                "ingredient_name": "Niacinamide",
                "inci_name": "NIACINAMIDE",
                "cas_no": "98-92-0",
                "ec_no": None,
                "function_names": json.dumps(["skin conditioning", "active"]),
                "description": "fixture",
                "annex_no": "III",
                "ref_no": "1",
                "status": "Active",
                "regulation_refs": "{}",
                "source_url": "urn:fixture:cosing",
                "parsed_at": utc_now_iso(),
                "provenance": {},
                "raw_record": {},
            }
        )
        db.upsert_pcpc_parsed(
            {
                "run_id": run.run_id,
                "source_raw_id": None,
                "record_key": "pcpc:fixture:niacinamide",
                "inci_name": "NIACINAMIDE",
                "preferred_name": "Niacinamide",
                "cas_number": "98-92-0",
                "synonyms": ["Nicotinamide"],
                "functions": ["active", "skin conditioning"],
                "description": "fixture",
                "regulatory_notes": None,
                "column_mapping": {},
                "mapping_warnings": [],
                "parsed_at": utc_now_iso(),
                "provenance": {},
                "raw_record": {},
            }
        )

        db.upsert_enrichment(
            {
                "run_id": run.run_id,
                "record_key": "pubchem:fixture:niacinamide",
                "match_status": "matched",
                "confidence": 1.0,
                "lookup_method": "fixture",
                "cid": 936,
                "canonical_smiles": "C=CC=CC=C",
                "isomeric_smiles": None,
                "inchi": None,
                "inchikey": None,
                "molecular_formula": "C6H7NO",
                "molecular_weight": 121.1,
                "iupac_name": "Niacinamide",
                "synonyms": ["Nicotinamide"],
                "xlogp": None,
                "computed_props": {},
                "source_url": "urn:fixture:pubchem",
                "raw_ref_id": None,
                "enriched_at": utc_now_iso(),
                "provenance": {},
            }
        )

        db.upsert_source_link(niacinamide_id, "cosing", "cosing_parsed", "cosing:fixture:niacinamide", "", {"source_evidence": {"annex_no": "III"}})
        db.upsert_source_link(niacinamide_id, "pcpc", "pcpc_parsed", "pcpc:fixture:niacinamide", "", {})
        db.upsert_source_link(niacinamide_id, "pubchem", "ingredient_enrichment", "pubchem:fixture:niacinamide", "", {})
        db.upsert_source_link(citric_id, "cosing", "cosing_parsed", "cosing:fixture:niacinamide", "", {})

        # Role normalization
        role_stats = infer_role_tags(db)
        assert role_stats["processed_masters"] >= 1
        assert _count(db, "ingredient_role_tags") >= 1

        # Stability inference (works both with and without RDKit)
        stability_stats = infer_stability(db)
        assert "processed" in stability_stats
        if Chem is None:
            assert "note" in stability_stats
        else:
            assert _count(db, "ingredient_stability_profile") >= 1

        # Pairwise tags + deterministic rule seed
        assert seed_pairwise_rules(db) >= 4
        pairwise_stats = infer_pairwise_tags(db)
        assert pairwise_stats["processed_masters"] >= 1
        assert _count(db, "ingredient_pairwise_tags") >= 1

        # Curated compatibility import fixture
        curated_csv = root / "curated.csv"
        with curated_csv.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "canonical_name",
                    "inci_name",
                    "cas_number",
                    "unii",
                    "pubchem_cid",
                    "preferred_ph_min",
                    "preferred_ph_max",
                    "ph_sensitive",
                    "ph_notes",
                    "stability_flags",
                    "mitigations",
                    "pairwise_tags",
                    "override_mw",
                    "override_logp",
                    "override_tpsa",
                    "override_hbd",
                    "override_hba",
                    "evidence_source_type",
                    "evidence_reference",
                    "evidence_url",
                    "evidence_excerpt",
                    "confidence_score",
                    "notes",
                ],
            )
            writer.writeheader()
            writer.writerow(
                {
                    "canonical_name": "Niacinamide",
                    "cas_number": "98-92-0",
                    "preferred_ph_min": "5.0",
                    "preferred_ph_max": "7.0",
                    "ph_sensitive": "true",
                    "ph_notes": "Best stability in mildly acidic-neutral range",
                    "stability_flags": "OXIDATION_SENSITIVE",
                    "mitigations": "ANTIOXIDANT;OPAQUE_PACK",
                    "pairwise_tags": "REDUCER",
                    "override_mw": "122.12",
                    "evidence_source_type": "LITERATURE",
                    "evidence_reference": "fixture-ref",
                    "evidence_url": "https://example.test",
                    "evidence_excerpt": "fixture excerpt",
                    "confidence_score": "0.9",
                    "notes": "fixture import",
                }
            )

        curated_stats = import_curated_compatibility(db, curated_csv)
        assert curated_stats["rows_input"] == 1
        assert _count(db, "ingredient_ph_profile") >= 1
        assert _count(db, "ingredient_manual_overrides") >= 1

        # Priority scoring + selection
        score_stats = score_priority_ingredients(db)
        assert score_stats["scored"] >= 2

        select_stats = select_top_ingredients(db, top_n=2)
        assert select_stats["selected"] == 2

        # Idempotent reruns: infer commands should not duplicate inferred rows.
        before_role = _count(db, "ingredient_role_tags")
        before_stab = _count(db, "ingredient_stability_profile")
        before_pair = _count(db, "ingredient_pairwise_tags")

        infer_role_tags(db)
        infer_stability(db)
        infer_pairwise_tags(db)

        assert _count(db, "ingredient_role_tags") == before_role
        assert _count(db, "ingredient_stability_profile") == before_stab
        assert _count(db, "ingredient_pairwise_tags") == before_pair

        # Template generation
        template = ensure_curated_template(root / "data" / "templates" / "compatibility_seed_template.csv")
        assert template.exists()


def main() -> None:
    test_compatibility_pipeline_smoke()
    print("compatibility smoke tests passed")


if __name__ == "__main__":
    main()
