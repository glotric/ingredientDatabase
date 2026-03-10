from __future__ import annotations

import json
import re
from collections import defaultdict

from ingredient_pipeline.storage.db import Database

ROLE_MAP = [
    (r"\bpreservative\b", "PRESERVATIVE", 0.95),
    (r"\buv\s*filter\b|\bsunscreen\b", "UV_FILTER", 0.95),
    (r"\bcolorant\b|\bcolourant\b|\bcolor\b", "COLORANT", 0.9),
    (r"\bfragrance\b|\bperfuming\b|\bparfum\b", "FRAGRANCE", 0.85),
    (r"\bsolvent\b", "SOLVENT", 0.8),
    (r"\bemulsif", "EMULSIFIER", 0.82),
    (r"\bsurfact|\bcleansing\b", "SURFACTANT", 0.82),
    (r"\bhumect", "HUMECTANT", 0.84),
    (r"\bemollient\b", "EMOLLIENT", 0.82),
    (r"\bbuffer\b|\bpH adjust\b", "BUFFER", 0.8),
    (r"\bchelat|\bsequestr", "CHELATOR", 0.85),
    (r"\bactive\b", "ACTIVE", 0.7),
]


def _extract_source_values(db: Database) -> dict[int, list[tuple[str, str]]]:
    out: dict[int, list[tuple[str, str]]] = defaultdict(list)
    with db.connect() as conn:
        rows = conn.execute(
            """
            SELECT sl.master_id, sl.source_name, sl.source_table, sl.source_record_key
            FROM source_links sl
            """
        ).fetchall()

        for r in rows:
            master_id = int(r["master_id"])
            source_name = str(r["source_name"])
            source_table = str(r["source_table"])
            source_key = str(r["source_record_key"])

            if source_table == "cosing_parsed":
                row = conn.execute("SELECT function_names FROM cosing_parsed WHERE record_key = ?", (source_key,)).fetchone()
                if row and row["function_names"]:
                    try:
                        vals = json.loads(row["function_names"])
                        for v in vals:
                            out[master_id].append((source_name, str(v)))
                    except Exception:
                        out[master_id].append((source_name, str(row["function_names"])))

            if source_table == "pcpc_parsed":
                row = conn.execute("SELECT functions_json FROM pcpc_parsed WHERE record_key = ?", (source_key,)).fetchone()
                if row and row["functions_json"]:
                    vals = json.loads(row["functions_json"] or "[]")
                    for v in vals:
                        out[master_id].append((source_name, str(v)))

            if source_table == "fda_unii_parsed":
                row = conn.execute("SELECT ingredient_type FROM fda_unii_parsed WHERE record_key = ?", (source_key,)).fetchone()
                if row and row["ingredient_type"]:
                    out[master_id].append((source_name, str(row["ingredient_type"])))

    return out


def infer_role_tags(db: Database) -> dict[str, int]:
    values = _extract_source_values(db)
    inserted = 0

    for master_id, pairs in values.items():
        best_tag = None
        best_score = -1.0

        for source_name, source_value in pairs:
            val = source_value.lower()
            for pattern, role_tag, score in ROLE_MAP:
                if re.search(pattern, val):
                    db.upsert_role_tag(
                        ingredient_master_id=master_id,
                        role_tag=role_tag,
                        source_name=source_name,
                        source_value=source_value,
                        confidence_score=score,
                        is_primary=False,
                    )
                    inserted += 1
                    if score > best_score:
                        best_score = score
                        best_tag = role_tag

        if best_tag:
            db.clear_primary_role_flags(master_id)
            db.upsert_role_tag(
                ingredient_master_id=master_id,
                role_tag=best_tag,
                source_name="normalized",
                source_value="auto-primary",
                confidence_score=best_score,
                is_primary=True,
            )

    return {"processed_masters": len(values), "role_tag_events": inserted}
