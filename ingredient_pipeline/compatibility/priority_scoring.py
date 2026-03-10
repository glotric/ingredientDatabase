from __future__ import annotations

import csv
import json
from pathlib import Path

from ingredient_pipeline.storage.db import Database

ROLE_WEIGHTS = {
    "PRESERVATIVE": 100,
    "UV_FILTER": 100,
    "COLORANT": 85,
    "ACTIVE": 85,
    "FRAGRANCE": 70,
    "EMULSIFIER": 65,
    "SURFACTANT": 65,
    "HUMECTANT": 60,
    "EMOLLIENT": 50,
    "CHELATOR": 50,
    "BUFFER": 45,
    "SOLVENT": 40,
}

SOURCE_COVERAGE_WEIGHTS = {
    "pcpc": 30,
    "cosing": 30,
    "fda_unii": 15,
    "pubchem": 10,
    "chebi": 5,
}


def _clamp(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))


def score_priority_ingredients(db: Database, scoring_version: str = "v1") -> dict[str, int]:
    with db.connect() as conn:
        masters = conn.execute("SELECT id, canonical_name FROM ingredient_master").fetchall()
        source_links = conn.execute("SELECT master_id, source_name, evidence_json FROM source_links").fetchall()
        aliases = conn.execute("SELECT master_id, COUNT(*) AS c FROM ingredient_aliases GROUP BY master_id").fetchall()
        roles = conn.execute("SELECT ingredient_master_id, role_tag FROM ingredient_role_tags").fetchall()
        ids = conn.execute("SELECT master_id, identifier_type FROM ingredient_identifiers").fetchall()

    alias_count = {int(r["master_id"]): int(r["c"]) for r in aliases}

    source_by_master: dict[int, list[tuple[str, str]]] = {}
    annex_relevance: dict[int, float] = {}
    for row in source_links:
        mid = int(row["master_id"])
        source_by_master.setdefault(mid, []).append((str(row["source_name"]), str(row["evidence_json"] or "{}")))
        ev = json.loads(row["evidence_json"] or "{}")
        src_ev = ev.get("source_evidence") or {}
        if src_ev.get("annex_no"):
            annex_relevance[mid] = 1.0

    roles_by_master: dict[int, list[str]] = {}
    for row in roles:
        roles_by_master.setdefault(int(row["ingredient_master_id"]), []).append(str(row["role_tag"]))

    id_types_by_master: dict[int, set[str]] = {}
    for row in ids:
        id_types_by_master.setdefault(int(row["master_id"]), set()).add(str(row["identifier_type"]))

    scored = []
    for m in masters:
        mid = int(m["id"])
        link_rows = source_by_master.get(mid, [])
        link_count = len(link_rows)

        frequency = _clamp((link_count * 4.0) + min(alias_count.get(mid, 0), 30) * 1.2)

        source_names = {s for s, _ in link_rows}
        source_cov = sum(SOURCE_COVERAGE_WEIGHTS.get(s, 0) for s in source_names)
        source_cov = _clamp(source_cov)

        role_score = 0.0
        for role in roles_by_master.get(mid, []):
            role_score = max(role_score, ROLE_WEIGHTS.get(role, 0))

        regulatory = 0.0
        if annex_relevance.get(mid):
            regulatory += 60
        if any(r in {"PRESERVATIVE", "UV_FILTER", "COLORANT"} for r in roles_by_master.get(mid, [])):
            regulatory += 40
        regulatory = _clamp(regulatory)

        id_types = id_types_by_master.get(mid, set())
        if {"CAS", "UNII", "PUBCHEM_CID"}.issubset(id_types):
            name_conf = 95.0
        elif "CAS" in id_types and ("UNII" in id_types or "PUBCHEM_CID" in id_types):
            name_conf = 85.0
        elif "CAS" in id_types or "UNII" in id_types or "PUBCHEM_CID" in id_types:
            name_conf = 70.0
        else:
            name_conf = 45.0

        total = (
            0.40 * frequency
            + 0.20 * source_cov
            + 0.20 * role_score
            + 0.10 * regulatory
            + 0.10 * name_conf
        )

        scored.append(
            {
                "ingredient_master_id": mid,
                "frequency_score": round(frequency, 4),
                "source_coverage_score": round(source_cov, 4),
                "role_importance_score": round(role_score, 4),
                "regulatory_relevance_score": round(regulatory, 4),
                "name_confidence_score": round(name_conf, 4),
                "total_priority_score": round(total, 4),
            }
        )

    scored.sort(key=lambda r: r["total_priority_score"], reverse=True)
    for idx, row in enumerate(scored, start=1):
        db.upsert_priority_score(
            ingredient_master_id=row["ingredient_master_id"],
            frequency_score=row["frequency_score"],
            source_coverage_score=row["source_coverage_score"],
            role_importance_score=row["role_importance_score"],
            regulatory_relevance_score=row["regulatory_relevance_score"],
            name_confidence_score=row["name_confidence_score"],
            total_priority_score=row["total_priority_score"],
            rank_position=idx,
            selected_for_curated_kb=False,
            priority_tier=None,
            scoring_version=scoring_version,
        )

    return {"scored": len(scored)}


def select_top_ingredients(db: Database, top_n: int = 300) -> dict[str, int]:
    with db.connect() as conn:
        rows = conn.execute(
            "SELECT ingredient_master_id, rank_position FROM ingredient_priority_scores ORDER BY total_priority_score DESC"
        ).fetchall()

    selected = 0
    for row in rows:
        rank = int(row["rank_position"] or 0)
        if rank <= 0:
            continue
        if rank <= 100:
            sel, tier = True, "TIER_1"
        elif rank <= top_n:
            sel, tier = True, "TIER_2"
        else:
            sel, tier = False, None
        db.update_priority_selection(
            ingredient_master_id=int(row["ingredient_master_id"]),
            selected_for_curated_kb=sel,
            priority_tier=tier,
            rank_position=rank,
        )
        if sel:
            selected += 1

    return {"selected": selected, "top_n": top_n}


def export_top_ingredients(db: Database, out: Path, top_n: int = 300) -> str:
    out.parent.mkdir(parents=True, exist_ok=True)
    with db.connect() as conn:
        rows = conn.execute(
            """
            SELECT ips.ingredient_master_id, im.canonical_name, ips.total_priority_score,
                   ips.rank_position, ips.selected_for_curated_kb, ips.priority_tier,
                   ips.frequency_score, ips.source_coverage_score, ips.role_importance_score,
                   ips.regulatory_relevance_score, ips.name_confidence_score
            FROM ingredient_priority_scores ips
            JOIN ingredient_master im ON im.id = ips.ingredient_master_id
            ORDER BY ips.total_priority_score DESC
            LIMIT ?
            """,
            (top_n,),
        ).fetchall()

    with out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "ingredient_master_id",
                "canonical_name",
                "total_priority_score",
                "rank_position",
                "selected_for_curated_kb",
                "priority_tier",
                "frequency_score",
                "source_coverage_score",
                "role_importance_score",
                "regulatory_relevance_score",
                "name_confidence_score",
            ],
        )
        writer.writeheader()
        for r in rows:
            writer.writerow(dict(r))
    return str(out)
