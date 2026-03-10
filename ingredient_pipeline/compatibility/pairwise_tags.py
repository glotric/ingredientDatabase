from __future__ import annotations

import json
import re

from ingredient_pipeline.storage.db import Database

try:
    from rdkit import Chem
except Exception:  # noqa: BLE001
    Chem = None


NAME_HEURISTICS = [
    (r"\bniacinamide\b", "REDUCER", 0.55, "name heuristic"),
    (r"\bascorb", "REDUCER", 0.75, "ascorbate family"),
    (r"\bcitric acid\b|\blactic acid\b|\bglycolic acid\b|\bsalicylic acid\b", "STRONG_ACID", 0.75, "acid name heuristic"),
    (r"\bpeptide\b|\bpalmitoyl .*peptide\b", "PEPTIDE", 0.8, "peptide naming heuristic"),
]


def _role_based_tags(role: str) -> list[tuple[str, float, str]]:
    r = role.upper()
    if r in {"SURFACTANT"}:
        return [("ANIONIC", 0.45, "role-based heuristic")]
    if r in {"CHELATOR"}:
        return [("METAL_REACTIVE", 0.55, "role-based heuristic")]
    return []


def infer_pairwise_tags(db: Database) -> dict[str, int]:
    processed = 0
    inserted = 0

    with db.connect() as conn:
        masters = conn.execute("SELECT id, canonical_name FROM ingredient_master").fetchall()
        role_rows = conn.execute("SELECT ingredient_master_id, role_tag FROM ingredient_role_tags").fetchall()
        role_map: dict[int, list[str]] = {}
        for r in role_rows:
            role_map.setdefault(int(r["ingredient_master_id"]), []).append(str(r["role_tag"]))

        smiles_rows = conn.execute(
            """
            SELECT sl.master_id, ie.canonical_smiles, ie.isomeric_smiles
            FROM source_links sl
            JOIN ingredient_enrichment ie ON ie.record_key = sl.source_record_key
            WHERE ie.canonical_smiles IS NOT NULL OR ie.isomeric_smiles IS NOT NULL
            """
        ).fetchall()
        smiles_map: dict[int, str] = {}
        for s in smiles_rows:
            mid = int(s["master_id"])
            smiles_map[mid] = str(s["canonical_smiles"] or s["isomeric_smiles"])
        existing_rows = conn.execute(
            """
            SELECT ingredient_master_id, pairwise_tag, derivation_method, notes
            FROM ingredient_pairwise_tags
            """
        ).fetchall()
    existing = {
        (int(r["ingredient_master_id"]), str(r["pairwise_tag"]), str(r["derivation_method"]), str(r["notes"] or ""))
        for r in existing_rows
    }

    for m in masters:
        master_id = int(m["id"])
        name = str(m["canonical_name"] or "")
        lname = name.lower()
        processed += 1

        tags: list[tuple[str, float, str]] = []
        for pat, tag, conf, note in NAME_HEURISTICS:
            if re.search(pat, lname):
                tags.append((tag, conf, note))

        for role in role_map.get(master_id, []):
            tags.extend(_role_based_tags(role))

        smi = smiles_map.get(master_id)
        if Chem is not None and smi:
            mol = Chem.MolFromSmiles(smi)
            if mol is not None:
                ester = Chem.MolFromSmarts("C(=O)O")
                phenol = Chem.MolFromSmarts("c1ccc(O)cc1")
                if ester and mol.HasSubstructMatch(ester):
                    tags.append(("ESTER_RICH", 0.6, "SMARTS ester motif"))
                if phenol and mol.HasSubstructMatch(phenol):
                    tags.append(("POLYPHENOL", 0.55, "SMARTS phenol motif"))

        # Deduplicate by tag keeping max confidence
        by_tag: dict[str, tuple[float, str]] = {}
        for tag, conf, note in tags:
            prev = by_tag.get(tag)
            if prev is None or conf > prev[0]:
                by_tag[tag] = (conf, note)

        for tag, (conf, note) in by_tag.items():
            derivation_method = "SMARTS_RULE" if "SMARTS" in note else "INFERRED"
            key = (master_id, tag, derivation_method, note)
            if key in existing:
                continue
            evidence_id = db.insert_compatibility_evidence(
                ingredient_master_id=master_id,
                evidence_type="PAIRWISE_TAG",
                source_type="SMARTS_RULE" if "SMARTS" in note else "MANUAL",
                source_reference="pairwise-inference",
                extracted_text=f"name={name}; note={note}",
                confidence_score=conf,
            )
            db.upsert_pairwise_tag(
                ingredient_master_id=master_id,
                pairwise_tag=tag,
                derivation_method=derivation_method,
                confidence_score=conf,
                notes=note,
                evidence_id=evidence_id,
            )
            existing.add(key)
            inserted += 1

    return {"processed_masters": processed, "pairwise_tag_events": inserted}
