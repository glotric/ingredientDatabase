from __future__ import annotations

import json
import re

from ingredient_pipeline.storage.db import Database

try:
    from rdkit import Chem
except Exception:  # noqa: BLE001
    Chem = None

RULES = [
    {
        "flag": "PHOTOLABILE",
        "mitigations": ["OPAQUE_PACK"],
        "smarts": ["C=CC=CC=C", "C=CC=CC=CC=C"],
        "confidence": 0.55,
        "note": "Conjugated polyene-like system",
    },
    {
        "flag": "OXIDATION_SENSITIVE",
        "mitigations": ["ANTIOXIDANT", "CHELATOR", "AIRLESS_PACK", "OPAQUE_PACK"],
        "smarts": ["c1ccc(O)cc1", "c1cc(O)c(O)cc1"],
        "confidence": 0.65,
        "note": "Phenol/catechol-like motif",
    },
    {
        "flag": "HYDROLYSIS_SENSITIVE",
        "mitigations": ["PH_BUFFERING"],
        "smarts": ["C(=O)O", "O=C1O"],
        "confidence": 0.6,
        "note": "Ester/lactone-like motif",
    },
]


def _iter_structures(db: Database):
    with db.connect() as conn:
        rows = conn.execute(
            """
            SELECT im.id AS master_id, ie.record_key, ie.canonical_smiles, ie.isomeric_smiles
            FROM ingredient_master im
            JOIN source_links sl ON sl.master_id = im.id
            JOIN ingredient_enrichment ie ON ie.record_key = sl.source_record_key
            WHERE ie.match_status = 'matched' AND (ie.canonical_smiles IS NOT NULL OR ie.isomeric_smiles IS NOT NULL)
            """
        ).fetchall()
    for r in rows:
        yield int(r["master_id"]), str(r["record_key"]), str(r["canonical_smiles"] or r["isomeric_smiles"])


def infer_stability(db: Database) -> dict[str, int]:
    if Chem is None:
        return {"processed": 0, "inferred_flags": 0, "skipped": 0, "note": "RDKit unavailable"}

    compiled = []
    for rule in RULES:
        queries = [Chem.MolFromSmarts(s) for s in rule["smarts"]]
        queries = [q for q in queries if q is not None]
        compiled.append((rule, queries))

    processed = 0
    inferred = 0
    skipped = 0
    with db.connect() as conn:
        existing_rows = conn.execute(
            """
            SELECT ingredient_master_id, flag, mitigation, derivation_method, evidence_summary
            FROM ingredient_stability_profile
            WHERE is_active = 1
            """
        ).fetchall()
    existing = {
        (
            int(r["ingredient_master_id"]),
            str(r["flag"]),
            str(r["mitigation"] or ""),
            str(r["derivation_method"]),
            str(r["evidence_summary"] or ""),
        )
        for r in existing_rows
    }

    for master_id, record_key, smiles in _iter_structures(db):
        mol = Chem.MolFromSmiles(smiles)
        if mol is None:
            skipped += 1
            continue
        processed += 1

        for rule, queries in compiled:
            matched = any(mol.HasSubstructMatch(q) for q in queries)
            if not matched:
                continue

            rule_targets = [
                (master_id, rule["flag"], mitigation, "SMARTS_RULE", rule["note"])
                for mitigation in rule["mitigations"]
            ]
            if all(k in existing for k in rule_targets):
                continue

            evidence_id = db.insert_compatibility_evidence(
                ingredient_master_id=master_id,
                evidence_type="STABILITY_FLAG",
                source_type="SMARTS_RULE",
                source_reference=f"smarts:{rule['flag']}",
                source_url=None,
                extracted_text=f"record_key={record_key}; note={rule['note']}; smiles={smiles}",
                date_accessed=None,
                confidence_score=rule["confidence"],
            )
            for mitigation in rule["mitigations"]:
                key = (master_id, rule["flag"], mitigation, "SMARTS_RULE", rule["note"])
                if key in existing:
                    continue
                db.upsert_stability_profile(
                    ingredient_master_id=master_id,
                    flag=rule["flag"],
                    mitigation=mitigation,
                    derivation_method="SMARTS_RULE",
                    confidence_score=rule["confidence"],
                    evidence_summary=rule["note"],
                    evidence_id=evidence_id,
                )
                existing.add(key)
                inferred += 1

            if rule["flag"] == "PHOTOLABILE":
                secondary_note = "Conjugated system oxidative risk"
                secondary_targets = [
                    (master_id, "OXIDATION_SENSITIVE", mitigation, "SMARTS_RULE", secondary_note)
                    for mitigation in ["ANTIOXIDANT", "CHELATOR", "AIRLESS_PACK", "OPAQUE_PACK"]
                ]
                if all(k in existing for k in secondary_targets):
                    continue
                evidence_id2 = db.insert_compatibility_evidence(
                    ingredient_master_id=master_id,
                    evidence_type="STABILITY_FLAG",
                    source_type="SMARTS_RULE",
                    source_reference="smarts:OXIDATION_SENSITIVE_by_polyene",
                    extracted_text=f"record_key={record_key}; conjugated system suggests oxidation sensitivity",
                    confidence_score=0.5,
                )
                for mitigation in ["ANTIOXIDANT", "CHELATOR", "AIRLESS_PACK", "OPAQUE_PACK"]:
                    key = (master_id, "OXIDATION_SENSITIVE", mitigation, "SMARTS_RULE", secondary_note)
                    if key in existing:
                        continue
                    db.upsert_stability_profile(
                        ingredient_master_id=master_id,
                        flag="OXIDATION_SENSITIVE",
                        mitigation=mitigation,
                        derivation_method="SMARTS_RULE",
                        confidence_score=0.5,
                        evidence_summary=secondary_note,
                        evidence_id=evidence_id2,
                    )
                    existing.add(key)
                    inferred += 1

    return {"processed": processed, "inferred_flags": inferred, "skipped": skipped}
