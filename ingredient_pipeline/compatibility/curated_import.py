from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from ingredient_pipeline.storage.db import Database


def _read_rows(path: Path) -> list[dict[str, Any]]:
    ext = path.suffix.lower()
    if ext == ".csv":
        return pd.read_csv(path).fillna("").to_dict(orient="records")
    if ext in {".xlsx", ".xls"}:
        return pd.read_excel(path).fillna("").to_dict(orient="records")
    raise ValueError("Curated compatibility import supports CSV/XLSX")


def _as_bool(v: Any) -> bool | None:
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in {"", "na", "n/a"}:
        return None
    if s in {"1", "true", "yes", "y"}:
        return True
    if s in {"0", "false", "no", "n"}:
        return False
    return None


def _split_multi(value: Any) -> list[str]:
    if value is None:
        return []
    s = str(value).strip()
    if not s:
        return []
    if ";" in s:
        return [x.strip() for x in s.split(";") if x.strip()]
    if "|" in s:
        return [x.strip() for x in s.split("|") if x.strip()]
    return [s]


def _normalize_id(v: Any) -> str:
    return "".join(ch for ch in str(v or "").upper() if ch.isalnum())


def _normalize_name(v: Any) -> str:
    return " ".join("".join(ch.lower() if ch.isalnum() else " " for ch in str(v or "")).split())


def _resolve_master_id(db: Database, row: dict[str, Any]) -> int | None:
    cas = _normalize_id(row.get("cas_number"))
    unii = _normalize_id(row.get("unii"))
    cid = _normalize_id(row.get("pubchem_cid"))

    if cas:
        mid = db.find_master_by_identifier("CAS", cas)
        if mid:
            return mid
    if unii:
        mid = db.find_master_by_identifier("UNII", unii)
        if mid:
            return mid
    if cid:
        mid = db.find_master_by_identifier("PUBCHEM_CID", cid)
        if mid:
            return mid

    for key in ["canonical_name", "inci_name"]:
        name = _normalize_name(row.get(key))
        if name:
            mid = db.find_master_by_normalized_name(name)
            if mid:
                return mid
    return None


def import_curated_compatibility(db: Database, file_path: Path) -> dict[str, int]:
    rows = _read_rows(file_path)

    inserted_ph = 0
    inserted_stability = 0
    inserted_pairwise = 0
    inserted_overrides = 0
    skipped_unmatched = 0

    for rec in rows:
        master_id = _resolve_master_id(db, rec)
        if not master_id:
            skipped_unmatched += 1
            continue

        source_type = str(rec.get("evidence_source_type") or "MANUAL")
        source_reference = str(rec.get("evidence_reference") or "")
        source_url = str(rec.get("evidence_url") or "")
        extracted_text = str(rec.get("evidence_excerpt") or "")
        confidence = float(rec.get("confidence_score") or 0.8)

        ph_min = float(rec["preferred_ph_min"]) if str(rec.get("preferred_ph_min") or "").strip() else None
        ph_max = float(rec["preferred_ph_max"]) if str(rec.get("preferred_ph_max") or "").strip() else None
        ph_sensitive = _as_bool(rec.get("ph_sensitive"))
        if ph_min is not None or ph_max is not None or ph_sensitive is not None or str(rec.get("ph_notes") or "").strip():
            evidence_id = db.insert_compatibility_evidence(
                ingredient_master_id=master_id,
                evidence_type="PH_PROFILE",
                source_type=source_type,
                source_reference=source_reference,
                source_url=source_url,
                extracted_text=extracted_text,
                date_accessed=None,
                confidence_score=confidence,
            )
            db.upsert_ph_profile(
                ingredient_master_id=master_id,
                preferred_ph_min=ph_min,
                preferred_ph_max=ph_max,
                ph_sensitive=ph_sensitive,
                ph_notes=str(rec.get("ph_notes") or "") or None,
                derivation_method="CURATED",
                confidence_score=confidence,
                evidence_id=evidence_id,
            )
            inserted_ph += 1

        mitigations = _split_multi(rec.get("mitigations"))
        for flag in _split_multi(rec.get("stability_flags")):
            evidence_id = db.insert_compatibility_evidence(
                ingredient_master_id=master_id,
                evidence_type="STABILITY_FLAG",
                source_type=source_type,
                source_reference=source_reference,
                source_url=source_url,
                extracted_text=extracted_text,
                date_accessed=None,
                confidence_score=confidence,
            )
            for mitigation in mitigations or [""]:
                db.upsert_stability_profile(
                    ingredient_master_id=master_id,
                    flag=flag,
                    mitigation=mitigation,
                    derivation_method="CURATED",
                    confidence_score=confidence,
                    evidence_summary=str(rec.get("notes") or "curated import"),
                    evidence_id=evidence_id,
                )
                inserted_stability += 1

        for tag in _split_multi(rec.get("pairwise_tags")):
            evidence_id = db.insert_compatibility_evidence(
                ingredient_master_id=master_id,
                evidence_type="PAIRWISE_TAG",
                source_type=source_type,
                source_reference=source_reference,
                source_url=source_url,
                extracted_text=extracted_text,
                date_accessed=None,
                confidence_score=confidence,
            )
            db.upsert_pairwise_tag(
                ingredient_master_id=master_id,
                pairwise_tag=tag,
                derivation_method="CURATED",
                confidence_score=confidence,
                notes=str(rec.get("notes") or "curated import"),
                evidence_id=evidence_id,
            )
            inserted_pairwise += 1

        for field_name, col in [
            ("molecular_weight", "override_mw"),
            ("xlogp", "override_logp"),
            ("tpsa", "override_tpsa"),
            ("hbd", "override_hbd"),
            ("hba", "override_hba"),
        ]:
            raw = str(rec.get(col) or "").strip()
            if not raw:
                continue
            try:
                val_num = float(raw)
                val_text = None
            except ValueError:
                val_num = None
                val_text = raw
            db.upsert_manual_override(
                ingredient_master_id=master_id,
                field_name=field_name,
                override_value_text=val_text,
                override_value_num=val_num,
                reason=str(rec.get("notes") or "curated compatibility import"),
                source_type=source_type or "INTERNAL_REVIEW",
                reference_text=source_reference,
            )
            db.insert_compatibility_evidence(
                ingredient_master_id=master_id,
                evidence_type="MANUAL_OVERRIDE",
                source_type=source_type,
                source_reference=source_reference,
                source_url=source_url,
                extracted_text=extracted_text,
                date_accessed=None,
                confidence_score=confidence,
            )
            inserted_overrides += 1

    return {
        "rows_input": len(rows),
        "inserted_ph": inserted_ph,
        "inserted_stability": inserted_stability,
        "inserted_pairwise": inserted_pairwise,
        "inserted_overrides": inserted_overrides,
        "skipped_unmatched": skipped_unmatched,
    }
