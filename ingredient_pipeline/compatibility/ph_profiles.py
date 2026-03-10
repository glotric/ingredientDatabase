from __future__ import annotations

from ingredient_pipeline.storage.db import Database


def upsert_inferred_ph_profile(
    db: Database,
    ingredient_master_id: int,
    preferred_ph_min: float | None,
    preferred_ph_max: float | None,
    ph_sensitive: bool | None,
    notes: str,
    confidence: float,
) -> None:
    evidence_id = db.insert_compatibility_evidence(
        ingredient_master_id=ingredient_master_id,
        evidence_type="PH_PROFILE",
        source_type="SMARTS_RULE",
        source_reference="inferred-basic-rule",
        extracted_text=notes,
        confidence_score=confidence,
    )
    db.upsert_ph_profile(
        ingredient_master_id=ingredient_master_id,
        preferred_ph_min=preferred_ph_min,
        preferred_ph_max=preferred_ph_max,
        ph_sensitive=ph_sensitive,
        ph_notes=notes,
        derivation_method="INFERRED",
        confidence_score=confidence,
        evidence_id=evidence_id,
    )
