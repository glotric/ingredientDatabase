from __future__ import annotations

from pathlib import Path

from ingredient_pipeline.storage.db import Database

SEED_RULES = [
    {
        "rule_code": "R-NIACINAMIDE-LOWPH-ACID",
        "ingredient_name_a": "NIACINAMIDE",
        "ingredient_name_b": None,
        "tag_a": None,
        "tag_b": "STRONG_ACID",
        "condition_context": {"ph_lt": 5.0},
        "severity": "WARNING",
        "penalty_points": 20,
        "message": "Niacinamide may be less stable in strongly acidic systems.",
        "suggestion": "Keep pH near neutral where possible.",
        "source_type": "MANUAL",
    },
    {
        "rule_code": "R-CATIONIC-ANIONIC",
        "ingredient_name_a": None,
        "ingredient_name_b": None,
        "tag_a": "CATIONIC",
        "tag_b": "ANIONIC",
        "condition_context": {},
        "severity": "HIGH",
        "penalty_points": 35,
        "message": "Cationic and anionic systems can be incompatible.",
        "suggestion": "Review surfactant system; use nonionic bridge if needed.",
        "source_type": "MANUAL",
    },
    {
        "rule_code": "R-OXIDIZER-OXIDATION_SENSITIVE",
        "ingredient_name_a": None,
        "ingredient_name_b": None,
        "tag_a": "OXIDIZER",
        "tag_b": "OXIDATION_SENSITIVE",
        "condition_context": {},
        "severity": "HIGH",
        "penalty_points": 40,
        "message": "Oxidizers may degrade oxidation-sensitive ingredients.",
        "suggestion": "Add antioxidant/chelators or separate incompatible actives.",
        "source_type": "MANUAL",
    },
    {
        "rule_code": "R-PEPTIDE-STRONG_ACID",
        "ingredient_name_a": None,
        "ingredient_name_b": None,
        "tag_a": "PEPTIDE",
        "tag_b": "STRONG_ACID",
        "condition_context": {"ph_lt": 5.0},
        "severity": "WARNING",
        "penalty_points": 25,
        "message": "Peptides may lose performance under strongly acidic conditions.",
        "suggestion": "Control pH and consider encapsulation.",
        "source_type": "MANUAL",
    },
]


def seed_pairwise_rules(db: Database) -> int:
    for rule in SEED_RULES:
        db.upsert_pairwise_rule(
            rule_code=rule["rule_code"],
            ingredient_name_a=rule["ingredient_name_a"],
            ingredient_name_b=rule["ingredient_name_b"],
            tag_a=rule["tag_a"],
            tag_b=rule["tag_b"],
            condition_context=rule["condition_context"],
            severity=rule["severity"],
            penalty_points=rule["penalty_points"],
            message=rule["message"],
            suggestion=rule["suggestion"],
            source_type=rule["source_type"],
        )
    return len(SEED_RULES)


def ensure_curated_template(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        return path

    header = [
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
    ]
    path.write_text(",".join(header) + "\n", encoding="utf-8")
    return path
