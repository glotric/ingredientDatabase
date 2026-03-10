from __future__ import annotations

import difflib
import logging
import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from ingredient_pipeline.storage.db import Database

logger = logging.getLogger(__name__)

NON_ALNUM = re.compile(r"[^a-z0-9]+")
STRONG_IDENTIFIER_TYPES = ["CAS", "UNII", "PUBCHEM_CID", "CHEBI_ID"]

CANONICAL_NAME_PRIORITY = {
    "pcpc_inci": 100,
    "cosing_inci": 90,
    "existing": 80,
    "fda_unii_preferred": 70,
    "pubchem_name": 60,
    "chebi_preferred": 50,
    "fallback": 10,
}


@dataclass(slots=True)
class MatchDecision:
    master_id: int
    match_method: str
    confidence_score: float
    evidence: dict[str, Any]


def normalize_name(value: str | None) -> str:
    if not value:
        return ""
    s = value.strip().lower()
    s = s.replace("’", "'")
    s = NON_ALNUM.sub(" ", s)
    return " ".join(s.split())


def normalize_identifier(value: str | None) -> str:
    if not value:
        return ""
    return NON_ALNUM.sub("", value.strip().upper())


class IdentityResolver:
    def __init__(self, db: Database):
        self.db = db

    def run(self) -> dict[str, int]:
        run_id = self.db.start_normalization_run()
        created = 0
        linked = 0
        candidate_count = 0
        conflict_candidates = 0

        try:
            rows = self.db.iter_identity_input_rows()
            master_name_index: dict[int, str] = {}

            for rec in rows:
                decision = self._match_existing(rec)
                if decision is None:
                    canonical_name = self._choose_record_best_name(rec)
                    normalized_name = normalize_name(canonical_name)
                    if not canonical_name:
                        continue
                    existing = self.db.find_master_by_name_pair(canonical_name, normalized_name)
                    master_id = existing or self.db.insert_master(canonical_name=canonical_name, normalized_name_key=normalized_name)
                    decision = MatchDecision(
                        master_id=master_id,
                        match_method="new_master",
                        confidence_score=1.0,
                        evidence={"reason": "no deterministic existing match"},
                    )
                    if not existing:
                        created += 1

                linked += 1
                self._attach_record(decision, rec)
                master = next((r for r in self.db.get_master_rows() if int(r["id"]) == decision.master_id), None)
                if master:
                    master_name_index[decision.master_id] = str(master["normalized_name_key"])

            candidate_count = self._generate_fuzzy_candidates(master_name_index)
            conflict_candidates = len(self.db.conflicts_report_rows())

            summary = {
                "masters_created": created,
                "source_rows_linked": linked,
                "merge_candidates": candidate_count,
                "identifier_conflicts": conflict_candidates,
            }
            self.db.complete_normalization_run(run_id, "success", summary=summary)
            return summary
        except Exception as exc:  # noqa: BLE001
            self.db.complete_normalization_run(run_id, "failed", notes=str(exc))
            raise

    def _match_existing(self, rec: dict[str, Any]) -> MatchDecision | None:
        ids = self._record_ids(rec)

        if ids["CAS"]:
            master_id = self.db.find_master_by_identifier("CAS", ids["CAS"])
            if master_id:
                return MatchDecision(master_id, "exact_cas", 1.0, {"cas": ids["CAS"]})
        if ids["UNII"]:
            master_id = self.db.find_master_by_identifier("UNII", ids["UNII"])
            if master_id:
                return MatchDecision(master_id, "exact_unii", 1.0, {"unii": ids["UNII"]})
        if ids["PUBCHEM_CID"]:
            master_id = self.db.find_master_by_identifier("PUBCHEM_CID", ids["PUBCHEM_CID"])
            if master_id:
                return MatchDecision(master_id, "exact_pubchem_cid", 1.0, {"pubchem_cid": ids["PUBCHEM_CID"]})
        if ids["CHEBI_ID"]:
            master_id = self.db.find_master_by_identifier("CHEBI_ID", ids["CHEBI_ID"])
            if master_id:
                return MatchDecision(master_id, "exact_chebi_id", 1.0, {"chebi_id": ids["CHEBI_ID"]})

        normalized_inci = normalize_name(rec.get("inci_name"))
        if normalized_inci:
            decision = self._match_name_with_conflict_checks(rec, normalized_inci, "exact_normalized_inci")
            if decision:
                return decision

        normalized_pref = normalize_name(rec.get("preferred_name") or rec.get("name"))
        if normalized_pref:
            decision = self._match_name_with_conflict_checks(rec, normalized_pref, "exact_normalized_preferred_name")
            if decision:
                return decision

        aliases = [normalize_name(x) for x in rec.get("aliases") or [] if x]
        for alias in aliases:
            decision = self._match_name_with_conflict_checks(rec, alias, "synonym_match", confidence=0.88)
            if decision:
                return decision

        return None

    def _match_name_with_conflict_checks(
        self,
        rec: dict[str, Any],
        normalized_name: str,
        method: str,
        confidence: float = 0.95,
    ) -> MatchDecision | None:
        candidate_master_ids = self.db.find_masters_by_normalized_name(normalized_name)
        if not candidate_master_ids:
            return None

        rec_ids = self._record_ids(rec)
        for master_id in candidate_master_ids:
            conflicts = self._find_identifier_conflicts(master_id, rec_ids)
            if conflicts:
                self.db.insert_merge_candidate(
                    left_master_id=master_id,
                    right_master_id=self._create_conflict_placeholder_master(rec, normalized_name),
                    match_method="identifier_conflict",
                    confidence_score=0.4,
                    evidence={
                        "normalized_name": normalized_name,
                        "conflicting_identifiers": conflicts,
                        "source_record_key": rec["source_record_key"],
                    },
                )
                continue
            return MatchDecision(master_id, method, confidence, {"name": normalized_name})

        return None

    def _create_conflict_placeholder_master(self, rec: dict[str, Any], normalized_name: str) -> int:
        name = self._choose_record_best_name(rec)
        canonical = (name or normalized_name).strip()
        if not canonical:
            canonical = "unknown ingredient conflict"
        canonical = f"{canonical} [conflict:{rec.get('source_name')}:{rec.get('source_record_key')}]"
        norm = normalize_name(canonical)
        existing = self.db.find_master_by_name_pair(canonical, norm)
        if existing:
            return existing
        return self.db.insert_master(canonical_name=canonical, normalized_name_key=norm)

    def _find_identifier_conflicts(self, master_id: int, rec_ids: dict[str, str]) -> dict[str, dict[str, str]]:
        master_ids = self.db.get_master_identifiers(master_id)
        conflicts: dict[str, dict[str, str]] = {}
        for id_type in STRONG_IDENTIFIER_TYPES:
            rec_val = rec_ids.get(id_type)
            master_vals = master_ids.get(id_type) or set()
            if rec_val and master_vals and rec_val not in master_vals:
                conflicts[id_type] = {
                    "record_value": rec_val,
                    "master_values": ",".join(sorted(master_vals)),
                }
        return conflicts

    def _record_ids(self, rec: dict[str, Any]) -> dict[str, str]:
        return {
            "CAS": normalize_identifier(rec.get("cas")),
            "UNII": normalize_identifier(rec.get("unii")),
            "PUBCHEM_CID": normalize_identifier(rec.get("pubchem_cid")),
            "CHEBI_ID": normalize_identifier(rec.get("chebi_id")),
        }

    def _choose_record_best_name(self, rec: dict[str, Any]) -> str:
        source = rec.get("source_name")
        inci_name = rec.get("inci_name")
        preferred = rec.get("preferred_name")
        name = rec.get("name")

        if source == "pcpc" and inci_name:
            return str(inci_name)
        if source == "cosing" and inci_name:
            return str(inci_name)
        if source == "fda_unii" and preferred:
            return str(preferred)
        if source == "pubchem" and preferred:
            return str(preferred)
        if source == "chebi" and preferred:
            return str(preferred)
        return str(inci_name or preferred or name or "")

    def _name_priority(self, source_name: str, inci_name: str | None, preferred: str | None) -> int:
        if source_name == "pcpc" and inci_name:
            return CANONICAL_NAME_PRIORITY["pcpc_inci"]
        if source_name == "cosing" and inci_name:
            return CANONICAL_NAME_PRIORITY["cosing_inci"]
        if source_name == "fda_unii" and preferred:
            return CANONICAL_NAME_PRIORITY["fda_unii_preferred"]
        if source_name == "pubchem" and preferred:
            return CANONICAL_NAME_PRIORITY["pubchem_name"]
        if source_name == "chebi" and preferred:
            return CANONICAL_NAME_PRIORITY["chebi_preferred"]
        return CANONICAL_NAME_PRIORITY["fallback"]

    def _attach_record(self, decision: MatchDecision, rec: dict[str, Any]) -> None:
        master_id = decision.master_id
        source_name = rec["source_name"]
        source_table = rec["source_table"]
        source_record_key = rec["source_record_key"]

        current_master_row = next((r for r in self.db.get_master_rows() if int(r["id"]) == master_id), None)
        current_priority = CANONICAL_NAME_PRIORITY["existing"]
        if current_master_row:
            current_name = str(current_master_row["canonical_name"])
            current_priority = self._name_priority("existing", current_name, current_name)

        candidate_name = self._choose_record_best_name(rec)
        candidate_priority = self._name_priority(source_name, rec.get("inci_name"), rec.get("preferred_name"))
        if candidate_name and candidate_priority > current_priority:
            self.db.update_master_canonical_name(master_id, candidate_name, normalize_name(candidate_name))

        aliases = [rec.get("name"), rec.get("inci_name"), rec.get("preferred_name")] + (rec.get("aliases") or [])
        dedup_aliases = []
        seen = set()
        for alias in aliases:
            if not alias:
                continue
            norm_alias = normalize_name(str(alias))
            if not norm_alias or norm_alias in seen:
                continue
            seen.add(norm_alias)
            dedup_aliases.append(str(alias))

        for alias in dedup_aliases:
            self.db.upsert_alias(master_id, alias, normalize_name(alias), source_name, source_record_key)

        ids = self._record_ids(rec)
        if ids["CAS"]:
            self.db.upsert_identifier(master_id, "CAS", rec["cas"], ids["CAS"], source_name, 1.0)
        if rec.get("ec"):
            self.db.upsert_identifier(master_id, "EC", rec["ec"], normalize_identifier(rec["ec"]), source_name, 0.95)
        if ids["UNII"]:
            self.db.upsert_identifier(master_id, "UNII", rec["unii"], ids["UNII"], source_name, 1.0)
        if ids["PUBCHEM_CID"]:
            self.db.upsert_identifier(master_id, "PUBCHEM_CID", str(rec["pubchem_cid"]), ids["PUBCHEM_CID"], source_name, 1.0)
        if ids["CHEBI_ID"]:
            self.db.upsert_identifier(master_id, "CHEBI_ID", rec["chebi_id"], ids["CHEBI_ID"], source_name, 1.0)

        self.db.upsert_source_link(
            master_id=master_id,
            source_name=source_name,
            source_table=source_table,
            source_record_key=source_record_key,
            raw_ref=rec.get("raw_ref") or "",
            evidence={
                "match_method": decision.match_method,
                "confidence_score": decision.confidence_score,
                "matched_fields": list(decision.evidence.keys()),
                "evidence": decision.evidence,
                "source_evidence": rec.get("evidence") or {},
            },
        )
        self.db.touch_master(master_id)

    def _generate_fuzzy_candidates(self, master_name_index: dict[int, str]) -> int:
        names_by_master = {mid: n for mid, n in master_name_index.items() if n}
        buckets: dict[str, list[tuple[int, str]]] = defaultdict(list)
        for mid, name in names_by_master.items():
            buckets[name[:5]].append((mid, name))

        created = 0
        for _, entries in buckets.items():
            if len(entries) < 2:
                continue
            for i in range(len(entries)):
                for j in range(i + 1, len(entries)):
                    left_id, left_name = entries[i]
                    right_id, right_name = entries[j]
                    if left_id == right_id:
                        continue
                    ratio = difflib.SequenceMatcher(a=left_name, b=right_name).ratio()
                    if ratio >= 0.92 and left_name != right_name:
                        self.db.insert_merge_candidate(
                            left_master_id=left_id,
                            right_master_id=right_id,
                            match_method="fuzzy_name_candidate",
                            confidence_score=round(ratio, 4),
                            evidence={"left_name": left_name, "right_name": right_name},
                        )
                        created += 1
        return created
