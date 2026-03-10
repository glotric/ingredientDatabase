from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

from ingredient_pipeline.storage.db import Database, utc_now_iso
from ingredient_pipeline.utils.http_client import RateLimitedSession, save_json_payload

logger = logging.getLogger(__name__)

PUBCHEM_BASE = "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
PROPERTY_FIELDS = [
    "CanonicalSMILES",
    "IsomericSMILES",
    "InChI",
    "InChIKey",
    "MolecularFormula",
    "MolecularWeight",
    "IUPACName",
    "XLogP",
]


@dataclass(slots=True)
class PubChemMatchResult:
    match_status: str
    confidence: float
    lookup_method: str
    cid: int | None
    raw_ref_id: int | None
    details: dict[str, Any]


class PubChemEnricher:
    def __init__(self, http: RateLimitedSession, db: Database, raw_dir: Path):
        self.http = http
        self.db = db
        self.raw_dir = raw_dir
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    def enrich_records(self, run_id: int, records: list[Any], resume: bool = True) -> dict[str, int]:
        matched = 0
        unmatched = 0
        ambiguous = 0
        failed = 0

        start_id = int(self.db.get_checkpoint("pubchem_enrich_last_parsed_id") or "0") if resume else 0

        for rec in records:
            parsed_id = int(rec["id"])
            if parsed_id <= start_id:
                continue

            record_key = rec["record_key"]
            try:
                result = self._enrich_one(run_id, rec)
                self._persist_enrichment(run_id, rec, result)

                if result.match_status == "matched":
                    matched += 1
                elif result.match_status == "ambiguous":
                    ambiguous += 1
                else:
                    unmatched += 1

            except Exception as exc:  # noqa: BLE001
                failed += 1
                self.db.insert_failure(
                    run_id=run_id,
                    stage="enrich-pubchem",
                    record_key=record_key,
                    source="pubchem",
                    error=str(exc),
                    details={"parsed_id": parsed_id},
                    retriable=True,
                )
            finally:
                self.db.set_checkpoint("pubchem_enrich_last_parsed_id", str(parsed_id), run_id)

        return {
            "matched": matched,
            "unmatched": unmatched,
            "ambiguous": ambiguous,
            "failed": failed,
        }

    def _enrich_one(self, run_id: int, rec: Any) -> PubChemMatchResult:
        candidates: list[tuple[str, str]] = []
        if rec["cas_no"]:
            candidates.append(("exact_cas", str(rec["cas_no"])))
        if rec["inci_name"]:
            candidates.append(("exact_name", str(rec["inci_name"])))
        if rec["ingredient_name"] and rec["ingredient_name"] != rec["inci_name"]:
            candidates.append(("synonym_match", str(rec["ingredient_name"])))

        for method, query in candidates:
            cids, raw_ref = self._lookup_cids(run_id, rec["record_key"], method, query)
            if not cids:
                continue
            if len(cids) > 1:
                return PubChemMatchResult(
                    match_status="ambiguous",
                    confidence=0.4,
                    lookup_method=method,
                    cid=None,
                    raw_ref_id=raw_ref,
                    details={"candidate_cids": cids, "query": query},
                )

            cid = cids[0]
            props, props_raw_ref = self._fetch_properties(run_id, rec["record_key"], cid)
            syns, _ = self._fetch_synonyms(run_id, rec["record_key"], cid)
            details = {"properties": props, "synonyms": syns}

            return PubChemMatchResult(
                match_status="matched",
                confidence=self._confidence_for_method(method),
                lookup_method=method,
                cid=cid,
                raw_ref_id=props_raw_ref or raw_ref,
                details=details,
            )

        return PubChemMatchResult(
            match_status="no_match",
            confidence=0.0,
            lookup_method="none",
            cid=None,
            raw_ref_id=None,
            details={"reason": "No CID found for CAS/name/synonym strategies"},
        )

    def _lookup_cids(self, run_id: int, record_key: str, query_type: str, query: str) -> tuple[list[int], int | None]:
        safe_query = quote(query)
        endpoint = f"{PUBCHEM_BASE}/compound/name/{safe_query}/cids/JSON"
        response = self.http.request("GET", endpoint)

        payload: dict[str, Any]
        if response.status_code == 404:
            payload = {"IdentifierList": {"CID": []}}
        else:
            response.raise_for_status()
            payload = response.json()

        raw_ref = self._store_raw(run_id, record_key, query, query_type, endpoint, response.status_code, payload)
        cids = payload.get("IdentifierList", {}).get("CID") or []
        return [int(c) for c in cids], raw_ref

    def _fetch_properties(self, run_id: int, record_key: str, cid: int) -> tuple[dict[str, Any], int | None]:
        fields = ",".join(PROPERTY_FIELDS)
        endpoint = f"{PUBCHEM_BASE}/compound/cid/{cid}/property/{fields}/JSON"
        response = self.http.request("GET", endpoint)
        response.raise_for_status()
        payload = response.json()
        raw_ref = self._store_raw(run_id, record_key, str(cid), "cid_property", endpoint, response.status_code, payload)

        props = (payload.get("PropertyTable", {}).get("Properties") or [{}])[0]
        return props, raw_ref

    def _fetch_synonyms(self, run_id: int, record_key: str, cid: int) -> tuple[list[str], int | None]:
        endpoint = f"{PUBCHEM_BASE}/compound/cid/{cid}/synonyms/JSON"
        response = self.http.request("GET", endpoint)

        if response.status_code == 404:
            payload = {"InformationList": {"Information": [{"Synonym": []}]}}
        else:
            response.raise_for_status()
            payload = response.json()

        raw_ref = self._store_raw(run_id, record_key, str(cid), "cid_synonyms", endpoint, response.status_code, payload)
        info = payload.get("InformationList", {}).get("Information") or []
        if not info:
            return [], raw_ref
        syns = info[0].get("Synonym") or []
        return [str(x) for x in syns], raw_ref

    def _store_raw(
        self,
        run_id: int,
        record_key: str,
        query: str,
        query_type: str,
        endpoint: str,
        http_status: int,
        payload: dict[str, Any],
    ) -> int:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        local_path = self.raw_dir / f"{record_key.replace(':', '_')}_{query_type}_{stamp}.json"
        save_json_payload(local_path, payload)

        return self.db.insert_pubchem_raw(
            {
                "run_id": run_id,
                "record_key": record_key,
                "query": query,
                "query_type": query_type,
                "endpoint": endpoint,
                "http_status": http_status,
                "retrieved_at": utc_now_iso(),
                "local_path": str(local_path),
                "response": payload,
            }
        )

    def _persist_enrichment(self, run_id: int, rec: Any, result: PubChemMatchResult) -> None:
        if result.match_status == "no_match":
            self.db.upsert_unmatched(
                run_id=run_id,
                record_key=rec["record_key"],
                reason=result.details.get("reason", "no_match"),
                query_tried=json.dumps(
                    {
                        "cas": rec["cas_no"],
                        "inci": rec["inci_name"],
                        "ingredient": rec["ingredient_name"],
                    }
                ),
                details=result.details,
            )

        props = result.details.get("properties", {})
        self.db.upsert_enrichment(
            {
                "run_id": run_id,
                "record_key": rec["record_key"],
                "match_status": result.match_status,
                "confidence": result.confidence,
                "lookup_method": result.lookup_method,
                "cid": result.cid,
                "canonical_smiles": props.get("CanonicalSMILES"),
                "isomeric_smiles": props.get("IsomericSMILES"),
                "inchi": props.get("InChI"),
                "inchikey": props.get("InChIKey"),
                "molecular_formula": props.get("MolecularFormula"),
                "molecular_weight": props.get("MolecularWeight"),
                "iupac_name": props.get("IUPACName"),
                "synonyms": result.details.get("synonyms", []),
                "xlogp": props.get("XLogP"),
                "computed_props": props,
                "source_url": "https://pubchem.ncbi.nlm.nih.gov/docs/pug-rest",
                "raw_ref_id": result.raw_ref_id,
                "enriched_at": utc_now_iso(),
                "provenance": {
                    "source": "pubchem_pug_rest",
                    "lookup_method": result.lookup_method,
                    "match_status": result.match_status,
                    "ingestion_timestamp": utc_now_iso(),
                },
            }
        )

    @staticmethod
    def _confidence_for_method(method: str) -> float:
        if method == "exact_cas":
            return 0.99
        if method == "exact_name":
            return 0.9
        if method == "synonym_match":
            return 0.75
        return 0.0
