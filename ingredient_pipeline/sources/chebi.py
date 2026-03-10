from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ingredient_pipeline.storage.db import Database, utc_now_iso
from ingredient_pipeline.utils.http_client import RateLimitedSession, save_json_payload

logger = logging.getLogger(__name__)

CHEBI_BASE = "https://www.ebi.ac.uk/chebi/backend/api/public"


@dataclass(slots=True)
class ChebiAcquireStats:
    acquired: int
    failed: int


class ChebiClient:
    def __init__(self, http: RateLimitedSession, db: Database, raw_dir: Path, enabled: bool):
        self.http = http
        self.db = db
        self.raw_dir = raw_dir
        self.enabled = enabled
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    def acquire(self, run_id: int, id_list_file: Path | None = None, max_queries: int = 200) -> ChebiAcquireStats:
        if not self.enabled:
            logger.info("ChEBI disabled via config (CHEBI_ENABLED=false). Skipping.")
            return ChebiAcquireStats(acquired=0, failed=0)

        if id_list_file:
            seeds = self._load_id_list(id_list_file)
            return self._acquire_by_ids(run_id, seeds)

        seeds = self._seed_terms_from_existing(max_queries=max_queries)
        return self._acquire_by_search_terms(run_id, seeds)

    def _load_id_list(self, path: Path) -> list[str]:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        out = []
        for line in lines:
            s = line.strip()
            if not s:
                continue
            if not s.upper().startswith("CHEBI:"):
                s = f"CHEBI:{s}"
            out.append(s)
        return sorted(set(out))

    def _seed_terms_from_existing(self, max_queries: int) -> list[str]:
        rows = self.db.iter_identity_input_rows()
        seen: set[str] = set()
        out: list[str] = []
        for row in rows:
            for candidate in [row.get("inci_name"), row.get("preferred_name"), row.get("name")]:
                if not candidate:
                    continue
                term = str(candidate).strip()
                if len(term) < 3:
                    continue
                key = term.lower()
                if key in seen:
                    continue
                seen.add(key)
                out.append(term)
                if len(out) >= max_queries:
                    return out
        return out

    def _acquire_by_search_terms(self, run_id: int, terms: list[str]) -> ChebiAcquireStats:
        acquired = 0
        failed = 0
        start_idx = int(self.db.get_checkpoint("chebi_acquire_search_idx") or "0")

        for idx, term in enumerate(terms):
            if idx < start_idx:
                continue
            endpoint = f"{CHEBI_BASE}/es_search/"
            resp = self.http.request("GET", endpoint, params={"term": term, "size": 5, "page": 1})
            if resp.status_code >= 400:
                failed += 1
                self.db.insert_failure(
                    run_id,
                    "acquire-chebi",
                    error=f"Chebi search status={resp.status_code}",
                    source=endpoint,
                    record_key=f"term:{term}",
                    details={"term": term},
                    retriable=resp.status_code in {429, 500, 502, 503, 504},
                )
                self.db.set_checkpoint("chebi_acquire_search_idx", str(idx + 1), run_id)
                continue

            payload = resp.json()
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            local_path = self.raw_dir / f"search_{idx}_{stamp}.json"
            digest = save_json_payload(local_path, payload)
            self.db.insert_chebi_raw(
                {
                    "run_id": run_id,
                    "source_url": endpoint,
                    "payload_format": "json",
                    "http_status": resp.status_code,
                    "retrieved_at": utc_now_iso(),
                    "sha256": digest,
                    "local_path": str(local_path),
                    "metadata": {"mode": "search", "term": term, "index": idx},
                }
            )
            acquired += 1
            self.db.set_checkpoint("chebi_acquire_search_idx", str(idx + 1), run_id)

        return ChebiAcquireStats(acquired=acquired, failed=failed)

    def _acquire_by_ids(self, run_id: int, chebi_ids: list[str]) -> ChebiAcquireStats:
        acquired = 0
        failed = 0
        start_idx = int(self.db.get_checkpoint("chebi_acquire_id_idx") or "0")

        for idx, chebi_id in enumerate(chebi_ids):
            if idx < start_idx:
                continue
            endpoint = f"{CHEBI_BASE}/compound/{chebi_id}/"
            resp = self.http.request("GET", endpoint)
            if resp.status_code >= 400:
                failed += 1
                self.db.insert_failure(
                    run_id,
                    "acquire-chebi",
                    error=f"Chebi compound status={resp.status_code}",
                    source=endpoint,
                    record_key=f"chebi_id:{chebi_id}",
                    details={"chebi_id": chebi_id},
                    retriable=resp.status_code in {429, 500, 502, 503, 504},
                )
                self.db.set_checkpoint("chebi_acquire_id_idx", str(idx + 1), run_id)
                continue

            payload = resp.json()
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            local_path = self.raw_dir / f"compound_{chebi_id.replace(':', '_')}_{stamp}.json"
            digest = save_json_payload(local_path, payload)
            self.db.insert_chebi_raw(
                {
                    "run_id": run_id,
                    "source_url": endpoint,
                    "payload_format": "json",
                    "http_status": resp.status_code,
                    "retrieved_at": utc_now_iso(),
                    "sha256": digest,
                    "local_path": str(local_path),
                    "metadata": {"mode": "id", "chebi_id": chebi_id, "index": idx},
                }
            )
            acquired += 1
            self.db.set_checkpoint("chebi_acquire_id_idx", str(idx + 1), run_id)

        return ChebiAcquireStats(acquired=acquired, failed=failed)


class ChebiParser:
    def __init__(self, db: Database):
        self.db = db

    def parse_all(self, run_id: int, resume: bool = True) -> dict[str, int]:
        parsed = 0
        failures = 0
        start_raw_id = int(self.db.get_checkpoint("chebi_parse_last_raw_id") or "0") if resume else 0

        for raw in self.db.list_chebi_raw():
            raw_id = int(raw["id"])
            if raw_id <= start_raw_id:
                continue
            try:
                payload = json.loads(Path(raw["local_path"]).read_text(encoding="utf-8"))
                mode = json.loads(raw["metadata_json"] or "{}").get("mode")
                if mode == "search":
                    parsed += self._parse_search_payload(run_id, raw, payload)
                else:
                    parsed += self._parse_compound_payload(run_id, raw, payload)
            except Exception as exc:  # noqa: BLE001
                failures += 1
                self.db.insert_failure(
                    run_id,
                    "parse-chebi",
                    error=str(exc),
                    source=raw["source_url"],
                    record_key=f"chebi_raw:{raw_id}",
                    details={"raw_id": raw_id},
                    retriable=False,
                )
            finally:
                self.db.set_checkpoint("chebi_parse_last_raw_id", str(raw_id), run_id)

        return {"parsed": parsed, "failures": failures}

    def _parse_search_payload(self, run_id: int, raw: Any, payload: dict[str, Any]) -> int:
        count = 0
        for hit in payload.get("results") or []:
            src = hit.get("_source") or {}
            chebi_id = src.get("chebi_accession")
            if not chebi_id:
                continue
            row = {
                "id": src.get("id") or hit.get("_id"),
                "chebi_accession": chebi_id,
                "name": src.get("name"),
                "definition": src.get("definition"),
                "default_structure": {
                    "smiles": src.get("smiles"),
                    "standard_inchi_key": src.get("inchikey"),
                },
                "database_accessions": {},
                "roles_classification": [],
                "names": {},
            }
            self._upsert_chebi_row(run_id, raw, row)
            count += 1
        return count

    def _parse_compound_payload(self, run_id: int, raw: Any, payload: dict[str, Any]) -> int:
        self._upsert_chebi_row(run_id, raw, payload)
        return 1

    def _upsert_chebi_row(self, run_id: int, raw: Any, row: dict[str, Any]) -> None:
        chebi_id = row.get("chebi_accession") or row.get("chebi_id") or ""
        preferred_name = row.get("name")

        synonyms: list[str] = []
        names = row.get("names") or {}
        if isinstance(names, dict):
            for _, vals in names.items():
                for x in vals:
                    if isinstance(x, dict) and x.get("name"):
                        synonyms.append(x["name"])

        cas_number = None
        pubchem_cid = None
        xrefs = []
        db_accessions = row.get("database_accessions") or {}
        if isinstance(db_accessions, dict):
            for xref_type, entries in db_accessions.items():
                for e in entries or []:
                    acc = e.get("accession_number") if isinstance(e, dict) else None
                    src = e.get("source_name") if isinstance(e, dict) else None
                    if acc:
                        xrefs.append({"type": xref_type, "source": src, "value": acc})
                    if xref_type == "CAS" and not cas_number and acc:
                        cas_number = acc
                    if src and str(src).lower().startswith("pubchem") and not pubchem_cid and acc:
                        pubchem_cid = acc

        roles = [r.get("name") for r in (row.get("roles_classification") or []) if isinstance(r, dict) and r.get("name")]
        smiles = ((row.get("default_structure") or {}) if isinstance(row.get("default_structure"), dict) else {}).get("smiles")
        inchi_key = ((row.get("default_structure") or {}) if isinstance(row.get("default_structure"), dict) else {}).get("standard_inchi_key")
        source_url = f"https://www.ebi.ac.uk/chebi/searchId.do?chebiId={chebi_id}"

        basis = "|".join([chebi_id or "", preferred_name or "", cas_number or "", pubchem_cid or ""])
        record_key = f"chebi:{hashlib.sha1(basis.encode('utf-8')).hexdigest()}"

        self.db.upsert_chebi_parsed(
            {
                "run_id": run_id,
                "source_raw_id": raw["id"],
                "record_key": record_key,
                "chebi_id": chebi_id,
                "preferred_name": preferred_name,
                "synonyms": sorted(set([s for s in synonyms if s])),
                "definition": row.get("definition"),
                "cas_number": cas_number,
                "pubchem_cid": pubchem_cid,
                "inchi_key": inchi_key,
                "smiles": smiles,
                "roles": roles,
                "xrefs": xrefs,
                "source_url": source_url,
                "parsed_at": utc_now_iso(),
                "provenance": {
                    "source": "chebi_api",
                    "raw_id": raw["id"],
                    "retrieved_at": raw["retrieved_at"],
                },
                "raw_record": row,
            }
        )
