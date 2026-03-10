from __future__ import annotations

import csv
import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from ingredient_pipeline.storage.db import Database, utc_now_iso
from ingredient_pipeline.utils.http_client import RateLimitedSession, save_binary_payload, save_json_payload

logger = logging.getLogger(__name__)

COSING_BASE = "https://ec.europa.eu/growth/tools-databases/cosing"
RUNTIME_CONFIG_URL = f"{COSING_BASE}/assets/env-json-config.json"
DEFAULT_ANNEXES = ["II", "III", "IV", "V", "VI"]


@dataclass(slots=True)
class CosingRuntimeConfig:
    eu_search_api_url: str
    eu_search_api_key: str
    descr_file_url: str
    export_to_file_url: str


class CosingClient:
    def __init__(self, http: RateLimitedSession, db: Database, raw_dir: Path):
        self.http = http
        self.db = db
        self.raw_dir = raw_dir
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    def fetch_runtime_config(self, run_id: int) -> CosingRuntimeConfig:
        response = self.http.request("GET", RUNTIME_CONFIG_URL)
        response.raise_for_status()
        payload = response.json()

        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        local_path = self.raw_dir / f"runtime_config_{ts}.json"
        digest = save_json_payload(local_path, payload)

        self.db.insert_cosing_raw(
            {
                "run_id": run_id,
                "source_kind": "runtime_config",
                "source_url": RUNTIME_CONFIG_URL,
                "payload_format": "json",
                "http_status": response.status_code,
                "retrieved_at": utc_now_iso(),
                "sha256": digest,
                "local_path": str(local_path),
                "metadata": {"retrieval_method": "official_runtime_config"},
            }
        )

        return CosingRuntimeConfig(
            eu_search_api_url=payload["euSearchApiUrl"],
            eu_search_api_key=payload["euSearchApiKey"],
            descr_file_url=payload["descrFileUrl"],
            export_to_file_url=payload["exportToFileUrl"],
        )

    def acquire_annex_exports(
        self,
        run_id: int,
        runtime: CosingRuntimeConfig,
        annexes: list[str] | None = None,
        formats: list[str] | None = None,
    ) -> int:
        annexes = annexes or DEFAULT_ANNEXES
        formats = formats or ["csv"]
        acquired = 0

        for annex in annexes:
            for fmt in formats:
                directive = f"annexes/{annex}/export-{fmt}"
                url = f"{runtime.export_to_file_url}api/{directive}"
                response = self.http.request("GET", url)
                if response.status_code >= 400:
                    self.db.insert_failure(
                        run_id=run_id,
                        stage="acquire-cosing",
                        record_key=f"annex:{annex}:{fmt}",
                        source=url,
                        error=f"Failed annex export with status {response.status_code}",
                        details={"annex": annex, "format": fmt},
                        retriable=response.status_code in {429, 500, 502, 503, 504},
                    )
                    continue

                stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                local_path = self.raw_dir / f"annex_{annex}_{stamp}.{fmt}"
                digest = save_binary_payload(local_path, response.content)
                self.db.insert_cosing_raw(
                    {
                        "run_id": run_id,
                        "source_kind": "annex_export",
                        "source_url": url,
                        "payload_format": fmt,
                        "http_status": response.status_code,
                        "retrieved_at": utc_now_iso(),
                        "sha256": digest,
                        "local_path": str(local_path),
                        "metadata": {
                            "annex": annex,
                            "format": fmt,
                            "retrieval_method": "official_annex_export",
                            "content_disposition": response.headers.get("Content-Disposition"),
                        },
                    }
                )
                acquired += 1
                logger.info("Acquired CosIng annex export: annex=%s fmt=%s", annex, fmt)

        return acquired

    def acquire_search_seed(
        self,
        run_id: int,
        runtime: CosingRuntimeConfig,
        page_size: int = 200,
        max_pages: int = 30,
        include_substances: bool = True,
    ) -> int:
        acquired = 0
        scopes = [{"itemType": "ingredient"}]
        if include_substances:
            scopes.append({"itemType": "substance"})

        for scope in scopes:
            item_type = scope["itemType"]
            stage_name = f"cosing_search_{item_type}_page"
            start_page = int(self.db.get_checkpoint(stage_name) or "1")

            for page in range(start_page, max_pages + 1):
                query = {"bool": {"must": [{"term": scope}]}}
                params = {
                    "apiKey": runtime.eu_search_api_key,
                    "text": "*",
                    "pageSize": page_size,
                    "pageNumber": page,
                }

                files = {
                    "query": (None, json.dumps(query), "application/json"),
                    "sort": (None, json.dumps([{"field": "substanceId", "order": "ASC"}]), "application/json"),
                }

                response = self.http.request("POST", runtime.eu_search_api_url, params=params, files=files)
                if response.status_code >= 400:
                    self.db.insert_failure(
                        run_id=run_id,
                        stage="acquire-cosing",
                        record_key=f"search:{item_type}:page:{page}",
                        source=runtime.eu_search_api_url,
                        error=f"CosIng search returned status {response.status_code}",
                        details={"item_type": item_type, "page": page},
                        retriable=response.status_code in {429, 500, 502, 503, 504},
                    )
                    continue

                payload = response.json()
                total_results = int(payload.get("totalResults") or 0)
                results = payload.get("results") or []

                stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                local_path = self.raw_dir / f"search_{item_type}_p{page}_{stamp}.json"
                digest = save_json_payload(local_path, payload)
                self.db.insert_cosing_raw(
                    {
                        "run_id": run_id,
                        "source_kind": "search_api",
                        "source_url": runtime.eu_search_api_url,
                        "payload_format": "json",
                        "http_status": response.status_code,
                        "retrieved_at": utc_now_iso(),
                        "sha256": digest,
                        "local_path": str(local_path),
                        "metadata": {
                            "item_type": item_type,
                            "page": page,
                            "page_size": page_size,
                            "total_results": total_results,
                            "retrieval_method": "official_search_api",
                        },
                    }
                )
                acquired += 1
                self.db.set_checkpoint(stage_name, str(page + 1), run_id)

                if not results:
                    break

        return acquired


class CosingParser:
    def __init__(self, db: Database):
        self.db = db

    def parse_all(self, run_id: int) -> dict[str, int]:
        rows = self.db.list_cosing_raw()
        parsed = 0
        failures = 0

        for row in rows:
            source_kind = row["source_kind"]
            try:
                if source_kind == "search_api":
                    parsed += self._parse_search_json(run_id, row)
                elif source_kind == "annex_export" and row["payload_format"].lower() == "csv":
                    parsed += self._parse_annex_csv(run_id, row)
            except Exception as exc:  # noqa: BLE001
                failures += 1
                self.db.insert_failure(
                    run_id=run_id,
                    stage="parse-cosing",
                    source=row["source_url"],
                    record_key=f"raw:{row['id']}",
                    error=str(exc),
                    details={"raw_id": row["id"], "path": row["local_path"]},
                    retriable=False,
                )

        return {"parsed": parsed, "failures": failures}

    def _parse_search_json(self, run_id: int, raw_row: Any) -> int:
        payload = json.loads(Path(raw_row["local_path"]).read_text(encoding="utf-8"))
        count = 0

        for item in payload.get("results", []):
            metadata = item.get("metadata", {})
            record = self._from_search_metadata(metadata, item, raw_row)
            self.db.upsert_cosing_parsed(record | {"run_id": run_id, "source_raw_id": raw_row["id"], "parsed_at": utc_now_iso()})
            count += 1

        return count

    def _from_search_metadata(self, metadata: dict[str, Any], item: dict[str, Any], raw_row: Any) -> dict[str, Any]:
        def pick(key: str) -> str | None:
            val = metadata.get(key)
            if isinstance(val, list) and val:
                out = val[0]
                return None if out in {"", "-", "[]", None} else str(out)
            return None

        inci_name = pick("inciName") or pick("nameOfCommonIngredientsGlossary")
        ingredient_name = pick("nameOfCommonIngredientsGlossary") or inci_name
        cas_no = pick("casNo")
        ec_no = pick("ecNo")
        item_type = pick("itemType")
        function_names = metadata.get("functionName") or []
        status = pick("status")
        annex_no = pick("annexNo")
        ref_no = pick("refNo") or pick("refNo_digit")
        description = pick("chemicalDescription") or pick("chemicalName")
        substance_id = pick("substanceId")

        unique_basis = substance_id or "|".join(
            [item_type or "", inci_name or "", cas_no or "", ec_no or "", annex_no or "", ref_no or ""]
        )
        record_key = f"cosing:{substance_id}" if substance_id else f"cosing:{hashlib.sha1(unique_basis.encode('utf-8')).hexdigest()}"

        regulations = {
            "relatedRegulations": metadata.get("relatedRegulations") or [],
            "otherRegulations": metadata.get("otherRegulations") or [],
            "sccsOpinion": metadata.get("sccsOpinion") or [],
            "officialJournalPublication": metadata.get("officialJournalPublication") or [],
        }

        return {
            "record_key": record_key,
            "item_type": item_type,
            "ingredient_name": ingredient_name,
            "inci_name": inci_name,
            "cas_no": cas_no,
            "ec_no": ec_no,
            "function_names": json.dumps(function_names, ensure_ascii=False),
            "description": description,
            "annex_no": annex_no,
            "ref_no": ref_no,
            "status": status,
            "regulation_refs": json.dumps(regulations, ensure_ascii=False),
            "source_url": item.get("url") or raw_row["source_url"],
            "provenance": {
                "source": "cosing_search_api",
                "retrieved_at": raw_row["retrieved_at"],
                "raw_id": raw_row["id"],
                "fields_from_metadata": True,
            },
            "raw_record": item,
        }

    def _parse_annex_csv(self, run_id: int, raw_row: Any) -> int:
        path = Path(raw_row["local_path"])
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        header_idx = 0
        for idx, line in enumerate(lines):
            if "CAS Number" in line and ("Reference Number" in line or "Ref." in line):
                header_idx = idx
                break

        df = pd.read_csv(path, skiprows=header_idx)
        df = df.dropna(how="all")

        annex = self._annex_from_url(raw_row["source_url"])
        count = 0
        for _, rec in df.iterrows():
            row = {str(k).strip(): (None if pd.isna(v) else str(v).strip()) for k, v in rec.items()}
            name = (
                row.get("INCI Name/Substance Name")
                or row.get("Substance identification")
                or row.get("Chemical name / INN")
                or row.get("Chemical name / INN / XAN")
                or row.get("Name of Common Ingredients Glossary")
            )
            cas_no = row.get("CAS Number")
            ec_no = row.get("EC Number")
            ref_no = row.get("Reference Number") or row.get("Ref.") or row.get("Ref")

            basis = "|".join([annex or "", ref_no or "", name or "", cas_no or "", ec_no or ""])
            record_key = f"annex:{hashlib.sha1(basis.encode('utf-8')).hexdigest()}"

            parsed_row = {
                "run_id": run_id,
                "source_raw_id": raw_row["id"],
                "record_key": record_key,
                "item_type": "substance",
                "ingredient_name": name,
                "inci_name": name,
                "cas_no": cas_no,
                "ec_no": ec_no,
                "function_names": None,
                "description": row.get("Chemical/IUPAC Name") or row.get("Maximum concentration in ready for use preparation"),
                "annex_no": annex,
                "ref_no": ref_no,
                "status": None,
                "regulation_refs": json.dumps(
                    {
                        "Regulation": row.get("Regulation"),
                        "Other Directives/Regulations": row.get("Other Directives/Regulations"),
                        "SCCS opinions": row.get("SCCS opinions"),
                    },
                    ensure_ascii=False,
                ),
                "source_url": raw_row["source_url"],
                "parsed_at": utc_now_iso(),
                "provenance": {
                    "source": "cosing_annex_export_csv",
                    "retrieved_at": raw_row["retrieved_at"],
                    "raw_id": raw_row["id"],
                },
                "raw_record": row,
            }
            self.db.upsert_cosing_parsed(parsed_row)
            count += 1

        return count

    @staticmethod
    def _annex_from_url(url: str) -> str | None:
        parts = url.strip("/").split("/")
        if "annexes" in parts:
            idx = parts.index("annexes")
            if idx + 1 < len(parts):
                return parts[idx + 1]
        return None
