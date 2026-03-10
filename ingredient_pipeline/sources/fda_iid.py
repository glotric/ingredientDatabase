from __future__ import annotations

import csv
import hashlib
import io
import logging
import os
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup

from ingredient_pipeline.storage.db import Database, utc_now_iso
from ingredient_pipeline.utils.http_client import RateLimitedSession, save_binary_payload

logger = logging.getLogger(__name__)

FDA_IID_PAGE = "https://www.fda.gov/drugs/drug-approvals-and-databases/inactive-ingredients-database-download"
FDA_IID_FALLBACK_DOWNLOAD = "https://www.fda.gov/media/190589/download"


@dataclass(slots=True)
class FdaIidDownloadInfo:
    source_page_url: str
    download_url: str


def _normalize_cas(value: str | None) -> str | None:
    if not value:
        return None
    out = value.strip()
    if not out:
        return None
    return out


def discover_iid_download_url(page_html: str, page_url: str = FDA_IID_PAGE) -> str | None:
    soup = BeautifulSoup(page_html, "lxml")
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        text = (a.get_text(" ", strip=True) or "").lower()
        if "/media/" in href and "/download" in href:
            if "inactive ingredient" in text or "download" in text:
                if href.startswith("http"):
                    return href
                return f"https://www.fda.gov{href}"
    return None


class FdaIidClient:
    def __init__(self, http: RateLimitedSession, db: Database, raw_dir: Path):
        self.http = http
        self.db = db
        self.raw_dir = raw_dir
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    def discover_download(self, run_id: int) -> FdaIidDownloadInfo:
        override = os.getenv("FDA_IID_DOWNLOAD_URL")
        if override:
            return FdaIidDownloadInfo(source_page_url=FDA_IID_PAGE, download_url=override)
        resp = self.http.request("GET", FDA_IID_PAGE)
        resp.raise_for_status()
        download_url = discover_iid_download_url(resp.text, FDA_IID_PAGE) or FDA_IID_FALLBACK_DOWNLOAD
        return FdaIidDownloadInfo(source_page_url=FDA_IID_PAGE, download_url=download_url)

    def acquire(self, run_id: int) -> dict[str, Any]:
        info = self.discover_download(run_id)
        resp = self.http.request("GET", info.download_url)
        resp.raise_for_status()

        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        ext = "zip"
        local_path = self.raw_dir / f"fda_iid_{stamp}.{ext}"
        digest = save_binary_payload(local_path, resp.content)

        self.db.insert_fda_iid_raw(
            {
                "run_id": run_id,
                "source_url": info.download_url,
                "payload_format": ext,
                "http_status": resp.status_code,
                "retrieved_at": utc_now_iso(),
                "sha256": digest,
                "local_path": str(local_path),
                "metadata": {
                    "source_page": info.source_page_url,
                    "retrieval_method": "official_bulk_download",
                    "content_disposition": resp.headers.get("Content-Disposition"),
                },
            }
        )
        return {"download_url": info.download_url, "bytes": len(resp.content), "sha256": digest}


class FdaIidParser:
    def __init__(self, db: Database):
        self.db = db

    def parse_all(self, run_id: int, resume: bool = True) -> dict[str, int]:
        parsed = 0
        failures = 0
        start_raw_id = int(self.db.get_checkpoint("fda_iid_parse_last_raw_id") or "0") if resume else 0

        for raw in self.db.list_fda_iid_raw():
            raw_id = int(raw["id"])
            if raw_id <= start_raw_id:
                continue
            try:
                parsed += self._parse_zip(run_id, raw)
            except Exception as exc:  # noqa: BLE001
                failures += 1
                self.db.insert_failure(
                    run_id=run_id,
                    stage="parse-fda-iid",
                    record_key=f"fda_iid_raw:{raw_id}",
                    source=raw["source_url"],
                    error=str(exc),
                    details={"raw_id": raw_id, "path": raw["local_path"]},
                    retriable=False,
                )
            finally:
                self.db.set_checkpoint("fda_iid_parse_last_raw_id", str(raw_id), run_id)

        return {"parsed": parsed, "failures": failures}

    def _parse_zip(self, run_id: int, raw: Any) -> int:
        path = Path(raw["local_path"])
        count = 0

        with zipfile.ZipFile(path) as zf:
            csv_name = next((n for n in zf.namelist() if n.lower().endswith(".csv") and "iir_ocomm" in n.lower()), None)
            if not csv_name:
                csv_name = next((n for n in zf.namelist() if n.lower().endswith(".csv")), None)
            if not csv_name:
                raise ValueError("No CSV member found in IID zip")

            with zf.open(csv_name) as member:
                text = member.read().decode("utf-8-sig", errors="replace")
                reader = csv.DictReader(io.StringIO(text))
                for rec in reader:
                    row = {k: (v.strip() if isinstance(v, str) else v) for k, v in rec.items()}
                    name = row.get("INGREDIENT_NAME")
                    route = row.get("ROUTE")
                    dosage_form = row.get("DOSAGE_FORM")
                    cas = _normalize_cas(row.get("CAS_NUMBER"))
                    unii = (row.get("UNII") or "").strip() or None

                    basis = "|".join([name or "", route or "", dosage_form or "", cas or "", unii or "", row.get("POTENCY_AMOUNT") or "", row.get("POTENCY_UNIT") or ""])
                    record_key = f"fda_iid:{hashlib.sha1(basis.encode('utf-8')).hexdigest()}"

                    self.db.upsert_fda_iid_parsed(
                        {
                            "run_id": run_id,
                            "source_raw_id": raw["id"],
                            "record_key": record_key,
                            "inactive_ingredient": name,
                            "route": route,
                            "dosage_form": dosage_form,
                            "cas_number": cas,
                            "unii": unii,
                            "potency_amount": row.get("POTENCY_AMOUNT"),
                            "potency_unit": row.get("POTENCY_UNIT"),
                            "maximum_daily_exposure": row.get("MAXIMUM_DAILY_EXPOSURE"),
                            "maximum_daily_exposure_unit": row.get("MAXIMUM_DAILY_EXPOSURE_UNIT"),
                            "record_updated": row.get("RECORD_UPDATED"),
                            "parsed_at": utc_now_iso(),
                            "provenance": {
                                "source": "fda_iid_bulk",
                                "raw_id": raw["id"],
                                "member": csv_name,
                                "retrieved_at": raw["retrieved_at"],
                            },
                            "raw_record": row,
                        }
                    )
                    count += 1

        return count
