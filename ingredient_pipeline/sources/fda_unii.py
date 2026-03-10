from __future__ import annotations

import csv
import hashlib
import io
import logging
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup

from ingredient_pipeline.storage.db import Database, utc_now_iso
from ingredient_pipeline.utils.http_client import RateLimitedSession, save_binary_payload

logger = logging.getLogger(__name__)

UNII_ARCHIVE_PAGE = "https://precision.fda.gov/uniisearch/archive"
UNII_LATEST_FILES = [
    "https://precision.fda.gov/uniisearch/archive/latest/UNIIs.zip",
    "https://precision.fda.gov/uniisearch/archive/latest/UNII_Data.zip",
]


def discover_unii_zip_links(page_html: str) -> list[str]:
    soup = BeautifulSoup(page_html, "lxml")
    out: list[str] = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if href.lower().endswith(".zip"):
            if href.startswith("http"):
                out.append(href)
            else:
                out.append(f"https://precision.fda.gov{href}")
    return sorted(set(out))


@dataclass(slots=True)
class FdaUniiDownloadResult:
    files_downloaded: int
    bytes_downloaded: int


class FdaUniiClient:
    def __init__(self, http: RateLimitedSession, db: Database, raw_dir: Path):
        self.http = http
        self.db = db
        self.raw_dir = raw_dir
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    def _discover_links(self) -> list[str]:
        resp = self.http.request("GET", UNII_ARCHIVE_PAGE)
        resp.raise_for_status()
        discovered = discover_unii_zip_links(resp.text)
        latest = [x for x in discovered if "/archive/latest/" in x]
        if latest:
            return latest
        return UNII_LATEST_FILES

    def acquire(self, run_id: int) -> FdaUniiDownloadResult:
        links = self._discover_links()
        if not links:
            links = UNII_LATEST_FILES

        files_downloaded = 0
        bytes_downloaded = 0
        for url in links:
            if not url.lower().endswith(".zip"):
                continue
            if "unii" not in url.lower():
                continue

            resp = self.http.request("GET", url)
            if resp.status_code >= 400:
                self.db.insert_failure(
                    run_id=run_id,
                    stage="acquire-fda-unii",
                    source=url,
                    error=f"Download failed status={resp.status_code}",
                    details={"url": url},
                    retriable=resp.status_code in {429, 500, 502, 503, 504},
                )
                continue

            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            filename = url.rstrip("/").split("/")[-1]
            local_path = self.raw_dir / f"{stamp}_{filename}"
            digest = save_binary_payload(local_path, resp.content)

            self.db.insert_fda_unii_raw(
                {
                    "run_id": run_id,
                    "source_url": url,
                    "payload_format": "zip",
                    "http_status": resp.status_code,
                    "retrieved_at": utc_now_iso(),
                    "sha256": digest,
                    "local_path": str(local_path),
                    "metadata": {
                        "source_page": UNII_ARCHIVE_PAGE,
                        "retrieval_method": "official_bulk_download",
                        "content_disposition": resp.headers.get("Content-Disposition"),
                    },
                }
            )
            files_downloaded += 1
            bytes_downloaded += len(resp.content)

        return FdaUniiDownloadResult(files_downloaded=files_downloaded, bytes_downloaded=bytes_downloaded)


class FdaUniiParser:
    def __init__(self, db: Database):
        self.db = db

    def parse_all(self, run_id: int, resume: bool = True) -> dict[str, int]:
        parsed = 0
        failures = 0
        start_raw_id = int(self.db.get_checkpoint("fda_unii_parse_last_raw_id") or "0") if resume else 0

        for raw in self.db.list_fda_unii_raw():
            raw_id = int(raw["id"])
            if raw_id <= start_raw_id:
                continue

            try:
                parsed += self._parse_zip(run_id, raw)
            except Exception as exc:  # noqa: BLE001
                failures += 1
                self.db.insert_failure(
                    run_id=run_id,
                    stage="parse-fda-unii",
                    record_key=f"fda_unii_raw:{raw_id}",
                    source=raw["source_url"],
                    error=str(exc),
                    details={"raw_id": raw_id, "path": raw["local_path"]},
                    retriable=False,
                )
            finally:
                self.db.set_checkpoint("fda_unii_parse_last_raw_id", str(raw_id), run_id)

        return {"parsed": parsed, "failures": failures}

    def _parse_zip(self, run_id: int, raw: Any) -> int:
        path = Path(raw["local_path"])
        count = 0

        with zipfile.ZipFile(path) as zf:
            members = [n for n in zf.namelist() if n.lower().endswith(".txt")]
            for member in members:
                lower = member.lower()
                if "unii_names" in lower:
                    count += self._parse_names_file(run_id, raw, zf, member)
                elif "unii_records" in lower:
                    count += self._parse_records_file(run_id, raw, zf, member)

        return count

    def _parse_names_file(self, run_id: int, raw: Any, zf: zipfile.ZipFile, member: str) -> int:
        with zf.open(member) as fh:
            text = fh.read().decode("utf-8-sig", errors="replace")

        reader = csv.DictReader(io.StringIO(text), delimiter="\t")
        count = 0
        for rec in reader:
            row = {k: (v.strip() if isinstance(v, str) else v) for k, v in rec.items()}
            name = row.get("Name")
            unii = row.get("UNII")
            preferred = row.get("Display Name")
            name_type = row.get("TYPE")
            basis = "|".join([member, name or "", unii or "", preferred or "", name_type or ""])
            record_key = f"fda_unii:names:{hashlib.sha1(basis.encode('utf-8')).hexdigest()}"

            self.db.upsert_fda_unii_parsed(
                {
                    "run_id": run_id,
                    "source_raw_id": raw["id"],
                    "dataset_kind": "names",
                    "record_key": record_key,
                    "unii": unii,
                    "preferred_name": preferred,
                    "name": name,
                    "name_type": name_type,
                    "cas_number": None,
                    "ec_number": None,
                    "pubchem_cid": None,
                    "inchi_key": None,
                    "smiles": None,
                    "ingredient_type": None,
                    "substance_type": None,
                    "parsed_at": utc_now_iso(),
                    "provenance": {
                        "source": "fda_unii_bulk",
                        "dataset_kind": "names",
                        "member": member,
                        "raw_id": raw["id"],
                    },
                    "raw_record": row,
                }
            )
            count += 1

        return count

    def _parse_records_file(self, run_id: int, raw: Any, zf: zipfile.ZipFile, member: str) -> int:
        with zf.open(member) as fh:
            text = fh.read().decode("utf-8-sig", errors="replace")

        reader = csv.DictReader(io.StringIO(text), delimiter="\t")
        count = 0
        for rec in reader:
            row = {k: (v.strip() if isinstance(v, str) else v) for k, v in rec.items()}
            preferred = row.get("Display Name")
            unii = row.get("UNII")
            basis = "|".join([member, unii or "", preferred or "", row.get("UUID") or ""])
            record_key = f"fda_unii:records:{hashlib.sha1(basis.encode('utf-8')).hexdigest()}"

            self.db.upsert_fda_unii_parsed(
                {
                    "run_id": run_id,
                    "source_raw_id": raw["id"],
                    "dataset_kind": "records",
                    "record_key": record_key,
                    "unii": unii,
                    "preferred_name": preferred,
                    "name": preferred,
                    "name_type": "preferred",
                    "cas_number": row.get("RN"),
                    "ec_number": row.get("EC"),
                    "pubchem_cid": row.get("PUBCHEM"),
                    "inchi_key": row.get("INCHIKEY"),
                    "smiles": row.get("SMILES"),
                    "ingredient_type": row.get("INGREDIENT_TYPE"),
                    "substance_type": row.get("SUBSTANCE_TYPE"),
                    "parsed_at": utc_now_iso(),
                    "provenance": {
                        "source": "fda_unii_bulk",
                        "dataset_kind": "records",
                        "member": member,
                        "raw_id": raw["id"],
                    },
                    "raw_record": row,
                }
            )
            count += 1

        return count
