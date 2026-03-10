from __future__ import annotations

import csv
import hashlib
import json
import logging
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from ingredient_pipeline.storage.db import Database, utc_now_iso
from ingredient_pipeline.utils.http_client import sha256_bytes

logger = logging.getLogger(__name__)


def _norm(s: str) -> str:
    return "".join(ch for ch in s.lower().strip() if ch.isalnum())


@dataclass(slots=True)
class PcpcImportResult:
    stored_path: str
    sha256: str


class PcpcClient:
    """PCPC/wINCI adapter for licensed manual exports only."""

    def __init__(self, db: Database, raw_dir: Path):
        self.db = db
        self.raw_dir = raw_dir
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    def import_file(self, run_id: int, file_path: Path) -> PcpcImportResult:
        source = file_path.resolve()
        if not source.exists() or not source.is_file():
            raise FileNotFoundError(f"PCPC file not found: {source}")

        stamp = utc_now_iso().replace(":", "").replace("+", "_")
        target = self.raw_dir / f"pcpc_{stamp}_{source.name}"
        shutil.copy2(source, target)

        digest = sha256_bytes(target.read_bytes())
        ext = source.suffix.lower().lstrip(".") or "bin"
        self.db.insert_pcpc_raw(
            {
                "run_id": run_id,
                "source_url": f"file://{source}",
                "payload_format": ext,
                "http_status": None,
                "retrieved_at": utc_now_iso(),
                "sha256": digest,
                "local_path": str(target),
                "metadata": {
                    "import_mode": "manual_licensed_export",
                    "original_filename": source.name,
                },
            }
        )
        return PcpcImportResult(stored_path=str(target), sha256=digest)


class PcpcParser:
    def __init__(self, db: Database):
        self.db = db

    def parse_all(self, run_id: int, resume: bool = True) -> dict[str, int]:
        parsed = 0
        warnings = 0
        failures = 0
        start_raw_id = int(self.db.get_checkpoint("pcpc_parse_last_raw_id") or "0") if resume else 0

        for raw in self.db.list_pcpc_raw():
            raw_id = int(raw["id"])
            if raw_id <= start_raw_id:
                continue
            try:
                p, w = self._parse_one(run_id, raw)
                parsed += p
                warnings += w
            except Exception as exc:  # noqa: BLE001
                failures += 1
                self.db.insert_failure(
                    run_id,
                    "parse-pcpc",
                    error=str(exc),
                    source=raw["source_url"],
                    record_key=f"pcpc_raw:{raw_id}",
                    details={"raw_id": raw_id, "path": raw["local_path"]},
                    retriable=False,
                )
            finally:
                self.db.set_checkpoint("pcpc_parse_last_raw_id", str(raw_id), run_id)

        return {"parsed": parsed, "mapping_warnings": warnings, "failures": failures}

    def _read_table(self, path: Path) -> list[dict[str, Any]]:
        ext = path.suffix.lower()
        if ext == ".csv":
            return pd.read_csv(path).to_dict(orient="records")
        if ext == ".tsv":
            return pd.read_csv(path, sep="\t").to_dict(orient="records")
        if ext in {".xlsx", ".xls"}:
            return pd.read_excel(path).to_dict(orient="records")
        if ext == ".json":
            payload = json.loads(path.read_text(encoding="utf-8", errors="replace"))
            if isinstance(payload, dict):
                if "rows" in payload and isinstance(payload["rows"], list):
                    return payload["rows"]
                return [payload]
            if isinstance(payload, list):
                return payload
        raise ValueError(f"Unsupported PCPC file format: {ext}")

    def _parse_one(self, run_id: int, raw: Any) -> tuple[int, int]:
        rows = self._read_table(Path(raw["local_path"]))
        if not rows:
            return 0, 0

        column_mapping, mapping_warnings = self._detect_mapping(rows[0].keys())
        count = 0
        warnings_count = 0

        for rec in rows:
            row = {str(k): rec.get(k) for k in rec.keys()}
            mapped, warn = self._map_row(row, column_mapping)
            warnings_count += len(warn)
            mapping_warnings_all = sorted(set(mapping_warnings + warn))

            basis = "|".join(
                [
                    mapped.get("inci_name") or "",
                    mapped.get("preferred_name") or "",
                    mapped.get("cas_number") or "",
                    ",".join(mapped.get("synonyms") or []),
                ]
            )
            record_key = f"pcpc:{hashlib.sha1(basis.encode('utf-8')).hexdigest()}"

            self.db.upsert_pcpc_parsed(
                {
                    "run_id": run_id,
                    "source_raw_id": raw["id"],
                    "record_key": record_key,
                    "inci_name": mapped.get("inci_name"),
                    "preferred_name": mapped.get("preferred_name"),
                    "cas_number": mapped.get("cas_number"),
                    "synonyms": mapped.get("synonyms") or [],
                    "functions": mapped.get("functions") or [],
                    "description": mapped.get("description"),
                    "regulatory_notes": mapped.get("regulatory_notes"),
                    "column_mapping": column_mapping,
                    "mapping_warnings": mapping_warnings_all,
                    "parsed_at": utc_now_iso(),
                    "provenance": {
                        "source": "pcpc_manual_import",
                        "raw_id": raw["id"],
                        "retrieved_at": raw["retrieved_at"],
                    },
                    "raw_record": row,
                }
            )
            count += 1

        return count, warnings_count

    def _detect_mapping(self, columns: Any) -> tuple[dict[str, str], list[str]]:
        cols = [str(c) for c in columns]
        norms = {c: _norm(c) for c in cols}

        def pick(*needles: str) -> str | None:
            for col, n in norms.items():
                if any(x in n for x in needles):
                    return col
            return None

        mapping = {
            "inci_name": pick("inci"),
            "preferred_name": pick("preferredname", "ingredientname", "commonname", "name"),
            "cas_number": pick("cas", "casnumber"),
            "synonyms": pick("synonym", "aliases", "aka"),
            "functions": pick("function", "use", "purpose"),
            "description": pick("description", "definition"),
            "regulatory_notes": pick("regulatory", "note", "restriction"),
        }

        warnings = []
        for k, v in mapping.items():
            if v is None:
                warnings.append(f"No clear column found for {k}")
        return mapping, warnings

    def _split_multi(self, value: Any) -> list[str]:
        if value is None:
            return []
        text = str(value).strip()
        if not text:
            return []
        if ";" in text:
            return [x.strip() for x in text.split(";") if x.strip()]
        if "|" in text:
            return [x.strip() for x in text.split("|") if x.strip()]
        return [text]

    def _map_row(self, row: dict[str, Any], mapping: dict[str, str | None]) -> tuple[dict[str, Any], list[str]]:
        warnings: list[str] = []

        def get(field: str) -> Any:
            col = mapping.get(field)
            if col is None:
                return None
            return row.get(col)

        inci_name = (str(get("inci_name")).strip() if get("inci_name") is not None else None) or None
        preferred_name = (str(get("preferred_name")).strip() if get("preferred_name") is not None else None) or None
        cas_number = (str(get("cas_number")).strip() if get("cas_number") is not None else None) or None
        synonyms = self._split_multi(get("synonyms"))
        functions = self._split_multi(get("functions"))
        description = (str(get("description")).strip() if get("description") is not None else None) or None
        regulatory_notes = (str(get("regulatory_notes")).strip() if get("regulatory_notes") is not None else None) or None

        if not inci_name and not preferred_name:
            warnings.append("Neither INCI nor preferred name mapped for row")

        return (
            {
                "inci_name": inci_name,
                "preferred_name": preferred_name,
                "cas_number": cas_number,
                "synonyms": synonyms,
                "functions": functions,
                "description": description,
                "regulatory_notes": regulatory_notes,
            },
            warnings,
        )
