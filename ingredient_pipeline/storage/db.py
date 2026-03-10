from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True)
class RunContext:
    run_id: int
    command: str


class Database:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;

                CREATE TABLE IF NOT EXISTS acquisition_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    command TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    summary_json TEXT,
                    notes TEXT
                );

                CREATE TABLE IF NOT EXISTS normalization_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    summary_json TEXT,
                    notes TEXT
                );

                CREATE TABLE IF NOT EXISTS checkpoints (
                    stage TEXT PRIMARY KEY,
                    cursor_value TEXT,
                    run_id INTEGER,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES acquisition_runs(id)
                );

                CREATE TABLE IF NOT EXISTS cosing_raw (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    source_kind TEXT NOT NULL,
                    source_url TEXT NOT NULL,
                    payload_format TEXT NOT NULL,
                    http_status INTEGER,
                    retrieved_at TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    local_path TEXT NOT NULL,
                    metadata_json TEXT,
                    UNIQUE(source_url, sha256),
                    FOREIGN KEY(run_id) REFERENCES acquisition_runs(id)
                );

                CREATE TABLE IF NOT EXISTS cosing_parsed (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    source_raw_id INTEGER,
                    record_key TEXT NOT NULL UNIQUE,
                    item_type TEXT,
                    ingredient_name TEXT,
                    inci_name TEXT,
                    cas_no TEXT,
                    ec_no TEXT,
                    function_names TEXT,
                    description TEXT,
                    annex_no TEXT,
                    ref_no TEXT,
                    status TEXT,
                    regulation_refs TEXT,
                    source_url TEXT,
                    parsed_at TEXT NOT NULL,
                    provenance_json TEXT,
                    raw_record_json TEXT,
                    FOREIGN KEY(run_id) REFERENCES acquisition_runs(id),
                    FOREIGN KEY(source_raw_id) REFERENCES cosing_raw(id)
                );

                CREATE TABLE IF NOT EXISTS pubchem_raw (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    record_key TEXT NOT NULL,
                    query TEXT,
                    query_type TEXT,
                    endpoint TEXT,
                    http_status INTEGER,
                    retrieved_at TEXT NOT NULL,
                    local_path TEXT,
                    response_json TEXT,
                    FOREIGN KEY(run_id) REFERENCES acquisition_runs(id)
                );

                CREATE TABLE IF NOT EXISTS ingredient_enrichment (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    record_key TEXT NOT NULL UNIQUE,
                    match_status TEXT NOT NULL,
                    confidence REAL,
                    lookup_method TEXT,
                    cid INTEGER,
                    canonical_smiles TEXT,
                    isomeric_smiles TEXT,
                    inchi TEXT,
                    inchikey TEXT,
                    molecular_formula TEXT,
                    molecular_weight REAL,
                    iupac_name TEXT,
                    synonyms_json TEXT,
                    xlogp REAL,
                    computed_props_json TEXT,
                    source_url TEXT,
                    raw_ref_id INTEGER,
                    enriched_at TEXT NOT NULL,
                    provenance_json TEXT,
                    FOREIGN KEY(run_id) REFERENCES acquisition_runs(id),
                    FOREIGN KEY(raw_ref_id) REFERENCES pubchem_raw(id)
                );

                CREATE TABLE IF NOT EXISTS fda_iid_raw (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    source_url TEXT NOT NULL,
                    payload_format TEXT NOT NULL,
                    http_status INTEGER,
                    retrieved_at TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    local_path TEXT NOT NULL,
                    metadata_json TEXT,
                    UNIQUE(source_url, sha256),
                    FOREIGN KEY(run_id) REFERENCES acquisition_runs(id)
                );

                CREATE TABLE IF NOT EXISTS fda_iid_parsed (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    source_raw_id INTEGER,
                    record_key TEXT NOT NULL UNIQUE,
                    inactive_ingredient TEXT,
                    route TEXT,
                    dosage_form TEXT,
                    cas_number TEXT,
                    unii TEXT,
                    potency_amount TEXT,
                    potency_unit TEXT,
                    maximum_daily_exposure TEXT,
                    maximum_daily_exposure_unit TEXT,
                    record_updated TEXT,
                    parsed_at TEXT NOT NULL,
                    provenance_json TEXT,
                    raw_record_json TEXT,
                    FOREIGN KEY(run_id) REFERENCES acquisition_runs(id),
                    FOREIGN KEY(source_raw_id) REFERENCES fda_iid_raw(id)
                );

                CREATE TABLE IF NOT EXISTS fda_unii_raw (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    source_url TEXT NOT NULL,
                    payload_format TEXT NOT NULL,
                    http_status INTEGER,
                    retrieved_at TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    local_path TEXT NOT NULL,
                    metadata_json TEXT,
                    UNIQUE(source_url, sha256),
                    FOREIGN KEY(run_id) REFERENCES acquisition_runs(id)
                );

                CREATE TABLE IF NOT EXISTS fda_unii_parsed (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    source_raw_id INTEGER,
                    dataset_kind TEXT NOT NULL,
                    record_key TEXT NOT NULL UNIQUE,
                    unii TEXT,
                    preferred_name TEXT,
                    name TEXT,
                    name_type TEXT,
                    cas_number TEXT,
                    ec_number TEXT,
                    pubchem_cid TEXT,
                    inchi_key TEXT,
                    smiles TEXT,
                    ingredient_type TEXT,
                    substance_type TEXT,
                    parsed_at TEXT NOT NULL,
                    provenance_json TEXT,
                    raw_record_json TEXT,
                    FOREIGN KEY(run_id) REFERENCES acquisition_runs(id),
                    FOREIGN KEY(source_raw_id) REFERENCES fda_unii_raw(id)
                );

                CREATE TABLE IF NOT EXISTS chebi_raw (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    source_url TEXT NOT NULL,
                    payload_format TEXT NOT NULL,
                    http_status INTEGER,
                    retrieved_at TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    local_path TEXT NOT NULL,
                    metadata_json TEXT,
                    UNIQUE(source_url, sha256),
                    FOREIGN KEY(run_id) REFERENCES acquisition_runs(id)
                );

                CREATE TABLE IF NOT EXISTS chebi_parsed (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    source_raw_id INTEGER,
                    record_key TEXT NOT NULL UNIQUE,
                    chebi_id TEXT,
                    preferred_name TEXT,
                    synonyms_json TEXT,
                    definition TEXT,
                    cas_number TEXT,
                    pubchem_cid TEXT,
                    inchi_key TEXT,
                    smiles TEXT,
                    roles_json TEXT,
                    xrefs_json TEXT,
                    source_url TEXT,
                    parsed_at TEXT NOT NULL,
                    provenance_json TEXT,
                    raw_record_json TEXT,
                    FOREIGN KEY(run_id) REFERENCES acquisition_runs(id),
                    FOREIGN KEY(source_raw_id) REFERENCES chebi_raw(id)
                );

                CREATE TABLE IF NOT EXISTS pcpc_raw (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    source_url TEXT NOT NULL,
                    payload_format TEXT NOT NULL,
                    http_status INTEGER,
                    retrieved_at TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    local_path TEXT NOT NULL,
                    metadata_json TEXT,
                    UNIQUE(source_url, sha256),
                    FOREIGN KEY(run_id) REFERENCES acquisition_runs(id)
                );

                CREATE TABLE IF NOT EXISTS pcpc_parsed (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    source_raw_id INTEGER,
                    record_key TEXT NOT NULL UNIQUE,
                    inci_name TEXT,
                    preferred_name TEXT,
                    cas_number TEXT,
                    synonyms_json TEXT,
                    functions_json TEXT,
                    description TEXT,
                    regulatory_notes TEXT,
                    column_mapping_json TEXT,
                    mapping_warnings_json TEXT,
                    parsed_at TEXT NOT NULL,
                    provenance_json TEXT,
                    raw_record_json TEXT,
                    FOREIGN KEY(run_id) REFERENCES acquisition_runs(id),
                    FOREIGN KEY(source_raw_id) REFERENCES pcpc_raw(id)
                );

                CREATE TABLE IF NOT EXISTS ingredient_master (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    canonical_name TEXT NOT NULL,
                    normalized_name_key TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'active',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    notes TEXT,
                    UNIQUE(normalized_name_key, canonical_name)
                );

                CREATE TABLE IF NOT EXISTS ingredient_aliases (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    master_id INTEGER NOT NULL,
                    alias TEXT NOT NULL,
                    normalized_alias TEXT NOT NULL,
                    source_name TEXT,
                    source_record_key TEXT,
                    created_at TEXT NOT NULL,
                    UNIQUE(master_id, normalized_alias, source_name, source_record_key),
                    FOREIGN KEY(master_id) REFERENCES ingredient_master(id)
                );

                CREATE TABLE IF NOT EXISTS ingredient_identifiers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    master_id INTEGER NOT NULL,
                    identifier_type TEXT NOT NULL,
                    identifier_value TEXT NOT NULL,
                    normalized_value TEXT NOT NULL,
                    source_name TEXT,
                    confidence_score REAL,
                    created_at TEXT NOT NULL,
                    UNIQUE(identifier_type, normalized_value, master_id),
                    FOREIGN KEY(master_id) REFERENCES ingredient_master(id)
                );

                CREATE TABLE IF NOT EXISTS source_links (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    master_id INTEGER NOT NULL,
                    source_name TEXT NOT NULL,
                    source_table TEXT NOT NULL,
                    source_record_key TEXT NOT NULL,
                    raw_ref TEXT,
                    evidence_json TEXT,
                    created_at TEXT NOT NULL,
                    UNIQUE(source_name, source_table, source_record_key),
                    FOREIGN KEY(master_id) REFERENCES ingredient_master(id)
                );

                CREATE TABLE IF NOT EXISTS ingredient_merge_candidates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    left_master_id INTEGER NOT NULL,
                    right_master_id INTEGER NOT NULL,
                    match_method TEXT NOT NULL,
                    confidence_score REAL NOT NULL,
                    evidence_json TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    created_at TEXT NOT NULL,
                    UNIQUE(left_master_id, right_master_id, match_method),
                    FOREIGN KEY(left_master_id) REFERENCES ingredient_master(id),
                    FOREIGN KEY(right_master_id) REFERENCES ingredient_master(id)
                );

                CREATE TABLE IF NOT EXISTS ingredient_merge_decisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    candidate_id INTEGER NOT NULL,
                    decision TEXT NOT NULL,
                    decided_by TEXT,
                    decided_at TEXT NOT NULL,
                    notes TEXT,
                    FOREIGN KEY(candidate_id) REFERENCES ingredient_merge_candidates(id)
                );

                CREATE TABLE IF NOT EXISTS compatibility_evidence (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ingredient_master_id INTEGER NOT NULL,
                    evidence_type TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    source_reference TEXT,
                    source_url TEXT,
                    extracted_text TEXT,
                    date_accessed TEXT,
                    confidence_score REAL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(ingredient_master_id) REFERENCES ingredient_master(id)
                );

                CREATE TABLE IF NOT EXISTS ingredient_ph_profile (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ingredient_master_id INTEGER NOT NULL,
                    preferred_ph_min REAL,
                    preferred_ph_max REAL,
                    ph_sensitive INTEGER,
                    ph_notes TEXT,
                    derivation_method TEXT NOT NULL,
                    confidence_score REAL,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    evidence_id INTEGER,
                    FOREIGN KEY(ingredient_master_id) REFERENCES ingredient_master(id),
                    FOREIGN KEY(evidence_id) REFERENCES compatibility_evidence(id)
                );

                CREATE TABLE IF NOT EXISTS ingredient_stability_profile (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ingredient_master_id INTEGER NOT NULL,
                    flag TEXT NOT NULL,
                    mitigation TEXT,
                    derivation_method TEXT NOT NULL,
                    confidence_score REAL,
                    evidence_summary TEXT,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    evidence_id INTEGER,
                    UNIQUE(ingredient_master_id, flag, mitigation, derivation_method, evidence_summary, is_active),
                    FOREIGN KEY(ingredient_master_id) REFERENCES ingredient_master(id),
                    FOREIGN KEY(evidence_id) REFERENCES compatibility_evidence(id)
                );

                CREATE TABLE IF NOT EXISTS ingredient_role_tags (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ingredient_master_id INTEGER NOT NULL,
                    role_tag TEXT NOT NULL,
                    source_name TEXT,
                    source_value TEXT,
                    confidence_score REAL,
                    is_primary INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(ingredient_master_id, role_tag, source_name, source_value),
                    FOREIGN KEY(ingredient_master_id) REFERENCES ingredient_master(id)
                );

                CREATE TABLE IF NOT EXISTS ingredient_pairwise_tags (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ingredient_master_id INTEGER NOT NULL,
                    pairwise_tag TEXT NOT NULL,
                    derivation_method TEXT NOT NULL,
                    confidence_score REAL,
                    notes TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    evidence_id INTEGER,
                    UNIQUE(ingredient_master_id, pairwise_tag, derivation_method, notes),
                    FOREIGN KEY(ingredient_master_id) REFERENCES ingredient_master(id),
                    FOREIGN KEY(evidence_id) REFERENCES compatibility_evidence(id)
                );

                CREATE TABLE IF NOT EXISTS ingredient_manual_overrides (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ingredient_master_id INTEGER NOT NULL,
                    field_name TEXT NOT NULL,
                    override_value_text TEXT,
                    override_value_num REAL,
                    reason TEXT,
                    source_type TEXT,
                    reference_text TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(ingredient_master_id, field_name, override_value_text, override_value_num, source_type, reference_text),
                    FOREIGN KEY(ingredient_master_id) REFERENCES ingredient_master(id)
                );

                CREATE TABLE IF NOT EXISTS pairwise_compatibility_rules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    rule_code TEXT NOT NULL UNIQUE,
                    ingredient_name_a TEXT,
                    ingredient_name_b TEXT,
                    tag_a TEXT,
                    tag_b TEXT,
                    condition_context_json TEXT,
                    severity TEXT NOT NULL,
                    penalty_points REAL,
                    message TEXT NOT NULL,
                    suggestion TEXT,
                    source_type TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS ingredient_priority_scores (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ingredient_master_id INTEGER NOT NULL UNIQUE,
                    frequency_score REAL,
                    source_coverage_score REAL,
                    role_importance_score REAL,
                    regulatory_relevance_score REAL,
                    name_confidence_score REAL,
                    total_priority_score REAL,
                    rank_position INTEGER,
                    selected_for_curated_kb INTEGER NOT NULL DEFAULT 0,
                    priority_tier TEXT,
                    scoring_version TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(ingredient_master_id) REFERENCES ingredient_master(id)
                );

                CREATE TABLE IF NOT EXISTS failures (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    stage TEXT NOT NULL,
                    record_key TEXT,
                    source TEXT,
                    error TEXT NOT NULL,
                    details_json TEXT,
                    retriable INTEGER NOT NULL DEFAULT 0,
                    occurred_at TEXT NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES acquisition_runs(id)
                );

                CREATE TABLE IF NOT EXISTS unmatched (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    record_key TEXT NOT NULL,
                    query_tried TEXT,
                    reason TEXT,
                    details_json TEXT,
                    created_at TEXT NOT NULL,
                    UNIQUE(record_key),
                    FOREIGN KEY(run_id) REFERENCES acquisition_runs(id)
                );
                """
            )

    def start_run(self, command: str) -> RunContext:
        with self.connect() as conn:
            cur = conn.execute(
                "INSERT INTO acquisition_runs (command, status, started_at) VALUES (?, 'running', ?)",
                (command, utc_now_iso()),
            )
            return RunContext(run_id=int(cur.lastrowid), command=command)

    def complete_run(self, run_id: int, status: str, summary: dict[str, Any] | None = None, notes: str | None = None) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE acquisition_runs
                SET status = ?, completed_at = ?, summary_json = ?, notes = ?
                WHERE id = ?
                """,
                (status, utc_now_iso(), json.dumps(summary or {}), notes, run_id),
            )

    def start_normalization_run(self) -> int:
        with self.connect() as conn:
            cur = conn.execute(
                "INSERT INTO normalization_runs(status, started_at) VALUES ('running', ?)",
                (utc_now_iso(),),
            )
            return int(cur.lastrowid)

    def complete_normalization_run(self, run_id: int, status: str, summary: dict[str, Any] | None = None, notes: str | None = None) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE normalization_runs
                SET status = ?, completed_at = ?, summary_json = ?, notes = ?
                WHERE id = ?
                """,
                (status, utc_now_iso(), json.dumps(summary or {}), notes, run_id),
            )

    def set_checkpoint(self, stage: str, cursor_value: str, run_id: int) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO checkpoints(stage, cursor_value, run_id, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(stage) DO UPDATE SET
                    cursor_value=excluded.cursor_value,
                    run_id=excluded.run_id,
                    updated_at=excluded.updated_at
                """,
                (stage, cursor_value, run_id, utc_now_iso()),
            )

    def get_checkpoint(self, stage: str) -> str | None:
        with self.connect() as conn:
            row = conn.execute("SELECT cursor_value FROM checkpoints WHERE stage = ?", (stage,)).fetchone()
            return str(row[0]) if row else None

    def _insert_raw(self, table: str, payload: dict[str, Any]) -> int | None:
        with self.connect() as conn:
            cur = conn.execute(
                f"""
                INSERT OR IGNORE INTO {table} (
                    run_id, source_url, payload_format, http_status, retrieved_at,
                    sha256, local_path, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["run_id"],
                    payload["source_url"],
                    payload["payload_format"],
                    payload.get("http_status"),
                    payload["retrieved_at"],
                    payload["sha256"],
                    payload["local_path"],
                    json.dumps(payload.get("metadata") or {}),
                ),
            )
            return int(cur.lastrowid) if cur.lastrowid else None

    def insert_cosing_raw(self, payload: dict[str, Any]) -> int | None:
        with self.connect() as conn:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO cosing_raw(
                    run_id, source_kind, source_url, payload_format, http_status,
                    retrieved_at, sha256, local_path, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["run_id"],
                    payload["source_kind"],
                    payload["source_url"],
                    payload["payload_format"],
                    payload.get("http_status"),
                    payload["retrieved_at"],
                    payload["sha256"],
                    payload["local_path"],
                    json.dumps(payload.get("metadata") or {}),
                ),
            )
            return int(cur.lastrowid) if cur.lastrowid else None

    def insert_fda_iid_raw(self, payload: dict[str, Any]) -> int | None:
        return self._insert_raw("fda_iid_raw", payload)

    def insert_fda_unii_raw(self, payload: dict[str, Any]) -> int | None:
        return self._insert_raw("fda_unii_raw", payload)

    def insert_chebi_raw(self, payload: dict[str, Any]) -> int | None:
        return self._insert_raw("chebi_raw", payload)

    def insert_pcpc_raw(self, payload: dict[str, Any]) -> int | None:
        return self._insert_raw("pcpc_raw", payload)

    def list_cosing_raw(self, source_kind: str | None = None) -> list[sqlite3.Row]:
        with self.connect() as conn:
            if source_kind:
                rows = conn.execute("SELECT * FROM cosing_raw WHERE source_kind=? ORDER BY id", (source_kind,)).fetchall()
            else:
                rows = conn.execute("SELECT * FROM cosing_raw ORDER BY id").fetchall()
            return list(rows)

    def list_fda_iid_raw(self) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return list(conn.execute("SELECT * FROM fda_iid_raw ORDER BY id").fetchall())

    def list_fda_unii_raw(self) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return list(conn.execute("SELECT * FROM fda_unii_raw ORDER BY id").fetchall())

    def list_chebi_raw(self) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return list(conn.execute("SELECT * FROM chebi_raw ORDER BY id").fetchall())

    def list_pcpc_raw(self) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return list(conn.execute("SELECT * FROM pcpc_raw ORDER BY id").fetchall())

    def upsert_cosing_parsed(self, row: dict[str, Any]) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO cosing_parsed(
                    run_id, source_raw_id, record_key, item_type, ingredient_name, inci_name,
                    cas_no, ec_no, function_names, description, annex_no, ref_no, status,
                    regulation_refs, source_url, parsed_at, provenance_json, raw_record_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(record_key) DO UPDATE SET
                    run_id=excluded.run_id,
                    source_raw_id=excluded.source_raw_id,
                    item_type=excluded.item_type,
                    ingredient_name=excluded.ingredient_name,
                    inci_name=excluded.inci_name,
                    cas_no=excluded.cas_no,
                    ec_no=excluded.ec_no,
                    function_names=excluded.function_names,
                    description=excluded.description,
                    annex_no=excluded.annex_no,
                    ref_no=excluded.ref_no,
                    status=excluded.status,
                    regulation_refs=excluded.regulation_refs,
                    source_url=excluded.source_url,
                    parsed_at=excluded.parsed_at,
                    provenance_json=excluded.provenance_json,
                    raw_record_json=excluded.raw_record_json
                """,
                (
                    row["run_id"],
                    row.get("source_raw_id"),
                    row["record_key"],
                    row.get("item_type"),
                    row.get("ingredient_name"),
                    row.get("inci_name"),
                    row.get("cas_no"),
                    row.get("ec_no"),
                    row.get("function_names"),
                    row.get("description"),
                    row.get("annex_no"),
                    row.get("ref_no"),
                    row.get("status"),
                    row.get("regulation_refs"),
                    row.get("source_url"),
                    row["parsed_at"],
                    json.dumps(row.get("provenance") or {}),
                    json.dumps(row.get("raw_record") or {}),
                ),
            )

    def upsert_fda_iid_parsed(self, row: dict[str, Any]) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO fda_iid_parsed(
                    run_id, source_raw_id, record_key, inactive_ingredient, route, dosage_form,
                    cas_number, unii, potency_amount, potency_unit, maximum_daily_exposure,
                    maximum_daily_exposure_unit, record_updated, parsed_at, provenance_json,
                    raw_record_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(record_key) DO UPDATE SET
                    run_id=excluded.run_id,
                    source_raw_id=excluded.source_raw_id,
                    inactive_ingredient=excluded.inactive_ingredient,
                    route=excluded.route,
                    dosage_form=excluded.dosage_form,
                    cas_number=excluded.cas_number,
                    unii=excluded.unii,
                    potency_amount=excluded.potency_amount,
                    potency_unit=excluded.potency_unit,
                    maximum_daily_exposure=excluded.maximum_daily_exposure,
                    maximum_daily_exposure_unit=excluded.maximum_daily_exposure_unit,
                    record_updated=excluded.record_updated,
                    parsed_at=excluded.parsed_at,
                    provenance_json=excluded.provenance_json,
                    raw_record_json=excluded.raw_record_json
                """,
                (
                    row["run_id"],
                    row.get("source_raw_id"),
                    row["record_key"],
                    row.get("inactive_ingredient"),
                    row.get("route"),
                    row.get("dosage_form"),
                    row.get("cas_number"),
                    row.get("unii"),
                    row.get("potency_amount"),
                    row.get("potency_unit"),
                    row.get("maximum_daily_exposure"),
                    row.get("maximum_daily_exposure_unit"),
                    row.get("record_updated"),
                    row["parsed_at"],
                    json.dumps(row.get("provenance") or {}),
                    json.dumps(row.get("raw_record") or {}),
                ),
            )

    def upsert_fda_unii_parsed(self, row: dict[str, Any]) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO fda_unii_parsed(
                    run_id, source_raw_id, dataset_kind, record_key, unii, preferred_name,
                    name, name_type, cas_number, ec_number, pubchem_cid, inchi_key, smiles,
                    ingredient_type, substance_type, parsed_at, provenance_json, raw_record_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(record_key) DO UPDATE SET
                    run_id=excluded.run_id,
                    source_raw_id=excluded.source_raw_id,
                    dataset_kind=excluded.dataset_kind,
                    unii=excluded.unii,
                    preferred_name=excluded.preferred_name,
                    name=excluded.name,
                    name_type=excluded.name_type,
                    cas_number=excluded.cas_number,
                    ec_number=excluded.ec_number,
                    pubchem_cid=excluded.pubchem_cid,
                    inchi_key=excluded.inchi_key,
                    smiles=excluded.smiles,
                    ingredient_type=excluded.ingredient_type,
                    substance_type=excluded.substance_type,
                    parsed_at=excluded.parsed_at,
                    provenance_json=excluded.provenance_json,
                    raw_record_json=excluded.raw_record_json
                """,
                (
                    row["run_id"],
                    row.get("source_raw_id"),
                    row["dataset_kind"],
                    row["record_key"],
                    row.get("unii"),
                    row.get("preferred_name"),
                    row.get("name"),
                    row.get("name_type"),
                    row.get("cas_number"),
                    row.get("ec_number"),
                    row.get("pubchem_cid"),
                    row.get("inchi_key"),
                    row.get("smiles"),
                    row.get("ingredient_type"),
                    row.get("substance_type"),
                    row["parsed_at"],
                    json.dumps(row.get("provenance") or {}),
                    json.dumps(row.get("raw_record") or {}),
                ),
            )

    def upsert_chebi_parsed(self, row: dict[str, Any]) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO chebi_parsed(
                    run_id, source_raw_id, record_key, chebi_id, preferred_name, synonyms_json,
                    definition, cas_number, pubchem_cid, inchi_key, smiles, roles_json, xrefs_json,
                    source_url, parsed_at, provenance_json, raw_record_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(record_key) DO UPDATE SET
                    run_id=excluded.run_id,
                    source_raw_id=excluded.source_raw_id,
                    chebi_id=excluded.chebi_id,
                    preferred_name=excluded.preferred_name,
                    synonyms_json=excluded.synonyms_json,
                    definition=excluded.definition,
                    cas_number=excluded.cas_number,
                    pubchem_cid=excluded.pubchem_cid,
                    inchi_key=excluded.inchi_key,
                    smiles=excluded.smiles,
                    roles_json=excluded.roles_json,
                    xrefs_json=excluded.xrefs_json,
                    source_url=excluded.source_url,
                    parsed_at=excluded.parsed_at,
                    provenance_json=excluded.provenance_json,
                    raw_record_json=excluded.raw_record_json
                """,
                (
                    row["run_id"],
                    row.get("source_raw_id"),
                    row["record_key"],
                    row.get("chebi_id"),
                    row.get("preferred_name"),
                    json.dumps(row.get("synonyms") or []),
                    row.get("definition"),
                    row.get("cas_number"),
                    row.get("pubchem_cid"),
                    row.get("inchi_key"),
                    row.get("smiles"),
                    json.dumps(row.get("roles") or []),
                    json.dumps(row.get("xrefs") or []),
                    row.get("source_url"),
                    row["parsed_at"],
                    json.dumps(row.get("provenance") or {}),
                    json.dumps(row.get("raw_record") or {}),
                ),
            )

    def upsert_pcpc_parsed(self, row: dict[str, Any]) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO pcpc_parsed(
                    run_id, source_raw_id, record_key, inci_name, preferred_name, cas_number,
                    synonyms_json, functions_json, description, regulatory_notes, column_mapping_json,
                    mapping_warnings_json, parsed_at, provenance_json, raw_record_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(record_key) DO UPDATE SET
                    run_id=excluded.run_id,
                    source_raw_id=excluded.source_raw_id,
                    inci_name=excluded.inci_name,
                    preferred_name=excluded.preferred_name,
                    cas_number=excluded.cas_number,
                    synonyms_json=excluded.synonyms_json,
                    functions_json=excluded.functions_json,
                    description=excluded.description,
                    regulatory_notes=excluded.regulatory_notes,
                    column_mapping_json=excluded.column_mapping_json,
                    mapping_warnings_json=excluded.mapping_warnings_json,
                    parsed_at=excluded.parsed_at,
                    provenance_json=excluded.provenance_json,
                    raw_record_json=excluded.raw_record_json
                """,
                (
                    row["run_id"],
                    row.get("source_raw_id"),
                    row["record_key"],
                    row.get("inci_name"),
                    row.get("preferred_name"),
                    row.get("cas_number"),
                    json.dumps(row.get("synonyms") or []),
                    json.dumps(row.get("functions") or []),
                    row.get("description"),
                    row.get("regulatory_notes"),
                    json.dumps(row.get("column_mapping") or {}),
                    json.dumps(row.get("mapping_warnings") or []),
                    row["parsed_at"],
                    json.dumps(row.get("provenance") or {}),
                    json.dumps(row.get("raw_record") or {}),
                ),
            )

    def iter_cosing_parsed(self, only_unenriched: bool = False) -> list[sqlite3.Row]:
        with self.connect() as conn:
            if only_unenriched:
                rows = conn.execute(
                    """
                    SELECT cp.* FROM cosing_parsed cp
                    LEFT JOIN ingredient_enrichment ie ON ie.record_key = cp.record_key
                    WHERE ie.id IS NULL
                    ORDER BY cp.id
                    """
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM cosing_parsed ORDER BY id").fetchall()
            return list(rows)

    def iter_fda_iid_parsed(self) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return list(conn.execute("SELECT * FROM fda_iid_parsed ORDER BY id").fetchall())

    def iter_fda_unii_parsed(self) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return list(conn.execute("SELECT * FROM fda_unii_parsed ORDER BY id").fetchall())

    def iter_chebi_parsed(self) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return list(conn.execute("SELECT * FROM chebi_parsed ORDER BY id").fetchall())

    def iter_pcpc_parsed(self) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return list(conn.execute("SELECT * FROM pcpc_parsed ORDER BY id").fetchall())

    def insert_pubchem_raw(self, row: dict[str, Any]) -> int:
        with self.connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO pubchem_raw(
                    run_id, record_key, query, query_type, endpoint, http_status,
                    retrieved_at, local_path, response_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["run_id"],
                    row["record_key"],
                    row.get("query"),
                    row.get("query_type"),
                    row.get("endpoint"),
                    row.get("http_status"),
                    row["retrieved_at"],
                    row.get("local_path"),
                    json.dumps(row.get("response") or {}),
                ),
            )
            return int(cur.lastrowid)

    def upsert_enrichment(self, row: dict[str, Any]) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO ingredient_enrichment(
                    run_id, record_key, match_status, confidence, lookup_method, cid,
                    canonical_smiles, isomeric_smiles, inchi, inchikey, molecular_formula,
                    molecular_weight, iupac_name, synonyms_json, xlogp, computed_props_json,
                    source_url, raw_ref_id, enriched_at, provenance_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(record_key) DO UPDATE SET
                    run_id=excluded.run_id,
                    match_status=excluded.match_status,
                    confidence=excluded.confidence,
                    lookup_method=excluded.lookup_method,
                    cid=excluded.cid,
                    canonical_smiles=excluded.canonical_smiles,
                    isomeric_smiles=excluded.isomeric_smiles,
                    inchi=excluded.inchi,
                    inchikey=excluded.inchikey,
                    molecular_formula=excluded.molecular_formula,
                    molecular_weight=excluded.molecular_weight,
                    iupac_name=excluded.iupac_name,
                    synonyms_json=excluded.synonyms_json,
                    xlogp=excluded.xlogp,
                    computed_props_json=excluded.computed_props_json,
                    source_url=excluded.source_url,
                    raw_ref_id=excluded.raw_ref_id,
                    enriched_at=excluded.enriched_at,
                    provenance_json=excluded.provenance_json
                """,
                (
                    row["run_id"],
                    row["record_key"],
                    row["match_status"],
                    row.get("confidence"),
                    row.get("lookup_method"),
                    row.get("cid"),
                    row.get("canonical_smiles"),
                    row.get("isomeric_smiles"),
                    row.get("inchi"),
                    row.get("inchikey"),
                    row.get("molecular_formula"),
                    row.get("molecular_weight"),
                    row.get("iupac_name"),
                    json.dumps(row.get("synonyms") or []),
                    row.get("xlogp"),
                    json.dumps(row.get("computed_props") or {}),
                    row.get("source_url"),
                    row.get("raw_ref_id"),
                    row["enriched_at"],
                    json.dumps(row.get("provenance") or {}),
                ),
            )

    def insert_failure(
        self,
        run_id: int,
        stage: str,
        error: str,
        record_key: str | None = None,
        source: str | None = None,
        details: dict[str, Any] | None = None,
        retriable: bool = False,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO failures(run_id, stage, record_key, source, error, details_json, retriable, occurred_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    stage,
                    record_key,
                    source,
                    error,
                    json.dumps(details or {}),
                    1 if retriable else 0,
                    utc_now_iso(),
                ),
            )

    def upsert_unmatched(self, run_id: int, record_key: str, reason: str, query_tried: str, details: dict[str, Any]) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO unmatched(run_id, record_key, query_tried, reason, details_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(record_key) DO UPDATE SET
                    run_id=excluded.run_id,
                    query_tried=excluded.query_tried,
                    reason=excluded.reason,
                    details_json=excluded.details_json,
                    created_at=excluded.created_at
                """,
                (run_id, record_key, query_tried, reason, json.dumps(details), utc_now_iso()),
            )

    def insert_master(self, canonical_name: str, normalized_name_key: str, notes: str | None = None) -> int:
        now = utc_now_iso()
        with self.connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO ingredient_master(canonical_name, normalized_name_key, created_at, updated_at, notes)
                VALUES (?, ?, ?, ?, ?)
                """,
                (canonical_name, normalized_name_key, now, now, notes),
            )
            return int(cur.lastrowid)

    def touch_master(self, master_id: int) -> None:
        with self.connect() as conn:
            conn.execute("UPDATE ingredient_master SET updated_at = ? WHERE id = ?", (utc_now_iso(), master_id))

    def upsert_alias(self, master_id: int, alias: str, normalized_alias: str, source_name: str, source_record_key: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO ingredient_aliases(
                    master_id, alias, normalized_alias, source_name, source_record_key, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (master_id, alias, normalized_alias, source_name, source_record_key, utc_now_iso()),
            )

    def upsert_identifier(
        self,
        master_id: int,
        identifier_type: str,
        identifier_value: str,
        normalized_value: str,
        source_name: str,
        confidence_score: float,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO ingredient_identifiers(
                    master_id, identifier_type, identifier_value, normalized_value, source_name, confidence_score, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    master_id,
                    identifier_type,
                    identifier_value,
                    normalized_value,
                    source_name,
                    confidence_score,
                    utc_now_iso(),
                ),
            )

    def upsert_source_link(
        self,
        master_id: int,
        source_name: str,
        source_table: str,
        source_record_key: str,
        raw_ref: str,
        evidence: dict[str, Any],
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO source_links(master_id, source_name, source_table, source_record_key, raw_ref, evidence_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_name, source_table, source_record_key) DO UPDATE SET
                    master_id=excluded.master_id,
                    raw_ref=excluded.raw_ref,
                    evidence_json=excluded.evidence_json
                """,
                (
                    master_id,
                    source_name,
                    source_table,
                    source_record_key,
                    raw_ref,
                    json.dumps(evidence or {}),
                    utc_now_iso(),
                ),
            )

    def insert_merge_candidate(
        self,
        left_master_id: int,
        right_master_id: int,
        match_method: str,
        confidence_score: float,
        evidence: dict[str, Any],
    ) -> None:
        if left_master_id == right_master_id:
            return
        l, r = sorted((left_master_id, right_master_id))
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO ingredient_merge_candidates(
                    left_master_id, right_master_id, match_method, confidence_score, evidence_json, status, created_at
                ) VALUES (?, ?, ?, ?, ?, 'pending', ?)
                """,
                (l, r, match_method, confidence_score, json.dumps(evidence), utc_now_iso()),
            )

    def find_master_by_identifier(self, identifier_type: str, normalized_value: str) -> int | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT master_id FROM ingredient_identifiers
                WHERE identifier_type = ? AND normalized_value = ?
                ORDER BY id LIMIT 1
                """,
                (identifier_type, normalized_value),
            ).fetchone()
            return int(row[0]) if row else None

    def find_master_by_normalized_name(self, normalized_name_key: str) -> int | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT id FROM ingredient_master WHERE normalized_name_key = ? ORDER BY id LIMIT 1",
                (normalized_name_key,),
            ).fetchone()
            return int(row[0]) if row else None

    def find_masters_by_normalized_name(self, normalized_name_key: str) -> list[int]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT id FROM ingredient_master WHERE normalized_name_key = ? ORDER BY id",
                (normalized_name_key,),
            ).fetchall()
            return [int(r[0]) for r in rows]

    def get_master_identifiers(self, master_id: int) -> dict[str, set[str]]:
        out: dict[str, set[str]] = {}
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT identifier_type, normalized_value FROM ingredient_identifiers WHERE master_id = ?",
                (master_id,),
            ).fetchall()
        for row in rows:
            out.setdefault(str(row["identifier_type"]), set()).add(str(row["normalized_value"]))
        return out

    def update_master_canonical_name(self, master_id: int, canonical_name: str, normalized_name_key: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE ingredient_master
                SET canonical_name = ?, normalized_name_key = ?, updated_at = ?
                WHERE id = ?
                  AND NOT EXISTS (
                      SELECT 1
                      FROM ingredient_master other
                      WHERE other.id <> ?
                        AND other.canonical_name = ?
                        AND other.normalized_name_key = ?
                  )
                """,
                (canonical_name, normalized_name_key, utc_now_iso(), master_id, master_id, canonical_name, normalized_name_key),
            )

    def find_master_by_name_pair(self, canonical_name: str, normalized_name_key: str) -> int | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT id FROM ingredient_master
                WHERE canonical_name = ? AND normalized_name_key = ?
                ORDER BY id LIMIT 1
                """,
                (canonical_name, normalized_name_key),
            ).fetchone()
            return int(row[0]) if row else None

    def get_master_rows(self) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return list(conn.execute("SELECT * FROM ingredient_master ORDER BY id").fetchall())

    def iter_identity_input_rows(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        with self.connect() as conn:
            for r in conn.execute("SELECT * FROM cosing_parsed"):
                rows.append(
                    {
                        "source_name": "cosing",
                        "source_table": "cosing_parsed",
                        "source_record_key": r["record_key"],
                        "name": r["inci_name"] or r["ingredient_name"],
                        "inci_name": r["inci_name"],
                        "preferred_name": r["ingredient_name"] or r["inci_name"],
                        "aliases": [r["ingredient_name"], r["inci_name"]],
                        "cas": r["cas_no"],
                        "ec": r["ec_no"],
                        "unii": None,
                        "pubchem_cid": None,
                        "chebi_id": None,
                        "raw_ref": str(r["source_raw_id"] or ""),
                        "evidence": {"status": r["status"], "annex_no": r["annex_no"]},
                    }
                )
            for r in conn.execute("SELECT * FROM ingredient_enrichment"):
                rows.append(
                    {
                        "source_name": "pubchem",
                        "source_table": "ingredient_enrichment",
                        "source_record_key": r["record_key"],
                        "name": r["iupac_name"],
                        "inci_name": None,
                        "preferred_name": r["iupac_name"],
                        "aliases": json.loads(r["synonyms_json"] or "[]")[:20],
                        "cas": None,
                        "ec": None,
                        "unii": None,
                        "pubchem_cid": str(r["cid"]) if r["cid"] else None,
                        "chebi_id": None,
                        "raw_ref": str(r["raw_ref_id"] or ""),
                        "evidence": {"match_status": r["match_status"], "lookup_method": r["lookup_method"]},
                    }
                )
            for r in conn.execute("SELECT * FROM fda_iid_parsed"):
                rows.append(
                    {
                        "source_name": "fda_iid",
                        "source_table": "fda_iid_parsed",
                        "source_record_key": r["record_key"],
                        "name": r["inactive_ingredient"],
                        "inci_name": None,
                        "preferred_name": r["inactive_ingredient"],
                        "aliases": [r["inactive_ingredient"]],
                        "cas": r["cas_number"],
                        "ec": None,
                        "unii": r["unii"],
                        "pubchem_cid": None,
                        "chebi_id": None,
                        "raw_ref": str(r["source_raw_id"] or ""),
                        "evidence": {"route": r["route"], "dosage_form": r["dosage_form"]},
                    }
                )
            for r in conn.execute("SELECT * FROM fda_unii_parsed"):
                aliases = [r["name"], r["preferred_name"]]
                rows.append(
                    {
                        "source_name": "fda_unii",
                        "source_table": "fda_unii_parsed",
                        "source_record_key": r["record_key"],
                        "name": r["preferred_name"] or r["name"],
                        "inci_name": None,
                        "preferred_name": r["preferred_name"] or r["name"],
                        "aliases": [a for a in aliases if a],
                        "cas": r["cas_number"],
                        "ec": r["ec_number"],
                        "unii": r["unii"],
                        "pubchem_cid": r["pubchem_cid"],
                        "chebi_id": None,
                        "raw_ref": str(r["source_raw_id"] or ""),
                        "evidence": {"dataset_kind": r["dataset_kind"], "name_type": r["name_type"]},
                    }
                )
            for r in conn.execute("SELECT * FROM chebi_parsed"):
                aliases = json.loads(r["synonyms_json"] or "[]")
                rows.append(
                    {
                        "source_name": "chebi",
                        "source_table": "chebi_parsed",
                        "source_record_key": r["record_key"],
                        "name": r["preferred_name"],
                        "inci_name": None,
                        "preferred_name": r["preferred_name"],
                        "aliases": [a for a in aliases if a],
                        "cas": r["cas_number"],
                        "ec": None,
                        "unii": None,
                        "pubchem_cid": r["pubchem_cid"],
                        "chebi_id": r["chebi_id"],
                        "raw_ref": str(r["source_raw_id"] or ""),
                        "evidence": {"roles": json.loads(r["roles_json"] or "[]")[:10]},
                    }
                )
            for r in conn.execute("SELECT * FROM pcpc_parsed"):
                rows.append(
                    {
                        "source_name": "pcpc",
                        "source_table": "pcpc_parsed",
                        "source_record_key": r["record_key"],
                        "name": r["inci_name"] or r["preferred_name"],
                        "inci_name": r["inci_name"],
                        "preferred_name": r["preferred_name"] or r["inci_name"],
                        "aliases": json.loads(r["synonyms_json"] or "[]"),
                        "cas": r["cas_number"],
                        "ec": None,
                        "unii": None,
                        "pubchem_cid": None,
                        "chebi_id": None,
                        "raw_ref": str(r["source_raw_id"] or ""),
                        "evidence": {"mapping_warnings": json.loads(r["mapping_warnings_json"] or "[]")},
                    }
                )
        return rows

    def insert_compatibility_evidence(
        self,
        ingredient_master_id: int,
        evidence_type: str,
        source_type: str,
        source_reference: str | None = None,
        source_url: str | None = None,
        extracted_text: str | None = None,
        date_accessed: str | None = None,
        confidence_score: float | None = None,
    ) -> int:
        with self.connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO compatibility_evidence(
                    ingredient_master_id, evidence_type, source_type, source_reference, source_url,
                    extracted_text, date_accessed, confidence_score, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ingredient_master_id,
                    evidence_type,
                    source_type,
                    source_reference,
                    source_url,
                    extracted_text,
                    date_accessed,
                    confidence_score,
                    utc_now_iso(),
                ),
            )
            return int(cur.lastrowid)

    def upsert_role_tag(
        self,
        ingredient_master_id: int,
        role_tag: str,
        source_name: str,
        source_value: str,
        confidence_score: float,
        is_primary: bool = False,
    ) -> None:
        now = utc_now_iso()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO ingredient_role_tags(
                    ingredient_master_id, role_tag, source_name, source_value, confidence_score,
                    is_primary, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(ingredient_master_id, role_tag, source_name, source_value) DO UPDATE SET
                    confidence_score = excluded.confidence_score,
                    is_primary = MAX(ingredient_role_tags.is_primary, excluded.is_primary),
                    updated_at = excluded.updated_at
                """,
                (
                    ingredient_master_id,
                    role_tag,
                    source_name,
                    source_value,
                    confidence_score,
                    1 if is_primary else 0,
                    now,
                    now,
                ),
            )

    def clear_primary_role_flags(self, ingredient_master_id: int) -> None:
        with self.connect() as conn:
            conn.execute("UPDATE ingredient_role_tags SET is_primary = 0, updated_at = ? WHERE ingredient_master_id = ?", (utc_now_iso(), ingredient_master_id))

    def upsert_stability_profile(
        self,
        ingredient_master_id: int,
        flag: str,
        mitigation: str,
        derivation_method: str,
        confidence_score: float,
        evidence_summary: str,
        evidence_id: int | None = None,
    ) -> None:
        now = utc_now_iso()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO ingredient_stability_profile(
                    ingredient_master_id, flag, mitigation, derivation_method, confidence_score,
                    evidence_summary, is_active, created_at, updated_at, evidence_id
                ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
                """,
                (
                    ingredient_master_id,
                    flag,
                    mitigation,
                    derivation_method,
                    confidence_score,
                    evidence_summary,
                    now,
                    now,
                    evidence_id,
                ),
            )

    def upsert_pairwise_tag(
        self,
        ingredient_master_id: int,
        pairwise_tag: str,
        derivation_method: str,
        confidence_score: float,
        notes: str,
        evidence_id: int | None = None,
    ) -> None:
        now = utc_now_iso()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO ingredient_pairwise_tags(
                    ingredient_master_id, pairwise_tag, derivation_method, confidence_score, notes,
                    created_at, updated_at, evidence_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ingredient_master_id,
                    pairwise_tag,
                    derivation_method,
                    confidence_score,
                    notes,
                    now,
                    now,
                    evidence_id,
                ),
            )

    def upsert_ph_profile(
        self,
        ingredient_master_id: int,
        preferred_ph_min: float | None,
        preferred_ph_max: float | None,
        ph_sensitive: bool | None,
        ph_notes: str | None,
        derivation_method: str,
        confidence_score: float | None,
        evidence_id: int | None = None,
    ) -> None:
        with self.connect() as conn:
            # Never overwrite curated with inferred.
            if derivation_method == "INFERRED":
                curated = conn.execute(
                    """
                    SELECT id FROM ingredient_ph_profile
                    WHERE ingredient_master_id = ? AND is_active = 1
                      AND derivation_method IN ('CURATED', 'SUPPLIER_TDS', 'LITERATURE')
                    LIMIT 1
                    """,
                    (ingredient_master_id,),
                ).fetchone()
                if curated:
                    return

            existing = conn.execute(
                """
                SELECT id FROM ingredient_ph_profile
                WHERE ingredient_master_id = ? AND is_active = 1
                  AND COALESCE(preferred_ph_min, -1) = COALESCE(?, -1)
                  AND COALESCE(preferred_ph_max, -1) = COALESCE(?, -1)
                  AND COALESCE(ph_sensitive, -1) = COALESCE(?, -1)
                  AND COALESCE(ph_notes, '') = COALESCE(?, '')
                  AND derivation_method = ?
                LIMIT 1
                """,
                (
                    ingredient_master_id,
                    preferred_ph_min,
                    preferred_ph_max,
                    1 if ph_sensitive is True else 0 if ph_sensitive is False else None,
                    ph_notes,
                    derivation_method,
                ),
            ).fetchone()
            if existing:
                return

            now = utc_now_iso()
            conn.execute(
                """
                INSERT INTO ingredient_ph_profile(
                    ingredient_master_id, preferred_ph_min, preferred_ph_max, ph_sensitive,
                    ph_notes, derivation_method, confidence_score, is_active, created_at, updated_at, evidence_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
                """,
                (
                    ingredient_master_id,
                    preferred_ph_min,
                    preferred_ph_max,
                    1 if ph_sensitive is True else 0 if ph_sensitive is False else None,
                    ph_notes,
                    derivation_method,
                    confidence_score,
                    now,
                    now,
                    evidence_id,
                ),
            )

    def upsert_manual_override(
        self,
        ingredient_master_id: int,
        field_name: str,
        override_value_text: str | None,
        override_value_num: float | None,
        reason: str | None,
        source_type: str | None,
        reference_text: str | None,
    ) -> None:
        now = utc_now_iso()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO ingredient_manual_overrides(
                    ingredient_master_id, field_name, override_value_text, override_value_num, reason,
                    source_type, reference_text, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ingredient_master_id,
                    field_name,
                    override_value_text,
                    override_value_num,
                    reason,
                    source_type,
                    reference_text,
                    now,
                    now,
                ),
            )

    def upsert_pairwise_rule(
        self,
        rule_code: str,
        ingredient_name_a: str | None,
        ingredient_name_b: str | None,
        tag_a: str | None,
        tag_b: str | None,
        condition_context: dict[str, Any],
        severity: str,
        penalty_points: float,
        message: str,
        suggestion: str | None,
        source_type: str,
    ) -> None:
        now = utc_now_iso()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO pairwise_compatibility_rules(
                    rule_code, ingredient_name_a, ingredient_name_b, tag_a, tag_b,
                    condition_context_json, severity, penalty_points, message, suggestion,
                    source_type, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(rule_code) DO UPDATE SET
                    ingredient_name_a = excluded.ingredient_name_a,
                    ingredient_name_b = excluded.ingredient_name_b,
                    tag_a = excluded.tag_a,
                    tag_b = excluded.tag_b,
                    condition_context_json = excluded.condition_context_json,
                    severity = excluded.severity,
                    penalty_points = excluded.penalty_points,
                    message = excluded.message,
                    suggestion = excluded.suggestion,
                    source_type = excluded.source_type,
                    updated_at = excluded.updated_at
                """,
                (
                    rule_code,
                    ingredient_name_a,
                    ingredient_name_b,
                    tag_a,
                    tag_b,
                    json.dumps(condition_context or {}),
                    severity,
                    penalty_points,
                    message,
                    suggestion,
                    source_type,
                    now,
                    now,
                ),
            )

    def upsert_priority_score(
        self,
        ingredient_master_id: int,
        frequency_score: float,
        source_coverage_score: float,
        role_importance_score: float,
        regulatory_relevance_score: float,
        name_confidence_score: float,
        total_priority_score: float,
        rank_position: int | None,
        selected_for_curated_kb: bool,
        priority_tier: str | None,
        scoring_version: str,
    ) -> None:
        now = utc_now_iso()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO ingredient_priority_scores(
                    ingredient_master_id, frequency_score, source_coverage_score, role_importance_score,
                    regulatory_relevance_score, name_confidence_score, total_priority_score, rank_position,
                    selected_for_curated_kb, priority_tier, scoring_version, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(ingredient_master_id) DO UPDATE SET
                    frequency_score = excluded.frequency_score,
                    source_coverage_score = excluded.source_coverage_score,
                    role_importance_score = excluded.role_importance_score,
                    regulatory_relevance_score = excluded.regulatory_relevance_score,
                    name_confidence_score = excluded.name_confidence_score,
                    total_priority_score = excluded.total_priority_score,
                    rank_position = excluded.rank_position,
                    selected_for_curated_kb = excluded.selected_for_curated_kb,
                    priority_tier = excluded.priority_tier,
                    scoring_version = excluded.scoring_version,
                    updated_at = excluded.updated_at
                """,
                (
                    ingredient_master_id,
                    frequency_score,
                    source_coverage_score,
                    role_importance_score,
                    regulatory_relevance_score,
                    name_confidence_score,
                    total_priority_score,
                    rank_position,
                    1 if selected_for_curated_kb else 0,
                    priority_tier,
                    scoring_version,
                    now,
                    now,
                ),
            )

    def update_priority_selection(
        self,
        ingredient_master_id: int,
        selected_for_curated_kb: bool,
        priority_tier: str | None,
        rank_position: int | None = None,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE ingredient_priority_scores
                SET selected_for_curated_kb = ?,
                    priority_tier = ?,
                    rank_position = COALESCE(?, rank_position),
                    updated_at = ?
                WHERE ingredient_master_id = ?
                """,
                (
                    1 if selected_for_curated_kb else 0,
                    priority_tier,
                    rank_position,
                    utc_now_iso(),
                    ingredient_master_id,
                ),
            )

    def summary_counts(self) -> dict[str, int]:
        with self.connect() as conn:
            return {
                "cosing_raw": int(conn.execute("SELECT COUNT(*) FROM cosing_raw").fetchone()[0]),
                "cosing_parsed": int(conn.execute("SELECT COUNT(*) FROM cosing_parsed").fetchone()[0]),
                "fda_iid_raw": int(conn.execute("SELECT COUNT(*) FROM fda_iid_raw").fetchone()[0]),
                "fda_iid_parsed": int(conn.execute("SELECT COUNT(*) FROM fda_iid_parsed").fetchone()[0]),
                "fda_unii_raw": int(conn.execute("SELECT COUNT(*) FROM fda_unii_raw").fetchone()[0]),
                "fda_unii_parsed": int(conn.execute("SELECT COUNT(*) FROM fda_unii_parsed").fetchone()[0]),
                "chebi_raw": int(conn.execute("SELECT COUNT(*) FROM chebi_raw").fetchone()[0]),
                "chebi_parsed": int(conn.execute("SELECT COUNT(*) FROM chebi_parsed").fetchone()[0]),
                "pcpc_raw": int(conn.execute("SELECT COUNT(*) FROM pcpc_raw").fetchone()[0]),
                "pcpc_parsed": int(conn.execute("SELECT COUNT(*) FROM pcpc_parsed").fetchone()[0]),
                "pubchem_raw": int(conn.execute("SELECT COUNT(*) FROM pubchem_raw").fetchone()[0]),
                "pubchem_attempted": int(conn.execute("SELECT COUNT(DISTINCT record_key) FROM pubchem_raw").fetchone()[0]),
                "enriched": int(conn.execute("SELECT COUNT(*) FROM ingredient_enrichment").fetchone()[0]),
                "ingredient_master": int(conn.execute("SELECT COUNT(*) FROM ingredient_master").fetchone()[0]),
                "ingredient_aliases": int(conn.execute("SELECT COUNT(*) FROM ingredient_aliases").fetchone()[0]),
                "ingredient_identifiers": int(conn.execute("SELECT COUNT(*) FROM ingredient_identifiers").fetchone()[0]),
                "merge_candidates": int(conn.execute("SELECT COUNT(*) FROM ingredient_merge_candidates").fetchone()[0]),
                "ingredient_ph_profile": int(conn.execute("SELECT COUNT(*) FROM ingredient_ph_profile").fetchone()[0]),
                "ingredient_stability_profile": int(conn.execute("SELECT COUNT(*) FROM ingredient_stability_profile").fetchone()[0]),
                "ingredient_role_tags": int(conn.execute("SELECT COUNT(*) FROM ingredient_role_tags").fetchone()[0]),
                "ingredient_pairwise_tags": int(conn.execute("SELECT COUNT(*) FROM ingredient_pairwise_tags").fetchone()[0]),
                "ingredient_manual_overrides": int(conn.execute("SELECT COUNT(*) FROM ingredient_manual_overrides").fetchone()[0]),
                "compatibility_evidence": int(conn.execute("SELECT COUNT(*) FROM compatibility_evidence").fetchone()[0]),
                "pairwise_compatibility_rules": int(conn.execute("SELECT COUNT(*) FROM pairwise_compatibility_rules").fetchone()[0]),
                "ingredient_priority_scores": int(conn.execute("SELECT COUNT(*) FROM ingredient_priority_scores").fetchone()[0]),
                "unmatched": int(conn.execute("SELECT COUNT(*) FROM unmatched").fetchone()[0]),
                "failures": int(conn.execute("SELECT COUNT(*) FROM failures").fetchone()[0]),
                "ambiguous": int(conn.execute("SELECT COUNT(*) FROM ingredient_enrichment WHERE match_status='ambiguous'").fetchone()[0]),
                "matched": int(conn.execute("SELECT COUNT(*) FROM ingredient_enrichment WHERE match_status='matched'").fetchone()[0]),
            }

    def export_table(self, table: str) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(f"SELECT * FROM {table}").fetchall()
            return [dict(r) for r in rows]

    def duplicates_report_rows(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    im.id AS master_id,
                    im.canonical_name,
                    COUNT(sl.id) AS source_link_count,
                    GROUP_CONCAT(sl.source_name || ':' || sl.source_record_key, ' | ') AS source_records
                FROM ingredient_master im
                JOIN source_links sl ON sl.master_id = im.id
                GROUP BY im.id, im.canonical_name
                HAVING COUNT(sl.id) > 1
                ORDER BY source_link_count DESC, im.id ASC
                """
            ).fetchall()
            return [dict(r) for r in rows]

    def conflicts_report_rows(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    mc.id,
                    mc.left_master_id,
                    l.canonical_name AS left_name,
                    mc.right_master_id,
                    r.canonical_name AS right_name,
                    mc.match_method,
                    mc.confidence_score,
                    mc.evidence_json,
                    mc.status,
                    mc.created_at
                FROM ingredient_merge_candidates mc
                JOIN ingredient_master l ON l.id = mc.left_master_id
                JOIN ingredient_master r ON r.id = mc.right_master_id
                WHERE mc.match_method = 'identifier_conflict'
                ORDER BY mc.id DESC
                """
            ).fetchall()
            return [dict(r) for r in rows]
