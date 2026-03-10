from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
from typing import Any

from ingredient_pipeline.config import Settings
from ingredient_pipeline.compatibility.curated_import import import_curated_compatibility
from ingredient_pipeline.compatibility.pairwise_tags import infer_pairwise_tags
from ingredient_pipeline.compatibility.priority_scoring import (
    export_top_ingredients,
    score_priority_ingredients,
    select_top_ingredients,
)
from ingredient_pipeline.compatibility.role_normalizer import infer_role_tags
from ingredient_pipeline.compatibility.schema import ensure_curated_template, seed_pairwise_rules
from ingredient_pipeline.compatibility.stability_rules import infer_stability
from ingredient_pipeline.enrichment.pubchem import PubChemEnricher
from ingredient_pipeline.phase2.identity import IdentityResolver
from ingredient_pipeline.sources.cosing import CosingClient, CosingParser
from ingredient_pipeline.sources.chebi import ChebiClient, ChebiParser
from ingredient_pipeline.sources.fda_iid import FdaIidClient, FdaIidParser
from ingredient_pipeline.sources.fda_unii import FdaUniiClient, FdaUniiParser
from ingredient_pipeline.sources.pcpc import PcpcClient, PcpcParser
from ingredient_pipeline.storage.db import Database
from ingredient_pipeline.utils.http_client import build_session

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.db = Database(settings.db_path)
        self.http = build_session(
            user_agent=settings.user_agent,
            requests_per_second=settings.requests_per_second,
            timeout_seconds=settings.timeout_seconds,
            max_retries=settings.max_retries,
        )

    def acquire_cosing(
        self,
        annex_formats: list[str] | None = None,
        annexes: list[str] | None = None,
        page_size: int = 200,
        max_pages: int = 30,
        include_substances: bool = True,
    ) -> dict[str, int]:
        run = self.db.start_run("acquire-cosing")
        client = CosingClient(self.http, self.db, self.settings.cosing_raw_dir)
        try:
            runtime = client.fetch_runtime_config(run.run_id)
            annex_count = client.acquire_annex_exports(run.run_id, runtime, annexes=annexes, formats=annex_formats)
            search_count = client.acquire_search_seed(
                run.run_id,
                runtime,
                page_size=page_size,
                max_pages=max_pages,
                include_substances=include_substances,
            )
            summary = {"annex_exports": annex_count, "search_pages": search_count}
            self.db.complete_run(run.run_id, "success", summary=summary)
            return summary
        except Exception as exc:  # noqa: BLE001
            self.db.insert_failure(run.run_id, "acquire-cosing", str(exc), source="cosing")
            self.db.complete_run(run.run_id, "failed", notes=str(exc))
            raise

    def parse_cosing(self) -> dict[str, int]:
        run = self.db.start_run("parse-cosing")
        parser = CosingParser(self.db)
        try:
            stats = parser.parse_all(run.run_id)
            self.db.complete_run(run.run_id, "success", summary=stats)
            return stats
        except Exception as exc:  # noqa: BLE001
            self.db.insert_failure(run.run_id, "parse-cosing", str(exc), source="cosing")
            self.db.complete_run(run.run_id, "failed", notes=str(exc))
            raise

    def acquire_fda_iid(self) -> dict[str, Any]:
        run = self.db.start_run("acquire-fda-iid")
        client = FdaIidClient(self.http, self.db, self.settings.fda_iid_raw_dir)
        try:
            summary = client.acquire(run.run_id)
            self.db.complete_run(run.run_id, "success", summary=summary)
            return summary
        except Exception as exc:  # noqa: BLE001
            self.db.insert_failure(run.run_id, "acquire-fda-iid", str(exc), source="fda_iid")
            self.db.complete_run(run.run_id, "failed", notes=str(exc))
            raise

    def parse_fda_iid(self, resume: bool = True) -> dict[str, int]:
        run = self.db.start_run("parse-fda-iid")
        parser = FdaIidParser(self.db)
        try:
            stats = parser.parse_all(run.run_id, resume=resume)
            self.db.complete_run(run.run_id, "success", summary=stats)
            return stats
        except Exception as exc:  # noqa: BLE001
            self.db.insert_failure(run.run_id, "parse-fda-iid", str(exc), source="fda_iid")
            self.db.complete_run(run.run_id, "failed", notes=str(exc))
            raise

    def acquire_fda_unii(self) -> dict[str, Any]:
        run = self.db.start_run("acquire-fda-unii")
        client = FdaUniiClient(self.http, self.db, self.settings.fda_unii_raw_dir)
        try:
            result = client.acquire(run.run_id)
            summary = {"files_downloaded": result.files_downloaded, "bytes_downloaded": result.bytes_downloaded}
            self.db.complete_run(run.run_id, "success", summary=summary)
            return summary
        except Exception as exc:  # noqa: BLE001
            self.db.insert_failure(run.run_id, "acquire-fda-unii", str(exc), source="fda_unii")
            self.db.complete_run(run.run_id, "failed", notes=str(exc))
            raise

    def acquire_chebi(self, id_list_file: Path | None = None, max_queries: int = 200) -> dict[str, Any]:
        run = self.db.start_run("acquire-chebi")
        client = ChebiClient(self.http, self.db, self.settings.data_dir / "raw" / "chebi", enabled=self.settings.chebi_enabled)
        try:
            result = client.acquire(run.run_id, id_list_file=id_list_file, max_queries=max_queries)
            summary = {"acquired": result.acquired, "failed": result.failed, "enabled": self.settings.chebi_enabled}
            self.db.complete_run(run.run_id, "success", summary=summary)
            return summary
        except Exception as exc:  # noqa: BLE001
            self.db.insert_failure(run.run_id, "acquire-chebi", str(exc), source="chebi")
            self.db.complete_run(run.run_id, "failed", notes=str(exc))
            raise

    def parse_chebi(self, resume: bool = True) -> dict[str, int]:
        run = self.db.start_run("parse-chebi")
        parser = ChebiParser(self.db)
        try:
            stats = parser.parse_all(run.run_id, resume=resume)
            self.db.complete_run(run.run_id, "success", summary=stats)
            return stats
        except Exception as exc:  # noqa: BLE001
            self.db.insert_failure(run.run_id, "parse-chebi", str(exc), source="chebi")
            self.db.complete_run(run.run_id, "failed", notes=str(exc))
            raise

    def import_pcpc(self, file_path: Path) -> dict[str, Any]:
        run = self.db.start_run("import-pcpc")
        client = PcpcClient(self.db, self.settings.data_dir / "raw" / "pcpc")
        try:
            result = client.import_file(run.run_id, file_path=file_path)
            summary = {"stored_path": result.stored_path, "sha256": result.sha256}
            self.db.complete_run(run.run_id, "success", summary=summary)
            return summary
        except Exception as exc:  # noqa: BLE001
            self.db.insert_failure(run.run_id, "import-pcpc", str(exc), source="pcpc")
            self.db.complete_run(run.run_id, "failed", notes=str(exc))
            raise

    def parse_pcpc(self, resume: bool = True) -> dict[str, int]:
        run = self.db.start_run("parse-pcpc")
        parser = PcpcParser(self.db)
        try:
            stats = parser.parse_all(run.run_id, resume=resume)
            self.db.complete_run(run.run_id, "success", summary=stats)
            return stats
        except Exception as exc:  # noqa: BLE001
            self.db.insert_failure(run.run_id, "parse-pcpc", str(exc), source="pcpc")
            self.db.complete_run(run.run_id, "failed", notes=str(exc))
            raise

    def parse_fda_unii(self, resume: bool = True) -> dict[str, int]:
        run = self.db.start_run("parse-fda-unii")
        parser = FdaUniiParser(self.db)
        try:
            stats = parser.parse_all(run.run_id, resume=resume)
            self.db.complete_run(run.run_id, "success", summary=stats)
            return stats
        except Exception as exc:  # noqa: BLE001
            self.db.insert_failure(run.run_id, "parse-fda-unii", str(exc), source="fda_unii")
            self.db.complete_run(run.run_id, "failed", notes=str(exc))
            raise

    def enrich_pubchem(self, only_unenriched: bool = True, resume: bool = True) -> dict[str, int]:
        run = self.db.start_run("enrich-pubchem")
        enricher = PubChemEnricher(self.http, self.db, self.settings.pubchem_raw_dir)
        try:
            records = self.db.iter_cosing_parsed(only_unenriched=only_unenriched)
            stats = enricher.enrich_records(run.run_id, records=records, resume=resume)
            self.db.complete_run(run.run_id, "success", summary=stats)
            return stats
        except Exception as exc:  # noqa: BLE001
            self.db.insert_failure(run.run_id, "enrich-pubchem", str(exc), source="pubchem")
            self.db.complete_run(run.run_id, "failed", notes=str(exc))
            raise

    def normalize_identities(self) -> dict[str, int]:
        resolver = IdentityResolver(self.db)
        return resolver.run()

    def build_master_ingredients(self) -> dict[str, int]:
        return self.normalize_identities()

    def rebuild_master_ingredients(self) -> dict[str, int]:
        return self.normalize_identities()

    def run_all(self) -> dict[str, Any]:
        a = self.acquire_cosing()
        p = self.parse_cosing()
        e = self.enrich_pubchem(only_unenriched=True, resume=True)
        summary = self.summary_report()
        return {"acquire": a, "parse": p, "enrich": e, "summary": summary}

    def phase2_run_all(self) -> dict[str, Any]:
        steps = {
            "acquire_fda_iid": self.acquire_fda_iid(),
            "parse_fda_iid": self.parse_fda_iid(resume=True),
            "acquire_fda_unii": self.acquire_fda_unii(),
            "parse_fda_unii": self.parse_fda_unii(resume=True),
            "acquire_chebi": self.acquire_chebi(),
            "parse_chebi": self.parse_chebi(resume=True),
            "parse_pcpc": self.parse_pcpc(resume=True),
            "normalize_identities": self.normalize_identities(),
        }
        steps["summary"] = self.summary_report()
        return steps

    def infer_role_tags(self) -> dict[str, int]:
        return infer_role_tags(self.db)

    def infer_stability(self) -> dict[str, int]:
        return infer_stability(self.db)

    def infer_pairwise_tags(self) -> dict[str, int]:
        seeded = seed_pairwise_rules(self.db)
        stats = infer_pairwise_tags(self.db)
        return {"seeded_rules": seeded, **stats}

    def import_curated_compatibility(self, file_path: Path) -> dict[str, int]:
        return import_curated_compatibility(self.db, file_path)

    def score_priority_ingredients(self, scoring_version: str = "v1") -> dict[str, int]:
        return score_priority_ingredients(self.db, scoring_version=scoring_version)

    def select_top_ingredients(self, top_n: int = 300) -> dict[str, int]:
        return select_top_ingredients(self.db, top_n=top_n)

    def export_top_ingredients(self, out_path: Path | None = None, top_n: int = 300) -> str:
        out_path = out_path or (self.settings.exports_dir / "top_ingredients.csv")
        return export_top_ingredients(self.db, out=out_path, top_n=top_n)

    def ensure_compatibility_template(self) -> str:
        return str(ensure_curated_template(self.settings.data_dir / "templates" / "compatibility_seed_template.csv"))

    def enrich_compatibility(self, top_n: int = 300) -> dict[str, Any]:
        role = self.infer_role_tags()
        stability = self.infer_stability()
        pairwise = self.infer_pairwise_tags()
        scored = self.score_priority_ingredients()
        selected = self.select_top_ingredients(top_n=top_n)
        return {
            "infer_role_tags": role,
            "infer_stability": stability,
            "infer_pairwise_tags": pairwise,
            "score_priority_ingredients": scored,
            "select_top_ingredients": selected,
        }

    def resume_last_run(self) -> dict[str, Any]:
        p = self.parse_cosing()
        e = self.enrich_pubchem(only_unenriched=True, resume=True)
        return {"parse": p, "enrich": e, "summary": self.summary_report()}

    def export_jsonl(self, out_dir: Path | None = None) -> dict[str, str]:
        out_dir = out_dir or self.settings.exports_dir
        out_dir.mkdir(parents=True, exist_ok=True)

        targets: dict[str, str] = {}
        for table in [
            "cosing_raw",
            "cosing_parsed",
            "fda_iid_raw",
            "fda_iid_parsed",
            "fda_unii_raw",
            "fda_unii_parsed",
            "chebi_raw",
            "chebi_parsed",
            "pcpc_raw",
            "pcpc_parsed",
            "pubchem_raw",
            "ingredient_enrichment",
            "acquisition_runs",
            "normalization_runs",
            "ingredient_master",
            "ingredient_aliases",
            "ingredient_identifiers",
            "source_links",
            "ingredient_merge_candidates",
            "ingredient_merge_decisions",
            "ingredient_ph_profile",
            "ingredient_stability_profile",
            "ingredient_role_tags",
            "ingredient_pairwise_tags",
            "ingredient_manual_overrides",
            "compatibility_evidence",
            "pairwise_compatibility_rules",
            "ingredient_priority_scores",
            "failures",
            "unmatched",
        ]:
            path = out_dir / f"{table}.jsonl"
            with path.open("w", encoding="utf-8") as f:
                for row in self.db.export_table(table):
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
            targets[table] = str(path)

        return targets

    def export_master_jsonl(self, out_dir: Path | None = None) -> dict[str, str]:
        out_dir = out_dir or self.settings.exports_dir
        out_dir.mkdir(parents=True, exist_ok=True)
        tables = [
            "ingredient_master",
            "ingredient_aliases",
            "ingredient_identifiers",
            "source_links",
            "ingredient_merge_candidates",
            "ingredient_ph_profile",
            "ingredient_stability_profile",
            "ingredient_role_tags",
            "ingredient_pairwise_tags",
            "ingredient_manual_overrides",
            "compatibility_evidence",
            "ingredient_priority_scores",
        ]
        out: dict[str, str] = {}
        for table in tables:
            path = out_dir / f"{table}.jsonl"
            with path.open("w", encoding="utf-8") as f:
                for row in self.db.export_table(table):
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
            out[table] = str(path)
        return out

    def export_csv_summary(self, out_path: Path | None = None) -> str:
        out_path = out_path or (self.settings.exports_dir / "summary.csv")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        counts = self.summary_report()

        with out_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["metric", "value"])
            for key, value in counts.items():
                writer.writerow([key, value])

        return str(out_path)

    def export_master_csv(self, out_path: Path | None = None) -> str:
        out_path = out_path or (self.settings.exports_dir / "master_summary.csv")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        counts = self.summary_report()
        keep = ["ingredient_master", "ingredient_aliases", "ingredient_identifiers", "merge_candidates"]
        with out_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["metric", "value"])
            for key in keep:
                writer.writerow([key, counts.get(key, 0)])
        return str(out_path)

    def export_duplicates_report(self, out_path: Path | None = None) -> str:
        out_path = out_path or (self.settings.exports_dir / "duplicates_report.csv")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        rows = self.db.duplicates_report_rows()
        with out_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["master_id", "canonical_name", "source_link_count", "source_records"])
            writer.writeheader()
            writer.writerows(rows)
        return str(out_path)

    def export_conflicts_report(self, out_path: Path | None = None) -> str:
        out_path = out_path or (self.settings.exports_dir / "conflicts_report.csv")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        rows = self.db.conflicts_report_rows()
        with out_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "id",
                    "left_master_id",
                    "left_name",
                    "right_master_id",
                    "right_name",
                    "match_method",
                    "confidence_score",
                    "evidence_json",
                    "status",
                    "created_at",
                ],
            )
            writer.writeheader()
            writer.writerows(rows)
        return str(out_path)

    def summary_report(self) -> dict[str, int]:
        counts = self.db.summary_counts()
        return {
            "total_acquired_from_cosing": counts["cosing_raw"],
            "total_parsed_cosing": counts["cosing_parsed"],
            "total_acquired_fda_iid": counts["fda_iid_raw"],
            "total_parsed_fda_iid": counts["fda_iid_parsed"],
            "total_acquired_fda_unii": counts["fda_unii_raw"],
            "total_parsed_fda_unii": counts["fda_unii_parsed"],
            "total_acquired_chebi": counts["chebi_raw"],
            "total_parsed_chebi": counts["chebi_parsed"],
            "total_imported_pcpc": counts["pcpc_raw"],
            "total_parsed_pcpc": counts["pcpc_parsed"],
            "total_sent_to_pubchem": counts["pubchem_attempted"],
            "matched": counts["matched"],
            "unmatched": counts["unmatched"],
            "ambiguous": counts["ambiguous"],
            "ingredient_master": counts["ingredient_master"],
            "ingredient_aliases": counts["ingredient_aliases"],
            "ingredient_identifiers": counts["ingredient_identifiers"],
            "merge_candidates": counts["merge_candidates"],
            "ingredient_ph_profile": counts["ingredient_ph_profile"],
            "ingredient_stability_profile": counts["ingredient_stability_profile"],
            "ingredient_role_tags": counts["ingredient_role_tags"],
            "ingredient_pairwise_tags": counts["ingredient_pairwise_tags"],
            "ingredient_priority_scores": counts["ingredient_priority_scores"],
            "failed": counts["failures"],
        }
