from __future__ import annotations

import argparse
import json
from pathlib import Path

from ingredient_pipeline.config import load_settings
from ingredient_pipeline.pipeline import Pipeline
from ingredient_pipeline.utils.logging_utils import configure_logging


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingredient acquisition pipeline (phase 1 + phase 2)")
    sub = parser.add_subparsers(dest="command", required=True)

    acq = sub.add_parser("acquire-cosing", help="Acquire CosIng raw payloads")
    acq.add_argument("--annexes", default="II,III,IV,V,VI", help="Comma-separated annexes")
    acq.add_argument("--formats", default="csv", help="Comma-separated formats: csv,xls,pdf")
    acq.add_argument("--page-size", type=int, default=200)
    acq.add_argument("--max-pages", type=int, default=30)
    acq.add_argument("--ingredients-only", action="store_true")

    sub.add_parser("parse-cosing", help="Parse CosIng raw files into parsed table")

    sub.add_parser("acquire-fda-iid", help="Acquire FDA IID bulk download")
    iid_parse = sub.add_parser("parse-fda-iid", help="Parse FDA IID raw downloads")
    iid_parse.add_argument("--no-resume", action="store_true")

    sub.add_parser("acquire-fda-unii", help="Acquire FDA UNII/GSRS bulk downloads")
    unii_parse = sub.add_parser("parse-fda-unii", help="Parse FDA UNII raw downloads")
    unii_parse.add_argument("--no-resume", action="store_true")

    acq_chebi = sub.add_parser("acquire-chebi", help="Acquire ChEBI API raw payloads (optional by config)")
    acq_chebi.add_argument("--id-list", default=None, help="Optional file with one ChEBI ID per line")
    acq_chebi.add_argument("--max-queries", type=int, default=200, help="Max search terms when seeding from existing data")
    chebi_parse = sub.add_parser("parse-chebi", help="Parse ChEBI raw payloads")
    chebi_parse.add_argument("--no-resume", action="store_true")

    import_pcpc = sub.add_parser("import-pcpc", help="Import licensed/manual PCPC export file")
    import_pcpc.add_argument("--file", required=True, help="Path to local PCPC export (CSV/TSV/XLSX/JSON)")
    pcpc_parse = sub.add_parser("parse-pcpc", help="Parse imported PCPC files")
    pcpc_parse.add_argument("--no-resume", action="store_true")

    enrich = sub.add_parser("enrich-pubchem", help="Enrich parsed records with PubChem")
    enrich.add_argument("--all-records", action="store_true", help="Reprocess all parsed records")
    enrich.add_argument("--no-resume", action="store_true", help="Ignore checkpoints")

    sub.add_parser("normalize-identities", help="Run phase-2 deterministic normalization and identity resolution")
    sub.add_parser("build-master-ingredients", help="Build/refresh ingredient master layer")
    sub.add_parser("rebuild-master-ingredients", help="Rebuild/refresh canonical ingredient master layer")

    sub.add_parser("run-all", help="Run phase-1 acquire -> parse -> enrich")
    sub.add_parser("phase2-run-all", help="Run FDA acquisition/parsing + phase-2 normalization")
    sub.add_parser("resume-last-run", help="Resume from checkpoints")

    export_jsonl = sub.add_parser("export-jsonl", help="Export all core tables to JSONL")
    export_jsonl.add_argument("--out-dir", default=None)

    export_csv = sub.add_parser("export-csv-summary", help="Export summary counts to CSV")
    export_csv.add_argument("--out", default=None)

    export_master_jsonl = sub.add_parser("export-master-jsonl", help="Export master tables to JSONL")
    export_master_jsonl.add_argument("--out-dir", default=None)

    export_master_csv = sub.add_parser("export-master-csv", help="Export master summary CSV")
    export_master_csv.add_argument("--out", default=None)

    export_dups = sub.add_parser("export-duplicates-report", help="Export merged duplicates report CSV")
    export_dups.add_argument("--out", default=None)
    export_conflicts = sub.add_parser("export-conflicts-report", help="Export identifier/name conflict report CSV")
    export_conflicts.add_argument("--out", default=None)

    sub.add_parser("infer-role-tags", help="Infer normalized ingredient role tags from source functions")
    sub.add_parser("infer-stability", help="Infer stability flags via RDKit/SMARTS deterministic rules")
    sub.add_parser("infer-pairwise-tags", help="Infer pairwise interaction tags and seed deterministic pairwise rules")

    import_curated = sub.add_parser("import-curated-compatibility", help="Import local curated compatibility CSV/XLSX")
    import_curated.add_argument("--file", required=True, help="Path to curated compatibility file")

    score_prio = sub.add_parser("score-priority-ingredients", help="Compute ingredient priority scores")
    score_prio.add_argument("--version", default="v1", help="Scoring version label")

    select_top = sub.add_parser("select-top-ingredients", help="Mark top ingredients for curated compatibility KB")
    select_top.add_argument("--top-n", type=int, default=300, help="Top N ingredients to select")

    export_top = sub.add_parser("export-top-ingredients", help="Export top-ranked ingredients CSV")
    export_top.add_argument("--out", default=None, help="CSV output path")
    export_top.add_argument("--top-n", type=int, default=300, help="Rows to export")

    enrich_compat = sub.add_parser("enrich-compatibility", help="Run compatibility inference convenience pipeline")
    enrich_compat.add_argument("--top-n", type=int, default=300, help="Top N to select after scoring")

    sub.add_parser("create-curated-template", help="Create compatibility curated import CSV template")

    sub.add_parser("summary", help="Print summary report")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    settings = load_settings()
    configure_logging(settings.log_dir)
    pipeline = Pipeline(settings)

    if args.command == "acquire-cosing":
        annexes = [x.strip() for x in args.annexes.split(",") if x.strip()]
        formats = [x.strip() for x in args.formats.split(",") if x.strip()]
        result = pipeline.acquire_cosing(
            annexes=annexes,
            annex_formats=formats,
            page_size=args.page_size,
            max_pages=args.max_pages,
            include_substances=not args.ingredients_only,
        )
    elif args.command == "parse-cosing":
        result = pipeline.parse_cosing()
    elif args.command == "acquire-fda-iid":
        result = pipeline.acquire_fda_iid()
    elif args.command == "parse-fda-iid":
        result = pipeline.parse_fda_iid(resume=not args.no_resume)
    elif args.command == "acquire-fda-unii":
        result = pipeline.acquire_fda_unii()
    elif args.command == "parse-fda-unii":
        result = pipeline.parse_fda_unii(resume=not args.no_resume)
    elif args.command == "acquire-chebi":
        id_list = Path(args.id_list).resolve() if args.id_list else None
        result = pipeline.acquire_chebi(id_list_file=id_list, max_queries=args.max_queries)
    elif args.command == "parse-chebi":
        result = pipeline.parse_chebi(resume=not args.no_resume)
    elif args.command == "import-pcpc":
        result = pipeline.import_pcpc(Path(args.file).resolve())
    elif args.command == "parse-pcpc":
        result = pipeline.parse_pcpc(resume=not args.no_resume)
    elif args.command == "enrich-pubchem":
        result = pipeline.enrich_pubchem(only_unenriched=not args.all_records, resume=not args.no_resume)
    elif args.command == "normalize-identities":
        result = pipeline.normalize_identities()
    elif args.command == "build-master-ingredients":
        result = pipeline.build_master_ingredients()
    elif args.command == "rebuild-master-ingredients":
        result = pipeline.rebuild_master_ingredients()
    elif args.command == "run-all":
        result = pipeline.run_all()
    elif args.command == "phase2-run-all":
        result = pipeline.phase2_run_all()
    elif args.command == "resume-last-run":
        result = pipeline.resume_last_run()
    elif args.command == "export-jsonl":
        out_dir = Path(args.out_dir).resolve() if args.out_dir else None
        result = pipeline.export_jsonl(out_dir=out_dir)
    elif args.command == "export-csv-summary":
        out = Path(args.out).resolve() if args.out else None
        result = {"path": pipeline.export_csv_summary(out)}
    elif args.command == "export-master-jsonl":
        out_dir = Path(args.out_dir).resolve() if args.out_dir else None
        result = pipeline.export_master_jsonl(out_dir=out_dir)
    elif args.command == "export-master-csv":
        out = Path(args.out).resolve() if args.out else None
        result = {"path": pipeline.export_master_csv(out)}
    elif args.command == "export-duplicates-report":
        out = Path(args.out).resolve() if args.out else None
        result = {"path": pipeline.export_duplicates_report(out)}
    elif args.command == "export-conflicts-report":
        out = Path(args.out).resolve() if args.out else None
        result = {"path": pipeline.export_conflicts_report(out)}
    elif args.command == "infer-role-tags":
        result = pipeline.infer_role_tags()
    elif args.command == "infer-stability":
        result = pipeline.infer_stability()
    elif args.command == "infer-pairwise-tags":
        result = pipeline.infer_pairwise_tags()
    elif args.command == "import-curated-compatibility":
        result = pipeline.import_curated_compatibility(Path(args.file).resolve())
    elif args.command == "score-priority-ingredients":
        result = pipeline.score_priority_ingredients(scoring_version=args.version)
    elif args.command == "select-top-ingredients":
        result = pipeline.select_top_ingredients(top_n=args.top_n)
    elif args.command == "export-top-ingredients":
        out = Path(args.out).resolve() if args.out else None
        result = {"path": pipeline.export_top_ingredients(out_path=out, top_n=args.top_n)}
    elif args.command == "enrich-compatibility":
        result = pipeline.enrich_compatibility(top_n=args.top_n)
    elif args.command == "create-curated-template":
        result = {"path": pipeline.ensure_compatibility_template()}
    elif args.command == "summary":
        result = pipeline.summary_report()
    else:
        parser.error(f"Unknown command: {args.command}")
        return

    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
