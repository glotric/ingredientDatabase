from __future__ import annotations

import csv
import io
import json
import tempfile
import zipfile
from pathlib import Path

from ingredient_pipeline.phase2.identity import IdentityResolver
from ingredient_pipeline.sources.chebi import ChebiClient
from ingredient_pipeline.sources.chebi import ChebiParser
from ingredient_pipeline.sources.fda_iid import discover_iid_download_url, FdaIidParser
from ingredient_pipeline.sources.fda_unii import discover_unii_zip_links, FdaUniiParser
from ingredient_pipeline.sources.pcpc import PcpcClient, PcpcParser
from ingredient_pipeline.storage.db import Database, utc_now_iso


def _make_iid_zip(path: Path) -> None:
    header = [
        "INGREDIENT_NAME",
        "ROUTE",
        "DOSAGE_FORM",
        "CAS_NUMBER",
        "UNII",
        "POTENCY_AMOUNT",
        "POTENCY_UNIT",
        "MAXIMUM_DAILY_EXPOSURE",
        "MAXIMUM_DAILY_EXPOSURE_UNIT",
        "RECORD_UPDATED",
    ]
    rows = [
        ["GLYCERIN", "TOPICAL", "CREAM", "56-81-5", "PDC6A3C0OX", "5", "%w/w", "", "", "2026-01-01"],
        ["WATER", "TOPICAL", "CREAM", "7732-18-5", "059QF0KO0R", "", "", "", "", "2026-01-01"],
    ]
    sio = io.StringIO()
    writer = csv.writer(sio)
    writer.writerow(header)
    writer.writerows(rows)

    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("IIR_OCOMM.csv", sio.getvalue())


def _make_unii_zip(path: Path) -> None:
    names_txt = "Name\tTYPE\tUNII\tDisplay Name\nGLYCERIN\tcn\tPDC6A3C0OX\tGLYCERIN\n"
    records_txt = (
        "UNII\tDisplay Name\tRN\tEC\tNCIT\tRXCUI\tPUBCHEM\tSMSID\tEPA_CompTox\tCATALOGUE_OF_LIFE\tITIS\tNCBI\tPLANTS\tPOWO\tGRIN\tMPNS\tINN_ID\tUSAN_ID\tDAILYMED\tMF\tINCHIKEY\tSMILES\tINGREDIENT_TYPE\tSUBSTANCE_TYPE\tUUID\n"
        "PDC6A3C0OX\tGLYCERIN\t56-81-5\t\t\t\t753\t\t\t\t\t\t\t\t\t\t\t\t\tC3H8O3\tPEDCQBHIVMGVHV-UHFFFAOYSA-N\tC(C(CO)O)O\tINGREDIENT SUBSTANCE\tchemical\tuuid-1\n"
        "XXXXXXX111\tALPHA SAMPLE\t111-11-1\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\tINGREDIENT SUBSTANCE\tchemical\tuuid-2\n"
    )
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("UNII_Names_test.txt", names_txt)
        zf.writestr("UNII_Records_test.txt", records_txt)


def _make_chebi_json(path: Path) -> None:
    payload = {
        "id": 17754,
        "chebi_accession": "CHEBI:17754",
        "name": "glycerol",
        "definition": "A trihydroxy alcohol",
        "names": {
            "SYNONYM": [
                {"name": "glycerin"},
                {"name": "propane-1,2,3-triol"},
            ]
        },
        "default_structure": {
            "smiles": "C(C(CO)O)O",
            "standard_inchi_key": "PEDCQBHIVMGVHV-UHFFFAOYSA-N",
        },
        "database_accessions": {
            "CAS": [{"accession_number": "56-81-5", "source_name": "ChemIDplus"}],
            "MANUAL_X_REF": [{"accession_number": "753", "source_name": "PubChem"}],
        },
        "roles_classification": [{"name": "solvent"}],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _make_pcpc_csv(path: Path) -> None:
    rows = [
        {
            "INCI Name": "GLYCERIN",
            "CAS Number": "56-81-5",
            "Synonyms": "GLYCEROL;1,2,3-PROPANETRIOL",
            "Functions": "HUMECTANT;SOLVENT",
            "Description": "PCPC sample row",
        },
        {
            "INCI Name": "ALPHA SAMPLE",
            "CAS Number": "999-99-9",
            "Synonyms": "ALPHA SAMPLE",
            "Functions": "SKIN CONDITIONING",
            "Description": "Conflicting CAS test",
        },
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def test_source_discovery_helpers() -> None:
    iid_html = '<html><body><a href="/media/190589/download">Download</a></body></html>'
    url = discover_iid_download_url(iid_html)
    assert url == "https://www.fda.gov/media/190589/download"

    unii_html = '<html><body><a href="/uniisearch/archive/latest/UNIIs.zip">UNIIs.zip</a></body></html>'
    links = discover_unii_zip_links(unii_html)
    assert "https://precision.fda.gov/uniisearch/archive/latest/UNIIs.zip" in links


def test_parsers_merge_and_conflicts() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        db = Database(root / "test.db")

        iid_zip = root / "iid.zip"
        unii_zip = root / "unii.zip"
        chebi_json = root / "chebi_compound.json"
        pcpc_csv = root / "pcpc.csv"

        _make_iid_zip(iid_zip)
        _make_unii_zip(unii_zip)
        _make_chebi_json(chebi_json)
        _make_pcpc_csv(pcpc_csv)

        run = db.start_run("smoke")
        db.insert_fda_iid_raw(
            {
                "run_id": run.run_id,
                "source_url": "https://www.fda.gov/media/190589/download",
                "payload_format": "zip",
                "http_status": 200,
                "retrieved_at": utc_now_iso(),
                "sha256": "iid-sha",
                "local_path": str(iid_zip),
                "metadata": {},
            }
        )
        db.insert_fda_unii_raw(
            {
                "run_id": run.run_id,
                "source_url": "https://precision.fda.gov/uniisearch/archive/latest/UNII_Data.zip",
                "payload_format": "zip",
                "http_status": 200,
                "retrieved_at": utc_now_iso(),
                "sha256": "unii-sha",
                "local_path": str(unii_zip),
                "metadata": {},
            }
        )
        db.insert_chebi_raw(
            {
                "run_id": run.run_id,
                "source_url": "https://www.ebi.ac.uk/chebi/backend/api/public/compound/CHEBI:17754/",
                "payload_format": "json",
                "http_status": 200,
                "retrieved_at": utc_now_iso(),
                "sha256": "chebi-sha",
                "local_path": str(chebi_json),
                "metadata": {"mode": "id"},
            }
        )

        # ChEBI acquisition smoke with fake HTTP client and tiny ID list
        class _Resp:
            def __init__(self, payload: dict):
                self.status_code = 200
                self._payload = payload

            def json(self):
                return self._payload

        class _Http:
            def request(self, method, url, **kwargs):
                _ = method, kwargs
                return _Resp(json.loads(chebi_json.read_text(encoding="utf-8")))

        id_list = root / "chebi_ids.txt"
        id_list.write_text("CHEBI:17754\n", encoding="utf-8")
        acq_stats = ChebiClient(_Http(), db, root / "chebi_raw", enabled=True).acquire(run.run_id, id_list_file=id_list)
        assert acq_stats.acquired >= 1

        iid_stats = FdaIidParser(db).parse_all(run.run_id, resume=False)
        unii_stats = FdaUniiParser(db).parse_all(run.run_id, resume=False)
        chebi_stats = ChebiParser(db).parse_all(run.run_id, resume=False)

        assert iid_stats["parsed"] >= 2
        assert unii_stats["parsed"] >= 2
        assert chebi_stats["parsed"] >= 1

        # PCPC manual import + parse
        pcpc_client = PcpcClient(db, root / "pcpc_raw")
        pcpc_client.import_file(run.run_id, pcpc_csv)
        pcpc_stats = PcpcParser(db).parse_all(run.run_id, resume=False)
        assert pcpc_stats["parsed"] >= 2

        # CosIng rows for same-CAS merge and same-name conflicting CAS
        db.upsert_cosing_parsed(
            {
                "run_id": run.run_id,
                "source_raw_id": None,
                "record_key": "cosing:test:glycerin",
                "item_type": "ingredient",
                "ingredient_name": "GLYCERIN",
                "inci_name": "GLYCERIN",
                "cas_no": "56-81-5",
                "ec_no": None,
                "function_names": None,
                "description": None,
                "annex_no": None,
                "ref_no": None,
                "status": "Active",
                "regulation_refs": "{}",
                "source_url": "urn:test",
                "parsed_at": utc_now_iso(),
                "provenance": {},
                "raw_record": {},
            }
        )
        db.upsert_cosing_parsed(
            {
                "run_id": run.run_id,
                "source_raw_id": None,
                "record_key": "cosing:test:alpha-conflict",
                "item_type": "ingredient",
                "ingredient_name": "ALPHA SAMPLE",
                "inci_name": "ALPHA SAMPLE",
                "cas_no": "111-11-1",
                "ec_no": None,
                "function_names": None,
                "description": None,
                "annex_no": None,
                "ref_no": None,
                "status": "Active",
                "regulation_refs": "{}",
                "source_url": "urn:test2",
                "parsed_at": utc_now_iso(),
                "provenance": {},
                "raw_record": {},
            }
        )

        summary = IdentityResolver(db).run()
        assert summary["source_rows_linked"] > 0
        counts = db.summary_counts()
        assert counts["ingredient_master"] >= 1
        assert counts["ingredient_identifiers"] >= 1

        # Expect duplicate merging and conflict queue entries
        dups = db.duplicates_report_rows()
        assert any("glycer" in (row["canonical_name"] or "").lower() for row in dups)
        conflicts = db.conflicts_report_rows()
        assert len(conflicts) >= 1


def main() -> None:
    test_source_discovery_helpers()
    test_parsers_merge_and_conflicts()
    print("phase2 smoke tests passed")


if __name__ == "__main__":
    main()
