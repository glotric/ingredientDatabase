from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(slots=True)
class Settings:
    base_dir: Path
    data_dir: Path
    db_path: Path
    log_dir: Path
    cosing_raw_dir: Path
    pubchem_raw_dir: Path
    fda_iid_raw_dir: Path
    fda_unii_raw_dir: Path
    exports_dir: Path
    requests_per_second: float
    max_retries: int
    timeout_seconds: int
    user_agent: str
    chebi_enabled: bool


DEFAULT_USER_AGENT = "ingredient-pipeline/0.1 (+phase1-acquisition)"


def load_settings() -> Settings:
    load_dotenv()
    base_dir = Path(os.getenv("PIPELINE_BASE_DIR", Path.cwd())).resolve()
    data_dir = Path(os.getenv("PIPELINE_DATA_DIR", base_dir / "data")).resolve()
    db_path = Path(os.getenv("PIPELINE_DB_PATH", data_dir / "ingredient_pipeline.db")).resolve()
    log_dir = Path(os.getenv("PIPELINE_LOG_DIR", data_dir / "logs")).resolve()

    cosing_raw_dir = data_dir / "raw" / "cosing"
    pubchem_raw_dir = data_dir / "raw" / "pubchem"
    fda_iid_raw_dir = data_dir / "raw" / "fda_iid"
    fda_unii_raw_dir = data_dir / "raw" / "fda_unii"
    exports_dir = data_dir / "exports"

    return Settings(
        base_dir=base_dir,
        data_dir=data_dir,
        db_path=db_path,
        log_dir=log_dir,
        cosing_raw_dir=cosing_raw_dir,
        pubchem_raw_dir=pubchem_raw_dir,
        fda_iid_raw_dir=fda_iid_raw_dir,
        fda_unii_raw_dir=fda_unii_raw_dir,
        exports_dir=exports_dir,
        requests_per_second=float(os.getenv("PIPELINE_REQUESTS_PER_SECOND", "2.0")),
        max_retries=int(os.getenv("PIPELINE_MAX_RETRIES", "5")),
        timeout_seconds=int(os.getenv("PIPELINE_TIMEOUT_SECONDS", "30")),
        user_agent=os.getenv("PIPELINE_USER_AGENT", DEFAULT_USER_AGENT),
        chebi_enabled=os.getenv("CHEBI_ENABLED", "false").lower() in {"1", "true", "yes", "on"},
    )
