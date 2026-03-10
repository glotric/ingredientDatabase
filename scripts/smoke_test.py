from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ingredient_pipeline.config import load_settings
from ingredient_pipeline.pipeline import Pipeline
from ingredient_pipeline.utils.logging_utils import configure_logging


def main() -> None:
    settings = load_settings()
    configure_logging(settings.log_dir)
    pipeline = Pipeline(settings)
    summary = pipeline.summary_report()
    print("Smoke test OK")
    print(summary)


if __name__ == "__main__":
    main()
