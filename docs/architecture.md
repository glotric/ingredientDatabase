# Architecture Note

## Source adapters
- CosIng: export + search API acquisition.
- UNII/GSRS: FDA archive ZIP ingestion.
- PubChem: PUG-REST enrichment.
- PCPC: local manual-import adapter (licensed source, no scraping).
- ChEBI: optional API adapter behind config flag.

Each adapter writes raw payload metadata + parsed rows.

## Canonical layer
- `ingredient_master`: canonical ingredient node.
- `ingredient_aliases`: deduplicated aliases (source-attributed).
- `ingredient_identifiers`: normalized identifiers (CAS/UNII/PubChem CID/ChEBI ID/EC).
- `source_links`: provenance links to source rows with match evidence.
- `ingredient_merge_candidates`: fuzzy review queue + identifier conflict queue.
- `ingredient_merge_decisions`: review decisions.

## Duplicate handling
Deterministic merge priority:
1. CAS
2. UNII
3. PubChem CID
4. ChEBI ID
5. normalized INCI
6. normalized preferred/common name
7. synonym

Conflict rule:
- Same name + conflicting strong identifiers -> no auto-merge, emit `identifier_conflict` candidate.

## Canonical naming priority
1. PCPC INCI
2. CosIng INCI
3. existing canonical
4. UNII preferred name
5. PubChem preferred/IUPAC
6. ChEBI preferred

## Deferred
- Regulatory rules engine and conflict adjudication UI.

## Compatibility layer (on top of canonical)
The compatibility layer reads canonical records from `ingredient_master` and stores separate compatibility facts with provenance:
- `ingredient_ph_profile`
- `ingredient_stability_profile`
- `ingredient_role_tags`
- `ingredient_pairwise_tags`
- `ingredient_manual_overrides`
- `compatibility_evidence`
- `pairwise_compatibility_rules`
- `ingredient_priority_scores`

### Automatic vs curated
- Automatic:
  - Role normalization from source functions.
  - Stability flags via conservative RDKit SMARTS heuristics.
  - Pairwise tags from deterministic name/role/SMARTS heuristics.
  - Priority scoring and top-ingredient selection.
- Curated:
  - CSV/XLSX import for pH ranges, stability flags, pairwise tags, and manual overrides.
  - Curated rows are additive and are not overwritten by inferred rows.

### Top ingredient selection
Priority score is computed as:
- `0.40 * frequency_score`
- `0.20 * source_coverage_score`
- `0.20 * role_importance_score`
- `0.10 * regulatory_relevance_score`
- `0.10 * name_confidence_score`

Selection tiers:
- top 100 -> `TIER_1`
- next 200 (default up to top 300) -> `TIER_2`
