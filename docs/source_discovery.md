# Source Discovery Note

Verified: 2026-03-09

## CosIng
- Runtime config: `https://ec.europa.eu/growth/tools-databases/cosing/assets/env-json-config.json`
- Annex export API path: `https://api.tech.ec.europa.eu/cosing20/1.0/api/annexes/{ANNEX}/export-{csv|xls|pdf}`
- Search API: `https://api.tech.ec.europa.eu/search-api/prod/rest/search`

## FDA IID
- Page: `https://www.fda.gov/drugs/drug-approvals-and-databases/inactive-ingredients-database-download`
- Download link discovered from page (`/media/.../download`), ingested as ZIP.

## FDA UNII/GSRS
- Archive page: `https://precision.fda.gov/uniisearch/archive`
- Latest ZIPs:
  - `https://precision.fda.gov/uniisearch/archive/latest/UNIIs.zip`
  - `https://precision.fda.gov/uniisearch/archive/latest/UNII_Data.zip`

## PubChem
- PUG-REST endpoints under: `https://pubchem.ncbi.nlm.nih.gov/rest/pug/`

## PCPC / wINCI
- Pages referenced for documentation/licensing context only:
  - `https://www.personalcarecouncil.org/resources/inci/`
  - `https://incipedia.personalcarecouncil.org/`
  - `https://www.personalcarecouncil.org/resources/winci-web-based-ingredient-dictionary/`
- Implemented as manual file import only (no scraping).

## ChEBI
- API docs: `https://www.ebi.ac.uk/chebi/backend/api/docs/`
- Schema: `https://www.ebi.ac.uk/chebi/backend/api/schema/`
- Endpoints used:
  - `/chebi/backend/api/public/es_search/`
  - `/chebi/backend/api/public/compound/{chebi_id}/`
