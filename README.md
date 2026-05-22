# SAE 2026 — French Real Estate Market Analysis

## Business Question

> Given a price, a location, and a set of property characteristics, is this a good deal?

The goal of this project is to build a complete Business Intelligence tool that helps buyers, sellers, and real estate professionals answer one core question: **is this property fairly priced?**

To answer it, students must collect, clean, cross-reference, and expose data from multiple public sources, covering transaction history, energy performance, noise exposure, socio-economic context, and geographic information.

---

## Data Sources

### 1. DVF — Demandes de Valeurs Foncières
- **What**: Every property sale registered in France — price, surface area, number of rooms, property type (apartment, house, land), address, and date of transaction.
- **Source**: [data.gouv.fr](https://www.data.gouv.fr/fr/datasets/demandes-de-valeurs-foncieres/)
- **Format**: CSV (one file per year, per department, or as a national file)
- **Volume**: Tens of millions of rows nationally — students will need to think carefully about partitioning and incremental loading strategies.
- **Key fields**: `valeur_fonciere`, `surface_reelle_bati`, `nombre_pieces_principales`, `type_local`, `adresse_*`, `code_commune`, `date_mutation`

### 2. DPE — Diagnostic de Performance Énergétique
- **What**: Energy performance audits for existing properties — energy rating (A to G), estimated annual energy cost, heating system type, insulation quality.
- **Source**: [data.ademe.fr](https://data.ademe.fr/datasets/dpe03existant)
- **Format**: CSV
- **Join strategy**: Match to DVF by normalized address and commune code.
- **Key business question**: Does an A or B energy rating translate into a measurable price premium? Are buyers becoming more sensitive to energy ratings over time?

### 3. PEB — Plan d'Exposition au Bruit (Airport Noise Zones)
- **What**: Official noise exposure zone polygons around French airports (zones A, B, C, D — from most to least exposed). Properties located in these zones are subject to mandatory disclosure during any sale, as part of the État des Risques et Pollutions (ERP) document.
- **Sources**:
  - [GéoRisques API](https://georisques.gouv.fr) — REST API returning risk exposure (including PEB zone) for a given address or set of coordinates.
  - [data.gouv.fr — DGAC](https://www.data.gouv.fr/fr/datasets/?q=plan+exposition+bruit) — GeoJSON / Shapefile polygons per airport.
- **Format**: JSON (API) and GeoJSON / Shapefile (polygon boundaries)
- **Join strategy**: Spatial join — check whether a transaction's coordinates fall inside a PEB polygon.
- **Key business question**: Is there a measurable noise discount in PEB zones? Does it scale with zone severity? Does it vary by airport?

### 4. INSEE — Socio-economic Data per Commune
- **What**: Median household income, unemployment rate, population, age distribution, household composition — at commune or IRIS (sub-commune) level.
- **Source**: [insee.fr](https://www.insee.fr/fr/statistiques)
- **Format**: CSV and Excel (older datasets are often `.xlsx`)
- **Join strategy**: Join to DVF by commune INSEE code (`code_commune`).
- **Key business question**: Do higher-income communes show faster price growth? Is there a gentrification signal in rising transaction volumes combined with rising prices?

### 5. BAN — Base Adresse Nationale
- **What**: The national address database, mapping every French address to GPS coordinates (latitude/longitude).
- **Source**: [adresse.data.gouv.fr](https://adresse.data.gouv.fr/data/ban/adresses/latest/)
- **Format**: CSV
- **Role**: Geocoding bridge. DVF contains addresses but no coordinates. BAN is the key that unlocks all spatial analysis — without it, PEB zone joins and proximity calculations are not possible.

### 6. Administrative Boundaries
- **What**: Polygon geometries for communes, departments, and regions.
- **Source**: [data.gouv.fr — IGN Admin Express](https://www.data.gouv.fr/fr/datasets/adminexpress/)
- **Format**: GeoJSON / Shapefile
- **Role**: Spatial aggregation — group transactions by geographic zone, power choropleth maps in dashboards.

### 7. Points of Interest — Transport and Schools
- **What**: Train and metro station locations, school locations.
- **Sources**:
  - [SNCF Open Data](https://data.sncf.com/) — station coordinates (JSON / REST API)
  - [Overpass API](https://overpass-turbo.dev/) (OpenStreetMap) — schools and transport stops (JSON)
- **Format**: JSON / REST API
- **Join strategy**: Proximity join — compute distance between each transaction (from BAN) and its nearest station or school.
- **Key business question**: Does proximity to a train station add a measurable price premium? How many meters before the effect disappears?

---

### Format Overview

| Source | Format | Primary Join Key |
|---|---|---|
| DVF | CSV | address + commune code |
| DPE | CSV | address + commune code |
| PEB (GéoRisques) | JSON (REST API) | lat/lng coordinates |
| PEB (DGAC) | GeoJSON / Shapefile | spatial (polygon contains point) |
| INSEE | CSV / Excel | commune INSEE code |
| BAN | CSV | address → lat/lng |
| Admin boundaries | GeoJSON / Shapefile | commune INSEE code |
| Transport / Schools | JSON / REST API | lat/lng (proximity) |

---

## Architecture

The pipeline has three main stages: data ingestion, quality control and transformation, and reporting. Students are free to choose between an **ETL** (transform before loading) or **ELT** (transform after loading) approach, and to select the tools they are most comfortable with at each stage.

### Stage 1 — Data Ingestion

The ingestion stage is responsible for collecting raw data from all sources and persisting it to storage. Each source brings its own challenges:

- **Bulk file downloads** (DVF, DPE, BAN, INSEE): Download CSV or Excel files, parse them, and insert rows into raw tables. The main challenge here is volume — DVF alone can exceed 10 million rows nationally.
- **REST API calls** (GéoRisques, SNCF, Overpass): For each transaction with known coordinates, call the API and store the response. Students will need to handle rate limiting, pagination, and partial failures gracefully.
- **Geospatial files** (PEB, Admin Express): Load GeoJSON or Shapefile polygon data so that spatial queries can run efficiently.

A key concern at this stage is **dependency management**: some assets depend on others (coordinates from BAN are needed before PEB zone lookups can happen). The ingestion pipeline should make these dependencies explicit and allow partial re-runs without reprocessing everything from scratch.

### Stage 2 — Quality Control and Transformation

Raw data is rarely ready for analysis. This stage cleans, enriches, and joins the raw sources into analysis-ready tables.

Typical transformation steps for this project include:

- **Address normalization**: DVF address formats are inconsistent. Matching them to BAN entries requires normalization (lowercasing, removing special characters, handling abbreviations like `r.` for `rue`).
- **Spatial joins**: Determine, for each transaction, whether it falls inside a PEB noise zone and which commune polygon it belongs to.
- **Proximity calculations**: Compute the distance from each transaction to the nearest train station and school.
- **Deduplication**: DVF can contain multiple rows per transaction (one per cadastral parcel). These must be collapsed into a single transaction record.
- **Outlier filtering**: Some DVF entries are clearly erroneous (e.g., 1€ transactions between family members, or extreme price per m² values). Students must define and document their filtering rules.
- **Aggregations**: Compute price per m² by commune, by year, by property type, by DPE rating, by PEB zone, etc.

Data quality checks should be explicit and traceable: if a uniqueness constraint is violated or an unexpected null appears, the pipeline should surface it clearly rather than silently producing wrong results.

### Stage 3 — Reporting

The final stage exposes the clean, aggregated data to end users through dashboards and interactive tools.

The central dashboard should answer the main business question directly: **given a price, a location, and a set of property dimensions, is this a fair price?**

Suggested dashboard views:

- **Price map**: Price per m² by commune or department, filterable by property type and year, rendered as a choropleth map.
- **Price estimator**: Given user inputs (location, surface area, number of rooms, property type), show where this property would sit relative to comparable recent transactions.
- **DPE impact**: Scatter plot of price per m² vs. energy rating, controlling for location. Quantify the energy premium or discount.
- **Noise discount**: Compare price per m² inside and outside PEB zones, by zone severity and by airport.
- **Transport premium**: Show how price per m² varies as a function of distance to the nearest train station.
- **Market trends**: Evolution of transaction volume and median price over time, by area — identify rising or cooling markets.
- **Opportunity finder**: Rank communes by value score — combining below-average price, above-average income, good energy stock, and low noise exposure.

---

## Key Challenges

- **Volume**: Handling tens of millions of DVF rows efficiently requires thinking about indexing, partitioning, and incremental loads from day one.
- **Geocoding at scale**: Joining millions of DVF addresses to BAN entries is a non-trivial string matching problem.
- **Spatial operations**: Checking point-in-polygon membership for PEB zones and computing station proximity requires a spatial-capable database or library.
- **Format diversity**: Students must ingest CSV, Excel, JSON, REST APIs, GeoJSON, and Shapefiles — each with different parsing strategies.
- **Data quality**: DVF contains dirty data. Filtering rules must be explicit, documented, and defensible.
- **Slowly changing data**: Prices, income levels, and energy ratings all change year over year. The data model must account for time.

---

## Further Reading

For an example of a complete end-to-end project built with a modern data stack (pipeline orchestration, SQL-based transformation, and dashboarding), see this article on analyzing the Pokemon TCG Pocket metagame using a similar three-stage architecture:

[Using a modern data stack to analyze the Pokemon TCG Pocket metagame](https://medium.com/@louislourson/using-a-modern-data-stack-to-analyze-the-pokemon-tcg-pocket-metagame-6ed69c01c9d5)
