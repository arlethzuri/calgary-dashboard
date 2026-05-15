# demo4 — Calgary geospatial layer explorer

Interactive Bokeh map for exploring a **curated subset** of Calgary-area open data. Processed GeoParquet files are bundled under `data/`.

**Map window:** ~12 km square centered on downtown Calgary.

## Quick start

From the **repository root**:

```bash
conda env create -f calgary_dev.yml   # once
conda activate calgary_dev
pip install -e .
bokeh serve demos/demo4/app.py --show
```

The browser should open automatically. If not, go to the URL printed in the terminal (usually http://localhost:5006).

### Requirements

- Python 3.9+ with: `bokeh`, `geopandas`, `pandas`, `shapely`, `numpy`
- Editable install of this repo (`pip install -e .`) so `calgary_dashboard` imports resolve

### Data location

The app loads layers from **`demos/demo4/data/`** first. If that folder is empty, it falls back to `data/calgary/processed_data` at the repo root (or `DATA_ROOT` from `.env`).

## Using the map

| Sidebar section | What it shows |
|-----------------|---------------|
| **Flood** | Annual-chance flood polygons + regulatory bylaw hazard |
| **Solar** | Solar production sites |
| **Load capacity (ENMAX)** | Feeder remaining load capacity (KVA bins) |
| **Hosting capacity (ENMAX)** | Feeder remaining hosting capacity (kW bins) |
| **Open Calgary** | Hydrology, communities, parks/cemeteries, natural areas, land use, growth forecast, schools |
| **Census choropleth** | StatCan 2021 population & dwelling counts by dissemination area |

- Check layers to overlay them on the basemap.
- **Hover** over Open Calgary features to see attribute details (internal `:` fields are hidden).
- Flood checkboxes use readable names (e.g. “1 in 100 annual flood probability”).
- July 2024 ENMAX load layers are selected by default.

---

## Datasets included (26 layers)

### Open Calgary — Flood (13)

| Map label | Source dataset |
|-----------|----------------|
| 1 in 2 annual flood probability | 1-in-2 flood map (50% annual chance) |
| 1 in 10 annual flood probability | 1-in-10 flood map |
| 1 in 20 annual flood probability | 1-in-20 flood map |
| 1 in 35 annual flood probability | 1-in-35 flood map |
| 1 in 50 annual flood probability | 1-in-50 flood map |
| 1 in 75 annual flood probability | 1-in-75 flood map |
| 1 in 100 annual flood probability | 1-in-100 flood map |
| 1 in 200 annual flood probability | 1-in-200 flood map |
| 1 in 350 annual flood probability | 1-in-350 flood map |
| 1 in 500 annual flood probability | 1-in-500 flood map |
| 1 in 750 annual flood probability | 1-in-750 flood map |
| 1 in 1000 annual flood probability | 1-in-1000 flood map |
| Regulatory flood hazard (bylaw) | Regulatory flood map — flood hazard |

### Open Calgary — Other (8)

| Theme | Dataset |
|-------|---------|
| Solar | Solar production sites map |
| Hydrology | Hydrology |
| Communities | Community boundaries |
| Parks | Parks & cemeteries |
| Environment | Natural areas |
| Planning | Land use districts |
| Growth | Suburban residential growth (SRG) forecast |
| Amenities | Schools in communities |

### ENMAX (4)

| Layer | Description |
|-------|-------------|
| Single-phase load capacity | Estimated remaining load capacity — July 2024 |
| Two/three-phase load capacity | Estimated remaining load capacity — July 2024 |
| Two/three-phase load capacity | Estimated remaining load capacity — December 2025 |
| Three-phase hosting capacity | Estimated remaining hosting capacity — February 2025 |

### Statistics Canada (1)

| Layer | Description |
|-------|-------------|
| Population & dwellings | 2021 population and dwelling counts (dissemination areas) |

**Total: 26 feature layers** (~300 MB under `data/`).

---

## Refreshing bundled data

From the repo root, after processed data is updated:

```bash
PYTHONPATH=src python3 demos/demo4/copy_demo_data.py
```

This copies the allowlisted layers from `data/calgary/processed_data` into `demos/demo4/data/`.

---

## Related demos

- **demo3** — same UI but expects data under `demos/data/` or legacy paths.
