### Module Development Notes
Here are ad-hoc instructions on how data can be extracted from [ENMAX's System Resources](https://www.enmax.com/system-resources)

#### Inspecting network activity at https://www.enmax.com/system-resources/load-capacity-map
1.  we find the URL that points to the Esri web app viewer: https://geoarm.maps.arcgis.com/apps/webappviewer/index.html?id=2a148b189b884654b691b2909bef16df
2. we find URL that points to Esri REST API from where we can query ENMAX's publicly available data: https://services1.arcgis.com/NKgP4VcXUzEyOnmg/ArcGIS/rest/services. We refer to this as REST Services Directory (RSD).

#### Using the [Esri Developer API](https://developers.arcgis.com/rest/)
1. Most of the 'Features' available on each FeatureServer listed at the RSD can be accessed with [`query`](https://developers.arcgis.com/rest/services-reference/enterprise/query-feature-service-layer/). 
2. We use [`pyesridump`](https://github.com/openaddresses/pyesridump/tree/master) to scrape the data available at the FeatureServer. See `scrape_enmax_data.py`.

#### All downloaded data can be found in `../../data/enmax`
---
### Next Steps
Downloading the data onto our machine allows us to access it without having to use REST API.

However, a better approach may be using REST API to automate regularly getting the latest version of ENMAX data and track changes so we can build a historical dataset of publicly available ENMAX maps and data.