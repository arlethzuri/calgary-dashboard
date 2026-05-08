### General Overview
Here are ad-hoc instructions on how data can be extracted from [Open Calgary](https://data.calgary.ca/)

#### Using Socrata API (now Tyler Technologies Data and Insights Division)
1. We can use [Discovery API](https://dev.socrata.com/docs/other/discovery#?route=get-/catalog/v1-search_context-domain-domains-domain-) to access API endpoints of most Open Calgary datasets: https://api.us.socrata.com/api/catalog/v1?domains=data.calgary.ca&search_context=data.calgary.ca&only=datasets
    
    a. One can find categories of datasets here: https://api.us.socrata.com/api/catalog/v1/domain_categories?domains=data.calgary.ca

2. We [create an account at Open Calgary with Tyler Data & Insights](https://data.calgary.ca/login) so we can create an App Token ensuring we do not hit data download limits.

3. Using `manual_get_data.py` we download datasets we identified by exploring the Open Calgary open data portal. URLs to the datasets are in `manually_selected_datasets.txt`.
---
### Next steps

For now, we **manually** select a few datasets to download, however this takes a lot of effort and exertise to read through the descriptions, metadata, etc. Consider adding an AI that reads metadata for each dataset and decides whether to download the dataset and justify why.

Consider continuing this in `automate_get_data.py`