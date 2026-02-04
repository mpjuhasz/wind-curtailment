# wind-curtailment

Analysis of the UK's wind generation and curtailment. For the write-up, and visuals, check out: https://mpjuhasz.com/posts/wind-curtailment/

## data

Various different datasources are merged, corrected and extended to get to a comprehensible dataset on wind energy sites in the UK, their BM-units and generation data. For raw, interim and processed datasets, see `/data`, and for details on the sources and processing of the datasets refer to the `/data/catalogue.yaml` and the README.

The following licences cover the raw datasets:

**Elexon BMRS Data**
  - Contains BMRS data © Elexon Limited copyright and database right 2025
  - Licence: https://www.elexon.co.uk/bsc/data/balancing-mechanism-reporting-agent/copyright-licence-bmrs-data/

**UK Renewable Energy Planning Database (REPD)**
- Contains public sector information licensed under the Open Government Licence v3.0
- Licence: https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/

**Wikidata**
- Data from Wikidata (CC0 Public Domain, no attribution required)
- https://www.wikidata.org/


**hackcollective/wind-curtailment**
- Authors: Peter Dudfield, Archy de Berker, Ben Lumley
- DOI: https://doi.org/10.5281/zenodo.13936552
- Licensed under Creative Commons Attribution 4.0 International (CC BY 4.0)
- License: https://creativecommons.org/licenses/by/4.0/

## processing

To obtain the generation data in a processed format, run:
```
just query <output-folder>
```
to configure this run, see `src/elexon/config.yaml`.

A good chunk of the data processing was done manually, and using notebooks - see these Marimo notebooks in `/notebooks`
