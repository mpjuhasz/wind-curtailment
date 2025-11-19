# wind-curtailment

Analysis of the UK's wind generation and curtailment. For the write-up, and visuals, check out: mpjuhasz.com/posts/wind-curtailment

## data

Various different datasources are merged, corrected and extended to get to a comprehensible dataset on wind energy sites in the UK, their BM-units and generation data. For raw, interim and processed datasets, see `/data`, and for details on the sources and processing of the datasets refer to the `/data/catalogue.yaml` and the README. 

The following licences cover the raw datasets:
- BMRS Data made available by Elexon Limited at https://bmrs.elexon.co.uk/
- gov.uk data 


## processing

To obtain the generation data in a processed format, run:
```
just query <output-folder>
```
to configure this run, see `src/elexon/config.yaml`.

A good chunk of the data processing was done manually, and using notebooks - see these Marimo notebooks in `/notebooks`