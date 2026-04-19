# Minnesota Mortgage Fair Lending Dashboard (2024 HMDA)

An interactive fair lending analysis dashboard built on 2024 Home Mortgage Disclosure Act (HMDA) data for Minnesota. Surfaces approval rate disparities, denial reason patterns, and higher-priced lending rates by race and institution using a Snowflake data warehouse pipeline and a Streamlit front end.

## Live Demo

[Streamlit Community Cloud deployment](https://hmda-fair-lending-dashboard-hw7pxn5ibzqkvjhhycjnso.streamlit.app/)

## Background

The Home Mortgage Disclosure Act (HMDA) requires nearly every U.S. mortgage lender to publicly disclose loan-level data on every application they receive. This project uses 2024 Minnesota HMDA data (177,638 applications across 99 fields) to compute fair lending screening metrics at the institution level.

The CFPB's existing HMDA Data Browser surfaces raw counts but does not compute derived fair lending metrics like the Adverse Impact Ratio (AIR) or higher-priced lending rates by race. This dashboard fills that gap.

**Key metrics surfaced:**
- Approval rate by race/ethnicity, statewide and per institution
- Adverse Impact Ratio (AIR) vs. White applicants per institution
- Primary denial reason distribution by race
- Higher-priced loan rate (rate spread ≥ 1.5) by institution
- Applicant income tier (relative to FFIEC Area Median Income) by metro area

**Note:** All disparity metrics are unadjusted and do not control for credit score, LTV, or DTI. They are intended as screening tools to identify institutions warranting further review, consistent with how CFPB and DOJ use unadjusted AIR as a first-pass filter in fair lending examinations.

## Data Sources

| File | Source | Description |
|------|--------|-------------|
| Minnesota LAR 2024 | [FFIEC HMDA Platform](https://ffiec.cfpb.gov/data-publication/snapshot-national-loan-level-dataset/2024) | Loan/Application Records filtered to MN |
| Transmittal Sheet 2024 | Same | Maps LEI to institution name |
| MSA/MD Description 2024 | Same | Maps metro area codes to names |

## Architecture

```
HMDA raw CSVs
     |
     v
Snowflake RAW schema
  HMDA_LAR_2024, HMDA_TS_2024, HMDA_MSABD_2024
     |
     v
Snowflake CUR schema -- 8 curation transformations
  - Action taken labels + approval flag
  - Denial reason labels
  - DTI re-binning into standard 10-point buckets
  - Higher-priced loan flag (rate spread >= 1.5)
  - Race/ethnicity cleaning (Free Form Text -> Not Available)
  - Age flag (any applicant above 62)
  - HOEPA status labels
  - Stored procedure: AMI income bucketing + tract minority category
     |
     v
Snowflake AGG schema -- 4 aggregation tables + materialized view
  AGG_APPROVAL_BY_RACE_INSTITUTION
  AGG_LOAN_METRICS_BY_INSTITUTION
  AGG_DENIAL_REASONS_BY_RACE
  AGG_INCOME_AMI_BY_MSA
  AGG_MV_APPROVAL_RATES_RELIABLE  (materialized view)
  FN_APPROVAL_RATES_BY_INSTITUTION (table function / API layer)
     |
     v
Exported to DuckDB (hmda_2024.duckdb)
     |
     v
Streamlit dashboard (dashboard.py)
```

## Repository Structure

```
hmda-fair-lending-dashboard/
├── Final_Project_Draft.sql    Full Snowflake pipeline (all 6 worksheet sections)
├── dashboard.py               Streamlit app
├── create_db.py               Builds hmda_2024.duckdb from exported CSVs
├── requirements.txt           Python dependencies
├── data/
│   └── hmda_2024.duckdb       Pre-built DuckDB database
└── README.md
```

## Running Locally

**1. Clone the repo**

    git clone https://github.com/yourusername/hmda-fair-lending-dashboard.git
    cd hmda-fair-lending-dashboard

**2. Install dependencies**

    pip install -r requirements.txt

**3. Run the dashboard**

    streamlit run dashboard.py

The `data/hmda_2024.duckdb` file is included in the repo — no Snowflake account needed to run the dashboard locally.

## Rebuilding the Database from Scratch

If you want to rebuild from raw HMDA data:

1. Download the 2024 Minnesota LAR, Transmittal Sheet, and MSA/MD Description files from the [FFIEC HMDA Platform](https://ffiec.cfpb.gov/data-publication/snapshot-national-loan-level-dataset/2024)
2. Run the full Snowflake pipeline in `Final_Project_Draft.sql`
3. Export the four AGG tables as CSVs into the `data/` folder
4. Run `python3 create_db.py` to rebuild `hmda_2024.duckdb`

## Future Work

- Expand to national data for 2018-2024 to enable time-series trend analysis
- Add Bayesian Improved Surname Geocoding (BISG) for proxy race estimation on records where race was not self-reported
- Integrate [SolasAI disparity library](https://github.com/SolasAI/solas-ai-disparity) for statistically rigorous AIR with Fisher's exact test significance flags

## Acknowledgments

- Data: FFIEC / Consumer Financial Protection Bureau
- Disparity methodology informed by [SolasAI](https://github.com/SolasAI/solas-ai-disparity) (CC BY-NC-ND 4.0)
- Built as a final project for SEIS 732 Data Warehousing, University of St. Thomas (Spring 2026)
