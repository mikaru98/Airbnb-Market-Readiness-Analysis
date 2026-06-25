# Airbnb Market Readiness Analysis

## Repository Outline

`This section briefly explains the content of each file pushed to the repository.`

Example:

```text
1. description.md - Main project documentation
2. airflow_ES.yaml - Docker Compose configuration for running PostgreSQL, Airflow, Elasticsearch, and Kibana
3. dags/DAG.py - Airflow ETL pipeline from PostgreSQL to Elasticsearch
4. dags/database_query.txt - PostgreSQL table creation and raw data insertion script
5. dags/data_raw.csv - Original Airbnb dataset
6. dags/extracted.csv - Temporary extracted data from PostgreSQL
7. dags/data_clean.csv - Cleaned dataset for Elasticsearch and Kibana
8. dags/GX.ipynb - Great Expectations validation notebook
9. dags/gx/ - Great Expectations suite, checkpoint, validation results, and data docs
10. images/ - Kibana dashboard screenshots and insights
```

## Problem Background

This project analyzes Airbnb listing data from several major cities, including Amsterdam, Bangkok, Barcelona, London, Paris, Rome, and Sydney.

From a business perspective, this project is designed to help a short-term rental expansion team understand market readiness across these seven major cities. The analysis focuses on the number of listings, room types, availability levels, review activity, license ownership, and host segmentation.

Through this analysis, the business team can compare which cities show high demand, which cities are more competitive, and which cities may carry higher operational or regulatory risks.

## Project Output

The main output of this project is an automated ETL pipeline and a Kibana dashboard.

The pipeline is built using Apache Airflow to extract data from PostgreSQL, clean the data using Python, save the cleaned results into a CSV file, and send the cleaned data to Elasticsearch. After that, the data is visualized using Kibana in the form of a dashboard.

The Kibana dashboard contains:

* 1 markdown introduction and objective
* 6 main visualizations
* Business insights for each visualization
* Conclusions and follow-up recommendations based on the exploration results

## Data

The dataset used in this project is Airbnb listing data from seven major cities, sourced from Kaggle.

The raw data contains:

* Number of rows: `292,802`
* Initial number of columns: `20`
* Number of columns after cleaning: `24`
* Number of duplicates after cleaning: `0`
* Missing values after cleaning: `0`

Additional columns created after the cleaning process to support visualization and exploration:

* `has_price`
* `has_license`
* `host_segment`
* `listing_key`

Listing distribution by city:

| City      | Number of Listings |
| --------- | -----------------: |
| London    |             96,871 |
| Paris     |             81,853 |
| Rome      |             37,652 |
| Bangkok   |             28,806 |
| Barcelona |             19,410 |
| Sydney    |             17,730 |
| Amsterdam |             10,480 |

Important note: price data for Paris and Sydney is not available, so pricing-related recommendations can only be applied to cities that have price data.

## Method

1. Raw data was inserted into PostgreSQL in the `table_m3` table.
2. Airflow runs a DAG with three main tasks:

   * `fetch_from_postgresql`
   * `data_cleaning`
   * `post_to_elasticsearch`
3. Data is extracted from PostgreSQL and saved as a temporary file.
4. Data is cleaned using Python and Pandas.
5. The cleaning process includes:

   * Removing duplicate data
   * Normalizing column names into lowercase and snake_case format
   * Filling missing values
   * Converting date formats
   * Creating additional columns for analysis
6. The cleaned data is validated using Great Expectations.
7. The cleaned data is sent to Elasticsearch.
8. The data is visualized using Kibana.

## Stacks

* Python
* Pandas
* Apache Airflow
* PostgreSQL
* SQLAlchemy
* Great Expectations
* Elasticsearch
* Kibana
* Docker Compose

## Reference

* Kaggle Dataset URL: [Airbnb Listings NYC, London, Paris, Tokyo, and More](https://www.kaggle.com/datasets/darkmatternet/airbnb-listings-nyc-london-paris-tokyo-and-more)
