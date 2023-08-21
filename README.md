# dbt-synthea

This project is a data modeling exercise using synthetic healthcare patient data with dbt, or data build tool. It uses the [dbt-duckdb](https://github.com/jwills/dbt-duckdb) adapter for dbt to create a local database file that can be queried using SQL to run the data model and perform the transformations.

## Data

Download here: https://synthetichealth.github.io/synthea/

## dbt models

A SQL model is a select statement. Models are defined in .sql files (typically in your models directory):

- Each .sql file contains one model / select statement
- The model name is inherited from the filename
- Models can be nested in subdirectories within the models directory

## dbt functions

https://docs.getdbt.com/reference/dbt-jinja-functions/ref

The most important function in dbt is **ref()**; it's impossible to build even moderately complex models without it. ref() is how you reference one model within another. This is a very common behavior, as typically models are built to be "stacked" on top of one another.

ref() is, under the hood, actually doing two important things. First, it is interpolating the schema into your model file to allow you to change your deployment schema via configuration. Second, it is using these references between models to automatically build the dependency graph. This will enable dbt to deploy models in the correct order when using dbt run.

## Data transformation best practices

dbt has some recommended best practices for transforming data. The data model is divided into different layers, each of which has certain operations applied to it. The layers and their associated operations are:

- base
  - Optional
  - Use base_ as a prefix
  - This is typically a one-to-one mapping to raw data sources
  - Only include columns that will be used later in the pipeline
  - Rename columns to be more user-friendly and consistent (e.g., snake_case)
  - Cast fields to appropriate data types (e.g., date, timestamp, int, float)
  - Remove duplicate rows
- staging
  - use stg_ as a prefix
  - Rename columns to be more user-friendly and consistent (e.g., snake_case)
  - Cast fields to appropriate data types (e.g., date, timestamp, int, float)
  - Remove duplicate rows
  - Typically no joins are performed in staging, but there can be exceptions to this rule
  - These build on the base models to provide a clean, consistent interface to the data. They should:
    - Use consistent aliasing to provide a unified view across all sources
    - Implement business logic specific to the data source
    - Be tested to ensure data quality adn adherence to business rules
- marts
  - use "dim_" or "fct_" as a prefix since these are generally dimension and fact tables
  - Transformations applied in the marts layer:
    - Joins of multiple staging models
    - CASE WHEN logic
    - Window functions and aggregations
  - Provide summarized, aggregated, and denormalized views of the data tailored to the needs of the business
- intermediate
  - Optional
  - If used, lives within the marts layer
  - use int_ as a prefix
  - This layer performs transformations that are too complex to be performed in the marts layer, such as complex joins
- report
  - Optional
  - Use "report_" or "rpt_" as a prefix
  - This layer represents some kind of report to prepare for an audience that can't write their own SQL queries to join the fact and dimension tables you cretaed. If they do know SQL, the marts layer should have everything they need to create their own reports instead of you creating these reports ahead of time
  - This layer typically consists of **wide tables** that have all the columns at the correct grain for analysis

Quote from dbt's modular data modeling techniques guide:
> In our data models in dbt, we’re aiming to bring data together and standardize much of the prep work that comes with making an analysis. We are not looking to pre-build in the data warehouse every analysis or complex aggregation that may come up in the future.

See this [guide](https://www.getdbt.com/analytics-engineering/modular-data-modeling-technique/) for more information.

## Query readability

Other than the model itself and its associated naming conventions for folders, always strive to keep individual model SQL files to roughly 100 lines of code for high readability. Generally data models shorter than 100 lines have avoided doing overly complex joining, either by limiting the raw number of joins, or by joining in simple ways (repeatedly on the same key).

If some of your queries contain a lot of boilerplate such as UNION ALL statements, consider moving that code into a [jinja macro](https://docs.getdbt.com/docs/build/jinja-macros) to keep the model SQL files short and readable. Macros are ideal for thing that are either impossible or tedious to do in SQL code.

## Testing

Once the data model has been built, it's time to add tests to ensure the data model is working as expected. Tests are written in SQL and are run against the data warehouse. They are run using the dbt test command. See the [testing](https://www.getdbt.com/analytics-engineering/transformation/data-testing/) section of the dbt docs for more information.

## Instructions

Once your environment has been defined and you have used mamba (or conda) to create it and install the necessary packages, you can clone this repo and run dbt commands from the command line.

```bash
git clone https://github.com/ZackLarsen/dbt-synthea.git
```

Change into the dbt-synthea directory from the command line:
```bash
cd dbt-synthea
```

To create a new dbt project, dbt instructs us to run this command in the terminal, but it seems to fail, so I skipped this step and just created a profiles.yml file manually (see the next step for details on the contents).

```bash
dbt init dbt-synthea
```

Instead of running **dbt init**, I simply created my profiles.yml file using guidance from Josh Wills in his dbt-duckdb GitHub repository and the corresponding page on the dbt docs site.
- https://github.com/jwills/dbt-duckdb
- https://docs.getdbt.com/docs/core/connect-data-platform/duckdb-setup

For example, to use DuckDB, your profiles.yml file should look like this:

```yaml
synthea:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: '/Users/zacklarsen/Documents/Documents - Zack’s Mac mini/Projects/dbt-synthea/synthea.duckdb'
      extensions:
        - httpfs
        - parquet
```

Ensure your profile is setup correctly from the command line:

```bash
dbt debug
```

Load the CSVs with the demo data set. This materializes the CSVs as tables in your target schema. Note that a typical dbt project **does not require this step** since dbt assumes your raw data is already in your warehouse.

```bash
dbt seed
```

Run dbt run in your terminal to compile and run your dbt project. This will create a compiled SQL file for your example_model and execute it against your DuckDB database.

```bash
dbt run
```

> **NOTE:** If this steps fails, it might mean that you need to make small changes to the SQL in the models folder to adjust for the flavor of SQL of your target database. Definitely consider this if you are using a community-contributed adapter.

Test the output of the models:

```bash
dbt test
```

Generate documentation for the project:

```bash
dbt docs generate
```

View the documentation for the project:

```bash
dbt docs serve
```

Navigate to your browser and go to http://localhost:8080 to view the documentation. Click on the lower right corner to view the data lineage graph for your project, which shows a DAG of the source tables and transformations.

## Query newly created tables from seeds using DuckDB

At the terminal, issue these two commands. The first initializes a DuckDB session and the second opens our saved synthea duckdb database file created when we ran the dbt models.

```bash
duckdb

.open synthea.duckdb
```

In the duckdb session that opens, you can query the newly created tables by issuing SQL commands.:

```sql
SELECT *
FROM patients
LIMIT 10;
```

## Resources

- https://github.com/synthetichealth/synthea
- https://github.com/dbt-labs/jaffle_shop
- https://docs.getdbt.com/docs/core/connect-data-platform/duckdb-setup
- https://docs.getdbt.com/docs/core/connect-data-platform/duckdb-setup
- https://www.getdbt.com/analytics-engineering/modular-data-modeling-technique/
- https://github.com/dbt-labs/dbt-learn-demo

## Lineage Graph

![Lineage Graph](/etc/lineage.png)

### Seeds
This repo contains [seeds](https://docs.getdbt.com/docs/building-a-dbt-project/seeds) that include synthetically-generated but realistic healthcare data from the Synthea project.

## Setup

Define your python environment in an environment.yaml file. It must have duckdb and dbt-duckdb installed to work properly. It can look something like this:

```yaml
name: dbt_duckdb
channels:
  - defaults
  - conda-forge
dependencies:
  - python=3.10
  - pip
  - pip:
    - pyarrow
    - polars
    - duckdb
    - dbt-duckdb
    - ipykernel
    - jupyter
    - deltalake
    - seaborn
    - matplotlib
```

Install the DuckDB adapter for dbt and other necessary dependencies by running the following mamba command in your terminal:

```bash
mamba env create -f environment.yaml
```

To update your environment, run the following mamba command in your terminal:

```bash
mamba env update --file environment.yaml --prune
```

To activate the environment, run the following mamba command in your terminal:

```bash
mamba activate dbt_duckdb
```
