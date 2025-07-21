# 📦 Maven Toy Store dbt Project

This project is dedicated to transforming raw data into reliable, well-structured datasets for analytics and reporting.

### Project Overview

This repository contains the dbt models and configurations used to process and analyze data stored in Snowflake. The main gold of this project was to load the data in snowflake, connect it with dbt and implement medallion architecture to extract important insights from the data. 

### Architecture and Organization

Our project follows the **Medallion Architecture** pattern, which consists of multiple layers:
- **Staging (Bronze):** Initial data cleaning and standardization.
- **Intermediate (Silver):** Data enrichment and joining across sources.
- **Marts (Gold):** Business-facing summary tables and metrics.

This layered approach ensures data integrity, minimizes code repetition, and facilitates easier maintenance and scalability.

### Data Transformation Workflow
**Staging Layer**

The staging layer is responsible for:
- Ingesting raw data from source tables.
- Handling missing values, data type conversions, and preliminary cleaning.
- Standardizing column names and formats.

**Intermediate Layer**

The intermediate layer:
- Joins and enriches data from multiple staging models.
- Applies business logic and calculations needed for downstream consumption.
- Prepares datasets for aggregation.

**Mart Layer**

The marts layer:
- Aggregates and summarizes data to answer key business questions.
- Builds tables for reporting, dashboarding, and advanced analytics (e.g., total sales by store, inventory snapshots, trend analysis).

### Snapshots

This project leverages dbt’s snapshot functionality to implement Slowly Changing Dimension (SCD) Type 2 logic:

- Tracks changes in records over time, maintaining a historical view of data.
- Adds metadata fields such as dbt_scd_id, dbt_updated_at, dbt_valid_from, and dbt_valid_to.
- Ensures both previous and current values are available for auditing and historical analysis.

Snapshots provide transparency into how and when data changes, supporting compliance and robust analytics. Whenever some value or data is changed, the dbt_valid_to column is changed from "null" to the timestamp the data got changed and a new column is added with the updated values preserving both the old value and the new one.

### Orchestration and Automation
Automated orchestration is configured via dbt Cloud (or your orchestration tool):
- Scheduled jobs execute dbt commands at predefined intervals.
- Ensures models and snapshots are kept up-to-date without manual intervention.
- Facilitates continuous integration and deployment practices.

This automation streamlines data pipeline management and guarantees the availability of the latest data.

### Learning Outcomes
Through the development and deployment of this dbt project, we have gained hands-on experience with:
- Implementing medallion architecture in real-world data pipelines.
- Cleaning and transforming diverse data types for analytics.
- Setting up automated data orchestration and job scheduling.
- Tracking historical changes with dbt snapshots and SCD Type 2 methodology.
- Maintaining a scalable and maintainable analytics codebase.

It helped me to understand how data cleaning and analytics works in real world and learnt how to setup my project using the medallion architecture - breaking it into staging(bronze), intermediate(silver) and marts(gold). Setting up orchestrations and automated jobs in dbt gave me a taste of how real data pipelines run on a schedule. And exploring snapshots enables me to understand how data changes over time. This project taught me how to organize a data transformation workflow from start to finish, and made me confident working with dbt!