# CMS Open Payments Analytics Platform on AWS

## Project Overview

This project builds an end-to-end batch data engineering pipeline on AWS around the **CMS Open Payments General Payments** dataset. The goal is to ingest raw public payment data, store it in a structured cloud data lake, transform it into analytics-ready datasets, and expose it through a dashboard for business and analytical use.

The project is designed as a production-style capstone rather than a simple academic exercise. It focuses on reproducibility, orchestration, cloud architecture, data quality, and clear analytics output.

---

## Problem Description

The CMS Open Payments dataset contains financial transactions and transfers of value made by applicable manufacturers and group purchasing organizations to healthcare providers and teaching hospitals in the United States.

While the raw dataset is publicly available, it is not immediately useful for ongoing analysis in its source form because:

- the data is large and requires structured ingestion
- raw files are not automatically organized for analytics workflows
- repeated manual analysis is inefficient and error-prone
- analysts and stakeholders need curated views, not raw source extracts
- there is no built-in operational pipeline for scheduled refresh, validation, and dashboard serving

The problem this project solves is:

> How do we build a reliable AWS-based data platform that ingests CMS Open Payments data, stores it in a cloud data lake, transforms it into queryable analytics-ready models, and serves insights through a reproducible dashboard workflow?

---

## Project Goal

The goal of this project is to build a complete AWS-based batch analytics pipeline that:

1. extracts CMS Open Payments General Payments data from the source
2. validates and stores raw outputs in a structured data lake on Amazon S3
3. exposes the raw data through a warehouse/query layer
4. transforms the raw dataset into analytics-ready tables using a modeling layer
5. orchestrates the full workflow with Apache Airflow
6. serves a dashboard with clear analytical views over payment activity
7. demonstrates real-world data engineering practices suitable for portfolio and job applications

---

## Key Objectives

This project is intended to demonstrate the following:

- building a real batch ingestion pipeline with Python
- using Docker for reproducible execution
- storing raw and processed artifacts in Amazon S3
- using AWS query and metadata services for analytics access
- modeling data with dbt for analytics use cases
- orchestrating the workflow with Apache Airflow
- validating pipeline outputs with structured checks and manifests
- producing a dashboard for business-facing analysis
- documenting the project clearly enough for peer review and reproducibility

---

## Business / Analytical Questions

The analytics layer and dashboard will aim to answer questions such as:

- How do total payment amounts trend over time?
- Which companies make the highest total payments?
- What categories or natures of payment are most common?
- Which states or geographies receive the most payment activity?
- Who are the top recipients by payment amount or payment count?
- How does payment behavior change across months or years?

These questions make the project more than a raw ingestion exercise. They define the purpose of the transformation and dashboard layers.

---

## Scope of the Project

### In Scope

- CMS Open Payments **General Payments** dataset
- batch ingestion workflow
- raw data landing in S3
- metadata and audit artifacts
- warehouse/query layer for raw and transformed data
- dbt transformations for analytics-ready tables
- Airflow orchestration
- dashboard visualization
- reproducible repository and documentation

### Out of Scope for Initial Delivery

- streaming ingestion
- machine learning use cases
- full multi-dataset CMS Open Payments coverage
- near-real-time refresh
- advanced cost optimization beyond reasonable project design
- enterprise-grade security hardening beyond project scope

---

## Proposed Architecture

The project follows a lakehouse-style batch architecture:

**Source -> Python Ingestion -> S3 Data Lake -> Athena/Glue Query Layer -> dbt Transformations -> Analytics Marts -> Dashboard**

At a high level:

1. Python ingestion jobs extract CMS Open Payments data and generate run artifacts.
2. Raw files, manifests, and audit logs are stored in Amazon S3.
3. AWS Glue Data Catalog and Amazon Athena provide query access over raw and transformed datasets.
4. dbt builds staging, fact, dimension, and mart models for analytics.
5. Airflow orchestrates ingestion, validation, S3 upload, metadata refresh, and transformation steps.
6. A dashboard exposes the final insights for exploration.

---

## Tools and Technologies

The following tools are planned for the final project implementation.

### Core Data Engineering

- **Python** — ingestion, validation, file processing, logging, and AWS integration
- **Docker** — reproducible execution environment
- **Apache Airflow** — workflow orchestration and scheduling
- **dbt Core** — transformation and analytics modeling

### AWS Services

- **Amazon EC2** — host Airflow and pipeline execution environment
- **Amazon S3** — cloud data lake for raw and metadata artifacts
- **AWS Glue Data Catalog** — metadata/catalog layer for queryable datasets
- **Amazon Athena** — SQL query engine over S3-backed data
- **IAM roles/policies** — controlled access between services

### Infrastructure / DevOps

- **Terraform** — infrastructure as code for AWS resources
- **Git / GitHub** — version control and project delivery
- **Structured logging and manifest files** — operational visibility and run tracking

### Visualization

- **Streamlit** or another lightweight dashboard tool — final interactive analytics output

---

## Why This Stack

This stack was selected because it supports both fast project delivery and strong data engineering signaling:

- AWS shows cloud engineering capability
- S3 + Athena + Glue provides a practical analytics architecture for a lake-based system
- Airflow demonstrates orchestration maturity
- dbt demonstrates transformation discipline and analytics engineering structure
- Docker improves reproducibility
- Terraform adds infrastructure credibility

The architecture is intentionally designed to balance execution speed, portfolio strength, and realistic engineering practices.

---

## Expected Deliverables

The final project is expected to include:

- an ingestion pipeline for CMS Open Payments data
- raw data and metadata stored in S3
- queryable datasets in Athena
- dbt models for staging and analytics marts
- an Airflow DAG that orchestrates the pipeline
- a dashboard with at least two analytical views
- a documented repository with setup and run instructions
- architecture diagram and screenshots for submission

---

## Initial Repository Direction

The repository will eventually include sections for:

- project overview
- architecture
- setup and execution
- data model
- orchestration
- dashboard
- screenshots
- reproducibility notes
- future improvements

This current README is the initial project-definition version and will be expanded later with:

- architecture image
- DAG screenshots
- Athena screenshots
- dbt model screenshots
- dashboard screenshots
- setup commands and environment details

---

## Success Criteria

The project will be considered successful when:

- the pipeline can run end-to-end from ingestion to analytics output
- raw outputs are reproducibly stored in S3
- transformed tables are queryable and support dashboard use cases
- orchestration is visible and functional in Airflow
- the dashboard answers at least two meaningful analytical questions
- the repo is clean, documented, and ready for review
- the final result is strong enough to be used in both the Zoomcamp submission and a professional portfolio

---

## Current Status

This README is the initial framing document for the project.

Next updates will add:

- finalized architecture diagram
- repository structure
- pipeline flow details
- setup instructions
- transformation model details
- dashboard links and screenshots

