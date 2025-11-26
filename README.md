# Big Data Analytics: US Used Cars Analysis

![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.0-orange) ![Apache Hive](https://img.shields.io/badge/Apache%20Hive-SQL-yellow) ![Docker](https://img.shields.io/badge/Docker-Linux_Container-2496ed)

## 📌 Project Overview

This repository contains a large-scale data analysis project focusing on the **US Used Cars Dataset**. [cite_start]The primary objective was to process, clean, and analyze approximately **3 million records** of vehicle listings to extract market insights[cite: 316].

[cite_start]The project leverages **Apache Spark (PySpark)** and **Apache Hive** concepts to handle the volume of data efficiently, employing both structured query paradigms (SQL) and low-level functional programming (MapReduce)[cite: 355, 570].

## 🛠️ Infrastructure

[cite_start]To ensure a stable and reproducible execution environment, this project was deployed using a **Linux-based Docker container**[cite: 314]. [cite_start]The Spark Session was initialized with `.master("local[*]")` to maximize resource utilization by using all available CPU cores for parallel processing [cite: 321-322].

## 📊 Dataset & Preprocessing

[cite_start]**Source:** Kaggle US Used Cars Dataset (~3M records, 66 attributes)[cite: 316].

A rigorous **ETL (Extract, Transform, Load)** pipeline was implemented to prepare the raw data for analysis 

[Image of ETL data pipeline steps]
. The process involved:
* [cite_start]**Ingestion & Dimensionality Reduction:** Parsing complex CSVs (multi-line descriptions) and reducing the dataset from 66 to 7 key columns (e.g., `make`, `model`, `price`) to optimize memory[cite: 329, 353].
* [cite_start]**Data Cleaning:** Standardizing numerical types (casting `price` and `year`), systematically handling null values, and filtering out semantic noise (e.g., invalid model names like "volume control")[cite: 334, 342, 348].

## 📝 Implemented Assignments

The project addresses two specific analytical questions using different Big Data paradigms:

### 1. Market & Price Aggregation (Spark SQL)
We treated the data as a structured relational entity to extract key market indicators. [cite_start]Using Spark SQL, we computed the **total volume**, **price ranges** (minimum, maximum, and average), and **active manufacturing years** grouped by car Make and Model[cite: 357]. [cite_start]The results were stored in Parquet format for efficiency[cite: 358].

### 2. Listing Behavior & Text Analysis (MapReduce)
[cite_start]We utilized RDD transformations (`map`, `reduceByKey`) to analyze unstructured aspects of the listings[cite: 571]. [cite_start]This task calculated the **average number of days** a vehicle remains on the market and performed text mining to extract the **Top 3 most frequent keywords** from vehicle descriptions, identifying common selling points per group[cite: 569].

## ⏱️ Performance Benchmarking

[cite_start]A scalability analysis was conducted to measure processing overhead relative to data volume [cite: 454-455]:

| Dataset Size | Processing Time |
| :--- | :--- |
| **10%** | [cite_start]~58 seconds [cite: 590] |
| **30%** | [cite_start]~106 seconds [cite: 590] |
| **50%** | [cite_start]~248 seconds [cite: 590] |
| **100%** | [cite_start]~14.9 minutes (886s) [cite: 575] |

---

*Project developed at Università degli Studi Roma Tre*
