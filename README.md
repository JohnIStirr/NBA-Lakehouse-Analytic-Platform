# 🏀NBA Lakehouse Data Engineering Project

An end-to-end data engineering project that builds a modern lakehouse using the Medallion Architecture on Databricks. The project ingests historical NBA data from a SQLite database, transforms it through Bronze and Silver layers using PySpark and Delta Lake, and prepares business-ready datasets for analytics in the Gold layer.

---

## Project Overview

This project demonstrates common data engineering practices including:

- Data ingestion from relational databases
- Medallion Architecture (Bronze → Silver → Gold)
- Data modeling using dimensional design
- ETL development with PySpark
- Delta Lake table management
- Data quality validation
- Business-oriented data transformations

---

## Tech Stack

| Technology | Purpose |
|------------|---------|
| Databricks | Data Engineering Platform |
| Apache Spark (PySpark) | Distributed ETL Processing |
| Delta Lake | ACID Lakehouse Storage |
| SQL | Data Querying & Validation |
| Python | ETL Development |
| SQLite | Source Database |

---

## Dataset

Source: NBA historical data from Kaggle

The source SQLite database contains:

- Teams
- Players
- Games
- Game summaries
- Officials
- Line scores
- Play-by-play events
- Player statistics

---

# Architecture

```
                SQLite Database
                       │
                       ▼
              Export to Parquet Files
                       │
                       ▼
                Bronze Layer
          (Raw Delta Tables)
                       │
                       ▼
                Silver Layer
     (Cleaned & Modeled Tables)
                       │
                       ▼
                 Gold Layer
        (Business Aggregations)
```

---

# Current Data Model

### Dimension Tables

### dim_team

One record per NBA team.

Key attributes:

- Team ID
- Team Name
- Abbreviation
- City
- State
- Year Founded

---

### dim_player

One record per NBA player.

Key attributes:

- Player ID
- Player Name
- First Name
- Last Name
- Active Status

---

### Fact Table

### fact_game

**Grain**

> One row per team per game.

Key metrics:

- Points
- Rebounds
- Assists
- Steals
- Blocks
- Turnovers
- Shooting Percentages
- Plus/Minus
- Home/Away
- Win/Loss
- Season Type

---

# Current Progress

## Bronze Layer ✅

- SQLite extraction
- Parquet ingestion
- Delta table creation
- Raw data preservation

---

## Silver Layer ✅

Implemented dimensional modeling:

- dim_team
- dim_player
- fact_game

Completed transformations include:

- Data cleansing
- Schema standardization
- Team-game normalization
- Derived business columns
- Data quality validation
- Duplicate handling
- Broadcast metadata aggregation

---

## Gold Layer 🚧

Currently under development.

Planned datasets:

- Team Season Summary
- Team Rankings
- Home vs Away Performance
- Shooting Efficiency
- Season Leaders

---

# Interesting Engineering Challenges

### Team-Game Fact Modeling

The original dataset stores one game with both home and away statistics.

The pipeline transforms this into a normalized fact table with **one row per team per game**, which simplifies downstream analytics.

---

### Handling One-to-Many Joins

Game summary data contains multiple rows for nationally televised games.

Instead of creating duplicate fact records, broadcast metadata is aggregated prior to joining, preserving all broadcaster information while maintaining the correct fact table grain.

---

### Data Quality Validation

Validation checks include:

- Duplicate detection
- Primary key validation
- Null checks
- Fact table grain verification
- Row count reconciliation

---

# Project Structure

```
nba-lakehouse/
│
├── notebooks/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── docs/
│
├── images/
│
└── README.md
```

---

# Roadmap

- [x] Bronze Layer
- [x] Silver Layer
- [ ] Gold Layer
- [ ] Databricks SQL Dashboard
- [ ] Performance Optimization
- [ ] Unit Testing
- [ ] CI/CD Pipeline

---

# Future Improvements

- Incremental data loading
- Delta Live Tables
- Automated data quality testing
- Workflow orchestration
- Dashboard development
- Performance benchmarking
- Streaming data ingestion

---

# Skills Demonstrated

- Data Engineering
- PySpark
- Delta Lake
- SQL
- Data Modeling
- ETL Pipeline Development
- Medallion Architecture
- Data Quality Engineering
- Lakehouse Design
