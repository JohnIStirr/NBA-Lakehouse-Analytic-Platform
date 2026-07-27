# System Architecture

## Overview

This project follows the Medallion Architecture.

Source
    ↓
SQLite Database
    ↓
Parquet Landing
    ↓
Bronze
    ↓
Silver
    ↓
Gold
    ↓
Dashboard

## Bronze Layer

Purpose:
- Preserve raw source data
- Minimal transformations
- Delta tables

## Silver Layer

Purpose:
- Clean data
- Standardize schemas
- Build dimensional model

## Gold Layer

Purpose:
- Business-ready aggregations
- Dashboard reporting
