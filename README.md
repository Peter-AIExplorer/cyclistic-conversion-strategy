# Cyclistic Conversion Strategy: Targeting the Weekend Value Gap
Cyclistic Ride Share: Google Data Analytics Professional Certificate Capstone Project

This project details the end-to-end data analysis process for Cyclistic, a fictional bike-share company, focusing on converting high-value casual riders into annual members. The analysis identifies a specific, actionable financial pain point within the casual segment to optimize marketing spend for a high Return on Investment (ROI) campaign.

## Key Project Outcomes

The analysis of 12 months of trip data isolated three core findings, moving beyond generalized averages to reveal precise conversion triggers:

| Finding | Metric Focus | Key Insight |
| :--- | :--- | :--- |
| **WHO** | P75 Trip Duration | Casual Classic Riders are the highest-value conversion target. |
| **WHY** | Weekend P75 vs. Weekday P75 | The segment's Weekend P75 duration is **33.0 minutes** (5 minutes longer than weekdays), confirming they are consistently incurring overage fees during leisure trips. |
| **WHEN** | Trip Frequency Heatmap | The optimal campaign deployment window is **Saturday, 1 PM – 4 PM**, the segment's moment of highest engagement. |

## 🛠 Project Structure & Tools

The entire project workflow is detailed and traceable within this repository:

-   **Data Source:** The source is clearly cited as being provided by [**Lyft Bikes and Scooters, LLC (“Bikeshare”) and the City of Chicago’s (“City”) Divvy bicycle sharing service**](https://divvybikes.com/data-license-agreement)**.**  The data was sourced from multiple monthly ZIP files stored on [**Amazon Cloud**](https://divvy-tripdata.s3.amazonaws.com/index.html) covering `Nov 2024 to Oct 2025`.
    
-   **Database & Cleaning:** Google BigQuery (SQL) for data cleaning, aggregation, and feature engineering.
    
-   **Visualization & Presentation:** Tableau Public for the interactive dashboard and strategic visualizations.
    

### Repository Content


| Folder / File | Description |
| :--- | :--- |
| `sql_queries/` | Contains the BigQuery SQL scripts used for data cleaning (`cyclistics_preprocessing.sql`) and final analysis/aggregation (`cyclistics_analysis.sql`). This is the raw data proof. |
| `tableau_dashboard/` | Contains the live interactive dashboard (`index.html`) embedded using Tableau Public. |
| `README.md` (This File) | Executive summary and project documentation. |

## 🔗 Live Interactive Dashboard

Explore the findings and supporting visuals directly.

[**VIEW THE LIVE TABLEAU DASHBOARD**](https://www.google.com/search?q=https://your-github-username.github.io/your-repo-name/ "null")

_**Note:**_ _For the full narrative, detailed methodology, and executive summary, please see the associated [Medium Post Link](https://medium.com/@obepeter92/cyclistic-ride-share-google-data-analytics-professional-certificate-capstone-project-de2bec8e8ccd)._