# End-to-End Data Warehouse & Analytics Project

![SQL](https://img.shields.io/badge/Language-SQL-blue)
![ETL](https://img.shields.io/badge/ETL-Extract%20Transform%20Load-green)
![DataWarehouse](https://img.shields.io/badge/Architecture-Medallion%20(Bronze%2FSilver%2FGold)-orange)

## 📌 Project Overview
This project demonstrates a complete Data Warehousing solution, building a modern **ETL pipeline** using **SQL Server**. The goal was to transform raw sales data from disparate sources (CRM CSVs and ERP CSVs) into a structured Star Schema optimized for analytics and reporting.

The project follows the **Medallion Architecture** (Bronze, Silver, Gold layers) to ensure data quality and scalability.

## 📂 Repository Structure
- **`datasets/`**: Raw CSV files acting as the source systems.
- **`scripts/`**: SQL scripts for every stage of the pipeline.
- **`docs/`**: Architecture diagrams and schemas.

## 🏗️ Architecture
The project is designed using a layered architecture:
1.  **Bronze Layer (Raw):** Direct ingestion of CSV files (bulk load). No transformation.
2.  **Silver Layer (Cleansed):** Data cleaning, standardization (naming conventions), and handling nulls/duplicates.
3.  **Gold Layer (Curated):** Business-logic application, Star Schema modeling (Facts & Dimensions), and View generation for BI reporting.

## 🛠️ Technical Skills Demonstrated
- **Advanced SQL:** CTEs, Window Functions (`RANK`, `LAG`, `OVER`), Aggregations.
- **Data Modeling:** Star Schema design (Fact vs. Dimension tables).
- **ETL Processes:** Bulk inserts, stored procedures, data validation, and transformation.
- **Data Quality:** Handling nulls, deduplication logic, and string manipulation.
- **Analytics:** Trend analysis, cumulative growth, and moving averages.

## 🚀 How to Run the Project
1.  **Prerequisites:** SQL Server and SQL Server Management Studio (SSMS).
2.  **Clone the Repo:**
    ```bash
    git clone [https://github.com/your-username/sql-data-warehouse-project.git](https://github.com/your-username/sql-data-warehouse-project.git)
    ```
3.  **Setup Database:** Run `scripts/1_init_database.sql` to create the database and schemas.
4.  **Load Data:**
    - Update the CSV file paths in `scripts/2_bronze_layer.sql` to match your local directory.
    - Run the script to ingest raw data.
5.  **Process Silver Layer:** Run `scripts/3_silver_layer.sql` to clean and standardize data.
6.  **Build Gold Layer:** Run `scripts/4_gold_layer.sql` to generate dimensions and facts.
7.  **Analyze:** Run `scripts/5_analytics_and_reporting.sql` to view key insights and generate reporting views.

## 📊 Key Insights & Analytics
The analysis script reveals several critical business insights:
- **Seasonality:** Sales peak significantly in [Month] due to [Reason if known].
- **Customer Value:** The top 10% of customers contribute to X% of total revenue.
- **Product Performance:** Category [Name] has seen a Y% decline in Year-over-Year growth, indicating a need for strategic review.
