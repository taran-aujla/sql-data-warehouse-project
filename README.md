# Data Warehouse and Analytics Project
Building a Modern  data warehouse with SQL Server, ETL processes, Data modeling, and analytics

Welcome to the Data Warehouse and Analytics Project repository! 🚀
This project demonstrates a comprehensive data warehousing and analytics solution, from building a data warehouse to generating actionable insights. Designed as a portfolio project, it highlights industry best practices in data engineering and analytics.

Tools used -- 
1) Draw.io (for data warehouse Architecture, Data Flow Visualization, and Relationship of fact and dimensions using tables)
2) Server Management Studio (SSMS)- to efficiently manage, query, and visualize data transformations across the Bronze, Silver, and Gold layers within the data warehouse and execute the ETL workflow.
![image](https://github.com/user-attachments/assets/1033ed2e-bf4a-4983-bfee-7340e5fba23f)


 Data Pipeline Overview: From Raw Files to Business Insights
 
🗂️ 1. Sources
Input Systems: CRM and ERP systems export data as CSV files.

Interface: Data is collected from folders containing these files.

Objective: Act as the raw source feeding into the data warehouse.

🟫 2. Bronze Layer: Raw Data Storage
Object Type: Tables

Load Strategy:

Batch Processing

Full Load

Truncate & Insert

Transformations: None — raw data is loaded as-is.

Data Model: No specific model; retains original structure.

Purpose: Preserve original data for traceability and rollback.

⬜ 3. Silver Layer: Cleaned & Standardized Data
Object Type: Tables

Load Strategy:

Batch Processing

Full Load

Truncate & Insert

Transformations:

Data Cleansing (e.g., null handling, duplicates)

Standardization (formatting consistency)

Normalization (relational structuring)

Derived Columns (e.g., computed fields)

Data Enrichment (external lookups, mappings)

Data Model: Still none — primarily structured for internal transformation logic.

Purpose: Prepare data for business-friendly modeling in the Gold Layer.

🟨 4. Gold Layer: Business-Ready Data
Object Type: Views (Not tables)

Load Strategy: No Load — virtualized using SQL Views.

Transformations:

Business Logic Implementation

Data Integrations (joining dimensions/facts)

Aggregations (summing, averaging, etc.)

Data Model:

Star Schema

Flat Tables

Aggregated Tables

Purpose: Serve analytical tools and reporting systems with clean, contextual data.

📊 5. Consume Layer: Data Consumption Tools
BI & Reporting: Tools like Power BI, Tableau connect to Gold Views for dashboards.

Ad-Hoc SQL Queries: Analysts run custom queries directly on Gold Views.

Machine Learning: Gold Layer also serves as clean training data for ML models.

Objective: Enable fast, reliable, and scalable business decision-making.

