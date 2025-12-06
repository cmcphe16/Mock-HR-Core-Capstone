# Mock-HR-Core-Capstone
Mock HR Core Capstone

A fully-developed end-to-end Human Resources analytics solution built using Python ETL, SQL Server, and Power BI, designed to simulate an enterprise-grade HR analytics environment. This project demonstrates data engineering, data modeling, BI dashboard design, and security implementation using real-world practices.

🚀 Project Overview
This capstone project models the HR data lifecycle from raw CSV files through a curated analytics layer. The goals of the project are:
To design and implement a full ETL pipeline using Python/pandas.
To build a SQL Server data warehouse with dimension and fact tables.
To implement role-based security (RLS/CLS) for multiple user personas.
To develop interactive Power BI dashboards for Compensation, Workforce Diversity, Attendance, PTO, and more.
To demonstrate advanced analytics, modeling, and governance capabilities.

📁 Repository Structure
Mock-HR-Core-Capstone/
│
├── etl_pipeline/
│   └── etl_final.py
│
├── sql_scripts/
│   ├── create_tables.sql
│   ├── load_data.sql
│   ├── rls_policies.sql
│   └── validation_queries.sql
│
├── powerbi/
│   ├── MockHR_Dashboard.pbix
│   │   ├── dashboard1_applicant_pipeline
│   │   ├── dashboard2_attrition_retention
│   │   ├── dashboard5_compensation_structure
│   │   └── ...
│   └── role_definitions/
│       ├── HR_Manager.md
│       ├── HR_Analyst.md
│       ├── Data_Analyst.md
│       └── Employee_Self_Service.md
│
├── data_model/
│   ├── star_schema
│   ├── relationship_diagram
│   └── org_hierarchy_flow
│
└── dataset_samples/
    ├── DimEmployee_sample.csv
    ├── FactAttendance_sample.csv
    └── FactRequests_sample.csv

🛠 Technologies Used
Python (pandas, numpy)
SQL Server (SQL, views, constraints, RLS)
Power BI Desktop (DAX, data modeling, role security)
GitHub for version control and documentation

🔄 ETL Pipeline Summary
The ETL pipeline (written in Python) performs:
CSV ingestion from multiple HR data sources
Date parsing and formatting corrections
Normalization of employee, attendance, PTO, and manager data
Creation of key columns like surrogate keys and foreign keys
Loading data into SQL Server using SQLAlchemy
Post-load validation (row counts, referential integrity checks)
This pipeline ensures all datasets are clean, reliable, and analytics-ready.

🗄 SQL Server Data Warehouse
The SQL Server database includes:
Dimension Tables
DimEmployee
DimDepartment
DimManager
DimCalendar
DimSurvey
DimRequestType
DimSensitivity / BridgeUserDepartment for security

Fact Tables
FactAttendance
FactRequests
FactPTO
FactSurveyResponses
It uses a star schema with relationship constraints and a clear grain for each fact table.

🔐 Security Architecture
Role-based security is implemented in both SQL Server and Power BI:
SQL Level
HR Managers → full read access
HR Analysts → access except sensitive tables
Analysts → restricted data (no sensitive columns)
Employee Self-Service → filtered to that employee’s own records

Power BI Level
Implemented using:
RLS with dynamic filtering
BridgeUserEmployee
BridgeUserDepartment
UPN mappings for Azure AD identities
This ensures that every user only sees the data they are authorized to see.

📊 Power BI Dashboards
Includes multiple dashboards with KPI cards, visuals, and advanced DAX measures.
Example dashboards:
Recruiting Pipeline
Attrition & Retention
Compensation & Workforce Structure
Diversity, Equity & Inclusion
Attendance & PTO
Each dashboard is built using real HR metrics with drill-down interactions and tooltips.

▶ How to Run This Project
Clone the repository
Install Python dependencies (pip install -r requirements.txt)
Update SQL connection string in etl_pipeline/etl_final.py
Run ETL script to load SQL Server
Download MockHR_Dashboard.pbix
Update Power BI data source credentials
Refresh all dashboards

👤 Authors
Cameron McPherson
Joseph Velasquez
Dede teteh
Musa Mustafa
