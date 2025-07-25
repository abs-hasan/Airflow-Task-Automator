# 🚀 Partner Performance Scoring Pipeline (Airflow + SQL + Salesforce + GSheets)

![GitHub](https://img.shields.io/badge/GitHub-Repository-blue?logo=github)
![Python](https://img.shields.io/badge/Python-3.9+-blue)
![Airflow](https://img.shields.io/badge/Airflow-DAG-green)
![SQL%20Server](https://img.shields.io/badge/SQL-Server-informational)
![Salesforce](https://img.shields.io/badge/CRM-Salesforce-lightgrey)
![Google%20Sheets](https://img.shields.io/badge/API-Google%20Sheets-yellow)

## 📈 Project Summary

This **production-grade data pipeline** automates the end-to-end calculation of customer referral-to-win scores for partnered vendors.

It integrates multiple systems (SQL Server, Salesforce, Google Sheets) and applies robust business logic to filter, transform, and update weekly partner performance metrics.

> 🔥 **Why This Project Matters**\
> This solution is used to **track sales conversions**, identify **top-performing partners**, and **surface operational insights** — replacing manual Excel reports with a fully automated, reliable process.


## How the Process Works
I pull the data from SQL Server, process the data using Python, and then push the data to Google Sheets and the CRM system.

### Features
- **Extract**: Retrieves opportunity and partner data from SQL Server for a 90-day period (120 to 30 days ago).
- **Transform**: Processes data with Python and pandas to:
  - Filter opportunities based on date, product interest, and status.
  - Merge opportunity and partner data.
  - Calculate customer experience scores and stage-specific metrics.
- **Load**: Updates Google Sheets with summary and stage-breakdown reports and pushes scores to Salesforce.
- **Automation**: Scheduled weekly via Airflow.
- **Scalability**: Modular design supports additional data sources or metrics.

## How It Helps the Business
- **Saves Time**: Before this pipeline, teams spent hours each week manually collecting data and preparing results. Now, it’s all automated.
- **Reduces Errors**: Manual work often had mistakes. This pipeline keeps data clean and accurate.
- **Improves Decisions**: Stakeholders can now view partner performance easily and take timely action to improve client outcomes and retention.


## 🧠 Skills Demonstrated

| Category                  | Highlights                                          |
| ------------------------- | --------------------------------------------------- |
| **Data Engineering**      | SQL extraction, pyodbc, Airflow scheduling          |
| **ETL & Automation**      | End-to-end DAG orchestration with retry logic       |
| **Data Transformation**   | Pandas filtering, grouping, scoring, enrichment     |
| **External Integrations** | Google Sheets API, Salesforce Python SDK            |
| **Cloud & Secrets**       | Airflow `BaseHook` for connection security          |
| **Code Quality**          | Modular functions, clear naming, exception handling |
| **Real-World Use Case**   | Referral pipeline from CRM to executive scorecard   |

---

## 🛠 Architecture Overview

```
graph TD;
    A [SQL Server<br>Opportunity + Partner Tables] --> B[Python <br> (Pandas ETL)];
    B --> C[Filtered + Enriched DataFrame];
    C --> D[Score Calculation <br> + Stage Breakdown];
    D --> E[Google Sheets];
    D --> F[Salesforce API <br> Score Update];
    A -->|Airflow DAG Trigger| B;
```

---

## 📅 DAG Schedule & Details

| Property      | Value                      |
| ------------- | -------------------------- |
| DAG Name      | `data_processing_pipeline` |
| Trigger       | Every Tuesday at 7PM       |
| Retries       | 1 retry (5-min delay)      |
| Alerts        | Email on failure & retry   |
| Airflow Owner | `data_team`                |

---

## 🔍 Business Logic Summary

| Step        | Description                                                           |
| ----------- | --------------------------------------------------------------------- |
| 1️⃣ Extract | SQL Server queries pull referrals from last 120–30 days               |
| 2️⃣ Filter  | Only includes `Care at Home` / `Home Care` with valid referral status |
| 3️⃣ Merge   | Opportunities merged with partner metadata                            |
| 4️⃣ Score   | Calculates `Closed Won %` and assigns tier group                      |
| 5️⃣ Report  | Uploads summary and stage-level breakdown to Google Sheets            |
| 6️⃣ Update  | Pushes calculated scores back to Salesforce via API                   |

## Sample Output
The pipeline produces two datasets:
1. **Summary Report** (Google Sheets: `Summary` tab):
   | partner_id | name            | partner_state | referred | closed_won | referral | score | group   |
   |------------|-----------------|---------------|----------|------------|----------|-------|---------|
   | P001       | Partner A       | CA            | 100      | 60         | 80       | 75.0  | 100 - 60 |
   | P002       | Partner B       | NY            | 50       | 20         | 40       | 50.0  | 50 - 59  |

2. **Stage Breakdown** (Google Sheets: `Breakdown by Stage` tab):
   | partner_id | name            | partner_state | referred | referred_newly_funded | referral_newly_funded | closed_won_newly_funded | score_newly_funded | referred_switching | referral_switching | closed_won_switching | score_switching |
   |------------|-----------------|---------------|----------|-----------------------|-----------------------|-------------------------|--------------------|--------------------|--------------------|---------------------|-----------------|
   | P001       | Partner A       | CA            | 100      | 60                    | 50                    | 45                      | 75.0               | 40                 | 30                 | 15                  | 50.0            |


## 🔐 Airflow Connections Used

| Conn ID                  | Description                                  |
| ------------------------ | -------------------------------------------- |
| `sql_conn`               | SQL Server (pyodbc) connection               |
| `gsheets_conn`           | Google Service Account key (Sheets API)      |
| `salesforce_conn`        | Salesforce credentials (username + token)    |
| `partner_score_sheet_id` | Google Sheet ID (stored as Airflow Variable) |

---

## ✅ How to Run (Airflow Setup)

1. Add connections to Airflow Admin UI (`sql_conn`, `gsheets_conn`, `salesforce_conn`)
2. Set Airflow Variable: `partner_score_sheet_id`
3. Place the DAG in `~/airflow/dags/`
4. Activate scheduler:
   ```bash
   airflow scheduler
   airflow webserver
   ```
5. Trigger manually or wait for schedule (Tuesday 7PM)

---

## 📦 Requirements

```bash
pip install -r requirements.txt
```

```text
apache-airflow
pandas
pyodbc
simple-salesforce
google-api-python-client
google-auth
```

---

## Code Files
- [DAG Code](https://github.com/abs-hasan/Airflow-Task-Automator/blob/main/Dag_File.py): Sets up the Airflow schedule and tasks.
- [Operator Code](https://github.com/abs-hasan/Airflow-Task-Automator/blob/main/Score_Calculation.py): Handles the data processing and connections.
