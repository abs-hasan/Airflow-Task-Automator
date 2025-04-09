from datetime import date, timedelta
from airflow.hooks.base import BaseHook
import pandas as pd
import numpy as np
import pyodbc
import json
from googleapiclient.discovery import build
from google.oauth2 import service_account
from typing import Tuple
import logging
from simple_salesforce import Salesforce

# Establish connection to SQL Server
def sql_server_connection():
    conn = BaseHook.get_connection('sql_conn') 
    connection_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={conn.host};"
        f"DATABASE={conn.schema};"
        f"UID={conn.login};"
        f"PWD={conn.password};"
    )
    sql_conn = pyodbc.connect(connection_str)
    cursor = sql_conn.cursor()
    return cursor

# Establish connection to Google Sheets
def establish_gsheets_connection():
    conn = BaseHook.get_connection('gsheets_conn') 
    keyfile_str = conn.extra_dejson.get("extra__google_cloud_platform__keyfile_dict")
    if not keyfile_str:
        raise ValueError("Google Sheets keyfile not found in connection extras.")
    creds = service_account.Credentials.from_service_account_info(
        keyfile_str, scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    return build('sheets', 'v4', credentials=creds)

# Establish connection to Salesforce
def connect_salesforce() -> Salesforce:
    conn = BaseHook.get_connection('salesforce_conn')  
    extra = conn.extra_dejson
    sf = Salesforce(
        username=conn.login,
        password=conn.password,
        organizationId=extra.get("organizationId"),
        security_token=extra.get("security_token")
    )
    return sf

# Calculate date range for data filtering
def date_range(start_count_days, end_count_days) -> Tuple[str, str]:
    date_start = date.today() - timedelta(days=start_count_days)
    date_end = date.today() - timedelta(days=end_count_days)
    return date_start.strftime('%Y-%m-%d'), date_end.strftime('%Y-%m-%d')

# Construct a SQL SELECT query
def build_query_sql_table(table: str, columns='*', where_clause=None):
    if isinstance(columns, list):
        columns = ", ".join(columns)
    sql_query = f"SELECT {columns} FROM {table}"
    if where_clause:
        sql_query += " WHERE " + where_clause
    return sql_query

# Create a DataFrame from SQL query results
def create_dataframe(sql_conn, sql_query):
    sql_conn.execute(sql_query)
    columns = [column[0] for column in sql_conn.description]
    rows = sql_conn.fetchall()
    data = [dict(zip(columns, row)) for row in rows]
    df = pd.DataFrame(data)
    return df

# Filter DataFrame based on conditions
def filter_data(df):
    date_start, date_end = date_range(120, 30)
    date_columns = 'referral_date'  
    filters_condition = [
        "product_interest in ['Care at Home', 'Home Care']",
        f"referral_date >= '{date_start}'",
        f"referral_date <= '{date_end}'",
        "unfunded_referral != 'True'"
    ]
    select_ops_columns = ['id', 'referral_date', 'stage_name', 'partner_selected', 'assessment_stage']
    
    if date_columns:
        if isinstance(date_columns, str):
            date_columns = [date_columns]
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
    
    if filters_condition:
        query_str = " and ".join(filters_condition)
        df = df.query(query_str)

    if select_ops_columns:
        df = df[select_ops_columns]
    return df

# Merge opportunity and partner DataFrames
def merge_df(df_opportunity, df_partner):
    df_merged = pd.merge(
        df_opportunity, df_partner,
        left_on='partner_selected', right_on='id',
        how='left'
    )
    df_merged.rename(columns={'id_x': 'opportunity_id'}, inplace=True)
    return df_merged

# Filter rows by stage and add markers
def filter_row_by_stage(df):
    df['closed_won'] = np.where(df['stage_name'] == "Closed Won", df['opportunity_id'], np.nan)
    df['referral'] = np.where(df['stage_name'] == "Referral", df['opportunity_id'], np.nan)
    df = df.query('stage_name not in ["Closed Withdrawn", "Closed Lost - Customer Cancelled"]')
    return df

# Aggregate counts by group
def count_values(df: pd.DataFrame, groupby_columns: list):
    df_referrals = (
        df.groupby(groupby_columns).agg(
            referred=('opportunity_id', 'nunique'),
            closed_won=('closed_won', 'nunique'),
            referral=('referral', 'nunique')
        ).reset_index()
        .rename(columns={
            'partner_selected': 'partner_id',
            'state': "partner_state"
        })
    )
    return df_referrals

# Calculate customer experience scores
def calculate_scores(df: pd.DataFrame) -> pd.DataFrame:
    df["score"] = round((df["closed_won"] / df["referred"]) * 100).fillna(0)
    df = df.sort_values(by="score", ascending=False)
    df["group"] = df["score"].apply(
        lambda score: "100 - 60" if score >= 60 else 
                      "50 - 59" if score >= 50 else 
                      "40 - 49" if score >= 40 else 
                      "0 - 39"
    )
    return df

# Update Google Sheet with results
def update_google_sheet(service, df: pd.DataFrame, sheet_name: str, **kwargs):
    spreadsheet_id = '[YOUR_SPREADSHEET_ID]'  # Placeholder for Google Sheet ID
    df_list = df.values.tolist()
    try:
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A2",
            valueInputOption="USER_ENTERED",
            body={"values": df_list}
        ).execute()
        print(f"Successfully updated {sheet_name} in Google Sheet.")
    except Exception as e:
        raise Exception(f"Failed to update Google Sheet: {str(e)}")

# Push data to Salesforce
def push_data_to_salesforce(sf: Salesforce, df: pd.DataFrame, log_file: str = 'salesforce_update.log') -> None:
    logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(message)s')
    for index, row in df.iterrows():
        partner_id = row['partner_id']
        customer_score = row['score']
        data_to_update = {'score': customer_score}
        sf.CustomObject__c.update(partner_id, data_to_update)  
        logging.info(f"Updated Partner Id {partner_id} with Score {customer_score}")
        print(f"Updated Partner Id {partner_id} with Score {customer_score}")

# Aggregate counts by stage
def count_values_by_stage(df: pd.DataFrame) -> pd.DataFrame:
    total_referred_df = (
        df.groupby(["partner_id", "name", "partner_state"])["referred"]
          .sum()
          .reset_index()
    )
    filtered = df.query('assessment_stage in ["Newly Funded", "Switching"]')
    pivot_df = filtered.pivot_table(
        index=["partner_id", "name", "partner_state"],
        columns="assessment_stage",
        values=["closed_won", "referral", "referred"],
        aggfunc="sum",
        fill_value=0
    )
    pivot_df.columns = [f"{metric}_{stage}" for metric, stage in pivot_df.columns]
    pivot_df = pivot_df.reset_index()
    result = total_referred_df.merge(pivot_df, on=["partner_id", "name", "partner_state"], how="left")
    for col in result.columns:
        if col.startswith("referred_") or col.startswith("closed_won_") or col.startswith("referral_"):
            result[col] = result[col].fillna(0)
    return result

# Calculate extended stage scores
def calculate_scores_extended(df: pd.DataFrame) -> pd.DataFrame:
    df["score_newly_funded"] = round((df["closed_won_newly_funded"] / df["referred_newly_funded"]) * 100).fillna(0)
    df["score_switching"] = round((df["closed_won_switching"] / df["referred_switching"]) * 100).fillna(0)
    return df

# Reorder DataFrame columns
def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    desired_order = [
        "partner_id", "name", "partner_state", "referred",
        "referred_newly_funded", "referral_newly_funded", "closed_won_newly_funded",
        "score_newly_funded",
        "referred_switching", "referral_switching", "closed_won_switching", 
        "score_switching"
    ]
    cols_order = [col for col in desired_order if col in df.columns]
    remaining_cols = [col for col in df.columns if col not in cols_order]
    return df[cols_order + remaining_cols]



# Main function to run the pipeline
def main_partner_score():
    sql_cursor = sql_server_connection()
    date_start, date_end = date_range(120, 30)
    
    opportunities_query = build_query_sql_table('opportunity_table', where_clause=f"referral_date BETWEEN '{date_start}' AND '{date_end}'")
    opportunities_df = create_dataframe(sql_cursor, opportunities_query)
    
    filtered_opportunities_df = filter_data(opportunities_df)
    
    partner_sql_query = build_query_sql_table('partner_table')
    partner_df = create_dataframe(sql_cursor, partner_sql_query)
    
    combined_df = merge_df(filtered_opportunities_df, partner_df)
    final_df = filter_row_by_stage(combined_df)
    initial_group_columns = ['partner_selected', 'name', 'state']
    summary_df = count_values(final_df, initial_group_columns)
    cs_scores = calculate_scores(summary_df)
    
    gs_client = establish_gsheets_connection()
    update_google_sheet(gs_client, cs_scores, 'Summary')
    
    print("Data to be upserted:")
    print(cs_scores.head())
    
    sf = connect_salesforce()
    push_data_to_salesforce(sf, cs_scores)
    
    stage_group_columns = ['partner_selected', 'name', 'state', 'assessment_stage']
    stage_summary_df = count_values(final_df, stage_group_columns)
    stage_detail_df = count_values_by_stage(stage_summary_df)
    
    stage_scored_df = calculate_scores_extended(stage_detail_df)
    final_stage_df = reorder_columns(stage_scored_df)
    
    update_google_sheet(gs_client, final_stage_df, 'Breakdown by Stage')