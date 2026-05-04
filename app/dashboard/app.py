import time
from io import StringIO

import boto3
import pandas as pd
import streamlit as st


st.set_page_config(page_title="Open Payments Analytics", layout="wide")

DATABASE = "openpayments"
ATHENA_OUTPUT = "s3://openpayments-dezoomcamp2026-us-west-1-1f83ec/athena-query-results/"
AWS_REGION = "us-west-1"


QUERIES = {
    "kpi_summary": """
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT applicable_manufacturer_or_applicable_gpo_making_payment_name) AS distinct_companies,
            COUNT(DISTINCT record_id) AS distinct_records,
            SUM(CAST(total_amount_of_payment_usdollars AS DOUBLE)) AS total_payment_amount
        FROM openpayments.raw_general_payments
    """,
    "monthly_totals": """
        SELECT
            year,
            date_format(
                try(date_parse(date_of_payment, '%m/%d/%Y')),
                '%Y-%m'
            ) AS payment_month,
            SUM(CAST(total_amount_of_payment_usdollars AS DOUBLE)) AS total_payment_amount
        FROM openpayments.raw_general_payments
        WHERE try(date_parse(date_of_payment, '%m/%d/%Y')) IS NOT NULL
          AND year IN ('2023', '2024')
        GROUP BY 1, 2
        ORDER BY 2, 1
    """,
    "top_companies": """
        SELECT
            applicable_manufacturer_or_applicable_gpo_making_payment_name AS company_name,
            SUM(CAST(total_amount_of_payment_usdollars AS DOUBLE)) AS total_payment_amount
        FROM openpayments.raw_general_payments
        WHERE applicable_manufacturer_or_applicable_gpo_making_payment_name IS NOT NULL
          AND trim(applicable_manufacturer_or_applicable_gpo_making_payment_name) <> ''
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 10
    """,
    "payment_nature": """
        SELECT
            nature_of_payment_or_transfer_of_value AS payment_nature,
            SUM(CAST(total_amount_of_payment_usdollars AS DOUBLE)) AS total_payment_amount
        FROM openpayments.raw_general_payments
        WHERE nature_of_payment_or_transfer_of_value IS NOT NULL
          AND trim(nature_of_payment_or_transfer_of_value) <> ''
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 15
    """,
    "year_comparison": """
        SELECT
            year,
            COUNT(*) AS payment_count,
            SUM(CAST(total_amount_of_payment_usdollars AS DOUBLE)) AS total_payment_amount,
            AVG(CAST(total_amount_of_payment_usdollars AS DOUBLE)) AS avg_payment_amount
        FROM openpayments.raw_general_payments
        WHERE year IN ('2023', '2024')
        GROUP BY 1
        ORDER BY 1
    """,
}


def run_athena_query(query: str) -> pd.DataFrame:
    client = boto3.client("athena", region_name=AWS_REGION)

    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
    )

    query_execution_id = response["QueryExecutionId"]

    while True:
        execution = client.get_query_execution(QueryExecutionId=query_execution_id)
        state = execution["QueryExecution"]["Status"]["State"]

        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break

        time.sleep(1)

    if state != "SUCCEEDED":
        reason = execution["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")
        raise RuntimeError(f"Athena query failed: {reason}")

    result_path = execution["QueryExecution"]["ResultConfiguration"]["OutputLocation"]

    s3 = boto3.client("s3", region_name=AWS_REGION)
    bucket = result_path.replace("s3://", "").split("/")[0]
    key = "/".join(result_path.replace("s3://", "").split("/")[1:])

    obj = s3.get_object(Bucket=bucket, Key=key)
    csv_data = obj["Body"].read().decode("utf-8")

    return pd.read_csv(StringIO(csv_data))


@st.cache_data(show_spinner=False)
def load_data(name: str) -> pd.DataFrame:
    return run_athena_query(QUERIES[name])


def clean_monthly_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["payment_month"] = pd.to_datetime(df["payment_month"], format="%Y-%m", errors="coerce")
    df = df.dropna(subset=["payment_month"])

    if "year" in df.columns:
        df["year"] = df["year"].astype(str)
        df = df[df["year"].isin(["2023", "2024"])]

    df = df[df["payment_month"].dt.year.isin([2023, 2024])]
    df["payment_month"] = df["payment_month"].dt.strftime("%Y-%m")

    return df


st.title("CMS Open Payments Analytics Platform")
st.caption("AWS batch analytics MVP for 2023 and 2024 General Payments data")

with st.spinner("Loading dashboard data..."):
    kpi_df = load_data("kpi_summary")
    monthly_df = load_data("monthly_totals")
    companies_df = load_data("top_companies")
    nature_df = load_data("payment_nature")
    comparison_df = load_data("year_comparison")

monthly_df = clean_monthly_df(monthly_df)

k1, k2, k3, k4 = st.columns(4)

k1.metric("Total Rows", f"{int(kpi_df.loc[0, 'total_rows']):,}")
k2.metric("Distinct Companies", f"{int(kpi_df.loc[0, 'distinct_companies']):,}")
k3.metric("Distinct Records", f"{int(kpi_df.loc[0, 'distinct_records']):,}")
k4.metric("Total Payment Amount", f"${float(kpi_df.loc[0, 'total_payment_amount']):,.2f}")

st.subheader("Monthly Total Payments by Year")
monthly_pivot = (
    monthly_df.pivot(index="payment_month", columns="year", values="total_payment_amount")
    .fillna(0)
    .sort_index()
)
st.line_chart(monthly_pivot)

col1, col2 = st.columns(2)

with col1:
    st.subheader("Top 10 Companies by Total Payment Amount")
    companies_df["total_payment_amount"] = pd.to_numeric(
        companies_df["total_payment_amount"], errors="coerce"
    )
    companies_df = companies_df.dropna(subset=["total_payment_amount"])
    companies_df = companies_df.set_index("company_name")
    st.bar_chart(companies_df)

with col2:
    st.subheader("Top Payment Natures")
    nature_df["total_payment_amount"] = pd.to_numeric(
        nature_df["total_payment_amount"], errors="coerce"
    )
    nature_df = nature_df.dropna(subset=["total_payment_amount"])
    nature_df = nature_df.set_index("payment_nature")
    st.bar_chart(nature_df)

st.subheader("2023 vs 2024 Comparison")
comparison_df["payment_count"] = pd.to_numeric(comparison_df["payment_count"], errors="coerce")
comparison_df["total_payment_amount"] = pd.to_numeric(
    comparison_df["total_payment_amount"], errors="coerce"
)
comparison_df["avg_payment_amount"] = pd.to_numeric(
    comparison_df["avg_payment_amount"], errors="coerce"
)
st.dataframe(comparison_df, use_container_width=True)