import streamlit as st
import pandas as pd
import plotly.express as px
import duckdb

st.set_page_config(
    page_title="Minnesota Mortgage Fair Lending Dashboard",
    layout="wide"
)

st.title("Minnesota Mortgage Fair Lending Dashboard")
st.caption(
    "Source: 2024 HMDA Loan/Application Register, FFIEC/CFPB. "
    "Disparity metrics are unadjusted and do not control for "
    "credit score, LTV, or DTI. Use as a screening tool only."
)

import os
DB_PATH = os.path.join(os.path.dirname(__file__), "Data", "hmda_2024.duckdb")

@st.cache_data
def load():
    conn = duckdb.connect(DB_PATH, read_only=True)
    approval  = conn.execute("SELECT * FROM agg_approval_by_race").df()
    metrics   = conn.execute("SELECT * FROM agg_loan_metrics").df()
    denial    = conn.execute("SELECT * FROM agg_denial_reasons").df()
    income    = conn.execute("SELECT * FROM agg_income_ami").df()
    conn.close()
    return approval, metrics, denial, income

approval, metrics, denial, income = load()

# Normalize column names to lowercase for safety
approval.columns  = [c.lower() for c in approval.columns]
metrics.columns   = [c.lower() for c in metrics.columns]
denial.columns    = [c.lower() for c in denial.columns]
income.columns    = [c.lower() for c in income.columns]

# ------------------------------------------------------------
# Tile 1: Approval Rates by Race
# ------------------------------------------------------------
col1, col2 = st.columns(2)

with col1:
    st.subheader("Mortgage Approval Rate by Race / Ethnicity")
    st.caption("All Minnesota lenders combined, 2024")

    agg1 = (
        approval.groupby("derived_race_clean")
        .agg(
            total_applications=("total_applications", "sum"),
            total_approved=("total_approved", "sum")
        )
        .reset_index()
    )
    agg1["approval_rate"] = (
        agg1["total_approved"] / agg1["total_applications"] * 100
    ).round(1)
    agg1 = agg1.sort_values("approval_rate", ascending=False)

    fig1 = px.bar(
        agg1,
        x="derived_race_clean",
        y="approval_rate",
        text="approval_rate",
        color="derived_race_clean",
        labels={
            "derived_race_clean": "Race / Ethnicity",
            "approval_rate": "Approval Rate (%)"
        },
        color_discrete_sequence=px.colors.qualitative.Safe
    )
    fig1.add_hline(
        y=80, line_dash="dash", line_color="red",
        annotation_text="80% Adverse Impact Threshold"
    )
    fig1.update_traces(textposition="outside")
    fig1.update_layout(showlegend=False, xaxis_tickangle=-30)
    st.plotly_chart(fig1, use_container_width=True)

# ------------------------------------------------------------
# Tile 2: Top Lenders by Volume + Higher-Priced Rate
# ------------------------------------------------------------
with col2:
    st.subheader("Top 15 Lenders by Application Volume")
    st.caption("Color = % of loans classified as higher-priced (rate spread ≥ 1.5)")

    top15 = (
        metrics[metrics["respondent_name"].notna()]
        .sort_values("total_applications", ascending=False)
        .head(15)
    )

    fig2 = px.bar(
        top15,
        x="total_applications",
        y="respondent_name",
        orientation="h",
        color="higher_priced_rate_pct",
        color_continuous_scale="RdYlGn_r",
        labels={
            "total_applications": "Total Applications",
            "respondent_name": "Institution",
            "higher_priced_rate_pct": "Higher-Priced Rate (%)"
        },
        text="total_applications"
    )
    fig2.update_layout(yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig2, use_container_width=True)

# ------------------------------------------------------------
# Tile 3: Denial Reasons by Race
# ------------------------------------------------------------
col3, col4 = st.columns(2)

with col3:
    st.subheader("Primary Denial Reasons by Race / Ethnicity")
    st.caption("Denied applications only")

    denial_filtered = denial[
        denial["denial_reason_1_label"] != "Not Applicable"
    ]

    fig3 = px.bar(
        denial_filtered,
        x="denial_reason_1_label",
        y="denial_count",
        color="derived_race_clean",
        barmode="group",
        labels={
            "denial_reason_1_label": "Primary Denial Reason",
            "denial_count": "Number of Denials",
            "derived_race_clean": "Race / Ethnicity"
        },
        color_discrete_sequence=px.colors.qualitative.Safe
    )
    fig3.update_layout(xaxis_tickangle=-30)
    st.plotly_chart(fig3, use_container_width=True)

# ------------------------------------------------------------
# Tile 4: Income Tiers by Metro Area
# ------------------------------------------------------------
with col4:
    st.subheader("Applicant Income Tier by Minnesota Metro Area")
    st.caption(
        "Income tiers relative to FFIEC Area Median Income (AMI). "
        "Low = <50% AMI, Moderate = 50-80%, Middle = 80-120%, Upper = >120%"
    )

    income_filtered = income[
        income["income_to_ami_bucket"].notna() &
        ~income["income_to_ami_bucket"].isin(["Unknown", "Exempt"]) &
        income["msa_md_name"].notna()
    ]

    tier_order = [
        "Low (<50% AMI)",
        "Moderate (50-80% AMI)",
        "Middle (80-120% AMI)",
        "Upper (>120% AMI)"
    ]

    fig4 = px.bar(
        income_filtered,
        x="msa_md_name",
        y="total_applications",
        color="income_to_ami_bucket",
        barmode="stack",
        category_orders={"income_to_ami_bucket": tier_order},
        labels={
            "msa_md_name": "Metro Area",
            "total_applications": "Total Applications",
            "income_to_ami_bucket": "Income Tier"
        },
        color_discrete_sequence=px.colors.qualitative.Safe
    )
    fig4.update_layout(xaxis_tickangle=-30)
    st.plotly_chart(fig4, use_container_width=True)

# ------------------------------------------------------------
# Institution lookup — bonus interactive element
# ------------------------------------------------------------
st.divider()
st.subheader("Look Up a Specific Lender")
st.caption(
    "Search for an institution to see its approval rates by race. "
    "AIR = Adverse Impact Ratio vs. White applicants. "
    "Values below 0.80 meet the standard threshold for further review."
)

institutions = sorted(
    approval["respondent_name"].dropna().unique().tolist()
)
selected = st.selectbox("Select institution:", institutions)

if selected:
    inst_data = approval[
        approval["respondent_name"] == selected
    ].copy()

    white_rate = inst_data.loc[
        inst_data["derived_race_clean"] == "White",
        "approval_rate_pct"
    ]
    white_rate = white_rate.values[0] if len(white_rate) > 0 else None

    if white_rate:
        inst_data["air_vs_white"] = (
            inst_data["approval_rate_pct"] / white_rate
        ).round(4)
    else:
        inst_data["air_vs_white"] = None

    inst_data = inst_data.sort_values(
        "approval_rate_pct", ascending=False
    )

    st.dataframe(
        inst_data[[
            "derived_race_clean",
            "total_applications",
            "total_approved",
            "total_denied",
            "approval_rate_pct",
            "air_vs_white"
        ]].rename(columns={
            "derived_race_clean":  "Race / Ethnicity",
            "total_applications":  "Applications",
            "total_approved":      "Approved",
            "total_denied":        "Denied",
            "approval_rate_pct":   "Approval Rate (%)",
            "air_vs_white":        "AIR vs. White"
        }),
        use_container_width=True,
        hide_index=True
    )
    
