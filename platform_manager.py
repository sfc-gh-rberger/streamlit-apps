import streamlit as st
import plotly.express as px
import pandas as pd
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="Snowflake platform monitor",
    page_icon=":material/monitoring:",
    layout="wide",
)

# ── Constants ──────────────────────────────────────────────────────────────────

CHART_HEIGHT       = 300
SMALL_CHART_HEIGHT = 260
_PLOTLY_CONFIG     = {"displayModeBar": False}

DATE_RANGES: dict = {
    "1 week":        {"days": 7,    "trunc": "day",   "tick": "%b %d"},
    "2 weeks":       {"days": 14,   "trunc": "day",   "tick": "%b %d"},
    "Last month":    {"days": 30,   "trunc": "day",   "tick": "%b %d"},
    "Current month": {"days": None, "trunc": "day",   "tick": "%b %d", "current_month": True},
    "Last 3 months": {"days": 90,   "trunc": "week",  "tick": "%b %d"},
    "Last year":     {"days": 365,  "trunc": "month", "tick": "%b %Y"},
}

# ── Connection ─────────────────────────────────────────────────────────────────

@st.cache_resource
def get_session():
    return get_active_session()


def run_query(sql: str) -> pd.DataFrame:
    try:
        df = get_session().sql(sql).to_pandas()
        df.columns = df.columns.str.lower()
        return df
    except Exception as e:
        st.warning(str(e), icon=":material/warning:")
        return pd.DataFrame()

# ── Chart helpers ──────────────────────────────────────────────────────────────

_TRUNC_DTICK = {"day": 86400000, "week": 604800000, "month": "M1"}


def _layout(fig, title: str, height: int, tick: str | None) -> None:
    fig.update_layout(
        margin=dict(l=0, r=0, t=50, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02,
                    xanchor="left", x=0, font=dict(size=9), title_text=""),
        xaxis_title=None, yaxis_title=None, height=height,
    )
    if tick:
        fig.update_xaxes(
            tickformat=tick,
            tickmode="linear",
            dtick=_TRUNC_DTICK.get(trunc),
            tickangle=-45,
            automargin=True,
        )


def bar(df, x, y, color=None, title="", height=SMALL_CHART_HEIGHT, tick=None):
    fig = px.bar(df, x=x, y=y, color=color, barmode="stack") if color else px.bar(df, x=x, y=y)
    _layout(fig, title, height, tick)
    return fig


def area(df, x, y, color=None, title="", height=SMALL_CHART_HEIGHT, tick=None):
    fig = px.area(df, x=x, y=y, color=color)
    _layout(fig, title, height, tick)
    return fig


def hbar(df, x, y, title="", height=SMALL_CHART_HEIGHT):
    fig = px.bar(df, x=x, y=y, orientation="h")
    fig.update_layout(
        margin=dict(l=0, r=0, t=10, b=20),
        xaxis_title=None, yaxis_title=None, height=height,
    )
    fig.update_yaxes(categoryorder="total ascending")
    return fig


def _top_n_other(df: pd.DataFrame, group_col: str, value_col: str, n: int = 8) -> pd.DataFrame:
    if df.empty or group_col not in df.columns:
        return df
    top = df.groupby(group_col)[value_col].sum().nlargest(n).index
    out = df.copy()
    out[group_col] = out[group_col].where(out[group_col].isin(top), "Other")
    agg_cols = [c for c in df.columns if c not in (group_col, value_col)]
    return out.groupby(agg_cols + [group_col], as_index=False)[value_col].sum().sort_values(agg_cols).reset_index(drop=True)


def chart_card(df, fig_fn, *args, empty_label: str = "", **kwargs):
    title = kwargs.pop("title", "") or empty_label
    with st.container(border=True):
        if title:
            st.markdown(f"**{title}**")
        if not df.empty:
            st.plotly_chart(fig_fn(df, *args, **kwargs), use_container_width=True, config=_PLOTLY_CONFIG)
        else:
            if len(df.columns) == 0:
                st.caption(":material/error: Query failed — check ACCOUNT_USAGE privileges.")
            else:
                st.caption("No activity recorded in this period.")

# ── Sidebar ────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("### :material/tune: Filters")
    date_range = st.radio("Date range", list(DATE_RANGES.keys()), index=2)
    st.divider()
    st.markdown("### :material/manage_accounts: Identity")
    stale_threshold = st.number_input("Stale user threshold (days)", min_value=1, value=90, step=1)

cfg   = DATE_RANGES[date_range]
tick  = cfg["tick"]
trunc = cfg["trunc"]
days  = cfg.get("days", 30)
cm    = cfg.get("current_month", False)

# ── Query functions ────────────────────────────────────────────────────────────
# Overview

@st.cache_data(ttl=600)
def q_kpi(days, cm):
    c = "usage_date >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"usage_date > CURRENT_DATE - {days}"
    return run_query(f"SELECT SUM(credits_used) as total_credits FROM snowflake.account_usage.metering_daily_history WHERE {c}")

@st.cache_data(ttl=600)
def q_kpi_prior(days, cm):
    if cm:
        c = "usage_date >= DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE)) AND usage_date < DATE_TRUNC('month', CURRENT_DATE)"
    else:
        c = f"usage_date > CURRENT_DATE - {days * 2} AND usage_date <= CURRENT_DATE - {days}"
    return run_query(f"SELECT SUM(credits_used) as total_credits FROM snowflake.account_usage.metering_daily_history WHERE {c}")

@st.cache_data(ttl=3600)
def q_storage_kpi():
    return run_query("SELECT SUM(average_database_bytes)/POWER(1024,4) as total_tb FROM snowflake.account_usage.database_storage_usage_history WHERE usage_date >= CURRENT_DATE - 1")

@st.cache_data(ttl=600)
def q_metering(trunc, days, cm):
    c = "usage_date >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"usage_date > CURRENT_DATE - {days}"
    return run_query(f"SELECT DATE_TRUNC('{trunc}', usage_date) as period, service_type, SUM(credits_used) as credits_used FROM snowflake.account_usage.metering_daily_history WHERE {c} GROUP BY ALL ORDER BY 1,2")

@st.cache_data(ttl=600)
def q_warehouse(trunc, days, cm):
    c = "start_time >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"start_time > CURRENT_TIMESTAMP - INTERVAL '{days} days'"
    return run_query(f"SELECT DATE_TRUNC('{trunc}', start_time) as period, warehouse_name, SUM(credits_used) as credits_used FROM snowflake.account_usage.warehouse_metering_history WHERE {c} GROUP BY ALL ORDER BY 1,2")

@st.cache_data(ttl=600)
def q_storage_trend(trunc, days, cm):
    c = "usage_date >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"usage_date > CURRENT_DATE - {days}"
    return run_query(f"SELECT DATE_TRUNC('{trunc}', usage_date) as period, SUM(average_database_bytes)/POWER(1024,3) as total_gb FROM snowflake.account_usage.database_storage_usage_history WHERE {c} GROUP BY ALL ORDER BY 1")

# Detail — AI & Cortex

@st.cache_data(ttl=600)
def q_cortex_analyst(trunc, days, cm):
    c = "start_time >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"start_time > CURRENT_TIMESTAMP - INTERVAL '{days} days'"
    return run_query(f"SELECT DATE_TRUNC('{trunc}', start_time) as period, SUM(credits) as credits_used FROM snowflake.account_usage.cortex_analyst_usage_history WHERE {c} GROUP BY ALL ORDER BY 1")

@st.cache_data(ttl=600)
def q_cortex_functions(trunc, days, cm):
    c = "start_time >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"start_time > CURRENT_TIMESTAMP - INTERVAL '{days} days'"
    return run_query(f"SELECT DATE_TRUNC('{trunc}', start_time) as period, model_name, SUM(token_credits) as credits_used FROM snowflake.account_usage.cortex_functions_usage_history WHERE {c} GROUP BY ALL ORDER BY 1,2")

@st.cache_data(ttl=600)
def q_cortex_search(trunc, days, cm):
    c = "usage_date >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"usage_date > CURRENT_DATE - {days}"
    return run_query(f"SELECT DATE_TRUNC('{trunc}', usage_date) as period, service_name, SUM(credits) as credits_used FROM snowflake.account_usage.cortex_search_daily_usage_history WHERE {c} GROUP BY ALL ORDER BY 1,2")

@st.cache_data(ttl=600)
def q_doc_ai(trunc, days, cm):
    c = "start_time >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"start_time > CURRENT_TIMESTAMP - INTERVAL '{days} days'"
    return run_query(f"SELECT DATE_TRUNC('{trunc}', start_time) as period, operation_name, SUM(credits_used) as credits_used FROM snowflake.account_usage.document_ai_usage_history WHERE {c} GROUP BY ALL ORDER BY 1,2")

@st.cache_data(ttl=600)
def q_sf_intelligence(trunc, days, cm):
    c = "start_time >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"start_time > CURRENT_TIMESTAMP - INTERVAL '{days} days'"
    return run_query(f"SELECT DATE_TRUNC('{trunc}', start_time) as period, snowflake_intelligence_name, SUM(token_credits) as credits_used FROM snowflake.account_usage.snowflake_intelligence_usage_history WHERE {c} GROUP BY ALL ORDER BY 1,2")

@st.cache_data(ttl=600)
def q_cortex_agent(trunc, days, cm):
    c = "start_time >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"start_time > CURRENT_TIMESTAMP - INTERVAL '{days} days'"
    return run_query(f"SELECT DATE_TRUNC('{trunc}', start_time) as period, agent_name, SUM(token_credits) as credits_used FROM snowflake.account_usage.cortex_agent_usage_history WHERE {c} GROUP BY ALL ORDER BY 1,2")

@st.cache_data(ttl=600)
def q_cortex_fine_tuning(trunc, days, cm):
    c = "start_time >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"start_time > CURRENT_TIMESTAMP - INTERVAL '{days} days'"
    return run_query(f"SELECT DATE_TRUNC('{trunc}', start_time) as period, model_name, SUM(token_credits) as credits_used FROM snowflake.account_usage.cortex_fine_tuning_usage_history WHERE {c} GROUP BY ALL ORDER BY 1,2")

# Detail — Serverless

@st.cache_data(ttl=600)
def q_serverless(trunc, days, cm):
    c = "start_time >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"start_time > CURRENT_TIMESTAMP - INTERVAL '{days} days'"
    return run_query(f"SELECT DATE_TRUNC('{trunc}', start_time) as period, task_name, SUM(credits_used) as credits_used FROM snowflake.account_usage.serverless_task_history WHERE {c} GROUP BY ALL ORDER BY 1,2")

@st.cache_data(ttl=600)
def q_dmf(trunc, days, cm):
    c = "start_time >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"start_time > CURRENT_TIMESTAMP - INTERVAL '{days} days'"
    return run_query(f"SELECT DATE_TRUNC('{trunc}', start_time) as period, table_name, SUM(credits_used) as credits_used FROM snowflake.account_usage.data_quality_monitoring_usage_history WHERE {c} GROUP BY ALL ORDER BY 1,2")

@st.cache_data(ttl=600)
def q_event_usage(trunc, days, cm):
    c = "start_time >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"start_time > CURRENT_TIMESTAMP - INTERVAL '{days} days'"
    return run_query(f"SELECT DATE_TRUNC('{trunc}', start_time) as period, SUM(credits_used) as credits_used FROM snowflake.account_usage.event_usage_history WHERE {c} GROUP BY ALL ORDER BY 1")

@st.cache_data(ttl=600)
def q_spcs(trunc, days, cm):
    c = "start_time >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"start_time > CURRENT_TIMESTAMP - INTERVAL '{days} days'"
    return run_query(f"SELECT DATE_TRUNC('{trunc}', start_time) as period, compute_pool_name, SUM(credits_used) as credits_used FROM snowflake.account_usage.snowpark_container_services_history WHERE {c} GROUP BY ALL ORDER BY 1,2")

@st.cache_data(ttl=600)
def q_application(trunc, days, cm):
    c = "usage_date >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"usage_date > CURRENT_DATE - {days}"
    return run_query(f"SELECT DATE_TRUNC('{trunc}', usage_date) as period, application_name, SUM(credits_used) as credits_used FROM snowflake.account_usage.application_daily_usage_history WHERE {c} GROUP BY ALL ORDER BY 1,2")

@st.cache_data(ttl=600)
def q_pipe_usage(trunc, days, cm):
    c = "start_time >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"start_time > CURRENT_TIMESTAMP - INTERVAL '{days} days'"
    return run_query(f"SELECT DATE_TRUNC('{trunc}', start_time) as period, pipe_name, SUM(credits_used) as credits_used FROM snowflake.account_usage.pipe_usage_history WHERE {c} GROUP BY ALL ORDER BY 1,2")

@st.cache_data(ttl=600)
def q_auto_clustering(trunc, days, cm):
    c = "start_time >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"start_time > CURRENT_TIMESTAMP - INTERVAL '{days} days'"
    return run_query(f"SELECT DATE_TRUNC('{trunc}', start_time) as period, table_name, SUM(credits_used) as credits_used FROM snowflake.account_usage.automatic_clustering_history WHERE {c} GROUP BY ALL ORDER BY 1,2")

@st.cache_data(ttl=600)
def q_mv_refresh(trunc, days, cm):
    c = "start_time >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"start_time > CURRENT_TIMESTAMP - INTERVAL '{days} days'"
    return run_query(f"SELECT DATE_TRUNC('{trunc}', start_time) as period, table_name, SUM(credits_used) as credits_used FROM snowflake.account_usage.materialized_view_refresh_history WHERE {c} GROUP BY ALL ORDER BY 1,2")

@st.cache_data(ttl=600)
def q_search_optimization(trunc, days, cm):
    c = "start_time >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"start_time > CURRENT_TIMESTAMP - INTERVAL '{days} days'"
    return run_query(f"SELECT DATE_TRUNC('{trunc}', start_time) as period, table_name, SUM(credits_used) as credits_used FROM snowflake.account_usage.search_optimization_history WHERE {c} GROUP BY ALL ORDER BY 1,2")

@st.cache_data(ttl=600)
def q_query_acceleration(trunc, days, cm):
    c = "start_time >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"start_time > CURRENT_TIMESTAMP - INTERVAL '{days} days'"
    return run_query(f"SELECT DATE_TRUNC('{trunc}', start_time) as period, warehouse_name, SUM(credits_used) as credits_used FROM snowflake.account_usage.query_acceleration_history WHERE {c} GROUP BY ALL ORDER BY 1,2")

# Detail — Data transfer & replication

@st.cache_data(ttl=600)
def q_data_transfer(trunc, days, cm):
    c = "start_time >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"start_time > CURRENT_TIMESTAMP - INTERVAL '{days} days'"
    return run_query(f"SELECT DATE_TRUNC('{trunc}', start_time) as period, transfer_type, SUM(bytes_transferred)/POWER(1024,3) as gb_transferred FROM snowflake.account_usage.data_transfer_history WHERE {c} GROUP BY ALL ORDER BY 1,2")

@st.cache_data(ttl=600)
def q_replication(trunc, days, cm):
    c = "start_time >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"start_time > CURRENT_TIMESTAMP - INTERVAL '{days} days'"
    return run_query(f"SELECT DATE_TRUNC('{trunc}', start_time) as period, replication_group_name, SUM(credits_used) as credits_used FROM snowflake.account_usage.replication_group_usage_history WHERE {c} GROUP BY ALL ORDER BY 1,2")

# Detail — Storage

@st.cache_data(ttl=600)
def q_stage_storage(trunc, days, cm):
    c = "usage_date >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"usage_date > CURRENT_DATE - {days}"
    return run_query(f"SELECT DATE_TRUNC('{trunc}', usage_date) as period, AVG(average_stage_bytes)/POWER(1024,3) as stage_gb FROM snowflake.account_usage.stage_storage_usage_history WHERE {c} GROUP BY ALL ORDER BY 1")

@st.cache_data(ttl=600)
def q_storage_by_db(trunc, days, cm):
    c = "usage_date >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"usage_date > CURRENT_DATE - {days}"
    return run_query(f"SELECT DATE_TRUNC('{trunc}', usage_date) as period, database_name, AVG(average_database_bytes) as avg_bytes FROM snowflake.account_usage.database_storage_usage_history WHERE {c} GROUP BY ALL ORDER BY 1,2")

@st.cache_data(ttl=3600)
def q_largest_schemas():
    return run_query("SELECT table_catalog||'.'||table_schema as schema_name, SUM(active_bytes) as total_bytes FROM snowflake.account_usage.table_storage_metrics GROUP BY 1 ORDER BY 2 DESC LIMIT 15")

@st.cache_data(ttl=3600)
def q_largest_tables():
    return run_query("SELECT table_catalog||'.'||table_schema||'.'||table_name as table_name, SUM(active_bytes) as total_bytes FROM snowflake.account_usage.table_storage_metrics GROUP BY 1 ORDER BY 2 DESC LIMIT 15")

# FinOps — combined attribution query

@st.cache_data(ttl=600)
def q_finops_combined(trunc, days, cm):
    wh = "start_time >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"start_time > CURRENT_TIMESTAMP - INTERVAL '{days} days'"
    qh = "q.start_time >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"q.start_time > CURRENT_TIMESTAMP - INTERVAL '{days} days'"
    return run_query(f"""
        WITH wh AS (
            SELECT warehouse_name, DATE_TRUNC('{trunc}', start_time) as period, SUM(credits_used) as wh_credits
            FROM snowflake.account_usage.warehouse_metering_history WHERE {wh} GROUP BY 1,2
        ),
        base AS (
            SELECT q.role_name, q.query_type, q.user_name,
                   TRIM(NVL(l.reported_client_type,'')||' '||COALESCE(l.reported_client_version,'')) as client,
                   q.warehouse_name, DATE_TRUNC('{trunc}', q.start_time) as period, SUM(q.execution_time) as ms
            FROM snowflake.account_usage.query_history q
            LEFT JOIN snowflake.account_usage.login_history l ON q.authn_event_id=l.event_id
            WHERE {qh} AND q.warehouse_name IS NOT NULL AND q.execution_status='SUCCESS'
            GROUP BY 1,2,3,4,5,6
        ),
        wt AS (SELECT warehouse_name, period, SUM(ms) as total_ms FROM base GROUP BY 1,2)
        SELECT b.role_name, b.query_type, b.user_name, b.client, b.period,
               ROUND(SUM(b.ms/NULLIF(t.total_ms,0)*w.wh_credits),4) as attributed_credits
        FROM base b
        JOIN wt t ON b.warehouse_name=t.warehouse_name AND b.period=t.period
        JOIN wh w ON b.warehouse_name=w.warehouse_name AND b.period=w.period
        GROUP BY ALL ORDER BY b.period
    """)

# Warehouse performance — combined query_history scan

@st.cache_data(ttl=600)
def q_wh_perf_combined(trunc, days, cm):
    c = "start_time >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"start_time > CURRENT_TIMESTAMP - INTERVAL '{days} days'"
    return run_query(f"""
        SELECT DATE_TRUNC('{trunc}', start_time) as period, warehouse_name, warehouse_size,
               COUNT(*) as total_queries,
               SUM(CASE WHEN queued_overload_time>0 THEN 1 ELSE 0 END) as queued_count,
               ROUND(AVG(queued_overload_time)/1000.0,2) as avg_overload_wait_sec,
               SUM(CASE WHEN bytes_spilled_to_remote_storage>0 THEN 1 ELSE 0 END) as spilling_queries,
               ROUND(SUM(bytes_spilled_to_remote_storage)/1e9,2) as remote_spill_gb,
               ROUND(SUM(bytes_spilled_to_local_storage)/1e9,2) as local_spill_gb
        FROM snowflake.account_usage.query_history
        WHERE {c} AND warehouse_name IS NOT NULL
        GROUP BY 1,2,3 ORDER BY 1,2
    """)

@st.cache_data(ttl=600)
def q_top_cost_queries(days, cm):
    wh = "start_time >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"start_time > CURRENT_TIMESTAMP - INTERVAL '{days} days'"
    qh = "q.start_time >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"q.start_time > CURRENT_TIMESTAMP - INTERVAL '{days} days'"
    return run_query(f"""
        WITH wh_c AS (SELECT warehouse_name, SUM(credits_used) as total_credits FROM snowflake.account_usage.warehouse_metering_history WHERE {wh} GROUP BY 1),
        wh_t AS (SELECT warehouse_name, SUM(execution_time) as total_ms FROM snowflake.account_usage.query_history q WHERE {qh} AND q.warehouse_name IS NOT NULL AND q.execution_status='SUCCESS' GROUP BY 1),
        pq AS (SELECT q.query_hash, ANY_VALUE(q.query_id) as sample_query_id, LEFT(ANY_VALUE(q.query_text),200) as sample_query,
               COUNT(*) as execution_count, ROUND(AVG(q.execution_time)/1000.0,2) as avg_exec_seconds,
               SUM(q.execution_time) as total_ms, ANY_VALUE(q.warehouse_name) as warehouse_name,
               ANY_VALUE(q.user_name) as user_name, ANY_VALUE(q.role_name) as role_name
               FROM snowflake.account_usage.query_history q WHERE {qh} AND q.execution_status='SUCCESS' AND q.query_hash IS NOT NULL GROUP BY q.query_hash)
        SELECT p.execution_count, p.avg_exec_seconds, p.user_name, p.role_name, p.warehouse_name,
               p.sample_query_id, p.sample_query,
               ROUND(p.total_ms/NULLIF(t.total_ms,0)*c.total_credits,4) as attributed_credits
        FROM pq p LEFT JOIN wh_t t ON p.warehouse_name=t.warehouse_name LEFT JOIN wh_c c ON p.warehouse_name=c.warehouse_name
        ORDER BY attributed_credits DESC NULLS LAST LIMIT 10
    """)

# Security — combined user base query

@st.cache_data(ttl=3600)
def q_user_base():
    return run_query("""SELECT name, type, email, has_password, has_rsa_public_key, has_mfa, ext_authn_duo,
        default_role, disabled, deleted_on, last_success_login, password_last_set_time, created_on
        FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY name ORDER BY created_on DESC) as rn
              FROM snowflake.account_usage.users)
        WHERE rn=1 AND deleted_on IS NULL AND disabled=FALSE AND name!='SNOWFLAKE'""")

@st.cache_data(ttl=600)
def q_login_failures_trend(trunc, days, cm):
    c = "event_timestamp >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"event_timestamp > CURRENT_TIMESTAMP - INTERVAL '{days} days'"
    return run_query(f"SELECT DATE_TRUNC('{trunc}', event_timestamp) as period, error_message, COUNT(*) as failures FROM snowflake.account_usage.login_history WHERE is_success='NO' AND {c} GROUP BY ALL ORDER BY 1,2")

@st.cache_data(ttl=600)
def q_login_failures_detail(days, cm):
    c = "event_timestamp >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"event_timestamp > CURRENT_TIMESTAMP - INTERVAL '{days} days'"
    return run_query(f"SELECT user_name, error_message, COUNT(*) as num_failures FROM snowflake.account_usage.login_history WHERE is_success='NO' AND {c} GROUP BY 1,2 ORDER BY 3 DESC LIMIT 20")

@st.cache_data(ttl=600)
def q_auth_methods(days, cm):
    c = "event_timestamp >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"event_timestamp > CURRENT_TIMESTAMP - INTERVAL '{days} days'"
    return run_query(f"SELECT TRIM(first_authentication_factor||' '||COALESCE(second_authentication_factor,'')) as auth_method, COUNT(*) as login_count FROM snowflake.account_usage.login_history WHERE is_success='YES' AND user_name!='WORKSHEETS_APP_USER' AND {c} GROUP BY 1 ORDER BY 2 DESC")

@st.cache_data(ttl=3600)
def q_aa_grants(days, cm):
    c = "end_time >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"end_time > CURRENT_TIMESTAMP - INTERVAL '{days} days'"
    return run_query(f"SELECT user_name, query_text, end_time FROM snowflake.account_usage.query_history WHERE execution_status='SUCCESS' AND query_type='GRANT' AND query_text ILIKE '%grant%accountadmin%to%' AND {c} ORDER BY end_time DESC LIMIT 50")

@st.cache_data(ttl=3600)
def q_aa_no_mfa():
    return run_query("SELECT u.name, TIMEDIFF('day',last_success_login,CURRENT_TIMESTAMP())||' days ago' as last_login, TIMEDIFF('day',password_last_set_time,CURRENT_TIMESTAMP())||' days ago' as password_age FROM snowflake.account_usage.users u JOIN snowflake.account_usage.grants_to_users g ON g.grantee_name=u.name AND g.role='ACCOUNTADMIN' AND g.deleted_on IS NULL WHERE u.ext_authn_duo=FALSE AND u.deleted_on IS NULL AND u.has_password=TRUE ORDER BY last_success_login DESC")

@st.cache_data(ttl=3600)
def q_most_privileged():
    return run_query("""
        WITH rh AS (
            SELECT grantee_name, name FROM snowflake.account_usage.grants_to_roles WHERE granted_on='ROLE' AND privilege='USAGE' AND deleted_on IS NULL
            UNION ALL SELECT 'root', r.name FROM snowflake.account_usage.roles r WHERE deleted_on IS NULL
                AND NOT EXISTS (SELECT 1 FROM snowflake.account_usage.grants_to_roles g WHERE g.granted_on='ROLE' AND g.privilege='USAGE' AND g.name=r.name AND g.deleted_on IS NULL)
        ),
        rpp AS (SELECT name, SYS_CONNECT_BY_PATH(name,' -> ') as path FROM rh CONNECT BY grantee_name=PRIOR name START WITH grantee_name='root'),
        rp  AS (SELECT name, SUBSTR(path,LEN(' -> ')) as path FROM rpp),
        rpv AS (SELECT TRIM(SPLIT(path,' -> ')[0]) as role, COUNT(*) as num_privs FROM rp LEFT JOIN snowflake.account_usage.grants_to_roles gtr ON rp.name=gtr.grantee_name AND gtr.granted_on!='ROLE' AND gtr.deleted_on IS NULL GROUP BY TRIM(SPLIT(path,' -> ')[0]))
        SELECT u.grantee_name as user_name, COUNT(rp.role) as num_roles, SUM(rp.num_privs) as num_privs
        FROM snowflake.account_usage.grants_to_users u JOIN rpv rp ON rp.role=u.role WHERE u.deleted_on IS NULL GROUP BY 1 ORDER BY 3 DESC LIMIT 20
    """)

@st.cache_data(ttl=3600)
def q_user_network_policies():
    return run_query("""WITH current_users AS (
            SELECT name, type, email FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY name ORDER BY created_on DESC) as rn FROM snowflake.account_usage.users
            ) WHERE rn=1 AND deleted_on IS NULL AND disabled=FALSE AND name!='SNOWFLAKE'),
        user_np AS (
            SELECT ref_entity_name as user_name, policy_name
            FROM snowflake.account_usage.policy_references
            WHERE policy_kind='NETWORK_POLICY' AND ref_entity_domain='USER' AND policy_status='ACTIVE')
        SELECT u.name, COALESCE(u.type,'LEGACY') as user_type, u.email,
               NVL(p.policy_name,'(account default)') as network_policy
        FROM current_users u LEFT JOIN user_np p ON u.name=p.user_name
        ORDER BY p.policy_name NULLS FIRST, u.name""")

@st.cache_data(ttl=3600)
def q_user_auth_policies():
    return run_query("""WITH current_users AS (
            SELECT name, type FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY name ORDER BY created_on DESC) as rn FROM snowflake.account_usage.users
            ) WHERE rn=1 AND deleted_on IS NULL AND disabled=FALSE AND name!='SNOWFLAKE'),
        acct_ap AS (
            SELECT policy_name FROM snowflake.account_usage.policy_references
            WHERE policy_kind='AUTHENTICATION_POLICY' AND ref_entity_domain='ACCOUNT' AND policy_status='ACTIVE' LIMIT 1),
        user_ap AS (
            SELECT ref_entity_name as user_name, policy_name
            FROM snowflake.account_usage.policy_references
            WHERE policy_kind='AUTHENTICATION_POLICY' AND ref_entity_domain='USER' AND policy_status='ACTIVE')
        SELECT u.name, COALESCE(u.type,'LEGACY') as user_type,
               NVL(p.policy_name, NVL(a.policy_name,'NONE')) as auth_policy
        FROM current_users u LEFT JOIN user_ap p ON u.name=p.user_name LEFT JOIN acct_ap a ON TRUE
        ORDER BY auth_policy, u.name""")

@st.cache_data(ttl=600)
def q_config_changes(days, cm):
    c = "end_time >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"end_time > CURRENT_TIMESTAMP - INTERVAL '{days} days'"
    return run_query(f"""SELECT query_text, user_name, role_name, end_time FROM snowflake.account_usage.query_history
        WHERE execution_status='SUCCESS' AND query_type NOT IN ('SELECT') AND {c}
          AND (query_text ILIKE '%create role%' OR query_text ILIKE '%manage grants%' OR query_text ILIKE '%create integration%'
            OR query_text ILIKE '%create share%' OR query_text ILIKE '%ownership%' OR query_text ILIKE '%drop table%'
            OR query_text ILIKE '%drop database%' OR query_text ILIKE '%create stage%' OR query_text ILIKE '%drop stage%'
            OR query_text ILIKE '%alter stage%')
        ORDER BY end_time DESC LIMIT 100""")

@st.cache_data(ttl=600)
def q_network_policy_changes(days, cm):
    c = "end_time >= DATE_TRUNC('month', CURRENT_DATE)" if cm else f"end_time > CURRENT_TIMESTAMP - INTERVAL '{days} days'"
    return run_query(f"""SELECT user_name, query_text, end_time FROM snowflake.account_usage.query_history
        WHERE execution_status='SUCCESS' AND query_type!='SELECT' AND {c}
          AND (query_type IN ('CREATE_NETWORK_POLICY','ALTER_NETWORK_POLICY','DROP_NETWORK_POLICY')
            OR query_text ILIKE '% set network_policy%' OR query_text ILIKE '% unset network_policy%')
        ORDER BY end_time DESC""")

# ── Render ─────────────────────────────────────────────────────────────────────

_TAB_LABELS = [":material/home: Overview", ":material/bar_chart: Detailed costs",
               ":material/account_balance: FinOps", ":material/speed: Warehouse performance",
               ":material/security: Security"]
_active_tab = st.radio("Section", _TAB_LABELS, horizontal=True, label_visibility="collapsed")

# ── Overview ───────────────────────────────────────────────────────────────────

if _active_tab == _TAB_LABELS[0]:
    with st.spinner("Loading overview…"):
        kpi_df         = q_kpi(days, cm)
        kpi_prior_df   = q_kpi_prior(days, cm)
        storage_kpi_df = q_storage_kpi()
        metering_df    = q_metering(trunc, days, cm)
        warehouse_df   = q_warehouse(trunc, days, cm)
        storage_trend_df = q_storage_trend(trunc, days, cm)

    total_credits = float(kpi_df["total_credits"].iloc[0]) if not kpi_df.empty and not pd.isna(kpi_df["total_credits"].iloc[0]) else 0.0
    total_tb      = float(storage_kpi_df["total_tb"].iloc[0]) if not storage_kpi_df.empty and not pd.isna(storage_kpi_df["total_tb"].iloc[0]) else 0.0

    prior_credits = float(kpi_prior_df["total_credits"].iloc[0]) if not kpi_prior_df.empty and not pd.isna(kpi_prior_df["total_credits"].iloc[0]) else None
    if prior_credits and prior_credits > 0:
        _delta_pct   = (total_credits - prior_credits) / prior_credits * 100
        _delta_label = f"{_delta_pct:+.1f}% vs prior period"
        _delta_inv   = True
    else:
        _delta_label = None
        _delta_inv   = False

    _now             = pd.Timestamp.now()
    _days_elapsed    = _now.day
    _days_in_month   = _now.days_in_month
    projected_credits = (total_credits / _days_elapsed * _days_in_month) if cm and _days_elapsed > 0 else None

    with st.container(horizontal=True):
        st.metric("Credits used", f"{total_credits:,.1f}",
                  delta=_delta_label, delta_color="inverse" if _delta_inv else "off", border=True)
        st.metric("Current storage", f"{total_tb:.2f} TB", border=True)
        if projected_credits is not None:
            st.metric("Projected month-end",
                      f"{projected_credits:,.1f} credits",
                      help="Linear extrapolation from month-to-date actuals",
                      border=True)

    with st.container(border=True):
        st.subheader("Credit usage by service type")
        if not metering_df.empty:
            st.plotly_chart(bar(metering_df, "period", "credits_used", "service_type", height=CHART_HEIGHT, tick=tick), use_container_width=True, config=_PLOTLY_CONFIG)
        else:
            st.caption("No data available.")

    col1, col2 = st.columns(2)
    with col1:
        chart_card(_top_n_other(warehouse_df, "warehouse_name", "credits_used"), bar, "period", "credits_used", "warehouse_name", title="Warehouse credits", height=SMALL_CHART_HEIGHT, tick=tick)
    with col2:
        chart_card(storage_trend_df, area, "period", "total_gb", title="Storage (GB)", height=SMALL_CHART_HEIGHT, tick=tick)

# ── Detailed costs ─────────────────────────────────────────────────────────────

elif _active_tab == _TAB_LABELS[1]:
    d1, d2, d3, d4, d5 = st.tabs([":material/robot_2: AI & Cortex", ":material/bolt: Serverless & compute", ":material/storage: Storage", ":material/sync: Transfer & replication", ":material/apps: Applications"])

    with d1:
        with st.spinner("Loading AI & Cortex costs…"):
            cortex_analyst_df   = q_cortex_analyst(trunc, days, cm)
            cortex_functions_df = q_cortex_functions(trunc, days, cm)
            cortex_search_df    = q_cortex_search(trunc, days, cm)
            doc_ai_df           = q_doc_ai(trunc, days, cm)
            sf_intelligence_df  = q_sf_intelligence(trunc, days, cm)
            cortex_agent_df     = q_cortex_agent(trunc, days, cm)
            cortex_ft_df        = q_cortex_fine_tuning(trunc, days, cm)
        col1, col2 = st.columns(2)
        with col1:  chart_card(cortex_analyst_df, bar, "period", "credits_used", title="Cortex Analyst", tick=tick)
        with col2:  chart_card(_top_n_other(cortex_functions_df, "model_name", "credits_used"), bar, "period", "credits_used", "model_name", title="Cortex Functions", tick=tick)
        col3, col4 = st.columns(2)
        with col3:  chart_card(_top_n_other(cortex_search_df, "service_name", "credits_used"), bar, "period", "credits_used", "service_name", title="Cortex Search", tick=tick)
        with col4:  chart_card(doc_ai_df, bar, "period", "credits_used", "operation_name", title="Document AI", tick=tick)
        col5, col6 = st.columns(2)
        with col5:  chart_card(_top_n_other(sf_intelligence_df, "snowflake_intelligence_name", "credits_used"), bar, "period", "credits_used", "snowflake_intelligence_name", title="Snowflake Intelligence", tick=tick)
        with col6:  chart_card(_top_n_other(cortex_agent_df, "agent_name", "credits_used"), bar, "period", "credits_used", "agent_name", title="Cortex Agents", tick=tick)
        col7, col8 = st.columns(2)
        with col7:  chart_card(_top_n_other(cortex_ft_df, "model_name", "credits_used"), bar, "period", "credits_used", "model_name", title="Cortex Fine-tuning", tick=tick)
        with col8:  st.empty()

    with d2:
        with st.spinner("Loading serverless costs…"):
            serverless_df  = q_serverless(trunc, days, cm)
            dmf_df         = q_dmf(trunc, days, cm)
            event_df       = q_event_usage(trunc, days, cm)
            spcs_df        = q_spcs(trunc, days, cm)
            pipe_df        = q_pipe_usage(trunc, days, cm)
            auto_cluster_df = q_auto_clustering(trunc, days, cm)
            mv_refresh_df  = q_mv_refresh(trunc, days, cm)
            search_opt_df  = q_search_optimization(trunc, days, cm)
            query_accel_df = q_query_acceleration(trunc, days, cm)
        col1, col2 = st.columns(2)
        with col1:  chart_card(_top_n_other(serverless_df, "task_name", "credits_used"), bar, "period", "credits_used", "task_name", title="Serverless tasks", tick=tick)
        with col2:  chart_card(_top_n_other(dmf_df, "table_name", "credits_used"), bar, "period", "credits_used", "table_name", title="Data quality (DMF)", tick=tick)
        col3, col4 = st.columns(2)
        with col3:  chart_card(event_df, bar, "period", "credits_used", title="Event usage", tick=tick)
        with col4:  chart_card(_top_n_other(spcs_df, "compute_pool_name", "credits_used"), bar, "period", "credits_used", "compute_pool_name", title="SPCS", tick=tick)
        col5, col6 = st.columns(2)
        with col5:  chart_card(_top_n_other(pipe_df, "pipe_name", "credits_used"), bar, "period", "credits_used", "pipe_name", title="Snowpipe", tick=tick)
        with col6:  chart_card(_top_n_other(auto_cluster_df, "table_name", "credits_used"), bar, "period", "credits_used", "table_name", title="Automatic clustering", tick=tick)
        col7, col8 = st.columns(2)
        with col7:  chart_card(_top_n_other(mv_refresh_df, "table_name", "credits_used"), bar, "period", "credits_used", "table_name", title="Materialized view refresh", tick=tick)
        with col8:  chart_card(_top_n_other(search_opt_df, "table_name", "credits_used"), bar, "period", "credits_used", "table_name", title="Search optimization", tick=tick)
        col9, col10 = st.columns(2)
        with col9:  chart_card(_top_n_other(query_accel_df, "warehouse_name", "credits_used"), bar, "period", "credits_used", "warehouse_name", title="Query acceleration", tick=tick)
        with col10: st.empty()

    with d3:
        with st.spinner("Loading storage costs…"):
            storage_db_df    = q_storage_by_db(trunc, days, cm)
            stage_storage_df = q_stage_storage(trunc, days, cm)
            schemas_df       = q_largest_schemas()
            tables_df        = q_largest_tables()
        col1, col2 = st.columns(2)
        with col1:  chart_card(_top_n_other(storage_db_df, "database_name", "avg_bytes"), area, "period", "avg_bytes", "database_name", title="Storage by database (bytes)", tick=tick)
        with col2:  chart_card(stage_storage_df, area, "period", "stage_gb", title="Stage storage (GB)", tick=tick)
        col3, col4 = st.columns(2)
        with col3:  chart_card(schemas_df, hbar, "total_bytes", "schema_name", title="Largest schemas (bytes)")
        with col4:  chart_card(tables_df, hbar, "total_bytes", "table_name", title="Largest tables (bytes)")

    with d4:
        with st.spinner("Loading transfer & replication…"):
            data_transfer_df = q_data_transfer(trunc, days, cm)
            replication_df   = q_replication(trunc, days, cm)
        col1, col2 = st.columns(2)
        with col1:  chart_card(data_transfer_df, bar, "period", "gb_transferred", "transfer_type", title="Data transfer (GB)", tick=tick)
        with col2:  chart_card(_top_n_other(replication_df, "replication_group_name", "credits_used"), bar, "period", "credits_used", "replication_group_name", title="Replication credits", tick=tick)

    with d5:
        with st.spinner("Loading application costs…"):
            application_df = q_application(trunc, days, cm)
        chart_card(application_df, bar, "period", "credits_used", "application_name", title="Application credits", tick=tick)

# ── FinOps ─────────────────────────────────────────────────────────────────────

elif _active_tab == _TAB_LABELS[2]:
    st.caption(":material/info: Credit attribution is estimated by proportioning warehouse credits across queries by execution time. These are not actual billed costs.")
    with st.spinner("Loading FinOps attribution…"):
        finops_df = q_finops_combined(trunc, days, cm)

    if not finops_df.empty:
        role_df       = finops_df.groupby(["role_name", "period"], as_index=False)["attributed_credits"].sum()
        query_type_df = finops_df.groupby(["query_type", "period"], as_index=False)["attributed_credits"].sum()
        user_df       = finops_df.groupby(["user_name", "period"], as_index=False)["attributed_credits"].sum()
        client_df     = finops_df.groupby(["client", "period"], as_index=False)["attributed_credits"].sum()
    else:
        role_df = query_type_df = user_df = client_df = pd.DataFrame()

    col1, col2 = st.columns(2)
    with col1:
        chart_card(_top_n_other(role_df, "role_name", "attributed_credits"), bar, "period", "attributed_credits", "role_name", title="Estimated credits by role", tick=tick)
    with col2:
        chart_card(_top_n_other(query_type_df, "query_type", "attributed_credits"), bar, "period", "attributed_credits", "query_type", title="Estimated credits by query type", tick=tick)
    col3, col4 = st.columns(2)
    with col3:
        chart_card(_top_n_other(user_df, "user_name", "attributed_credits"), bar, "period", "attributed_credits", "user_name", title="Estimated credits by user", tick=tick)
    with col4:
        chart_card(_top_n_other(client_df, "client", "attributed_credits"), bar, "period", "attributed_credits", "client", title="Estimated credits by client type", tick=tick)

# ── Warehouse performance ──────────────────────────────────────────────────────

elif _active_tab == _TAB_LABELS[3]:
    with st.spinner("Loading warehouse performance…"):
        wh_perf_df = q_wh_perf_combined(trunc, days, cm)
        top_cost_df = q_top_cost_queries(days, cm)

    if not wh_perf_df.empty:
        queuing_trend_df = wh_perf_df.groupby(["period", "warehouse_name"], as_index=False).agg(
            queued_count=("queued_count", "sum"),
            avg_overload_wait_sec=("avg_overload_wait_sec", "mean"))
        queuing_summary_df = wh_perf_df.groupby(["warehouse_name", "warehouse_size"], as_index=False).agg(
            total_queries=("total_queries", "sum"),
            queued_queries=("queued_count", "sum"))
        queuing_summary_df["pct_queued"] = (100.0 * queuing_summary_df["queued_queries"] / queuing_summary_df["total_queries"]).round(1)
        queuing_summary_df["avg_wait_sec"] = wh_perf_df.groupby(["warehouse_name", "warehouse_size"])["avg_overload_wait_sec"].mean().values
        queuing_summary_df = queuing_summary_df.sort_values("pct_queued", ascending=False)
        spilling_df = wh_perf_df.loc[wh_perf_df["remote_spill_gb"] > 0].groupby(
            ["warehouse_name", "warehouse_size"], as_index=False).agg(
            spilling_queries=("spilling_queries", "sum"),
            remote_spill_gb=("remote_spill_gb", "sum"),
            local_spill_gb=("local_spill_gb", "sum")).sort_values("remote_spill_gb", ascending=False)
    else:
        queuing_trend_df = queuing_summary_df = spilling_df = pd.DataFrame()

    st.subheader(":material/schedule: Queuing")
    col1, col2 = st.columns(2)
    with col1:
        chart_card(_top_n_other(queuing_trend_df, "warehouse_name", "queued_count"), bar, "period", "queued_count", "warehouse_name", title="Queued queries over time", tick=tick)
    with col2:
        chart_card(_top_n_other(queuing_trend_df, "warehouse_name", "avg_overload_wait_sec"), bar, "period", "avg_overload_wait_sec", "warehouse_name", title="Avg overload wait (seconds)", tick=tick)

    if not queuing_summary_df.empty:
        with st.container(border=True):
            st.subheader("Queuing summary by warehouse")
            st.dataframe(queuing_summary_df, hide_index=True, use_container_width=True, column_config={
                "warehouse_name": "Warehouse", "warehouse_size": "Size",
                "total_queries":  st.column_config.NumberColumn("Total queries", format="%d"),
                "queued_queries": st.column_config.NumberColumn("Queued queries", format="%d"),
                "pct_queued":     st.column_config.NumberColumn("% queued", format="%.1f%%"),
                "avg_wait_sec":   st.column_config.NumberColumn("Avg wait (s)", format="%.2f"),
            })

    st.subheader(":material/storage: Remote storage spilling")
    if not spilling_df.empty:
        with st.container(border=True):
            st.dataframe(spilling_df, hide_index=True, use_container_width=True, column_config={
                "warehouse_name":  "Warehouse", "warehouse_size": "Size",
                "spilling_queries": st.column_config.NumberColumn("Spilling queries", format="%d"),
                "remote_spill_gb": st.column_config.NumberColumn("Remote spill (GB)", format="%.2f"),
                "local_spill_gb":  st.column_config.NumberColumn("Local spill (GB)", format="%.2f"),
            })
    else:
        st.caption("No remote storage spilling detected in this period.")

    st.subheader(":material/query_stats: Top 10 credit-consuming queries")
    st.caption(":material/info: Credits are estimated by proportioning warehouse credits across queries by execution time.")
    if not top_cost_df.empty:
        with st.container(border=True):
            st.dataframe(top_cost_df, hide_index=True, use_container_width=True, column_config={
                "execution_count":    st.column_config.NumberColumn("Executions", format="%d"),
                "avg_exec_seconds":   st.column_config.NumberColumn("Avg exec (s)", format="%.2f"),
                "user_name":          st.column_config.TextColumn("User"),
                "role_name":          st.column_config.TextColumn("Role"),
                "warehouse_name":     st.column_config.TextColumn("Warehouse"),
                "sample_query_id":    st.column_config.TextColumn("Sample query ID", width="small"),
                "sample_query":       st.column_config.TextColumn("Query (sample)", width="large"),
                "attributed_credits": st.column_config.NumberColumn("Credits", format="%.4f"),
            })
    else:
        st.caption("No query data available for this period.")

# ── Security ───────────────────────────────────────────────────────────────────

elif _active_tab == _TAB_LABELS[4]:
    with st.spinner("Loading security data…"):
        user_base_df = q_user_base()

    now_ts = pd.Timestamp.now(tz="UTC")

    def _to_utc(val):
        ts = pd.Timestamp(val)
        return ts if ts.tzinfo is not None else ts.tz_localize("UTC")

    def _derive_stale(df, threshold):
        if df.empty:
            return pd.DataFrame()
        d = df.copy()
        d["days_since_login"] = d.apply(
            lambda r: (now_ts - _to_utc(r["last_success_login"] if pd.notna(r["last_success_login"]) else r["created_on"])).days, axis=1)
        return d.loc[d["days_since_login"] >= threshold, ["name", "days_since_login"]].sort_values("days_since_login", ascending=False).head(50)

    def _derive_old_pwd(df):
        if df.empty:
            return pd.DataFrame()
        d = df.loc[df["password_last_set_time"].notna()].copy()
        d["password_age_days"] = d["password_last_set_time"].apply(lambda x: (now_ts - _to_utc(x)).days)
        return d[["name", "password_age_days"]].sort_values("password_age_days", ascending=False).head(30)

    def _derive_human_no_mfa(df):
        if df.empty:
            return pd.DataFrame()
        mask = ((df["type"].isna()) | (df["type"].isin(["PERSON", ""]))) & (df["has_password"] == True) & (df["has_mfa"] == False)
        d = df.loc[mask].copy()
        d["days_since_login"] = d.apply(
            lambda r: (now_ts - _to_utc(r["last_success_login"] if pd.notna(r["last_success_login"]) else r["created_on"])).days, axis=1)
        return d[["name", "email", "has_rsa_public_key", "days_since_login"]].sort_values("days_since_login")

    def _derive_svc_pwd(df):
        if df.empty:
            return pd.DataFrame()
        mask = (df["type"] == "SERVICE") & (df["has_password"] == True)
        return df.loc[mask, ["name", "has_rsa_public_key", "default_role"]].sort_values("name")

    def _derive_default_aa(df):
        if df.empty:
            return pd.DataFrame()
        mask = df["default_role"] == "ACCOUNTADMIN"
        d = df.loc[mask].copy()
        d["user_type"] = d["type"].fillna("LEGACY")
        return d[["name", "user_type", "email"]].sort_values("name")

    def _derive_legacy_type(df):
        if df.empty:
            return pd.DataFrame()
        mask = df["type"].isna() | (df["type"] == "")
        return df.loc[mask, ["name", "email", "has_password", "has_rsa_public_key", "has_mfa"]].sort_values("name")

    stale_df       = _derive_stale(user_base_df, stale_threshold)
    old_pwd_df     = _derive_old_pwd(user_base_df)
    human_no_mfa_df = _derive_human_no_mfa(user_base_df)
    svc_pwd_df     = _derive_svc_pwd(user_base_df)
    default_aa_df  = _derive_default_aa(user_base_df)
    legacy_type_df = _derive_legacy_type(user_base_df)

    s1, s2, s3, s4, s5, s6 = st.tabs([
        ":material/lock: Authentication",
        ":material/admin_panel_settings: Privileged access",
        ":material/person: Identity management",
        ":material/shield: Least privilege",
        ":material/verified_user: Posture",
        ":material/build: Configuration",
    ])

    with s1:
        with st.spinner("Loading auth data…"):
            failures_trend_df  = q_login_failures_trend(trunc, days, cm)
            failures_detail_df = q_login_failures_detail(days, cm)
            auth_methods_df    = q_auth_methods(days, cm)
        col1, col2 = st.columns(2)
        with col1:
            chart_card(_top_n_other(failures_trend_df, "error_message", "failures"), bar, "period", "failures", "error_message", title="Login failures over time", tick=tick)
        with col2:
            chart_card(auth_methods_df, hbar, "login_count", "auth_method", title="Successful logins by auth method")
        if not failures_detail_df.empty:
            with st.container(border=True):
                st.subheader("Login failures by user and reason")
                st.dataframe(failures_detail_df, hide_index=True, use_container_width=True, column_config={
                    "user_name": "User", "error_message": "Reason",
                    "num_failures": st.column_config.NumberColumn("Failures", format="%d"),
                })

    with s2:
        with st.spinner("Loading privileged access…"):
            aa_grants_df = q_aa_grants(days, cm)
            aa_no_mfa_df = q_aa_no_mfa()
        if not aa_grants_df.empty:
            with st.container(border=True):
                st.subheader("ACCOUNTADMIN grants")
                st.dataframe(aa_grants_df, hide_index=True, use_container_width=True, column_config={
                    "user_name":  "Granted by",
                    "query_text": st.column_config.TextColumn("Statement", width="large"),
                    "end_time":   st.column_config.DatetimeColumn("Time"),
                })
        else:
            st.caption("No ACCOUNTADMIN grants in this period.")
        if not aa_no_mfa_df.empty:
            with st.container(border=True):
                st.subheader("ACCOUNTADMIN users without MFA")
                st.dataframe(aa_no_mfa_df, hide_index=True, use_container_width=True, column_config={
                    "name": "User", "last_login": "Last login", "password_age": "Password age",
                })
        else:
            st.caption("All ACCOUNTADMIN users have MFA enabled.")

    with s3:
        col1, col2 = st.columns(2)
        with col1:
            if not stale_df.empty:
                with st.container(border=True):
                    st.subheader(f"Stale users (inactive \u2265 {stale_threshold} days)")
                    st.dataframe(stale_df, hide_index=True, use_container_width=True, column_config={
                        "name": "User",
                        "days_since_login": st.column_config.NumberColumn("Days since login", format="%d"),
                    })
        with col2:
            if not old_pwd_df.empty:
                with st.container(border=True):
                    st.subheader("Oldest passwords")
                    st.dataframe(old_pwd_df, hide_index=True, use_container_width=True, column_config={
                        "name": "User",
                        "password_age_days": st.column_config.NumberColumn("Password age (days)", format="%d"),
                    })

    with s4:
        with st.spinner("Loading privilege data…"):
            privileged_df = q_most_privileged()
        if not privileged_df.empty:
            with st.container(border=True):
                st.subheader("Most privileged users")
                st.dataframe(privileged_df, hide_index=True, use_container_width=True, column_config={
                    "user_name": "User",
                    "num_roles": st.column_config.NumberColumn("Roles", format="%d"),
                    "num_privs": st.column_config.NumberColumn("Total privileges", format="%d"),
                })

    with s5:
        col1, col2 = st.columns(2)
        with col1:
            if not human_no_mfa_df.empty:
                with st.container(border=True):
                    st.subheader(":material/warning: Human users with password but no MFA")
                    st.dataframe(human_no_mfa_df, hide_index=True, use_container_width=True, column_config={
                        "name": "User", "email": "Email",
                        "has_rsa_public_key": st.column_config.CheckboxColumn("RSA key"),
                        "days_since_login": st.column_config.NumberColumn("Days since login", format="%d"),
                    })
            else:
                st.caption(":material/check_circle: All human users with passwords have MFA enabled.")
        with col2:
            if not svc_pwd_df.empty:
                with st.container(border=True):
                    st.subheader(":material/warning: Service accounts using password auth")
                    st.caption("Service accounts should use key-pair authentication.")
                    st.dataframe(svc_pwd_df, hide_index=True, use_container_width=True, column_config={
                        "name": "User",
                        "has_rsa_public_key": st.column_config.CheckboxColumn("RSA key"),
                        "default_role": "Default role",
                    })
            else:
                st.caption(":material/check_circle: No service accounts using password authentication.")
        col3, col4 = st.columns(2)
        with col3:
            if not default_aa_df.empty:
                with st.container(border=True):
                    st.subheader(":material/warning: Users with default role ACCOUNTADMIN")
                    st.dataframe(default_aa_df, hide_index=True, use_container_width=True, column_config={
                        "name": "User", "user_type": "Type", "email": "Email",
                    })
            else:
                st.caption(":material/check_circle: No users have ACCOUNTADMIN as their default role.")
        with col4:
            if not legacy_type_df.empty:
                with st.container(border=True):
                    st.subheader(":material/info: Users without a user type set")
                    st.caption("Users should have TYPE set to PERSON or SERVICE.")
                    st.dataframe(legacy_type_df, hide_index=True, use_container_width=True, column_config={
                        "name": "User", "email": "Email",
                        "has_password": st.column_config.CheckboxColumn("Password"),
                        "has_rsa_public_key": st.column_config.CheckboxColumn("RSA key"),
                        "has_mfa": st.column_config.CheckboxColumn("MFA"),
                    })
            else:
                st.caption(":material/check_circle: All users have a TYPE set.")
        with st.spinner("Loading policy assignments…"):
            user_net_pol_df  = q_user_network_policies()
            user_auth_pol_df = q_user_auth_policies()
        if not user_net_pol_df.empty:
            with st.container(border=True):
                st.subheader("Network policy assignments")
                st.dataframe(user_net_pol_df, hide_index=True, use_container_width=True, column_config={
                    "name": "User", "user_type": "Type", "email": "Email", "network_policy": "Network policy",
                })
        if not user_auth_pol_df.empty:
            with st.container(border=True):
                st.subheader("Authentication policy assignments")
                st.dataframe(user_auth_pol_df, hide_index=True, use_container_width=True, column_config={
                    "name": "User", "user_type": "Type", "auth_policy": "Auth policy",
                })

    with s6:
        with st.spinner("Loading config changes…"):
            config_df  = q_config_changes(days, cm)
            network_df = q_network_policy_changes(days, cm)
        if not config_df.empty:
            with st.container(border=True):
                st.subheader("Privileged object changes")
                st.dataframe(config_df, hide_index=True, use_container_width=True, column_config={
                    "query_text": st.column_config.TextColumn("Statement", width="large"),
                    "user_name": "User", "role_name": "Role",
                    "end_time": st.column_config.DatetimeColumn("Time"),
                })
        else:
            st.caption("No privileged object changes in this period.")
        if not network_df.empty:
            with st.container(border=True):
                st.subheader("Network policy changes")
                st.dataframe(network_df, hide_index=True, use_container_width=True, column_config={
                    "user_name": "User",
                    "query_text": st.column_config.TextColumn("Statement", width="large"),
                    "end_time": st.column_config.DatetimeColumn("Time"),
                })
        else:
            st.caption("No network policy changes in this period.")
