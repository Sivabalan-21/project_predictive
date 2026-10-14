import os
import sys

import pandas as pd
import streamlit as st

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from ui.api_client import ApiClient, ApiError

st.set_page_config(page_title="Live Monitoring · PredictMaint", page_icon="📡", layout="wide")

st.markdown("""
<style>
.main{background:#0A1628;}
h1,h2,h3{color:#F1F5F9!important;}
.stMetric{background:#112240!important;border-radius:12px!important;border:1px solid #1E3A5F!important;}
.stMetric label{color:#94A3B8!important;font-size:12px!important;}
.stMetric [data-testid="stMetricValue"]{color:#F1F5F9!important;}
div[data-testid="metric-container"]{background:#112240;border-radius:12px;border:1px solid #1E3A5F;padding:16px;}
</style>
<div style="display:flex;align-items:center;gap:12px;margin-bottom:1rem;">
  <span style="font-size:28px;">📡</span>
  <div>
    <h1 style="margin:0;font-size:24px;color:#F1F5F9;">Live Monitoring</h1>
    <p style="margin:0;color:#64748B;font-size:13px;">Predictions and alerts from the FastAPI backend (PostgreSQL)</p>
  </div>
</div>
""", unsafe_allow_html=True)

if "api_client" not in st.session_state:
    st.session_state["api_client"] = ApiClient()
client: ApiClient = st.session_state["api_client"]


def _show_error(err: ApiError) -> None:
    if err.kind == "unauthorized":
        client.logout()
        st.warning("Session expired or not signed in. Please sign in again.")
    elif err.kind == "forbidden":
        st.error(f"Your role is not allowed to view this data: {err.detail}")
    elif err.kind in ("unavailable", "timeout"):
        st.error(f"API unreachable: {err.detail}")
    elif err.kind == "rate_limited":
        st.error("Too many attempts. Wait a moment and try again.")
    else:
        st.error(f"API error ({err.kind}): {err.detail}")


# ---- connection status (public endpoints; no token needed) -----------------------------------------------------
st.subheader("Connection")
try:
    client.health()
    ready = client.ready()
    if ready.status == "ready":
        st.success(f"API reachable at {client.base_url} · database: {ready.database} · kafka: {ready.kafka}")
    else:
        st.warning(f"API reachable at {client.base_url} but NOT ready · database: {ready.database} · kafka: {ready.kafka}. "
                   "Data below may be unavailable.")
except ApiError as err:
    _show_error(err)
    st.info("Start the API (see README) or set API_BASE_URL, then refresh this page.")
    st.stop()

# ---- sign in ---------------------------------------------------------------------------------------------------
if not client.is_authenticated:
    with st.form("login"):
        user = st.text_input("Username")
        pwd = st.text_input("Password", type="password")
        if st.form_submit_button("Sign in") and user and pwd:
            try:
                client.login(user, pwd)
                st.rerun()
            except ApiError as err:
                _show_error(err)
    st.stop()

top = st.columns([1, 1, 1, 3])
if top[0].button("Refresh"):
    st.rerun()
auto = top[1].toggle("Auto-refresh (10 s)", value=False)
if top[2].button("Sign out"):
    client.logout()
    st.rerun()
machine = st.text_input("Filter by machine ID (optional)", value="").strip() or None


def render() -> None:
    try:
        preds, ppage = client.predictions(page=1, page_size=50, machine_id=machine)
        alerts, apage = client.alerts(status="ACTIVE", page=1, page_size=50)
    except ApiError as err:
        _show_error(err)
        return
    latest = preds[0] if preds else None
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Predictions stored", f"{ppage.total:,}")
    c2.metric("Active alerts", f"{apage.total:,}")
    c3.metric("Latest status", latest.prediction if latest else "—")
    c4.metric("Latest risk score", f"{latest.risk_score:.2f}" if latest else "—")
    st.subheader("Active alerts")
    if alerts:
        st.dataframe(pd.DataFrame([a.__dict__ for a in alerts]).drop(columns=["resolved_at"]), use_container_width=True,
                     hide_index=True)
    else:
        st.info("No active alerts.")
    st.subheader(f"Latest predictions (showing {len(preds)} of {ppage.total:,})")
    if preds:
        st.dataframe(pd.DataFrame([p.__dict__ for p in preds]), use_container_width=True, hide_index=True)
    else:
        st.info("No predictions found. Data appears once the Kafka consumer ingests sensor readings.")
    st.caption("risk_score is the mean RF/XGBoost failure probability, not a calibrated probability.")


st.fragment(run_every=10 if auto else None)(render)()
