import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from src.database import fetch_history, create_tables, fault_counts

st.set_page_config(page_title="History · PredictMaint", page_icon="📋", layout="wide")
st.markdown("""
<style>
.main{background:#0A1628;}
h1,h2,h3{color:#F1F5F9!important;}
div[data-testid="metric-container"]{background:#112240;border-radius:12px;
    border:1px solid #1E3A5F;padding:16px;}
.stMetric label{color:#94A3B8!important;font-size:12px!important;}
.stMetric [data-testid="stMetricValue"]{color:#F1F5F9!important;}
</style>""", unsafe_allow_html=True)

st.markdown("""
<div style="display:flex;align-items:center;gap:12px;margin-bottom:1.5rem;">
  <span style="font-size:28px;">📋</span>
  <div>
    <h1 style="margin:0;font-size:24px;color:#F1F5F9;">Prediction History</h1>
    <p style="margin:0;color:#64748B;font-size:13px;">Predictions stored in the local SQLite database (legacy demo store)</p>
  </div>
</div>
""", unsafe_allow_html=True)

create_tables()

try:
    df = fetch_history()

    if df.empty:
        st.info("No prediction history yet. Go to Predict page to start.")
    else:
        total    = len(df)
        failures = (df['final_status'] == 'FAILURE').sum()
        normal   = total - failures

        c1,c2,c3,c4 = st.columns(4)
        c1.metric("Total Records", total)
        c2.metric("Normal", normal)
        c3.metric("Failures", failures)
        c4.metric("Model Agreement",
                  f"{((df['rf_prediction']==df['xgb_prediction'])&(df['xgb_prediction']==df['iso_prediction'])).sum()}/{total}")

        st.markdown("<br>", unsafe_allow_html=True)

        # Filters
        col1, col2 = st.columns([1, 3])
        with col1:
            status_filter = st.selectbox("Filter by status", ["All","NORMAL","FAILURE"])
        with col2:
            fault_options = ["All"] + list(fault_counts(df))
            fault_filter = st.selectbox("Filter by fault type", fault_options)

        filtered = df.copy()
        if status_filter != "All":
            filtered = filtered[filtered['final_status'] == status_filter]
        if fault_filter != "All":
            filtered = filtered[filtered['fault_type'].str.contains(fault_filter, regex=False)]

        st.markdown(f"""<p style="color:#64748B;font-size:13px;">
            Showing {len(filtered)} of {total} records</p>""", unsafe_allow_html=True)

        # Styled table
        display_cols = ['timestamp','air_temp','process_temp','rotational_speed',
                        'torque','tool_wear','final_status','fault_type']
        display_cols = [c for c in display_cols if c in filtered.columns]

        def style_status(val):
            if str(val) == 'FAILURE': return 'color:#EF4444;font-weight:600'
            if str(val) == 'NORMAL':  return 'color:#10B981;font-weight:600'
            return ''

        styled = filtered[display_cols].style.map(style_status, subset=['final_status'])
        st.dataframe(styled, use_container_width=True, height=350)

        st.markdown("<br>", unsafe_allow_html=True)

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("""<p style="color:#94A3B8;font-size:12px;font-weight:600;
                letter-spacing:1px;">FAULT DISTRIBUTION</p>""", unsafe_allow_html=True)
            fault_series = pd.Series(fault_counts(df)).sort_values(ascending=False)
            if not fault_series.empty:
                fig = go.Figure(go.Bar(
                    x=fault_series.values, y=fault_series.index,
                    orientation='h',
                    marker_color=['#EF4444','#F59E0B','#8B5CF6','#0EA5E9','#10B981'][:len(fault_series)]
                ))
                fig.update_layout(height=250, paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)', font_color='#94A3B8',
                    margin=dict(t=0,b=0,l=0,r=0),
                    xaxis=dict(gridcolor='#1E3A5F'),
                    yaxis=dict(gridcolor='#1E3A5F'))
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No faults recorded yet.")

        with col2:
            st.markdown("""<p style="color:#94A3B8;font-size:12px;font-weight:600;
                letter-spacing:1px;">MODEL AGREEMENT</p>""", unsafe_allow_html=True)
            agree = ((df['rf_prediction']==df['xgb_prediction']) &
                     (df['xgb_prediction']==df['iso_prediction'])).sum()
            disagree = total - agree
            fig2 = go.Figure(go.Pie(
                labels=['All Agreed','Disagreed'],
                values=[agree, disagree],
                hole=0.6,
                marker_colors=['#10B981','#F59E0B']
            ))
            fig2.update_layout(height=250, paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)', font_color='#94A3B8',
                margin=dict(t=0,b=0,l=0,r=0),
                legend=dict(font=dict(color='#94A3B8')))
            st.plotly_chart(fig2, use_container_width=True)

        # Download
        st.markdown("<br>", unsafe_allow_html=True)
        csv = filtered.to_csv(index=False)
        st.download_button("⬇️ Download as CSV", csv,
                           "prediction_history.csv", "text/csv",
                           use_container_width=True)

except Exception as e:
    st.error(f"Database error: {e}")