import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from src.database import fetch_history, create_tables, fault_counts

st.set_page_config(page_title="Dashboard · PredictMaint", page_icon="📊", layout="wide")

st.markdown("""
<style>
.main{background:#0A1628;}
h1,h2,h3{color:#F1F5F9!important;}
.stMetric{background:#112240!important;border-radius:12px!important;border:1px solid #1E3A5F!important;}
.stMetric label{color:#94A3B8!important;font-size:12px!important;}
.stMetric [data-testid="stMetricValue"]{color:#F1F5F9!important;}
div[data-testid="metric-container"]{background:#112240;border-radius:12px;border:1px solid #1E3A5F;padding:16px;}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div style="display:flex;align-items:center;gap:12px;margin-bottom:1.5rem;">
  <span style="font-size:28px;">📊</span>
  <div>
    <h1 style="margin:0;font-size:24px;color:#F1F5F9;">Machine Health Dashboard</h1>
    <p style="margin:0;color:#64748B;font-size:13px;">Prediction history · Fault detection · Sensor analytics</p>
  </div>
</div>
""", unsafe_allow_html=True)

create_tables()

try:
    df = fetch_history()

    if df.empty:
        st.markdown("""
        <div style="background:#112240;border-radius:12px;border:1px solid #1E3A5F;
             padding:3rem;text-align:center;">
          <div style="font-size:48px;margin-bottom:1rem;">🔍</div>
          <h3 style="color:#F1F5F9;">No predictions yet</h3>
          <p style="color:#64748B;">Go to the Predict page to start monitoring machine health.</p>
        </div>
        """, unsafe_allow_html=True)
    else:
        total    = len(df)
        failures = (df['final_status'] == 'FAILURE').sum()
        normal   = total - failures
        rate     = failures / total * 100

        # KPI row
        c1,c2,c3,c4 = st.columns(4)
        c1.metric("Total Predictions", total)
        c2.metric("Normal", normal, delta=f"{100-rate:.1f}% healthy")
        c3.metric("Failures Detected", failures, delta=f"{rate:.1f}% rate", delta_color="inverse")
        c4.metric("Most Common Fault", next(iter(fault_counts(df)), "None"))

        st.markdown("<br>", unsafe_allow_html=True)

        # Charts row 1
        col1, col2 = st.columns([1, 2])

        with col1:
            st.markdown("""<div style="background:#112240;border-radius:12px;
                border:1px solid #1E3A5F;padding:1rem;margin-bottom:1rem;">
                <p style="color:#94A3B8;font-size:12px;margin:0 0 8px;font-weight:600;
                letter-spacing:1px;">HEALTH STATUS</p>""", unsafe_allow_html=True)
            fig_pie = go.Figure(go.Pie(
                labels=['Normal', 'Failure'],
                values=[normal, failures],
                hole=0.65,
                marker_colors=['#10B981', '#EF4444'],
                textinfo='none'
            ))
            fig_pie.update_layout(
                showlegend=True, height=220,
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font_color='#94A3B8',
                margin=dict(t=0,b=0,l=0,r=0),
                legend=dict(font=dict(color='#94A3B8', size=12))
            )
            fig_pie.add_annotation(text=f"{100-rate:.0f}%<br><span style='font-size:10px'>Normal</span>",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=20, color='#10B981'))
            st.plotly_chart(fig_pie, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

        with col2:
            st.markdown("""<div style="background:#112240;border-radius:12px;
                border:1px solid #1E3A5F;padding:1rem;">
                <p style="color:#94A3B8;font-size:12px;margin:0 0 8px;font-weight:600;
                letter-spacing:1px;">PREDICTIONS TIMELINE</p>""", unsafe_allow_html=True)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['status_num'] = (df['final_status'] == 'FAILURE').astype(int)
            fig_time = go.Figure()
            fig_time.add_trace(go.Scatter(
                x=df['timestamp'], y=df['status_num'],
                mode='markers',
                marker=dict(
                    color=df['status_num'].map({0:'#10B981', 1:'#EF4444'}),
                    size=10, line=dict(width=1, color='#0D1B2A')
                ),
                hovertemplate='%{x}<br>Status: %{text}<extra></extra>',
                text=df['final_status']
            ))
            fig_time.update_layout(
                height=220, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                font_color='#94A3B8', margin=dict(t=0,b=30,l=0,r=0),
                xaxis=dict(gridcolor='#1E3A5F', color='#64748B'),
                yaxis=dict(gridcolor='#1E3A5F', color='#64748B',
                          tickvals=[0,1], ticktext=['Normal','Failure'])
            )
            st.plotly_chart(fig_time, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # Sensor trends
        st.markdown("""<p style="color:#94A3B8;font-size:12px;font-weight:600;
            letter-spacing:1px;">SENSOR TRENDS</p>""", unsafe_allow_html=True)

        col1, col2, col3 = st.columns(3)
        sensor_configs = [
            ('air_temp', 'Air Temperature (K)', '#0EA5E9', col1),
            ('torque', 'Torque (Nm)', '#F59E0B', col2),
            ('tool_wear', 'Tool Wear (min)', '#EF4444', col3),
        ]

        for col_name, label, color, col in sensor_configs:
            if col_name in df.columns:
                with col:
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=df['timestamp'], y=df[col_name],
                        mode='lines', line=dict(color=color, width=2),
                        fill='tozeroy',
                        fillcolor=color.replace(')', ',0.1)').replace('rgb','rgba') if 'rgb' in color else color+'19'
                    ))
                    fig.update_layout(
                        title=dict(text=label, font=dict(color='#94A3B8', size=12)),
                        height=180, paper_bgcolor='rgba(0,0,0,0)',
                        plot_bgcolor='rgba(0,0,0,0)', font_color='#64748B',
                        margin=dict(t=30,b=20,l=30,r=10),
                        xaxis=dict(gridcolor='#1E3A5F', showticklabels=False),
                        yaxis=dict(gridcolor='#1E3A5F')
                    )
                    st.plotly_chart(fig, use_container_width=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # Fault breakdown
        if failures > 0:
            st.markdown("""<p style="color:#94A3B8;font-size:12px;font-weight:600;
                letter-spacing:1px;">FAULT TYPE BREAKDOWN</p>""", unsafe_allow_html=True)
            fault_df = pd.Series(fault_counts(df)).rename_axis('Fault Type').reset_index(name='Count')
            fig_fault = go.Figure(go.Bar(
                x=fault_df['Count'], y=fault_df['Fault Type'],
                orientation='h',
                marker_color=['#EF4444','#F59E0B','#8B5CF6','#0EA5E9','#10B981'][:len(fault_df)],
                text=fault_df['Count'], textposition='inside'
            ))
            fig_fault.update_layout(
                height=200, paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)', font_color='#94A3B8',
                margin=dict(t=0,b=20,l=10,r=10),
                xaxis=dict(gridcolor='#1E3A5F'),
                yaxis=dict(gridcolor='#1E3A5F')
            )
            st.plotly_chart(fig_fault, use_container_width=True)

        # Recent table
        st.markdown("""<p style="color:#94A3B8;font-size:12px;font-weight:600;
            letter-spacing:1px;margin-top:1rem;">RECENT PREDICTIONS</p>""", unsafe_allow_html=True)
        display_cols = ['timestamp','air_temp','process_temp','torque','tool_wear','final_status','fault_type']
        display_cols = [c for c in display_cols if c in df.columns]
        st.dataframe(
            df[display_cols].head(10).style.map(
                lambda v: 'color:#EF4444;font-weight:600' if str(v) == 'FAILURE' else 'color:#10B981;font-weight:600',
                subset=['final_status'] if 'final_status' in display_cols else []
            ),
            use_container_width=True, height=300
        )

except Exception as e:
    st.error(f"Error: {e}")