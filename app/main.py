import os
import sys

import streamlit as st

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from ml.reporting import headline, load_metrics

st.set_page_config(
    page_title="PredictMaint AI",
    page_icon="⚙️",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
[data-testid="stSidebar"] {
    background: #0D1B2A;
    border-right: 1px solid #1E3A5F;
}
[data-testid="stSidebar"] * { color: #CBD5E1 !important; }
[data-testid="stSidebar"] .st-emotion-cache-1cypcdb { color: #38BDF8 !important; }
.main { background: #0A1628; }
.stMetric { background: #112240 !important; border-radius: 12px !important;
            border: 1px solid #1E3A5F !important; padding: 1rem !important; }
.stMetric label { color: #94A3B8 !important; font-size: 12px !important; }
.stMetric [data-testid="stMetricValue"] { color: #F1F5F9 !important; font-size: 28px !important; }
h1,h2,h3 { color: #F1F5F9 !important; }
.stMarkdown p { color: #CBD5E1; }
div[data-testid="metric-container"] { background: #112240; border-radius: 12px;
    border: 1px solid #1E3A5F; padding: 16px; }
</style>
""", unsafe_allow_html=True)

# Hero section
st.markdown("""
<div style="background:linear-gradient(135deg,#0D1B2A 0%,#112240 100%);
     border-radius:16px;border:1px solid #1E3A5F;padding:2.5rem 2rem;margin-bottom:1.5rem;">
  <div style="display:flex;align-items:center;gap:16px;margin-bottom:1rem;">
    <div style="background:#0EA5E9;border-radius:12px;width:48px;height:48px;
         display:flex;align-items:center;justify-content:center;font-size:24px;">⚙️</div>
    <div>
      <h1 style="margin:0;font-size:28px;font-weight:700;color:#F1F5F9;">
        PredictMaint <span style="color:#0EA5E9;">AI</span></h1>
      <p style="margin:0;color:#64748B;font-size:14px;">
        Predictive Maintenance System · AI4I 2020 Dataset · UCI Repository</p>
    </div>
  </div>
  <p style="color:#94A3B8;font-size:15px;line-height:1.7;max-width:700px;margin:0;">
    An intelligent machine learning system that analyses temperature, speed, torque and tool-wear 
    sensor data to predict industrial machine failures before they occur — 
    shifting maintenance from reactive to proactive.</p>
</div>
""", unsafe_allow_html=True)

# Stats row (read from reports/model_metrics.json - nothing hardcoded)
report = load_metrics()
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Dataset", "AI4I 2020")
if report:
    h = headline(report)
    col2.metric("Samples", f"{h['rows']:,}")
    col3.metric("Features", len(report["feature_set"]["features"]))
    col4.metric("Decision method", h["method"])
    col5.metric("Test F1", f"{h['f1']:.2f}", f"95% CI {h['f1_ci'][0]:.2f}-{h['f1_ci'][1]:.2f}", delta_color="off")
else:
    st.warning("reports/model_metrics.json not found. Run:  python -m ml.training.train")

st.markdown("<br>", unsafe_allow_html=True)

# Navigation cards
st.markdown("### Navigate")
c1, c2, c3, c4 = st.columns(4)

cards = [
    ("📊", "Dashboard", "Live machine health overview with sensor trends and failure distribution.", "#0EA5E9"),
    ("🔍", "Predict", "Enter sensor values to get real-time fault classification and health status.", "#10B981"),
    ("📋", "History", "View all past predictions stored in the database with download option.", "#8B5CF6"),
    ("📈", "Model Report", "Compare Random Forest, XGBoost, and Isolation Forest performance.", "#F59E0B"),
]

for col, (icon, title, desc, color) in zip([c1, c2, c3, c4], cards):
    col.markdown(f"""
    <div style="background:#112240;border-radius:12px;border:1px solid #1E3A5F;
         padding:1.25rem;height:160px;cursor:pointer;transition:border-color 0.2s;">
      <div style="font-size:28px;margin-bottom:8px;">{icon}</div>
      <h3 style="color:{color};margin:0 0 6px;font-size:16px;font-weight:600;">{title}</h3>
      <p style="color:#64748B;font-size:13px;margin:0;line-height:1.5;">{desc}</p>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# Pipeline overview
st.markdown("### ML Pipeline")
st.markdown("""
<div style="background:#112240;border-radius:12px;border:1px solid #1E3A5F;padding:1.5rem;">
  <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;">
""" + "".join([
    f"""<div style="display:flex;align-items:center;gap:8px;">
      <div style="background:#0D1B2A;border:1px solid {color};border-radius:8px;
           padding:8px 16px;font-size:13px;color:{color};font-weight:500;">{step}</div>
      {"<span style='color:#1E3A5F;font-size:20px;'>→</span>" if i < 5 else ""}
    </div>"""
    for i, (step, color) in enumerate([
        ("UCI Dataset", "#0EA5E9"), ("Train/Test Split", "#8B5CF6"),
        ("Scale + SMOTE (train only)", "#F59E0B"), ("Model Training", "#10B981"),
        ("Test Evaluation", "#F59E0B"), ("Prediction Service", "#0EA5E9")
    ])
]) + """
  </div>
</div>
""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)
st.markdown("""
<p style="color:#334155;font-size:12px;text-align:center;">
Nelcy V [2548419] · Sivabalan A [2548426] · Swasthika D [2548427] · 
Master of Data Science · CHRIST (Deemed to be University) · 2025–2026</p>
""", unsafe_allow_html=True)