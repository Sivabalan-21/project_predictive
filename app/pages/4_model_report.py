import os
import sys

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from ml import config
from ml.fault_classification import RULE_BASIS
from ml.reporting import MISSING_HINT, load_metrics, pct

st.set_page_config(page_title="Model Report · PredictMaint", page_icon="📈", layout="wide")
st.markdown("""
<style>
.main{background:#0A1628;}
h1,h2,h3{color:#F1F5F9!important;}
div[data-testid="metric-container"]{background:#112240;border-radius:12px;border:1px solid #1E3A5F;padding:16px;}
.stMetric label{color:#94A3B8!important;font-size:12px!important;}
.stMetric [data-testid="stMetricValue"]{color:#F1F5F9!important;}
</style>""", unsafe_allow_html=True)

st.title("📈 Model Performance Report")
st.caption("Leak-free evaluation. Every number on this page is read from reports/model_metrics.json.")

report = load_metrics()
if report is None:
    st.error(MISSING_HINT)
    st.stop()

sel = report["test"]["selected"]
ci = sel["bootstrap_95ci"]
st.subheader(f"Selected decision method: `{sel['name']}`")
st.caption(f"Chosen by {report['selection']['criterion']}. The held-out test set played no part in the choice.")
c1, c2, c3, c4 = st.columns(4)
c1.metric("Precision", pct(sel["precision"]), f"95% CI {pct(ci['precision'][0], 0)}–{pct(ci['precision'][1], 0)}", delta_color="off")
c2.metric("Recall", pct(sel["recall"]), f"95% CI {pct(ci['recall'][0], 0)}–{pct(ci['recall'][1], 0)}", delta_color="off")
c3.metric("F1", pct(sel["f1"]), f"95% CI {pct(ci['f1'][0], 0)}–{pct(ci['f1'][1], 0)}", delta_color="off")
c4.metric("Test failures", report["split"]["test_failures"], f"of {report['split']['test_rows']} rows", delta_color="off")

st.info("Only about 3.4% of rows are failures, so accuracy is not used to rank models: always predicting "
        "'normal' already scores ~96.6%. Selection used F1; precision and recall are shown alongside. "
        "Several methods are within noise of each other (fold-to-fold F1 varies by about ±4 points).")

LABELS = {"rf_class_weight": "Random Forest (class weights)", "rf_smote": "Random Forest + SMOTE",
          "xgboost": "XGBoost", "isolation_forest": "Isolation Forest"}
rows = [{"Model": LABELS[k], **{n: m[n] for n in ("precision", "recall", "f1", "roc_auc", "pr_auc", "accuracy")}}
        for k, m in report["test"]["models"].items()]
models_df = pd.DataFrame(rows)

st.subheader("The four models alone (held-out test set)")
st.dataframe(models_df.style.format({c: "{:.3f}" for c in models_df.columns[1:]}, na_rep="n/a"),
             use_container_width=True, hide_index=True)
fig = go.Figure()
for metric, color in (("precision", "#0EA5E9"), ("recall", "#10B981"), ("f1", "#8B5CF6")):
    fig.add_trace(go.Bar(name=metric.capitalize(), x=models_df["Model"], y=models_df[metric], marker_color=color))
fig.update_layout(barmode="group", height=340, yaxis_range=[0, 1], paper_bgcolor="rgba(0,0,0,0)",
                  plot_bgcolor="rgba(0,0,0,0)", font_color="#94A3B8", legend=dict(font=dict(color="#94A3B8")))
st.plotly_chart(fig, use_container_width=True)
st.caption("Isolation Forest is unsupervised; its AUC comes from its continuous anomaly score, and it is a weak failure detector on this data.")

st.subheader("Decision methods compared")
cv = report["cross_validation"]["decision_candidates"]
cand = pd.DataFrame([{"Method": n, "CV F1 (train, out-of-fold)": cv[n]["f1"], "CV F1 ± fold std": cv[n]["fold_f1_std"],
                      "Test precision": m["precision"], "Test recall": m["recall"], "Test F1": m["f1"],
                      "Test ROC-AUC": m["roc_auc"], "Selected": "★" if n == sel["name"] else ""}
                     for n, m in report["test"]["decision_candidates"].items()])
st.dataframe(cand.style.format({c: "{:.3f}" for c in cand.columns[1:7]}, na_rep="n/a"),
             use_container_width=True, hide_index=True)
fs = report["feature_set"]["cv_comparison"]
st.caption("Feature set chosen by cross-validation on training data: "
           + ", ".join(f"{k} (best OOF F1 {v['best_oof_f1']:.3f})" for k, v in fs.items())
           + f" → **{report['feature_set']['selected']}**.")

st.subheader("Plots")
for fname, title in (("confusion_matrix.png", "Confusion matrices"), ("feature_importance.png", "Feature importance")):
    path = config.REPORTS_DIR / fname
    if path.exists():
        st.image(str(path), caption=title, use_container_width=True)

st.subheader("Risk score")
rs = report["risk_score"]
st.write(f"{rs['definition']}. **Not calibrated** (Brier score {rs['brier_score']:.4f}, informational only).")

st.subheader("Fault rules vs. dataset labels")
fr = report["fault_rules_validation"]
fault_rows = [{"Fault": c, "Rule": RULE_BASIS[c]["rule"], "Basis": RULE_BASIS[c]["basis"],
               "Precision": fr[c].get("precision"), "Recall": fr[c].get("recall"), "Caveat": RULE_BASIS[c]["caveat"]}
              for c in ("TWF", "HDF", "PWF", "OSF")]
st.dataframe(pd.DataFrame(fault_rows), use_container_width=True, hide_index=True)
st.caption(fr["_scope"] + " Exact agreement reflects how the synthetic dataset was generated, not real-machine physics.")

st.subheader("Limitations")
for item in report["limitations"]:
    st.markdown(f"- {item}")
st.caption(f"Generated {report['generated_at_utc']} · scikit-learn {report['library_versions']['scikit-learn']} · "
           f"xgboost {report['library_versions']['xgboost']} · seed {report['seed']}")
