import streamlit as st
import sys, os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from ml.prediction import InvalidReadingError, ModelLoadError, get_prediction_service
from src.database import insert_prediction, create_tables

st.set_page_config(page_title="Predict · PredictMaint", page_icon="🔍", layout="wide")

st.markdown("""
<style>
.main{background:#0A1628;}
h1,h2,h3{color:#F1F5F9!important;}
.stSlider > div > div > div > div{background:#0EA5E9!important;}
div[data-testid="metric-container"]{background:#112240;border-radius:12px;
    border:1px solid #1E3A5F;padding:16px;}
.stMetric label{color:#94A3B8!important;font-size:12px!important;}
.stMetric [data-testid="stMetricValue"]{color:#F1F5F9!important;}
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def load_service():
    return get_prediction_service()

try:
    service = load_service()
except ModelLoadError as exc:
    st.error(f"Model artifacts unavailable: {exc}")
    st.stop()

st.markdown("""
<div style="display:flex;align-items:center;gap:12px;margin-bottom:1.5rem;">
  <span style="font-size:28px;">🔍</span>
  <div>
    <h1 style="margin:0;font-size:24px;color:#F1F5F9;">Machine Health Prediction</h1>
    <p style="margin:0;color:#64748B;font-size:13px;">Enter sensor readings · Get instant fault classification</p>
  </div>
</div>
""", unsafe_allow_html=True)

col_input, col_result = st.columns([1, 1], gap="large")

with col_input:
    st.markdown("""<div style="background:#112240;border-radius:12px;border:1px solid #1E3A5F;padding:1.5rem;">
    <p style="color:#0EA5E9;font-size:12px;font-weight:600;letter-spacing:1px;margin:0 0 1rem;">
    SENSOR INPUT PARAMETERS</p>""", unsafe_allow_html=True)

    air_temp  = st.slider("🌡 Air Temperature (K)",     290.0, 310.0, 300.0, 0.1)
    proc_temp = st.slider("🔥 Process Temperature (K)", 305.0, 320.0, 310.0, 0.1)
    rot_speed = st.slider("⚙️ Rotational Speed (RPM)",  1000,  2900,  1500,  10)
    torque    = st.slider("🔩 Torque (Nm)",              1.0,   80.0,  40.0,  0.1)
    tool_wear = st.slider("🔧 Tool Wear (min)",          0,     260,   100,   1)

    # Risk indicators
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("""<p style="color:#94A3B8;font-size:12px;font-weight:600;
        letter-spacing:1px;">RISK INDICATORS</p>""", unsafe_allow_html=True)

    temp_diff = proc_temp - air_temp
    power = torque * rot_speed * 2 * 3.141592653589793 / 60  # watts

    r1, r2 = st.columns(2)
    r1.metric("Temp Differential", f"{temp_diff:.1f} K",
              delta="Normal" if temp_diff >= 8.6 else "⚠ Low",
              delta_color="normal" if temp_diff >= 8.6 else "inverse")
    r2.metric("Power (torque × speed, W)", f"{power:,.0f} W",
              delta="Normal" if 3500 <= power <= 9000 else "⚠ Out of range",
              delta_color="normal" if 3500 <= power <= 9000 else "inverse")

    st.markdown("</div>", unsafe_allow_html=True)

    if st.button("🚀 Predict Machine Health", type="primary", use_container_width=True):
        st.session_state['predict_clicked'] = True
        st.session_state['input_data'] = {
            'air_temperature': air_temp, 'process_temperature': proc_temp,
            'rotational_speed': rot_speed, 'torque': torque, 'tool_wear': tool_wear
        }

with col_result:
    if st.session_state.get('predict_clicked'):
        input_data = st.session_state['input_data']
        try:
            result = service.predict(input_data)
        except InvalidReadingError as exc:
            st.error(f"Invalid input: {exc}")
            st.stop()

        is_fail = result.is_failure
        status_color = "#EF4444" if is_fail else "#10B981"
        status_bg = "#2D1B1B" if is_fail else "#1B2D1B"

        st.markdown(f"""
        <div style="background:{status_bg};border:2px solid {status_color};
             border-radius:16px;padding:2rem;text-align:center;margin-bottom:1rem;">
          <div style="font-size:48px;margin-bottom:0.5rem;">{"🚨" if is_fail else "✅"}</div>
          <h2 style="color:{status_color};font-size:28px;font-weight:700;margin:0;">
            {"MACHINE FAILURE" if is_fail else "NORMAL OPERATION"}</h2>
          <p style="color:#94A3B8;margin:0.5rem 0 0;font-size:14px;">
            {"Maintenance attention recommended." if is_fail else "No failure predicted."}</p>
        </div>
        """, unsafe_allow_html=True)

        if is_fail:
            from ml.fault_classification import FAULT_NAMES
            lines = "".join(f"<p style='color:#F59E0B;font-size:16px;font-weight:700;margin:4px 0 0;'>"
                            f"{c} — {FAULT_NAMES[c]}{' (residual: no rule matched)' if c == 'RNF' else ''}</p>"
                            for c in result.fault_codes)
            st.markdown(f"""
            <div style="background:#1A1A2E;border:1px solid #F59E0B;border-radius:12px;
                 padding:1rem;margin-bottom:1rem;text-align:center;">
              <p style="color:#64748B;font-size:11px;margin:0;letter-spacing:1px;">FAULT TYPE (RULE-BASED)</p>{lines}
            </div>""", unsafe_allow_html=True)
        elif result.fault_indicators:
            st.caption("Rule indicators present but no failure predicted: " + ", ".join(result.fault_indicators))

        st.markdown("""<p style="color:#94A3B8;font-size:12px;font-weight:600;
            letter-spacing:1px;margin:1rem 0 0.5rem;">MODEL OUTPUTS</p>""", unsafe_allow_html=True)
        m1, m2, m3 = st.columns(3)
        for col, name, pred, detail in [
            (m1, "Random Forest", result.rf_prediction, f"p = {result.rf_probability:.2f}"),
            (m2, "XGBoost", result.xgb_prediction, f"p = {result.xgb_probability:.2f}"),
            (m3, "Isolation Forest", result.isolation_prediction, f"anomaly score {result.isolation_anomaly_score:.2f}"),
        ]:
            v_color = "#EF4444" if pred == 1 else "#10B981"
            col.markdown(f"""
            <div style="background:#0D1B2A;border:1px solid {v_color};border-radius:10px;padding:0.75rem;text-align:center;">
              <p style="color:#64748B;font-size:10px;margin:0;">{name}</p>
              <p style="color:{v_color};font-size:16px;font-weight:700;margin:4px 0;">{"FAILURE" if pred == 1 else "NORMAL"}</p>
              <p style="color:#475569;font-size:10px;margin:0;">{detail}</p>
            </div>""", unsafe_allow_html=True)

        st.markdown(f"""<p style="color:#94A3B8;font-size:12px;font-weight:600;letter-spacing:1px;
            margin:1rem 0 0.25rem;">RISK SCORE: {result.risk_score:.2f}</p>
            <p style="color:#64748B;font-size:11px;margin:0 0 0.5rem;">Mean of RF and XGBoost failure probabilities.
            Model-derived, not a calibrated confidence.</p>""", unsafe_allow_html=True)
        st.progress(min(max(result.risk_score, 0.0), 1.0))
        st.caption(f"Final decision method: {result.decision_method} · model {result.model_version}. "
                   "The per-model outputs above use each model's default cut-off and may differ from the final decision.")

        try:
            create_tables()
            insert_prediction(input_data, result)
            st.success("✅ Prediction saved to database")
        except Exception as e:  # noqa: BLE001 - surfaced to the user, not swallowed
            st.warning(f"Could not save: {e}")
    else:
        st.markdown("""
        <div style="background:#112240;border-radius:12px;border:1px solid #1E3A5F;
             padding:3rem;text-align:center;height:400px;
             display:flex;flex-direction:column;align-items:center;justify-content:center;">
          <div style="font-size:48px;margin-bottom:1rem;">⚙️</div>
          <h3 style="color:#F1F5F9;">Awaiting sensor input</h3>
          <p style="color:#64748B;">Adjust the sliders on the left and click Predict to analyse machine health.</p>
        </div>
        """, unsafe_allow_html=True)
