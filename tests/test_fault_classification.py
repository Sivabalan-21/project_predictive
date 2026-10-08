import math

import pytest

from ml.fault_classification import (RULE_BASIS, classify, fault_indicators, power_watts)
from ml.training.evaluate import fault_rules_validation

BASE = {"air_temperature": 300.0, "process_temperature": 310.0, "rotational_speed": 1500.0,
        "torque": 40.0, "tool_wear": 100.0}


def reading(**kw):
    return {**BASE, **kw}


def test_healthy_reading_has_no_indicators():
    assert fault_indicators(BASE) == ()


# ---- HDF: (process - air) < 8.6 K AND rpm < 1380
@pytest.mark.parametrize("diff,rpm,expected", [
    (8.5, 1379, True), (8.6, 1379, False), (8.5, 1380, False), (12.0, 1000, False),
])
def test_hdf_boundaries(diff, rpm, expected):
    r = reading(process_temperature=300.0 + diff, rotational_speed=rpm, torque=40.0)
    # keep power in range so only HDF can fire: torque chosen per rpm
    r["torque"] = 6000 * 60 / (2 * math.pi * rpm)
    assert ("HDF" in fault_indicators(r)) is expected


# ---- PWF: power in WATTS = torque * rpm * 2*pi/60, outside [3500, 9000]
def test_power_is_computed_in_watts_not_torque_times_rpm():
    assert power_watts(40, 1500) == pytest.approx(6283.185, rel=1e-6)
    assert 40 * 1500 == 60000  # the OLD (wrong) quantity would have flagged this healthy row


@pytest.mark.parametrize("torque,rpm,expected", [
    (40, 800, True),     # 3351 W  < 3500
    (40, 1500, False),   # 6283 W  in range
    (70, 1300, True),    # 9529 W  > 9000
    (50, 1700, False),   # 8901 W  in range
])
def test_pwf_thresholds(torque, rpm, expected):
    r = reading(torque=torque, rotational_speed=rpm, tool_wear=0, process_temperature=312.0)
    assert ("PWF" in fault_indicators(r)) is expected


# ---- OSF: wear*torque > 11000 (L) / 12000 (M) / 13000 (H)
@pytest.mark.parametrize("product,product_value,expected", [
    ("L", 11_500, True), ("M", 11_500, False), ("H", 11_500, False),
    ("L", 12_500, True), ("M", 12_500, True), ("H", 12_500, False),
    ("H", 13_500, True),
])
def test_osf_depends_on_product_type(product, product_value, expected):
    r = reading(torque=50.0, tool_wear=product_value / 50.0, rotational_speed=1500, process_temperature=312.0)
    assert ("OSF" in fault_indicators(r, product)) is expected


def test_osf_without_product_type_uses_most_sensitive_limit():
    r = reading(torque=50.0, tool_wear=230.0)           # 11,500 -> only exceeds the L limit
    assert "OSF" in fault_indicators(r, None)
    assert "OSF" not in fault_indicators(r, "M")


# ---- TWF window
def test_twf_window_start():
    assert "TWF" not in fault_indicators(reading(tool_wear=199.9, torque=20.0))
    assert "TWF" in fault_indicators(reading(tool_wear=200.0, torque=20.0))


# ---- classify(): kept apart from the binary failure decision
def test_not_failure_reports_indicators_but_no_codes():
    fa = classify(reading(tool_wear=210.0, torque=35.0), None, is_failure=False)  # 5498 W, 7350 minNm: only TWF
    assert fa.indicators == ("TWF",) and fa.codes == () and fa.fault_type is None


def test_failure_without_matching_rule_is_residual_rnf():
    fa = classify(BASE, None, is_failure=True)
    assert fa.indicators == () and fa.codes == ("RNF",) and fa.fault_type == "RNF"


def test_multiple_faults_have_stable_order():
    r = reading(process_temperature=305.0, rotational_speed=600.0, torque=50.0, tool_wear=250.0)
    # diff 5 K & 600 rpm -> HDF; 3142 W -> PWF; 250x50=12,500 > 11,000 (L) -> OSF; wear>=200 -> TWF
    fa = classify(r, "L", is_failure=True)
    assert fa.codes == ("TWF", "HDF", "PWF", "OSF") and fa.fault_type == "TWF+HDF+PWF+OSF"


def test_every_rule_documents_its_basis():
    assert set(RULE_BASIS) == {"TWF", "HDF", "PWF", "OSF", "RNF"}
    assert all(v["basis"] in {"dataset_definition", "engineering_assumption"} and v["caveat"] for v in RULE_BASIS.values())


# ---- the rules reproduce the dataset's own labels
def test_rules_match_dataset_labels(dataset):
    df, _ = dataset
    v = fault_rules_validation(df)
    for code in ("HDF", "PWF", "OSF"):
        assert v[code]["fp"] == 0 and v[code]["fn"] == 0 and v[code]["tp"] == v[code]["label_count"] > 0
    # TWF is random inside the window: rule flags risk (high recall, low precision) - not a detector
    assert v["TWF"]["recall"] >= 0.97 and v["TWF"]["precision"] < 0.10
