import numpy as np
import pytest

from ml.ensemble import EnsembleSpec, decide, risk_score

OUT = {"rf": np.array([0.9, 0.2, 0.6, 0.1]), "xgb": np.array([0.8, 0.7, 0.1, 0.1]), "iso": np.array([1, 0, 1, 0])}


def test_single_and_threshold():
    pred, score = decide(EnsembleSpec("s", "single", ("rf",), threshold=0.5), OUT)
    assert pred.tolist() == [1, 0, 1, 0] and score.tolist() == OUT["rf"].tolist()


def test_vote_majority_2_of_3_row_by_row():
    spec = EnsembleSpec("v", "vote", ("rf", "xgb", "iso"), {"rf": 0.5, "xgb": 0.5}, min_votes=2)
    pred, score = decide(spec, OUT)
    # votes per row: (rf,xgb,iso) = (1,1,1) (0,1,0) (1,0,1) (0,0,0)
    assert pred.tolist() == [1, 0, 1, 0]
    assert score.tolist() == pytest.approx([1.0, 1 / 3, 2 / 3, 0.0])


def test_vote_both_and_either():
    both = EnsembleSpec("b", "vote", ("rf", "xgb"), {"rf": 0.5, "xgb": 0.5}, min_votes=2)
    either = EnsembleSpec("e", "vote", ("rf", "xgb"), {"rf": 0.5, "xgb": 0.5}, min_votes=1)
    assert decide(both, OUT)[0].tolist() == [1, 0, 0, 0]
    assert decide(either, OUT)[0].tolist() == [1, 1, 1, 0]


def test_prob_mean_and_risk_score_is_plain_mean():
    spec = EnsembleSpec("m", "prob_mean", ("rf", "xgb"), threshold=0.5)
    pred, score = decide(spec, OUT)
    assert score.tolist() == pytest.approx([0.85, 0.45, 0.35, 0.1]) and pred.tolist() == [1, 0, 0, 0]
    assert risk_score(OUT).tolist() == pytest.approx(score.tolist())


def test_spec_roundtrip_and_flags():
    spec = EnsembleSpec("v", "vote", ("rf", "iso"), {"rf": 0.3}, min_votes=2, description="x")
    assert EnsembleSpec.from_dict(spec.to_dict()) == spec
    assert spec.uses_isolation_forest and not spec.has_continuous_score


def test_unknown_kind_raises():
    with pytest.raises(ValueError):
        decide(EnsembleSpec("x", "magic", ("rf",)), OUT)
