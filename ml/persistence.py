"""Portable model-artifact persistence with save -> reload round-trip validation.

Why this module exists (root cause of "xgboost._c_api.XGBoostError: input stream corrupted")
--------------------------------------------------------------------------------------------
Pickling an ``xgboost.Booster`` (which is what ``joblib.dump(XGBClassifier)`` does) calls
``XGBoosterSerializeToBuffer``. That buffer contains the *training configuration*, including
``rng_state``: a **text dump of a C++ ``std::mt19937``** written by ``SaveRng`` as
``ss << std::hex << rng`` and read back by ``LoadRng`` as ``ss >> std::hex >> rng``
(XGBoost 3.4.1, ``src/common/random.cc``). That text format is implementation-defined:

* libstdc++ (Linux / GCC) always writes and reads the engine state in **decimal**;
* MSVC STL (Windows) honours ``std::hex``.

A model trained and pickled on Linux therefore stores decimal numbers, and loading it on
Windows parses them as hex, overflows 32 bits and raises "input stream corrupted". The
file is byte-identical (SHA-256 matches) - the *format* is simply not portable. Any model
with ``subsample`` or ``colsample_*`` < 1 stores the state (this project uses 0.8 / 0.8).

Fix: store the booster in XGBoost's documented, cross-platform model format (UBJSON via
``Booster.save_raw``). It holds only the trees and learner parameters - no RNG state and no
C++-stdlib-specific text. The resulting ``xgboost.joblib`` is still a normal joblib file that
``joblib.load`` returns as an ``XGBClassifier``; no project import is needed to load it.

``save_and_validate`` additionally proves the artifacts load and reproduce their outputs
bit-for-bit, in this process and in a clean child process, before a manifest may be written.
"""
from __future__ import annotations

import contextlib
import copyreg
import hashlib
import logging
import os
import subprocess
import sys
import zlib
from pathlib import Path
from typing import Any, Iterator, Mapping

import joblib
import numpy as np
import xgboost

log = logging.getLogger(__name__)

# Byte patterns that must never appear inside a shipped artifact because their encoding
# depends on the C++ standard library of the machine that wrote them.
NON_PORTABLE_MARKERS: tuple[bytes, ...] = (b"rng_state",)
XGBOOST_SERIALIZATION = "xgboost.Booster.save_raw('ubj') inside joblib (portable; no RNG state)"


class ArtifactValidationError(RuntimeError):
    """A saved artifact failed the save -> reload round-trip check."""


# --------------------------------------------------------------------------- dumping
def _reduce_booster(booster: xgboost.Booster):
    """Pickle a Booster as its portable UBJSON model instead of the stdlib-specific state buffer."""
    return xgboost.Booster, (None, None, bytearray(booster.save_raw(raw_format="ubj")))


@contextlib.contextmanager
def portable_xgboost_pickling() -> Iterator[None]:
    """Temporarily make pickle/joblib serialise ``xgboost.Booster`` portably."""
    previous = copyreg.dispatch_table.get(xgboost.Booster)
    copyreg.pickle(xgboost.Booster, _reduce_booster)
    try:
        yield
    finally:
        if previous is None:
            copyreg.dispatch_table.pop(xgboost.Booster, None)
        else:
            copyreg.dispatch_table[xgboost.Booster] = previous


def dump_artifact(obj: Any, path: Path, compress: int = 3) -> None:
    """Write ``obj`` to ``path`` (joblib) with portable XGBoost serialisation, atomically."""
    tmp = path.with_name(path.name + ".tmp")
    with portable_xgboost_pickling():
        joblib.dump(obj, tmp, compress=compress)
    os.replace(tmp, path)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def pickle_payload(path: Path) -> bytes:
    """Return the uncompressed joblib payload (joblib compress=N writes a plain zlib stream)."""
    raw = Path(path).read_bytes()
    try:
        return zlib.decompress(raw)
    except zlib.error:
        return raw  # uncompressed joblib file


def find_non_portable_markers(path: Path) -> list[str]:
    payload = pickle_payload(path)
    return [m.decode() for m in NON_PORTABLE_MARKERS if m in payload]


# ------------------------------------------------------------------ reference outputs
def artifact_outputs(obj: Any, probe_scaled: np.ndarray, probe_raw: np.ndarray) -> np.ndarray:
    """Deterministic output of a loaded artifact on a fixed probe (dispatch by capability)."""
    if hasattr(obj, "predict_proba"):          # RandomForestClassifier, XGBClassifier
        return np.asarray(obj.predict_proba(probe_scaled))
    if hasattr(obj, "score_samples"):          # IsolationForest
        return np.asarray(obj.score_samples(probe_scaled))
    if hasattr(obj, "transform"):              # StandardScaler (takes raw, unscaled features)
        return np.asarray(obj.transform(probe_raw))
    raise ArtifactValidationError(f"don't know how to probe artifact of type {type(obj).__name__}")


def _child_main(directory: str, probe_npz: str, out_npz: str) -> int:
    """Run in a *fresh* interpreter: load every artifact from disk and write its outputs."""
    # Copy the probes into ordinary arrays and close the NpzFile immediately, so the child
    # never holds probe.npz open (matters on Windows, where open files cannot be deleted).
    with np.load(probe_npz) as probes:
        scaled = np.array(probes["scaled"], copy=True)
        raw = np.array(probes["raw"], copy=True)
    outputs = {}
    for fname in sorted(p.name for p in Path(directory).glob("*.joblib")):
        obj = joblib.load(Path(directory) / fname)
        outputs[fname] = artifact_outputs(obj, scaled, raw)
        outputs[fname + "::type"] = np.array(type(obj).__name__)
    # Pass an explicitly opened file object: np.savez then never opens a path of its own, and
    # the ``with`` guarantees the handle is closed before the interpreter exits.
    with open(out_npz, "wb") as fh:
        np.savez(fh, **outputs)
    return 0


def save_and_validate(artifacts: Mapping[str, Any], directory: Path, probe_scaled: np.ndarray,
                      probe_raw: np.ndarray, fresh_process: bool = True) -> dict:
    """Dump every artifact, then prove each one reloads and reproduces the original's outputs.

    Checks, per artifact: (1) no non-portable marker bytes in the file; (2) ``joblib.load`` in
    this process returns the same type and bit-identical outputs; (3) the same in a clean child
    interpreter. Raises ArtifactValidationError on the first failure. Returns a record for the
    manifest (hashes + what was verified).
    """
    directory.mkdir(parents=True, exist_ok=True)
    reference: dict[str, np.ndarray] = {}
    for fname, obj in artifacts.items():
        reference[fname] = artifact_outputs(obj, probe_scaled, probe_raw)
        dump_artifact(obj, directory / fname)

    for fname, obj in artifacts.items():
        path = directory / fname
        bad = find_non_portable_markers(path)
        if bad:
            raise ArtifactValidationError(f"{fname} contains platform-dependent data {bad}; it would not load on other OSes")
        try:
            reloaded = joblib.load(path)
        except Exception as exc:  # noqa: BLE001
            raise ArtifactValidationError(f"{fname} failed to reload after saving: {exc}") from exc
        if type(reloaded) is not type(obj):
            raise ArtifactValidationError(f"{fname} reloaded as {type(reloaded).__name__}, expected {type(obj).__name__}")
        if not np.array_equal(artifact_outputs(reloaded, probe_scaled, probe_raw), reference[fname]):
            raise ArtifactValidationError(f"{fname} reloaded but its outputs differ from the trained model")

    if fresh_process:
        _validate_in_fresh_process(artifacts, directory, reference, probe_scaled, probe_raw)

    return {
        "method": "save -> reload round-trip; outputs compared bit-for-bit on a held-out probe",
        "probe_rows": int(len(probe_scaled)),
        "in_process_reload": True,
        "fresh_process_reload": bool(fresh_process),
        "non_portable_markers_absent": [m.decode() for m in NON_PORTABLE_MARKERS],
        "artifacts_verified": sorted(artifacts),
        "sha256": {fname: sha256_file(directory / fname) for fname in artifacts},
    }


def _validate_in_fresh_process(artifacts, directory: Path, reference, probe_scaled, probe_raw) -> None:
    import tempfile
    root = Path(__file__).resolve().parent.parent
    with tempfile.TemporaryDirectory() as tmp:
        probe_npz, out_npz = str(Path(tmp) / "probe.npz"), str(Path(tmp) / "out.npz")
        with open(probe_npz, "wb") as fh:
            np.savez(fh, scaled=probe_scaled, raw=probe_raw)
        # Popen + communicate() (instead of run) so termination is explicit: communicate() drains
        # and closes stdout/stderr and wait() reaps the child. The ``with`` on Popen closes the
        # pipes and waits again, and the finally-kill covers any exceptional exit path.
        with subprocess.Popen([sys.executable, "-m", "ml.persistence", str(directory), probe_npz, out_npz],
                              cwd=root, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, text=True) as child:
            try:
                stdout, stderr = child.communicate()
                child.wait()
            finally:
                if child.poll() is None:
                    child.kill()
                    child.wait()
        if child.returncode != 0:
            raise ArtifactValidationError(f"clean-process reload failed:\n{stderr.strip() or stdout.strip()}")

        # Read everything we need into plain, owned arrays and close the NpzFile *before* the
        # temporary directory is removed. No array returned from here keeps the zip handle alive.
        got_types: dict[str, str] = {}
        got_outputs: dict[str, np.ndarray] = {}
        with np.load(out_npz) as got:
            for fname in artifacts:
                got_types[fname] = str(got[fname + "::type"])
                got_outputs[fname] = np.array(got[fname], copy=True)
        # ``got`` is closed here; comparisons below touch only the copies.
        for fname, obj in artifacts.items():
            if got_types[fname] != type(obj).__name__:
                raise ArtifactValidationError(f"{fname}: clean process loaded {got_types[fname]}, expected {type(obj).__name__}")
            if not np.array_equal(got_outputs[fname], reference[fname]):
                raise ArtifactValidationError(f"{fname}: clean-process outputs differ from the trained model")


if __name__ == "__main__":  # python -m ml.persistence <artifact_dir> <probe.npz> <out.npz>
    sys.exit(_child_main(*sys.argv[1:4]))
