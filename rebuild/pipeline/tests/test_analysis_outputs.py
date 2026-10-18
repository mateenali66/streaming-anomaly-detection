"""analysis/numbers_tex.py, check.py and tables.py: formats, the claims harness and the caption rule (protocol 7.2)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis import check as K  # noqa: E402
from analysis import numbers_tex as N  # noqa: E402
from analysis import tables as T  # noqa: E402


def test_number_formats():
    assert N.f3(0.12345) == "0.123" and N.s3(0.0123) == "+0.012" and N.s3(-0.0004) == "0.000"
    assert N.pfmt(0.00195) == "0.00195" and N.pfmt(0.0750) == "0.075" and N.pfmt(9.2e-6) == "9.20\\times 10^{-6}"
    assert N.tex_num("-0.128") == "\\ensuremath{-0.128}" and N.tex_num("0.571") == "0.571"


def test_book_refuses_a_key_twice():
    b = N.Book()
    b.add("a.b", "1", 1, "x.json", "a", "s")
    with pytest.raises(SystemExit, match="twice"):
        b.add("a.b", "2", 2, "x.json", "a", "s")


def test_walk_handles_keys_with_dots_and_bars():
    obj = {"w": {"C1": {"contrasts": {"LSTM_AE|pooled_auc": {"pooled": {"mean": 0.5}}}}}, "x.y": [1, {"z": 3}]}
    assert K._walk(obj, "w.C1.contrasts.LSTM_AE|pooled_auc.pooled.mean") == 0.5
    assert K._walk(obj, "x.y.1.z") == 3


def test_check_claims_catches_a_changed_source(tmp_path):
    (tmp_path / "r.json").write_text(json.dumps({"a": {"b": 0.25}}))
    claims = {"numbers": {"k": {"value": 0.25, "source": "r.json", "field": "a.b"}}}
    assert K.check_claims(tmp_path, claims) == []
    (tmp_path / "r.json").write_text(json.dumps({"a": {"b": 0.26}}))
    assert len(K.check_claims(tmp_path, claims)) == 1


def test_bare_numbers_and_captions():
    tex = ("\\begin{abstract}We saw \\R{a.b} and 0.73 in 2026.\\end{abstract}\\section{Methods}Ten is 10."
           "\\section{Results}Table~\\ref{t} gives 12 runs \\cite{x2020}.\\begin{table}\\caption{Short one.}"
           "\\end{table}")
    hits = K.bare_numbers(tex, {"4"})
    assert hits == [("abstract", "0.73"), ("results", "12")]
    long = "\\caption{" + " ".join(["word"] * 21) + "}"
    assert K.long_captions(long)[0][0] == 21 and K.long_captions("\\caption{" + " ".join(["w"] * 20) + "}") == []


def test_table_generator_refuses_a_long_caption():
    with pytest.raises(SystemExit, match="caption of 21 words"):
        T.caption(" ".join(["word"] * 21))
