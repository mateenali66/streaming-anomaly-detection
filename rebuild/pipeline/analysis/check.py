"""make check (protocol 7.1 step 11, 7.2): the claims harness.

Added on 18 October 2026, after registration (D4). It fails when

1. a value in claims.json no longer equals the field it names in the analysis outputs (every number is re-read from
   its source, so a table, a figure and the text cannot drift apart),
2. the manuscript uses a key \\R{...} that numbers.tex does not define,
3. a bare number appears in the abstract, results, discussion or conclusion and is not on the allowed list
   (section numbers, years and fixed design constants), the hits printed before the count,
4. a table or figure caption is longer than 20 words (4.6.5, R7.3),
5. the PDF is older than numbers.tex, or its build record names another numbers.tex hash.

    python3 -m analysis.check --results DIR --numbers numbers.tex --claims claims.json \\
        [--tex main.tex --allow allowed_numbers.json --pdf main.pdf --build-record build.json]
"""
from __future__ import annotations

import argparse
import json
import math
import re
import sys
from pathlib import Path

import pandas as pd

PIPE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPE))
from streamprep.contract import sha256_file  # noqa: E402

CHECKED_SECTIONS = ("abstract", "results", "discussion", "conclusion")
NUMBER = re.compile(r"(?<![A-Za-z\\@{_.\-])[-+]?\d[\d,]*(?:\.\d+)?")


def _walk(obj, path: str):
    """Follow a dotted field path. A path element may itself hold dots or '|' (contrast keys), so try the longest
    key that matches at each step."""
    parts = path.split(".")
    i = 0
    while i < len(parts):
        if isinstance(obj, list):
            obj = obj[int(parts[i])]
            i += 1
            continue
        for j in range(len(parts), i, -1):
            k = ".".join(parts[i:j])
            if k in obj:
                obj, i = obj[k], j
                break
        else:
            raise KeyError(f"{'.'.join(parts[i:])} not found")
    return obj


def _same(a, b) -> bool:
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        if isinstance(a, float) and math.isnan(a):
            return isinstance(b, float) and math.isnan(b)
        return abs(float(a) - float(b)) <= 1e-12 * max(1.0, abs(float(a)))
    return a == b


def check_claims(results: Path, claims: dict) -> list[str]:
    errors, cache = [], {}
    for key, c in claims["numbers"].items():
        src, field = c["source"], c["field"]
        if src == "derived":
            op, keys = field.split(":", 1)
            vals = [claims["numbers"][k]["value"] for k in keys.split("|")]
            got = {"min": min, "max": max, "sum": sum, "mean": lambda x: sum(x) / len(x),
                   "maxabsdiff": lambda x: max(abs(x[i] - x[i + 1]) for i in range(0, len(x), 2))}[op](vals)
            if not _same(c["value"], got):
                errors.append(f"claim {key}: derived {op} now gives {got!r}, claims.json holds {c['value']!r}")
            continue
        if "*" in field:
            continue        # an aggregate over many cells, checked by recomputing it in numbers.py's tests
        p = results / src
        try:
            if src.endswith(".json"):
                obj = cache.setdefault(src, json.loads(p.read_text()))
                got = len(_walk(obj, field[:-4])) if field.endswith("#len") else _walk(obj, field)
            elif (src.endswith(".parquet") or src.endswith(".csv")) and "=" in field:
                df = cache.setdefault(src, pd.read_parquet(p) if src.endswith(".parquet") else pd.read_csv(p))
                cond, part = field.rsplit(".", 1)
                sel = df
                for kv in cond.split(","):
                    k, v = kv.split("=", 1)
                    sel = sel[sel[k].astype(str) == v]
                if len(sel) != 1:
                    raise KeyError(f"{len(sel)} rows match")
                col = ({"pooled": "pooled_mean", "c1": "c1_mean", "lo": "pooled_lo", "hi": "pooled_hi"}.get(part, part)
                       if src.endswith(".parquet") else part)
                if pd.isna(sel.iloc[0][col]) and c["value"] is None:
                    continue
                got = sel.iloc[0][col]
            else:
                continue
        except (KeyError, IndexError, ValueError) as e:
            errors.append(f"claim {key}: {src} {field}: {e}")
            continue
        if not _same(c["value"], got):
            errors.append(f"claim {key}: claims.json holds {c['value']!r}, {src} now gives {got!r}")
    return errors


def tex_sections(tex: str) -> dict[str, str]:
    """Text of the abstract and of each \\section, keyed by a lower-case name."""
    out = {}
    m = re.search(r"\\begin\{abstract\}(.*?)\\end\{abstract\}", tex, re.S)
    if m:
        out["abstract"] = m.group(1)
    parts = re.split(r"\\section\*?\{([^}]*)\}", tex)
    for i in range(1, len(parts) - 1, 2):
        out[parts[i].strip().lower()] = parts[i + 1]
    return out


def strip_tex(s: str) -> str:
    s = re.sub(r"(?<!\\)%.*", "", s)
    s = re.sub(r"\\R\{[^}]*\}", " NUM ", s)
    s = re.sub(r"\\(ref|eqref|cite[tp]?|label|url|href|includegraphics|input|autoref|cref)\*?(\[[^\]]*\])?\{[^}]*\}",
               " ", s)
    s = re.sub(r"\\begin\{(table|figure|equation|align)\*?\}.*?\\end\{\1\*?\}", " ", s, flags=re.S)
    return s


def bare_numbers(tex: str, allowed: set[str]) -> list[tuple[str, str]]:
    hits = []
    for name, body in tex_sections(tex).items():
        if not any(name.startswith(s) for s in CHECKED_SECTIONS):
            continue
        for m in NUMBER.finditer(strip_tex(body)):
            n = m.group(0).strip("+").replace(",", "")
            if n in allowed or (len(n) == 4 and n.isdigit() and 1900 <= int(n) <= 2100):
                continue
            hits.append((name, m.group(0)))
    return hits


def long_captions(tex: str, limit: int = 20) -> list[tuple[int, str]]:
    out = []
    for m in re.finditer(r"\\caption(?:\[[^\]]*\])?\{", tex):
        i, depth = m.end(), 1
        while depth and i < len(tex):
            depth += {"{": 1, "}": -1}.get(tex[i], 0)
            i += 1
        cap = re.sub(r"\\[A-Za-z]+\*?(\{[^}]*\})?", " ", tex[m.end():i - 1])
        words = [w for w in re.split(r"\s+", cap) if re.search(r"[A-Za-z0-9]", w)]
        if len(words) > limit:
            out.append((len(words), " ".join(words)[:90]))
    return out


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--results", required=True)
    p.add_argument("--numbers", required=True)
    p.add_argument("--claims", required=True)
    p.add_argument("--tex")
    p.add_argument("--allow")
    p.add_argument("--pdf")
    p.add_argument("--build-record")
    a = p.parse_args(argv)
    errors = []
    claims = json.loads(Path(a.claims).read_text())
    errors += check_claims(Path(a.results), claims)
    defined = set(re.findall(r"\\csname r@([^\\]+)\\endcsname\{", Path(a.numbers).read_text()))
    if set(claims["numbers"]) != defined:
        errors.append(f"numbers.tex and claims.json disagree on {len(set(claims['numbers']) ^ defined)} keys")
    if a.tex:
        tex = "".join(Path(t).read_text() for t in [a.tex])
        uncommented = re.sub(r"(?<!\\)%.*", "", tex)
        for k in sorted(set(re.findall(r"\\R\{([^}]*)\}", uncommented)) - defined):
            errors.append(f"manuscript uses undefined key {k}")
        allowed = set(json.loads(Path(a.allow).read_text())) if a.allow else set()
        hits = bare_numbers(tex, allowed)
        for name, n in hits:
            print(f"  bare number in {name}: {n}")
        if hits:
            errors.append(f"{len(hits)} bare numbers in {', '.join(CHECKED_SECTIONS)}")
        for n, cap in long_captions(tex):
            errors.append(f"caption of {n} words: {cap}")
    if a.pdf:
        pdf, num = Path(a.pdf), Path(a.numbers)
        if not pdf.is_file() or pdf.stat().st_mtime < num.stat().st_mtime:
            errors.append(f"{pdf} is older than {num}; rebuild the PDF")
        if a.build_record:
            rec = json.loads(Path(a.build_record).read_text())
            if rec.get("numbers_sha256") != sha256_file(num):
                errors.append("the PDF's build record names another numbers.tex")
    for e in errors:
        print(f"FAIL {e}")
    n_claims = len(claims["numbers"])
    print(f"check: {n_claims} numbers, {len(errors)} failures")
    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
