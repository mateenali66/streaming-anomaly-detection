"""Fill {{key}} placeholders in a markdown letter with the numbers of claims.json, as plain text (D6).

The response to the reviewers and the cover letter quote results. Rendering them from the same claims.json as the
manuscript keeps every number identical across the three documents. A placeholder with no key stops the render.

    python3 -m analysis.render_letter --claims claims.json --template letter.tmpl.md --out letter.md
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


def plain(tex: str) -> str:
    s = tex.replace("\\ensuremath{", "").replace("}", "").replace("{", "")
    s = re.sub(r"\\times 10\^(-?\d+)", lambda m: f" x 10^{m.group(1)}", s)
    s = s.replace("\\%", "%").replace("-", "−") if s.startswith("-") else s.replace("\\%", "%")
    return s


def render(claims: dict, text: str) -> str:
    def sub(m):
        k = m.group(1).strip()
        if k not in claims["numbers"]:
            raise SystemExit(f"ERROR: letter uses unknown key {k}")
        return plain(claims["numbers"][k]["text"])
    return re.sub(r"\{\{([^}]+)\}\}", sub, text)


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--claims", required=True)
    p.add_argument("--template", required=True)
    p.add_argument("--out", required=True)
    a = p.parse_args(argv)
    claims = json.loads(Path(a.claims).read_text())
    Path(a.out).write_text(render(claims, Path(a.template).read_text()))
    print(f"rendered {a.out}")


if __name__ == "__main__":
    main()
