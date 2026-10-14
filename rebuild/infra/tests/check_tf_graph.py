#!/usr/bin/env python3
"""Check from `terraform graph` output that a destroy guard is destroyed FIRST (REVIEW4 B3 and M5). Offline.

Terraform destroys a resource only after every resource that depends on it. So the guard runs before everything it can reach
through dependency edges. Every managed resource of the root (data sources excluded) must be reachable from the guard, or a
destroy may delete it in parallel with the guard, before the guard has decided.

  check_tf_graph.py GRAPH.dot --guard terraform_data.teardown_guard [--allow REGEX ...]
Exit 0 when every resource is reachable, 1 otherwise (the unreachable ones are listed).
"""
from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict

NODE = re.compile(r'^\s*"([^"]+)"\s*\[label=')
EDGE = re.compile(r'^\s*"([^"]+)"\s*->\s*"([^"]+)"')


def parse(text: str) -> tuple[set[str], dict[str, set[str]]]:
    nodes: set[str] = set()
    edges: dict[str, set[str]] = defaultdict(set)
    for line in text.splitlines():
        m = EDGE.match(line)
        if m:
            edges[m.group(1)].add(m.group(2))
            nodes.update(m.groups())
            continue
        m = NODE.match(line)
        if m:
            nodes.add(m.group(1))
    return nodes, edges


def is_data(name: str) -> bool:
    return name.startswith("data.") or ".data." in name


def unreachable(nodes: set[str], edges: dict[str, set[str]], guard: str, allow: list[str]) -> list[str]:
    seen, todo = {guard}, [guard]
    while todo:
        for nxt in edges.get(todo.pop(), ()):
            if nxt not in seen:
                seen.add(nxt)
                todo.append(nxt)
    pats = [re.compile(a) for a in allow]
    return sorted(n for n in nodes if n != guard and not is_data(n) and n not in seen and not any(p.search(n) for p in pats))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("graph")
    ap.add_argument("--guard", required=True)
    ap.add_argument("--allow", action="append", default=[], help="regex of a resource that may be destroyed in parallel")
    a = ap.parse_args()
    nodes, edges = parse(open(a.graph).read())
    if a.guard not in nodes:
        print(f"FAIL {a.guard} is not in the graph")
        return 1
    left = unreachable(nodes, edges, a.guard, a.allow)
    if left:
        print(f"FAIL {len(left)} resource(s) not reachable from {a.guard}; a destroy may delete them before the guard decides:")
        for n in left:
            print(f"       {n}")
        return 1
    print(f"ok   every resource of the root is destroyed after {a.guard} ({len(nodes)} graph nodes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
