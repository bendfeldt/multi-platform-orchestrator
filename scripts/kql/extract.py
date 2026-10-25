"""Prints the alert queries of a verbose `terraform test` plan as a JSON list."""

import json
import re
import sys

lines = sys.stdin.read().splitlines()
queries, i = [], 0
while i < len(lines):
    if re.search(r"query *= <<-EOT", lines[i]):
        i += 1
        body = []
        while lines[i].strip() != "EOT":
            body.append(lines[i])
            i += 1
        indent = min(len(line) - len(line.lstrip()) for line in body if line.strip())
        queries.append("\n".join(line[indent:] for line in body))
    i += 1
if not queries:
    sys.exit("no queries found in the plan")
print(json.dumps(list(dict.fromkeys(queries))))
