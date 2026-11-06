#!/usr/bin/env bash
# bash build.sh ROUND -> out/ROUND/{index,drawer,alert,empty}.html (+ artifact.html without doctype)
set -euo pipefail
H=$(cd -- "$(dirname -- "$0")" && pwd)
R=${1:?usage: build.sh ROUND}
O="$H/out/$R"
if [[ -e "$O" ]]; then echo "refusing to overwrite $O" >&2; exit 1; fi
mkdir -p "$O"
python3 - "$H/src" "$O" <<'EOF'
import sys, pathlib
src, out = map(pathlib.Path, sys.argv[1:])
shell = (src / "shell.html").read_text()
css, app, fx = ((src / n).read_text() for n in ("app.css", "app.js", "fixtures.js"))
def page(spec):
    return (shell.replace("/*CSS*/", css).replace("/*FIXTURE*/", fx)
            .replace("/*APP*/", app).replace("/*SPEC*/", spec))
for name, spec in [("index", "overview"), ("drawer", "drawer"), ("alert", "alert"), ("empty", "empty"), ("live", "live")]:
    (out / f"{name}.html").write_text('<!doctype html>\n<html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover"></head><body>\n'
        + page(spec) + "\n</body></html>\n")
(out / "artifact.html").write_text(page("live"))
EOF
( cd "$H/src" && shasum -a 256 * ) > "$O/SOURCES.sha256"
echo "built $O"
