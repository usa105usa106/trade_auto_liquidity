import py_compile, compileall, subprocess, sys
from pathlib import Path

root = Path(__file__).resolve().parent
for file in root.glob("*.py"):
    py_compile.compile(str(file), doraise=True)

assert compileall.compile_dir(str(root), quiet=1)

res = subprocess.run([sys.executable, "-m", "pytest", "-q"], cwd=root, text=True, capture_output=True, timeout=60)
print(res.stdout)
if res.returncode:
    print(res.stderr)
    raise SystemExit(res.returncode)

print("AUDIT PASSED")
