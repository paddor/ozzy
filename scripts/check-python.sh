#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/ozzy_tools.sh"
cd "$ozzy_repo_root"

ozzy_python_version="${OZZY_PYTHON_VERSION:-3.12}"
export UV_PROJECT_ENVIRONMENT="$CARGO_TARGET_DIR/python/$ozzy_python_version/venv"
export RUFF_CACHE_DIR="$CARGO_TARGET_DIR/python/ruff-cache"
export MYPY_CACHE_DIR="$CARGO_TARGET_DIR/python/mypy-cache"
export UV_LINK_MODE="${UV_LINK_MODE:-copy}"
uv sync --project bindings/python --locked --no-install-project --python "$ozzy_python_version"
export PYO3_PYTHON="$UV_PROJECT_ENVIRONMENT/bin/python"
ozzy_python_base="$(dirname "$(dirname "$(readlink -f "$PYO3_PYTHON")")")"
export LD_LIBRARY_PATH="$ozzy_python_base/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
ozzy_python_wheels="$CARGO_TARGET_DIR/python/$ozzy_python_version/wheels"
mkdir -p "$ozzy_python_wheels"
# A wheel of an earlier version would conflict with the one built below.
rm -f "$ozzy_python_wheels"/ozzy-*.whl

uv run --project bindings/python --no-sync ruff check bindings/python/python bindings/python/tests bindings/python/examples
uv run --project bindings/python --no-sync ruff format --check bindings/python/python bindings/python/tests bindings/python/examples
uv run --project bindings/python --no-sync mypy bindings/python/python/ozzy
PYTHONHOME="$ozzy_python_base" ./scripts/ozzy_cargo nextest run -p ozzy-python --test-threads 1
uv run --project bindings/python --no-sync maturin build --locked \
    --manifest-path bindings/python/Cargo.toml --interpreter "$PYO3_PYTHON" --out "$ozzy_python_wheels"
uv pip install --python "$PYO3_PYTHON" --reinstall "$ozzy_python_wheels"/ozzy-*.whl
env -u PYTHONPATH "$PYO3_PYTHON" -m mypy.stubtest ozzy._native
export OZZY_TEST_PYTHON="$PYO3_PYTHON"
./scripts/ozzy_cargo nextest run -p ozzy-python --features wheel-tests --test ozzy_python_wheel \
    --profile extended --test-threads 1
