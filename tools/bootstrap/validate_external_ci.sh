#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

MODE="normal"
SMOKE_FAIL_STEP=""

usage() {
  cat <<'USAGE'
Usage: tools/bootstrap/validate_external_ci.sh [--mode normal|bootstrap-only] [--smoke-fail-step STEP]

Modes:
  normal          Requires at least one lifecycle test file and runs it (default).
  bootstrap-only  Allows lifecycle suite to be absent and prints NOT PRESENT.

Optional smoke failure:
  --smoke-fail-step <lint|format|typecheck|import|build|lifecycle>
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      MODE="${2:-}"
      shift 2
      ;;
    --smoke-fail-step)
      SMOKE_FAIL_STEP="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ "$MODE" != "normal" && "$MODE" != "bootstrap-only" ]]; then
  echo "Invalid --mode: $MODE (expected normal or bootstrap-only)" >&2
  exit 2
fi

maybe_smoke_fail() {
  local step="$1"
  if [[ -n "$SMOKE_FAIL_STEP" && "$SMOKE_FAIL_STEP" == "$step" ]]; then
    echo "SMOKE_FAIL_STEP triggered at step=$step" >&2
    return 91
  fi
}

run_step() {
  local step="$1"
  shift
  maybe_smoke_fail "$step"
  echo "STEP $step: $*"
  "$@"
  local exit_code=$?
  echo "STEP $step: exit=$exit_code"
}

collect_lifecycle_tests() {
  find tests -type f \( -name "*lifecycle*.py" -o -path "*/lifecycle/*" \) 2>/dev/null | sort
}

echo "VALIDATION_MODE=$MODE"
echo "PYTHON=$(python --version 2>&1)"
echo "RUFF=$(ruff --version 2>&1)"
echo "PYRIGHT=$(pyright --version 2>&1)"

run_step lint ruff check .
run_step format ruff format --check .
run_step typecheck pyright
run_step import python -c "import wavelink; print(wavelink.__version__)"
run_step build python -m build

mapfile -t lifecycle_tests < <(collect_lifecycle_tests)
lifecycle_count="${#lifecycle_tests[@]}"
echo "LIFECYCLE_TEST_FILES=$lifecycle_count"

if [[ "$lifecycle_count" -eq 0 ]]; then
  if [[ "$MODE" == "bootstrap-only" ]]; then
    echo "LIFECYCLE_SUITE=NOT PRESENT"
    exit 0
  fi

  echo "LIFECYCLE_SUITE=REQUIRED_BUT_MISSING" >&2
  exit 14
fi

echo "LIFECYCLE_SUITE=RUNNING"
run_step lifecycle python -m pytest -q "${lifecycle_tests[@]}"
echo "LIFECYCLE_SUITE=PASS"
