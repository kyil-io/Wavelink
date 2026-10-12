#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

MODE="normal"
SMOKE_FAIL_STEP=""
VALID_STEPS=(lint format typecheck import build lifecycle)

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

if [[ -n "$SMOKE_FAIL_STEP" ]]; then
  is_valid_smoke_step=0
  for valid_step in "${VALID_STEPS[@]}"; do
    if [[ "$SMOKE_FAIL_STEP" == "$valid_step" ]]; then
      is_valid_smoke_step=1
      break
    fi
  done
  if [[ "$is_valid_smoke_step" -eq 0 ]]; then
    echo "Invalid --smoke-fail-step: $SMOKE_FAIL_STEP" >&2
    echo "Allowed values: ${VALID_STEPS[*]}" >&2
    exit 2
  fi
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
  printf "STEP %s:" "$step"
  printf " %q" "$@"
  printf "\n"

  set +e
  maybe_smoke_fail "$step"
  local exit_code=$?
  if [[ "$exit_code" -eq 0 ]]; then
    "$@"
    exit_code=$?
  fi
  set -e

  echo "STEP $step: exit=$exit_code"
  return "$exit_code"
}

collect_lifecycle_tests() {
  if [[ ! -e tests ]]; then
    echo "LIFECYCLE_DISCOVERY=tests-dir-absent"
    return 3
  fi

  if [[ ! -d tests ]]; then
    echo "LIFECYCLE_DISCOVERY_ERROR=tests path exists but is not a directory: tests" >&2
    return 21
  fi

  local discovered=""
  local discovery_exit=0
  set +e
  discovered="$(find tests -type f \( -name "*lifecycle*.py" -o -path "*/lifecycle/*" \) | sort)"
  discovery_exit=$?
  set -e

  if [[ "$discovery_exit" -ne 0 ]]; then
    echo "LIFECYCLE_DISCOVERY_ERROR=find/sort failed while discovering lifecycle tests (exit=$discovery_exit)" >&2
    return "$discovery_exit"
  fi

  if [[ -n "$discovered" ]]; then
    printf '%s\n' "$discovered"
  fi
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

collect_output=""
collect_exit=0
set +e
collect_output="$(collect_lifecycle_tests)"
collect_exit=$?
set -e

lifecycle_tests=()
if [[ "$collect_exit" -eq 0 && -n "$collect_output" ]]; then
  mapfile -t lifecycle_tests <<< "$collect_output"
fi

lifecycle_count="${#lifecycle_tests[@]}"
echo "LIFECYCLE_TEST_FILES=$lifecycle_count"

if [[ "$collect_exit" -eq 3 ]]; then
  if [[ "$MODE" == "bootstrap-only" ]]; then
    echo "LIFECYCLE_SUITE=NOT PRESENT"
    exit 0
  fi

  echo "LIFECYCLE_DISCOVERY_ERROR=tests directory is missing" >&2
  echo "LIFECYCLE_SUITE=REQUIRED_BUT_MISSING" >&2
  exit 14
fi

if [[ "$collect_exit" -ne 0 ]]; then
  echo "LIFECYCLE_SUITE=DISCOVERY_FAILED" >&2
  exit "$collect_exit"
fi

if [[ "$lifecycle_count" -eq 0 ]]; then
  echo "LIFECYCLE_SUITE=REQUIRED_BUT_MISSING" >&2
  exit 14
fi

echo "LIFECYCLE_SUITE=RUNNING"
run_step lifecycle python -m pytest -q "${lifecycle_tests[@]}"
echo "LIFECYCLE_SUITE=PASS"
