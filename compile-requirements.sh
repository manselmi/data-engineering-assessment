#!/usr/bin/env -S -- zsh -f

# Stop at any error, treat unset vars as errors and make pipelines exit
# with a non-zero exit code if any command in the pipeline exits with a
# non-zero exit code.
set -o ERR_EXIT
set -o NO_UNSET
set -o PIPE_FAIL


die() {
  >&2 printf "${@}"
  exit 1
}


PIP_TOOLS_URL=https://github.com/jazzband/pip-tools


REQUIREMENTS_IN="${1-}"
if [[ -z "${REQUIREMENTS_IN}" ]]; then
  die \
    'ERROR: expects one argument, a requirements*.in file: %s\n'
    "${PIP_TOOLS_URL}"
fi
if ! command -v pip-compile &>/dev/null; then
  die \
    'ERROR: ensure pip-compile is available: %s\n'
    "${PIP_TOOLS_URL}"
fi


CUSTOM_COMPILE_COMMAND="./compile-requirements.sh ${REQUIREMENTS_IN}" \
  pip-compile \
    --annotate \
    --build-isolation \
    --generate-hashes \
    --header \
    --no-emit-index-url \
    --no-emit-trusted-host \
    --verbose \
    -- \
    "${REQUIREMENTS_IN}"
