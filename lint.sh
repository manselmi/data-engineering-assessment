#!/usr/bin/env -S -- zsh -f

# Stop at any error, treat unset vars as errors and make pipelines exit
# with a non-zero exit code if any command in the pipeline exits with a
# non-zero exit code.
set -o ERR_EXIT
set -o NO_UNSET
set -o PIPE_FAIL


THIS_DIR="${0:a:h}"
pushd -q -- "${THIS_DIR}"


flake8
isort --check-only --diff --quiet -- .
