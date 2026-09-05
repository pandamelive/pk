#!/bin/bash
set -e

RUNNER_NAME="${RUNNER_NAME:-docker-runner-$(hostname)}"
RUNNER_LABELS="${RUNNER_LABELS:-docker,rust,self-hosted}"
RUNNER_GROUP="${RUNNER_GROUP:-Default}"

echo "=== GitHub Actions Self-Hosted Runner ==="
echo "Repository: ${REPO_URL}"
echo "Runner name: ${RUNNER_NAME}"
echo "Labels: ${RUNNER_LABELS}"

# 注册 runner
echo "Registering runner..."
/opt/runner/config.sh \
    --unattended \
    --url "${REPO_URL}" \
    --token "${RUNNER_TOKEN}" \
    --name "${RUNNER_NAME}" \
    --labels "${RUNNER_LABELS}" \
    --runnergroup "${RUNNER_GROUP}" \
    --replace

# 退出时自动注销
cleanup() {
    echo ""
    echo "Removing runner..."
    /opt/runner/config.sh remove --unattended --token "${RUNNER_TOKEN}" || true
    exit 0
}
trap cleanup SIGTERM SIGINT

# 启动 runner
echo "Starting runner..."
/opt/runner/run.sh
