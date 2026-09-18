#!/usr/bin/env bash
# Create a FASTDB user without requiring browser access to MailHog.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
NAMESPACE="${FASTDB_NAMESPACE:-fastdb-arbutus-dev}"

if [[ $EUID -eq 0 ]]; then
  echo "Error: run this script as your normal user, not with sudo." >&2
  exit 1
fi

for command_name in docker kubectl; do
  command -v "$command_name" >/dev/null || {
    echo "Error: $command_name is required." >&2
    exit 1
  }
done

docker info >/dev/null 2>&1 || {
  echo "Error: cannot access Docker as $(id -un)." >&2
  exit 1
}

kubectl get deployment/postgres --namespace "$NAMESPACE" >/dev/null || {
  echo "Error: FASTDB is not installed in namespace $NAMESPACE." >&2
  exit 1
}

# Use the same shell image as the running FASTDB deployment.
SHELL_IMAGE="$(
  kubectl get deployment/shell \
    --namespace "$NAMESPACE" \
    --output=jsonpath='{.spec.template.spec.containers[0].image}'
)"
if [[ -z "$SHELL_IMAGE" ]]; then
  echo "Error: could not determine the shell image in namespace $NAMESPACE." >&2
  exit 1
fi

docker image inspect "$SHELL_IMAGE" >/dev/null 2>&1 || {
  echo "Error: shell image not found in Docker: $SHELL_IMAGE" >&2
  echo "Pull or build this image before creating a user." >&2
  exit 1
}

read -r -p "Username [test_user]: " USERNAME
USERNAME="${USERNAME:-test_user}"
read -r -p "Display name [TESTUSER]: " DISPLAY_NAME
DISPLAY_NAME="${DISPLAY_NAME:-TESTUSER}"
read -r -p "Email [test_user@mailhog]: " EMAIL
EMAIL="${EMAIL:-test_user@mailhog}"

# make_password.py emits SQL, so restrict these fields to simple values before
# passing them to it. The password is never included in that SQL.
if [[ ! "$USERNAME" =~ ^[A-Za-z0-9_.-]+$ ]]; then
  echo "Error: username may contain only letters, numbers, '.', '_' and '-'." >&2
  exit 1
fi
if [[ ! "$DISPLAY_NAME" =~ ^[A-Za-z0-9_.[:space:]-]+$ ]]; then
  echo "Error: display name contains unsupported characters." >&2
  exit 1
fi
if [[ ! "$EMAIL" =~ ^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+$ ]]; then
  echo "Error: enter a simple email address without quotes or spaces." >&2
  exit 1
fi

if kubectl exec --namespace "$NAMESPACE" deployment/postgres -- \
  psql -U postgres -d fastdb -tAc \
    "SELECT 1 FROM authuser WHERE username = '$USERNAME'" |
  grep -Fxq 1; then
  echo "Error: user '$USERNAME' already exists." >&2
  exit 1
fi

# Never expose password-bearing commands if the script was invoked with
# tracing enabled (for example, with `bash -x`).
set +x
read -r -s -p "Password: " PASSWORD
echo
read -r -s -p "Confirm password: " PASSWORD_CONFIRMATION
echo
if (( ${#PASSWORD} < 4 )); then
  echo "Error: password must be at least 4 characters long." >&2
  exit 1
fi
if [[ "$PASSWORD" != "$PASSWORD_CONFIRMATION" ]]; then
  echo "Error: passwords do not match." >&2
  exit 1
fi

echo "Generating FASTDB authentication keys..."
INSERT_SQL="$({
  printf '%s\n' "$PASSWORD"
} | docker run --rm -i \
  --network none \
  --volume "$REPO_ROOT:/code:ro" \
  --entrypoint python \
  "$SHELL_IMAGE" \
  -c '
import runpy
import sys

script, username, email, display_name = sys.argv[1:]
password = sys.stdin.readline().rstrip("\n")
sys.argv = [
    script,
    "--username", username,
    "--email", email,
    "--displayname", display_name,
    "--password", password,
]
runpy.run_path(script, run_name="__main__")
' \
  /code/extern/rkwebutil/rkwebutil/make_password.py \
  "$USERNAME" \
  "$EMAIL" \
  "$DISPLAY_NAME")"
unset PASSWORD PASSWORD_CONFIRMATION

echo "Creating user '$USERNAME'..."
printf '%s;\n' "$INSERT_SQL" |
  kubectl exec -i --namespace "$NAMESPACE" deployment/postgres -- \
    psql -v ON_ERROR_STOP=1 -U postgres -d fastdb
unset INSERT_SQL

echo
echo "User '$USERNAME' is ready."
echo "Use the username and password you entered from any authorized FASTDB client."
echo "No client configuration or .fastdb.ini file was created by this script."
