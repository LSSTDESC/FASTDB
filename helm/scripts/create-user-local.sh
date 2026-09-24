#!/usr/bin/env bash
# Create the fixed local test user and open its password-reset page.
set -euo pipefail

CLUSTER="fastdb-local"
NAMESPACE="$CLUSTER"
CONTEXT="kind-$CLUSTER"
WEB_URL="http://localhost:8080"
MAILHOG_URL="http://localhost:8025"

USERNAME="test_user"
DISPLAY_NAME="TESTUSER"
EMAIL="test_user@mailhog"

for command_name in kubectl curl python3 open; do
  command -v "$command_name" >/dev/null || {
    echo "Error: $command_name is required." >&2
    exit 1
  }
done

echo "Username: $USERNAME"

kubectl --context "$CONTEXT" get namespace "$NAMESPACE" >/dev/null

if kubectl --context "$CONTEXT" exec -i -n "$NAMESPACE" deployment/postgres -- \
  psql -U postgres -d fastdb -tAc \
    "SELECT 1 FROM authuser WHERE username = '$USERNAME'" | grep -Fxq '1'; then
  echo "Error: user '$USERNAME' already exists." >&2
  echo "Use the password-reset page at $WEB_URL to set a new password." >&2
  exit 1
fi

kubectl --context "$CONTEXT" exec -i -n "$NAMESPACE" deployment/postgres -- \
  psql -U postgres -d fastdb \
    -c "INSERT INTO authuser (username, displayname, email)
        VALUES ('$USERNAME', '$DISPLAY_NAME', '$EMAIL');"

MESSAGES_JSON="$(mktemp)"
trap 'rm -f "$MESSAGES_JSON"' EXIT

MAILHOG_READY=false
for _ in {1..30}; do
  if curl -fsS "$MAILHOG_URL/api/v2/messages" >"$MESSAGES_JSON"; then
    MAILHOG_READY=true
    break
  fi
  sleep 1
done

if [[ "$MAILHOG_READY" != true ]]; then
  echo "Error: MailHog is not reachable at $MAILHOG_URL." >&2
  exit 1
fi

curl -fsS -X POST "$WEB_URL/auth/getpasswordresetlink" \
  -H "Content-Type: application/json" \
  --data "{\"username\":\"$USERNAME\"}" >/dev/null

RESET_URL=""
for _ in {1..30}; do
  curl -fsS "$MAILHOG_URL/api/v2/messages" >"$MESSAGES_JSON"

  RESET_URL="$(
    python3 - "$MESSAGES_JSON" "$USERNAME" <<'PY'
import json
import re
import sys

message_file, username = sys.argv[1:]
with open(message_file, encoding="utf-8") as stream:
    messages = json.load(stream)

for message in messages.get("items", []):
    text = (
        message.get("Content", {}).get("Body", "")
        + "\n"
        + message.get("Raw", {}).get("Data", "")
    )
    if f"password reset for {username}" not in text:
        continue

    match = re.search(
        r"https?://[^\s<]+/auth/resetpassword\?uuid=[0-9a-f-]+",
        text,
    )
    if match:
        print(match.group(0))
        break
PY
  )"

  [[ -n "$RESET_URL" ]] && break
  sleep 1
done

if [[ -z "$RESET_URL" ]]; then
  echo "Error: no password-reset link found in MailHog." >&2
  exit 1
fi

# A locally rebuilt install should already generate HTTP links.  Retain this
# fallback for clusters built before the local external URL was configured.
if [[ "$RESET_URL" == https://* ]]; then
  RESET_URL="http://${RESET_URL#https://}"
fi

echo "Password-reset URL: $RESET_URL"
echo "Set the password manually in the browser."
open "$WEB_URL"
open "$RESET_URL"
open "$MAILHOG_URL"
