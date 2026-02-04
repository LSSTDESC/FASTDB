# How to Create a User and Log In

FASTDB uses challenge-response authentication (RKAuth). Users must exist
in the `authuser` PostgreSQL table before they can log in. A password is
set through a reset-password link delivered by email (captured by Mailhog
in development).

## Prerequisites

- Mailhog must be enabled in your deployment so the password-reset email
  can be captured. In Helm values, set `mailhog.enabled: true`.
- The `webap` pod must be running.

## 1. Create the user in PostgreSQL

Open a psql session on the primary Postgres pod and insert a row into
`authuser`. Only `username`, `displayname`, and `email` are required;
the password fields (`pubkey`, `privkey`) are populated later via the
password-reset flow.

### Kubernetes (Helm deployment)

```bash
# Replace <namespace> with your namespace (e.g. fastdb-local, ccosta-dev)
kubectl exec -it -n <namespace> deployment/postgres -- \
  psql -U postgres -d fastdb -c \
  "INSERT INTO authuser (username, displayname, email)
   VALUES ('<username>', '<Display Name>', '<user>@mailhog');"
```

### Docker Compose (local development)

```bash
docker compose exec postgres \
  psql -U postgres -d fastdb -c \
  "INSERT INTO authuser (username, displayname, email)
   VALUES ('<username>', '<Display Name>', '<user>@mailhog');"
```

Replace `<username>`, `<Display Name>`, and `<user>` with the desired
values. The email domain does not matter for Mailhog — anything will be
delivered — but `<user>@mailhog` is the convention used in tests.

## 2. Trigger a password-reset email

1. Open the FASTDB web application in your browser.
2. On the login page, click **"Request Password Reset"**.
3. Enter either the **username** or **email** you used in step 1.
4. Click **"Email Password Reset Link"**.

The application sends an email containing a password-reset URL to the
address on file. In development this email is captured by Mailhog.

## 3. Retrieve the reset link from Mailhog

### Option A: Mailhog Web UI

If Mailhog has external access enabled, open the web UI in a browser:

| Deployment | URL |
|---|---|
| Docker Compose | `http://localhost:8025` |
| Kind (NodePort) | `http://localhost:30025` |

Find the email titled **"fastdb password reset"** and copy the reset
URL from the message body.

### Option B: kubectl logs

If the Mailhog web UI is not exposed, the reset URL appears in the
pod logs:

```bash
# Kubernetes
kubectl logs -n <namespace> deployment/mailhog | grep resetpassword
```

```bash
# Docker Compose
docker compose logs mailhog | grep resetpassword
```

The log line contains a URL of the form:

```
https://<host>/auth/resetpassword?uuid=<uuid>
```

## 4. Set the password

1. Open the reset URL from step 3 in your browser.
2. Enter and confirm a new password.
3. Click the submit button.

The browser generates an RSA key pair, encrypts the private key with
your password, and stores both keys in the database. You can now log in
with your username and password.

## Quick-reference: full workflow in one go

```bash
NAMESPACE=fastdb-local   # adjust to your namespace

# Create user
kubectl exec -it -n "$NAMESPACE" deployment/postgres -- \
  psql -U postgres -d fastdb -c \
  "INSERT INTO authuser (username, displayname, email)
   VALUES ('alice', 'Alice Developer', 'alice@mailhog');"

# (In browser: go to the login page → "Request Password Reset" →
#  enter "alice" → "Email Password Reset Link")

# Grab the reset URL from mailhog logs
kubectl logs -n "$NAMESPACE" deployment/mailhog | grep resetpassword
```
