# How to Create a User and Log In

FASTDB uses challenge-response authentication (RKAuth). A user record must
contain an RSA public key and an encrypted private key before that user can log
in. The Helm deployment provides two ways to create those credentials:

1. Use a password-reset email captured by MailHog. This is the intended local
   Kind workflow.
2. Generate the credentials locally and insert them directly into PostgreSQL.
   This is the intended single-node VM workflow.

## Local Kind: create a user through MailHog

The local workflow exercises the same browser-based password-reset process a
normal web user would use. MailHog captures the email locally instead of
sending it to a real address.

First install FASTDB using the local Kind helpers:

```bash
./helm/scripts/create-local-cluster.sh
./helm/scripts/install-local-fastdb.sh
```

Then run:

```bash
./helm/scripts/create-user-local.sh
```

The script:

1. Creates the fixed development account `test_user` in PostgreSQL.
2. Requests a password-reset email from FASTDB.
3. Reads the reset link from MailHog.
4. Opens FASTDB, the reset page, and MailHog in the browser.

Enter the desired password on the reset page. The browser generates the RSA
key pair, encrypts the private key with the password, and stores the resulting
credentials in FASTDB.

The local services are available at:

| Service | URL |
|---|---|
| FASTDB | `http://localhost:8080` |
| MailHog | `http://localhost:8025` |

If `test_user` already exists, use the **Request Password Reset** option on the
FASTDB login page rather than trying to create the account again.

## Single-node VM: insert a user directly

A VM may not have browser access or a public route to its FASTDB and MailHog
services. In that case, create a complete user record directly from the VM:

```bash
./helm/scripts/create-user-singlenode.sh
```

By default, the script operates in the `fastdb-arbutus-dev` namespace. Set a
different namespace when necessary:

```bash
FASTDB_NAMESPACE=<namespace> ./helm/scripts/create-user-singlenode.sh
```

The script prompts for the username, display name, email, and password. It
then:

1. Uses the FASTDB shell image to generate the RKAuth keys and SQL.
2. Sends the generated SQL directly to PostgreSQL through `kubectl exec`.
3. Removes the password and generated SQL from its shell variables.

The password is read without displaying it and is not included in the SQL or
passed as a command-line argument. This workflow does not require MailHog or a
browser-accessible password-reset link.

After either workflow completes, log in with the username and password that
were created. Applications using the FASTDB Python client will also need those
credentials in their own `.fastdb.ini` configuration or secret-management
system; neither user-creation script creates client configuration.

## Manual MailHog reset

To reset an existing user's password manually:

1. Open the FASTDB login page.
2. Select **Request Password Reset**.
3. Enter the username or email address.
4. Open MailHog and follow the link in the captured message.
5. Enter and confirm the new password.

This requires `mailhog.enabled: true` and network access to both FASTDB and the
MailHog web interface.
