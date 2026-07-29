# Google Cloud setup

These steps use only the Google Cloud Console. They do not depend on the
account currently selected by the local `gcloud` CLI.

## 1. Identify the application service account

Open the local service-account JSON and copy only its `client_email` and
`project_id` values. Do not paste or upload its `private_key` anywhere.

In Google Cloud Console, select that project and open **IAM & Admin → IAM**.
Find the `client_email` principal and grant:

- **Cloud SQL Client**
- **Cloud SQL Instance User**
- the existing Cloud Storage role used to write the ingestion bucket

The local JSON remains a development fallback. When the collector moves to
Compute Engine, attach this service account to the VM instead of copying the
key file to the VM.

## 2. Enable APIs

Open **APIs & Services → Library** and enable:

- Cloud SQL Admin API
- Compute Engine API
- Cloud Storage API

Secret Manager and Artifact Registry can be enabled when deployment secrets
and container publishing are added.

## 3. Create Cloud SQL PostgreSQL

Open **SQL → Create instance → PostgreSQL** and use:

- Instance ID: `sportsbettor-postgres`
- PostgreSQL: version 16 or the current default supported version
- Edition: Enterprise
- Region: the same region that will contain the Compute Engine VM
- Availability: single zone initially
- Machine: the smallest available development/shared-core configuration
- Storage: 10 GB with automatic storage increases enabled
- Connections: public IP enabled
- Authorized networks: none
- Database flag: `cloudsql.iam_authentication=on`
- Automated backups: enabled

Save the generated `postgres` administrator password in a password manager.
The application does not use this password.

After creation, copy the **Connection name** from the instance Overview page.
It has the form:

```text
project-id:region:sportsbettor-postgres
```

## 4. Create the database and IAM database user

On the instance:

1. Open **Databases → Create database**.
2. Create `sportsbettor`.
3. Open **Users → Add user account**.
4. Select **Cloud IAM** authentication.
5. Select or enter the service account from step 1.

For PostgreSQL connections, the service-account database username is its email
without the `.gserviceaccount.com` suffix. For example:

```text
collector@project-id.iam.gserviceaccount.com
```

becomes:

```text
collector@project-id.iam
```

## 5. Grant schema privileges

Open **Cloud SQL Studio**, select the `sportsbettor` database, and authenticate
as the built-in `postgres` administrator. Run the following after replacing
the quoted role:

```sql
GRANT CONNECT ON DATABASE sportsbettor
TO "collector@project-id.iam";

GRANT USAGE, CREATE ON SCHEMA public
TO "collector@project-id.iam";
```

The `CREATE` privilege lets the service account run Alembic migrations. It does
not make the account a PostgreSQL superuser.

## 6. Configure and migrate locally

Copy the Cloud SQL settings from `.env.example` into `src/.env`:

```text
CLOUD_SQL_INSTANCE_CONNECTION_NAME=project-id:region:sportsbettor-postgres
POSTGRES_DB=sportsbettor
CLOUD_SQL_IAM_USER=collector@project-id.iam
CLOUD_SQL_IP_TYPE=PUBLIC
GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/ai-sports-bettor-559e8837739f.json
```

Then install dependencies and apply the schema:

```bash
python -m pip install -e .
python -m alembic upgrade head
```

The migration creates:

- `raw_ingest_objects`
- `news_events`
- `news_media`
- `ingest_cursors`

Start ingestion with:

```bash
python -m src.ingest_news.X_pull
```

## 7. Later: create the ingestion VM

Open **Compute Engine → VM instances → Create instance**:

- Region: the same region as Cloud SQL
- Machine type: `e2-small` initially
- Boot disk: current Debian or Ubuntu LTS
- Service account: the application service account from step 1
- Access scopes: allow full access to Cloud APIs and restrict access with IAM
- Firewall: do not enable public HTTP or HTTPS
- Availability policy: automatic restart enabled

On the VM, do not copy the JSON key. The Cloud SQL connector and GCS client use
the attached service account through Application Default Credentials. Set:

```text
CLOUD_SQL_INSTANCE_CONNECTION_NAME=project-id:region:sportsbettor-postgres
POSTGRES_DB=sportsbettor
CLOUD_SQL_IAM_USER=collector@project-id.iam
CLOUD_SQL_IP_TYPE=PUBLIC
```

The X and future Polymarket collectors should run as separate containers or
`systemd` services with automatic restart. Their durable state remains in
Cloud SQL and GCS, so replacement of the VM does not lose ingestion state.
