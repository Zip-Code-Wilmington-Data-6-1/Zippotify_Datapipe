# Local Setup: Docker + Postgres + S3 Wrapper

This guide shows how to run the backend stack (API + Postgres) and verify S3 access locally after pulling the dev branch.

## 1. Prerequisites

Install:
- Docker + Docker Compose
- Python 3.11
- AWS CLI v2
- (Optional) Make sure you are added to the IAM user/role with S3 bucket access

Clone / update:
```bash
git checkout dev
git pull
```

## 2. AWS Profile (No Secrets in Repo)

Configure a local AWS profile (example profile name: tracktionai-dev):
```bash
aws configure --profile tracktionai-dev
# Enter: Access Key, Secret, Region: us-east-1, Output: json
# OR if using SSO:
# aws configure sso --profile tracktionai-dev
# aws sso login --profile tracktionai-dev
```

Export (optional) for convenience:
```bash
echo 'export AWS_PROFILE=tracktionai-dev' >> ~/.zshrc
echo 'export AWS_REGION=us-east-1' >> ~/.zshrc
source ~/.zshrc
aws sts get-caller-identity   # should succeed
```

If you see “profile not found” create/login to it first.

## 3. S3 Naming / Paths

Shared bucket (current integration):
- Bucket: zcw-students-projects
- Base prefix for our project: data/tracktionai/
- Raw listen events layout:
  data/tracktionai/Raw/listen_events/year=YYYY/month=MM/day=DD/hour=HH/part-*.jsonl

Wrapper defaults (see src/io/s3_client.py):
- BUCKET: zcw-students-projects
- BASE_PREFIX: data/tracktionai/

You do NOT need to change these unless using a different environment. Do not add secrets to .env for S3.

## 4. Python Environment (Host)

Create / update a virtual environment (only needed if you run scripts outside Docker):
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.api.txt  # or requirements.txt if unified
```

Quick smoke (host):
```bash
python -m src.io.s3_smoke
```
Expected output: list with a key and "ok".

## 5. Docker Services

Files:
- Dockerfile (builds API image)
- docker-compose.yml (db + api)
- .dockerignore (reduces build context)

Start:
```bash
docker compose up -d --build
docker compose ps
```

API depends on DB healthcheck. First build may take a few minutes.

## 6. Health + DB Verification

Host → API:
```bash
curl -s http://localhost:8000/health
curl -s http://localhost:8000/health/db
```
Expected: {"status":"ok"} and {"db":true}

Inside container:
```bash
docker compose exec api python - <<'PY'
from sqlalchemy import text, create_engine
import os
print("DB URL:", os.getenv("DATABASE_URL"))
engine = create_engine(os.getenv("DATABASE_URL"), future=True)
with engine.connect() as c:
    print("SELECT 1 ->", c.execute(text("SELECT 1")).scalar())
PY
```

## 7. S3 Verification (Container)

Host AWS creds are mounted read-only into the container at /root/.aws.
```bash
docker compose exec api python - <<'PY'
from src.io.s3_client import put_text, list_keys, get_bytes
put_text("Raw/listen_events/smoke/docker-test.txt", "hi-docker")
print("Recent keys:", list_keys("Raw/listen_events/smoke/")[-3:])
print("Content:", get_bytes("Raw/listen_events/smoke/docker-test.txt").decode())
PY
```

CLI (optional):
```bash
aws s3 ls s3://zcw-students-projects/data/tracktionai/Raw/listen_events/ --recursive | head
```

## 8. Adding New Raw Data

Example upload (UTC partition):
```bash
UTC_DATE=$(date -u +year=%Y/month=%m/day=%d/hour=%H)
aws s3 cp data/sample/events_small_combined.jsonl \
  s3://zcw-students-projects/data/tracktionai/Raw/listen_events/${UTC_DATE}/part-00000.jsonl
```

## 9. Common Issues / Fixes

| Symptom | Cause | Fix |
|---------|-------|-----|
| AccessDenied listing all buckets | No s3:ListAllMyBuckets | Use full path: aws s3 ls s3://bucket/prefix/ |
| Profile not found | AWS_PROFILE exported but not created | aws configure --profile tracktionai-dev |
| S3 smoke fails in container | Missing profile / SSO login | Run aws sso login (if SSO) on host; restart container |
| API can’t reach DB | Using localhost in DATABASE_URL inside container | Must use host db service name: db |
| Huge Docker build context | data/ not ignored | Confirm .dockerignore includes data/ |

View logs:
```bash
docker compose logs api --tail 200
docker compose logs db --tail 100
```

## 10. Updating Dependencies

If adding a library:
- Add to requirements.api.txt (or requirements.txt)
- Rebuild: docker compose build api --no-cache
- Restart: docker compose up -d

## 11. Cleanup

Stop services:
```bash
docker compose down
```
Remove volumes (destroys DB data):
```bash
docker compose down -v
```

## 12. Do / Don’t

Do:
- Use AWS profiles (no hardcoded keys)
- Keep bucket + prefix consistent
- Commit code only (no credentials, large data, or secrets)

Don’t:
- Commit ~/.aws
- Put secrets in .env for S3
- Force push dev without coordination

## 13. Next (Optional Roadmap)

- Add Bronze → Silver transform (JSONL → Parquet in Silver/)
- Add unit tests for S3 wrapper
- Introduce Makefile targets (make up, make s3-smoke)
- Later: dedicated tracktionai-dev-data bucket

## Quick Command Summary

```bash
# After pull
aws sts get-caller-identity
python -m src.io.s3_smoke
docker compose up -d --build
curl -s http://localhost:8000/health
docker compose exec api python -m src.io.s3_smoke
```

Reach out if any step fails with the exact command + error