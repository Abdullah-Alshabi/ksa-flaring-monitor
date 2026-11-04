#!/usr/bin/env bash
# =============================================================================
# KSA Flaring Monitor: Google Cloud setup (Day 1)
# Run in Cloud Shell:   bash setup_gcp.sh <project-id>
# Safe to re-run: every step skips what already exists.
# =============================================================================
set -euo pipefail

PROJECT_ID="${1:?Usage: bash setup_gcp.sh <project-id>   (6-30 chars, lowercase letters, digits, hyphens)}"
REGION="us-central1"
BUCKET="${PROJECT_ID}-raw"        # raw files from NASA / World Bank, never edited
BUDGET_USD=50
SECRET="firms-map-key"

say() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }

# ---------------------------------------------------------------------------
say "1/8 Project"
if gcloud projects describe "$PROJECT_ID" >/dev/null 2>&1; then
  echo "Project $PROJECT_ID already exists."
else
  gcloud projects create "$PROJECT_ID" --name="KSA Flaring Monitor"
fi
BILLING_ID="$(gcloud billing accounts list --filter='open=true' --format='value(name.basename())' --limit=1)"
[[ -n "$BILLING_ID" ]] || { echo "No open billing account found." >&2; exit 1; }
CURRENT_BILLING="$(gcloud billing projects describe "$PROJECT_ID" --format='value(billingAccountName.basename())' 2>/dev/null || true)"
if [[ "$CURRENT_BILLING" == "$BILLING_ID" ]]; then
  echo "Billing already linked."
else
  gcloud billing projects link "$PROJECT_ID" --billing-account="$BILLING_ID" >/dev/null
fi
gcloud config set project "$PROJECT_ID" >/dev/null
PROJECT_NUMBER="$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')"
echo "Project: $PROJECT_ID (number $PROJECT_NUMBER), billing: $BILLING_ID"

# ---------------------------------------------------------------------------
say "2/8 Enabling APIs (1-2 minutes)"
gcloud services enable \
  bigquery.googleapis.com storage.googleapis.com dataform.googleapis.com \
  run.googleapis.com cloudscheduler.googleapis.com artifactregistry.googleapis.com \
  cloudbuild.googleapis.com secretmanager.googleapis.com monitoring.googleapis.com \
  logging.googleapis.com billingbudgets.googleapis.com iam.googleapis.com \
  iamcredentials.googleapis.com

# ---------------------------------------------------------------------------
say "3/8 Cloud Storage bucket: gs://$BUCKET"
if gcloud storage buckets describe "gs://$BUCKET" >/dev/null 2>&1; then
  echo "Bucket already exists."
else
  gcloud storage buckets create "gs://$BUCKET" --location="$REGION" \
    --uniform-bucket-level-access --public-access-prevention
fi

# ---------------------------------------------------------------------------
say "4/8 BigQuery datasets: bronze, silver, gold, ops (location $REGION)"
for DS in bronze silver gold ops; do
  if bq --project_id="$PROJECT_ID" show --dataset "$PROJECT_ID:$DS" >/dev/null 2>&1; then
    echo "Dataset $DS already exists."
  else
    bq --project_id="$PROJECT_ID" --location="$REGION" mk --dataset \
      --description="KSA flaring monitor: $DS layer" "$PROJECT_ID:$DS"
  fi
done

# ---------------------------------------------------------------------------
say "5/8 Service accounts (one per job, least privilege)"
create_sa() {
  if gcloud iam service-accounts describe "$1@${PROJECT_ID}.iam.gserviceaccount.com" >/dev/null 2>&1; then
    echo "Service account $1 already exists."
  else
    gcloud iam service-accounts create "$1" --display-name="$2"
  fi
}
create_sa extractor-sa "Cloud Run extractor: NASA FIRMS + World Bank -> GCS, starts Dataform"
create_sa dataform-sa  "Dataform workflow execution"
create_sa scheduler-sa "Cloud Scheduler: starts the extractor job"
EXTRACTOR_SA="extractor-sa@${PROJECT_ID}.iam.gserviceaccount.com"
DATAFORM_SA="dataform-sa@${PROJECT_ID}.iam.gserviceaccount.com"
SCHEDULER_SA="scheduler-sa@${PROJECT_ID}.iam.gserviceaccount.com"
sleep 15

bind_project() { gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:$1" --role="$2" --condition=None >/dev/null; echo "  $1 -> $2 (project)"; }
bind_bucket()  { gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" --member="serviceAccount:$1" --role="$2" >/dev/null; echo "  $1 -> $2 (bucket)"; }

# Extractor: write raw files, load them into bronze, start Dataform
bind_bucket  "$EXTRACTOR_SA" "roles/storage.objectAdmin"
bind_project "$EXTRACTOR_SA" "roles/bigquery.jobUser"
bind_project "$EXTRACTOR_SA" "roles/dataform.editor"
bind_project "$EXTRACTOR_SA" "roles/bigquery.dataEditor"   # loads bronze + writes ops.ingest_manifest

# Dataform: read raw files, build silver/gold
bind_bucket  "$DATAFORM_SA" "roles/storage.objectViewer"
bind_project "$DATAFORM_SA" "roles/bigquery.dataEditor"
bind_project "$DATAFORM_SA" "roles/bigquery.jobUser"

# ---------------------------------------------------------------------------
say "6/8 Secret for the NASA FIRMS MAP_KEY"
if ! gcloud secrets describe "$SECRET" >/dev/null 2>&1; then
  gcloud secrets create "$SECRET" --replication-policy=automatic >/dev/null
  echo "  Created secret $SECRET."
fi
if [[ "$(gcloud secrets versions list "$SECRET" --filter='state=enabled' --format='value(name)' | wc -l)" -eq 0 ]]; then
  echo "  Paste your FIRMS MAP_KEY now (it will not be shown), then press Enter:"
  read -rs KEY_INPUT
  printf '%s' "$KEY_INPUT" | gcloud secrets versions add "$SECRET" --data-file=- >/dev/null
  unset KEY_INPUT
  echo "  Key stored in Secret Manager."
else
  echo "  Secret already has a value."
fi
gcloud secrets add-iam-policy-binding "$SECRET" --member="serviceAccount:$EXTRACTOR_SA" \
  --role="roles/secretmanager.secretAccessor" >/dev/null
echo "  extractor-sa -> secretAccessor on $SECRET"

# ---------------------------------------------------------------------------
say "7/8 Dataform service agent can act as dataform-sa"
gcloud beta services identity create --service=dataform.googleapis.com --project="$PROJECT_ID" >/dev/null 2>&1 || true
DATAFORM_AGENT="service-${PROJECT_NUMBER}@gcp-sa-dataform.iam.gserviceaccount.com"
if gcloud iam service-accounts add-iam-policy-binding "$DATAFORM_SA" \
     --member="serviceAccount:$DATAFORM_AGENT" --role="roles/iam.serviceAccountTokenCreator" >/dev/null 2>&1; then
  echo "  $DATAFORM_AGENT -> serviceAccountTokenCreator on dataform-sa"
else
  echo "  Dataform agent not ready yet (normal): re-run this script after creating the Dataform repository."
fi

# ---------------------------------------------------------------------------
say "8/8 Budget alert: ${BUDGET_USD} USD (emails at 40% and 100%)"
if gcloud billing budgets list --billing-account="$BILLING_ID" --format='value(displayName)' 2>/dev/null \
     | grep -qx "ksa-flaring-budget"; then
  echo "Budget already exists."
else
  gcloud billing budgets create --billing-account="$BILLING_ID" --display-name="ksa-flaring-budget" \
    --budget-amount="${BUDGET_USD}USD" --filter-projects="projects/${PROJECT_ID}" \
    --threshold-rule=percent=0.4 --threshold-rule=percent=1.0
fi

say "Done. Summary:"
echo "  Project:   $PROJECT_ID   Region: $REGION"
echo "  Bucket:    gs://$BUCKET"
echo "  Datasets:  bronze, silver, gold, ops"
echo "  Accounts:  $EXTRACTOR_SA"
echo "             $DATAFORM_SA"
echo "             $SCHEDULER_SA"
echo "  Secret:    $SECRET (value hidden)"
echo "  Budget:    ${BUDGET_USD} USD, alerts at 20 and 50 USD"
