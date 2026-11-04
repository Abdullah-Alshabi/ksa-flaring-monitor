#!/usr/bin/env bash
# =============================================================================
# Build and deploy the extractor as a Cloud Run Job, then run the first loads.
# Run in Cloud Shell from the repository root:   bash infra/deploy_extractor.sh
# Safe to re-run (redeploys the latest code).
# =============================================================================
set -euo pipefail
PROJECT_ID="$(gcloud config get-value project 2>/dev/null)"
REGION="us-central1"
JOB="firms-extractor"
PROJECT_NUMBER="$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')"
EXTRACTOR_SA="extractor-sa@${PROJECT_ID}.iam.gserviceaccount.com"
BUILD_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

say() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }
[[ "$PROJECT_ID" == ksa-flaring-* ]] || { echo "Active project is '$PROJECT_ID'. Run: gcloud config set project ksa-flaring-01" >&2; exit 1; }

say "1/3 Let Cloud Build build the container (default build account needs the Cloud Run builder role)"
gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:${BUILD_SA}" \
  --role="roles/run.builder" --condition=None >/dev/null
echo "  ${BUILD_SA} -> roles/run.builder"

say "2/3 Build + deploy Cloud Run Job '${JOB}' (3-5 minutes the first time)"
gcloud run jobs deploy "$JOB" \
  --source extractor \
  --region "$REGION" \
  --service-account "$EXTRACTOR_SA" \
  --set-secrets "FIRMS_KEY=firms-map-key:latest" \
  --set-env-vars "GOOGLE_CLOUD_PROJECT=${PROJECT_ID},REGION=${REGION},BUCKET=${PROJECT_ID}-raw,BACKFILL_START=2024-01-01" \
  --memory 2Gi --cpu 1 --task-timeout 3600 --max-retries 1 \
  --quiet

say "3/3 First loads: World Bank flare sites, then the NASA backfill (about 10-20 minutes)"
gcloud run jobs execute "$JOB" --region "$REGION" --args wb_sites --wait
gcloud run jobs execute "$JOB" --region "$REGION" --args backfill --wait

say "Done. Row counts:"
bq query --use_legacy_sql=false --format=pretty \
 "SELECT dataset, product, source, status, COUNT(*) files, SUM(row_count) rows
  FROM \`${PROJECT_ID}.ops.ingest_manifest\` GROUP BY 1,2,3,4 ORDER BY 1,2,3,4"
