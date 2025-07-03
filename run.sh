#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# ----------------------------
RUNNER="DirectRunner" # DataflowRunner or DirectRunner
GCP_PROJECT_ID="ml6-code-test-464510"                    
GCP_REGION="us-east1"                               
TEMP_LOCATION="gs://ml6-test-results/tmp/"               
STAGING_LOCATION="gs://ml6-test-results/staging/"
OUTPUT_LOCATION="gs://ml6-test-results/output_1/"       
JOB_NAME="my-dataflow-job-$(date +%Y%m%d-%H%M%S)"    
REQUIREMENTS_FILE="requirements.txt"

# ----------------------------
# Run the pipeline
# ----------------------------

python my_pipeline/main.py \
  --runner "$RUNNER" \
  --project "$GCP_PROJECT_ID" \
  --region "$GCP_REGION" \
  --temp_location "$TEMP_LOCATION" \
  --staging_location "$STAGING_LOCATION" \
  --requirements_file "$REQUIREMENTS_FILE" \
  --job_name "$JOB_NAME"\
  --output_location "$OUTPUT_LOCATION"

# ----------------------------
# End of script
# ----------------------------
