#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
context="arn:aws:eks:eu-central-1:047452524847:cluster/sdlc-marketing"
namespace="lakehouse-admin"
app_name="rewrite-data-files"

if kubectl --context "${context}" -n "${namespace}" get sparkapplication.spark.apache.org "${app_name}" >/dev/null 2>&1; then
  printf 'Deleting existing SparkApplication %s\n' "${app_name}"
  kubectl --context "${context}" -n "${namespace}" delete sparkapplication.spark.apache.org "${app_name}"
  kubectl --context "${context}" -n "${namespace}" wait --for=delete "sparkapplication.spark.apache.org/${app_name}" --timeout=120s
fi

kubectl apply --context "${context}" -n "${namespace}" -f "${script_dir}/rewrite-data-files.yaml"

printf 'Following logs for %s with stern\n' "${app_name}"
exec stern --context "${context}" -n "${namespace}" "^${app_name}.*-driver$"
