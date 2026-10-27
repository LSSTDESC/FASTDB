{{/*
Expand the name of the chart.
*/}}
{{- define "fastdb.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "fastdb.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "fastdb.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Namespace
*/}}
{{- define "fastdb.namespace" -}}
{{- .Values.global.namespace }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "fastdb.labels" -}}
helm.sh/chart: {{ include "fastdb.chart" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Build image reference from registry, repository, and tag
Usage: {{ include "fastdb.image" (dict "image" .Values.postgres.image "global" .Values.global) }}
*/}}
{{- define "fastdb.image" -}}
{{- $registry := .global.imageRegistry -}}
{{- $repository := .image.repository -}}
{{- $tag := default .global.imageTag .image.tag -}}
{{- if $registry -}}
{{- printf "%s/%s:%s" $registry $repository $tag -}}
{{- else -}}
{{- printf "%s:%s" $repository $tag -}}
{{- end -}}
{{- end }}

{{/*
Build image reference for external images (like mailhog)
Usage: {{ include "fastdb.externalImage" .Values.mailhog.image }}
*/}}
{{- define "fastdb.externalImage" -}}
{{- $registry := default "" .registry -}}
{{- $repository := .repository -}}
{{- $tag := default "latest" .tag -}}
{{- if $registry -}}
{{- printf "%s/%s:%s" $registry $repository $tag -}}
{{- else -}}
{{- printf "%s:%s" $repository $tag -}}
{{- end -}}
{{- end }}

{{/*
Image pull secrets
*/}}
{{- define "fastdb.imagePullSecrets" -}}
{{- if .Values.global.imagePullSecrets }}
imagePullSecrets:
{{- range .Values.global.imagePullSecrets }}
  - name: {{ .name }}
{{- end }}
{{- end }}
{{- end }}

{{/*
PVC name helper
Usage: {{ include "fastdb.pvcName" (dict "namespace" .Values.global.namespace "component" "postgres") }}
*/}}
{{- define "fastdb.pvcName" -}}
{{- printf "%s-%s-pvc" .namespace .component -}}
{{- end }}

{{/* Wait for a prerequisite Job using Python's standard library and the pod's
service account. A failed Job stops the dependent workload with a clear error. */}}
{{- define "fastdb.waitForJob" -}}
/venv/bin/python -u - <<'PY'
import json
import pathlib
import ssl
import time
import urllib.error
import urllib.request

account = pathlib.Path('/var/run/secrets/kubernetes.io/serviceaccount')
namespace = (account / 'namespace').read_text().strip()
token = (account / 'token').read_text().strip()
context = ssl.create_default_context(cafile=str(account / 'ca.crt'))
job = {{ . | quote }}
url = f'https://kubernetes.default.svc/apis/batch/v1/namespaces/{namespace}/jobs/{job}'
print(f'Waiting for Job {job} to complete...')
while True:
    request = urllib.request.Request(url, headers={'Authorization': f'Bearer {token}'})
    try:
        with urllib.request.urlopen(request, context=context, timeout=30) as response:
            status = json.load(response).get('status', {})
    except urllib.error.HTTPError as error:
        if error.code != 404:
            raise
    else:
        conditions = {item['type']: item['status'] for item in status.get('conditions', [])}
        if conditions.get('Complete') == 'True':
            print(f'Job {job} completed.')
            break
        if conditions.get('Failed') == 'True':
            raise SystemExit(f'Prerequisite Job {job} failed; inspect its logs.')
    time.sleep(5)
PY
{{- end }}
