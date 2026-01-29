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
