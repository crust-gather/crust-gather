{{/*
Expand the name of the chart.
*/}}
{{- define "crust-gather.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "crust-gather.fullname" -}}
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
{{- define "crust-gather.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Built-in chart labels.
*/}}
{{- define "crust-gather.defaultLabels" -}}
helm.sh/chart: {{ include "crust-gather.chart" . }}
{{ include "crust-gather.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Common labels merged with user-provided labels.
*/}}
{{- define "crust-gather.labels" -}}
{{- $labels := include "crust-gather.defaultLabels" . | fromYaml | default dict -}}
{{- with .Values.labels }}
{{- $labels = merge $labels . -}}
{{- end }}
{{- toYaml $labels -}}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "crust-gather.selectorLabels" -}}
app.kubernetes.io/name: {{ include "crust-gather.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "crust-gather.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "crust-gather.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Validate OCI auth settings.
*/}}
{{- define "crust-gather.validateOCIAuth" -}}
{{- $token := .Values.token | default "" -}}
{{- $username := .Values.username | default "" -}}
{{- $password := .Values.password | default "" -}}
{{- if and $token (or $username $password) -}}
{{- fail "token conflicts with username/password" -}}
{{- end -}}
{{- if or (and $username (not $password)) (and $password (not $username)) -}}
{{- fail "username and password must be set together" -}}
{{- end -}}
{{- end }}

{{/*
Return the name of the OCI auth secret, if configured.
*/}}
{{- define "crust-gather.ociAuthSecretName" -}}
{{- if or .Values.token .Values.username .Values.password -}}
{{- printf "%s-oci-auth" (include "crust-gather.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end }}

{{/*
Return the name of the CA secret, if configured.
*/}}
{{- define "crust-gather.caSecretName" -}}
{{- if .Values.ca_file -}}
{{- printf "%s-ca" (include "crust-gather.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end }}

{{/*
Label selector used to exclude chart-managed resources from collection.
*/}}
{{- define "crust-gather.collectExcludeLabels" -}}
app.kubernetes.io/name={{ include "crust-gather.name" . }},app.kubernetes.io/instance={{ .Release.Name }},app.kubernetes.io/managed-by={{ .Release.Service }}
{{- end }}

{{/*
Label selector used to exclude Helm release secrets from collection.
These secrets are managed by Helm itself and may contain rendered chart values.
*/}}
{{- define "crust-gather.collectExcludeHelmReleaseSecretLabels" -}}
name={{ .Release.Name }},owner=helm
{{- end }}
