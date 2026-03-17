{{/*
Expand the name of the chart.
*/}}
{{- define "logql-proxy.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
Truncated to 63 characters because Kubernetes name fields are limited.
*/}}
{{- define "logql-proxy.fullname" -}}
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
Create chart label value: "<name>-<version>".
*/}}
{{- define "logql-proxy.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels attached to every resource.
*/}}
{{- define "logql-proxy.labels" -}}
helm.sh/chart: {{ include "logql-proxy.chart" . }}
{{ include "logql-proxy.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels — used in Deployment.spec.selector and Service.spec.selector.
Must remain stable across upgrades.
*/}}
{{- define "logql-proxy.selectorLabels" -}}
app.kubernetes.io/name: {{ include "logql-proxy.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
ServiceAccount name.
*/}}
{{- define "logql-proxy.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "logql-proxy.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Name of the Secret that holds VictoriaLogs credentials.

Returns the existingSecret name when set; otherwise returns the name of the
Secret this chart creates (only rendered when credentials are provided inline).
Returns an empty string when no credentials are configured at all — callers
must check for emptiness before referencing the secret.
*/}}
{{- define "logql-proxy.credentialsSecretName" -}}
{{- if .Values.proxy.vlogs.existingSecret -}}
{{- .Values.proxy.vlogs.existingSecret -}}
{{- else if or .Values.proxy.vlogs.bearerToken .Values.proxy.vlogs.basicAuth.password -}}
{{- include "logql-proxy.fullname" . }}-credentials
{{- end -}}
{{- end }}

{{/*
Render the proxy config.yaml content from values.
Credentials are intentionally omitted; they are injected via PROXY_* env vars
from the Kubernetes Secret so they never appear in the ConfigMap.
*/}}
{{- define "logql-proxy.configYaml" -}}
server:
  listenAddr: {{ .Values.proxy.server.listenAddr | quote }}
  readTimeout: {{ .Values.proxy.server.readTimeout | quote }}
  writeTimeout: {{ .Values.proxy.server.writeTimeout | quote }}
  idleTimeout: {{ .Values.proxy.server.idleTimeout | quote }}
  gracefulTimeout: {{ .Values.proxy.server.gracefulTimeout | quote }}

vlogs:
  url: {{ .Values.proxy.vlogs.url | quote }}
  timeout: {{ .Values.proxy.vlogs.timeout | quote }}
  maxIdleConns: {{ .Values.proxy.vlogs.maxIdleConns }}
  maxConnsPerHost: {{ .Values.proxy.vlogs.maxConnsPerHost }}
  {{- if .Values.proxy.vlogs.extraHeaders }}
  extraHeaders:
    {{- toYaml .Values.proxy.vlogs.extraHeaders | nindent 4 }}
  {{- end }}
  {{- if .Values.proxy.vlogs.extraParams }}
  extraParams:
    {{- toYaml .Values.proxy.vlogs.extraParams | nindent 4 }}
  {{- end }}

limits:
  maxConcurrentQueries: {{ .Values.proxy.limits.maxConcurrentQueries }}
  maxQueueDepth: {{ .Values.proxy.limits.maxQueueDepth }}
  maxResponseBodyBytes: {{ .Values.proxy.limits.maxResponseBodyBytes | int }}
  maxStreamsPerResponse: {{ .Values.proxy.limits.maxStreamsPerResponse }}
  maxMemoryMB: {{ .Values.proxy.limits.maxMemoryMB }}
  maxQueryRangeHours: {{ .Values.proxy.limits.maxQueryRangeHours }}
  maxLimit: {{ .Values.proxy.limits.maxLimit }}
  defaultLimit: {{ .Values.proxy.limits.defaultLimit }}

labels:
  {{- if .Values.proxy.labels.knownLabels }}
  knownLabels:
    {{- toYaml .Values.proxy.labels.knownLabels | nindent 4 }}
  {{- else }}
  knownLabels: []
  {{- end }}
  metadataCacheTTL: {{ .Values.proxy.labels.metadataCacheTTL | quote }}
  metadataCacheSize: {{ .Values.proxy.labels.metadataCacheSize }}

log:
  level: {{ .Values.proxy.log.level | quote }}
  format: {{ .Values.proxy.log.format | quote }}
{{- end }}
