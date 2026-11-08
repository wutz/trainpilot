{{/* Chart name, overridable. */}}
{{- define "trainpilot.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Fully qualified app name. */}}
{{- define "trainpilot.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "trainpilot.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "trainpilot.labels" -}}
helm.sh/chart: {{ include "trainpilot.chart" . }}
{{ include "trainpilot.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/component: controller-manager
{{- end -}}

{{- define "trainpilot.selectorLabels" -}}
app.kubernetes.io/name: {{ include "trainpilot.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
control-plane: controller-manager
{{- end -}}

{{- define "trainpilot.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (printf "%s-manager" (include "trainpilot.fullname" .)) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "trainpilot.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}

{{- define "trainpilot.webhookServiceName" -}}
{{- printf "%s-webhook" (include "trainpilot.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "trainpilot.webhookCertName" -}}
{{- printf "%s-webhook-cert" (include "trainpilot.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "trainpilot.uiServiceName" -}}
{{- printf "%s-ui" (include "trainpilot.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
