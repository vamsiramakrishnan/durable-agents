{{/* Common helpers */}}

{{- define "tape.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "tape.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "tape.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "tape.labels" -}}
app.kubernetes.io/name: {{ include "tape.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "tape.serverSelector" -}}
app.kubernetes.io/name: {{ include "tape.name" . }}
app.kubernetes.io/component: server
{{- end -}}

{{- define "tape.reactorSelector" -}}
app.kubernetes.io/name: {{ include "tape.name" . }}
app.kubernetes.io/component: reactor
{{- end -}}
