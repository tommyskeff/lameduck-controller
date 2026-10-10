{{- define "lameduck.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 50 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "lameduck.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "lameduck.selectorLabels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "lameduck.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}

{{- define "lameduck.serviceAccount" -}}
{{- include "lameduck.fullname" . -}}
{{- end -}}

{{- define "lameduck.webhookService" -}}
{{- printf "%s-webhook" (include "lameduck.fullname" .) -}}
{{- end -}}

{{- define "lameduck.webhookSecret" -}}
{{- printf "%s-webhook-cert" (include "lameduck.fullname" .) -}}
{{- end -}}

{{- define "lameduck.fleetSecret" -}}
{{- printf "%s-fleet-cert" (include "lameduck.fullname" .) -}}
{{- end -}}
