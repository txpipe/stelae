{{- define "dolos-publisher.jobName" -}}
{{ required "network is required" .Values.network }}-backfill-{{ required "run is required" .Values.run }}
{{- end -}}

{{- define "dolos-publisher.configMapName" -}}
{{ .Release.Name }}-config
{{- end -}}

{{- define "dolos-publisher.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}
