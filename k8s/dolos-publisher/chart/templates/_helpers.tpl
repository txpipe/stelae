{{- define "dolos-publisher.jobName" -}}
{{ required "network is required" .Values.network }}-backfill-{{ required "run is required" .Values.run }}
{{- end -}}

{{- define "dolos-publisher.configMapName" -}}
{{ .Release.Name }}-config
{{- end -}}

{{- define "dolos-publisher.claimName" -}}
{{ .Values.persistence.existingClaim | default (printf "%s-data" .Release.Name) }}
{{- end -}}

{{- define "dolos-publisher.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
The batch values when all three are set, nothing when none is, and a
failed render for a partial set, which the publisher would refuse.
*/}}
{{- define "dolos-publisher.batch" -}}
{{- $b := .Values.batch | default dict -}}
{{- $set := list -}}
{{- range $k := list "epochs" "scratchMib" "reserveMib" -}}
{{- if not (kindIs "invalid" (get $b $k)) -}}{{- $set = append $set $k -}}{{- end -}}
{{- end -}}
{{- if eq (len $set) 3 -}}
{{ toYaml (pick $b "epochs" "scratchMib" "reserveMib") }}
{{- else if $set -}}
{{- fail (printf "batch.epochs, batch.scratchMib and batch.reserveMib go together; set all three or none (set: %s)" (join ", " $set)) -}}
{{- end -}}
{{- end -}}
