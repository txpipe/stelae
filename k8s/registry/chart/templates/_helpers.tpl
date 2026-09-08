{{- define "stelae-registry.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "stelae-registry.fullname" -}}
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

{{- define "stelae-registry.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "stelae-registry.labels" -}}
helm.sh/chart: {{ include "stelae-registry.chart" . }}
{{ include "stelae-registry.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Selector labels are the bare `app:` key the pre-chart manifests used, not the
app.kubernetes.io pair. A Deployment's selector is immutable, so keeping it
lets the running registry be adopted by `helm upgrade --take-ownership`
instead of being deleted and recreated mid-publish.
*/}}
{{- define "stelae-registry.selectorLabels" -}}
app: {{ include "stelae-registry.fullname" . }}
{{- end -}}

{{- define "stelae-registry.image" -}}
{{- $img := .Values.image -}}
{{- $repository := required "image.repository is required" $img.repository -}}
{{- $tag := required "image.tag is required" $img.tag -}}
{{- if $img.digest -}}
{{- printf "%s:%s@%s" $repository $tag $img.digest -}}
{{- else -}}
{{- printf "%s:%s" $repository $tag -}}
{{- end -}}
{{- end -}}
