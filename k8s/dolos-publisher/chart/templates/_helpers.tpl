{{- define "dolos-publisher.jobName" -}}
{{ required "network is required" .Values.network }}-backfill-{{ required "run is required" .Values.run }}
{{- end -}}

{{- define "dolos-publisher.cronJobName" -}}
{{ required "network is required" .Values.network }}-publisher
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
failed render for a partial set, which the publisher would refuse. A null
override deletes its key in Helm's merge, so a key counts only when present
with a value.
*/}}
{{- define "dolos-publisher.batch" -}}
{{- $b := .Values.batch | default dict -}}
{{- $set := list -}}
{{- range $k := list "epochs" "scratchMib" "reserveMib" -}}
{{- if hasKey $b $k -}}
{{- $v := index $b $k -}}
{{- if not (or (kindIs "invalid" $v) (eq (toString $v) "")) -}}{{- $set = append $set $k -}}{{- end -}}
{{- end -}}
{{- end -}}
{{- if eq (len $set) 3 -}}
{{ toYaml (pick $b "epochs" "scratchMib" "reserveMib") }}
{{- else if $set -}}
{{- fail (printf "batch.epochs, batch.scratchMib and batch.reserveMib go together; set all three or none (set: %s)" (join ", " $set)) -}}
{{- end -}}
{{- end -}}

{{/*
The publisher pod template, shared by the one-shot Job and the CronJob.
Called with a dict: "root" is the chart context and "app" the suffix of the
pod's app label after the network name.
*/}}
{{- define "dolos-publisher.podTemplate" -}}
{{- with .root -}}
metadata:
  labels:
    app: {{ .Values.network }}-{{ $.app }}
spec:
  restartPolicy: OnFailure
  securityContext:
    runAsNonRoot: true
    fsGroup: 65532
    {{- if .Values.persistence.enabled }}
    fsGroupChangePolicy: OnRootMismatch
    {{- end }}
  {{- with .Values.nodeSelector }}
  nodeSelector:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- with .Values.tolerations }}
  tolerations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  terminationGracePeriodSeconds: {{ .Values.terminationGracePeriodSeconds }}
  containers:
    - name: publisher
      image: {{ .Values.image.repository }}:{{ required "image.tag is required" .Values.image.tag }}
      command: ["/usr/local/bin/stelae-publisher"]
      args:
        - --config
        - /etc/publisher/dolos.toml
        - run
        - --repo
        - {{ required "repo is required" .Values.repo | quote }}
        {{- if .Values.insecure }}
        - --insecure
        {{- end }}
        - --concurrency
        - {{ required "concurrency is required" .Values.concurrency | quote }}
        {{- if .Values.allowGenesisFallback }}
        - --allow-genesis
        {{- end }}
        {{- with include "dolos-publisher.batch" . | fromYaml }}
        - --batch-epochs
        - {{ .epochs | int64 | quote }}
        - --batch-scratch-mib
        - {{ .scratchMib | int64 | quote }}
        - --batch-reserve-mib
        - {{ .reserveMib | int64 | quote }}
        {{- end }}
        {{- if .Values.directS3SecretName }}
        - --direct-s3-config
        - /run/direct-s3/direct-s3.json
        {{- end }}
      env:
        - name: DOLOS_STELAE_USER
          valueFrom:
            secretKeyRef:
              name: {{ .Values.publisherSecretName }}
              key: DOLOS_STELAE_REGISTRY_USER
        - name: DOLOS_STELAE_PASSWORD
          valueFrom:
            secretKeyRef:
              name: {{ .Values.publisherSecretName }}
              key: DOLOS_STELAE_REGISTRY_PASSWORD
      resources:
        {{- required "resources is required" .Values.resources | toYaml | nindent 8 }}
      volumeMounts:
        {{- if .Values.directS3SecretName }}
        - name: direct-s3
          mountPath: /run/direct-s3
          readOnly: true
        {{- end }}
        - name: data
          mountPath: /data
        - name: {{ .Values.network }}-config
          mountPath: /etc/publisher
  volumes:
    {{- if .Values.directS3SecretName }}
    - name: direct-s3
      secret:
        secretName: {{ .Values.directS3SecretName | quote }}
        defaultMode: 0440
        items:
          - key: direct-s3.json
            path: direct-s3.json
    {{- end }}
    - name: data
      {{- if .Values.persistence.enabled }}
      persistentVolumeClaim:
        claimName: {{ include "dolos-publisher.claimName" . }}
      {{- else }}
      emptyDir: {}
      {{- end }}
    - name: {{ .Values.network }}-config
      configMap:
        name: {{ include "dolos-publisher.configMapName" . }}
{{- end -}}
{{- end -}}
