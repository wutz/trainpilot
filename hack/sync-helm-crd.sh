#!/usr/bin/env bash
set -euo pipefail

src="config/crd/bases/trainpilot.wutz.dev_traintasks.yaml"
dst="charts/trainpilot/templates/crd.yaml"

ver="$(sed -n 's/^    controller-gen\.kubebuilder\.io\/version: //p' "$src" | head -n1)"
ver="${ver:-v0.22.0}"

{
  cat <<EOF
{{- if .Values.crds.install }}
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: traintasks.trainpilot.wutz.dev
  labels:
    app.kubernetes.io/name: trainpilot
    app.kubernetes.io/part-of: trainpilot
  annotations:
    controller-gen.kubebuilder.io/version: ${ver}
    {{- if .Values.crds.keep }}
    helm.sh/resource-policy: keep
    {{- end }}
EOF
  sed -n '/^spec:/,$p' "$src"
  echo '{{- end }}'
} > "$dst"
