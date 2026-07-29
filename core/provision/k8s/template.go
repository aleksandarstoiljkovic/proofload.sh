package k8s

import "text/template"

var manifestTmpl = template.Must(template.New("k8s").Parse(manifestTemplate))

const manifestTemplate = `apiVersion: v1
kind: Namespace
metadata:
  name: {{.Namespace}}
  labels:
    app.kubernetes.io/managed-by: proofload
---
apiVersion: v1
kind: Service
metadata:
  name: {{.Name}}
  namespace: {{.Namespace}}
  labels:
    app: {{.Name}}
spec:
  clusterIP: None
  selector:
    app: {{.Name}}
  ports:
    - name: {{.PortName}}
      port: {{.ContainerPort}}
      targetPort: {{.ContainerPort}}
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: {{.Name}}
  namespace: {{.Namespace}}
  labels:
    app: {{.Name}}
spec:
  serviceName: {{.Name}}
  replicas: {{.Replicas}}
  selector:
    matchLabels:
      app: {{.Name}}
  template:
    metadata:
      labels:
        app: {{.Name}}
    spec:
      containers:
        - name: {{.Container}}
          image: {{.Image}}
          ports:
            - name: {{.PortName}}
              containerPort: {{.ContainerPort}}
{{- if .Env}}
          env:
{{- range .Env}}
            - name: {{.K}}
              value: "{{.V}}"
{{- end}}
{{- end}}
{{- if .Readiness}}
          readinessProbe:
{{- if .Readiness.Exec}}
            exec:
              command: [{{range $i, $e := .Readiness.Exec}}{{if $i}}, {{end}}"{{$e}}"{{end}}]
{{- else}}
            tcpSocket:
              port: {{.Readiness.TCPPort}}
{{- end}}
            initialDelaySeconds: {{.Readiness.InitialDelay}}
            periodSeconds: {{.Readiness.Period}}
{{- end}}
{{- if or .CPULimit .MemLimit}}
          resources:
            limits:
{{- if .CPULimit}}
              cpu: "{{.CPULimit}}"
{{- end}}
{{- if .MemLimit}}
              memory: {{.MemLimit}}
{{- end}}
{{- end}}
`
