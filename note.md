```
# Run local
MONITORING_NAMESPACE=everest-monitoring \
go run ./cmd/main.go \
  --leader-elect=false --disable-webhook-server --system-namespace=everest-system --leader-election-id=percona-operator-local 
```