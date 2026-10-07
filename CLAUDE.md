# Lameduck Controller

Read [GOLDEN_RULES.md](GOLDEN_RULES.md) before writing any code. It overrides defaults.

## Layout

- `api/v1alpha1` — the `DrainSet` CRD types and every `lameduck.tommyjs.dev/` label and annotation key
- `cmd/` — the manager (`cmd/main.go`) and the sidecar (`cmd/sidecar`); one image holds both
- `internal/planner` — pure decisions: which pods to create, drain, un-drain, swap and route
- `internal/controller` — the reconciler, pod factory (injects the sidecar) and expectations
- `internal/revision` — template hashing and ControllerRevision history
- `internal/fleet` — the gRPC server sidecars stream to, token authentication and the in-memory registry of load and drain reports
- `internal/admission` — the pod eviction and delete webhooks that turn disruptions into drains
- `internal/sidecar` — the sidecar: the local SDK server and the uplink to the fleet server
- `pkg/sdk` — the Go SDK applications use to talk to their sidecar
- `proto/` — the sidecar and fleet protos; generated Go lives in `pkg/proto/lameduckv1`
- `config/` — kustomize manifests; `config/samples` holds a sample set and Service
- `charts/lameduck` — the Helm chart; `files/` holds copies of the generated CRD, ClusterRole and webhooks made by `make chart`

## Commands

On Windows the tools are run from `bin/` directly; the Makefile targets are for WSL and CI.

```bash
go build ./...
go vet ./...
go test ./...
bin/golangci-lint run ./...
bin/controller-gen rbac:roleName=manager-role crd:generateEmbeddedObjectMeta=true webhook paths=./api/v1alpha1 paths=./internal/controller paths=./internal/admission output:crd:artifacts:config=config/crd/bases
bin/controller-gen object paths=./api/v1alpha1
PATH="$PWD/bin:$PATH" buf generate
bin/kustomize build config/default
cp config/crd/bases/lameduck.tommyjs.dev_drainsets.yaml charts/lameduck/files/crd.yaml
cp config/rbac/role.yaml charts/lameduck/files/role.yaml
cp config/webhook/manifests.yaml charts/lameduck/files/webhooks.yaml
helm lint charts/lameduck
```

Images go to `ghcr.io/tommyskeff/lameduck-controller`; charts to `https://repo.tommyjs.dev/repository/helm/`.
