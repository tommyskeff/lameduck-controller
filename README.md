# Lameduck Controller

Lameduck is a Kubernetes controller that runs fleets of interchangeable pods which must finish their work before they are replaced.

A `DrainSet` never sends SIGTERM to a pod that still has work. When a pod has to go, it becomes a
lame duck: it stops receiving new traffic, keeps serving what it already has, and is deleted once it
reports it is done or its deadline passes.

## Features

- **Drains, not terminations.** Rollouts, scale-downs, evictions and `kubectl delete` drain the pod
  with a deadline instead of killing it.
- **Routing to the newest pods.** Pods carry `lameduck.tommyjs.dev/routable: "true"` while they should
  receive new traffic, so Services only send new connections to healthy, current pods.
- **Cancellable drains.** Reverting a rollout or scaling back up un-drains pods instead of replacing
  them.
- **Load-aware.** Pods report a load value; scale-down drains the least busy pod, and a drain can move
  to a pod that empties later.
- **Eviction-safe.** Node drains and autoscaler evictions become drains and are retried until the pod
  has finished. Annotate a pod with `lameduck.tommyjs.dev/terminate` to delete it immediately.
- **HorizontalPodAutoscaler support.** The scale subresource counts and selects only pods that are not
  draining, so an HPA scales on the pods that are taking new work.

Applications talk to the sidecar injected into every pod through an SDK:
[Go](pkg/sdk) or [Java](https://github.com/tommyskeff/lameduck-java-sdk).

## Requirements

Kubernetes 1.29 or later. The sidecar is injected as a native sidecar (an init container with
`restartPolicy: Always`), which older clusters do not run alongside the main containers.

The sidecar reserves the container name `lameduck-sidecar`, the volume name `lameduck-token` and
the sidecar port (9450, or the port in the controller's `--sidecar-address`) in every pod; a `DrainSet` whose template uses any of them is rejected.

## Autoscaling

### HorizontalPodAutoscaler

A `DrainSet` can be the target of an HPA. Its `status.replicas` and `status.selector` leave out
draining pods: the selector is the set's own selector plus `lameduck.tommyjs.dev/phase!=Draining`.
A draining pod is on its way out and takes no new traffic, so its falling load would otherwise drag
the average down and make the HPA scale in further while the replacement capacity is still needed.
Draining pods still use node resources until they finish, so leave room for them in quotas and node
capacity.

### Cluster autoscaler

When the cluster autoscaler scales a node down it evicts the pods on it. Lameduck turns each
eviction into a drain and answers `429 Too Many Requests` until the pod has finished. The cluster
autoscaler only retries an eviction for about two minutes (`--max-pod-eviction-time`), then gives up
on the node and tries again on a later scale-down pass. A node running pods whose drains outlast that
window therefore fails scale-down repeatedly, and the autoscaler logs eviction failures for it, until
the pods have drained. This is expected: the drains keep running across attempts, and the node can be
removed on a later pass once its pods are gone. Shorter `drain.timeouts.eviction` values bring
that pass forward.

## Example

```bash
helm repo add tommyjs https://repo.tommyjs.dev/repository/helm/
helm install lameduck tommyjs/lameduck -n lameduck-system --create-namespace
```

```yaml
apiVersion: lameduck.tommyjs.dev/v1alpha1
kind: DrainSet
metadata:
  name: game-server
spec:
  replicas: 3
  selector:
    matchLabels:
      app: game-server
  template:
    metadata:
      labels:
        app: game-server
    spec:
      containers:
      - name: server
        image: ghcr.io/example/game-server:1.0.0
        ports:
        - containerPort: 25565
  drain:
    timeouts:
      rollout: 1h
---
apiVersion: v1
kind: Service
metadata:
  name: game-server
spec:
  selector:
    app: game-server
    lameduck.tommyjs.dev/routable: "true"
  ports:
  - port: 25565
```
