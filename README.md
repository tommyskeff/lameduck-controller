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

Applications talk to the sidecar injected into every pod through an SDK:
[Go](pkg/sdk) or [Java](https://github.com/tommyskeff/lameduck-java-sdk).

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
