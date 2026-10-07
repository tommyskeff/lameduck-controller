# Golden rules

Read this before writing any code in this repository. These rules override defaults, house
style and whatever the kubebuilder scaffolding suggests. Rules are never renumbered — a rule
that stops applying is struck, not removed.

## Code

*Applies everywhere: `api/`, `cmd/`, `internal/`, `pkg/`, `proto/` and `config/`.*

1. **No comments.** Code explains itself through naming and structure. The exceptions are
   machine-read text only:
   - compiler and tool directives: `//go:build`, `//go:generate`, `//nolint:<linter>`;
   - kubebuilder and controller-gen markers: `// +kubebuilder:...`, `// +groupName=...`,
     `// +optional`, `// +required`, `// +listType=...`;
   - **doc comments on fields and types in `api/`**, because controller-gen turns them into
     the CRD's OpenAPI descriptions that `kubectl explain` shows. One short sentence each,
     only where the name is not enough;
   - generated files (`zz_generated.*`, `*.pb.go`, the CRD and RBAC YAML, `PROJECT`).

   The rule covers committed configuration too: kustomize YAML, workflows and the Makefile
   carry no explanatory comments or commented-out blocks.
2. **No dead scaffolding.** No TODO stubs, placeholder files, commented-out code or unused
   exports. A kubebuilder scaffold that is not used is deleted, not left disabled.
3. **No inline imports or dot imports.** Every package is imported at the top of the file under
   its own name. Imports are grouped standard library, third party, then this module
   (`goimports -local github.com/tommyskeff/lameduck-controller`).
4. **Names are formal and specific.** A type is named for the thing it is: `PodFactory`,
   `TokenAuthenticator`, `DisruptionHandler` — never `Manager`, `Helper` or a concept like
   `Routing`. Packages are short, singular and say what they hold (`planner`, `revision`,
   `fleet`); there is no `util`, `common` or `helpers` package.
5. **`internal/` does the hiding.** Everything outside `api/` and `pkg/` lives under
   `internal/`, so a package exports the types and members that make up its API without
   ceremony. Helpers that only make sense inside the package stay unexported.
6. **Labels, annotations and audiences are constants in `api/v1alpha1`.** Every
   `lameduck.tommyjs.dev/...` key is defined once there and referenced everywhere else.

## Kubernetes

*The controller, webhooks and manifests.*

7. **Decisions are pure.** What to create, drain, cancel or route is decided by
   `internal/planner` from a snapshot, with no client, clock or registry access. The
   reconciler gathers the snapshot and applies the plan. New behaviour goes into the planner
   with a test first.
8. **Reconcile is idempotent and expectation-guarded.** Every create, delete or drain records
   an expectation, and reconcile does nothing until the cache has caught up. Never act twice on
   a stale cache.
9. **Durable state lives on the API server; ephemeral state lives in the registry.** Drain
   reason, start and deadline are pod annotations so a controller restart loses nothing.
   Load and drained reports are in memory and are resent by sidecars on reconnect.
10. **Generated files are regenerated, never edited.** Run `make manifests generate proto chart`
    after changing markers, API types or protos, and commit the output with the change. The
    chart's `files/` are copies of the generated manifests; CI fails when they drift.
11. **API changes stay compatible within a version.** Defaults are set with
    `+kubebuilder:default` markers, not in code. Removing or renaming a field needs a new
    API version.

## gRPC

*`proto/`, the fleet server, the sidecar and the SDKs.*

12. **Protos pass `buf lint`.** Services end in `Service`, every RPC has its own request and
    response message.
13. **Thin handlers.** An RPC handler checks its request, calls one function on the agent or
    registry and maps the result. Logic does not live in handlers.
14. **Expected failures are gRPC statuses**, with the code that fits (`INVALID_ARGUMENT`,
    `FAILED_PRECONDITION`, `UNAUTHENTICATED`, `PERMISSION_DENIED`, `NOT_FOUND`). When a client
    needs to know why, the response carries an enum, never a sentence. Responses never carry
    error flags.

## Go

*All Go code.*

15. **Config is parsed once.** Each binary reads its flags and environment in `cmd/` into a
    config struct and exits on anything missing. Nothing else calls `os.Getenv` or
    `flag.Parse`; the SDK's `AddressFromEnvironment` is the one exception because it runs in
    the user's process.
16. **Dependencies are passed in.** Constructors and structs take their clients, clock
    (`k8s.io/utils/clock`) and registry. Only `cmd/` constructs real ones.
17. **No logging abstractions.** The controller logs through controller-runtime's `logr`
    (`logf.FromContext`); the sidecar logs through `log/slog` with the JSON handler. Never wrap
    either.
18. **Never log personal data or credentials.** No tokens, usernames or player data in logs.
19. **Errors are wrapped with context** (`fmt.Errorf("create pod: %w", err)`). No panics
    outside `main` and `init`.

## Verification

20. **`go build ./...`, `go vet ./...`, `golangci-lint run` and `go test ./...` pass before
    every commit.** On Windows, run the tools from `bin/` directly; the Makefile is for CI and
    WSL.
21. **Controller behaviour is verified in a cluster** — minikube locally — by deploying the
    image and the sample and reading the controller log, not by reading the source alone.
    A missing RBAC verb or a webhook that never matches both compile clean.

## Commits and pull requests

22. Conventional commit title, no body, no `Co-Authored-By` trailer, no generated-with footer.
23. **Pull request descriptions say what the change does, not how.** One short sentence on
    what the change accomplishes, and why when that is not obvious, followed by a bullet list
    of behaviour a reviewer cares about. No attribution footer.
24. **READMEs change only when asked.** Never edit or add a README as a side effect of other
    work.
