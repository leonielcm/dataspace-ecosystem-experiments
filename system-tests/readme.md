# Dataspace Ecosystem - System Tests

End-to-end tests (`EndToEndTest` tag) against a real dataspace deployment — Lourmel playground VM by default. They are **replayable**: a run cleans up after itself, so the suite can be rerun against the same VM without destroying and redeploying it.

## 0. Prerequisites

- VPN WireGuard access to the Lourmel playground (`infrastructure/scripts/lourmel/VPN-WIREGUARD.md`).
- SSH access to the target VM (`ubuntu@<vm-ip>`) — the DB reset between runs goes through SSH + `kubectl exec` on the VM itself, not a local port-forward.
- The VM already bootstrapped and onboarded at least once via `infrastructure/scripts/lourmel/init-dataspace-playground.sh` — creates the `authority`/`provider`/`consumer` holders and membership credentials, and seeds the `participant_id` billing registry that `ReportTest` needs (see "Known limitations" below).
- The telemetry stack up and running: `bash infrastructure/scripts/lourmel/telemetry-toggle.sh up --developer <your-dev-name> --yes` (SSH'd onto the VM). This stack (Kafka, MinIO, `telemetry-storage`/`telemetry-csv-manager`) is deliberately scaled to 0 between telemetry test sessions to save VM resources — if it's down, `ReportTest` fails with connection-refused rather than the `400`/`403` cases documented below. Safe to run even if it's already up (idempotent no-op).

## 1. Point the suite at your VM

Two required system properties (or the matching env var — `CLUSTER_HOSTNAME`, `DEVELOPER_NAMESPACE`):

- `-Dcluster.hostname=<ip-dash-encoded>.nip.io` — e.g. `192-168-1-220.nip.io` for VM IP `192.168.1.220`.
- `-Ddeveloper.namespace=<your-dev-name>` — used to compute each participant's K8s namespace (`<developer>-dse-<participant>`) for the DB reset.

Optional:

- `-Dssh.user=<user>` (default `ubuntu`) — SSH user for the DB reset.
- `-Dkubectl.context=<context-name>` + `KUBECONFIG=<path>` — only needed for the two Kafka tests, see below. Everything else in the suite never touches your local kubectl config.

## 2. Run the tests

### Complete command (recommended)
Runs the full suite (including Kafka tests) with Gradle cache reset and tee'd log file:

```bash
KUBECONFIG=~/.kube/<context-name>.yaml ./gradlew :system-tests:runner:cleanTest :system-tests:runner:test \
  -DincludeTags="EndToEndTest" \
  -Dcluster.hostname=192-168-1-220.nip.io \
  -Ddeveloper.namespace=<your-dev-name> \
  -Dkubectl.context=<context-name> \
  --console=plain 2>&1 | tee /tmp/e2e-run-$(date +%H%M).log
```

*(Note: `KUBECONFIG` and `-Dkubectl.context` are required for the Kafka tests — see section "Kafka tests" below for how to set up this scoped context. `:system-tests:runner:cleanTest` forces a clean rerun instead of Gradle reusing cached UP-TO-DATE test results).*

No port-forward needed.

## 3. Replaying without destroy/redeploy

Each run cleans up what it created: `@AfterAll` truncates the tables it populated (assets, policies, contract-definitions, negotiations, agreements, transfers; the issuer-side attestation/credential-definition/holder bookkeeping; the telemetry event/report data behind `ReportTest`). It also purges dead-end credential requests (states 100 and 500), expired leases, and stale anti-replay tokens (`edc_jti_validation`). `@BeforeAll` verifies the DB is already in that clean state and only forces a reset as a fallback, if a previous run was interrupted before reaching its own cleanup.

Never touched by either step — deliberately preserved across runs:

- Already-issued credentials (`edc_holder_credentialrequest`/`credential_resource` `ISSUED` rows) — expensive to reissue, and `requestCredential()` now checks first and skips reissuing a credential that's already valid.
- `participant_context` — the participant's own identity (DID, keys), not test data.
- Vault secrets.
- `participant_id` — the billing registry, seeded externally (prerequisites above), not by this suite.

If the DB ever ends up in a state this doesn't cover (e.g. a schema-level issue, not just leftover rows), the manual fallback is `init-dataspace-playground.sh --reset-db[=<participants>]` — destructive, drops and recreates the target participants' schemas.

## 4. Known limitations

- **Kafka stream test (`transfer_kafka_stream()`) is currently `@Disabled`.** It targets the real dse Kafka proxy (`kafka-proxy-k8s-manager-core`, dynamically provisioned per transfer on `proxy-provider:30001`, `SASL_SSL`/`PLAIN`, TLS), which isn't deployed on Lourmel playgrounds — tracked separately in #788. Re-enable once that proxy is wired up there. `transfer_kafka_stream_oauth()`, despite its name, never actually calls Kafka and passes normally once `kubectl.context` is set correctly (see below).
- **`createAsset` (and other provisioning calls) occasionally stall for several minutes before the response comes back.** Root cause not identified — confirmed not to be Jetty thread-pool exhaustion, cause otherwise unknown, intermittent. Retries now actually absorb this: two suite-side bugs used to defeat them (`withRetry()` wasn't catching the checked `SocketTimeoutException` this stall surfaces as, nor RestAssured's `AssertionError` on a bad status code; and a retry landing after a stall that had actually completed server-side used to hit a `409` and fail hard instead of being treated as success). A stall by itself is no longer expected to fail the run, just to add retry time.
- **`TransferTest` negotiation occasionally stalls in `REQUESTED`/`REQUESTING`/`VERIFIED` instead of reaching `FINALIZED` within the timeout.** Two confirmed upstream EDC bugs (not `dse` code, no fix in this suite's scope): a DCP token-validation timeout (STS/VP resolution) on an inbound DSP message, and a lease race condition on `ContractNegotiation` rows (`SqlContractNegotiationStore`). Intermittent, moves between different tests on each run rather than always hitting the same one; just retry. See ticket #684 for details on the root-cause analysis.
- **`ReportTest` needs `participant_id` seeded** (prerequisites above) — without it, `POST /telemetry-events` fails with a `400` (foreign key violation), not a clear "you forgot a setup step" error.

## 5. Performance experiment: before/after negotiation, direct vs connector

`ConnectorPerformanceTest` is an experiment rather than a regression test. It answers two questions:

- What happens when a resource is called before a contract exists, after one is negotiated, and after it is retired?
- What does going through the connector cost compared to calling the backend directly?

It is tagged `performance` and is **excluded from every regular run**, including `-DincludeTags=EndToEndTest` and CI's `-DrunAllTests=true`. It only runs when asked for with `-DincludeTags=performance`.

### 5.1 What it measures

The phases run in order:

| Phase (`phase` column) | What happens | Checked |
|---|---|---|
| `before-negotiation` | Calls without a contract: consumer data API with an unknown agreement id, then without a token; provider public API without a token, then with an invalid token; the backend directly. | 403 / 401 / 401 / 403, while the backend answers 200 |
| `negotiation` | `perf.coldRuns` fresh negotiations, each timed step by step: `catalog` (dataset request over DSP), `negotiation` (until `FINALIZED`), `transfer` (until `STARTED`), `edr` (until the EDR is stored), `first-data` (until the first 200), and `total`. | at least one negotiation succeeds |
| `after-negotiation` | For each payload size: `perf.warmup` warmup calls, then `perf.runs` measured calls on every path. The path order rotates on each iteration, so environment drift doesn't penalise one path. | 200 and exact byte count |
| `after-retirement` (playground only) | The provider retires the agreement. The phase measures how long until the consumer gets a 403 (`retirement-propagation`), then calls again. The agreement is reactivated afterwards. | consumer data API 403, backend still 200 |

The three paths compared in `after-negotiation`:

| Path (`path` column) | Route | Data planes crossed |
|---|---|---|
| `direct` | test → `kubectl port-forward` → `provider-backend:8080/api/provider/payload` | 0 |
| `provider-public` | test → port-forward → provider data plane `/api/public`, with the token from the EDR (`GET /v3/edrs/{id}/dataaddress`) | 1 |
| `consumer-data` | test → ingress → consumer `/dp/data` (agreement id as `Authorization`, as in `queryData`) → provider `/api/public` → backend | 2 |

The payload comes from `GET /api/provider/payload?size=<bytes>[&delayMs=<ms>]` on `provider-backend`. The maximum size is 50 MB. The asset is seeded as `perf-payload` with `proxyQueryParams=true`, so `size` reaches the backend through the connector.

Every timed call uses the JDK `HttpClient` with keep-alive. RestAssured is not used because it opens a new connection per request. Two durations are recorded per call: `ttfb` (until the response headers arrive) and `total` (until the last byte is read).

### 5.2 Run it

**Playground.** Same prerequisites as the e2e suite (sections 0 and 1), plus two things:

1. The kubeconfig from the Kafka section below, with the `services` and `pods/portforward` rules.
2. A `provider-backend` image that includes the `/payload` endpoint. Rebuild it with `./gradlew :system-tests:backend-service-provider:dockerize` and redeploy it through the infrastructure repository.

```bash
KUBECONFIG=~/.kube/<context-name>.yaml ./gradlew :system-tests:runner:cleanTest :system-tests:runner:test \
  -DincludeTags=performance \
  -Dcluster.hostname=192-168-1-220.nip.io \
  -Ddeveloper.namespace=<your-dev-name> \
  -Dkubectl.context=<context-name> \
  --console=plain 2>&1 | tee /tmp/perf-run-$(date +%H%M).log
```

**UAT.** Existing provider and asset. Only our consumer connector is used, and "direct" is the real API. There is no seeding, no port-forward, no `provider-public` path, no payload sizes and no retirement phase.

```bash
./gradlew :system-tests:runner:cleanTest :system-tests:runner:test \
  -DincludeTags=performance \
  -Dperf.target=uat \
  -Dperf.uat.consumer.managementUrl=https://<host>/<consumer>/cp/management \
  -Dperf.uat.consumer.dataUrl=https://<host>/<consumer>/dp/data \
  -Dperf.uat.apiKey=<consumer x-api-key> \
  -Dperf.uat.provider.did=did:web:... \
  -Dperf.uat.provider.dspUrl=https://<host>/<provider>/cp/dsp \
  -Dperf.uat.assetId=<asset id> \
  -Dperf.uat.connector.query='<query string sent through the connector>' \
  -Dperf.uat.direct.url='https://<real api>/<resource>?<same query>' \
  -Dperf.uat.direct.headers='X-Api-Key: <provider api key>' \
  --console=plain
```

The API key and the header values are never written to the logs or the report.

### 5.3 Settings

| Property | Default | Meaning |
|---|---|---|
| `perf.target` | `playground` | `playground` or `uat` |
| `perf.sizes` | `1k,100k,1m,10m` | payload sizes, `k`/`m` = ×1024 / ×1024² (playground only) |
| `perf.warmup` / `perf.runs` | `20` / `200` | warmup and measured calls per path and size |
| `perf.coldRuns` | `5` | fresh negotiations timed in the `negotiation` phase |
| `perf.rejectionRuns` | `max(10, runs/10)` | calls per path in the rejection phases |
| `perf.retirement` | `true` | run the `after-retirement` phase (playground only) |
| `perf.protocol` | `dataspace-protocol-http` | DSP protocol of the dataset, negotiation and transfer requests |
| `perf.outputDir` | `build/perf-results` | where the results go, relative to `system-tests/runner` |
| `perf.provider.namespace` | `<developer>-dse-provider` | namespace of `provider-backend` and the provider data plane |
| `perf.backend.service` / `perf.backend.port` | `svc/provider-backend` / `8080` | port-forward target for `direct` |
| `perf.provider.public.mode` | `portforward` | `portforward`, `ingress` (`perf.provider.public.ingressUrl`, default `http://<cluster.hostname>/provider/dp/public`) or `disabled` |
| `perf.provider.dataplane.service` / `perf.provider.dataplane.publicPort` | `svc/provider-dataplane` / `8181` | port-forward target for `provider-public` |

### 5.4 Results

Each run writes two files under `system-tests/runner/build/perf-results/`, and the summary table is also printed at the end of the log:

- `samples-<target>-<timestamp>.csv`: one row per call, warmup included (`warmup` column). Failures are kept, with `ok=false` and the error message.
- `summary-<target>-<timestamp>.json`: for each `(phase, path, size)` series, the count, failures, min/mean/p50/p95/p99/max/stddev of `total` and `ttfb` (in ms), and `overheadOverDirect`, which is the p50/p95/mean difference of each path with `direct` at the same size. Warmup and failed calls are excluded from the statistics.

Reading the numbers:

- **`direct` and `provider-public` go through the same port-forward tunnel; `consumer-data` goes through the ingress.**
  - `provider-public − direct` is therefore the cost of one data plane: token check, then proxying to the backend.
  - `consumer-data − provider-public` mixes the consumer data plane (EDR lookup, second hop) with the difference between ingress and tunnel.
  - All three paths share the network between the machine running Gradle and the VM. Run from a machine close to the VM.
- **`before-negotiation`: the backend still answers 200 directly.** The connector enforces the contract only on the paths that go through it.
- **The `negotiation` phase is subject to the stalls described in section 4 (#684).** A stalled negotiation is recorded as a failed `total` sample and does not abort the run. Look at the p50, not the max.
- **`after-retirement` / `provider-public`: does the provider data plane reject an EDR token issued before the retirement?** The answer is logged as a warning, not treated as a test failure.

## Kafka tests: setting up a kubectl context for your VM

`transfer_kafka_stream()`'s pod discovery (`kubectl exec` into a `kafkacat-*` pod) shells out to `kubectl` **locally, on the machine running Gradle** — unlike the rest of the suite, it does not go through SSH. Without a kubectl context pointing at your VM, `kubectl --context <KUBECTL_CONTEXT>` silently falls back to whatever local context you have configured (a stale local cluster, if any) instead of failing — producing a misleading result rather than a clear one.

Rather than using the VM's own cluster-admin kubeconfig (broad, sensitive), create a scoped `ServiceAccount` that can only do what this suite needs — `get`/`list` pods and `exec` into them, plus port-forwarding to a service for the performance experiment, nothing else, in the participant namespaces plus `default` (for `kafkacat` pod discovery):

```bash
ssh ubuntu@<vm-ip> '
set -e
kubectl create serviceaccount e2e-runner -n default --dry-run=client -o yaml | kubectl apply -f -
for ns in <developer>-dse-provider <developer>-dse-consumer <developer>-dse-authority <developer>-dse-external default; do
  cat <<EOF | kubectl apply -f -
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: e2e-runner-pods
  namespace: ${ns}
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list"]
  - apiGroups: [""]
    resources: ["pods/exec"]
    verbs: ["create"]
  # Only needed by the performance experiment (section 5): kubectl port-forward to a service.
  - apiGroups: [""]
    resources: ["services"]
    verbs: ["get"]
  - apiGroups: [""]
    resources: ["pods/portforward"]
    verbs: ["create"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: e2e-runner-pods
  namespace: ${ns}
subjects:
  - kind: ServiceAccount
    name: e2e-runner
    namespace: default
roleRef:
  kind: Role
  name: e2e-runner-pods
  apiGroup: rbac.authorization.k8s.io
EOF
done
'
```

Then generate a token and build a standalone kubeconfig from it (server address, CA cert, and the token — no need to touch the VM's own admin kubeconfig):

```bash
TOKEN=$(ssh ubuntu@<vm-ip> "kubectl create token e2e-runner -n default --duration=87600h")
CA=$(ssh ubuntu@<vm-ip> "kubectl config view --minify --flatten -o jsonpath='{.clusters[0].cluster.certificate-authority-data}'")

cat > ~/.kube/<context-name>.yaml <<EOF
apiVersion: v1
kind: Config
clusters:
  - name: <context-name>
    cluster:
      server: https://<vm-ip>:6443
      certificate-authority-data: ${CA}
users:
  - name: e2e-runner
    user:
      token: ${TOKEN}
contexts:
  - name: <context-name>
    context:
      cluster: <context-name>
      user: e2e-runner
      namespace: <developer>-dse-provider
current-context: <context-name>
EOF
chmod 600 ~/.kube/<context-name>.yaml
```

Sanity-check it before running the suite: `KUBECONFIG=~/.kube/<context-name>.yaml kubectl --context <context-name> -n <developer>-dse-provider get pods` should list pods; `... delete pod <name> --dry-run=server` should come back `Forbidden`.

Then run the suite with `KUBECONFIG=~/.kube/<context-name>.yaml` prefixed and `-Dkubectl.context=<context-name>` added to the command in section 2.
