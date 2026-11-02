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

## MCP over a local dataspace (no VM needed)

`system-tests/mcp-e2e` checks that an MCP (Model Context Protocol) server published as an asset can be called through the dataspace. Unlike the rest of this suite, it needs no VM: the whole dataspace starts in the test JVM.

How to publish an MCP server as an asset (`proxyHeaders=true`) and how the data plane relays MCP headers and statuses: see [`extensions/data-plane/data-plane-public-api-v2/README.md`](../extensions/data-plane/data-plane-public-api-v2/README.md).

### What runs

1. The provider publishes the MCP servers as `HttpData` assets through its management API, with a policy and a contract definition.
2. The consumer reads the catalog, negotiates a contract and starts an `HttpData-PULL` transfer over DSP.
3. The provider data plane issues the EDR, and the consumer stores it.
4. The official MCP Java SDK client calls the consumer data API with the agreement id as `Authorization`. The call goes through the consumer data plane, then the provider data plane (real token check), then the MCP server.

| Test | Checks |
|---|---|
| `mcpClient_shouldBeRejected_withoutAgreement` | without an agreement, the consumer data API answers `403` and nothing reaches the MCP server |
| `mcpClient_shouldReadResourceAndCallTool_afterNegotiation` | `initialize`, `resources/read` and `tools/call` on a stateless server; neither the agreement id nor the EDR token reach it |
| `mcpClient_shouldKeepTheSession_ofStatefulServer_afterNegotiation` | the `Mcp-Session-Id` returned on `initialize` comes back on every later request |
| `mcpClient_shouldReachRealServer_afterNegotiation` | opt-in, see below: same path to a real MCP server, compared with a direct connection to it |

### What is real, what is simulated

**Real:**

- two EDC connectors, provider and consumer, talking DSP;
- the project's data plane launcher (`launchers/data-plane/data-plane-base`), with its extensions and its data consumption metrics, on Postgres;
- the EDR token, signed and checked by the provider data plane;
- the official MCP Java SDK, client and servers.

**Simulated:**

- **Identity.** EDC's `iam-mock` replaces DCP (verifiable credentials, identity hub, issuer). The policy has no constraint, so access rules (membership, domain…) are not tested.
- **Control plane.** It is a plain EDC control plane (`controlplane-base-bom`), not the project's launcher, which requires DCP. Its stores and the vaults are in memory.
- **Consumer runtime.** The consumer control plane and data plane run in one runtime, so that the data plane sees the EDRs the control plane stores. Deployed, they are two runtimes sharing a database and a vault.
- **Infrastructure.** No Kubernetes, no ingress: everything runs on `localhost`.

### Run it

Requires Docker: Testcontainers starts the Postgres the data planes need.

```bash
./gradlew :system-tests:mcp-e2e:runner:test
```

The runtimes take about 30 seconds to start. The test is named `*IntegrationTest`, so it also runs in the CI integration job, which then needs Docker.

### Against a real MCP server

The last test runs only when `mcp.real.url` is set, so that no regular run calls a third-party service. The tool named by `mcp.real.tool` is called with the JSON object in `mcp.real.args`. Pick a read-only tool.

```bash
./gradlew :system-tests:mcp-e2e:runner:cleanTest :system-tests:mcp-e2e:runner:test --tests "*shouldReachRealServer*" \
  -Dmcp.real.url=https://mcp.parisjetaime.com/mcp \
  -Dmcp.real.tool=paris_get_events \
  -Dmcp.real.args='{"params":{"keyword":"jazz","limit":3}}'
```

The start of the tool's answer is printed in the test's standard output. To read it, open `system-tests/mcp-e2e/runner/build/reports/tests/test/index.html`, then the **Standard output** tab of `McpOverDataspaceIntegrationTest`.

Before blaming the dataspace for an unexpected answer, call the server directly. Whatever it answers directly, it answers the same through the dataspace:

```bash
curl -s -X POST https://mcp.parisjetaime.com/mcp -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"paris_get_top_events","arguments":{}}}'
```

## Kafka tests: setting up a kubectl context for your VM

`transfer_kafka_stream()`'s pod discovery (`kubectl exec` into a `kafkacat-*` pod) shells out to `kubectl` **locally, on the machine running Gradle** — unlike the rest of the suite, it does not go through SSH. Without a kubectl context pointing at your VM, `kubectl --context <KUBECTL_CONTEXT>` silently falls back to whatever local context you have configured (a stale local cluster, if any) instead of failing — producing a misleading result rather than a clear one.

Rather than using the VM's own cluster-admin kubeconfig (broad, sensitive), create a scoped `ServiceAccount` that can only do what this suite needs — `get`/`list` pods and `exec` into them, nothing else, in the participant namespaces plus `default` (for `kafkacat` pod discovery):

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
