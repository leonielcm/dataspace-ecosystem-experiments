package org.eclipse.edc.test.system;

import org.eclipse.edc.junit.annotations.EndToEndTest;
import org.eclipse.edc.test.system.AbstractParticipant.ParticipantClient;
import org.eclipse.edc.test.system.AbstractParticipant.ParticipantClient.Edr;
import org.eclipse.edc.test.system.perf.PerfConfig;
import org.eclipse.edc.test.system.perf.PerfReport;
import org.eclipse.edc.test.system.perf.PerfReport.Sample;
import org.eclipse.edc.test.system.perf.PortForward;
import org.eclipse.edc.test.system.perf.TimedHttpClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.IntPredicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.eclipse.edc.connector.controlplane.contract.spi.types.negotiation.ContractNegotiationStates.FINALIZED;
import static org.eclipse.edc.connector.controlplane.transfer.spi.types.TransferProcessStates.STARTED;
import static org.eclipse.edc.connector.controlplane.transfer.spi.types.TransferProcessStates.TERMINATED;
import static org.eclipse.edc.test.system.ParticipantConstants.CLUSTER_HOSTNAME;
import static org.eclipse.edc.test.system.ParticipantConstants.DEVELOPER_NAMESPACE;
import static org.eclipse.edc.test.system.ParticipantConstants.KUBECTL_CONTEXT;
import static org.eclipse.edc.test.system.ParticipantConstants.printConfiguration;
import static org.eclipse.edc.test.system.TestConstants.DEFAULT_TIMEOUT;
import static org.eclipse.edc.test.system.perf.PerfReport.PATH_DIRECT;
import static org.eclipse.edc.test.system.perf.PerfReport.PHASE_STEADY_STATE;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Experiment: calling a provider resource before and after contract negotiation, and the cost of
 * going through the connector compared to calling the backend directly. Not a regression test --
 * excluded from the regular e2e run, enabled with {@code -DincludeTags=performance}. See the
 * "Performance experiment" section of system-tests/readme.md for how to run it and read the
 * results.
 *
 * <p>Phases, in order:
 * <ol>
 *     <li>before negotiation: the connector rejects the call (403 unknown agreement, 401 no
 *     token), while the backend called directly answers;</li>
 *     <li>negotiation: cold cost of catalog, negotiation, transfer, EDR and first data call,
 *     step by step, over a fresh negotiation each time;</li>
 *     <li>after negotiation: steady-state latency of the three paths (direct, provider public API
 *     with the EDR token, consumer data API) at each payload size;</li>
 *     <li>after retirement (playground only): the agreement is retired and the connector rejects
 *     the call again.</li>
 * </ol>
 */
@EndToEndTest
@Tag(ConnectorPerformanceTest.PERFORMANCE_TAG)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ConnectorPerformanceTest extends AbstractEndToEndTests {

    static final String PERFORMANCE_TAG = "performance";

    private static final String ASSET_ID = "perf-payload";
    private static final String BACKEND_PAYLOAD_PATH = "/api/provider/payload";
    private static final String PROVIDER_PUBLIC_PATH = "/api/public";

    private static final String PHASE_BEFORE_NEGOTIATION = "before-negotiation";
    private static final String PHASE_NEGOTIATION = "negotiation";
    private static final String PHASE_AFTER_RETIREMENT = "after-retirement";

    private static final String PATH_PROVIDER_PUBLIC = "provider-public";
    private static final String PATH_CONSUMER_DATA = "consumer-data";

    private static final Duration POLL_INTERVAL = Duration.ofMillis(100);
    private static final Duration PORT_FORWARD_TIMEOUT = Duration.ofSeconds(30);

    private final PerfConfig config = PerfConfig.fromSystemProperties(DEVELOPER_NAMESPACE, CLUSTER_HOSTNAME);
    private final PerfReport report = new PerfReport(config.target().name().toLowerCase(Locale.ROOT), config.outputDir());
    private final TimedHttpClient http = new TimedHttpClient(Duration.ofSeconds(10), DEFAULT_TIMEOUT);
    private final LocalProvider localProvider = new LocalProvider();
    private final LocalConsumer localConsumer = new LocalConsumer();

    private ParticipantClient consumer;
    private ParticipantClient provider;
    private String assetId;
    private PortForward backendForward;
    private PortForward dataPlaneForward;
    private Negotiated negotiated;

    private record Negotiated(String transferProcessId, String agreementId, Edr edr) {
    }

    private record TimedPath(String name, Supplier<URI> uri, Map<String, String> headers, IntPredicate expectedStatus,
                             long expectedBytes) {
    }

    @BeforeAll
    void setUp() {
        printConfiguration();
        MONITOR.info("Connector performance experiment: " + config);

        if (isPlayground()) {
            // Same replay-safety as the other e2e classes: start from, and leave, empty EDC tables.
            resetParticipant("provider");
            resetParticipant("consumer");
            consumer = localConsumer.participantClient();
            provider = localProvider.participantClient();
            assetId = ASSET_ID;
            localProvider.createEntry(ASSET_ID, "Performance payload", "payload of configurable size, for the connector performance experiment", Map.of(
                    "type", "HttpData",
                    "baseUrl", "http://provider-backend:8080" + BACKEND_PAYLOAD_PATH,
                    "proxyQueryParams", Boolean.TRUE.toString()
            ));

            var playground = config.playground();
            backendForward = PortForward.open(KUBECTL_CONTEXT, playground.providerNamespace(), playground.backendService(),
                    playground.backendPort(), PORT_FORWARD_TIMEOUT);
            MONITOR.info("Direct access through " + backendForward);
            if ("portforward".equals(playground.providerPublicMode())) {
                dataPlaneForward = PortForward.open(KUBECTL_CONTEXT, playground.providerNamespace(), playground.dataPlaneService(),
                        playground.dataPlanePublicPort(), PORT_FORWARD_TIMEOUT);
                MONITOR.info("Provider public API through " + dataPlaneForward);
            }
        } else {
            var uat = config.uat();
            consumer = ParticipantClient.Builder.newInstance()
                    .name(uat.consumerName())
                    .id(uat.consumerDid())
                    .controlPlaneManagement(uat.consumerManagementUrl())
                    .dataPlaneData(uat.consumerDataUrl())
                    .apiKey(uat.apiKey())
                    .timeout(DEFAULT_TIMEOUT)
                    .build();
            provider = ParticipantClient.Builder.newInstance()
                    .name("provider")
                    .id(uat.providerDid())
                    .controlPlaneProtocol(uat.providerDspUrl())
                    // never called: the experiment only talks to the provider through our consumer
                    .dataPlaneData(uat.providerDspUrl())
                    .build();
            assetId = uat.assetId();
        }
        consumer.setProtocol(config.protocol());
    }

    @AfterAll
    void tearDown() {
        try {
            var metadata = new LinkedHashMap<String, Object>();
            metadata.put("config", config.describe());
            metadata.put("clusterHostname", CLUSTER_HOSTNAME);
            metadata.put("assetId", assetId);
            var files = report.write(metadata);
            MONITOR.info("Connector performance experiment results:%n%s%nWritten to %s".formatted(report.table(), files));
        } finally {
            if (backendForward != null) {
                backendForward.close();
            }
            if (dataPlaneForward != null) {
                dataPlaneForward.close();
            }
            if (isPlayground()) {
                DatabaseCleaner.truncateTables("provider", EDC_TABLES);
                DatabaseCleaner.truncateTables("consumer", EDC_TABLES);
            }
        }
    }

    @Test
    @Order(1)
    void beforeNegotiation_connectorRejectsWhileBackendAnswers() {
        var size = config.sizes().get(0);
        var unknownAgreement = UUID.randomUUID().toString();

        var paths = new ArrayList<TimedPath>();
        // On UAT the exact rejection status depends on that connector's version and gateway: any
        // 4xx counts, the actual status is in the CSV.
        IntPredicate unknownAgreementRejected = isPlayground() ? status -> status == 403 : ConnectorPerformanceTest::isClientError;
        IntPredicate missingTokenRejected = isPlayground() ? status -> status == 401 : ConnectorPerformanceTest::isClientError;
        paths.add(new TimedPath("consumer-data-unknown-agreement", () -> consumerDataUri(size),
                consumerDataHeaders(unknownAgreement), unknownAgreementRejected, -1));
        paths.add(new TimedPath("consumer-data-no-token", () -> consumerDataUri(size),
                consumer.authenticationHeaders(), missingTokenRejected, -1));
        if (config.providerPublicEnabled()) {
            // Contract-Id is sent on purpose: the data consumption metrics filter rejects calls
            // without it, which would hide the authorization outcome this phase looks at.
            paths.add(new TimedPath("provider-public-no-token", () -> providerPublicUri(PROVIDER_PUBLIC_PATH, size),
                    Map.of("Contract-Id", unknownAgreement), status -> status == 401, -1));
            paths.add(new TimedPath("provider-public-invalid-token", () -> providerPublicUri(PROVIDER_PUBLIC_PATH, size),
                    Map.of("Authorization", "invalid-token", "Contract-Id", unknownAgreement), status -> status == 403, -1));
        }
        paths.add(new TimedPath(PATH_DIRECT, () -> directUri(size), directHeaders(), status -> status == 200, expectedBytes(size)));

        var failures = runSeries(PHASE_BEFORE_NEGOTIATION, size, paths, 0, config.rejectionRuns());

        assertThat(failures).as("calls without an established contract").isEmpty();
    }

    @Test
    @Order(2)
    void negotiation_coldStartCost() {
        var successes = 0;
        for (var iteration = 0; iteration < config.coldRuns(); iteration++) {
            var start = System.nanoTime();
            try {
                negotiated = negotiate(iteration);
                successes++;
            } catch (RuntimeException | AssertionError e) {
                // A stalled negotiation (system-tests/readme.md, #684) is a result of the
                // experiment too: recorded as a failed sample rather than aborting the run.
                var elapsed = System.nanoTime() - start;
                report.add(new Sample(PHASE_NEGOTIATION, "total", -1, iteration, false, false, -1, 0, elapsed, elapsed, e.getMessage()));
                MONITOR.warning("Cold negotiation #%d failed after %d ms: %s".formatted(iteration, elapsed / 1_000_000, e.getMessage()));
            }
        }

        assertThat(successes).as("successful negotiations out of %d", config.coldRuns()).isPositive();
    }

    @ParameterizedTest(name = "payload of {0} bytes")
    @MethodSource("payloadSizes")
    @Order(3)
    void afterNegotiation_directVersusConnector(long size) {
        assumeTrue(negotiated != null, "no successful negotiation in the negotiation phase");

        var paths = new ArrayList<TimedPath>();
        paths.add(new TimedPath(PATH_DIRECT, () -> directUri(size), directHeaders(), status -> status == 200, expectedBytes(size)));
        if (config.providerPublicEnabled()) {
            paths.add(new TimedPath(PATH_PROVIDER_PUBLIC, () -> providerPublicUri(edrPath(negotiated.edr()), size),
                    Map.of("Authorization", negotiated.edr().authorization(), "Contract-Id", negotiated.agreementId()),
                    status -> status == 200, expectedBytes(size)));
        }
        paths.add(new TimedPath(PATH_CONSUMER_DATA, () -> consumerDataUri(size), consumerDataHeaders(negotiated.agreementId()),
                status -> status == 200, expectedBytes(size)));

        var failures = runSeries(PHASE_STEADY_STATE, size, paths, config.warmup(), config.runs());

        // Isolated failures are part of the measurement (failure count in the report); a path
        // that never answers means the setup is wrong, not that the connector is slow.
        for (var path : paths) {
            assertThat(failures.stream().filter(f -> f.startsWith(path.name() + " ")).count())
                    .as("failed calls on path %s at %d bytes: %s", path.name(), size, failures)
                    .isLessThan(config.runs());
        }
    }

    @Test
    @Order(4)
    void afterRetirement_connectorRejectsAgain() {
        assumeTrue(config.retirementPhase(), "retirement phase disabled (perf.retirement=false or perf.target=uat)");
        assumeTrue(negotiated != null, "no successful negotiation in the negotiation phase");

        var size = config.sizes().get(0);
        var providerAgreementId = getContractIdFromTransferProcess(localProvider,
                getProviderTransferProcessId(localProvider, negotiated.transferProcessId()));

        var start = System.nanoTime();
        retireAgreement(localProvider, providerAgreementId, "connector performance experiment");
        try {
            await().atMost(DEFAULT_TIMEOUT).pollDelay(Duration.ZERO).pollInterval(POLL_INTERVAL)
                    .until(() -> http.get(consumerDataUri(size), consumerDataHeaders(negotiated.agreementId())).status(), status -> status == 403);
            var propagation = System.nanoTime() - start;
            report.add(new Sample(PHASE_AFTER_RETIREMENT, "retirement-propagation", -1, 0, false, true, 403, 0, propagation, propagation, null));

            var paths = new ArrayList<TimedPath>();
            paths.add(new TimedPath(PATH_CONSUMER_DATA, () -> consumerDataUri(size), consumerDataHeaders(negotiated.agreementId()),
                    status -> status == 403, -1));
            if (config.providerPublicEnabled()) {
                // Open question this phase answers: does the provider data plane still accept an
                // EDR token issued before the retirement? Any rejection counts as expected.
                paths.add(new TimedPath(PATH_PROVIDER_PUBLIC, () -> providerPublicUri(edrPath(negotiated.edr()), size),
                        Map.of("Authorization", negotiated.edr().authorization(), "Contract-Id", negotiated.agreementId()),
                        status -> status >= 400 && status < 500, -1));
            }
            paths.add(new TimedPath(PATH_DIRECT, () -> directUri(size), directHeaders(), status -> status == 200, expectedBytes(size)));

            var failures = runSeries(PHASE_AFTER_RETIREMENT, size, paths, 0, config.rejectionRuns());
            failures.stream().filter(f -> f.startsWith(PATH_PROVIDER_PUBLIC + " ")).findFirst().ifPresent(f ->
                    MONITOR.warning("The provider public API still serves an EDR token issued before the retirement: " + f));

            assertThat(failures.stream().filter(f -> !f.startsWith(PATH_PROVIDER_PUBLIC + " ")))
                    .as("calls after the agreement retirement")
                    .isEmpty();
        } finally {
            reactivateAgreement(localProvider, providerAgreementId);
        }
    }

    Stream<Long> payloadSizes() {
        return config.sizes().stream();
    }

    /**
     * Catalog, negotiation, transfer, EDR and first successful data call, each timed on its own.
     * Polling every {@link #POLL_INTERVAL} bounds the resolution of the asynchronous steps.
     */
    private Negotiated negotiate(int iteration) {
        var size = config.sizes().get(0);
        var start = System.nanoTime();

        var offer = consumer.getOfferForAsset(provider, assetId);
        var afterCatalog = recordStep(iteration, "catalog", start);

        var negotiationId = consumer.initContractNegotiation(provider, offer);
        var negotiationState = awaitState(() -> consumer.getContractNegotiationState(negotiationId), FINALIZED.name(), "TERMINATED");
        assertThat(negotiationState).as("negotiation %s", negotiationId).isEqualTo(FINALIZED.name());
        var agreementId = consumer.getContractAgreementId(negotiationId);
        var afterNegotiation = recordStep(iteration, "negotiation", afterCatalog);

        var transferProcessId = consumer.initiateTransfer(provider, agreementId, null, null, HTTP_DATA_PULL);
        var transferState = awaitState(() -> consumer.getTransferProcessState(transferProcessId), STARTED.name(), TERMINATED.name());
        assertThat(transferState).as("transfer process %s", transferProcessId).isEqualTo(STARTED.name());
        var afterTransfer = recordStep(iteration, "transfer", afterNegotiation);

        var edr = await().atMost(DEFAULT_TIMEOUT).pollDelay(Duration.ZERO).pollInterval(POLL_INTERVAL)
                .until(() -> consumer.findEdr(transferProcessId), Optional::isPresent)
                .orElseThrow();
        var afterEdr = recordStep(iteration, "edr", afterTransfer);

        await().atMost(DEFAULT_TIMEOUT).pollDelay(Duration.ZERO).pollInterval(POLL_INTERVAL)
                .until(() -> http.get(consumerDataUri(size), consumerDataHeaders(agreementId)).status(), status -> status == 200);
        var afterFirstData = recordStep(iteration, "first-data", afterEdr);

        var total = afterFirstData - start;
        report.add(new Sample(PHASE_NEGOTIATION, "total", -1, iteration, false, true, 200, 0, total, total, null));
        MONITOR.info("Cold negotiation #%d: %d ms (agreement %s)".formatted(iteration, total / 1_000_000, agreementId));
        return new Negotiated(transferProcessId, agreementId, edr);
    }

    private long recordStep(int iteration, String step, long since) {
        var now = System.nanoTime();
        report.add(new Sample(PHASE_NEGOTIATION, step, -1, iteration, false, true, 200, 0, now - since, now - since, null));
        return now;
    }

    private String awaitState(Supplier<String> state, String expected, String terminal) {
        return await().atMost(DEFAULT_TIMEOUT).pollDelay(Duration.ZERO).pollInterval(POLL_INTERVAL)
                .until(state::get, s -> expected.equals(s) || terminal.equals(s));
    }

    /**
     * Calls every path {@code warmup} times, then {@code runs} times, rotating which path goes
     * first on each iteration so that a slow drift of the environment spreads evenly over all
     * paths instead of biasing whichever runs last. Returns a description of every measured call
     * whose outcome was not the expected one, each starting with the path name and a space.
     */
    private List<String> runSeries(String phase, long size, List<TimedPath> paths, int warmup, int runs) {
        var failures = new ArrayList<String>();
        for (var iteration = 0; iteration < warmup + runs; iteration++) {
            var isWarmup = iteration < warmup;
            for (var offset = 0; offset < paths.size(); offset++) {
                var path = paths.get((iteration + offset) % paths.size());
                var timing = http.get(path.uri().get(), path.headers());
                var ok = path.expectedStatus().test(timing.status()) &&
                        (path.expectedBytes() < 0 || timing.bytes() == path.expectedBytes());
                report.add(new Sample(phase, path.name(), size, iteration - warmup, isWarmup, ok, timing.status(), timing.bytes(),
                        timing.ttfbNanos(), timing.totalNanos(), timing.error()));
                if (!ok && !isWarmup) {
                    failures.add("%s #%d: status %d, %d bytes%s".formatted(path.name(), iteration - warmup, timing.status(),
                            timing.bytes(), timing.error() == null ? "" : ", " + timing.error()));
                }
            }
        }
        return failures;
    }

    private boolean isPlayground() {
        return config.target() == PerfConfig.Target.PLAYGROUND;
    }

    private long expectedBytes(long size) {
        return isPlayground() ? size : -1;
    }

    private String sizeQuery(long size) {
        return size < 0 ? "" : "?size=" + size;
    }

    private URI directUri(long size) {
        if (isPlayground()) {
            return URI.create(backendForward.baseUri() + BACKEND_PAYLOAD_PATH + sizeQuery(size));
        }
        return URI.create(config.uat().directUrl());
    }

    private Map<String, String> directHeaders() {
        return isPlayground() ? Map.of() : config.uat().directHeaders();
    }

    private URI consumerDataUri(long size) {
        var base = consumer.dataPlaneDataUri().toString();
        if (isPlayground()) {
            return URI.create(base + sizeQuery(size));
        }
        var query = config.uat().connectorQuery();
        return URI.create(query.isBlank() ? base : base + "?" + query);
    }

    /**
     * The consumer data plane looks the EDR up itself from the agreement id. On the playground,
     * what the existing e2e tests send (AbstractParticipant.queryData): the agreement id as both
     * Authorization and Contract-Id. On UAT, what proto-inspiration's EonaXClient sends: the API
     * key and Contract-Id only, Authorization being left to the gateway.
     */
    private Map<String, String> consumerDataHeaders(String agreementId) {
        var headers = new HashMap<>(consumer.authenticationHeaders());
        if (isPlayground()) {
            headers.put("Authorization", agreementId);
        }
        headers.put("Contract-Id", agreementId);
        return headers;
    }

    private static boolean isClientError(int status) {
        return status >= 400 && status < 500;
    }

    /**
     * The provider public API, either through the port-forward to the data plane service (the
     * EDR endpoint is an in-cluster URL) or through the ingress.
     */
    private URI providerPublicUri(String path, long size) {
        if (dataPlaneForward != null) {
            return URI.create(dataPlaneForward.baseUri() + path + sizeQuery(size));
        }
        return URI.create(config.playground().providerPublicIngressUrl() + sizeQuery(size));
    }

    private static String edrPath(Edr edr) {
        var path = URI.create(edr.endpoint()).getRawPath();
        return path == null || path.isEmpty() ? PROVIDER_PUBLIC_PATH : path;
    }
}
