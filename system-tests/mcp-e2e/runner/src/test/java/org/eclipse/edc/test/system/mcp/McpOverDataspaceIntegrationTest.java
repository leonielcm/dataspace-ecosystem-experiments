package org.eclipse.edc.test.system.mcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.json.TypeRef;
import io.modelcontextprotocol.json.jackson2.JacksonMcpJsonMapper;
import io.modelcontextprotocol.json.schema.JsonSchemaValidator;
import io.modelcontextprotocol.json.schema.JsonSchemaValidator.ValidationResponse;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.server.McpStatelessServerFeatures;
import io.modelcontextprotocol.server.McpStatelessSyncServer;
import io.modelcontextprotocol.server.McpSyncServer;
import io.modelcontextprotocol.server.transport.HttpServletStatelessServerTransport;
import io.modelcontextprotocol.server.transport.HttpServletStreamableServerTransportProvider;
import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.ReadResourceRequest;
import io.modelcontextprotocol.spec.McpSchema.ReadResourceResult;
import io.modelcontextprotocol.spec.McpSchema.Resource;
import io.modelcontextprotocol.spec.McpSchema.ServerCapabilities;
import io.modelcontextprotocol.spec.McpSchema.TextContent;
import io.modelcontextprotocol.spec.McpSchema.TextResourceContents;
import io.modelcontextprotocol.spec.McpSchema.Tool;
import jakarta.servlet.DispatcherType;
import jakarta.servlet.http.HttpServletRequest;
import org.eclipse.edc.junit.annotations.Runtime;
import org.eclipse.edc.junit.extensions.ComponentRuntimeExtension;
import org.eclipse.edc.junit.extensions.RuntimeExtension;
import org.eclipse.edc.junit.utils.Endpoints;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.system.configuration.Config;
import org.eclipse.edc.spi.system.configuration.ConfigFactory;
import org.eclipse.jetty.ee10.servlet.FilterHolder;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.PostgreSQLContainer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyPairGenerator;
import java.security.spec.ECGenParameterSpec;
import java.time.Duration;
import java.util.Base64;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.eclipse.edc.connector.controlplane.test.system.utils.PolicyFixtures.noConstraintPolicy;
import static org.eclipse.edc.connector.controlplane.transfer.spi.types.TransferProcessStates.STARTED;
import static org.eclipse.edc.util.io.Ports.getFreePort;

/**
 * Calls MCP servers through a whole, locally started dataspace: the provider publishes the MCP server as an asset
 * through its management API, the consumer negotiates a contract and starts a PULL transfer over DSP, then the
 * official MCP Java SDK client calls the consumer data API with the agreement id, which goes through the provider
 * data plane (real EDR token) to the MCP server.
 * <p>
 * What is not real: identity (EDC mock identity service instead of DCP, so a policy without constraint), control
 * plane storage and vaults (in memory). The consumer control plane and data plane share one runtime, where they share
 * a database and a vault in a deployment. The data planes are the project's data plane launcher, on a Postgres
 * started with Testcontainers (Docker required), which their data consumption metrics need.
 */
class McpOverDataspaceIntegrationTest {

    private static final String CONTROL_PLANE = ":system-tests:mcp-e2e:control-plane";
    private static final String DATA_PLANE = ":system-tests:mcp-e2e:data-plane";

    private static final String CONSUMER = "consumer";
    private static final String PROVIDER_CP = "provider-control-plane";
    private static final String PROVIDER_DP = "provider-data-plane";

    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    static {
        // started before the runtimes boot: their configuration is read lazily, at boot
        POSTGRES.start();
    }

    private static final Endpoints CONSUMER_ENDPOINTS = Endpoints.Builder.newInstance()
            .endpoint("management", () -> localUri("/management"))
            .endpoint("protocol", () -> localUri("/protocol"))
            .endpoint("control", () -> localUri("/control"))
            .endpoint("public", () -> localUri("/api/public"))
            .endpoint("data", () -> localUri("/api/data"))
            .build();
    private static final Endpoints PROVIDER_CP_ENDPOINTS = Endpoints.Builder.newInstance()
            .endpoint("management", () -> localUri("/management"))
            .endpoint("protocol", () -> localUri("/protocol"))
            .endpoint("control", () -> localUri("/control"))
            .build();
    private static final Endpoints PROVIDER_DP_ENDPOINTS = Endpoints.Builder.newInstance()
            .endpoint("control", () -> localUri("/control"))
            .endpoint("public", () -> localUri("/api/public"))
            .build();

    @RegisterExtension
    @Order(0)
    static final RuntimeExtension PROVIDER_CONTROL_PLANE = ComponentRuntimeExtension.Builder.newInstance()
            .name(PROVIDER_CP)
            .modules(CONTROL_PLANE)
            .endpoints(PROVIDER_CP_ENDPOINTS)
            .configurationProvider(() -> controlPlaneConfig("provider", PROVIDER_CP_ENDPOINTS))
            .build();

    @RegisterExtension
    @Order(1)
    static final RuntimeExtension PROVIDER_DATA_PLANE = ComponentRuntimeExtension.Builder.newInstance()
            .name(PROVIDER_DP)
            .modules(DATA_PLANE)
            .endpoints(PROVIDER_DP_ENDPOINTS)
            .configurationProvider(() -> dataPlaneConfig("provider", PROVIDER_DP_ENDPOINTS, PROVIDER_CP_ENDPOINTS))
            .build();

    @RegisterExtension
    @Order(2)
    static final RuntimeExtension CONSUMER_RUNTIME = ComponentRuntimeExtension.Builder.newInstance()
            .name(CONSUMER)
            .modules(CONTROL_PLANE, DATA_PLANE)
            .endpoints(CONSUMER_ENDPOINTS)
            .configurationProvider(() -> controlPlaneConfig(CONSUMER, CONSUMER_ENDPOINTS)
                    .merge(dataPlaneConfig(CONSUMER, CONSUMER_ENDPOINTS, CONSUMER_ENDPOINTS)))
            .build();

    private static final McpParticipant PROVIDER = McpParticipant.of("provider", PROVIDER_CP_ENDPOINTS);
    private static final McpParticipant CONSUMER_PARTICIPANT = McpParticipant.of(CONSUMER, CONSUMER_ENDPOINTS);

    private static final int MCP_PORT = getFreePort();
    private static final String RESOURCE_URI = "dataset://poi/sample";
    private static final String RESOURCE_JSON = "{\"id\":\"poi-1\",\"name\":\"Lac d'Annecy\",\"category\":\"nature\"}";
    private static final Resource RESOURCE = Resource.builder().uri(RESOURCE_URI).name("poi-sample").mimeType("application/json").build();

    private static final McpJsonMapper JSON_MAPPER = new JacksonMcpJsonMapper(new ObjectMapper());
    /**
     * Tool schema validation is not under test. The SDK's default validator needs com.networknt:json-schema-validator 2.x,
     * while the root build forces 1.5.8 on every test classpath (for mockserver).
     */
    private static final JsonSchemaValidator NO_SCHEMA_VALIDATION = (schema, content) -> ValidationResponse.asValid(null);
    private static final List<RecordedRequest> MCP_SERVER_REQUESTS = new CopyOnWriteArrayList<>();

    private static Server mcpHttpServer;
    private static McpStatelessSyncServer statelessMcpServer;
    private static McpSyncServer sessionMcpServer;

    @BeforeAll
    static void setUp(@Runtime(PROVIDER_DP) Vault providerDataPlaneVault, @Runtime(CONSUMER) Vault consumerVault) throws Exception {
        seedTokenKeys(providerDataPlaneVault);
        seedTokenKeys(consumerVault);
        startMcpServers();

        publishMcpAsset("mcp-stateless", "http://localhost:%d/mcp".formatted(MCP_PORT));
        publishMcpAsset("mcp-session", "http://localhost:%d/mcp-session".formatted(MCP_PORT));
    }

    @AfterAll
    static void stopMcpServers() throws Exception {
        if (statelessMcpServer != null) {
            statelessMcpServer.close();
        }
        if (sessionMcpServer != null) {
            sessionMcpServer.close();
        }
        if (mcpHttpServer != null) {
            mcpHttpServer.stop();
        }
    }

    @BeforeEach
    void clearRecordedRequests() {
        MCP_SERVER_REQUESTS.clear();
    }

    @Test
    void mcpClient_shouldBeRejected_withoutAgreement() throws Exception {
        var request = HttpRequest.newBuilder(dataApiUri())
                .header("Authorization", "unknown-agreement")
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}"))
                .build();

        var response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(403);
        assertThat(MCP_SERVER_REQUESTS).isEmpty();
    }

    @Test
    void mcpClient_shouldReadResourceAndCallTool_afterNegotiation() {
        var agreementId = negotiateAndStartTransfer("mcp-stateless");

        try (var client = mcpClient(agreementId)) {
            assertThat(client.initialize().serverInfo().name()).isEqualTo("poi-mcp-server");

            var resource = client.readResource(new ReadResourceRequest(RESOURCE_URI));
            assertThat(resource.contents()).singleElement()
                    .isInstanceOfSatisfying(TextResourceContents.class, contents -> assertThat(contents.text()).isEqualTo(RESOURCE_JSON));

            var toolResult = client.callTool(new CallToolRequest("search_poi", Map.of("query", "lac")));
            assertThat(toolResult.isError()).isFalse();
            assertThat(toolResult.content()).singleElement()
                    .isInstanceOfSatisfying(TextContent.class, content -> assertThat(content.text()).isEqualTo("1 result for lac"));
        }

        assertThat(requestsTo("/mcp")).isNotEmpty().allSatisfy(request -> {
            assertThat(request.headers()).containsKey("mcp-protocol-version");
            // neither the agreement id nor the provider data plane token reach the MCP server
            assertThat(request.headers()).doesNotContainKey("authorization");
        });
    }

    @Test
    void mcpClient_shouldKeepTheSession_ofStatefulServer_afterNegotiation() {
        var agreementId = negotiateAndStartTransfer("mcp-session");

        try (var client = mcpClient(agreementId)) {
            assertThat(client.initialize().serverInfo().name()).isEqualTo("poi-mcp-session-server");

            var resource = client.readResource(new ReadResourceRequest(RESOURCE_URI));
            assertThat(resource.contents()).singleElement()
                    .isInstanceOfSatisfying(TextResourceContents.class, contents -> assertThat(contents.text()).isEqualTo(RESOURCE_JSON));
        }

        var posts = requestsTo("/mcp-session").stream().filter(request -> request.method().equals("POST")).toList();
        assertThat(posts).hasSizeGreaterThanOrEqualTo(3); // initialize, notifications/initialized, resources/read
        assertThat(posts.get(0).headers()).doesNotContainKey("mcp-session-id");
        var sessionIds = posts.subList(1, posts.size()).stream().map(request -> request.headers().get("mcp-session-id")).distinct().toList();
        assertThat(sessionIds).singleElement().isNotNull();
    }

    /**
     * Same path, to a real MCP server on the Internet, compared with a direct connection to it. Skipped unless
     * {@code mcp.real.url} is set, since it calls a third-party service, e.g.
     * {@code -Dmcp.real.url=https://mcp.parisjetaime.com/mcp -Dmcp.real.tool=paris_get_events -Dmcp.real.args='{"keyword":"jazz"}'}.
     * The optional {@code mcp.real.tool} is called with the optional {@code mcp.real.args} (JSON object): pick a
     * read-only tool.
     */
    @Test
    @EnabledIfSystemProperty(named = "mcp.real.url", matches = ".+")
    void mcpClient_shouldReachRealServer_afterNegotiation() throws Exception {
        var url = URI.create(System.getProperty("mcp.real.url"));
        publishMcpAsset("mcp-real", url.toString());
        var agreementId = negotiateAndStartTransfer("mcp-real");

        List<String> directTools;
        try (var direct = mcpClient(HttpClientStreamableHttpTransport.builder(url.getScheme() + "://" + url.getAuthority())
                .endpoint(url.getPath().isEmpty() ? "/" : url.getPath())
                .jsonMapper(JSON_MAPPER)
                .build())) {
            direct.initialize();
            directTools = direct.listTools().tools().stream().map(Tool::name).toList();
        }

        try (var client = mcpClient(agreementId)) {
            var initializeResult = client.initialize();
            System.out.printf("Through the dataspace: %s %s, %d tools%n", initializeResult.serverInfo().name(),
                    initializeResult.serverInfo().version(), directTools.size());

            assertThat(client.listTools().tools()).extracting(Tool::name).containsExactlyElementsOf(directTools);

            var tool = System.getProperty("mcp.real.tool");
            if (tool != null) {
                var args = System.getProperty("mcp.real.args", "{}");
                var result = client.callTool(new CallToolRequest(tool, JSON_MAPPER.readValue(args, new TypeRef<Map<String, Object>>() { })));
                assertThat(result.isError()).isNotEqualTo(Boolean.TRUE);
                assertThat(result.content()).isNotEmpty();
                var text = result.content().get(0) instanceof TextContent content ? content.text() : result.content().get(0).toString();
                System.out.printf("%s %s through the dataspace: %s%n", tool, args, text.substring(0, Math.min(1000, text.length())));
            }
        }
    }

    /**
     * Negotiates a contract for the asset and starts a PULL transfer, then waits for the consumer to hold the EDR.
     *
     * @return the agreement id, which the consumer data API takes as {@code Authorization}.
     */
    private static String negotiateAndStartTransfer(String assetId) {
        var transferProcessId = CONSUMER_PARTICIPANT.requestAssetFrom(assetId, PROVIDER)
                .withTransferType("HttpData-PULL")
                .execute();
        CONSUMER_PARTICIPANT.awaitTransferToBeInState(transferProcessId, STARTED);
        await().atMost(Duration.ofSeconds(30)).until(() -> CONSUMER_PARTICIPANT.hasEdr(transferProcessId));
        return CONSUMER_PARTICIPANT.getAgreementId(transferProcessId);
    }

    /**
     * Publishes an MCP server the way a provider would: an HttpData asset relaying method, body and headers.
     */
    private static void publishMcpAsset(String assetId, String mcpUrl) {
        PROVIDER.createAsset(assetId, Map.of("description", "MCP server"), Map.of(
                "type", "HttpData",
                "baseUrl", mcpUrl,
                "proxyMethod", "true",
                "proxyBody", "true",
                "proxyHeaders", "true"));
        var policyId = PROVIDER.createPolicyDefinition(noConstraintPolicy());
        PROVIDER.createContractDefinition(assetId, "contract-" + assetId, policyId, policyId);
    }

    private static McpSyncClient mcpClient(String agreementId) {
        var dataApi = dataApiUri();
        return mcpClient(HttpClientStreamableHttpTransport.builder("http://localhost:" + dataApi.getPort())
                .endpoint(dataApi.getPath())
                .jsonMapper(JSON_MAPPER)
                .httpRequestCustomizer((request, method, uri, body, context) -> request.header("Authorization", agreementId))
                .build());
    }

    private static McpSyncClient mcpClient(HttpClientStreamableHttpTransport transport) {
        return McpClient.sync(transport)
                .jsonSchemaValidator(NO_SCHEMA_VALIDATION)
                .requestTimeout(Duration.ofSeconds(20))
                .initializationTimeout(Duration.ofSeconds(20))
                .build();
    }

    private static URI dataApiUri() {
        return CONSUMER_ENDPOINTS.getEndpoint("data").get();
    }

    private static Config controlPlaneConfig(String participantId, Endpoints endpoints) {
        return ConfigFactory.fromMap(Map.of(
                "edc.participant.id", participantId,
                "edc.dsp.callback.address", endpoints.getEndpoint("protocol").get().toString(),
                "edc.negotiation.consumer.state-machine.iteration-wait-millis", "50",
                "edc.negotiation.provider.state-machine.iteration-wait-millis", "50",
                "edc.transfer.state-machine.iteration-wait-millis", "50",
                "edc.data.plane.selector.state-machine.iteration-wait-millis", "100"));
    }

    private static Config dataPlaneConfig(String participantId, Endpoints endpoints, Endpoints controlPlaneEndpoints) {
        return ConfigFactory.fromMap(Map.ofEntries(
                Map.entry("edc.participant.id", participantId),
                Map.entry("edc.dataplane.api.public.baseurl", endpoints.getEndpoint("public").get().toString()),
                Map.entry("edc.dpf.selector.url", controlPlaneEndpoints.getEndpoint("control").get() + "/v1/dataplanes"),
                Map.entry("edc.transfer.proxy.token.signer.privatekey.alias", "private-key"),
                Map.entry("edc.transfer.proxy.token.verifier.publickey.alias", "public-key"),
                Map.entry("edc.dataplane.state-machine.iteration-wait-millis", "50"),
                Map.entry("edc.datasource.default.url", POSTGRES.getJdbcUrl()),
                Map.entry("edc.datasource.default.user", POSTGRES.getUsername()),
                Map.entry("edc.datasource.default.password", POSTGRES.getPassword()),
                Map.entry("edc.sql.schema.autocreate", "true")));
    }

    /**
     * Key pair the data plane signs and verifies its access tokens with.
     */
    private static void seedTokenKeys(Vault vault) throws Exception {
        var generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(new ECGenParameterSpec("secp256r1"));
        var keyPair = generator.generateKeyPair();
        vault.storeSecret("private-key", pem("PRIVATE KEY", keyPair.getPrivate().getEncoded()));
        vault.storeSecret("public-key", pem("PUBLIC KEY", keyPair.getPublic().getEncoded()));
    }

    private static String pem(String type, byte[] der) {
        var body = Base64.getMimeEncoder(64, "\n".getBytes()).encodeToString(der);
        return "-----BEGIN %s-----\n%s\n-----END %s-----\n".formatted(type, body, type);
    }

    private static void startMcpServers() throws Exception {
        var statelessTransport = HttpServletStatelessServerTransport.builder()
                .jsonMapper(JSON_MAPPER)
                .messageEndpoint("/mcp")
                .build();
        statelessMcpServer = McpServer.sync(statelessTransport)
                .jsonMapper(JSON_MAPPER)
                .jsonSchemaValidator(NO_SCHEMA_VALIDATION)
                .serverInfo("poi-mcp-server", "1.0.0")
                .capabilities(ServerCapabilities.builder().resources(false, false).tools(false).build())
                .resources(new McpStatelessServerFeatures.SyncResourceSpecification(RESOURCE, (context, request) -> readResource(request)))
                .tools(McpStatelessServerFeatures.SyncToolSpecification.builder()
                        .tool(Tool.builder()
                                .name("search_poi")
                                .description("Search points of interest")
                                .inputSchema(Map.of(
                                        "type", "object",
                                        "properties", Map.of("query", Map.of("type", "string")),
                                        "required", List.of("query")))
                                .build())
                        .callHandler((context, request) -> CallToolResult.builder()
                                .addTextContent("1 result for " + request.arguments().get("query"))
                                .isError(false)
                                .build())
                        .build())
                .build();

        var sessionTransport = HttpServletStreamableServerTransportProvider.builder()
                .jsonMapper(JSON_MAPPER)
                .mcpEndpoint("/mcp-session")
                .build();
        sessionMcpServer = McpServer.sync(sessionTransport)
                .jsonMapper(JSON_MAPPER)
                .jsonSchemaValidator(NO_SCHEMA_VALIDATION)
                .serverInfo("poi-mcp-session-server", "1.0.0")
                .capabilities(ServerCapabilities.builder().resources(false, false).build())
                .resources(new McpServerFeatures.SyncResourceSpecification(RESOURCE, (exchange, request) -> readResource(request)))
                .build();

        var handler = new ServletContextHandler();
        handler.addFilter(new FilterHolder((request, response, chain) -> {
            MCP_SERVER_REQUESTS.add(RecordedRequest.of((HttpServletRequest) request));
            chain.doFilter(request, response);
        }), "/*", EnumSet.of(DispatcherType.REQUEST));
        handler.addServlet(new ServletHolder(statelessTransport), "/mcp");
        handler.addServlet(new ServletHolder(sessionTransport), "/mcp-session");
        mcpHttpServer = new Server(MCP_PORT);
        mcpHttpServer.setHandler(handler);
        mcpHttpServer.start();
    }

    private static ReadResourceResult readResource(ReadResourceRequest request) {
        return new ReadResourceResult(List.of(new TextResourceContents(request.uri(), "application/json", RESOURCE_JSON)));
    }

    /**
     * Requests received on one MCP endpoint. Filtering by endpoint matters: closing a client of another test
     * (session DELETE) can still be in flight.
     */
    private static List<RecordedRequest> requestsTo(String path) {
        return MCP_SERVER_REQUESTS.stream().filter(request -> request.path().equals(path)).toList();
    }

    private static URI localUri(String path) {
        return URI.create("http://localhost:%d%s".formatted(getFreePort(), path));
    }

    /**
     * Request as received by the MCP server, header names lower-cased.
     */
    private record RecordedRequest(String path, String method, Map<String, String> headers) {

        static RecordedRequest of(HttpServletRequest request) {
            var headers = new HashMap<String, String>();
            request.getHeaderNames().asIterator()
                    .forEachRemaining(name -> headers.put(name.toLowerCase(), request.getHeader(name)));
            return new RecordedRequest(request.getRequestURI(), request.getMethod(), headers);
        }
    }
}
