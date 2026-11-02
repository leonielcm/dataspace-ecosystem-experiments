package org.eclipse.edc.dse.dataplane.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.json.McpJsonMapper;
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
import org.eclipse.edc.connector.dataplane.http.spi.HttpDataAddress;
import org.eclipse.edc.connector.dataplane.spi.iam.DataPlaneAuthorizationService;
import org.eclipse.edc.connector.dataplane.spi.port.TransferProcessApiClient;
import org.eclipse.edc.edr.spi.store.EndpointDataReferenceStore;
import org.eclipse.edc.edr.spi.types.EndpointDataReferenceEntry;
import org.eclipse.edc.junit.extensions.RuntimeExtension;
import org.eclipse.edc.junit.extensions.RuntimePerClassExtension;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.jetty.ee10.servlet.FilterHolder;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.eclipse.edc.dse.dataplane.api.ConsumerDataPlaneAuthorizationService.EDR_AUTH_CODE;
import static org.eclipse.edc.dse.dataplane.api.ConsumerDataPlaneAuthorizationService.EDR_ENDPOINT;
import static org.eclipse.edc.spi.constants.CoreConstants.EDC_NAMESPACE;
import static org.eclipse.edc.util.io.Ports.getFreePort;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Calls MCP servers (Streamable HTTP) with the official MCP Java SDK client, through both hops of the data plane
 * proxy: consumer data API ({@code /api/data}, agreement id as {@code Authorization}) then provider public API
 * ({@code /api/public}, EDR token) then the MCP server.
 * <p>
 * Contract negotiation is out of scope: the consumer EDR is seeded in the store and the provider-side token
 * check is stubbed, so only the data plane behaviour is under test.
 */
class McpThroughDataPlaneIntegrationTest {

    private static final int PUBLIC_PORT = getFreePort();
    private static final int DATA_PORT = getFreePort();
    private static final int MCP_PORT = getFreePort();

    private static final String PUBLIC_URL = "http://localhost:%d/api/public".formatted(PUBLIC_PORT);
    private static final String DATA_URL = "http://localhost:%d/api/data".formatted(DATA_PORT);
    /** Stateless MCP server: no session. */
    private static final String MCP_STATELESS_URL = "http://localhost:%d/mcp".formatted(MCP_PORT);
    /** Stateful MCP server: the session id it returns on initialize must come back on every later request. */
    private static final String MCP_SESSION_URL = "http://localhost:%d/mcp-session".formatted(MCP_PORT);

    /** Stateless MCP server, asset with {@code proxyHeaders=true}, as an MCP asset must be published. */
    private static final String AGREEMENT_MCP = "agreement-mcp";
    private static final String TOKEN_MCP = "provider-token-mcp";
    /** Stateful MCP server, asset with {@code proxyHeaders=true}. */
    private static final String AGREEMENT_SESSION = "agreement-mcp-session";
    private static final String TOKEN_SESSION = "provider-token-mcp-session";
    /** Stateless MCP server, asset published without {@code proxyHeaders}: the data plane behaves as before. */
    private static final String AGREEMENT_NO_HEADERS = "agreement-mcp-no-headers";
    private static final String TOKEN_NO_HEADERS = "provider-token-mcp-no-headers";

    private static final String RESOURCE_URI = "dataset://poi/sample";
    private static final String RESOURCE_JSON = "{\"id\":\"poi-1\",\"name\":\"Lac d'Annecy\",\"category\":\"nature\"}";
    private static final Resource RESOURCE = Resource.builder().uri(RESOURCE_URI).name("poi-sample").mimeType("application/json").build();

    private static final McpJsonMapper JSON_MAPPER = new JacksonMcpJsonMapper(new ObjectMapper());
    /**
     * Tool schema validation is not under test. The SDK's default validator needs com.networknt:json-schema-validator 2.x,
     * while the root build forces 1.5.8 on every test classpath (for mockserver).
     */
    private static final JsonSchemaValidator NO_SCHEMA_VALIDATION = (schema, content) -> ValidationResponse.asValid(null);
    private static final DataPlaneAuthorizationService PROVIDER_AUTHORIZATION = mock();
    private static final List<RecordedRequest> MCP_SERVER_REQUESTS = new CopyOnWriteArrayList<>();

    @RegisterExtension
    static final RuntimeExtension DATA_PLANE = new RuntimePerClassExtension()
            .setConfiguration(Map.of(
                    "web.http.port", String.valueOf(getFreePort()),
                    "web.http.path", "/api",
                    "web.http.public.port", String.valueOf(PUBLIC_PORT),
                    "web.http.public.path", "/api/public",
                    "web.http.data.port", String.valueOf(DATA_PORT),
                    "web.http.data.path", "/api/data",
                    "edc.dataplane.api.public.baseurl", PUBLIC_URL))
            .registerServiceMock(DataPlaneAuthorizationService.class, PROVIDER_AUTHORIZATION)
            // no control plane: only used to report push transfers, never for proxied calls
            .registerServiceMock(TransferProcessApiClient.class, mock());

    private static Server mcpHttpServer;
    private static McpStatelessSyncServer statelessMcpServer;
    private static McpSyncServer sessionMcpServer;

    @BeforeAll
    static void startMcpServersAndSeedEdrs(EndpointDataReferenceStore edrStore) throws Exception {
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

        seedEdr(edrStore, AGREEMENT_MCP, TOKEN_MCP);
        seedEdr(edrStore, AGREEMENT_SESSION, TOKEN_SESSION);
        seedEdr(edrStore, AGREEMENT_NO_HEADERS, TOKEN_NO_HEADERS);

        var assets = Map.of(
                TOKEN_MCP, mcpAssetAddress(MCP_STATELESS_URL, true),
                TOKEN_SESSION, mcpAssetAddress(MCP_SESSION_URL, true),
                TOKEN_NO_HEADERS, mcpAssetAddress(MCP_STATELESS_URL, false));
        when(PROVIDER_AUTHORIZATION.authorize(anyString(), anyMap())).thenAnswer(invocation -> {
            var address = assets.get(invocation.<String>getArgument(0));
            return address != null ? Result.success(address) : Result.failure("unknown token");
        });
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

    /**
     * Requests received on one MCP endpoint. Filtering by endpoint matters: closing a client of another test
     * (session DELETE) can still be in flight.
     */
    private static List<RecordedRequest> requestsTo(String path) {
        return MCP_SERVER_REQUESTS.stream().filter(request -> request.path().equals(path)).toList();
    }

    @Test
    void mcpClient_shouldReadResourceAndCallTool_throughConsumerAndProviderDataPlanes() {
        try (var client = mcpClient(AGREEMENT_MCP)) {
            var initializeResult = client.initialize();
            assertThat(initializeResult.serverInfo().name()).isEqualTo("poi-mcp-server");

            assertThat(client.listResources().resources())
                    .extracting(Resource::uri)
                    .containsExactly(RESOURCE_URI);

            var resource = client.readResource(new ReadResourceRequest(RESOURCE_URI));
            assertThat(resource.contents()).singleElement()
                    .isInstanceOfSatisfying(TextResourceContents.class, contents -> assertThat(contents.text()).isEqualTo(RESOURCE_JSON));

            assertThat(client.listTools().tools())
                    .extracting(Tool::name)
                    .containsExactly("search_poi");

            var toolResult = client.callTool(new CallToolRequest("search_poi", Map.of("query", "lac")));
            assertThat(toolResult.isError()).isFalse();
            assertThat(toolResult.content()).singleElement()
                    .isInstanceOfSatisfying(TextContent.class, content -> assertThat(content.text()).isEqualTo("1 result for lac"));
        }

        var requests = requestsTo("/mcp");
        assertThat(requests).isNotEmpty().allSatisfy(request -> {
            assertThat(request.headers()).containsKey("mcp-protocol-version");
            assertThat(request.headers().get("accept")).contains("text/event-stream");
            // the agreement id (consumer hop) and the EDR token (provider hop) never reach the MCP server
            assertThat(request.headers()).doesNotContainKey("authorization");
        });
        // JSON-RPC messages are POSTed; the client also opens a GET for the optional server-to-client SSE stream
        assertThat(requests)
                .filteredOn(request -> request.method().equals("POST"))
                .isNotEmpty()
                .allSatisfy(request -> assertThat(request.headers().get("accept")).contains("application/json"));
    }

    @Test
    void mcpClient_shouldKeepTheSession_ofStatefulServer() {
        try (var client = mcpClient(AGREEMENT_SESSION)) {
            assertThat(client.initialize().serverInfo().name()).isEqualTo("poi-mcp-session-server");

            var resource = client.readResource(new ReadResourceRequest(RESOURCE_URI));
            assertThat(resource.contents()).singleElement()
                    .isInstanceOfSatisfying(TextResourceContents.class, contents -> assertThat(contents.text()).isEqualTo(RESOURCE_JSON));
        }

        var posts = requestsTo("/mcp-session").stream().filter(request -> request.method().equals("POST")).toList();
        assertThat(posts).hasSizeGreaterThanOrEqualTo(3); // initialize, notifications/initialized, resources/read
        // initialize opens the session; every later request carries the id the server returned through both hops
        assertThat(posts.get(0).headers()).doesNotContainKey("mcp-session-id");
        var sessionIds = posts.subList(1, posts.size()).stream().map(request -> request.headers().get("mcp-session-id")).distinct().toList();
        assertThat(sessionIds).singleElement().isNotNull();
    }

    /**
     * A stateless MCP server answers 405 to the SSE stream GET. The provider hop relays it and marks it as relayed,
     * so the consumer hop relays it in turn.
     */
    @Test
    void sseStreamGet_shouldGetTheServer405_throughBothHops() throws Exception {
        var httpClient = HttpClient.newHttpClient();

        var providerHop = httpClient.send(sseGet(PUBLIC_URL, TOKEN_MCP), HttpResponse.BodyHandlers.ofString());
        assertThat(providerHop.statusCode()).isEqualTo(405);
        assertThat(providerHop.headers().firstValue("X-Dse-Proxy-Relayed")).contains("true");

        var consumerHop = httpClient.send(sseGet(DATA_URL, AGREEMENT_MCP), HttpResponse.BodyHandlers.ofString());
        assertThat(consumerHop.statusCode()).isEqualTo(405);
        assertThat(consumerHop.headers().firstValue("X-Dse-Proxy-Relayed")).isEmpty();
    }

    /**
     * Without {@code proxyHeaders}, the data plane behaves as before: headers are not relayed, so the MCP client
     * cannot even initialize, and a non-2xx answer of the source is reported as a 500.
     */
    @Test
    void assetWithoutProxyHeaders_shouldBehaveAsBefore() throws Exception {
        try (var client = mcpClient(AGREEMENT_NO_HEADERS)) {
            assertThatThrownBy(client::initialize).isInstanceOf(RuntimeException.class);
        }
        assertThat(requestsTo("/mcp")).isNotEmpty()
                .allSatisfy(request -> assertThat(request.headers().get("accept")).isNull());

        var consumerHop = HttpClient.newHttpClient().send(sseGet(DATA_URL, AGREEMENT_NO_HEADERS), HttpResponse.BodyHandlers.ofString());
        assertThat(consumerHop.statusCode()).isEqualTo(500);
        assertThat(consumerHop.body()).contains("Received code transferring HTTP data: 500");
    }

    private static McpSyncClient mcpClient(String agreementId) {
        var transport = HttpClientStreamableHttpTransport.builder("http://localhost:" + DATA_PORT)
                .endpoint("/api/data")
                .jsonMapper(JSON_MAPPER)
                .httpRequestCustomizer((request, method, uri, body, context) -> request.header("Authorization", agreementId))
                .build();
        return McpClient.sync(transport)
                .jsonSchemaValidator(NO_SCHEMA_VALIDATION)
                .requestTimeout(Duration.ofSeconds(10))
                .initializationTimeout(Duration.ofSeconds(10))
                .build();
    }

    private static ReadResourceResult readResource(ReadResourceRequest request) {
        return new ReadResourceResult(List.of(new TextResourceContents(request.uri(), "application/json", RESOURCE_JSON)));
    }

    /** Data address of the MCP asset, as the provider would publish it. */
    private static DataAddress mcpAssetAddress(String url, boolean relayHeaders) {
        var builder = HttpDataAddress.Builder.newInstance()
                .baseUrl(url)
                .proxyMethod(Boolean.TRUE.toString())
                .proxyBody(Boolean.TRUE.toString());
        if (relayHeaders) {
            builder.property(EDC_NAMESPACE + "proxyHeaders", Boolean.TRUE.toString());
        }
        return builder.build();
    }

    private static void seedEdr(EndpointDataReferenceStore edrStore, String agreementId, String providerToken) {
        var transferProcessId = UUID.randomUUID().toString();
        var entry = EndpointDataReferenceEntry.Builder.newInstance()
                .id(transferProcessId)
                .assetId("mcp-asset")
                .agreementId(agreementId)
                .contractNegotiationId(UUID.randomUUID().toString())
                .transferProcessId(transferProcessId)
                .providerId("provider")
                .createdAt(Instant.now().toEpochMilli())
                .build();
        var edr = DataAddress.Builder.newInstance()
                .type("https://w3id.org/idsa/v4.1/HTTP")
                .property(EDR_ENDPOINT, PUBLIC_URL)
                .property(EDR_AUTH_CODE, providerToken)
                .build();
        assertThat(edrStore.save(entry, edr).succeeded()).isTrue();
    }

    private static HttpRequest sseGet(String url, String authorization) {
        return HttpRequest.newBuilder(URI.create(url))
                .header("Authorization", authorization)
                .header("Accept", "text/event-stream")
                .GET()
                .build();
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
