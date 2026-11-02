package org.eclipse.edc.dse.dataplane.api.headers;

import org.eclipse.edc.connector.dataplane.http.spi.HttpDataAddress;
import org.eclipse.edc.connector.dataplane.http.spi.HttpRequestParams;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.edc.spi.constants.CoreConstants.EDC_NAMESPACE;

class ForwardedHeadersParamsDecoratorTest {

    private final ForwardedHeadersParamsDecorator decorator =
            new ForwardedHeadersParamsDecorator(ForwardedHeaders.of(ForwardedHeaders.DEFAULT_ALLOWED));

    @Test
    void decorate_shouldAddNothing_whenAddressDoesNotOptIn() {
        var address = addressBuilder().build();

        var params = decorator.decorate(createRequest(Map.of("forwarded-header:Mcp-Session-Id", "session-1")), address, newParamsBuilder());

        assertThat(params.build().getHeaders()).doesNotContainKey("Mcp-Session-Id");
    }

    @Test
    void decorate_shouldAddForwardedHeaders_whenAddressOptsIn() {
        var address = addressBuilder().property(EDC_NAMESPACE + "proxyHeaders", "true").build();

        var params = decorator.decorate(createRequest(Map.of(
                "forwarded-header:Mcp-Session-Id", "session-1",
                "forwarded-header:MCP-Protocol-Version", "2025-06-18",
                "method", "POST")), address, newParamsBuilder());

        assertThat(params.build().getHeaders())
                .containsEntry("Mcp-Session-Id", "session-1")
                .containsEntry("MCP-Protocol-Version", "2025-06-18")
                .doesNotContainKey("method");
    }

    @Test
    void decorate_shouldAcceptNonNamespacedFlag() {
        var address = addressBuilder().property("proxyHeaders", "true").build();

        var params = decorator.decorate(createRequest(Map.of("forwarded-header:Accept", "application/json")), address, newParamsBuilder());

        assertThat(params.build().getHeaders()).containsEntry("Accept", "application/json");
    }

    @Test
    void decorate_shouldIgnoreHeadersOutsideAllowList() {
        var address = addressBuilder().property("proxyHeaders", "true").build();

        var params = decorator.decorate(createRequest(Map.of("forwarded-header:X-Custom", "value")), address, newParamsBuilder());

        assertThat(params.build().getHeaders()).doesNotContainKey("X-Custom");
    }

    @Test
    void decorate_shouldKeepHeadersSetByProvider() {
        var address = addressBuilder()
                .property("proxyHeaders", "true")
                .addAdditionalHeader("accept", "application/json, text/event-stream")
                .authKey("Mcp-Session-Id")
                .build();

        var params = newParamsBuilder()
                .header("accept", "application/json, text/event-stream")
                .header("Mcp-Session-Id", "provider-value");

        decorator.decorate(createRequest(Map.of(
                "forwarded-header:Accept", "text/plain",
                "forwarded-header:Mcp-Session-Id", "client-value")), address, params);

        assertThat(params.build().getHeaders())
                .containsEntry("accept", "application/json, text/event-stream")
                .containsEntry("Mcp-Session-Id", "provider-value")
                .doesNotContainKey("Accept");
    }

    private static HttpDataAddress.Builder addressBuilder() {
        return HttpDataAddress.Builder.newInstance().baseUrl("http://mcp.example.com");
    }

    private static HttpRequestParams.Builder newParamsBuilder() {
        return HttpRequestParams.Builder.newInstance().baseUrl("http://mcp.example.com").method("POST");
    }

    private static DataFlowStartMessage createRequest(Map<String, String> properties) {
        return DataFlowStartMessage.Builder.newInstance()
                .id(UUID.randomUUID().toString())
                .processId(UUID.randomUUID().toString())
                .sourceDataAddress(DataAddress.Builder.newInstance().type("HttpData").build())
                .destinationDataAddress(DataAddress.Builder.newInstance().type("HttpData").build())
                .properties(properties)
                .build();
    }
}
