package org.eclipse.edc.dse.dataplane.api.headers;

import okhttp3.MediaType;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.eclipse.edc.connector.dataplane.http.params.HttpRequestFactory;
import org.eclipse.edc.connector.dataplane.http.spi.HttpRequestParams;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.StreamFailure;
import org.eclipse.edc.http.spi.EdcHttpClient;
import org.eclipse.edc.spi.monitor.Monitor;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.edc.dse.dataplane.api.headers.ForwardedHeaders.RELAYED_RESPONSE_HEADER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ProxyHttpDataSourceTest {

    private static final String URL = "http://mcp.example.com/mcp";

    private final EdcHttpClient httpClient = mock();

    @Test
    void openPartStream_shouldRelayStatusBodyAndAllowedHeaders() throws IOException {
        respondWith(response(405, "Method Not Allowed", "{\"error\":\"no stream\"}", Map.of(
                "Mcp-Session-Id", "session-1",
                "Set-Cookie", "secret",
                "X-Other", "value")));

        var part = openSinglePart(source(false));

        assertThat(part.statusCode()).isEqualTo(405);
        assertThat(part.mediaType()).startsWith("application/json");
        assertThat(part.headers()).containsExactly(Map.entry("Mcp-Session-Id", "session-1"));
        assertThat(new String(part.openStream().readAllBytes())).isEqualTo("{\"error\":\"no stream\"}");
    }

    @Test
    void openPartStream_shouldRelayEmptyAcceptedResponse() throws IOException {
        respondWith(response(202, "Accepted", null, Map.of()));

        var part = openSinglePart(source(false));

        assertThat(part.statusCode()).isEqualTo(202);
        assertThat(part.openStream().readAllBytes()).isEmpty();
    }

    @Test
    void openPartStream_onConsumerHop_shouldRelayErrorMarkedByProvider() throws IOException {
        respondWith(response(405, "Method Not Allowed", "{}", Map.of(RELAYED_RESPONSE_HEADER, "true")));

        var part = openSinglePart(source(true));

        assertThat(part.statusCode()).isEqualTo(405);
        assertThat(part.headers()).doesNotContainKey(RELAYED_RESPONSE_HEADER);
    }

    @Test
    void openPartStream_onConsumerHop_shouldFailAsBefore_whenErrorIsNotMarked() throws IOException {
        respondWith(response(500, "Internal Server Error", "{}", Map.of()));

        var result = source(true).openPartStream();

        assertThat(result.failed()).isTrue();
        assertThat(result.getFailure().getReason()).isEqualTo(StreamFailure.Reason.GENERAL_ERROR);
        assertThat(result.getFailureMessages()).containsExactly("Received code transferring HTTP data: 500 - Internal Server Error.");
    }

    @Test
    void openPartStream_onConsumerHop_shouldReportUnmarkedRejectionAsNotAuthorized() throws IOException {
        respondWith(response(403, "Forbidden", "{}", Map.of()));

        var result = source(true).openPartStream();

        assertThat(result.failed()).isTrue();
        assertThat(result.getFailure().getReason()).isEqualTo(StreamFailure.Reason.NOT_AUTHORIZED);
    }

    private ProxyHttpDataSource source(boolean consumerHop) {
        var params = HttpRequestParams.Builder.newInstance().baseUrl(URL).method("GET").build();
        return new ProxyHttpDataSource("mcp", params, consumerHop, ForwardedHeaders.of(ForwardedHeaders.DEFAULT_ALLOWED_RESPONSE),
                httpClient, new HttpRequestFactory(), mock(Monitor.class));
    }

    private void respondWith(Response response) throws IOException {
        when(httpClient.execute(any(Request.class))).thenReturn(response);
    }

    private static ProxyHttpPart openSinglePart(DataSource source) {
        var result = source.openPartStream();
        assertThat(result.succeeded()).isTrue();
        return result.getContent().findFirst()
                .map(ProxyHttpPart.class::cast)
                .orElseThrow();
    }

    private static Response response(int code, String message, String json, Map<String, String> headers) {
        var builder = new Response.Builder()
                .request(new Request.Builder().url(URL).build())
                .protocol(Protocol.HTTP_1_1)
                .code(code)
                .message(message)
                .body(json == null ? ResponseBody.create(new byte[0], null) : ResponseBody.create(json, MediaType.get("application/json")));
        headers.forEach(builder::header);
        return builder.build();
    }
}
