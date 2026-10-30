package org.eclipse.edc.test.system.perf;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class TimedHttpClientTest {

    private static final int BODY_SIZE = 200_000;

    private final TimedHttpClient client = new TimedHttpClient(Duration.ofSeconds(2), Duration.ofSeconds(5));
    private final AtomicReference<String> receivedHeader = new AtomicReference<>();
    private HttpServer server;

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/payload", exchange -> {
            receivedHeader.set(exchange.getRequestHeaders().getFirst("X-Test"));
            exchange.sendResponseHeaders(200, BODY_SIZE);
            try (var body = exchange.getResponseBody()) {
                body.write(new byte[BODY_SIZE]);
            }
        });
        server.createContext("/forbidden", exchange -> {
            exchange.sendResponseHeaders(403, -1);
            exchange.close();
        });
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    @Test
    void get_readsWholeBodyAndSendsHeaders() {
        var timing = client.get(uri("/payload"), Map.of("X-Test", "value"));

        assertThat(timing.status()).isEqualTo(200);
        assertThat(timing.bytes()).isEqualTo(BODY_SIZE);
        assertThat(timing.error()).isNull();
        assertThat(timing.ttfbNanos()).isPositive().isLessThanOrEqualTo(timing.totalNanos());
        assertThat(receivedHeader.get()).isEqualTo("value");
    }

    @Test
    void get_errorStatus_returnedNotThrown() {
        var timing = client.get(uri("/forbidden"), Map.of());

        assertThat(timing.status()).isEqualTo(403);
        assertThat(timing.bytes()).isZero();
    }

    @Test
    void get_connectionRefused_statusMinusOneWithError() throws IOException {
        int closedPort;
        try (var socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }

        var timing = client.get(URI.create("http://127.0.0.1:" + closedPort + "/"), Map.of());

        assertThat(timing.status()).isEqualTo(-1);
        assertThat(timing.error()).isNotBlank();
    }

    private URI uri(String path) {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + path);
    }
}
