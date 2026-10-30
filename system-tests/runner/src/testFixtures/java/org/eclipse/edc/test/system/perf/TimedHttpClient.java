package org.eclipse.edc.test.system.perf;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

/**
 * Plain JDK HTTP client used for every timed call, whatever the path (direct, provider public API,
 * consumer data API). RestAssured is deliberately not used here: it opens a new connection per
 * request and adds logging/filter overhead, which would blur the difference between paths. This
 * client keeps connections alive and measures two things per call:
 * <ul>
 *     <li>time to first byte: until the status line and headers are received;</li>
 *     <li>total time: until the last byte of the body has been read.</li>
 * </ul>
 */
public class TimedHttpClient {

    private static final int BUFFER_SIZE = 64 * 1024;

    private final HttpClient client;
    private final Duration requestTimeout;

    public TimedHttpClient(Duration connectTimeout, Duration requestTimeout) {
        this.client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(connectTimeout)
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
        this.requestTimeout = requestTimeout;
    }

    public Timing get(URI uri, Map<String, String> headers) {
        var builder = HttpRequest.newBuilder(uri).timeout(requestTimeout).GET();
        headers.forEach(builder::header);
        var request = builder.build();

        var start = System.nanoTime();
        try {
            var response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
            var ttfb = System.nanoTime() - start;
            long bytes;
            try (var body = response.body()) {
                bytes = drain(body);
            }
            return new Timing(response.statusCode(), bytes, ttfb, System.nanoTime() - start, null);
        } catch (IOException e) {
            return Timing.failure(System.nanoTime() - start, e.getClass().getSimpleName() + ": " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Timing.failure(System.nanoTime() - start, "interrupted");
        }
    }

    private static long drain(InputStream body) throws IOException {
        var buffer = new byte[BUFFER_SIZE];
        long total = 0;
        int read;
        while ((read = body.read(buffer)) != -1) {
            total += read;
        }
        return total;
    }

    /**
     * Result of one timed call. {@code status} is -1 when no HTTP response was received
     * (connection refused, timeout...), in which case {@code error} says why.
     */
    public record Timing(int status, long bytes, long ttfbNanos, long totalNanos, String error) {

        static Timing failure(long totalNanos, String error) {
            return new Timing(-1, 0, totalNanos, totalNanos, error);
        }
    }
}
