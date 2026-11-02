package org.eclipse.edc.dse.dataplane.api.headers;

import okhttp3.MediaType;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.eclipse.edc.connector.dataplane.http.params.HttpRequestFactory;
import org.eclipse.edc.connector.dataplane.http.spi.HttpRequestParams;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult;
import org.eclipse.edc.http.spi.EdcHttpClient;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.monitor.Monitor;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.eclipse.edc.connector.dataplane.http.spi.HttpDataAddress.OCTET_STREAM;
import static org.eclipse.edc.dse.dataplane.api.headers.ForwardedHeaders.RELAYED_RESPONSE_HEADER;

/**
 * HTTP data source of the proxy for addresses with {@code proxyHeaders=true}: unlike the EDC {@code HttpDataSource},
 * it hands the response status and the allowed response headers over to the public API, and relays non-2xx
 * answers instead of turning them into a failure.
 * <p>
 * On the consumer hop, a non-2xx answer of the provider is relayed only when the provider marked it with
 * {@link ForwardedHeaders#RELAYED_RESPONSE_HEADER}. Errors raised by the provider connector itself (rejected
 * token, source unreachable...) are reported exactly as the EDC {@code HttpDataSource} does.
 */
public class ProxyHttpDataSource implements DataSource {

    private static final int NOT_AUTHORIZED = 401;
    private static final int FORBIDDEN = 403;
    private static final int NOT_FOUND = 404;

    private final String name;
    private final HttpRequestParams params;
    private final boolean consumerHop;
    private final ForwardedHeaders responseHeaders;
    private final EdcHttpClient httpClient;
    private final HttpRequestFactory requestFactory;
    private final Monitor monitor;
    private final AtomicReference<Response> response = new AtomicReference<>();

    public ProxyHttpDataSource(String name, HttpRequestParams params, boolean consumerHop, ForwardedHeaders responseHeaders,
                               EdcHttpClient httpClient, HttpRequestFactory requestFactory, Monitor monitor) {
        this.name = name;
        this.params = params;
        this.consumerHop = consumerHop;
        this.responseHeaders = responseHeaders;
        this.httpClient = httpClient;
        this.requestFactory = requestFactory;
        this.monitor = monitor;
    }

    @Override
    public StreamResult<Stream<Part>> openPartStream() {
        var request = requestFactory.toRequest(params);
        monitor.debug(() -> "Executing proxied HTTP request: " + request.url());
        Response result;
        try {
            // NB: not closed here, the body is read after this method returns. close() releases it.
            result = httpClient.execute(request);
        } catch (IOException e) {
            throw new EdcException(e);
        }

        if (!result.isSuccessful() && consumerHop && result.header(RELAYED_RESPONSE_HEADER) == null) {
            try {
                return failure(result);
            } finally {
                result.close();
            }
        }

        response.set(result);
        var body = result.body();
        var content = Optional.ofNullable(body).map(ResponseBody::byteStream).orElseGet(InputStream::nullInputStream);
        var mediaType = Optional.ofNullable(body).map(ResponseBody::contentType).map(MediaType::toString).orElse(OCTET_STREAM);
        return StreamResult.success(Stream.of(new ProxyHttpPart(name, content, mediaType, result.code(), relayedHeaders(result))));
    }

    @Override
    public void close() {
        Optional.ofNullable(response.get()).ifPresent(Response::close);
    }

    private Map<String, String> relayedHeaders(Response result) {
        var headers = new LinkedHashMap<String, String>();
        result.headers().names().forEach(name -> headers.put(name, String.join(", ", result.headers(name))));
        return responseHeaders.select(headers);
    }

    /**
     * Same failures as the EDC {@code HttpDataSource}, so that unrelayed errors are reported as before.
     */
    private static <T> StreamResult<T> failure(Response result) {
        return switch (result.code()) {
            case NOT_AUTHORIZED, FORBIDDEN -> StreamResult.notAuthorized();
            case NOT_FOUND -> StreamResult.notFound();
            default -> StreamResult.error(format("Received code transferring HTTP data: %s - %s.", result.code(), result.message()));
        };
    }
}
