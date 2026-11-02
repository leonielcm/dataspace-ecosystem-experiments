package org.eclipse.edc.dse.dataplane.api.controller;

import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSink;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult;
import org.eclipse.edc.dse.dataplane.api.headers.ProxyHttpPart;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.response.StatusResult;
import org.eclipse.edc.spi.result.AbstractResult;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.eclipse.edc.spi.response.ResponseStatus.FATAL_ERROR;
import static org.eclipse.edc.util.async.AsyncUtils.asyncAllOf;

/**
 * Streams the data source to the client, like the EDC {@code AsyncStreamingDataSink}, and also hands over the status
 * and headers of a {@link ProxyHttpPart}. Any other part is answered with 200, as before.
 */
public class RelayingStreamingDataSink implements DataSink {

    private static final int OK = 200;

    private final ResponseContext responseContext;
    private final ExecutorService executorService;

    public RelayingStreamingDataSink(ResponseContext responseContext, ExecutorService executorService) {
        this.responseContext = responseContext;
        this.executorService = executorService;
    }

    @Override
    public CompletableFuture<StreamResult<Object>> transfer(DataSource source) {
        var streamResult = source.openPartStream();
        if (streamResult.failed()) {
            return completedFuture(StreamResult.failure(streamResult.getFailure()));
        }

        try (var partStream = streamResult.getContent()) {
            return partStream
                    .map(part -> supplyAsync(() -> transferPart(part), executorService))
                    .collect(asyncAllOf())
                    .thenApply(this::processResults);
        }
    }

    private StreamResult<Object> processResults(List<? extends StatusResult<?>> results) {
        if (results.stream().anyMatch(AbstractResult::failed)) {
            return StreamResult.error("Error transferring data");
        }
        return StreamResult.success();
    }

    private StatusResult<?> transferPart(DataSource.Part part) {
        Consumer<OutputStream> writer = outputStream -> {
            try {
                part.openStream().transferTo(outputStream);
            } catch (IOException e) {
                throw new EdcException(e);
            }
        };

        var registered = part instanceof ProxyHttpPart relayed
                ? responseContext.register(new RelayedResponse(relayed.statusCode(), relayed.headers(), relayed.mediaType(), true, writer))
                : responseContext.register(new RelayedResponse(OK, Map.of(), part.mediaType(), false, writer));

        return registered ? StatusResult.success() : StatusResult.failure(FATAL_ERROR, "Could not resume output stream write");
    }

    /**
     * Answers the client once the data source responded.
     */
    @FunctionalInterface
    public interface ResponseContext {

        /**
         * Resumes the client response.
         *
         * @return true if the response could be resumed.
         */
        boolean register(RelayedResponse response);
    }

    /**
     * Response to give the client.
     *
     * @param relayed whether status and headers come from the data source rather than being the default 200.
     */
    public record RelayedResponse(int statusCode, Map<String, String> headers, String mediaType, boolean relayed,
                                  Consumer<OutputStream> writer) {
    }
}
