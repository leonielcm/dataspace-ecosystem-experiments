package org.eclipse.edc.dse.dataplane.api.headers;

import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;

import java.io.InputStream;
import java.util.Map;

/**
 * Response of the data source, relayed as is by the proxy: status, allowed headers and body.
 *
 * @param statusCode status answered by the data source, 2xx or not.
 * @param headers    allowed response headers, keyed by their allow-list spelling.
 */
public record ProxyHttpPart(String name, InputStream content, String mediaType, int statusCode,
                            Map<String, String> headers) implements DataSource.Part {

    @Override
    public InputStream openStream() {
        return content;
    }
}
