package org.eclipse.edc.dse.dataplane.api.headers;

import org.eclipse.edc.connector.dataplane.http.spi.HttpDataAddress;
import org.eclipse.edc.connector.dataplane.http.spi.HttpParamsDecorator;
import org.eclipse.edc.connector.dataplane.http.spi.HttpRequestParams;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;

import java.util.HashSet;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import static org.eclipse.edc.dse.dataplane.api.headers.ForwardedHeaders.PROPERTY_PREFIX;

/**
 * Applies the headers captured by the public API ({@link ForwardedHeaders#PROPERTY_PREFIX} properties of the
 * {@link DataFlowStartMessage}) to the request sent to the data source.
 * <p>
 * Only for addresses with {@code proxyHeaders=true}. Headers the provider set on the address itself
 * ({@code header:*} additional headers, {@code authKey}) always win over relayed ones.
 */
public class ForwardedHeadersParamsDecorator implements HttpParamsDecorator {

    private final ForwardedHeaders forwardedHeaders;

    public ForwardedHeadersParamsDecorator(ForwardedHeaders forwardedHeaders) {
        this.forwardedHeaders = forwardedHeaders;
    }

    @Override
    public HttpRequestParams.Builder decorate(DataFlowStartMessage request, HttpDataAddress address, HttpRequestParams.Builder params) {
        if (!ForwardedHeaders.isEnabled(address)) {
            return params;
        }

        var providerHeaders = providerHeaders(address);
        request.getProperties().forEach((key, value) -> {
            if (!key.startsWith(PROPERTY_PREFIX)) {
                return;
            }
            var name = key.substring(PROPERTY_PREFIX.length());
            if (forwardedHeaders.isAllowed(name) && !providerHeaders.contains(lowerCase(name))) {
                params.header(name, value);
            }
        });
        return params;
    }

    private static Set<String> providerHeaders(HttpDataAddress address) {
        var names = new HashSet<String>();
        address.getAdditionalHeaders().keySet().forEach(name -> names.add(lowerCase(name)));
        Optional.ofNullable(address.getAuthKey()).ifPresent(name -> names.add(lowerCase(name)));
        return names;
    }

    private static String lowerCase(String name) {
        return name.toLowerCase(Locale.ROOT);
    }
}
