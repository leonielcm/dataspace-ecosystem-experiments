package org.eclipse.edc.dse.dataplane.api.headers;

import org.eclipse.edc.connector.dataplane.http.params.HttpRequestFactory;
import org.eclipse.edc.connector.dataplane.http.spi.HttpRequestParamsProvider;
import org.eclipse.edc.connector.dataplane.spi.pipeline.PipelineService;
import org.eclipse.edc.http.spi.EdcHttpClient;
import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.runtime.metamodel.annotation.Provider;
import org.eclipse.edc.runtime.metamodel.annotation.Setting;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;

/**
 * Relays an allow-list of request headers, and the response status with an allow-list of response headers, through
 * the data plane proxy (public API and consumer data API), for data addresses that opt in with {@code proxyHeaders=true}.
 */
@Extension(value = ForwardedHeadersExtension.NAME)
public class ForwardedHeadersExtension implements ServiceExtension {

    public static final String NAME = "DSE Data Plane Forwarded Headers";

    @Setting(description = "Comma-separated, case-insensitive list of request headers the data plane proxy relays to the data source " +
            "when the data address sets proxyHeaders=true.",
            key = "edc.dataplane.proxy.forwarded.headers", defaultValue = ForwardedHeaders.DEFAULT_ALLOWED)
    private String allowedHeaders;

    @Setting(description = "Comma-separated, case-insensitive list of response headers of the data source the data plane proxy relays " +
            "to the client when the data address sets proxyHeaders=true.",
            key = "edc.dataplane.proxy.forwarded.response.headers", defaultValue = ForwardedHeaders.DEFAULT_ALLOWED_RESPONSE)
    private String allowedResponseHeaders;

    @Inject
    private HttpRequestParamsProvider paramsProvider;

    @Inject
    private PipelineService pipelineService;

    @Inject
    private EdcHttpClient httpClient;

    private ForwardedHeaders forwardedHeaders;

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void initialize(ServiceExtensionContext context) {
        forwardedHeaders = ForwardedHeaders.of(allowedHeaders);
        paramsProvider.registerSourceDecorator(new ForwardedHeadersParamsDecorator(forwardedHeaders));
        pipelineService.registerFactory(new ProxyHttpDataSourceFactory(httpClient, paramsProvider, new HttpRequestFactory(),
                ForwardedHeaders.of(allowedResponseHeaders), context.getMonitor()));
    }

    @Provider
    public ForwardedHeaders forwardedHeaders() {
        return forwardedHeaders;
    }
}
