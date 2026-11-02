package org.eclipse.edc.dse.dataplane.api.headers;

import org.eclipse.edc.connector.dataplane.http.params.HttpRequestFactory;
import org.eclipse.edc.connector.dataplane.http.spi.HttpDataAddress;
import org.eclipse.edc.connector.dataplane.http.spi.HttpRequestParamsProvider;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSourceFactory;
import org.eclipse.edc.http.spi.EdcHttpClient;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;
import org.jetbrains.annotations.NotNull;

import static org.eclipse.edc.dse.dataplane.api.headers.ForwardedHeaders.CONSUMER_HOP;

/**
 * Creates {@link ProxyHttpDataSource}s. The public API gives an {@code HttpData} address with {@code proxyHeaders=true}
 * the {@link #TYPE} type, so the regular {@code HttpData} source factory keeps serving every other address.
 */
public class ProxyHttpDataSourceFactory implements DataSourceFactory {

    public static final String TYPE = "DseProxyHttpData";

    private final EdcHttpClient httpClient;
    private final HttpRequestParamsProvider paramsProvider;
    private final HttpRequestFactory requestFactory;
    private final ForwardedHeaders responseHeaders;
    private final Monitor monitor;

    public ProxyHttpDataSourceFactory(EdcHttpClient httpClient, HttpRequestParamsProvider paramsProvider,
                                      HttpRequestFactory requestFactory, ForwardedHeaders responseHeaders, Monitor monitor) {
        this.httpClient = httpClient;
        this.paramsProvider = paramsProvider;
        this.requestFactory = requestFactory;
        this.responseHeaders = responseHeaders;
        this.monitor = monitor;
    }

    @Override
    public String supportedType() {
        return TYPE;
    }

    @Override
    public DataSource createSource(DataFlowStartMessage request) {
        var address = HttpDataAddress.Builder.newInstance().copyFrom(request.getSourceDataAddress()).build();
        var consumerHop = Boolean.parseBoolean(String.valueOf(address.getProperty(CONSUMER_HOP)));
        return new ProxyHttpDataSource(address.getName(), paramsProvider.provideSourceParams(request), consumerHop,
                responseHeaders, httpClient, requestFactory, monitor);
    }

    @Override
    public @NotNull Result<Void> validateRequest(DataFlowStartMessage request) {
        try {
            createSource(request);
        } catch (Exception e) {
            return Result.failure("Failed to build ProxyHttpDataSource: " + e.getMessage());
        }
        return Result.success();
    }
}
