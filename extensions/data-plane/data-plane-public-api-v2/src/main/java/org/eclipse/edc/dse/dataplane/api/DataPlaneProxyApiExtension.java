package org.eclipse.edc.dse.dataplane.api;

import org.eclipse.edc.connector.dataplane.spi.pipeline.PipelineService;
import org.eclipse.edc.dse.dataplane.api.controller.DataPlanePublicApiV2Controller;
import org.eclipse.edc.dse.dataplane.api.headers.ForwardedHeaders;
import org.eclipse.edc.edr.spi.store.EndpointDataReferenceStore;
import org.eclipse.edc.runtime.metamodel.annotation.Configuration;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.runtime.metamodel.annotation.Setting;
import org.eclipse.edc.runtime.metamodel.annotation.Settings;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.eclipse.edc.web.jersey.mapper.EdcApiExceptionMapper;
import org.eclipse.edc.web.spi.WebService;
import org.eclipse.edc.web.spi.configuration.PortMapping;
import org.eclipse.edc.web.spi.configuration.PortMappingRegistry;

import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.newFixedThreadPool;

public class DataPlaneProxyApiExtension implements ServiceExtension {

    public static final String NAME = "Data API";
    public static final int DEFAULT_THREAD_POOL = 5;
    static final int DEFAULT_ADMIN_PORT = 8282;
    static final String DEFAULT_ADMIN_PATH = "/api/data";
    private static final String API_CONTEXT = "data";

    @Setting(value = "Thread pool size for the consumer data plane data api", type = "int")
    private static final String THREAD_POOL_SIZE = "edc.dpf.dataplane.consumer.gateway.thread.pool";
    @Configuration
    private DataApiConfiguration apiConfiguration;
    @Inject
    private WebService webService;
    @Inject
    private PipelineService pipelineService;
    @Inject
    private EndpointDataReferenceStore edrStore;
    @Inject
    private PortMappingRegistry portMappingRegistry;
    @Inject
    private ForwardedHeaders forwardedHeaders;

    private ExecutorService executorService;

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void initialize(ServiceExtensionContext context) {
        var portMapping = new PortMapping(API_CONTEXT, apiConfiguration.port(), apiConfiguration.path());
        portMappingRegistry.register(portMapping);

        executorService = newFixedThreadPool(context.getSetting(THREAD_POOL_SIZE, DEFAULT_THREAD_POOL));

        var controller = new DataPlanePublicApiV2Controller(pipelineService, executorService, new ConsumerDataPlaneAuthorizationService(edrStore), forwardedHeaders, false);
        webService.registerResource(API_CONTEXT, controller);
        webService.registerResource(API_CONTEXT, new EdcApiExceptionMapper());

    }

    @Override
    public void shutdown() {
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    @Settings
    record DataApiConfiguration(
            @Setting(key = "web.http." + API_CONTEXT + ".port", description = "Port for " + API_CONTEXT + " api context", defaultValue = DEFAULT_ADMIN_PORT + "")
            int port,
            @Setting(key = "web.http." + API_CONTEXT + ".path", description = "Path for " + API_CONTEXT + " api context", defaultValue = DEFAULT_ADMIN_PATH)
            String path
    ) {

    }

}
