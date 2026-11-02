package org.eclipse.edc.dse.dataplane.api;

import jakarta.ws.rs.core.HttpHeaders;
import org.eclipse.edc.connector.dataplane.http.spi.HttpDataAddress;
import org.eclipse.edc.connector.dataplane.spi.iam.DataPlaneAuthorizationService;
import org.eclipse.edc.edr.spi.store.EndpointDataReferenceStore;
import org.eclipse.edc.edr.spi.types.EndpointDataReferenceEntry;
import org.eclipse.edc.spi.entity.Entity;
import org.eclipse.edc.spi.query.Criterion;
import org.eclipse.edc.spi.query.QuerySpec;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.types.domain.DataAddress;

import java.util.Comparator;
import java.util.Map;

import static org.eclipse.edc.dse.dataplane.api.headers.ForwardedHeaders.CONSUMER_HOP;
import static org.eclipse.edc.dse.dataplane.api.headers.ForwardedHeaders.PROXY_HEADERS;
import static org.eclipse.edc.spi.constants.CoreConstants.EDC_NAMESPACE;
import static org.eclipse.edc.spi.result.Result.failure;

public class ConsumerDataPlaneAuthorizationService implements DataPlaneAuthorizationService {

    public static final String EDR_ENDPOINT = EDC_NAMESPACE + "endpoint";
    public static final String EDR_AUTH_KEY = HttpHeaders.AUTHORIZATION;
    public static final String EDR_AUTH_CODE = EDC_NAMESPACE + "authorization";
    public static final String CONTRACT_ID_HEADER = "Contract-Id";

    private final EndpointDataReferenceStore edrStore;

    public ConsumerDataPlaneAuthorizationService(EndpointDataReferenceStore edrStore) {
        this.edrStore = edrStore;
    }

    @Override
    public Result<DataAddress> authorize(String token, Map<String, Object> requestData) {
        var criterion = new Criterion(EndpointDataReferenceEntry.AGREEMENT_ID, "=", token);
        var querySpec = QuerySpec.Builder.newInstance()
                .filter(criterion)
                .build();

        var queryResult = edrStore.query(querySpec);
        if (queryResult.failed() || queryResult.getContent().isEmpty()) {
            return failure("No EDR satisfying criterion: %s".formatted(criterion.toString()));
        }

        var transferProcessId = queryResult.getContent().stream()
                .max(Comparator.comparingLong(Entity::getCreatedAt))
                .map(EndpointDataReferenceEntry::getTransferProcessId)
                .orElseThrow();

        return edrStore.resolveByTransferProcess(transferProcessId)
                .map(addr -> toHttpDataAddress(addr, token))
                .map(Result::success)
                .orElse(failure -> Result.failure(failure.getFailureDetail()));
    }

    private DataAddress toHttpDataAddress(DataAddress edr, String token) {
        return HttpDataAddress.Builder.newInstance()
                .baseUrl(edr.getStringProperty(EDR_ENDPOINT))
                .authKey(EDR_AUTH_KEY)
                .authCode(edr.getStringProperty(EDR_AUTH_CODE))
                .addAdditionalHeader(CONTRACT_ID_HEADER, token)
                .proxyPath(Boolean.TRUE.toString())
                .proxyQueryParams(Boolean.TRUE.toString())
                .proxyBody(Boolean.TRUE.toString())
                .proxyMethod(Boolean.TRUE.toString())
                // relay the allowed headers to the provider public API, which applies its own asset's proxyHeaders setting,
                // and the provider's answer back, errors only when the provider marked them as relayed from its source
                .property(EDC_NAMESPACE + PROXY_HEADERS, Boolean.TRUE.toString())
                .property(EDC_NAMESPACE + CONSUMER_HOP, Boolean.TRUE.toString())
                .build();
    }
}
