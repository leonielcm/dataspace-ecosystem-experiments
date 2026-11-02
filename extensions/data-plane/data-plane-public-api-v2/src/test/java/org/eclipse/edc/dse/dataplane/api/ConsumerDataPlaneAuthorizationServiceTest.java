package org.eclipse.edc.dse.dataplane.api;

import org.eclipse.edc.connector.dataplane.http.spi.HttpDataAddress;
import org.eclipse.edc.edr.spi.store.EndpointDataReferenceStore;
import org.eclipse.edc.edr.spi.types.EndpointDataReferenceEntry;
import org.eclipse.edc.spi.query.QuerySpec;
import org.eclipse.edc.spi.result.StoreResult;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.edc.dse.dataplane.api.ConsumerDataPlaneAuthorizationService.EDR_AUTH_CODE;
import static org.eclipse.edc.dse.dataplane.api.ConsumerDataPlaneAuthorizationService.EDR_AUTH_KEY;
import static org.eclipse.edc.dse.dataplane.api.ConsumerDataPlaneAuthorizationService.EDR_ENDPOINT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ConsumerDataPlaneAuthorizationServiceTest {
    private final EndpointDataReferenceStore edrStore = mock();
    private final org.eclipse.edc.dse.dataplane.api.ConsumerDataPlaneAuthorizationService service = new ConsumerDataPlaneAuthorizationService(edrStore);

    @Test
    void authorize_success() {
        var contractId = UUID.randomUUID().toString();
        var edr = edrDataAddress();
        var entries = List.of(
                createEndpointDataReferenceEntry("tp1", Instant.now()),
                createEndpointDataReferenceEntry("tp2", Instant.now().plusSeconds(10)),
                createEndpointDataReferenceEntry("tp3", Instant.now().plusSeconds(20))
        );
        when(edrStore.query(argThat(querySpec -> {
            var optional = querySpec.getFilterExpression().stream().findFirst();
            if (optional.isEmpty()) {
                return false;
            }
            var criterion = optional.get();
            return criterion.getOperandLeft().equals(EndpointDataReferenceEntry.AGREEMENT_ID) &&
                    criterion.getOperator().equals("=") &&
                    criterion.getOperandRight().equals(contractId);
        }))).thenReturn(StoreResult.success(entries));
        when(edrStore.resolveByTransferProcess("tp3")).thenReturn(StoreResult.success(edr));

        var result = service.authorize(contractId, null);

        assertThat(result.succeeded()).isTrue();
        assertThat(result.getContent()).satisfies(address -> assertThat(address).isInstanceOfSatisfying(HttpDataAddress.class, httpDataAddress -> {
            assertThat(httpDataAddress.getBaseUrl()).isEqualTo("http://example.com");
            assertThat(httpDataAddress.getAuthKey()).isEqualTo(EDR_AUTH_KEY);
            assertThat(httpDataAddress.getAuthCode()).isEqualTo("token");
            assertThat(httpDataAddress.getStringProperty("proxyHeaders")).isEqualTo("true");
            assertThat(httpDataAddress.getStringProperty("proxyHeadersConsumerHop")).isEqualTo("true");
        }));


    }

    @Test
    void resolve_unauthorized_if_noContractIdNotFound() {
        when(edrStore.query(any(QuerySpec.class))).thenReturn(StoreResult.notFound("error"));

        var result = service.authorize("unknown", null);

        assertThat(result.failed()).isTrue();
    }

    private static EndpointDataReferenceEntry createEndpointDataReferenceEntry(String transferProcessId, Instant createdAt) {
        return EndpointDataReferenceEntry.Builder.newInstance()
                .id(UUID.randomUUID().toString())
                .assetId("assetId")
                .agreementId("agreementId")
                .contractNegotiationId("contractNegotiationId")
                .transferProcessId(transferProcessId)
                .providerId("providerId")
                .createdAt(createdAt.getEpochSecond())
                .build();
    }

    private static DataAddress edrDataAddress() {
        return DataAddress.Builder.newInstance()
                .type("type")
                .property(EDR_ENDPOINT, "http://example.com")
                .property(EDR_AUTH_KEY, "Authorization")
                .property(EDR_AUTH_CODE, "token")
                .build();
    }
}