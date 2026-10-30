package org.eclipse.edc.test.system;

import io.restassured.specification.RequestSpecification;
import jakarta.json.JsonObject;
import jakarta.json.JsonValue;
import org.eclipse.edc.connector.controlplane.test.system.utils.Participant;
import org.eclipse.edc.connector.controlplane.test.system.utils.PolicyFixtures;
import org.eclipse.edc.junit.utils.LazySupplier;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import static io.restassured.RestAssured.given;
import static io.restassured.http.ContentType.JSON;
import static jakarta.json.Json.createObjectBuilder;
import static org.eclipse.dse.iam.policy.PolicyConstants.MEMBERSHIP_CONSTRAINT;
import static org.eclipse.edc.connector.controlplane.test.system.utils.PolicyFixtures.atomicConstraint;
import static org.eclipse.edc.jsonld.spi.JsonLdKeywords.CONTEXT;
import static org.eclipse.edc.jsonld.spi.JsonLdKeywords.ID;
import static org.eclipse.edc.jsonld.spi.JsonLdKeywords.TYPE;
import static org.eclipse.edc.jsonld.spi.JsonLdKeywords.VOCAB;
import static org.eclipse.edc.jsonld.spi.PropertyAndTypeNames.ODRL_ASSIGNER_ATTRIBUTE;
import static org.eclipse.edc.jsonld.spi.PropertyAndTypeNames.ODRL_POLICY_ATTRIBUTE;
import static org.eclipse.edc.jsonld.spi.PropertyAndTypeNames.ODRL_TARGET_ATTRIBUTE;
import static org.eclipse.edc.spi.constants.CoreConstants.EDC_NAMESPACE;
import static org.eclipse.edc.spi.core.CoreConstants.DSE_POLICY_NS;
import static org.eclipse.edc.spi.core.CoreConstants.DSE_POLICY_PREFIX;
import static org.eclipse.edc.test.system.ParticipantConstants.CLUSTER_HOSTNAME;
import static org.eclipse.edc.test.system.TestConstants.DEFAULT_TIMEOUT;

abstract class AbstractParticipant extends AbstractEntity {

    static final String API_KEY_HEADER = "x-api-key";

    @Override
    protected String identityHubIdentityUrl() {
        return "http://%s/%s/ih/identity".formatted(CLUSTER_HOSTNAME, name());
    }

    @Override
    protected String vaultUrl() {
        return "http://%s/%s/vault".formatted(CLUSTER_HOSTNAME, name());
    }

    /**
     * Like did()/identityHubIdentityUrl()/vaultUrl(): the in-cluster short hostname
     * ({@code <name>-controlplane}) only resolves within a single-namespace cluster. With one namespace per participant, a participant needing to reach
     * ANOTHER participant's control-plane DSP (contract negotiation, transfer) must go through
     * the external ingress, exactly like the other cross-participant endpoints.
     */
    protected String controlPlaneProtocolUrl() {
        return "http://%s/%s/cp/dsp".formatted(CLUSTER_HOSTNAME, name());
    }

    protected String controlPlaneManagementUrl() {
        return "http://%s/%s/cp/management".formatted(CLUSTER_HOSTNAME, name());
    }

    protected String dataPlaneDataUrl() {
        return "http://%s/%s/dp/data".formatted(CLUSTER_HOSTNAME, name());
    }

    protected String controlPlaneCatalogFilterUrl() {
        return "http://%s/%s/cp/management/catalog/participantCatalog".formatted(CLUSTER_HOSTNAME, name());
    }

    protected void createEntry(String assetId, String name, String description, Map<String, Object> dataAddressProps, JsonObject... additionalConstraints) {
        withRetry(PROVISIONING_MAX_ATTEMPTS, () -> {
            var client = participantClient();
            Map<String, Object> assetProps = Map.of(
                    "name", name,
                    "contenttype", "application/json",
                    "version", "1.0",
                    "description", description
            );
            createAssetOrSkip(assetId, assetProps, dataAddressProps);

            var constraints = new ArrayList<JsonObject>();
            constraints.add(atomicConstraint("%s:%s".formatted(DSE_POLICY_PREFIX, MEMBERSHIP_CONSTRAINT), "odrl:eq", "active"));
            constraints.addAll(Arrays.asList(additionalConstraints));

            var permissions = constraints.stream().map(this::createPermission).toList();
            var policy = createPolicyDefinition(PolicyFixtures.policy(permissions));
            client.createContractDefinition(assetId, UUID.randomUUID().toString(), policy, policy);
        });
    }

    private void createAssetOrSkip(String assetId, Map<String, Object> properties, Map<String, Object> dataAddressProperties) {
        var requestBody = createObjectBuilder()
                .add(CONTEXT, createObjectBuilder()
                        .add(VOCAB, EDC_NAMESPACE))
                .add(ID, assetId)
                .add("properties", createObjectBuilder(properties))
                .add("dataAddress", createObjectBuilder(dataAddressProperties))
                .build();
        postOrSkip(participantClient().baseManagementRequest().contentType(JSON).body(requestBody), "/v3/assets");
    }

    public String createPolicyDefinition(JsonObject policy) {
        var requestBody = createObjectBuilder()
                .add(CONTEXT, createObjectBuilder()
                        .add(VOCAB, EDC_NAMESPACE)
                        .add(DSE_POLICY_PREFIX, DSE_POLICY_NS))
                .add(TYPE, "PolicyDefinition")
                .add("policy", policy)
                .build();

        return participantClient().baseManagementRequest()
                .contentType(JSON)
                .body(requestBody)
                .when()
                .post("/v3/policydefinitions")
                .then()
                .log().ifError()
                .statusCode(200)
                .contentType(JSON)
                .extract().jsonPath().getString(ID);
    }

    protected void createSecret(String key, String value) {
        var requestBodyBuilder = createObjectBuilder()
                .add(CONTEXT, createObjectBuilder().add(VOCAB, EDC_NAMESPACE))
                .add(ID, key)
                .add(TYPE, "Secret")
                .add("value", value);

        var requestBody = requestBodyBuilder.build();

        withRetry(PROVISIONING_MAX_ATTEMPTS, () ->
                postOrSkip(participantClient().baseManagementRequest().contentType(JSON).body(requestBody), "/v3/secrets"));
    }

    public <T> T queryData(String contractId, Map<String, String> queryParams, int statusCode, Class<T> clazz) {
        return participantClient().baseDataRequest()
                .queryParams(queryParams)
                .headers(Map.of("Authorization", contractId, "Contract-Id", contractId))
                .when()
                .get()
                .then()
                .statusCode(statusCode)
                .extract()
                .body()
                .as(clazz);
    }

    public ParticipantClient participantClient() {
        return ParticipantClient.Builder.newInstance()
                .name(name())
                .id(did())
                .dataPlaneData(dataPlaneDataUrl())
                .controlPlaneManagement(controlPlaneManagementUrl())
                .controlPlaneProtocol(controlPlaneProtocolUrl())
                .timeout(DEFAULT_TIMEOUT)
                .build();
    }

    private JsonObject createPermission(JsonValue constraint) {
        return createObjectBuilder()
                .add("action", "use")
                .add("constraint", constraint)
                .build();
    }

    public void finishDataTransfer(String transferId) {
        participantClient().terminateTransfer(transferId);
    }

    public static class ParticipantClient extends Participant {

        protected LazySupplier<URI> dataPlaneData;
        protected String apiKey;

        public RequestSpecification baseDataRequest() {
            var request = given().baseUri(this.dataPlaneData.get().toString());
            return apiKey == null ? request : request.header(API_KEY_HEADER, apiKey);
        }

        public URI dataPlaneDataUri() {
            return dataPlaneData.get();
        }

        /**
         * Headers every direct call to this participant's APIs needs on top of the request's own
         * ones -- the API key, when one is configured (UAT), nothing otherwise.
         */
        public Map<String, String> authenticationHeaders() {
            return apiKey == null ? Map.of() : Map.of(API_KEY_HEADER, apiKey);
        }

        /**
         * Same offer as upstream's private getOfferForAsset(): the first policy of the provider's
         * dataset for this asset, with assigner and target filled in -- exposed so that the
         * catalog step can be timed on its own, separately from the negotiation.
         */
        public JsonObject getOfferForAsset(Participant provider, String assetId) {
            var dataset = getDatasetForAsset(provider, assetId);
            var policy = dataset.getJsonArray(ODRL_POLICY_ATTRIBUTE).get(0).asJsonObject();
            return createObjectBuilder(policy)
                    .add(ODRL_ASSIGNER_ATTRIBUTE, createObjectBuilder().add(ID, provider.getId()))
                    .add(ODRL_TARGET_ATTRIBUTE, createObjectBuilder().add(ID, dataset.get(ID)))
                    .build();
        }

        public String getContractAgreementId(String negotiationId) {
            return getContractNegotiationField(negotiationId, "contractAgreementId");
        }

        /**
         * The EDR the consumer control plane stored for a started transfer: the provider public
         * API endpoint and the token to call it with. Returns empty until the EDR exists.
         */
        public Optional<Edr> findEdr(String transferProcessId) {
            var response = baseManagementRequest()
                    .when()
                    .get("/v3/edrs/{id}/dataaddress", transferProcessId)
                    .then()
                    .extract();
            if (response.statusCode() != 200) {
                return Optional.empty();
            }
            Map<String, Object> body = response.jsonPath().getMap("$");
            var endpoint = edcProperty(body, "endpoint");
            var authorization = edcProperty(body, "authorization");
            return endpoint == null || authorization == null ? Optional.empty() : Optional.of(new Edr(endpoint, authorization));
        }

        private static String edcProperty(Map<String, Object> body, String name) {
            var value = body.containsKey(name) ? body.get(name) : body.get(EDC_NAMESPACE + name);
            return value == null ? null : value.toString();
        }

        public record Edr(String endpoint, String authorization) {
        }

        public static class Builder extends Participant.Builder<ParticipantClient, Builder> {

            protected Builder(ParticipantClient client) {
                super(client);
            }

            public Builder controlPlaneManagement(String controlPlaneManagement) {
                participant.controlPlaneManagement = new LazySupplier<>(() -> URI.create(controlPlaneManagement));
                return self();
            }

            public Builder controlPlaneProtocol(String controlPlaneProtocol) {
                participant.controlPlaneProtocol = new LazySupplier<>(() -> URI.create(controlPlaneProtocol));
                return self();
            }

            /**
             * Sends {@code x-api-key} on every management and data API request, for environments
             * (UAT) whose connector APIs are protected by one. Blank means no key.
             */
            public Builder apiKey(String apiKey) {
                if (apiKey != null && !apiKey.isBlank()) {
                    participant.apiKey = apiKey;
                    participant.enrichManagementRequest = request -> request.header(API_KEY_HEADER, apiKey);
                }
                return self();
            }

            public Builder dataPlaneData(String dataPlaneData) {
                participant.dataPlaneData = new LazySupplier<>(() -> URI.create(dataPlaneData));
                return self();
            }

            public ParticipantClient build() {
                super.build();
                Objects.requireNonNull(participant.dataPlaneData, "dataPlaneData");
                return participant;
            }

            protected Builder self() {
                return this;
            }

            public static ParticipantClient.Builder newInstance() {
                return new Builder(new ParticipantClient());
            }
        }
    }


}
