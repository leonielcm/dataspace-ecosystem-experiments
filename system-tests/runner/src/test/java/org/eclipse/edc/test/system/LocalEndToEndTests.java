package org.eclipse.edc.test.system;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import io.restassured.http.ContentType;
import io.restassured.response.Response;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.edc.connector.controlplane.contract.spi.types.negotiation.ContractNegotiationStates;
import org.eclipse.edc.connector.dataplane.spi.response.TransferErrorResponse;
import org.eclipse.edc.identityhub.spi.verifiablecredentials.model.VcStatus;
import org.eclipse.edc.junit.annotations.EndToEndTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.Key;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.restassured.RestAssured.given;
import static io.restassured.http.ContentType.JSON;
import static jakarta.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.eclipse.dse.iam.policy.PolicyConstants.DOMAIN_CREDENTIAL_TYPE;
import static org.eclipse.dse.iam.policy.PolicyConstants.GENERIC_CLAIM_CONSTRAINT;
import static org.eclipse.dse.iam.policy.PolicyConstants.MEMBERSHIP_CREDENTIAL_TYPE;
import static org.eclipse.dse.iam.policy.PolicyConstants.RESTRICTED_CATALOG_DISCOVERY_CONSTRAINT;
import static org.eclipse.edc.connector.controlplane.test.system.utils.PolicyFixtures.atomicConstraint;
import static org.eclipse.edc.connector.controlplane.transfer.spi.types.TransferProcessStates.TERMINATED;
import static org.eclipse.edc.jsonld.spi.JsonLdKeywords.ID;
import static org.eclipse.edc.spi.constants.CoreConstants.EDC_NAMESPACE;
import static org.eclipse.edc.spi.core.CoreConstants.DSE_POLICY_PREFIX;
import static org.eclipse.edc.spi.core.CoreConstants.DSE_POLICY_PROFILE_NS;
import static org.eclipse.edc.test.system.AbstractAuthority.DOMAIN_ROUTE;
import static org.eclipse.edc.test.system.AbstractAuthority.DOMAIN_TRAVEL;
import static org.eclipse.edc.test.system.LocalConsumer.ASSET_ID_CONSUMER_REST_API;
import static org.eclipse.edc.test.system.LocalProvider.ASSET_ID_FAILURE_REST_API;
import static org.eclipse.edc.test.system.LocalProvider.ASSET_ID_KAFKA_STREAM;
import static org.eclipse.edc.test.system.LocalProvider.ASSET_ID_REST_20_SEC_API;
import static org.eclipse.edc.test.system.LocalProvider.ASSET_ID_REST_API;
import static org.eclipse.edc.test.system.LocalProvider.ASSET_ID_REST_API_DOMAIN;
import static org.eclipse.edc.test.system.LocalProvider.ASSET_ID_REST_API_EMBEDDED_QUERY_PARAMS;
import static org.eclipse.edc.test.system.LocalProvider.ASSET_ID_REST_API_EXPIRED;
import static org.eclipse.edc.test.system.LocalProvider.ASSET_ID_REST_API_GENERIC_EQ_FULL_MEMBER;
import static org.eclipse.edc.test.system.LocalProvider.ASSET_ID_REST_API_GENERIC_EQ_STRICT;
import static org.eclipse.edc.test.system.LocalProvider.ASSET_ID_REST_API_GENERIC_IN;
import static org.eclipse.edc.test.system.LocalProvider.ASSET_ID_REST_API_GENERIC_NONE_OF;
import static org.eclipse.edc.test.system.LocalProvider.ASSET_ID_REST_API_GENERIC_NONE_OF_EXCLUDED;
import static org.eclipse.edc.test.system.LocalProvider.ASSET_ID_REST_API_OAUTH2;
import static org.eclipse.edc.test.system.LocalProvider.ASSET_ID_REST_API_PROFILE_NS_NONE_OF;
import static org.eclipse.edc.test.system.LocalProvider.ASSET_ID_REST_API_PROFILE_NS_NONE_OF_EXCLUDED;
import static org.eclipse.edc.test.system.LocalProvider.ASSET_ID_REST_API_ROUTE_DOMAIN_RESTRICTED;
import static org.eclipse.edc.test.system.LocalProvider.ASSET_ID_REST_API_TRAVEL_DOMAIN_RESTRICTED;
import static org.eclipse.edc.test.system.LocalProvider.EMBEDDED_QUERY_PARAM;
import static org.eclipse.edc.test.system.LocalProvider.OAUTH2_CLIENT_SECRET;
import static org.eclipse.edc.test.system.LocalProvider.OAUTH2_CLIENT_SECRET_KEY;
import static org.eclipse.edc.test.system.LocalProvider.POLICY_RESTRICTED_API;
import static org.eclipse.edc.test.system.ParticipantConstants.printConfiguration;
import static org.eclipse.edc.test.system.PostgresDataVerifier.verifyData;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@EndToEndTest
public class LocalEndToEndTests extends AbstractEndToEndTests {

    public static final String KAFKA_BROKER_PULL = "KafkaBroker-PULL";
    public static final String SECRET = "a-string-secret-at-least-256-bits-long";
    public static String dummyJwt;
    public static String dummyJwtWithMultipleRoles;
    public static String dummyJwtNonExistentParticipant;
    public static String dummyJwtExpired;
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TELEMETRY_TOPIC = "telemetry";
    private static final String VAULT_TOKEN = "root";
    public static final String REPORT_HEADER_WITHOUT_COUNTERPARTY_INFO = "contract_id,counterparty_name,data_transfer_response_status," +
            "total_transfer_size_in_kB,total_number_of_events";

    private static final LocalAuthority AUTHORITY = new LocalAuthority();
    private static final LocalProvider PROVIDER = new LocalProvider();
    private static final LocalConsumer CONSUMER = new LocalConsumer();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final List<String> USED_CONTRACT_ID = new ArrayList<>();
    private static KafkaConsumer<String, String> kafkaConsumer;

    private static String kafkacatPod = null;

    private static final int CREDENTIAL_REQUEST_MAX_ATTEMPTS = 5;

    private static final int VC_STATUS_ISSUED = VcStatus.ISSUED.code();

    public static void initializeParticipant(AbstractEntity participant) {
        AUTHORITY.createParticipant(participant.name(), participant.did());
        requestAndWaitForCredential(participant, MEMBERSHIP_CREDENTIAL_TYPE, "membership-credential-def-1");
        requestAndWaitForCredential(participant, DOMAIN_CREDENTIAL_TYPE, "domain-credential-def-1");
    }

    private static boolean hasIssuedCredential(AbstractEntity participant, String credentialType) {
        try {
            return participant.getCredentials(OBJECT_MAPPER, credentialType).stream()
                    .anyMatch(vc -> vc.getInt("state") == VC_STATUS_ISSUED);
        } catch (Exception | AssertionError e) {
            return false;
        }
    }

    private static void requestAndWaitForCredential(AbstractEntity participant, String credentialType, String credentialDefinitionId) {
        if (hasIssuedCredential(participant, credentialType)) {
            return;
        }
        participant.withRetry(CREDENTIAL_REQUEST_MAX_ATTEMPTS, () -> {
            participant.requestCredential(AUTHORITY.did(), credentialType, credentialDefinitionId);
            waitForCredentialType(participant, credentialType);
        });
    }

    @BeforeAll
    static void beforeAll() {
        printConfiguration();
        resetAllParticipants();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "local-e2e-test-group-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(List.of(TELEMETRY_TOPIC));

        AUTHORITY.defineMembershipCredential();
        AUTHORITY.defineDomainCredential();

        List.of(PROVIDER, CONSUMER, AUTHORITY).forEach(LocalEndToEndTests::initializeParticipant);
        seedData();

        Key key = Keys.hmacShaKeyFor(SECRET.getBytes());
        dummyJwt = Jwts.builder().header().add("alg", "HS256").add("typ", "JWT").and()
                .claims(Map.of("roles", List.of("Participant.consumer")))
                .signWith(key).compact();

        dummyJwtWithMultipleRoles = Jwts.builder().header().add("alg", "HS256").add("typ", "JWT").and()
                .claims(Map.of("roles", List.of("Read.consumer", "Participant.consumer")))
                .signWith(key).compact();

        dummyJwtNonExistentParticipant = Jwts.builder().header().add("alg", "HS256").add("typ", "JWT").and()
                .claims(Map.of("roles", List.of("Participant.doesnotexist")))
                .signWith(key).compact();

        dummyJwtExpired = Jwts.builder().header().add("alg", "HS256").add("typ", "JWT").and()
                .claims(Map.of("roles", List.of("Participant.consumer")))
                .expiration(Date.from(Instant.parse("2020-01-01T00:00:00Z")))
                .signWith(key).compact();
    }

    @AfterAll
    public static void afterAll() {
        if (kafkaConsumer != null) {
            long startTime = System.currentTimeMillis();
            while (!USED_CONTRACT_ID.isEmpty() && (System.currentTimeMillis() - startTime < 60000)) {
                var records = kafkaConsumer.poll(Duration.ofMillis(1000));
                records.forEach(record -> {
                    try {
                        JsonNode root = OBJECT_MAPPER.readTree(record.value());
                        String contractId = root.path("properties").path("contractId").asText();
                        String timestamp = root.path("createdAt").asText();
                        
                        ObjectNode telemetryEventNode = (ObjectNode) root.path("properties");
                        if (telemetryEventNode != null) {
                            telemetryEventNode.put("timestamp", timestamp);
                            String updatedTelemetryEvent = OBJECT_MAPPER.writeValueAsString(telemetryEventNode);
                            
                            given()
                                    .baseUri("%s".formatted(AUTHORITY.telemetryUrl()))
                                    .contentType(JSON)
                                    .body(updatedTelemetryEvent)
                                    .post()
                                    .then()
                                    .log().ifError()
                                    .statusCode(201);
                                    
                            if (USED_CONTRACT_ID.contains(contractId)) {
                                USED_CONTRACT_ID.remove(contractId);
                                if (!verifyData(contractId)) {
                                    System.out.println("Data verification failed for contract ID: " + contractId);
                                }
                            }
                        }
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                });
            }
            if (!USED_CONTRACT_ID.isEmpty()) {
                System.err.println("Timeout: USED_CONTRACT_ID is not empty after 1 minute. Remaining: " + USED_CONTRACT_ID);
            }
            kafkaConsumer.close();
        }

        // Leaves the DB the way the next run's beforeAll() expects to find it — see
        // resetAllParticipants()'s javadoc. Comes after the Kafka/telemetry drain above: that logic
        // needs the transfer/negotiation rows this truncates still present while it runs.
        for (String participant : ASSET_OWNING_PARTICIPANTS) {
            DatabaseCleaner.truncateTables(participant, EDC_TABLES);
        }
        DatabaseCleaner.truncateTables("authority", AUTHORITY_ISSUER_TABLES);
        DatabaseCleaner.truncateTables("authority", AUTHORITY_TELEMETRY_TABLES);
        for (String participant : PARTICIPANT_NAMES) {
            // purgeStaleEntries() covers dead credential-request rows too, not just leases/JTI: a
            // run that hit requestAndWaitForCredential's retry path leaves its own dead-end rows
            // (state 100/500) behind — resetAllParticipants() already purges these in beforeAll,
            // afterAll should leave the DB in the same clean state rather than deferring it.
            DatabaseCleaner.purgeStaleEntries(participant);
        }
    }

    private static void seedData() {
        // basic api
        PROVIDER.createEntry(ASSET_ID_REST_API, "Test Asset REST", "a basic REST API", Map.of(
                "type", "HttpData",
                "baseUrl", "http://provider-backend:8080/api/provider/data",
                "proxyQueryParams", Boolean.TRUE.toString()
        ), genericClaimConstraint(MEMBERSHIP_CREDENTIAL_TYPE, "name", "odrl:eq", "consumer"));

        // basic api
        PROVIDER.createEntry(ASSET_ID_REST_API_DOMAIN, "Test Asset REST", "a basic REST API", Map.of(
                "type", "HttpData",
                "baseUrl", "http://provider-backend:8080/api/provider/data",
                "proxyQueryParams", Boolean.TRUE.toString()
        ), genericClaimConstraint(DOMAIN_CREDENTIAL_TYPE, "domain", "odrl:eq", DOMAIN_ROUTE));

        // api restricted to consumers holding a DomainCredential whose domain is in ['route','travel']
        PROVIDER.createEntry(ASSET_ID_REST_API_GENERIC_IN, "Test Asset REST - generic claim IN", "a basic REST API restricted by a GenericClaim IN constraint", Map.of(
                "type", "HttpData",
                "baseUrl", "http://provider-backend:8080/api/provider/data",
                "proxyQueryParams", Boolean.TRUE.toString()
        ), genericClaimConstraint(DOMAIN_CREDENTIAL_TYPE, "domain", "odrl:isPartOf", "['route','travel']"));

        // api closed to consumers holding a DomainCredential whose domain is in the deny-list ['travel']
        PROVIDER.createEntry(ASSET_ID_REST_API_GENERIC_NONE_OF, "Test Asset REST - generic claim isNoneOf", "a basic REST API restricted by a GenericClaim isNoneOf constraint", Map.of(
                "type", "HttpData",
                "baseUrl", "http://provider-backend:8080/api/provider/data",
                "proxyQueryParams", Boolean.TRUE.toString()
        ), genericClaimConstraint(DOMAIN_CREDENTIAL_TYPE, "domain", "odrl:isNoneOf", "['travel']"));

        // control for the asset above: the deny-list contains the domain every consumer holds ('route')
        PROVIDER.createEntry(ASSET_ID_REST_API_GENERIC_NONE_OF_EXCLUDED, "Test Asset REST - generic claim isNoneOf excluded", "a basic REST API whose GenericClaim isNoneOf deny-list contains the consumer's domain", Map.of(
                "type", "HttpData",
                "baseUrl", "http://provider-backend:8080/api/provider/data",
                "proxyQueryParams", Boolean.TRUE.toString()
        ), genericClaimConstraint(DOMAIN_CREDENTIAL_TYPE, "domain", "odrl:isNoneOf", "['route','travel']"));

        // #799: the same isNoneOf pair, with the left operand written as a full IRI in the stable ODRL profile namespace
        // (the policy-definition @context still maps eox-policy to the legacy namespace, see AbstractParticipant)
        PROVIDER.createEntry(ASSET_ID_REST_API_PROFILE_NS_NONE_OF, "Test Asset REST - stable profile namespace isNoneOf", "a basic REST API restricted by a stable-namespace GenericClaim isNoneOf constraint", Map.of(
                "type", "HttpData",
                "baseUrl", "http://provider-backend:8080/api/provider/data",
                "proxyQueryParams", Boolean.TRUE.toString()
        ), profileGenericClaimConstraint(DOMAIN_CREDENTIAL_TYPE, "domain", "odrl:isNoneOf", "['travel']"));
        PROVIDER.createEntry(ASSET_ID_REST_API_PROFILE_NS_NONE_OF_EXCLUDED, "Test Asset REST - stable profile namespace isNoneOf excluded", "stable-namespace GenericClaim isNoneOf deny-list containing the consumer domain", Map.of(
                "type", "HttpData",
                "baseUrl", "http://provider-backend:8080/api/provider/data",
                "proxyQueryParams", Boolean.TRUE.toString()
        ), profileGenericClaimConstraint(DOMAIN_CREDENTIAL_TYPE, "domain", "odrl:isNoneOf", "['route','travel']"));

        // api restricted to consumers whose MembershipCredential membership.membershipType equals 'Full';
        // every seeded participant actually holds 'FullMember' (AbstractAuthority.createParticipantMembershipAttestation()),
        // so this constraint is never satisfied and the negotiation must terminate.
        PROVIDER.createEntry(ASSET_ID_REST_API_GENERIC_EQ_STRICT, "Test Asset REST - generic claim EQ strict", "a basic REST API restricted by a GenericClaim EQ constraint on membershipType", Map.of(
                "type", "HttpData",
                "baseUrl", "http://provider-backend:8080/api/provider/data",
                "proxyQueryParams", Boolean.TRUE.toString()
        ), genericClaimConstraint(MEMBERSHIP_CREDENTIAL_TYPE, "membership.membershipType", "odrl:eq", "Full"));

        // control for the strict asset above: same constraint path, but on the value every seeded participant holds
        PROVIDER.createEntry(ASSET_ID_REST_API_GENERIC_EQ_FULL_MEMBER, "Test Asset REST - generic claim EQ full member", "a basic REST API restricted by a GenericClaim EQ constraint on membershipType FullMember", Map.of(
                "type", "HttpData",
                "baseUrl", "http://provider-backend:8080/api/provider/data",
                "proxyQueryParams", Boolean.TRUE.toString()
        ), genericClaimConstraint(MEMBERSHIP_CREDENTIAL_TYPE, "membership.membershipType", "odrl:eq", "FullMember"));

        // api returning not authorized
        PROVIDER.createEntry(ASSET_ID_FAILURE_REST_API, "Failure REST API", "a REST API returning not authorized", Map.of(
                "type", "HttpData",
                "baseUrl", "http://provider-backend:8080/api/provider/failure"
        ));

        // api with embedded query params
        PROVIDER.createEntry(ASSET_ID_REST_API_EMBEDDED_QUERY_PARAMS, "Test Asset REST embedded query params", "a REST API with query params in the address", Map.of(
                "type", "HttpData",
                "baseUrl", "http://provider-backend:8080/api/provider/data",
                "queryParams", "message=%s".formatted(EMBEDDED_QUERY_PARAM),
                "proxyQueryParams", Boolean.TRUE.toString()
        ));

        // oauth2 api
        PROVIDER.createSecret(OAUTH2_CLIENT_SECRET_KEY, OAUTH2_CLIENT_SECRET);
        PROVIDER.createEntry(ASSET_ID_REST_API_OAUTH2, "Test Asset REST Oauth2", "a REST API with oauth2 authorization", Map.of(
                "type", "HttpData",
                "baseUrl", "http://provider-backend:8080/api/provider/oauth2data",
                "proxyQueryParams", Boolean.TRUE.toString(),
                "oauth2:clientId", "clientId",
                "oauth2:clientSecretKey", OAUTH2_CLIENT_SECRET_KEY,
                "oauth2:tokenUrl", "http://provider-backend:8080/api/oauth2/token"
        ));

        // asset with policy on claim that is not satisfied by any member
        PROVIDER.createEntry(POLICY_RESTRICTED_API, "Restricted API", "An API with a restricted policy", Map.of(
                "type", "HttpData",
                "baseUrl", "http://example.com"
        ), genericClaimConstraint(MEMBERSHIP_CREDENTIAL_TYPE, "name", "odrl:eq", "unknown"));

        // basic api
        PROVIDER.createEntry(ASSET_ID_REST_20_SEC_API, "Test Asset REST - 10sec", "a basic REST API available for 10 seconds", Map.of(
                "type", "HttpData",
                "baseUrl", "http://provider-backend:8080/api/provider/data",
                "proxyQueryParams", Boolean.TRUE.toString()
        ), atomicConstraint("inForceDate", "lteq", "contractAgreement+20s"));

        // basic api with an already-expired inForceDate policy — negotiation must terminate immediately
        PROVIDER.createEntry(ASSET_ID_REST_API_EXPIRED, "Test Asset REST - expired", "a basic REST API with an expired inForceDate policy", Map.of(
                "type", "HttpData",
                "baseUrl", "http://provider-backend:8080/api/provider/data",
                "proxyQueryParams", Boolean.TRUE.toString()
        ), atomicConstraint("inForceDate", "lteq", "2020-01-01T00:00:00Z"));

        // Kafka properties
        JsonObject jsonObject = Json.createObjectBuilder()
                .add("topic", "my-topic")
                .add("kafka.bootstrap.servers", "proxy-provider-oauth2:30003")
                .add("security.protocol", "SASL_SSL")
                .add("sasl.mechanism", "OAUTHBEARER")
                .add("tls_ca_secret", "proxy-provider-tls-ca")
                .add("sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
                        "clientId='<your-client-id>' clientSecret='<your-client-secret>' tenantId='<your-tenant-id>' scope='<your-client-id>/.default'").build();
        PROVIDER.createEntry(ASSET_ID_KAFKA_STREAM, "Test Asset Kafka", "a basic kafka stream", Map.of(
                "type", "Kafka",
                "properties", jsonObject
        ), genericClaimConstraint(MEMBERSHIP_CREDENTIAL_TYPE, "name", "odrl:eq", "consumer"));

        JsonObject jsonObject2 = Json.createObjectBuilder()
                .add("topic", "tst-topic")
                .add("kafka.bootstrap.servers", "proxy-provider:30001")
                .add("security.protocol", "SASL_SSL")
                .add("sasl.mechanism", "PLAIN")
                .add("tls_ca_secret", "proxy-provider-tls-ca")
                .add("sasl.jaas.config",
                        "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                                "username='provider' password='secret1'")
                .build();
        PROVIDER.createEntry(ASSET_ID_KAFKA_STREAM + "-tst-2", "Test Asset Kafka", "a basic kafka stream", Map.of(
                "type", "Kafka",
                "properties", jsonObject2
        ), genericClaimConstraint(MEMBERSHIP_CREDENTIAL_TYPE, "name", "odrl:eq", "consumer"));

        PROVIDER.createEntry(ASSET_ID_REST_API_ROUTE_DOMAIN_RESTRICTED, "Test Asset REST", "a basic REST API", Map.of(
                "type", "HttpData",
                "baseUrl", "http://provider-backend:8080/api/provider/data",
                "proxyQueryParams", Boolean.TRUE.toString()
        ), restrictedDiscoveryClaimConstraint(DOMAIN_CREDENTIAL_TYPE, "domain", "odrl:eq", DOMAIN_ROUTE));

        PROVIDER.createEntry(ASSET_ID_REST_API_TRAVEL_DOMAIN_RESTRICTED, "Test Asset REST", "a basic REST API", Map.of(
                "type", "HttpData",
                "baseUrl", "http://provider-backend:8080/api/provider/data",
                "proxyQueryParams", Boolean.TRUE.toString()
        ), restrictedDiscoveryClaimConstraint(DOMAIN_CREDENTIAL_TYPE, "domain", "odrl:eq", DOMAIN_TRAVEL));

        // asset published by the consumer itself, so its own catalog is no longer empty
        CONSUMER.createEntry(ASSET_ID_CONSUMER_REST_API, "Consumer REST API", "a basic REST API published by the consumer", Map.of(
                "type", "HttpData",
                "baseUrl", "http://provider-backend:8080/api/provider/data",
                "proxyQueryParams", Boolean.TRUE.toString()
        ));
    }

    private static JsonObject profileGenericClaimConstraint(String credentialType, String path, String operator, String rightOperand) {
        return atomicConstraint("%s%s.$.%s.%s".formatted(DSE_POLICY_PROFILE_NS, GENERIC_CLAIM_CONSTRAINT, credentialType, path), operator, rightOperand);
    }

    private static JsonObject genericClaimConstraint(String credentialType, String path, String operator, String rightOperand) {
        return atomicConstraint("%s:%s.$.%s.%s".formatted(DSE_POLICY_PREFIX, GENERIC_CLAIM_CONSTRAINT, credentialType, path), operator, rightOperand);
    }

    private static JsonObject restrictedDiscoveryClaimConstraint(String credentialType, String path, String operator, String rightOperand) {
        return atomicConstraint("%s:%s.$.%s.%s".formatted(DSE_POLICY_PREFIX, RESTRICTED_CATALOG_DISCOVERY_CONSTRAINT, credentialType, path), operator, rightOperand);
    }

    @Nested
    class CatalogTest {

        @Test
        void catalog_provider() {
            var assets = Set.of(
                    ASSET_ID_REST_API,
                    ASSET_ID_REST_API_DOMAIN,
                    ASSET_ID_REST_API_EMBEDDED_QUERY_PARAMS,
                    ASSET_ID_REST_API_OAUTH2,
                    POLICY_RESTRICTED_API,
                    ASSET_ID_REST_20_SEC_API,
                    ASSET_ID_REST_API_EXPIRED,
                    ASSET_ID_FAILURE_REST_API,
                    ASSET_ID_KAFKA_STREAM,
                    ASSET_ID_KAFKA_STREAM + "-tst-2",
                    ASSET_ID_REST_API_ROUTE_DOMAIN_RESTRICTED,
                    ASSET_ID_REST_API_TRAVEL_DOMAIN_RESTRICTED
            );

            var datasets = queryParticipantDatasets(AUTHORITY, PROVIDER.did(), PROVIDER.controlPlaneCatalogFilterUrl());
            var datasetIds = datasets.stream().map(dataset -> dataset.getString(ID)).collect(Collectors.toSet());
            
            assertThat(datasetIds).containsAll(assets);
            datasets.forEach(dataset -> assertThat(dataset.get(EDC_NAMESPACE + "createdAt")).isNotNull());
        }

        @Test
        void catalog_consumer() {
            // The consumer's own catalog must never leak provider-owned assets — it may contain
            // assets the consumer itself published (see catalog_consumer_with_assets()).
            assertThat(queryParticipantDatasets(AUTHORITY, CONSUMER.did(), CONSUMER.controlPlaneCatalogFilterUrl()))
                    .noneMatch(dataset -> ASSET_ID_REST_API.equals(dataset.getString(ID)));
        }

        @Test
        void catalog_consumer_with_assets() {
            var datasets = queryParticipantDatasets(AUTHORITY, CONSUMER.did(), CONSUMER.controlPlaneCatalogFilterUrl());

            assertThat(datasets).anyMatch(dataset -> ASSET_ID_CONSUMER_REST_API.equals(dataset.getString(ID)));
            datasets.stream()
                    .filter(dataset -> ASSET_ID_CONSUMER_REST_API.equals(dataset.getString(ID)))
                    .forEach(dataset -> assertThat(dataset.get(EDC_NAMESPACE + "createdAt")).isNotNull());
        }

        @Test
        void catalog_consumer_restricted() {
            assertThat(queryParticipantDatasets(AUTHORITY, PROVIDER.did(), CONSUMER.controlPlaneCatalogFilterUrl()))
                    .anyMatch(dataset -> ASSET_ID_REST_API_ROUTE_DOMAIN_RESTRICTED.equals(dataset.getString(ID)))
                    .noneMatch(dataset -> ASSET_ID_REST_API_TRAVEL_DOMAIN_RESTRICTED.equals(dataset.getString(ID)));
        }

        @Test
        void catalog_restricted_discovery_symmetric_travel_hidden() {
            // CONSUMER only holds a DomainCredential for 'route' (see AbstractAuthority.createParticipant()),
            // never one for 'travel'. The asset restricted to domain='travel' must therefore be filtered out
            // of the catalog/discovery response itself, not just rejected at contract negotiation time
            // (see transfer_forRestrictedDiscoveryAssets_NotAvailable for that other, contractual, observable).
            var datasets = queryParticipantDatasets(AUTHORITY, PROVIDER.did(), CONSUMER.controlPlaneCatalogFilterUrl());

            assertThat(datasets)
                    .noneMatch(dataset -> ASSET_ID_REST_API_TRAVEL_DOMAIN_RESTRICTED.equals(dataset.getString(ID)))
                    .anyMatch(dataset -> ASSET_ID_REST_API_ROUTE_DOMAIN_RESTRICTED.equals(dataset.getString(ID)));
        }
    }

    @Nested
    class TransferTest {

        @ParameterizedTest
        @ArgumentsSource(TransferTestProvider.class)
        void transfer_success(Map<String, String> queryParams, String assetId, String expected) {
            var expectedMsg = Map.of("message", expected);
            var transferProcessId = negotiationContractAndStartTransfer(CONSUMER, PROVIDER, assetId);

            // query by contract id
            var contractId = getContractIdFromTransferProcess(CONSUMER, transferProcessId);
            USED_CONTRACT_ID.add(contractId);
            await().atMost(TEST_TIMEOUT).untilAsserted(() -> {
                var data = CONSUMER.queryData(contractId, queryParams, 200, Map.class);
                assertThat(data).isEqualTo(expectedMsg);
            });
        }

        public static String deriveStandardizedServiceName() {
            return "proxy-provider";
        }

        @Test
        void transfer_kafka_stream_oauth() {
            var transferProcessId = negotiationContractAndStartTransfer(CONSUMER, PROVIDER, ASSET_ID_KAFKA_STREAM, KAFKA_BROKER_PULL);
            getContractIdFromTransferProcess(CONSUMER, transferProcessId);
            CONSUMER.finishDataTransfer(transferProcessId);
        }

        private void publishMessagesToKafka() throws IOException, InterruptedException {
            kafkacatPod = discoverPodName("", "kafkacat-provider");
            KafkaIntermediary.provider_publish(kafkacatPod);
        }

        @Test
        @Disabled("""
                Targets the real dse Kafka proxy (kafka-proxy-k8s-manager-core), dynamically \
                provisioned per transfer on proxy-provider:30001 with SASL_SSL/PLAIN — that proxy \
                isn't deployed on Lourmel playgrounds, so this fails there for lack of \
                infrastructure, not because of a code bug in #684's scope. Re-enable once the proxy \
                is wired up on Lourmel playgrounds (tracked in #788).""")
        void transfer_kafka_stream() throws Exception {
            var transferProcessId = negotiationContractAndStartTransfer(
                    CONSUMER, PROVIDER, ASSET_ID_KAFKA_STREAM + "-tst-2", KAFKA_BROKER_PULL
            );


            String serviceName = deriveStandardizedServiceName();
            int servicePort = 30001;  // Proxy port with SASL_SSL
            String topic = "tst-topic";
            String expectedMessage = "Hello from provider!";
            getContractIdFromTransferProcess(CONSUMER, transferProcessId);
            Thread.sleep(15000);
            kafkacatPod = discoverPodName("", "kafkacat-");
            KafkaIntermediary.provider_publish(kafkacatPod);
            boolean messageReceived = KafkaIntermediary.waitForKafkaMessage(serviceName, servicePort, topic, expectedMessage, Duration.ofSeconds(20), kafkacatPod);
            CONSUMER.finishDataTransfer(transferProcessId);
            assertTrue(messageReceived, () -> "Expected message not found in Kafka topic within timeout: " + expectedMessage);
        }

        private static String discoverPodName(String transferId, String filter) throws IOException, InterruptedException {
            long start = System.currentTimeMillis();
            while (System.currentTimeMillis() - start < 30000) {
                Process process = new ProcessBuilder("kubectl", "--context", ParticipantConstants.KUBECTL_CONTEXT, "get", "pods", "-n", "default", "-o", "jsonpath={.items[*].metadata.name}").start();
                process.waitFor();
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                    String output = reader.lines().collect(Collectors.joining(" ")).trim();
                    if (!output.isEmpty()) {
                        String nameFound = Arrays.stream(output.split("\s+")).filter(name -> name.contains(filter + transferId)).findFirst().orElse(null);
                        if (nameFound != null) return nameFound;
                    }
                }
                Thread.sleep(1000);
            }
            throw new RuntimeException("Timed out waiting for pod filter: " + filter);
        }

        @Test
        void transfer_failure() {
            var transferProcessId = negotiationContractAndStartTransfer(CONSUMER, PROVIDER, ASSET_ID_FAILURE_REST_API);

            var contractId = getContractIdFromTransferProcess(CONSUMER, transferProcessId);
            USED_CONTRACT_ID.add(contractId);
            await().atMost(TEST_TIMEOUT).untilAsserted(() -> {
                var response = CONSUMER.queryData(contractId, Map.of(), INTERNAL_SERVER_ERROR.getStatusCode(), TransferErrorResponse.class);
                assertThat(response.getErrors()).containsExactly("Received code transferring HTTP data: 500 - Internal Server Error.");
            });
        }

        @Test
        void transfer_rest_api_no_token_returns_401() {
            negotiationContractAndStartTransfer(CONSUMER, PROVIDER, ASSET_ID_REST_API);

            var response = CONSUMER.participantClient().baseDataRequest()
                    .when()
                    .get()
                    .then()
                    .statusCode(401)
                    .extract()
                    .body()
                    .as(TransferErrorResponse.class);

            assertThat(response.getErrors()).containsExactly("Missing Authorization Header");
        }

        @Test
        void transfer_whenContractExpiration_shouldTerminateTransferProcessAtExpiration() {
            var message = UUID.randomUUID().toString();
            var expectedMsg = Map.of("message", message);
            var transferProcessId = negotiationContractAndStartTransfer(CONSUMER, PROVIDER, ASSET_ID_REST_20_SEC_API);
            var contractId = getContractIdFromTransferProcess(CONSUMER, transferProcessId);
            USED_CONTRACT_ID.add(contractId);
            await().atMost(TEST_TIMEOUT).untilAsserted(() -> {
                var data = CONSUMER.queryData(contractId, Map.of("message", message), 200, Object.class);
                assertThat(data).isEqualTo(expectedMsg);
            });

            // check that after some time, the transfer process gets deprovisioned
            await().atMost(TEST_TIMEOUT).untilAsserted(() -> {
                var state = CONSUMER.participantClient().getTransferProcessState(transferProcessId);
                assertThat(state).isEqualTo(TERMINATED.name());
            });

            await().atMost(TEST_TIMEOUT).untilAsserted(() -> CONSUMER.queryData(contractId, Map.of(), 403, TransferErrorResponse.class));
        }

        @Test
        void transfer_forRestrictedDiscoveryAssets() {
            var message = UUID.randomUUID().toString();
            var expectedMsg = Map.of("message", message);
            var transferProcessId = negotiationContractAndStartTransfer(CONSUMER, PROVIDER, ASSET_ID_REST_API_ROUTE_DOMAIN_RESTRICTED);
            var contractId = getContractIdFromTransferProcess(CONSUMER, transferProcessId);
            USED_CONTRACT_ID.add(contractId);
            await().atMost(TEST_TIMEOUT).untilAsserted(() -> {
                var data = CONSUMER.queryData(contractId, Map.of("message", message), 200, Object.class);
                assertThat(data).isEqualTo(expectedMsg);
            });
        }

        @Test
        void transfer_forRestrictedDiscoveryAssets_NotAvailable() {
            var negoId = CONSUMER.participantClient().initContractNegotiation(PROVIDER.participantClient(), ASSET_ID_REST_API_TRAVEL_DOMAIN_RESTRICTED);
            await().atMost(TEST_TIMEOUT).untilAsserted(() -> {
                var state = CONSUMER.participantClient().getContractNegotiationState(negoId);
                assertThat(state).isEqualTo(ContractNegotiationStates.TERMINATED.name());
            });
        }

        @Test
        void transfer_whenPolicyNotMatched_shouldTerminate() {
            var negoId = CONSUMER.participantClient().initContractNegotiation(PROVIDER.participantClient(), POLICY_RESTRICTED_API);
            await().atMost(TEST_TIMEOUT).untilAsserted(() -> {
                var state = CONSUMER.participantClient().getContractNegotiationState(negoId);
                assertThat(state).isEqualTo(ContractNegotiationStates.TERMINATED.name());
            });
        }

        @Test
        @Tag("known-limitation")
        // inForceDate isn't enforced at negotiation time yet (only at transfer, see
        // transfer_whenContractExpiration_shouldTerminateTransferProcessAtExpiration). Asserts the
        // desired behavior rather than the current one, so the #971 fix turns this green on its own
        // instead of breaking it. Excluded from e2e runs until then (see build.gradle.kts).
        void contractNegotiation_expiredPolicy_shouldTerminate() {
            var negoId = CONSUMER.participantClient().initContractNegotiation(PROVIDER.participantClient(), ASSET_ID_REST_API_EXPIRED);
            await().atMost(TEST_TIMEOUT).untilAsserted(() -> {
                var state = CONSUMER.participantClient().getContractNegotiationState(negoId);
                assertThat(state).isEqualTo(ContractNegotiationStates.TERMINATED.name());
            });
        }

        @Test
        void contractNegotiation_delete_shouldRemoveNegotiation() {
            // Initiate a contract negotiation that will be terminated (policy not matched)
            var negoId = CONSUMER.participantClient().initContractNegotiation(PROVIDER.participantClient(), POLICY_RESTRICTED_API);
            
            // Wait for negotiation to reach TERMINATED state
            await().atMost(TEST_TIMEOUT).untilAsserted(() -> {
                var state = CONSUMER.participantClient().getContractNegotiationState(negoId);
                assertThat(state).isEqualTo(ContractNegotiationStates.TERMINATED.name());
            });
            CONSUMER.participantClient().baseManagementRequest().get("/v3/contractnegotiations/" + negoId).then().statusCode(200);
            CONSUMER.participantClient().baseManagementRequest().delete("/v3/contractnegotiations/" + negoId).then().statusCode(204);
            CONSUMER.participantClient().baseManagementRequest().get("/v3/contractnegotiations/" + negoId).then().statusCode(404);
        }

        @Test
        void contractNegotiation_success_withoutTransfer() {
            var negoId = CONSUMER.participantClient().initContractNegotiation(PROVIDER.participantClient(), ASSET_ID_REST_API);
            await().atMost(TEST_TIMEOUT).untilAsserted(() -> {
                var state = CONSUMER.participantClient().getContractNegotiationState(negoId);
                assertThat(state).isEqualTo(ContractNegotiationStates.FINALIZED.name());

                var agreementId = CONSUMER.participantClient().baseManagementRequest().get("/v3/contractnegotiations/" + negoId)
                        .then().statusCode(200).extract().body().jsonPath().getString("contractAgreementId");
                assertThat(agreementId).isNotNull();
            });
        }

        @Test
        void contract_negotiation_generic_claim_in_success() {
            // Provider asset rest-api-generic-in requires GenericClaim.$.DomainCredential.domain IN ['route','travel'].
            // CONSUMER already holds a DomainCredential with domain='route' via AbstractAuthority.createParticipant(),
            // so JsonPathCredentialConstraintFunction.parseArrayString() must resolve the constraint to true and the
            // negotiation must reach FINALIZED.
            var negoId = CONSUMER.participantClient().initContractNegotiation(PROVIDER.participantClient(), ASSET_ID_REST_API_GENERIC_IN);
            await().atMost(TEST_TIMEOUT).untilAsserted(() -> {
                var state = CONSUMER.participantClient().getContractNegotiationState(negoId);
                assertThat(state).isEqualTo(ContractNegotiationStates.FINALIZED.name());

                var agreementId = CONSUMER.participantClient().baseManagementRequest().get("/v3/contractnegotiations/" + negoId)
                        .then().statusCode(200).extract().body().jsonPath().getString("contractAgreementId");
                assertThat(agreementId).isNotNull();
            });
        }

        @Test
        void contract_negotiation_generic_claim_eq_on_membership_type_success() {
            // Control for contract_negotiation_generic_claim_eq_fails: same constraint path
            // (membership.membershipType) with the value the consumer actually holds ('FullMember'),
            // proving the path resolves and the strict asset is rejected because of the compared value.
            var negoId = CONSUMER.participantClient().initContractNegotiation(PROVIDER.participantClient(), ASSET_ID_REST_API_GENERIC_EQ_FULL_MEMBER);
            await().atMost(TEST_TIMEOUT).untilAsserted(() -> {
                var state = CONSUMER.participantClient().getContractNegotiationState(negoId);
                assertThat(state).isEqualTo(ContractNegotiationStates.FINALIZED.name());
            });
        }

        @Test
        void contract_negotiation_generic_claim_eq_fails() {
            // Provider asset rest-api-generic-eq-strict requires GenericClaim.$.MembershipCredential.membership.membershipType EQ 'Full'.
            // CONSUMER holds membershipType='FullMember', so the EQ comparison is false and the negotiation must reach TERMINATED.
            // The sibling success test uses the same path with 'FullMember' to prove the rejection comes from the compared value.
            var negoId = CONSUMER.participantClient().initContractNegotiation(PROVIDER.participantClient(), ASSET_ID_REST_API_GENERIC_EQ_STRICT);
            await().atMost(TEST_TIMEOUT).untilAsserted(() -> {
                var state = CONSUMER.participantClient().getContractNegotiationState(negoId);
                assertThat(state).isEqualTo(ContractNegotiationStates.TERMINATED.name());
            });
            CONSUMER.participantClient().baseManagementRequest().get("/v3/contractnegotiations/" + negoId).then().statusCode(200);
        }

        @Test
        void contract_negotiation_generic_claim_is_none_of_success() {
            // Provider asset rest-api-generic-none-of requires GenericClaim.$.DomainCredential.domain isNoneOf ['travel'].
            // CONSUMER holds a DomainCredential with domain='route', which is not in the deny-list, so the negotiation
            // must reach FINALIZED.
            var negoId = CONSUMER.participantClient().initContractNegotiation(PROVIDER.participantClient(), ASSET_ID_REST_API_GENERIC_NONE_OF);
            await().atMost(TEST_TIMEOUT).untilAsserted(() -> {
                var state = CONSUMER.participantClient().getContractNegotiationState(negoId);
                assertThat(state).isEqualTo(ContractNegotiationStates.FINALIZED.name());

                var agreementId = CONSUMER.participantClient().baseManagementRequest().get("/v3/contractnegotiations/" + negoId)
                        .then().statusCode(200).extract().body().jsonPath().getString("contractAgreementId");
                assertThat(agreementId).isNotNull();
            });
        }

        @Test
        void contract_negotiation_generic_claim_is_none_of_fails() {
            // Provider asset rest-api-generic-none-of-excluded requires GenericClaim.$.DomainCredential.domain isNoneOf ['route','travel'].
            // CONSUMER holds domain='route', which is in the deny-list, so the negotiation must reach TERMINATED.
            // The sibling success test uses the same path with a deny-list without 'route'.
            var negoId = CONSUMER.participantClient().initContractNegotiation(PROVIDER.participantClient(), ASSET_ID_REST_API_GENERIC_NONE_OF_EXCLUDED);
            await().atMost(TEST_TIMEOUT).untilAsserted(() -> {
                var state = CONSUMER.participantClient().getContractNegotiationState(negoId);
                assertThat(state).isEqualTo(ContractNegotiationStates.TERMINATED.name());
            });
            CONSUMER.participantClient().baseManagementRequest().get("/v3/contractnegotiations/" + negoId).then().statusCode(200);
        }

        @Test
        void contract_negotiation_profile_namespace_is_none_of_success() {
            // #799: same as contract_negotiation_generic_claim_is_none_of_success, with the left operand in the stable
            // profile namespace (https://eona-x.eu/vocabulary/odrl-profile#). Creating the policy definition must not
            // be rejected, and the negotiation must reach FINALIZED like its legacy-namespace twin.
            var negoId = CONSUMER.participantClient().initContractNegotiation(PROVIDER.participantClient(), ASSET_ID_REST_API_PROFILE_NS_NONE_OF);
            await().atMost(TEST_TIMEOUT).untilAsserted(() -> {
                var state = CONSUMER.participantClient().getContractNegotiationState(negoId);
                assertThat(state).isEqualTo(ContractNegotiationStates.FINALIZED.name());
            });
        }

        @Test
        void contract_negotiation_profile_namespace_is_none_of_fails() {
            // #799: control for the test above. The deny-list contains 'route', the consumer's domain: the
            // stable-namespace constraint must be evaluated (not dropped as unbound), so the negotiation is TERMINATED.
            var negoId = CONSUMER.participantClient().initContractNegotiation(PROVIDER.participantClient(), ASSET_ID_REST_API_PROFILE_NS_NONE_OF_EXCLUDED);
            await().atMost(TEST_TIMEOUT).untilAsserted(() -> {
                var state = CONSUMER.participantClient().getContractNegotiationState(negoId);
                assertThat(state).isEqualTo(ContractNegotiationStates.TERMINATED.name());
            });
        }

        public static class TransferTestProvider implements ArgumentsProvider {
            @Override
            public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) {
                var msg = UUID.randomUUID().toString();
                return Stream.of(
                        Arguments.of(Map.of("message", msg), ASSET_ID_REST_API, msg),
                        Arguments.of(Map.of("message", msg), ASSET_ID_REST_API_DOMAIN, msg),
                        Arguments.of(Map.of("message", msg), ASSET_ID_REST_API_OAUTH2, msg),
                        Arguments.of(Map.of(), ASSET_ID_REST_API_EMBEDDED_QUERY_PARAMS, EMBEDDED_QUERY_PARAM));
            }
        }

        @Test
        void transfer_retireAgreement_shouldBlockFurtherAccess() {
            var message = UUID.randomUUID().toString();
            var expectedMsg = Map.of("message", message);
            var transferProcessId = negotiationContractAndStartTransfer(CONSUMER, PROVIDER, ASSET_ID_REST_API);
            var contractId = getContractIdFromTransferProcess(CONSUMER, transferProcessId);
            USED_CONTRACT_ID.add(contractId);
            await().atMost(TEST_TIMEOUT).untilAsserted(() -> {
                var data = CONSUMER.queryData(contractId, Map.of("message", message), 200, Map.class);
                assertThat(data).isEqualTo(expectedMsg);
            });
            // Each side persists its own locally-generated contract agreement id -- confirmed live
            // on lourmel-e2e via direct DB inspection (provider's and consumer's edc_contract_agreement
            // rows for the same correlated negotiation never share an agr_id). CONSUMER's id (above)
            // is right for CONSUMER.queryData() (its own data-plane/EDR lookup), but retiring/
            // reactivating an agreement is a PROVIDER-side management-API operation addressed by
            // PROVIDER's own id, resolved here via the transfer processes' correlationId link.
            var providerContractId = getContractIdFromTransferProcess(PROVIDER,
                    getProviderTransferProcessId(PROVIDER, transferProcessId));
            retireAgreement(PROVIDER, providerContractId, "Test retirement reason");
            await().atMost(TEST_TIMEOUT).untilAsserted(() -> {
                var error = CONSUMER.queryData(contractId, Map.of("message", message), 403, TransferErrorResponse.class);
                assertThat(error.getErrors()).anyMatch(m -> m.contains("No EDR satisfying criterion"));
            });
            reactivateAgreement(PROVIDER, providerContractId);
            transferProcess(CONSUMER, PROVIDER, contractId);
            await().atMost(TEST_TIMEOUT).untilAsserted(() -> {
                var data = CONSUMER.queryData(contractId, Map.of("message", message), 200, Map.class);
                assertThat(data).isEqualTo(expectedMsg);
            });
        }

        protected void transferProcess(AbstractParticipant consumer, AbstractParticipant provider, String contractId) {
            var providerUrl = provider.controlPlaneProtocolUrl();
            var consumerDid = consumer.did();
            var body = Map.of(
                    "@context", Map.of(
                            "@vocab", "https://w3id.org/edc/v0.0.1/ns/"
                    ),
                    "@type", "TransferRequest",
                    "counterPartyAddress", providerUrl,
                    "protocol", "dataspace-protocol-http",
                    "connectorId", consumerDid,
                    "contractId", contractId,
                    "transferType", "HttpData-PULL"
            );
            consumer.participantClient().baseManagementRequest()
                    .contentType(ContentType.JSON)
                    .body(body)
                    .when()
                    .post("/v3/transferprocesses")
                    .then()
                    .log().ifError()
                    .statusCode(200);
        }
    }

    @Nested
    @Order(1)
    class ReportTest {
        public static String buildTelemetryJson(String id, String contractId, String participantDid, int responseStatusCode, int msgSize, Integer csvId, Timestamp timestamp) {
            try {
                ObjectNode root = OBJECT_MAPPER.createObjectNode();
                root.put("id", id);
                root.put("contractId", contractId);
                root.put("participantId", participantDid);
                root.put("responseStatusCode", responseStatusCode);
                root.put("msgSize", msgSize);
                if (csvId != null) {
                    root.put("csvId", csvId);
                } else {
                    root.putNull("csvId");
                }
                root.put("timestamp", timestamp.toInstant().toString());
                return OBJECT_MAPPER.writeValueAsString(root);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public static String buildGenerationJson(String participantName, Integer month, Integer year) {
            try {
                ObjectNode root = OBJECT_MAPPER.createObjectNode();
                root.put("participantName", participantName);
                root.put("month", month);
                root.put("year", year);
                return OBJECT_MAPPER.writeValueAsString(root);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Test
        void testReportGenerationSucceeds() {
            String ctId = UUID.randomUUID().toString();
            int month = 9;
            int year = 2025;
            Timestamp timestamp = Timestamp.valueOf(LocalDateTime.of(year, month, 20, 20, 18));
            given().baseUri(AUTHORITY.telemetryUrl()).contentType(JSON).body(buildTelemetryJson("1", ctId, CONSUMER.did(), 200, 20, null, timestamp)).post().then().statusCode(201);
            given().baseUri(AUTHORITY.telemetryUrl()).contentType(JSON).body(buildTelemetryJson("2", ctId, PROVIDER.did(), 200, 20, null, timestamp)).post().then().statusCode(201);
            given().baseUri(AUTHORITY.csvManagerUrl()).body(buildGenerationJson(CONSUMER.name(), month, year)).contentType(JSON).post().then().statusCode(201);

            Response response = given().baseUri(AUTHORITY.csvManagerUrl())
                    .params(Map.of("month", month, "year", year))
                    .contentType(JSON)
                    .header("Authorization", "Bearer " + dummyJwt)
                    .get().then().statusCode(200).contentType(containsString("text/csv")).extract().response();
            String expected = REPORT_HEADER_WITHOUT_COUNTERPARTY_INFO + "\n" + ctId + "," + PROVIDER.name() + "," + 200 + "," + 0.02 + "," + 1;
            assertEquals(expected, response.getBody().asString().trim());
        }

        @Test
        void testReportGenerationWithOnlyOnePartySucceeds() {
            String ctId = UUID.randomUUID().toString();
            int month = 12;
            int year = 2025;
            given().baseUri(AUTHORITY.telemetryUrl()).contentType(JSON).body(buildTelemetryJson("3", ctId, CONSUMER.did(), 400, 20, null, Timestamp.valueOf(LocalDateTime.of(year, month, 20, 20, 18)))).post().then().statusCode(201);
            given().baseUri(AUTHORITY.csvManagerUrl()).body(buildGenerationJson(CONSUMER.name(), month, year)).contentType(JSON).post().then().statusCode(201);
            Response response = given().baseUri(AUTHORITY.csvManagerUrl())
                    .params(Map.of("month", month, "year", year))
                    .contentType(JSON)
                    .header("Authorization", "Bearer " + dummyJwt)
                    .get().then().statusCode(200).contentType(containsString("text/csv")).extract().response();
            String expected = REPORT_HEADER_WITHOUT_COUNTERPARTY_INFO + "\n" + ctId + ",N/A," + 400 + "," + 0.02 + "," + 1;
            assertEquals(expected, response.getBody().asString().trim());
        }

        @Test
        void testRetrieveReportWithNonExistentParticipantFails() {
            String ctId = UUID.randomUUID().toString();
            int month = 1;
            int year = 2025;
            given().baseUri(AUTHORITY.telemetryUrl()).contentType(JSON).body(buildTelemetryJson("4", ctId, CONSUMER.did(), 200, 20, null, Timestamp.valueOf(LocalDateTime.of(year, month, 20, 20, 18)))).post().then().statusCode(201);
            given().baseUri(AUTHORITY.csvManagerUrl()).body(buildGenerationJson(CONSUMER.name(), month, year)).contentType(JSON).post().then().statusCode(201);
            given().baseUri(AUTHORITY.csvManagerUrl()).params(Map.of("month", month, "year", year)).contentType(JSON).header("Authorization", "Bearer " + dummyJwtNonExistentParticipant).get().then().statusCode(403);
        }

        @Test
        void testRetrieveReportWithExpiredTokenFails() {
            String ctId = UUID.randomUUID().toString();
            int month = 1;
            int year = 2025;
            given().baseUri(AUTHORITY.telemetryUrl()).contentType(JSON).body(buildTelemetryJson("5", ctId, CONSUMER.did(), 200, 20, null, Timestamp.valueOf(LocalDateTime.of(year, month, 20, 20, 18)))).post().then().statusCode(201);
            given().baseUri(AUTHORITY.csvManagerUrl()).body(buildGenerationJson(CONSUMER.name(), month, year)).contentType(JSON).post().then().statusCode(201);
            given().baseUri(AUTHORITY.csvManagerUrl()).params(Map.of("month", month, "year", year)).contentType(JSON).header("Authorization", "Bearer " + dummyJwtExpired).get().then().statusCode(401);
        }

        @Test
        void testRetrieveNonExistentReportFromExistentParticipantFails() {
            given().baseUri(AUTHORITY.csvManagerUrl()).params(Map.of("month", 2, "year", 2024)).contentType(JSON).header("Authorization", "Bearer " + dummyJwt).get().then().statusCode(404);
        }

        @Test
        void testRetrieveReportWithNoTokenFails() {
            String ctId = UUID.randomUUID().toString();
            int month = 1;
            int year = 2025;
            given().baseUri(AUTHORITY.telemetryUrl()).contentType(JSON).body(buildTelemetryJson("5", ctId, CONSUMER.did(), 200, 20, null, Timestamp.valueOf(LocalDateTime.of(year, month, 20, 20, 18)))).post().then().statusCode(201);
            given().baseUri(AUTHORITY.csvManagerUrl()).body(buildGenerationJson(CONSUMER.name(), month, year)).contentType(JSON).post().then().statusCode(201);
            given().baseUri(AUTHORITY.csvManagerUrl()).params(Map.of("month", month, "year", year)).contentType(JSON).get().then().statusCode(401);
        }

        @Test
        void testRetrieveReportWithMalformedJwtFails() {
            given().baseUri(AUTHORITY.csvManagerUrl()).params(Map.of("month", 9, "year", 2025)).contentType(JSON).header("Authorization", "Bearer notajwt").get().then().statusCode(401);
        }
    }
}
