package org.eclipse.edc.test.system;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.restassured.http.ContentType;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.JsonValue;
import org.eclipse.edc.jsonld.TitaniumJsonLd;
import org.eclipse.edc.jsonld.spi.JsonLd;
import org.eclipse.edc.jsonld.util.JacksonJsonLd;
import org.eclipse.edc.spi.monitor.ConsoleMonitor;
import org.eclipse.edc.spi.monitor.Monitor;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.eclipse.edc.connector.controlplane.transfer.spi.types.TransferProcessStates.STARTED;
import static org.eclipse.edc.jsonld.spi.JsonLdKeywords.ID;
import static org.eclipse.edc.jsonld.spi.PropertyAndTypeNames.DCAT_DATASET_ATTRIBUTE;
import static org.eclipse.edc.test.system.TestConstants.DEFAULT_TIMEOUT;

public class AbstractEndToEndTests {

    protected static final ObjectMapper MAPPER = JacksonJsonLd.createObjectMapper();
    protected static final Duration TEST_TIMEOUT = DEFAULT_TIMEOUT;
    protected static final Duration CREDENTIAL_ISSUANCE_TIMEOUT = Duration.ofSeconds(60);
    protected static final Monitor MONITOR = new ConsoleMonitor();
    private static final JsonLd JSON_LD = new TitaniumJsonLd(MONITOR);
    private static final Duration TEST_POLL_INTERVAL = Duration.ofMillis(500);
    public static final String HTTP_DATA_PULL = "HttpData-PULL";
    // The federated-catalog crawler (federated-catalog-core-2025, the only module that
    // registers a working crawler in 0.15.0) serializes Catalog#participantId under the
    // DSP 2025/1 namespace, not DSPACE_SCHEMA (still v0.8 upstream, see Namespaces.java) --
    // confirmed live on lourmel-e2e.
    private static final String DSPACE_SCHEMA_2025_1 = "https://w3id.org/dspace/2025/1/";

    protected static final List<String> PARTICIPANT_NAMES = List.of("authority", "provider", "consumer");
    protected static final List<String> ASSET_OWNING_PARTICIPANTS = List.of("provider", "consumer");

    // Tables the standard EDC control-plane schema creates during the suite's own @Test methods —
    // every participant has this schema (authority is a participant too), so it applies to all
    // three. Shared between LocalEndToEndTests and HeaderSecretFunctionalTest: both create
    // provider assets/policies/contract-defs and need the same replay-safety, but as separate
    // JUnit classes with their own independent @BeforeAll. Never includes
    // edc_holder_credentialrequest, participant_context, or edc_lease.
    protected static final List<String> EDC_TABLES = List.of(
            "edc_asset", "edc_policydefinitions", "edc_contract_definitions",
            "edc_contract_negotiation", "edc_contract_agreement", "edc_transfer_process",
            "edc_policy_monitor", "edc_edr_entry", "edc_accesstokendata", "edc_agreement_retirement");

    // authority also runs the issuer-service and telemetry-storage schemas on top of the standard
    // EDC_TABLES above. Only LocalEndToEndTests exercises these — HeaderSecretFunctionalTest never
    // touches authority at all, so it only ever resets 'provider' against EDC_TABLES.
    protected static final List<String> AUTHORITY_ISSUER_TABLES = List.of(
            "attestation_definitions", "credential_definitions", "holders",
            "membership_attestation", "domain_attestation", "edc_issuance_process");

    protected static final List<String> AUTHORITY_TELEMETRY_TABLES = List.of("telemetry_event", "report");

    /**
     * Verifies a participant's DB is in the state a previous run's own cleanup should have left it
     * in (empty of EDC_TABLES), and only forces a reset if it isn't — the nominal case is a
     * no-op check.
     */
    protected static void resetParticipant(String participant, List<String> tables) {
        if (!DatabaseCleaner.isEmpty(participant, tables)) {
            MONITOR.warning("DB not empty for '" + participant + "' at startup — the previous run probably didn't finish its cleanup. Forcing a reset before continuing.");
            DatabaseCleaner.truncateTables(participant, tables);
        }
    }

    /**
     * resetParticipant() against the standard EDC_TABLES — what any plain participant (provider,
     * consumer) needs. authority needs its extra table sets reset too, see resetAllParticipants().
     */
    protected static void resetParticipant(String participant) {
        resetParticipant(participant, EDC_TABLES);
    }

    /**
     * Full 3-participant reset (authority + provider + consumer), for @EndToEndTest classes that
     * exercise the whole dataspace rather than just 'provider' in isolation. Today only
     * LocalEndToEndTests needs this — HeaderSecretFunctionalTest calls resetParticipant("provider")
     * directly instead, since it never touches authority or consumer.
     */
    protected static void resetAllParticipants() {
        for (String participant : ASSET_OWNING_PARTICIPANTS) {
            resetParticipant(participant);
        }
        resetParticipant("authority", AUTHORITY_ISSUER_TABLES);
        resetParticipant("authority", AUTHORITY_TELEMETRY_TABLES);
        for (String participant : PARTICIPANT_NAMES) {
            DatabaseCleaner.purgeStaleEntries(participant);
        }
    }

    protected List<JsonObject> queryParticipantDatasets(AbstractAuthority authority, String participantDid, String catalogUrl) {
        AtomicReference<List<JsonObject>> datasets = new AtomicReference<>();
        await().atMost(TEST_TIMEOUT)
                .pollInterval(TEST_POLL_INTERVAL)
                .untilAsserted(() -> {
                    var catalog = authority.queryCatalog(MAPPER, JSON_LD, catalogUrl).stream()
                            .filter(isCatalogOf(participantDid))
                            .findFirst()
                            .orElseThrow(() -> new AssertionError("Failed to find Catalog for participant %s".formatted(participantDid)));
                    datasets.set(Optional.ofNullable(catalog.getJsonArray(DCAT_DATASET_ATTRIBUTE)).map(arr -> arr.stream().map(JsonValue::asJsonObject).toList()).orElse(List.of()));
                });

        return datasets.get();
    }

    protected String negotiationContractAndStartTransfer(AbstractParticipant consumer, AbstractParticipant provider, String assetId) {
        return negotiationContractAndStartTransfer(consumer, provider, assetId, HTTP_DATA_PULL);
    }

    protected String negotiationContractAndStartTransfer(AbstractParticipant consumer, AbstractParticipant provider, String assetId, String transferType) {
        var transferProcessId = consumer.participantClient().requestAssetFrom(assetId, provider.participantClient())
                .withTransferType(transferType)
                .execute();

        await().atMost(TEST_TIMEOUT).untilAsserted(() -> {
            var state = consumer.participantClient().getTransferProcessState(transferProcessId);
            assertThat(state).isEqualTo(STARTED.name());
        });

        return transferProcessId;
    }

    protected static String getContractIdFromTransferProcess(AbstractParticipant consumer, String transferProcessId) {
        return consumer.participantClient().baseManagementRequest().get("/v3/transferprocesses/" + transferProcessId).then().statusCode(200).extract().body().jsonPath().getString("contractId");
    }

    /**
     * Resolves the provider's own transfer process id for a negotiation the consumer
     * initiated, via the correlationId link (provider.transferProcess.correlationId ==
     * consumer's transferProcessId) -- each side persists its own locally-generated contract
     * agreement id, so the consumer's contractId can't be reused for provider-side operations.
     */
    protected static String getProviderTransferProcessId(AbstractParticipant provider, String consumerTransferProcessId) {
        var filter = Map.of(
                "@context", Map.of("@vocab", "https://w3id.org/edc/v0.0.1/ns/"),
                "filterExpression", List.of(Map.of(
                        "operandLeft", "correlationId",
                        "operator", "=",
                        "operandRight", consumerTransferProcessId
                )));
        return provider.participantClient().baseManagementRequest()
                .contentType(ContentType.JSON)
                .body(filter)
                .when()
                .post("/v3/transferprocesses/request")
                .then()
                .log().ifError()
                .statusCode(200)
                .extract().body().jsonPath().getString("[0].'@id'");
    }

    protected static void retireAgreement(AbstractParticipant participant, String contractId, String reason) {
        var body = Map.of(
                "@context", Map.of(
                        "eonax", "https://w3id.org/eonax/v0.0.1/ns/",
                        "edc", "https://w3id.org/edc/v0.0.1/ns/"
                ),
                "edc:agreementId", contractId,
                "eonax:reason", reason
        );

        participant.participantClient().baseManagementRequest()
                .contentType(ContentType.JSON)
                .body(body)
                .when()
                .post("/v3/contractagreements/retirements")
                .then()
                .log().ifError()
                .statusCode(204);
    }

    protected static void reactivateAgreement(AbstractParticipant participant, String contractId) {
        participant.participantClient().baseManagementRequest()
                .contentType(ContentType.JSON)
                .when()
                .delete("/v3/contractagreements/retirements/" + contractId)
                .then()
                .log().ifError()
                .statusCode(204);
    }

    /**
     * Waits for a single credential type to reach the holder. Issuance goes through a lease on
     * the same DB row from both the holder's own request-send state machine and the issuer's
     * delivery callback (org.eclipse.edc.sql.lease.SqlLeaseContext, upstream IdentityHub/Connector) —
     * firing every credential request for every participant up front, then checking them all
     * afterward, lets several of these leases contend at once and reliably stalls at CREATED.
     * Requesting one credential at a time and waiting for THIS ONE to land before moving on
     * (mirroring infrastructure/scripts/lourmel/init-dataspace-playground.sh's step5, which does
     * the same thing one participant at a time) avoids that contention.
     */
    protected static void waitForCredentialType(AbstractEntity participant, String credentialType) {
        await().atMost(CREDENTIAL_ISSUANCE_TIMEOUT).untilAsserted(() -> {
            boolean hasType = false;
            List<JsonObject> verifiableCredentials = participant.getCredentials(MAPPER, credentialType);
            for (JsonObject verifiableCredential : verifiableCredentials) {
                JsonObject vc = verifiableCredential.getJsonObject("verifiableCredential");
                JsonObject credential = vc.getJsonObject("credential");
                JsonArray types = credential.getJsonArray("type");
                for (int j = 0; j < types.size(); j++) {
                    if (types.getString(j).equals(credentialType)) {
                        hasType = true;
                    }
                }
            }
            assert hasType : "Missing " + credentialType + " type";
        });
    }

    private static Predicate<JsonObject> isCatalogOf(String did) {
        return catalog -> catalog.getJsonArray(DSPACE_SCHEMA_2025_1 + "participantId")
                .stream()
                .allMatch(jsonValue -> did.equals(jsonValue.asJsonObject().getString(ID)));
    }


}
