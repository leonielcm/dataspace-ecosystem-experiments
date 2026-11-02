package org.eclipse.edc.test.system.mcp;

import org.eclipse.edc.connector.controlplane.test.system.utils.Participant;
import org.eclipse.edc.junit.utils.Endpoints;

import java.util.Objects;

import static io.restassured.http.ContentType.JSON;

/**
 * Participant of the MCP end-to-end test, driven through its control plane management API.
 */
public class McpParticipant extends Participant {

    private McpParticipant() {
        super();
    }

    public static McpParticipant of(String id, Endpoints controlPlaneEndpoints) {
        return new Builder()
                .id(id)
                .name(id)
                .endpoints(controlPlaneEndpoints)
                .build();
    }

    /**
     * Id of the contract agreement a transfer process runs under: the consumer data API expects it as
     * {@code Authorization}.
     */
    public String getAgreementId(String transferProcessId) {
        return baseManagementRequest()
                .contentType(JSON)
                .when()
                .get("/v3/transferprocesses/{id}", transferProcessId)
                .then()
                .log().ifError()
                .statusCode(200)
                .extract().jsonPath().getString("contractId");
    }

    /**
     * Whether the consumer control plane received and stored the EDR of a transfer process.
     */
    public boolean hasEdr(String transferProcessId) {
        return baseManagementRequest()
                .contentType(JSON)
                .when()
                .get("/v3/edrs/{id}/dataaddress", transferProcessId)
                .then()
                .extract().statusCode() == 200;
    }

    private static class Builder extends Participant.Builder<McpParticipant, Builder> {

        private Builder() {
            super(new McpParticipant());
        }

        private Builder endpoints(Endpoints controlPlaneEndpoints) {
            participant.controlPlaneManagement = Objects.requireNonNull(controlPlaneEndpoints.getEndpoint("management"));
            participant.controlPlaneProtocol = Objects.requireNonNull(controlPlaneEndpoints.getEndpoint("protocol"));
            return this;
        }
    }
}
