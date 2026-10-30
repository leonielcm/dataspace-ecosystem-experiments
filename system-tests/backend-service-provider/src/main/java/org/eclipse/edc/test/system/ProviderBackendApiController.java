package org.eclipse.edc.test.system;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.crypto.ECDSAVerifier;
import com.nimbusds.jwt.SignedJWT;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.web.spi.exception.InvalidRequestException;
import org.eclipse.edc.web.spi.exception.NotAuthorizedException;

import java.security.interfaces.ECPublicKey;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

@Path("/provider")
public class ProviderBackendApiController {

    static final long MAX_PAYLOAD_SIZE = 50L * 1024 * 1024;
    static final long MAX_DELAY_MS = 60_000;
    private static final int CHUNK_SIZE = 8192;

    private final ECPublicKey publicKey;
    private final Monitor monitor;


    public ProviderBackendApiController(ECPublicKey publicKey, Monitor monitor) {
        this.publicKey = publicKey;
        this.monitor = monitor;

    }

    @Path("/data")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, String> getData(@DefaultValue("some information") @QueryParam("message") String message) {
        return Map.of("message", message);
    }

    /**
     * Streams {@code size} deterministic bytes, optionally after {@code delayMs}. Used by the
     * connector performance experiment to compare direct vs connector access at different payload
     * sizes -- the other endpoints only return a few bytes.
     */
    @Path("/payload")
    @GET
    // JSON too: an invalid size is rejected through EdcApiExceptionMapper, whose JSON error body
    // can't be written as octet-stream (that would turn the 400 into a 500).
    @Produces({ MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON })
    public Response getPayload(@DefaultValue("1024") @QueryParam("size") long size,
                               @DefaultValue("0") @QueryParam("delayMs") long delayMs) {
        if (size < 0 || size > MAX_PAYLOAD_SIZE) {
            throw new InvalidRequestException("size must be between 0 and %d bytes".formatted(MAX_PAYLOAD_SIZE));
        }
        if (delayMs < 0 || delayMs > MAX_DELAY_MS) {
            throw new InvalidRequestException("delayMs must be between 0 and %d".formatted(MAX_DELAY_MS));
        }
        StreamingOutput payload = output -> {
            if (delayMs > 0) {
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            var chunk = new byte[CHUNK_SIZE];
            Arrays.fill(chunk, (byte) 'x');
            var remaining = size;
            while (remaining > 0) {
                var length = (int) Math.min(remaining, CHUNK_SIZE);
                output.write(chunk, 0, length);
                remaining -= length;
            }
            output.flush();
        };
        return Response.ok(payload, MediaType.APPLICATION_OCTET_STREAM_TYPE).build();
    }

    @Path("/headers")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, String> getHeaders(@Context HttpHeaders headers) {
        return headers.getRequestHeaders().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> String.join(",", entry.getValue())));
    }

    @Path("/postdata")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> postData(Map<String, Object> requestBody) {
        monitor.info("Received request body: " + requestBody);
        requestBody.put("processedBy", "ProviderBackendApiController");
        return requestBody;
    }

    @Path("/oauth2data")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, String> getOauth2Data(@DefaultValue("some information") @QueryParam("message") String message, @HeaderParam("Authorization") String authorization) {
        if (authorization == null || !isAuthorized(authorization)) {
            throw new NotAuthorizedException("The authorization token is not valid: " + authorization);
        } else {
            return Map.of("message", message);
        }
    }

    @Path("/failure")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, String> failure() {
        throw new NotAuthorizedException("Not authorized");
    }

    private boolean isAuthorized(String authorization) {
        if (!authorization.startsWith("Bearer ")) {
            return false;
        }

        var token = authorization.replace("Bearer ", "");

        try {
            var jwt = SignedJWT.parse(token);
            return jwt.verify(new ECDSAVerifier(publicKey));
        } catch (ParseException | JOSEException e) {
            return false;
        }
    }


}
