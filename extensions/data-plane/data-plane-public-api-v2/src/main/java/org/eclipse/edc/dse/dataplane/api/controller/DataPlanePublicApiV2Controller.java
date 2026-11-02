/*
 *  Copyright (c) 2024 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Bayerische Motoren Werke Aktiengesellschaft (BMW AG) - initial API and implementation
 *       Bayerische Motoren Werke Aktiengesellschaft (BMW AG) - handle HEAD requests
 *
 */

package org.eclipse.edc.dse.dataplane.api.controller;

import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
import org.eclipse.edc.connector.dataplane.spi.iam.DataPlaneAuthorizationService;
import org.eclipse.edc.connector.dataplane.spi.pipeline.PipelineService;
import org.eclipse.edc.connector.dataplane.spi.response.TransferErrorResponse;
import org.eclipse.edc.dse.dataplane.api.headers.ForwardedHeaders;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static jakarta.ws.rs.core.MediaType.WILDCARD;
import static jakarta.ws.rs.core.Response.Status.FORBIDDEN;
import static jakarta.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static jakarta.ws.rs.core.Response.Status.UNAUTHORIZED;
import static jakarta.ws.rs.core.Response.status;

@Path("{any:.*}")
@Produces(WILDCARD)
public class DataPlanePublicApiV2Controller implements DataPlanePublicApiV2 {

    private final PipelineService pipelineService;
    private final DataFlowRequestSupplier requestSupplier;
    private final ExecutorService executorService;
    private final DataPlaneAuthorizationService authorizationService;
    private final boolean markRelayedResponses;

    /**
     * Creates the proxy controller of the provider public API or of the consumer data API.
     *
     * @param markRelayedResponses whether to mark the responses relayed from the data source with
     *                             {@link ForwardedHeaders#RELAYED_RESPONSE_HEADER}: true on the provider public API,
     *                             so that the consumer hop relays them in turn, false on the consumer data API.
     */
    public DataPlanePublicApiV2Controller(PipelineService pipelineService,
                                          ExecutorService executorService,
                                          DataPlaneAuthorizationService authorizationService,
                                          ForwardedHeaders forwardedHeaders,
                                          boolean markRelayedResponses) {
        this.pipelineService = pipelineService;
        this.authorizationService = authorizationService;
        this.requestSupplier = new DataFlowRequestSupplier(forwardedHeaders);
        this.executorService = executorService;
        this.markRelayedResponses = markRelayedResponses;
    }

    private static Response error(Response.Status status, List<String> errors) {
        return status(status).type(APPLICATION_JSON).entity(new TransferErrorResponse(errors)).build();
    }

    @GET
    @Override
    public void get(@Context ContainerRequestContext requestContext, @Suspended AsyncResponse response) {
        handle(requestContext, response);
    }

    @HEAD
    @Override
    public void head(@Context ContainerRequestContext requestContext, @Suspended AsyncResponse response) {
        handle(requestContext, response);
    }

    /**
     * Sends a {@link POST} request to the data source and returns data.
     *
     * @param requestContext Request context.
     * @param response       Data fetched from the data source.
     */
    @POST
    @Override
    public void post(@Context ContainerRequestContext requestContext, @Suspended AsyncResponse response) {
        handle(requestContext, response);
    }

    /**
     * Sends a {@link PUT} request to the data source and returns data.
     *
     * @param requestContext Request context.
     * @param response       Data fetched from the data source.
     */
    @PUT
    @Override
    public void put(@Context ContainerRequestContext requestContext, @Suspended AsyncResponse response) {
        handle(requestContext, response);
    }

    /**
     * Sends a {@link DELETE} request to the data source and returns data.
     *
     * @param requestContext Request context.
     * @param response       Data fetched from the data source.
     */
    @DELETE
    @Override
    public void delete(@Context ContainerRequestContext requestContext, @Suspended AsyncResponse response) {
        handle(requestContext, response);
    }

    /**
     * Sends a {@link PATCH} request to the data source and returns data.
     *
     * @param requestContext Request context.
     * @param response       Data fetched from the data source.
     */
    @PATCH
    @Override
    public void patch(@Context ContainerRequestContext requestContext, @Suspended AsyncResponse response) {
        handle(requestContext, response);
    }

    private void handle(ContainerRequestContext requestContext, AsyncResponse response) {
        var contextApi = new ContainerRequestContextApiImpl(requestContext);

        var token = contextApi.headers().get(HttpHeaders.AUTHORIZATION);
        if (token == null) {
            response.resume(error(UNAUTHORIZED, List.of("Missing Authorization Header")));
            return;
        }

        var sourceDataAddress = authorizationService.authorize(token, buildRequestData(requestContext));
        if (sourceDataAddress.failed()) {
            response.resume(error(FORBIDDEN, sourceDataAddress.getFailureMessages()));
            return;
        }

        var startMessage = requestSupplier.apply(contextApi, sourceDataAddress.getContent());

        processRequest(startMessage, response);
    }

    private Map<String, Object> buildRequestData(ContainerRequestContext requestContext) {
        var requestData = new HashMap<String, Object>();
        requestData.put("headers", requestContext.getHeaders());
        requestData.put("path", requestContext.getUriInfo());
        requestData.put("method", requestContext.getMethod());
        requestData.put("content-type", requestContext.getMediaType());
        return requestData;
    }

    private void processRequest(DataFlowStartMessage dataFlowStartMessage, AsyncResponse response) {

        RelayingStreamingDataSink.ResponseContext responseContext = relayedResponse -> {
            StreamingOutput output = t -> relayedResponse.writer().accept(t);
            var builder = Response.status(relayedResponse.statusCode()).entity(output).type(relayedResponse.mediaType());
            relayedResponse.headers().forEach(builder::header);
            if (relayedResponse.relayed() && markRelayedResponses) {
                builder.header(ForwardedHeaders.RELAYED_RESPONSE_HEADER, Boolean.TRUE.toString());
            }
            return response.resume(builder.build());
        };

        var sink = new RelayingStreamingDataSink(responseContext, executorService);

        pipelineService.transfer(dataFlowStartMessage, sink)
                .whenComplete((result, throwable) -> {
                    if (throwable == null) {
                        if (result.failed()) {
                            response.resume(error(INTERNAL_SERVER_ERROR, result.getFailureMessages()));
                        }
                    } else {
                        var error = "Unhandled exception occurred during data transfer: " + throwable.getMessage();
                        response.resume(error(INTERNAL_SERVER_ERROR, List.of(error)));
                    }
                });
    }

}
