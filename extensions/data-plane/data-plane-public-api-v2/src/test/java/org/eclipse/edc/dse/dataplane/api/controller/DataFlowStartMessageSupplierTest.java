/*
 *  Copyright (c) 2022 Amadeus
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Amadeus - initial API and implementation
 *
 */

package org.eclipse.edc.dse.dataplane.api.controller;

import jakarta.ws.rs.HttpMethod;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.edc.connector.dataplane.spi.schema.DataFlowRequestSchema;
import org.eclipse.edc.connector.dataplane.util.sink.AsyncStreamingDataSink;
import org.eclipse.edc.dse.dataplane.api.headers.ForwardedHeaders;
import org.eclipse.edc.dse.dataplane.api.headers.ProxyHttpDataSourceFactory;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.edc.spi.constants.CoreConstants.EDC_NAMESPACE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DataFlowStartMessageSupplierTest {


    private final DataFlowRequestSupplier supplier = new DataFlowRequestSupplier(ForwardedHeaders.of(ForwardedHeaders.DEFAULT_ALLOWED));

    private static DataAddress createDataAddress() {
        return DataAddress.Builder.newInstance().type("test-type").build();
    }

    @Test
    void verifyMapping_noInputBody() {
        var contextApi = mock(ContainerRequestContextApi.class);
        var address = createDataAddress();

        var method = HttpMethod.GET;
        var queryParams = "test-query-param";
        var path = "test-path";

        when(contextApi.method()).thenReturn(method);
        when(contextApi.queryParams()).thenReturn(queryParams);
        when(contextApi.path()).thenReturn(path);

        var request = supplier.apply(contextApi, address);

        assertThat(request.getId()).isNotBlank();
        assertThat(request.getDestinationDataAddress().getType()).isEqualTo(AsyncStreamingDataSink.TYPE);
        assertThat(request.getSourceDataAddress().getType()).isEqualTo(address.getType());
        assertThat(request.getProperties()).containsExactlyInAnyOrderEntriesOf(Map.of(
                DataFlowRequestSchema.PATH, path,
                DataFlowRequestSchema.METHOD, method,
                DataFlowRequestSchema.QUERY_PARAMS, queryParams

        ));
    }

    @Test
    void verifyMapping_withInputBody() {
        var contextApi = mock(ContainerRequestContextApi.class);
        var address = createDataAddress();

        var method = HttpMethod.GET;
        var queryParams = "test-query-param";
        var path = "test-path";
        var body = "Test request body";

        when(contextApi.method()).thenReturn(method);
        when(contextApi.queryParams()).thenReturn(queryParams);
        when(contextApi.path()).thenReturn(path);
        when(contextApi.mediaType()).thenReturn(MediaType.TEXT_PLAIN);
        when(contextApi.body()).thenReturn(body);

        var request = supplier.apply(contextApi, address);

        assertThat(request.getId()).isNotBlank();
        assertThat(request.getDestinationDataAddress().getType()).isEqualTo(AsyncStreamingDataSink.TYPE);
        assertThat(request.getSourceDataAddress().getType()).isEqualTo(address.getType());
        assertThat(request.getProperties()).containsExactlyInAnyOrderEntriesOf(Map.of(
                DataFlowRequestSchema.PATH, path,
                DataFlowRequestSchema.METHOD, method,
                DataFlowRequestSchema.QUERY_PARAMS, queryParams,
                DataFlowRequestSchema.BODY, body,
                DataFlowRequestSchema.MEDIA_TYPE, MediaType.TEXT_PLAIN
        ));
    }

    @Test
    void verifyMapping_forwardsAllowedHeadersOnly() {
        var contextApi = mock(ContainerRequestContextApi.class);

        when(contextApi.method()).thenReturn(HttpMethod.POST);
        when(contextApi.path()).thenReturn("mcp");
        when(contextApi.headers()).thenReturn(Map.of(
                "mcp-session-id", "session-1",
                "MCP-Protocol-Version", "2025-06-18",
                "Authorization", "agreement-id",
                "X-Custom", "ignored"));

        var request = supplier.apply(contextApi, createDataAddress());

        assertThat(request.getProperties())
                .containsEntry("forwarded-header:Mcp-Session-Id", "session-1")
                .containsEntry("forwarded-header:MCP-Protocol-Version", "2025-06-18")
                .doesNotContainKeys("forwarded-header:Authorization", "forwarded-header:X-Custom");
    }

    @Test
    void verifyMapping_usesProxySource_whenAddressRelaysHeaders() {
        var contextApi = mock(ContainerRequestContextApi.class);
        when(contextApi.method()).thenReturn(HttpMethod.POST);
        when(contextApi.path()).thenReturn("mcp");
        var address = DataAddress.Builder.newInstance()
                .type("HttpData")
                .property("baseUrl", "http://mcp.example.com")
                .property(EDC_NAMESPACE + "proxyHeaders", "true")
                .build();

        var request = supplier.apply(contextApi, address);

        assertThat(request.getSourceDataAddress().getType()).isEqualTo(ProxyHttpDataSourceFactory.TYPE);
        assertThat(request.getSourceDataAddress().getStringProperty("baseUrl")).isEqualTo("http://mcp.example.com");
    }

    @Test
    void verifyMapping_keepsSourceType_whenAddressDoesNotRelayHeaders() {
        var contextApi = mock(ContainerRequestContextApi.class);
        when(contextApi.method()).thenReturn(HttpMethod.GET);

        var request = supplier.apply(contextApi, DataAddress.Builder.newInstance().type("HttpData").build());

        assertThat(request.getSourceDataAddress().getType()).isEqualTo("HttpData");
    }
}
