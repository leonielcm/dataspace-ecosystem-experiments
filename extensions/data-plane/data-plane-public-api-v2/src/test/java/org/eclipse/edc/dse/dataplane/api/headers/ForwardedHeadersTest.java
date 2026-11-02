package org.eclipse.edc.dse.dataplane.api.headers;

import org.eclipse.edc.spi.EdcException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class ForwardedHeadersTest {

    @Test
    void select_shouldMatchCaseInsensitivelyAndUseAllowListSpelling() {
        var forwardedHeaders = ForwardedHeaders.of("Mcp-Session-Id, MCP-Protocol-Version");

        var selected = forwardedHeaders.select(Map.of(
                "mcp-session-id", "session-1",
                "mcp-protocol-version", "2025-06-18",
                "X-Other", "value"));

        assertThat(selected).containsExactlyInAnyOrderEntriesOf(Map.of(
                "Mcp-Session-Id", "session-1",
                "MCP-Protocol-Version", "2025-06-18"));
    }

    @Test
    void select_shouldReturnEmpty_whenNoHeaders() {
        assertThat(ForwardedHeaders.of(ForwardedHeaders.DEFAULT_ALLOWED).select(null)).isEmpty();
    }

    @Test
    void of_shouldIgnoreBlankEntries() {
        var forwardedHeaders = ForwardedHeaders.of(" , Accept,, ");

        assertThat(forwardedHeaders.isAllowed("accept")).isTrue();
        assertThat(forwardedHeaders.isAllowed("")).isFalse();
    }

    @Test
    void none_shouldAllowNothing() {
        assertThat(ForwardedHeaders.none().select(Map.of("Accept", "application/json"))).isEmpty();
        assertThat(ForwardedHeaders.of("")).satisfies(headers -> assertThat(headers.isAllowed("Accept")).isFalse());
    }

    @ParameterizedTest
    @ValueSource(strings = { "Authorization", "content-type", "Host", "Contract-Id", "Transfer-Encoding" })
    void of_shouldRejectHeadersThatAreNeverForwarded(String header) {
        assertThatExceptionOfType(EdcException.class)
                .isThrownBy(() -> ForwardedHeaders.of("Accept," + header))
                .withMessageContaining(header);
    }
}
