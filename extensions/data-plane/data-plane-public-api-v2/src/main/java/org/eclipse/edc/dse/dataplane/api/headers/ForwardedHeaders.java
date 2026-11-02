package org.eclipse.edc.dse.dataplane.api.headers;

import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.types.domain.DataAddress;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Allow-list of the headers the data plane proxy relays, for data addresses that set {@link #PROXY_HEADERS}
 * to {@code true}.
 * <p>
 * Request headers are carried from the public API to the data source as {@code DataFlowStartMessage} properties
 * prefixed with {@link #PROPERTY_PREFIX}, then applied by {@link ForwardedHeadersParamsDecorator}. Response
 * headers, together with the response status, are read by {@link ProxyHttpDataSource}. Headers are matched
 * case-insensitively and relayed under the spelling of the allow-list.
 */
public final class ForwardedHeaders {

    public static final String PROPERTY_PREFIX = "forwarded-header:";
    public static final String PROXY_HEADERS = "proxyHeaders";
    public static final String DEFAULT_ALLOWED = "Accept,MCP-Protocol-Version,Mcp-Session-Id,Last-Event-ID";
    public static final String DEFAULT_ALLOWED_RESPONSE = "Mcp-Session-Id";

    /**
     * Set by the provider public API on a response it relays from the data source (status and headers included).
     * The consumer hop relays a non-2xx answer of the provider only when this marker is present: any other error
     * comes from the provider connector itself and keeps being reported as before.
     */
    public static final String RELAYED_RESPONSE_HEADER = "X-Dse-Proxy-Relayed";

    /**
     * Internal data address property of the consumer hop (consumer data API to provider public API).
     */
    public static final String CONSUMER_HOP = "proxyHeadersConsumerHop";

    /**
     * Headers that are either handled by the proxy itself (authorization, content type, body framing) or
     * hop-by-hop, and therefore never relayed whatever the configuration says.
     */
    static final Set<String> NEVER_FORWARDED = Set.of(
            "authorization", "proxy-authorization", "proxy-authenticate", "cookie", "set-cookie", "contract-id",
            RELAYED_RESPONSE_HEADER.toLowerCase(Locale.ROOT),
            "host", "content-type", "content-length", "transfer-encoding", "connection", "keep-alive",
            "te", "trailer", "upgrade");

    private final Map<String, String> allowed;

    private ForwardedHeaders(Map<String, String> allowed) {
        this.allowed = allowed;
    }

    /**
     * Parse a comma-separated allow-list, e.g. {@code "Accept,Mcp-Session-Id"}. Blank entries are ignored.
     *
     * @throws EdcException if the list contains a header that is never relayed.
     */
    public static ForwardedHeaders of(String commaSeparated) {
        var names = Optional.ofNullable(commaSeparated).stream()
                .flatMap(value -> Arrays.stream(value.split(",")))
                .map(String::trim)
                .filter(name -> !name.isEmpty())
                .toList();

        var rejected = names.stream().filter(name -> NEVER_FORWARDED.contains(lowerCase(name))).toList();
        if (!rejected.isEmpty()) {
            throw new EdcException("Headers that cannot be forwarded by the data plane proxy: " + rejected);
        }

        var allowed = names.stream()
                .collect(Collectors.toMap(ForwardedHeaders::lowerCase, name -> name, (first, second) -> first, LinkedHashMap::new));
        return new ForwardedHeaders(allowed);
    }

    /**
     * Whether a data address opts in to header and status relaying.
     */
    public static boolean isEnabled(DataAddress address) {
        return Boolean.parseBoolean(String.valueOf(address.getProperty(PROXY_HEADERS)));
    }

    public static ForwardedHeaders none() {
        return new ForwardedHeaders(Map.of());
    }

    public boolean isAllowed(String name) {
        return allowed.containsKey(lowerCase(name));
    }

    /**
     * Keep the allowed headers of an incoming request.
     *
     * @param headers incoming request headers, one value per name.
     * @return allowed headers, keyed by their allow-list spelling.
     */
    public Map<String, String> select(Map<String, String> headers) {
        var selected = new LinkedHashMap<String, String>();
        Optional.ofNullable(headers).orElse(Map.of()).forEach((name, value) -> {
            var canonical = allowed.get(lowerCase(name));
            if (canonical != null && value != null) {
                selected.put(canonical, value);
            }
        });
        return selected;
    }

    private static String lowerCase(String name) {
        return name.toLowerCase(Locale.ROOT);
    }
}
