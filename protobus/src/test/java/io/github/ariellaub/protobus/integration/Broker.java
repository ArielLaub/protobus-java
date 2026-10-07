package io.github.ariellaub.protobus.integration;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Assumptions;

/**
 * The real broker the integration suites run against. They never default to one:
 * set PROTOBUS_TEST_AMQP_URL (and PROTOBUS_TEST_MGMT_URL for the management API),
 * for example to the docker-compose broker on 25672/25673.
 */
final class Broker {
    private Broker() {}

    static String amqpUrl() {
        String url = System.getenv("PROTOBUS_TEST_AMQP_URL");
        Assumptions.assumeTrue(url != null && !url.isEmpty(), "PROTOBUS_TEST_AMQP_URL is not set");
        return url;
    }

    private static final HttpClient http = HttpClient.newHttpClient();

    private static URI mgmt(String path) {
        String base = System.getenv("PROTOBUS_TEST_MGMT_URL");
        Assumptions.assumeTrue(base != null && !base.isEmpty(), "PROTOBUS_TEST_MGMT_URL is not set");
        return URI.create(base.replaceAll("/+$", "") + path);
    }

    private static String auth(URI uri) {
        String info = uri.getUserInfo() == null ? "guest:guest" : uri.getUserInfo();
        return "Basic " + Base64.getEncoder().encodeToString(info.getBytes(StandardCharsets.UTF_8));
    }

    private static HttpResponse<String> send(String method, String path) {
        URI uri = mgmt(path);
        URI bare = URI.create(uri.getScheme() + "://" + uri.getHost() + ":" + uri.getPort() + uri.getRawPath());
        try {
            return http.send(HttpRequest.newBuilder(bare).header("Authorization", auth(uri))
                    .method(method, HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            throw new IllegalStateException("management API: " + e, e);
        }
    }

    static void deleteQueue(String name) {
        send("DELETE", "/api/queues/%2f/" + URLEncoder.encode(name, StandardCharsets.UTF_8).replace("+", "%20"));
    }

    static void deleteExchange(String name) {
        send("DELETE", "/api/exchanges/%2f/" + URLEncoder.encode(name, StandardCharsets.UTF_8).replace("+", "%20"));
    }

    /** Delete a service's queues and exchanges: the main, retry, DLQ and event objects. */
    static void deleteService(String name) {
        for (String q : List.of(name, name + ".Retry", name + ".DLQ", name + ".Events", name + ".Events.Retry",
                name + ".Events.DLQ")) {
            deleteQueue(q);
        }
        for (String x : List.of(name + ".Retry.Exchange", name + ".Events.Retry.Exchange", name + ".Events.Redelivery")) {
            deleteExchange(x);
        }
    }

    /** Close every connection named protobus-java, as a broker restart or a network failure would. */
    static int closeConnections() {
        String body = send("GET", "/api/connections").body();
        Matcher m = Pattern.compile("\"name\":\"([^\"]+)\"").matcher(body);
        List<String> names = new ArrayList<>();
        Matcher cn = Pattern.compile("\"connection_name\":\"protobus-java\"").matcher(body);
        boolean any = cn.find();
        while (m.find()) names.add(m.group(1));
        int closed = 0;
        if (!any) return 0;
        for (String n : names) {
            if (!n.contains("->")) continue;
            HttpResponse<String> r = send("GET", "/api/connections/" + URLEncoder.encode(n, StandardCharsets.UTF_8)
                    .replace("+", "%20"));
            if (r.body().contains("\"connection_name\":\"protobus-java\"")) {
                send("DELETE", "/api/connections/" + URLEncoder.encode(n, StandardCharsets.UTF_8).replace("+", "%20"));
                closed++;
            }
        }
        return closed;
    }

    static int queueMessages(String queue) {
        String body = send("GET", "/api/queues/%2f/" + URLEncoder.encode(queue, StandardCharsets.UTF_8)).body();
        Matcher m = Pattern.compile("\"messages\":(\\d+)").matcher(body);
        return m.find() ? Integer.parseInt(m.group(1)) : 0;
    }
}
