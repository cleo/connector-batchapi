package com.cleo.labs.connector.batchapi.processor;

import java.net.HttpURLConnection;
import java.net.URI;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Strings;

public class ApiClient {

    public static String API_BASE = "/api";
    public static String AUTHENTICATION_URL = API_BASE+"/authentication";
    public static String CONNECTIONS_URL = API_BASE+"/connections";
    public static String AUTHENTICATORS_URL = API_BASE+"/authenticators";
    public static String ACTIONS_URL = API_BASE+"/actions";
    public static String CERTS_URL = API_BASE+"/certs";

    private String baseUrl;
    private String authToken;
    private boolean insecure;
    private boolean includeDefaults;
    private boolean traceRequests;

    private HttpClient httpClient = null;

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    public ApiClient(String url, String username, String password, boolean insecure) throws Exception {
        this.baseUrl = url;
        this.insecure = insecure;
        this.httpClient = getHttpClient();
        this.authToken = authorize(username, password);
        this.includeDefaults = false;
        this.traceRequests = false;
    }

    public ApiClient includeDefaults(boolean includeDefaults) {
        this.includeDefaults = includeDefaults;
        return this;
    }

    public ApiClient traceRequests(boolean traceRequests) {
        this.traceRequests = traceRequests;
        return this;
    }

    private HttpClient getHttpClient() {
        if (httpClient == null) {
            HttpClientBuilder builder = HttpClients.custom()
                    .setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build());
            if (insecure) {
                try {
                    builder.setSSLContext(new SSLContextBuilder().loadTrustMaterial(null, TrustAllStrategy.INSTANCE).build())
                            .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
                } catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException e) {
                    // connection will fail anyway, so just print a warning
                    System.err.println("warning: "+e.getMessage());
                }
            }
            httpClient = builder.build();
        }
        return httpClient;
    }

    /*------------------------------------------------------------------------*
     * Authorization                                                          *
     *------------------------------------------------------------------------*/

    private String authorize(String username, String password) throws Exception {
        HttpPost post = new HttpPost(this.baseUrl + AUTHENTICATION_URL);
        post.addHeader("content-type", "application/x-www-form-urlencoded");
        post.setEntity(new UrlEncodedFormEntity(Arrays.asList(new BasicNameValuePair("grant_type", "password"),
                new BasicNameValuePair("username", username), new BasicNameValuePair("password", password))));

        ObjectNode result = execute(post, 200);
        return result.get("access_token").asText();
    }

    /*------------------------------------------------------------------------*
     * Basic HTTP Operations                                                  *
     *------------------------------------------------------------------------*/

    private ObjectNode execute(HttpRequestBase request, int successCode) throws Exception {
        if (this.authToken != null) {
            request.addHeader("Authorization", "Bearer " + this.authToken);
        }
        try {
            HttpResponse response = httpClient.execute(request);
            int code = response.getStatusLine().getStatusCode();
            String body = response.getEntity() == null ? null : EntityUtils.toString(response.getEntity());
            if (code == successCode) {
                return body==null ? null : (ObjectNode) mapper.readTree(body);
            } else {
                String message = String.format("Failed HTTP Request (%d)", code);
                if (body != null) {
                    message += ": " + Json.getSubElementAsText(mapper.readTree(body), "message");
                }
                throw new UnexpectedCodeException(message, code);
            }
        } finally {
            request.reset();
        }
    }

    public ObjectNode get(JsonNode object) throws Exception {
        return get(Json.getHref(object));
    }

    public ObjectNode get(String href) throws Exception {
        HttpGet get = new HttpGet(this.baseUrl + href);
        if (traceRequests) {
            System.err.println("GET "+href);
        }
        return execute(get, 200);
    }

    public ObjectNode post(JsonNode entity, JsonNode object) throws Exception {
        return post(entity, Json.getHref(object));
    }

    private ObjectNode post(JsonNode entity, String href) throws Exception {
        return post(entity, new URI(this.baseUrl+href), 201);
    }

    private ObjectNode post(JsonNode entity, URI uri, int success) throws Exception {
        HttpPost post = new HttpPost(uri);
        post.addHeader("content-type", "application/json");
        if (entity != null && !entity.isMissingNode()) {
            post.setEntity(new StringEntity(entity.toString()));
        }
        if (traceRequests) {
            String href = uri.toString().replaceFirst("^.*?[^/](?=/[^/])", "");
            System.err.println("POST "+href+":\n"+(entity==null ? "" : entity.toPrettyString()));
        }
        return execute(post, success);
    }

    public ObjectNode put(JsonNode json, JsonNode object) throws Exception {
        return put(json, Json.getHref(object));
    }

    private ObjectNode put(JsonNode json, String href) throws Exception {
        return put(json, new URI(this.baseUrl+href));
    }

    private ObjectNode put(JsonNode entity, URI uri) throws Exception {
        HttpPut put = new HttpPut(uri);
        put.addHeader("content-type", "application/json");
        if (entity != null && !entity.isMissingNode()) {
            put.setEntity(new StringEntity(entity.toString()));
        }
        if (traceRequests) {
            String href = uri.toString().replaceFirst("^.*?[^/](?=/[^/])", "");
            System.err.println("PUT "+href+":\n"+(entity==null ? "" : entity.toPrettyString()));
        }
        return execute(put, 200);
    }

    public void delete(JsonNode object) throws Exception {
        delete(Json.getHref(object));
    }

    private void delete(String href) throws Exception {
        HttpDelete delete = new HttpDelete(this.baseUrl + href);
        if (traceRequests) {
            System.err.println("DELETE "+href);
        }
        execute(delete, 204);
    }

    /*------------------------------------------------------------------------*
     * VersaLex API Collections                                               *
     *------------------------------------------------------------------------*/

    public class JsonCollection implements Iterator<ObjectNode>, Iterable<ObjectNode> {
        private String path;
        private String filter;
        private int totalResults = -1;
        private int startIndex;
        private ArrayNode resources;
        private int index;
        private Exception exception = null;

        public JsonCollection(String path) {
            this(path, null);
        }

        public JsonCollection(String path, String filter) {
            this.path = path;
            this.filter = filter;
            startIndex = 0;
            fill();
        }

        private void fill() {
            try {
                URIBuilder uri = new URIBuilder(baseUrl + path).addParameter("startIndex", String.valueOf(startIndex));
                if (!Strings.isNullOrEmpty(filter)) {
                    uri.addParameter("filter", filter);
                }
                uri.addParameter("count", "100");
                HttpGet httpGet = new HttpGet(uri.build());
                if (authToken != null) {
                    httpGet.addHeader("Authorization", "Bearer " + authToken);
                }
                if (traceRequests) {
                    System.err.println("GET "+uri.toString().replaceFirst("^.*?[^/](?=/[^/])", ""));
                }
                HttpResponse response = httpClient.execute(httpGet);
                int responseCode = response.getStatusLine().getStatusCode();
                String responseBody = response.getEntity() == null ? null : EntityUtils.toString(response.getEntity());
                if (responseCode != 200) {
                    String msg = String.format("Failed HTTP Request (%d)", responseCode);
                    if (responseBody != null) {
                        try {
                            msg += ": " + ((ObjectNode) mapper.readTree(EntityUtils.toString(response.getEntity()))
                                    .get("message")).asText();
                        } catch (Exception e) {
                            // leave it just as a code
                        }
                    }
                    throw new Exception(msg);
                }
                ObjectNode responseJson = (ObjectNode) mapper.readTree(responseBody);
                totalResults = Json.asInt(responseJson.get("totalResults"));
                resources = (ArrayNode) responseJson.get("resources");
                startIndex += resources.size();
                index = 0;
            } catch (Exception e) {
                exception = e;
            }
        }

        @Override
        public boolean hasNext() {
            if (exception == null && totalResults > 0 && index >= resources.size() && startIndex < totalResults) {
                fill();
            }
            return exception == null && totalResults > 0 && index < resources.size();
        }

        @Override
        public ObjectNode next() {
            if (!hasNext()) {
                return null;
            }
            return (ObjectNode) resources.get(index++);
        }

        public int totalResults() {
            return this.totalResults;
        }

        public Exception exception() {
            return this.exception;
        }

        public void throwException() throws Exception {
            if (exception != null) {
                throw exception;
            }
        }

        @Override
        public Iterator<ObjectNode> iterator() {
            return this;
        }
    }

    /*------------------------------------------------------------------------*
     * Specific Resource Operations                                           *
     *------------------------------------------------------------------------*/

    public ObjectNode createConnection(JsonNode connectionJson) throws Exception {
        return post(connectionJson, CONNECTIONS_URL);
    }

    public ObjectNode createAuthenticator(JsonNode authenticatorJson) throws Exception {
        return post(authenticatorJson, AUTHENTICATORS_URL);
    }

    public ObjectNode createUser(ObjectNode userJson, ObjectNode authenticator) throws Exception {
        return post(userJson, Json.getSubElementAsText(authenticator, "_links.users.href"));
    }

    public ObjectNode createAction(JsonNode actionJson) throws Exception {
        return post(actionJson, ACTIONS_URL);
    }

    public ObjectNode importOrGetCert(JsonNode certJson) throws Exception {
        CertUtils.CertWithBundle bundle;
        try {
            bundle = CertUtils.cert(Json.getSubElementAsText(certJson, "certificate"));
        } catch (Exception e) {
            throw new ProcessingException("unable to parse certificate", e);
        }
        if (bundle.cert() == null) {
            throw new ProcessingException("unable to parse certificate");
        }
        String base64 = CertUtils.base64(bundle.cert());
        ObjectNode importCert = Json.mapper.createObjectNode();
        importCert.put("requestType", "importCert");
        importCert.put("import", base64);
        ObjectNode result;
        try {
            result = post(importCert, CERTS_URL);
        } catch (UnexpectedCodeException e) {
            if (e.code() == HttpURLConnection.HTTP_CONFLICT) {
                String serial = bundle.cert().getSerialNumber().toString(16);
                JsonCollection certs = new JsonCollection(CERTS_URL, "serialNumber eq \""+serial+"\"");
                for (ObjectNode c : certs) {
                    if (c.path("hasPrivateKey").asBoolean()) {
                        continue;
                    }
                    if (base64.equals(Json.getSubElementAsText(c, "certificate"))) {
                        result = c;
                        break;
                    }
                }
            }
            throw e;
        }
        // now import the bundle, if any, ignoring conflict exceptions
        for (X509Certificate cert : bundle.bundle()) {
            importCert.removeAll();
            importCert.put("requestType", "importCert");
            importCert.put("import", CertUtils.base64(cert));
            try {
                post(importCert, CERTS_URL);
            } catch (UnexpectedCodeException e) {
                if (e.code() != HttpURLConnection.HTTP_CONFLICT) {
                    throw e;
                }
            }
        }
        // return result
        return result;
    }

    private ObjectNode getResource(String resourcePath, String keyAttribute, String key) throws Exception {
        List<ObjectNode> list = getResources(resourcePath, keyAttribute + " eq \"" + key + "\"");
        if (list != null && !list.isEmpty()) {
            return list.get(0);
        }
        return null;
    }

    private List<ObjectNode> getResources(String resourcePath, String filter) throws Exception {
        JsonCollection resources = new JsonCollection("/api/" + resourcePath, filter);
        List<ObjectNode> list = new ArrayList<>();
        resources.forEachRemaining(list::add);
        resources.throwException();
        return list;
    }

    public ObjectNode getAuthenticator(String alias) throws Exception {
        return getResource("authenticators", "alias", alias);
    }

    public List<ObjectNode> getAuthenticators(String filter) throws Exception {
        return getResources("authenticators", filter);
    }

    public ObjectNode getUser(String username) throws Exception {
        return getUser(null, username);
    }

    public ObjectNode getUser(String authfilter, String username) throws Exception {
        List<ObjectNode> users = getUsers(authfilter, "username eq \"" + username + "\"", true);
        if (users.size() > 0) {
            return users.get(0);
        }
        return null;
    }

    public List<ObjectNode> getUsers(String authfilter, String filter) throws Exception {
        return getUsers(authfilter, filter, false);
    }

    public List<ObjectNode> getUsers(String authfilter, String filter, boolean stopAtOne) throws Exception {
        List<ObjectNode> list = new ArrayList<>();
        List<ObjectNode> authenticators = getAuthenticators(authfilter);
        for (ObjectNode authenticator : authenticators) {
            JsonCollection users = new JsonCollection(Json.getSubElementAsText(authenticator, "_links.users.href"),
                    filter);
            users.forEachRemaining(list::add);
            if (users.exception() != null) {
                throw users.exception();
            }
            if (stopAtOne && !list.isEmpty()) {
                break;
            }
        }
        return list;
    }

    public ObjectNode getConnection(String alias) throws Exception {
        String resourcePath = "connections";
        if (includeDefaults) {
            resourcePath += "?includeDefaults=true";
        }
        return getResource(resourcePath, "alias", alias);
    }

    public List<ObjectNode> getConnections(String filter) throws Exception {
        String resourcePath = "connections";
        if (includeDefaults) {
            resourcePath += "?includeDefaults=true";
        }
        return getResources(resourcePath, filter);
    }

    public List<ObjectNode> getActions(String filter) throws Exception {
        return getResources("actions", filter);
    }

    public ObjectNode runAction(ObjectNode action) throws Exception {
        return runAction(action, null);
    }

    public ObjectNode runAction(ObjectNode action, ObjectNode options) throws Exception {
        String path = Json.getSubElementAsText(action, "_links.run.href");
        String timeout = Json.getSubElementAsText(options, "timeout");
        String messagesCount = Json.getSubElementAsText(options, "messagesCount");
        URIBuilder uri = new URIBuilder(baseUrl + path);
        if (!Strings.isNullOrEmpty(timeout)) {
            uri.addParameter("timeout", timeout);
        }
        if (!Strings.isNullOrEmpty(messagesCount)) {
            uri.addParameter("messagesCount", messagesCount);
        }
        return post(null, uri.build(), 200);
    }

    public void deleteActions(ObjectNode connection) throws Exception {
        Iterator<JsonNode> actions = connection.get("_links").get("actions").elements();
        while (actions.hasNext()) {
            JsonNode action = actions.next();
            delete(Json.getSubElementAsText(action, "href"));
        }
    }

}
