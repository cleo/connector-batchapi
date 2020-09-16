package com.cleo.labs.connector.batchapi.processor;

import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
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

public class REST {

	public static String API_BASE = "/api";
	public static String AUTHENTICATION_URL = API_BASE+"/authentication";
	public static String CONNECTIONS_URL = API_BASE+"/connections";
	public static String AUTHENTICATORS_URL = API_BASE+"/authenticators";
	public static String ACTIONS_URL = API_BASE+"/actions";

	private String baseUrl;
	private String authToken;
	private boolean insecure;
	private boolean includeDefaults;
	private boolean traceRequests;

	private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

	public REST(String url, String username, String password, boolean insecure) throws Exception {
		this.baseUrl = url;
		this.insecure = insecure;
		this.authToken = authorize(username, password);
		this.includeDefaults = false;
		this.traceRequests = false;
	}

	public REST includeDefaults(boolean includeDefaults) {
	    this.includeDefaults = includeDefaults;
	    return this;
	}

	public REST traceRequests(boolean traceRequests) {
	    this.traceRequests = traceRequests;
	    return this;
	}

	private static HttpClient defaultHTTPClient = null;

	private HttpClient getDefaultHTTPClient() {
		if (defaultHTTPClient == null) {
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
			defaultHTTPClient = builder.build();
		}
		return defaultHTTPClient;
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
		HttpClient client = getDefaultHTTPClient();
		if (this.authToken != null) {
			request.addHeader("Authorization", "Bearer " + this.authToken);
		}
		try {
			HttpResponse response = client.execute(request);
			int code = response.getStatusLine().getStatusCode();
			String body = response.getEntity() == null ? null : EntityUtils.toString(response.getEntity());
			if (code == successCode) {
				return body==null ? null : (ObjectNode) mapper.readTree(body);
			} else {
				String message = String.format("Failed HTTP Request (%d)", code);
				if (body != null) {
                    message += ": " + Json.getSubElementAsText(mapper.readTree(body), "message");
				}
				throw new ProcessingException(message);
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

	public ObjectNode post(JsonNode json, JsonNode object) throws Exception {
	    return post(json, Json.getHref(object));
	}

	private ObjectNode post(JsonNode json, String href) throws Exception {
		HttpPost post = new HttpPost(this.baseUrl + href);
		post.setEntity(new StringEntity(json.toString()));
		post.addHeader("content-type", "application/json");
		if (traceRequests) {
		    System.err.println("POST "+href+":\n"+Json.mapper.valueToTree(json).toPrettyString());
		}
		return execute(post, 201);
	}

	public ObjectNode put(JsonNode json, JsonNode object) throws Exception {
	    return put(json, Json.getHref(object));
	}

	private ObjectNode put(JsonNode json, String href) throws Exception {
		HttpPut put = new HttpPut(this.baseUrl + href);
		put.setEntity(new StringEntity(json.toString()));
		put.addHeader("content-type", "application/json");
		if (traceRequests) {
		    System.err.println("PUT "+href+":\n"+Json.mapper.valueToTree(json).toPrettyString());
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

	public class JsonCollection implements Iterator<ObjectNode> {
		private String path;
		private String filter;
		private HttpClient httpClient = getDefaultHTTPClient();
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
				uri.addParameter("count", "2");
				HttpGet httpGet = new HttpGet(uri.build());
				if (authToken != null) {
					httpGet.addHeader("Authorization", "Bearer " + authToken);
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
	    List<ObjectNode> users = getUsers("username eq \"" + username + "\"");
        if (users.size() > 0) {
            return users.get(0);
        }
        return null;
	}

	public List<ObjectNode> getUsers(String filter) throws Exception {
	    List<ObjectNode> list = new ArrayList<>();
		JsonCollection authenticators = new JsonCollection("/api/authenticators");
		while (authenticators.hasNext()) {
			ObjectNode authenticator = authenticators.next();
			JsonCollection users = new JsonCollection(Json.getSubElementAsText(authenticator, "_links.users.href"),
					filter);
			users.forEachRemaining(list::add);
			if (users.exception() != null) {
				throw users.exception();
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

	public void deleteActions(ObjectNode connection) throws Exception {
		Iterator<JsonNode> actions = connection.get("_links").get("actions").elements();
		while (actions.hasNext()) {
			JsonNode action = actions.next();
			delete(Json.getSubElementAsText(action, "href"));
		}
	}

}
