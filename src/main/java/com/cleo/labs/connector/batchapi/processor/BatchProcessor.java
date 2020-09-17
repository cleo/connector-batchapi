package com.cleo.labs.connector.batchapi.processor;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.cleo.labs.connector.batchapi.processor.template.TemplateExpander;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;

public class BatchProcessor {
    public enum Option {generatePass};
    public BatchProcessor set(Option o) {
        return set(o, true);
    }
    public BatchProcessor set(Option o, boolean value) {
        if (value) {
            options.add(o);
        } else {
            options.remove(o);
        }
        return this;
    }
    public BatchProcessor setExportPassword(String exportPassword) {
        this.exportPassword = exportPassword;
        return this;
    }

    public enum Operation {
        add ("created"),
        list ("found"),
        update ("updating"),
        delete ("deleted"),
        preview ("previewed");

        private String tag;
        private Operation(String tag) {
            this.tag = tag;
        }
        public String tag() {
            return tag;
        }
    };

    public BatchProcessor setDefaultOperation(Operation defaultOperation) {
        if (defaultOperation == null) {
            throw new IllegalArgumentException("defaultOperation may not be null");
        }
        this.defaultOperation = defaultOperation;
        return this;
    }

    public BatchProcessor setTemplate(Path template) {
        File file = template.toFile();
        if (!file.exists()) {
            throw new IllegalArgumentException("template file not found: "+template);
        } else if (!file.isFile()) {
            throw new IllegalArgumentException("template file is not a file: "+template);
        }
        this.template = template;
        return this;
    }

    private REST api;
    private EnumSet<Option> options;
    private String exportPassword;
    private Operation defaultOperation;
    private Path template;

    public enum ResourceClass {
        user ("username"),
        authenticator ("authenticator"),
        connection ("connection");

        private String tag;
        private ResourceClass(String tag) {
            this.tag = tag;
        }
        public String tag() {
            return tag;
        }
    };

    public enum AuthenticatorType {nativeUser, systemLdap, authConnector, authenticator};
    private static final Set<String> AUTH_TYPES = EnumSet.allOf(AuthenticatorType.class)
            .stream()
            .map(AuthenticatorType::name)
            .collect(Collectors.toSet());

    private Map<String, ObjectNode> authenticatorCache = new HashMap<>();

    private void createActions(ObjectNode actions, ObjectNode resource) throws Exception {
        if (actions != null && actions.size() > 0) {
            ObjectNode updated = (ObjectNode)resource.get("actions");
            if (updated == null) {
                updated = Json.mapper.createObjectNode();
            } else {
                updated = updated.deepCopy();
            }
            String type = Json.getSubElementAsText(resource, "meta.resourceType");
            Iterator<JsonNode> elements = actions.elements();
            while (elements.hasNext()) {
                ObjectNode action = (ObjectNode) elements.next();
                if (action != null && action.isObject()) {
                    String actionName = Json.getSubElementAsText(action, "alias", "");
                    if (!actionName.isEmpty() && !actionName.equals("NA")) {
                        action.put("enabled", true);
                        action.put("type", "Commands");
                        switch (type) {
                        case "user":
                            action.putObject("authenticator")
                                    .put("href", Json.getSubElementAsText(resource, "_links.authenticator.href"))
                                    .putObject("user").put("href", Json.getHref(resource));
                            break;
                        case "authenticator":
                            action.putObject("authenticator").put("href", Json.getHref(resource));
                            break;
                        default:
                        }
                        action.putObject("connection").put("href", Json.getHref(resource));
                        String schedule = Json.getSubElementAsText(action, "schedule", "");
                        if (schedule.isEmpty() || schedule.equals("none") || schedule.equals("no")) {
                            action.remove("schedule");
                        }
                        Operation operation = Operation.valueOf(Json.asText(action.remove("operation"), "add"));
                        JsonNode existing = updated.get(actionName);
                        if (existing == null) {
                            if (!operation.equals(Operation.delete)) {
                                ObjectNode newAction = api.createAction(action);
                                updated.set(actionName, newAction);
                            }
                        } else {
                            if (operation.equals(Operation.delete)) {
                                api.delete(existing);
                                updated.remove(actionName);
                            } else {
                                ObjectNode newAction = api.put(actions, existing);
                                updated.replace(actionName, newAction);
                            }
                        }
                    }
                }
            }
            if (updated.size() > 0) {
                resource.set("actions", updated);
            } else {
                resource.remove("actions");
            }
        }
    }

    private static final String UPPER = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final String LOWER = "abcdefghijklmnopqrstuvwxyz";
    private static final String DIGIT = "0123456789";
    private static final String SEPARATOR = "-=:|+_/.";
    private static class RandomBuilder {
        private SecureRandom random = new SecureRandom();
        private StringBuilder s = new StringBuilder();
        public RandomBuilder add(String set, int n) {
            random.ints(0, set.length()).limit(n).mapToObj(set::charAt).forEach(s::append);
            return this;
        }
        @Override
        public String toString() {
            return s.toString();
        }
    }
    private static String generatePassword() {
        // formula: 5xupper sep 5xdigit sep 5xlower sep 5xdigit
        return new RandomBuilder()
                .add(UPPER, 5)
                .add(SEPARATOR, 1)
                .add(DIGIT, 5)
                .add(SEPARATOR, 1)
                .add(LOWER, 5)
                .add(SEPARATOR, 1)
                .add(DIGIT, 5)
                .toString();
    }

    private ObjectNode generatedPassword(String authenticator, ObjectNode entry) {
        //   alias: authenticator
        //   username: username
        //   email: email
        //   password: encrypted password
        String password = Json.getSubElement(entry, "accept.password").asText();
        String encrypted = Strings.isNullOrEmpty(exportPassword)
                ? password
                : OpenSSLCrypt.encrypt(exportPassword, password);
        ObjectNode result = Json.mapper.createObjectNode();
        result.put("authenticator", authenticator);
        result.put("username", entry.get("username").asText());
        result.put("email", entry.get("email").asText());
        result.put("password", encrypted);
        return result;
    }

    private ObjectNode updateResource(ObjectNode object, ObjectNode updates) throws Exception {
        String type = Json.getSubElementAsText(object, "meta.resourceType", "");
        ObjectNode updated = object.deepCopy();
        if (type.equals("user")) {
            updated.remove("authenticator"); // this is used as the authenticator alias, which isn't present in the real API
            updates.remove("authenticator");
        }
        JsonNode actions = updated.remove("actions");
        updated.setAll(updates);
        cleanup(updated);
        ObjectNode result = api.put(updated, object);
        if (actions != null) {
            result.set("actions", actions);
        }
        return result;
    }

    private static ObjectNode loadTemplate(String template) throws Exception {
        return (ObjectNode) Json.mapper.readTree(Resources.toString(Resources.getResource(template), Charsets.UTF_8));
    }

    private ObjectNode getAuthenticator(String alias) throws Exception {
        ObjectNode authenticator;
        if (authenticatorCache.containsKey(alias)) {
            authenticator = authenticatorCache.get(alias);
        } else {
            authenticator = api.getAuthenticator(alias);
            if (authenticator != null) {
                authenticator.set("authenticator", authenticator.remove("alias"));
                authenticatorCache.put(alias, authenticator);
            }
        }
        return authenticator;
    }

    private String getObjectName(ObjectNode object) {
        String alias = Json.getSubElementAsText(object, "username");
        if (alias == null) {
            alias = Json.getSubElementAsText(object, "authenticator");
            if (alias == null) {
                alias = Json.getSubElementAsText(object, "connection");
            }
        }
        return alias;
    }

    private ObjectNode createAuthenticatorFromTemplate(String alias) throws Exception {
        ObjectNode authTemplate = loadTemplate("template_authenticator.yaml");
        authTemplate.put("alias", alias);
        ObjectNode authenticator = api.createAuthenticator(authTemplate);
        if (authenticator == null) {
            throw new ProcessingException("authenticator not created");
        }
        authenticator.set("authenticator", authenticator.remove("alias"));
        authenticatorCache.put(alias, authenticator);
        return authenticator;
    }

    private ObjectNode normalizeActions(JsonNode actions) throws Exception {
        if (actions != null && actions.isArray()) {
            // convert from [ {alias=x, ...}, {alias=y, ...} ] to { x:{alias=x, ...},
            // y:{alias=y, ...} }
            ObjectNode map = Json.mapper.createObjectNode();
            Iterator<JsonNode> elements = ((ArrayNode) actions).elements();
            while (elements.hasNext()) {
                JsonNode action = elements.next();
                String alias = Json.getSubElementAsText(action, "alias");
                if (alias == null) {
                    throw new ProcessingException("action found with missing alias");
                }
                map.set(alias, action);
            }
            actions = map;
        }
        return (ObjectNode) actions;
    }

	/*------------------------------------------------------------------------*
	 * Get/List Operations                                                    *
	 *------------------------------------------------------------------------*/

    private ObjectNode injectActions(ObjectNode resource) throws Exception {
        JsonNode actionlinks = resource.path("_links").path("actions");
        if (!actionlinks.isMissingNode()) {
            ObjectNode actions = Json.mapper.createObjectNode();
            Iterator<JsonNode> elements = actionlinks.elements();
            while (elements.hasNext()) {
                JsonNode actionlink = elements.next();
                ObjectNode action = api.get(Json.getSubElementAsText(actionlink, "href"));
                actions.set(Json.getSubElementAsText(action, "alias"), action);
            }
            if (actions.size() > 0) {
                resource.set("actions", actions);
            }
        }
        return resource;
    }

    private List<ObjectNode> listUsers(Request request) throws Exception {
        List<ObjectNode> list = api.getUsers(request.resourceFilter);
        if (list.isEmpty()) {
            throw new ProcessingException("filter \""+request.resourceFilter+"\" returned no users");
        }
        for (int i = 0; i<list.size(); i++) {
            list.set(i, listUser(list.get(i)));
        }
        return list;
    }

    private ObjectNode listUser(Request request) throws Exception {
        ObjectNode user = api.getUser(request.resource);
        if (user == null) {
            throw new ProcessingException("user "+request.resource+" not found");
        }
        return listUser(user);
    }

    private ObjectNode listUser(ObjectNode user) throws Exception {
        injectActions(user);
        // inject Authenticator
        JsonNode authenticatorlink = user.path("_links").path("authenticator");
        if (!authenticatorlink.isMissingNode()) {
            ObjectNode authenticator = api.get(Json.getSubElementAsText(authenticatorlink, "href"));
            String alias = Json.getSubElementAsText(authenticator, "alias");
            if (alias != null) {
                // set host, but reorder things to get it near the top
                ObjectNode update = Json.mapper.createObjectNode();
                update.set("id", user.get("id"));
                update.set("username", user.get("username"));
                update.set("email",  user.get("email"));
                update.put("authenticator", alias);
                update.setAll(user);
                user = update;
            }
        }
        return user;
    }

    private List<ObjectNode> listAuthenticators(Request request, boolean includeUsers) throws Exception {
        List<ObjectNode> list = api.getAuthenticators(request.resource);
        if (list.isEmpty()) {
            throw new ProcessingException("filter \""+request.resourceFilter+"\" returned no authenticators");
        }
        for (ObjectNode authenticator : list) {
            authenticator.set("authenticator", authenticator.remove("alias"));
            injectActions(authenticator);
            // collect users, if requested
            if (includeUsers) {
                List<ObjectNode> userlist = new ArrayList<>();
                String userlink = Json.getSubElementAsText(authenticator, "_links.users.href");
                REST.JsonCollection users = api.new JsonCollection(userlink);
                while (users.hasNext()) {
                    ObjectNode user = users.next();
                    user = listUser(user);
                    userlist.add(user);
                }
                authenticator.putArray("users").addAll(userlist);
            }
        }
        return list;
    }

    private ObjectNode listAuthenticator(Request request, boolean includeUsers) throws Exception {
        ObjectNode authenticator = api.getAuthenticator(request.resource);
        if (authenticator == null) {
            throw new ProcessingException("authenticator "+request.resource+" not found");
        }
        authenticator.set("authenticator", authenticator.remove("alias"));
        injectActions(authenticator);
        // collect users, if requested
        if (includeUsers) {
            List<ObjectNode> userlist = new ArrayList<>();
            String userlink = Json.getSubElementAsText(authenticator, "_links.users.href");
			REST.JsonCollection users = api.new JsonCollection(userlink);
			while (users.hasNext()) {
			    ObjectNode user = users.next();
			    user = listUser(user);
			    userlist.add(user);
			}
			authenticator.putArray("users").addAll(userlist);
        }
        return authenticator;
    }

    private List<ObjectNode> listConnections(Request request) throws Exception {
        List<ObjectNode> list = api.getConnections(request.resourceFilter);
        if (list.isEmpty()) {
            throw new ProcessingException("filter \""+request.resourceFilter+"\" returned no connections");
        }
        for (int i = 0; i<list.size(); i++) {
            list.get(i).set("connection", list.get(i).remove("alias"));
            list.set(i, listConnection(list.get(i)));
        }
        return list;
    }

    private ObjectNode listConnection(Request request) throws Exception {
        ObjectNode connection = api.getConnection(request.resource);
        if (connection == null) {
            throw new ProcessingException("connection "+request.resource+" not found");
        }
        connection.set("connection", connection.remove("alias"));
        return listConnection(connection);
    }

    private ObjectNode listConnection(ObjectNode connection) throws Exception {
        return injectActions(connection);
    }

	/*------------------------------------------------------------------------*
	 * Cleanups -- Remove Metadata and Defaults                               *
	 *------------------------------------------------------------------------*/

    private ObjectNode cleanupActions(ObjectNode resource) {
        ObjectNode actions = (ObjectNode)resource.get("actions");
        if (actions != null) {
            Iterator<JsonNode> elements = actions.elements();
            while (elements.hasNext()) {
                ObjectNode action = (ObjectNode)elements.next();
                Json.removeElements(action,
                        "active",
                        "editable",
                        "runnable",
                        "running",
                        "ready",
                        "enabled=true",
                        "type=Commands",
                        "authenticator",
                        "connection",
                        "meta",
                        "_links");
            }
        }
        return resource;
    }

    private ObjectNode cleanupUser(ObjectNode user) {
        Json.removeElements(user,
                "active",
                "editable",
                "runnable",
                "ready",
                "enabled=true",
                "home.dir.default",
                "home.subfolders.default",
                "accept.lastPasswordReset",
                "accept.sftp.auth=[{\"type\":\"userPwd\"}]",
                "outgoing.partnerPackaging=false",
                "incoming.partnerPackaging=false",
                "meta",
                "_links");
        cleanupActions(user);
        return user;
    }

    private ObjectNode cleanupAuthenticator(ObjectNode authenticator) {
        Json.removeElements(authenticator,
                "active",
                "editable",
                "runnable",
                "ready",
                "enabled=true",
                "home.enabled=true",
                "home.dir.default",
                "home.subfolders.default=---\n- usage: download\n  path: inbox\\\n- usage: upload\n  path: outbox\\\n",
                "home.access=file",
                "privileges.transfers.view=true",
                "privileges.unify.enabled=false",
                "privileges.invitations.enabled=false",
                "privileges.twoFactorAuthentication.enabled=false",
                "incoming.filters.fileNamesPattern=\"*\"",
                "accept.security.requireIPFilter=false",
                "accept.security.passwordRules.enforce=false",
                "accept.security.passwordRules.minLength=8",
                "accept.security.passwordRules.cannotContainUserName=true",
                "accept.security.passwordRules.minUpperChars=1",
                "accept.security.passwordRules.minLowerChars=1",
                "accept.security.passwordRules.minNumericChars=1",
                "accept.security.passwordRules.minSpecialChars=1",
                "accept.security.passwordRules.noRepetitionCount=3",
                "accept.security.passwordRules.requirePasswordResetBeforeFirstUse=false",
                "accept.security.passwordRules.expiration.enabled=true",
                "accept.security.passwordRules.expiration.expiresDays=60",
                "accept.security.passwordRules.lockout.enabled=false",
                "accept.security.passwordRules.lockout.afterFailedAttempts=5",
                "accept.security.passwordRules.lockout.withinSeconds=60",
                "accept.security.passwordRules.lockout.lockoutMinutes=15",
                "accept.ftp.enabled=true",
                "accept.ftp.passiveModeUseExternalIP=false",
                "accept.ftp.autoDeleteDownloadedFile=false",
                "accept.ftp.activeModeSourcePort=0",
                "accept.ftp.ignoreDisconnectWithoutQuit=false",
                "accept.ftp.triggerAtUpload=false",
                "accept.sftp.enabled=true",
                "accept.sftp.prefixHome=false",
                "accept.http.enabled=true",
                "accept.requireSecurePort=false",
                "meta",
                "_links");
        cleanupActions(authenticator);
        return authenticator;
    }

    private ObjectNode cleanupConnection(ObjectNode connection) {
        // TODO: this may need more work, but maybe not
        Json.removeElements(connection,
                "active",
                "editable",
                "runnable",
                "ready",
                "enabled=true",
                "meta",
                "_links");
        cleanupActions(connection);
        return connection;
    }

    private ObjectNode cleanup(ObjectNode resource) {
        String type = Json.getSubElementAsText(resource, "meta.resourceType", "");
        switch (type) {
        case "user":
            return cleanupUser(resource);
        case "authenticator":
            return cleanupAuthenticator(resource);
        case "connection":
            return cleanupConnection(resource);
        default:
            return resource;
        }
    }

    private ArrayNode cleanup(ArrayNode list) {
        Iterator<JsonNode> elements = list.elements();
        while (elements.hasNext()) {
            ObjectNode element = (ObjectNode)elements.next();
            cleanup(element);
        }
        return list;
    }

	/*------------------------------------------------------------------------*
	 * Main File Processor                                                    *
	 *------------------------------------------------------------------------*/

    /**
     * Adds a "result" object containing "status" (success or error) and
     * "message" fields to the top of an ObjectNode
     * @param node the node to modify
     * @param success {@code true} for "success" else "error"
     * @param message the (optional) "message" to add
     * @return the modified node
     */
    public static ObjectNode insertResult(ObjectNode node, boolean success, String message) {
        ObjectNode result = Json.setSubElement((ObjectNode)node.get("result"), "status", success ? "success" : "error");
        Json.setSubElement(result, "message", message);
        return (ObjectNode) ((ObjectNode)Json.mapper.createObjectNode().set("result", result)).setAll(node);
    }

    private static class StackTraceCapture extends PrintStream {
        private boolean first = true;
        private ArrayNode trace;
        public StackTraceCapture(ArrayNode trace) {
            super (ByteStreams.nullOutputStream());
            this.trace = trace;
        }
        public void print(String s) {
            if (first) {
                first = false;
            } else {
                trace.add(s.replaceAll("^\\t*", ""));
            }
        }
    }

    public static ObjectNode insertResult(ObjectNode node, boolean success, Exception e) {
        ObjectNode update = insertResult(node, success, e.getMessage());
        if (!(e instanceof ProcessingException)) {
            ArrayNode trace = Json.mapper.createArrayNode();
            e.printStackTrace(new StackTraceCapture(trace));
            ObjectNode result = (ObjectNode)update.get("result");
            result.set("trace", trace);
        }
        return update;
    }

    private ObjectNode passwordReport(ArrayNode passwords) {
        // create an object like:
        //   result:
        //     status: success
        //     message: generated passwords
        //     passwords:
        //     - alias: authenticator
        //       username: username
        //       email: email
        //       password: encrypted password
        ObjectNode report = insertResult(Json.mapper.createObjectNode(), true, "generated passwords");
        Json.setSubElement(report, "result.passwords", passwords);
        return report;
    }

	/*- add processors -------------------------------------------------------*/

    private ObjectNode processAddUser(Request request, ObjectNode actions, ArrayNode results, ArrayNode passwords) throws Exception {
        // get or create the authenticator identified by "authenticator"
        String alias = Json.asText(request.entry.remove("authenticator"));
        if (alias == null) {
            throw new ProcessingException("\"authenticator\" required when adding a user");
        }
        ObjectNode authenticator = getAuthenticator(alias);
        if (authenticator == null) {
            authenticator = createAuthenticatorFromTemplate(alias);
            results.add(insertResult(authenticator, true, String.format("created authenticator %s with default template", alias)));
        }
        // Create user
        if (options.contains(Option.generatePass)) {
            Json.setSubElement(request.entry, "accept.password", generatePassword());
        }
        ObjectNode user = api.createUser(request.entry, authenticator);
        if (user == null) {
            throw new ProcessingException("user not created");
        }
        if (options.contains(Option.generatePass)) {
            passwords.add(generatedPassword(alias, request.entry));
        }
        if (actions != null) {
            createActions(actions, user);
        }
        user.put("authenticator", alias);
        results.add(insertResult(user, true, "created "+request.resource));
        return user;
    }

    private ObjectNode processAddAuthenticator(Request request, ObjectNode actions, ArrayNode results) throws Exception {
        request.entry.set("alias", request.entry.remove("authenticator"));
        ObjectNode authenticator = api.createAuthenticator(request.entry);
        if (authenticator == null) {
            throw new ProcessingException("error: authenticator not created");
        }
        request.entry.set("authenticator", request.entry.remove("alias"));
        if (actions != null) {
            createActions(actions, authenticator);
        }
        results.add(insertResult(authenticator, true, "created "+request.resource));
        return authenticator;
    }

    private ObjectNode processAddConnection(Request request, ObjectNode actions, ArrayNode results) throws Exception {
        request.entry.set("alias", request.entry.remove("connection"));
        ObjectNode connection = api.createConnection(request.entry);
        if (connection == null) {
            throw new ProcessingException("error: connection not created");
        }
        connection.set("connection", connection.remove("alias"));
        api.deleteActions(connection);
        if (actions != null) {
            createActions(actions, connection);
        }
        results.add(insertResult(connection, true, "created "+request.resource));
        return connection;
    }

    private ObjectNode processAdd(Request request, ObjectNode actions, ArrayNode results, ArrayNode passwords) throws Exception {
        switch (request.resourceClass) {
        case user:
            return processAddUser(request, actions, results, passwords);
        case authenticator:
            return processAddAuthenticator(request, actions, results);
        case connection:
            return processAddConnection(request, actions, results);
        default:
            throw new ProcessingException("unrecognized request");
        }
    }

	/*- list processors ------------------------------------------------------*/

    private List<ObjectNode> processListUser(Request request, ArrayNode results) throws Exception {
        List<ObjectNode> list;
        if (request.resourceFilter != null) {
            list = listUsers(request);
            int i = 1;
            for (ObjectNode user : list) {
                String message = String.format("%s user %s (%d of %d)",
                        request.operation.tag(),
                        Json.getSubElementAsText(user, ResourceClass.user.tag()),
                        i++, list.size());
                results.add(insertResult(user, true, message));
            }
        } else {
            ObjectNode found = listUser(request);
            String message = String.format("%s user %s",
                    request.operation.tag(),
                    request.resource);
            results.add(insertResult(found, true, message));
            list = Arrays.asList(found);
        }
        return list;
    }

    private List<ObjectNode> processListAuthenticator(Request request, ArrayNode results, boolean includeUsers) throws Exception {
        List<ObjectNode> list;
        if (request.resourceFilter != null) {
            list = listAuthenticators(request, includeUsers);
            int i = 1;
            for (ObjectNode authenticator : list) {
                ArrayNode users = (ArrayNode)authenticator.remove("users");
                String message = includeUsers
                        ? String.format("%s authenticator %s (%d of %d)",
                            request.operation.tag(),
                            Json.getSubElementAsText(authenticator, ResourceClass.authenticator.tag()),
                            i, list.size())
                        : String.format("%s authenticator %s (%d of %d) with %d users",
                            request.operation.tag(),
                            Json.getSubElementAsText(authenticator, ResourceClass.authenticator.tag()),
                            i, list.size(), users.size());
                ObjectNode authenticatorResult = insertResult(authenticator, true, message);
                ArrayNode userResults = authenticatorResult.putArray("users");
                for (int j=0; j<users.size(); j++) {
                    message = String.format("%s authenticator %s (%d of %d): user %d of %d",
                            request.operation.tag(),
                            Json.getSubElementAsText(authenticator, ResourceClass.authenticator.tag()),
                            i, list.size(), j+1, users.size());
                    userResults.add(insertResult((ObjectNode)users.get(j), true, message));
                }
                results.add(authenticatorResult);
                i++;
            }
        } else {
            ObjectNode authenticator = listAuthenticator(request, includeUsers);
            ArrayNode users = (ArrayNode)authenticator.remove("users");
            String message = includeUsers
                    ? String.format("%s authenticator %s",
                        request.operation.tag(), request.resource)
                    : String.format("%s authenticator %s with %d users",
                        request.operation.tag(), request.resource, users.size());
            ObjectNode authenticatorResult = insertResult(authenticator, true, message);
            ArrayNode userResults = authenticatorResult.putArray("users");
            for (int i=0; i<users.size(); i++) {
                message = String.format("%s authenticator %s: user %d of %d",
                        request.operation.tag(),
                        request.resource, i+1, users.size());
                userResults.add(insertResult((ObjectNode)users.get(i), true, message));
            }
            results.add(authenticatorResult);
            list = Arrays.asList(authenticator);
        }
        return list;
    }

    private List<ObjectNode> processListConnection(Request request, ArrayNode results) throws Exception {
        List<ObjectNode> list;
        if (request.resourceFilter != null) {
            list = listConnections(request);
            int i = 1;
            for (ObjectNode connection : list) {
                String message = String.format("%s connection %s (%d of %d)",
                        request.operation.tag(),
                        Json.getSubElementAsText(connection, ResourceClass.connection.tag()),
                        i++, list.size());
                results.add(insertResult(connection, true, message));
            }
        } else {
            ObjectNode found = listConnection(request);
            String message = String.format("%s connection %s",
                    request.operation.tag(), request.resource);
            results.add(insertResult(found, true, message));
            list = Arrays.asList(found);
        }
        return list;
    }

    private List<ObjectNode> processList(Request request, ArrayNode results) throws Exception {
        switch (request.resourceClass) {
        case user:
            return processListUser(request, results);
        case authenticator:
            return processListAuthenticator(request, results,
                    request.operation.equals(Operation.list) || request.operation.equals(Operation.delete));
        case connection:
            return processListConnection(request, results);
        default:
            throw new ProcessingException("unrecognized request");
        }
    }

    private void appendAndFlattenUsers(JsonNode tempResult, ArrayNode results) {
        results.add(tempResult);
        ArrayNode users = (ArrayNode)((ObjectNode)tempResult).remove("users");
        if (users != null && !users.isEmpty()) {
            users.forEach(results::add);
        }
    }

    private void appendAndFlattenUsers(ArrayNode tempResults, ArrayNode results) {
        if (tempResults != null && !tempResults.isEmpty()) {
            tempResults.forEach(tempResult -> appendAndFlattenUsers(tempResult, results));
        }
    }

	/*- request analyzer -----------------------------------------------------*/

    public static class Request {
        public ResourceClass resourceClass = null;
        public String resourceType = null;
        public String resource = null;
        public String resourceFilter = null;
        public ObjectNode entry = null;
        public Operation operation = null;
    }

    public Request analyzeRequest(ObjectNode entry, Operation operation) throws ProcessingException {
        Request request = new Request();
        request.entry = entry;
        request.operation = operation;

        // see if there is a filter (and remove it from the entry)
        request.resourceFilter = Json.asText(entry.remove("filter"));
        boolean filterable = operation == Operation.list || operation == Operation.delete || operation == Operation.update;
        if (request.resourceFilter != null && !filterable) {
            throw new ProcessingException("\"filter\" valid only for list and delete");
        }

        // look for the resource name under username/authenticator/connection
        for (ResourceClass r : EnumSet.allOf(ResourceClass.class)) {
            request.resource = Json.getSubElementAsText(entry, r.tag());
            if (request.resource != null) {
                request.resourceClass = r;
                break;
            }
        }
        if (request.resource == null) {
            // if there is no name, then a blank filter is implied or it's an error
            if (filterable) {
                if (request.resourceFilter == null) {
                    request.resourceFilter = "";
                }
            } else {
                throw new ProcessingException("\"username\", \"authenticator\", or \"connection\" name missing");
            }
        }

        // resourceType: if there is no name, try resolving based on type
        // note that blank/missing type works only for "user"
        request.resourceType = Json.getSubElementAsText(entry, "type",
                request.resourceClass == null ? null : request.resourceClass.name());

        if (request.resourceType == null) {
            throw new ProcessingException("\"username\", \"authenticator\", \"connection\" or \"type\" required");
        } else if (request.resourceType.equals("user")) {
            if (request.resourceClass == null) {
                request.resourceClass = ResourceClass.user;
            } else if (request.resourceClass != ResourceClass.user) {
                throw new ProcessingException("\"type\":\""+request.resourceType+"\" incompatible with \""+request.resourceClass.tag()+"\"");
            }
        } else if (AUTH_TYPES.contains(request.resourceType)) {
            if (request.resourceClass == null) {
                request.resourceClass = ResourceClass.authenticator;
            } else if (request.resourceClass != ResourceClass.authenticator) {
                throw new ProcessingException("\"type\":\""+request.resourceType+"\" incompatible with \""+request.resourceClass.tag()+"\"");
            }
            if (operation == Operation.add && request.resourceType.equals("authenticator")) {
                throw new ProcessingException("generic type \"authenticator\" not valid for add: use specific type");
            }
        } else {
            if (request.resourceClass == null) {
                request.resourceClass = ResourceClass.connection;
            } else if (request.resourceClass != ResourceClass.connection) {
                throw new ProcessingException("\"type\":\""+request.resourceType+"\" incompatible with \""+request.resourceClass.tag()+"\"");
            }
            if (operation == Operation.add && request.resourceType.equals("connection")) {
                throw new ProcessingException("generic type \"connection\" not valid for add: use specific type");
            }
        }

        // if there specific resource type for a list/delete, update filter
        if (request.operation != Operation.add && request.operation != Operation.update
                && !request.resourceType.equals(request.resourceClass.name())) {
            if (!Strings.isNullOrEmpty(request.resourceFilter)) {
                request.resourceFilter = "("+request.resourceFilter+") and ";
            } else {
                request.resourceFilter = "";
            }
            request.resourceFilter += "type eq \""+request.resourceType+"\"";
            // if there is a specific resource name requested, add it to the filter as well
            if (request.resource != null) {
                // this won't include users, so "alias" is always correct
                request.resourceFilter += " and alias eq \""+request.resource+"\"";
                request.resource = null;
            }
        }

        // remove the fake "user" type
        if (request.resourceClass == ResourceClass.user) {
            entry.remove("type");
        }

        return request;
    }

	/*- main file processor --------------------------------------------------*/

    private void loadTemplate(TemplateExpander expander) throws Exception {
        // check for an explicit template
        if (template != null) {
            expander.template(template);
            return;
        }

        // check for empty file
        List<Map<String,String>> lines = expander.data();
        if (lines.size() == 0) {
            throw new ProcessingException("could not parse data from file");
        }

        // authenticator files have a "UserAlias" header
        if (lines.get(0).containsKey("UserAlias")) {
            expander.template(TemplateExpander.class.getResource("default/authenticator.yaml"));
            return;
        }

        // now check for "type": no "type" means a user file
        String type = lines.get(0).get("type");
        if (type == null) {
            expander.template(TemplateExpander.class.getResource("default/user.yaml"));
            return;
        }

        // toss out files that don't have consistent "type"
        for (Map<String,String> line : lines) {
            if (!type.equals(line.get("type"))) {
                throw new ProcessingException("file rows must be of the same type ("+type+")");
            }
        }

        // load the template resource
        switch (type) {
        case "as2":
        case "sftp":
        case "ftp":
            expander.template(TemplateExpander.class.getResource("default/"+type+".yaml"));
            return;
        default:
            throw new ProcessingException("no default template for type "+type);
        }
    }

    private ArrayNode prepareContent(String content) throws Exception {
        // load file content into a string
        ArrayNode file = null;

        if (template == null) {
            // Option 1: try to load it as a JSON or YAML file
            try {
                JsonNode json = Json.mapper.readTree(content);
                // file is a list of entries to process:
                //   convert a single entry file into a list of one
                if (!json.isArray()) {
                    file = Json.mapper.createArrayNode();
                    file.add(json);
                } else {
                    file = (ArrayNode)json;
                    json = file.get(0);
                }
                // now file is an array and json is the first element: test it
                if (json.isObject()) {
                    return file;
                }
            } catch (Exception notjson) {
                // try something else
            }
            file = null;
        }

        // Option 2: see if it can be loaded as CSV
        try {
            TemplateExpander expander = new TemplateExpander();
            expander.loadCsv(content);
            loadTemplate(expander);
            file = Json.mapper.createArrayNode();
            for (TemplateExpander.ExpanderResult result : expander.expand()) {
                if (!result.success()) {
                    throw result.exception();
                }
                if (result.expanded().isArray()) {
                    file.addAll((ArrayNode)result.expanded());
                } else {
                    file.add(result.expanded());
                }
            }
        } catch (Exception e) {
            throw new ProcessingException(e.getMessage());
        }
        return file;
    }

    private ArrayNode results;
    private ArrayNode passwords;

    /**
     * Resets the processing state for a new run.
     */
    public void reset() {
        results = Json.mapper.createArrayNode();
        passwords = Json.mapper.createArrayNode();
    }

    /**
     * Cleans up and returns the results, resetting the processing
     * state for a new run.
     * @return the cleaned up results ready for printing
     */
    public ArrayNode calculateResults() {
        cleanup(results);
        if (passwords.size() > 0) {
            results.add(passwordReport(passwords));
        }
        ArrayNode calculated = results;
        reset();
        return calculated;
    }

    /**
     * The main file processor. Load the file into {@code content} before calling.
     * The {@code fn} is included only for error reporting.
     * @param fn the name of the file to use for error reporting
     * @param content the content of the file loaded into a String
     */
    public void processFile(String fn, String content) {
        ArrayNode file;
        try {
            file = prepareContent(content);
        } catch (ProcessingException e) {
            results.add(insertResult(Json.setSubElement(null, "result.file", fn), false, e.getMessage()));
            return;
        } catch (Exception e) {
            results.add(insertResult(Json.setSubElement(null, "result.file", fn), false, e));
            return;
        }

        Iterator<JsonNode> elements = file.elements();
        while (elements.hasNext()) {
            ObjectNode entry = (ObjectNode)elements.next();
            ObjectNode original = entry.deepCopy();
            try {
                Operation operation = entry.has("operation")
                        ? Operation.valueOf(Json.asText(entry.remove("operation")))
                        : defaultOperation;
                // collect actions into an ObjectNode
                ObjectNode actions = normalizeActions(entry.remove("actions"));
                // remove other stuff that might be from a reflected result
                entry.remove("result");
                entry.remove("id");
                if (entry.isEmpty()) {
                    continue;
                }
                // let's go
                Request request = analyzeRequest(entry, operation);
                switch (operation) {
                case preview:
                    results.add(insertResult(original, true, "request preview"));
                    break;
                case add:
                    processAdd(request, actions, results, passwords);
                    break;
                case list:
                    {
                        ArrayNode tempResults = Json.mapper.createArrayNode();
                        processList(request, tempResults);
                        appendAndFlattenUsers(tempResults, results);
                    }
                    break;
                case update:
                    {
                        ArrayNode tempResults = Json.mapper.createArrayNode();
                        List<ObjectNode> toUpdate = processList(request, tempResults);
                        for (int i=0; i<toUpdate.size(); i++) {
                            try {
                                ObjectNode updated = updateResource(toUpdate.get(i), entry);
                                if (actions != null) {
                                    createActions(actions, updated);
                                }
                                results.add(tempResults.get(i));
                                String message = String.format("%s %s updated",
                                    request.resourceClass.name(), getObjectName(updated));
                                if (request.resourceFilter != null) {
                                    message += String.format(" (%d of %d)", i+1, toUpdate.size());
                                }
                                results.add(insertResult(updated, true, message));
                            } catch (Exception e) {
                                results.add(insertResult(toUpdate.get(i), false, e));
                            }
                        }
                    }
                    break;
                case delete:
                    {
                        ArrayNode tempResults = Json.mapper.createArrayNode();
                        List<ObjectNode> toDelete = processList(request, tempResults);
                        for (int i=0; i<toDelete.size(); i++) {
                            try {
                                api.delete(toDelete.get(i));
                                appendAndFlattenUsers(tempResults.get(i), results);
                            } catch (Exception e) {
                                results.add(insertResult(toDelete.get(i), false, e));
                            }
                        }
                    }
                    break;
                default:
                    throw new ProcessingException("operation "+operation+" not supported");
                }
            } catch (Exception e) {
                results.add(insertResult(original, false, e));
            }
        }
    }

    /**
     * Convenience method for command line use: loads each filename and processes
     * it, producing output on the standard output. The {@code -} filename is
     * understood to mean "read the standard input"&mdash;use {@code ./-}
     * to process a file named {@code -}.
     * @param fns the filenames to process
     * @throws IOException
     */
    public void processFiles(String[] fns) throws IOException {
        for (String fn : fns) {
            String content = fn.equals("-")
                    ? new String(ByteStreams.toByteArray(System.in))
                    : new String(Files.readAllBytes(Paths.get(fn)));
            processFile(fn, content);
        }

        Json.mapper.writeValue(System.out, calculateResults());
    }

    /**
     * Fluent style constructor. Pass the REST api connection to use and
     * use set methods to set options.
     * @param api the REST api connection to use
     */
    public BatchProcessor(REST api) {
        this (api, EnumSet.noneOf(Option.class), null);
    }

    /**
     * All argument constructor.
     * @param api the REST api connection to use
     * @param options the set of toggle options
     * @param exportPassword the export password option
     */
    public BatchProcessor(REST api, EnumSet<Option> options, String exportPassword) {
        this.api = api;
        this.options = options;
        this.exportPassword = exportPassword;
        this.defaultOperation = Operation.add;
        this.template = null;
        reset();
    }
}
