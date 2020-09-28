package com.cleo.labs.connector.batchapi.processor;

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

import com.cleo.labs.connector.batchapi.processor.template.CsvExpander;
import com.cleo.labs.connector.batchapi.processor.template.TemplateExpander;
import com.cleo.labs.connector.batchapi.processor.versalex.StubVersaLex;
import com.cleo.labs.connector.batchapi.processor.versalex.VersaLex;
import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;

public class BatchProcessor {
    public enum Operation {
        add ("created"),
        list ("found"),
        update ("updating"),
        delete ("deleted"),
        preview ("previewed"),
        run ("run");

        private String tag;
        private Operation(String tag) {
            this.tag = tag;
        }
        public String tag() {
            return tag;
        }
    };

    public enum OutputFormat {yaml, json, csv};

    private REST api;
    private String exportPassword;
    private Operation defaultOperation;
    private String template;
    private boolean traceRequests;
    private boolean generatePasswords;
    private OutputFormat outputFormat;
    private String outputTemplate;
    private VersaLex versalex;

    public BatchProcessor setExportPassword(String exportPassword) {
        this.exportPassword = exportPassword;
        return this;
    }

    public BatchProcessor setDefaultOperation(Operation defaultOperation) {
        if (defaultOperation != null) {
            // null means "leave the same": add by default
            this.defaultOperation = defaultOperation;
        }
        return this;
    }

    public BatchProcessor setTemplate(Path template) throws IOException {
        return setTemplate(new String(Files.readAllBytes(template), Charsets.UTF_8));
    }

    public BatchProcessor setTemplate(String template) {
        this.template = template;
        return this;
    }

    public BatchProcessor setTraceRequests(boolean traceRequests) {
        this.traceRequests = traceRequests;
        return this;
    }

    public BatchProcessor setGeneratePasswords(boolean generatePasswords) {
        this.generatePasswords = generatePasswords;
        return this;
    }

    public BatchProcessor setOutputFormat(OutputFormat outputFormat) {
        if (outputFormat != null) {
            // null means "leave the same": yaml by default
            this.outputFormat = outputFormat;
        }
        return this;
    }

    public BatchProcessor setOutputTemplate(Path outputTemplate) throws IOException {
        return setOutputTemplate(new String(Files.readAllBytes(outputTemplate), Charsets.UTF_8));
    }

    public BatchProcessor setOutputTemplate(String outputTemplate) {
        this.outputTemplate = outputTemplate;
        return this;
    }

    private void loadVersaLex() {
        try {
            Class<?> clazz = Class.forName("com.cleo.labs.connector.batchapi.processor.versalex.RealVersaLex");
            Object object = clazz.newInstance();
            versalex = VersaLex.class.cast(object);
        } catch (Throwable e) {
            versalex = new StubVersaLex();
        }
    }

    public enum ResourceClass {
        user ("username"),
        authenticator ("authenticator"),
        connection ("connection"),
        any ("name");

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
    public enum ActionType {Commands, JavaScript};
    private static final Set<String> ACTION_TYPES = EnumSet.allOf(ActionType.class)
            .stream()
            .map(ActionType::name)
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
        String encrypted = OpenSSLCrypt.encrypt(exportPassword, password);
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
        String pwdhash = null;
        String password = null;
        if (type.equals("user")) {
            updated.remove("authenticator"); // this is used as the authenticator alias, which isn't present in the real API
            updates.remove("authenticator");
            pwdhash = Json.asText(updates.remove("pwdhash"));
        } else if (type.equals("connection")) {
            // if the existing image has a password, make sure it goes back in decrypted
            String p = Json.getSubElementAsText(updated, "connect.password");
            if (p != null) {
                Json.setSubElement(updated, "connect.password", versalex.decrypt(p));
                password = p; // now password is decrypted original, if available
            }
            // likewise if the update has a password make sure it is decrypted
            p = Json.getSubElementAsText(updates, "connect.password");
            if (p != null) {
                Json.setSubElement(updates, "connect.password", OpenSSLCrypt.decrypt(exportPassword, p));
                password = p; // now password is decrypted update, if available
            }
        } else if (type.equals("action")) {
            // get rid of reference elements added by listAction
            Json.removeElements(updated,
                    "authenticator",
                    "username",
                    "connection");
            Json.removeElements(updates,
                    "authenticator",
                    "username",
                    "connection");
        }
        JsonNode actions = updated.remove("actions");
        updated.setAll(updates);
        cleanup(updated);
        ObjectNode result = api.put(updated, object);
        if (type.equals("user")) {
            result = listUser(result);
            if (pwdhash != null) {
                versalex.set(Json.getSubElementAsText(result, "authenticator"),
                        Json.getSubElementAsText(result, "username"), "Pwdhash", pwdhash);
            }
        } else if (type.equals("connection")) {
            if (password != null) {
                // encrypt it into the result
                Json.setSubElement(result, "connect.password", OpenSSLCrypt.encrypt(exportPassword, password));
            }
        }
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
        String filter = request.resourceFilter.replace(NAMETOKEN, "username");
        String authenticator = Json.getSubElementAsText(request.entry, "authenticator");
        String authfilter = Strings.isNullOrEmpty(authenticator) ? null : "alias eq \""+authenticator+"\"";
        List<ObjectNode> list = api.getUsers(authfilter, filter);
        if (list.isEmpty()) {
            throw new NotFoundException("filter \""+filter+"\" returned no users"+
                (Strings.isNullOrEmpty(authenticator) ? "" : " in "+authenticator));
        }
        for (int i = 0; i<list.size(); i++) {
            list.set(i, listUser(list.get(i)));
        }
        return list;
    }

    private ObjectNode listUser(Request request) throws Exception {
        String authenticator = Json.getSubElementAsText(request.entry, "authenticator");
        String authfilter = Strings.isNullOrEmpty(authenticator) ? null : "alias eq \""+authenticator+"\"";
        ObjectNode user = api.getUser(authfilter, request.resource);
        if (user == null) {
            throw new NotFoundException("user "+request.resource+" not found"+
                (Strings.isNullOrEmpty(authenticator) ? "" : " in "+authenticator));
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
            String username = Json.getSubElementAsText(user, "username");
            String pwdhash = versalex.get(alias, username, "Pwdhash");
            if (alias != null) {
                // set host, but reorder things to get it near the top
                ObjectNode update = Json.mapper.createObjectNode();
                update.set("id", user.get("id"));
                update.put("username", username);
                update.set("email",  user.get("email"));
                update.put("authenticator", alias);
                if (!Strings.isNullOrEmpty(pwdhash)) {
                    update.put("pwdhash", pwdhash);
                }
                update.setAll(user);
                user = update;
            }
        }
        return user;
    }

    private static final String USERSTOKEN = "$$users$$"; // place to stash ArrayNode of users in an authenticator result

    private List<ObjectNode> listAuthenticators(Request request, boolean includeUsers) throws Exception {
        String filter = request.resourceFilter.replace(NAMETOKEN, "alias");
        List<ObjectNode> list = api.getAuthenticators(filter);
        if (list.isEmpty()) {
            throw new NotFoundException("filter \""+filter+"\" returned no authenticators");
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
                authenticator.putArray(USERSTOKEN).addAll(userlist);
            }
        }
        return list;
    }

    private ObjectNode listAuthenticator(Request request, boolean includeUsers) throws Exception {
        ObjectNode authenticator = api.getAuthenticator(request.resource);
        if (authenticator == null) {
            throw new NotFoundException("authenticator "+request.resource+" not found");
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
			authenticator.putArray(USERSTOKEN).addAll(userlist);
        }
        return authenticator;
    }

    private List<ObjectNode> listConnections(Request request) throws Exception {
        String filter = request.resourceFilter.replace(NAMETOKEN, "alias");
        List<ObjectNode> list = api.getConnections(filter);
        if (list.isEmpty()) {
            throw new NotFoundException("filter \""+filter+"\" returned no connections");
        }
        for (int i = 0; i<list.size(); i++) {
            list.set(i, listConnection(list.get(i)));
        }
        return list;
    }

    private ObjectNode listConnection(Request request) throws Exception {
        ObjectNode connection = api.getConnection(request.resource);
        if (connection == null) {
            throw new NotFoundException("connection "+request.resource+" not found");
        }
        return listConnection(connection);
    }

    private ObjectNode listConnection(ObjectNode connection) throws Exception {
        String alias = Json.asText(connection.remove("alias"));
        String password = versalex.decrypt(versalex.get(alias, "password"));
        connection.put("connection", alias);
        if (!Strings.isNullOrEmpty(password)) {
            Json.setSubElement(connection, "connect.password", OpenSSLCrypt.encrypt(exportPassword, password));
        }
        return injectActions(connection);
    }

    private List<ObjectNode> listActions(Request request) throws Exception {
        List<String> clauses = new ArrayList<>();
        if (request.action != null) {
            clauses.add("alias eq \""+request.action+"\"");
        }
        if (!Strings.isNullOrEmpty(request.actionFilter)) {
            clauses.add("("+request.actionFilter+")");
        }
        if (request.resource != null) {
            switch (request.resourceClass) {
            case user:
                clauses.add("authenticator.user.username eq \""+request.resource+"\"");
                String authenticator = Json.asText(request.entry.get("authenticator"));
                if (authenticator != null) {
                    clauses.add("authenticator.alias eq \""+authenticator+"\"");
                }
                break;
            case authenticator:
                clauses.add("authenticator.alias eq \""+request.resource+"\"");
                break;
            case connection:
                clauses.add("connection.alias eq \""+request.resource+"\"");
                break;
            default:
            }
        }
        String filter = clauses.stream().collect(Collectors.joining(" and "));
        List<ObjectNode> list = api.getActions(filter);
        for (ObjectNode action : list) {
            Json.setSubElement(action, "username", Json.getSubElementAsText(action, "authenticator.user.username"));
            Json.setSubElement(action, "authenticator", Json.getSubElementAsText(action, "authenticator.alias"));
            Json.setSubElement(action, "connection", Json.getSubElementAsText(action, "connection.alias"));
        }
        return list;
    }

	/*------------------------------------------------------------------------*
	 * Cleanups -- Remove Metadata and Defaults                               *
	 *------------------------------------------------------------------------*/

    private ObjectNode cleanupAction(ObjectNode action) {
        Json.removeElements(action,
                "active",
                "editable",
                "runnable",
                "running",
                "ready",
                "enabled=true",
             // "authenticator",
             // "connection",
                "meta",
                "_links");
        return action;
    }

    private ObjectNode cleanupActions(ObjectNode resource) {
        ObjectNode actions = (ObjectNode)resource.get("actions");
        if (actions != null) {
            Iterator<JsonNode> elements = actions.elements();
            while (elements.hasNext()) {
                ObjectNode action = (ObjectNode)elements.next();
                cleanupAction(action);
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
        case "action":
            return cleanupAction(resource);
        default:
            return resource;
        }
    }

    private ArrayNode cleanup(ArrayNode list) {
        Iterator<JsonNode> elements = list.elements();
        while (elements.hasNext()) {
            JsonNode element = elements.next();
            if (element.isObject()) {
                cleanup((ObjectNode)element);
            }
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
        ObjectNode update = insertResult(node, success, e.getClass().getSimpleName()+": "+e.getMessage());
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
        String pwdhash = Json.asText(request.entry.remove("pwdhash"));
        String generatedPwd = null;
        if (pwdhash != null || generatePasswords) {
            generatedPwd = generatePassword();
            Json.setSubElement(request.entry, "accept.password", generatedPwd);
        }
        ObjectNode user = api.createUser(request.entry, authenticator);
        if (user == null) {
            throw new ProcessingException("user not created");
        }
        if (pwdhash != null) {
            versalex.set(alias, request.resource, "Pwdhash", pwdhash);
            user.put("pwdhash", pwdhash);
        } else if (generatePasswords) {
            if (outputFormat == OutputFormat.csv) {
                Json.setSubElement(user, "accept.password", generatedPwd);
            } else {
                passwords.add(generatedPassword(alias, request.entry));
            }
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
        // decrypt the password if it's encrypted
        String password = OpenSSLCrypt.decrypt(exportPassword,
                Json.getSubElementAsText(request.entry, "connect.password"));
        if (password != null) {
            Json.setSubElement(request.entry, "connect.password", password);
        }
        ObjectNode connection = api.createConnection(request.entry);
        if (connection == null) {
            throw new ProcessingException("error: connection not created");
        }
        connection.set("connection", connection.remove("alias"));
        if (password != null) {
            // protect the password in the result output, even if it was clearText in the request
            Json.setSubElement(connection, "connect.password", OpenSSLCrypt.encrypt(exportPassword, password));
        }
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
                ArrayNode users = (ArrayNode)authenticator.remove(USERSTOKEN);
                if (users == null) {
                    users = Json.mapper.createArrayNode();
                }
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
                ArrayNode userResults = authenticatorResult.putArray(USERSTOKEN);
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
            ArrayNode users = (ArrayNode)authenticator.remove(USERSTOKEN);
            String message = includeUsers
                    ? String.format("%s authenticator %s",
                        request.operation.tag(), request.resource)
                    : String.format("%s authenticator %s with %d users",
                        request.operation.tag(), request.resource, users.size());
            ObjectNode authenticatorResult = insertResult(authenticator, true, message);
            ArrayNode userResults = authenticatorResult.putArray(USERSTOKEN);
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

    private List<ObjectNode> processListActions(Request request, ArrayNode results) throws Exception {
        List<ObjectNode> list;
        if (request.resource != null && request.resourceClass == ResourceClass.any ||
                request.resourceFilter != null) {
            // loop over the underlying resources
            list = new ArrayList<>();
            String action = request.action;
            String actionFilter = request.actionFilter;
            request.action = null;
            request.actionFilter = null;
            ArrayNode tempResults = Json.mapper.createArrayNode();
            for (ObjectNode resource : processList(request, tempResults)) {
                Request inner = new Request();
                inner.operation = request.operation;
                inner.action = action;
                inner.actionFilter = actionFilter;
                inner.resource = getObjectName(resource);
                inner.resourceClass = ResourceClass.valueOf(Json.getSubElementAsText(resource, "meta.resourceType"));
                inner.entry = resource;
                list.addAll(listActions(inner));
            }
        } else {
            list = listActions(request);
        }

        int i = 1;
        for (ObjectNode action : list) {
            String message = String.format("%s action %s (%d of %d)",
                    request.operation.tag(),
                    Json.getSubElementAsText(action, "alias"),
                    i++, list.size());
            results.add(insertResult(action, true, message));
        }
        return list;
    }

    private List<ObjectNode> processList(Request request, ArrayNode results) throws Exception {
        if (request.action != null || request.actionFilter != null) {
            return processListActions(request, results);
        }
        switch (request.resourceClass) {
        case user:
            return processListUser(request, results);
        case authenticator:
            return processListAuthenticator(request, results,
                    request.operation.equals(Operation.list) || request.operation.equals(Operation.delete));
        case connection:
            return processListConnection(request, results);
        case any:
        {
            List<ObjectNode> list = new ArrayList<>();
            try {
                list.addAll(processListUser(request, results));
            } catch (NotFoundException ignore) {}
            try {
                list.addAll(processListAuthenticator(request, results,
                            request.operation.equals(Operation.list) || request.operation.equals(Operation.delete)));
            } catch (NotFoundException ignore) {}
            try {
                list.addAll(processListConnection(request, results));
            } catch (NotFoundException ignore) {}
            if (list.isEmpty()) {
                throw new NotFoundException("filter \""+request.resourceFilter.replace("NAMETOKEN", "name")+"\" returned no users");
            }
            return list;
        }
        default:
            throw new ProcessingException("unrecognized request");
        }
    }

    private void appendAndFlattenUsers(JsonNode tempResult, ArrayNode results) {
        results.add(tempResult);
        ArrayNode users = (ArrayNode)((ObjectNode)tempResult).remove(USERSTOKEN);
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
        public ResourceClass resourceClass = null;  // indicates API endpoint: authenticator, user, connection
        public String resourceType = null;          // specific resource type, or generic type (same as class)
        public String resource = null;              // the resource name (username or alias in the API)
        public String resourceFilter = null;        // filter if using one
        public String action = null;                // action name
        public String actionFilter = null;          // filter for actions, if using one
        public ObjectNode entry = null;             // edited/cleaned up entry to query on
        public ObjectNode actions = null;           // actions separated out from entry and cleaned up
        public Operation operation = null;          // the operation
    }

    private static final String NAMETOKEN = "$$name$$";

    public Request analyzeRequest(ObjectNode original) throws Exception {
        Request request = new Request();

        // make a copy of the original to edit and process in the request
        request.entry = (ObjectNode)(original.deepCopy());

        // figure out the requested operation
        request.operation = request.entry.has("operation")
                ? Operation.valueOf(Json.asText(request.entry.remove("operation")))
                : defaultOperation;
        boolean existing = request.operation != Operation.add;

        // collect actions into an ObjectNode
        request.actions = normalizeActions(request.entry.remove("actions"));
        // remove other stuff that might be from a reflected result
        request.entry.remove("result");
        request.entry.remove("id");

        // see if there is a filter (and remove it from the entry)
        request.resourceFilter = Json.asText(request.entry.remove("filter"));
        if (request.resourceFilter != null && !existing) {
            throw new ProcessingException("\"filter\" valid only for list, update and delete");
        }

        // look for the resource name under username/authenticator/connection
        // if found we will know resourceClass and resource (name)
        for (ResourceClass r : EnumSet.allOf(ResourceClass.class)) {
            String resource = Json.getSubElementAsText(request.entry, r.tag());
            if (resource != null) {
                if (request.resourceClass==ResourceClass.user && r==ResourceClass.authenticator) {
                    // well, that's ok then and resourceClass should stay user
                    // note: this logic depends on user < authenticator in ResourceClass's "natural order"
                } else if (request.resourceClass != null) {
                    throw new ProcessingException("\""+r.tag()+"\" incompatible with \""+request.resourceClass.tag()+"\"");
                } else {
                    request.resource = resource;
                    request.resourceClass = r;
                }
            }
        }
        if (existing && request.resource == null && request.resourceFilter == null) {
            // if there is no name, then a blank filter is implied
            request.resourceFilter = "";
        }
        if (!existing && (request.resource == null || request.resourceClass==ResourceClass.any)) {
            // if it's an add and there is no name (or it's the generic "name"), that's an error
            throw new ProcessingException("\"username\", \"authenticator\" or \"connection\" required");
        }

        // resourceType: if there is no name, try resolving based on type
        // note that blank/missing type works only for "user"
        request.resourceType = Json.getSubElementAsText(request.entry, "type",
                request.resourceClass == null ? null : request.resourceClass.name());

        if (request.resourceType == null) {
            // this means there was no name and no type, so it's an "any" by default
            request.resourceClass = ResourceClass.any;
            request.resourceType = ResourceClass.any.name();
        } else if (request.resourceType.equals(ResourceClass.user.name())) {
            // the only user type is "user", so this forces class to user as well
            if (request.resourceClass == null || request.resourceClass == ResourceClass.any) {
                request.resourceClass = ResourceClass.user;
            } else if (request.resourceClass != ResourceClass.user) {
                throw new ProcessingException("\"type\":\""+request.resourceType+"\" incompatible with \""+request.resourceClass.tag()+"\"");
            }
        } else if (AUTH_TYPES.contains(request.resourceType)) {
            // any of the auth types force class to authenticator
            if (request.resourceClass == null || request.resourceClass == ResourceClass.any) {
                request.resourceClass = ResourceClass.authenticator;
            } else if (request.resourceClass != ResourceClass.authenticator) {
                throw new ProcessingException("\"type\":\""+request.resourceType+"\" incompatible with \""+request.resourceClass.tag()+"\"");
            }
            if (!existing && request.resourceType.equals("authenticator")) {
                throw new ProcessingException("generic type \"authenticator\" not valid for add: use specific type");
            }
        } else if (ACTION_TYPES.contains(request.resourceType)) {
            // then the type belongs to the action, so null it out for the parent resource
            request.resourceType = null;
        } else if (!request.resourceType.equals(ResourceClass.any.name())) {
            // any other type we'll assume to be a connection (except "any")
            if (request.resourceClass == null || request.resourceClass == ResourceClass.any) {
                request.resourceClass = ResourceClass.connection;
            } else if (request.resourceClass != ResourceClass.connection) {
                throw new ProcessingException("\"type\":\""+request.resourceType+"\" incompatible with \""+request.resourceClass.tag()+"\"");
            }
            if (!existing && request.resourceType.equals("connection")) {
                throw new ProcessingException("generic type \"connection\" not valid for add: use specific type");
            }
        }

        // if there specific resource type or name for an existing resource, update filter
        if (existing && request.resourceType != null &&
                (!request.resourceType.equals(request.resourceClass.name()) ||
                 request.resourceFilter != null && request.resource != null)) {
            List<String> clauses = new ArrayList<>();
            if (!Strings.isNullOrEmpty(request.resourceFilter)) {
                clauses.add("("+request.resourceFilter+")");
            }
            if (!request.resourceType.equals(request.resourceClass.name())) {
                clauses.add("type eq \""+request.resourceType+"\"");
            }
            // if there is a specific resource name requested, add it to the filter as well
            if (request.resource != null) {
                // this won't include users, so "alias" is always correct
                clauses.add(NAMETOKEN+" eq \""+request.resource+"\"");
                request.resource = null;
            }
            if (!clauses.isEmpty()) {
                request.resourceFilter = clauses.stream().collect(Collectors.joining(" and "));
            }
        }

        // remove the fake "user" type
        if (request.resourceClass == ResourceClass.user) {
            request.entry.remove("type");
        }

        // parse out action and actionFilter
        request.action = Strings.emptyToNull(Json.asText(request.entry.remove("action")));
        request.actionFilter = Json.asText(request.entry.remove("actionfilter")); // "" means "all"
        if (request.action != null || request.actionFilter != null) {
            if (!existing) {
                throw new ProcessingException("to add an action, add or update the parent resource");
            }
        } else if (request.operation == Operation.run) {
            request.actionFilter = ""; // if you say run then an "all" actionfilter is implied
        }

        return request;
    }

	/*- main file processor --------------------------------------------------*/

    private void loadTemplate(TemplateExpander expander) throws Exception {
        // check for an explicit template
        if (!Strings.isNullOrEmpty(template)) {
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

        if (Strings.isNullOrEmpty(template)) {
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
                    ObjectNode errorNode = Json.mapper.createObjectNode();
                    ObjectNode resultNode = errorNode.putObject("result");
                    ObjectNode csvNode = resultNode.putObject("csv");
                    csvNode.put("error", result.exception().getMessage());
                    csvNode.put("line", result.lineNumber());
                    csvNode.set("data", Json.mapper.valueToTree(result.line()));
                    file.add(errorNode);
                } else if (result.expanded().isArray()) {
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
            // pull the next element and make sure it's an object
            JsonNode element = elements.next();
            if (!element.isObject()) {
                results.add(insertResult(Json.setSubElement(null, "result.request", element.toString()), false, "invalid request"));
                continue;
            }
            // process the request
            ObjectNode original = (ObjectNode)element;
            try {
                Request request = analyzeRequest(original);
                if (traceRequests) {
                    System.err.println("REQUEST:");
                    System.err.println(Json.mapper.valueToTree(request).toPrettyString());
                }
                if (request.entry.isEmpty() &&
                        (request.operation==Operation.add || request.operation==Operation.update)) {
                    results.add(Json.setSubElement(original, "result.message", "empty request"));
                    continue;
                }
                switch (request.operation) {
                case preview:
                    results.add(insertResult(original, true, "request preview"));
                    break;
                case add:
                    processAdd(request, request.actions, results, passwords);
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
                                ObjectNode updated = updateResource(toUpdate.get(i), request.entry);
                                createActions(request.actions, updated);
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
                case run:
                    {
                        ArrayNode tempResults = Json.mapper.createArrayNode();
                        List<ObjectNode> toRun = processListActions(request, tempResults);
                        for (int i=0; i<toRun.size(); i++) {
                            try {
                                ObjectNode action = toRun.get(i);
                                ObjectNode output = api.runAction(action, request.entry);
                                String message = "ran action "+Json.getSubElementAsText(action, "alias");
                                if (toRun.size() > 1) {
                                    message += String.format(" (%d of %d)", i+1, toRun.size());
                                }
                                ObjectNode report = Json.mapper.createObjectNode();
                                report.set("output", output);
                                results.add(insertResult(report, true, message));
                            } catch (Exception e) {
                                results.add(insertResult(toRun.get(i), false, e));
                            }
                        }
                    }
                    break;
                default:
                    throw new ProcessingException("operation "+request.operation+" not supported");
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

        ArrayNode output = calculateResults();
        switch (outputFormat) {
        case yaml:
            Json.mapper.writeValue(System.out, output);
            break;
        case json:
            new ObjectMapper()
                .configure(SerializationFeature.INDENT_OUTPUT, true)
                .configure(Feature.AUTO_CLOSE_TARGET, false)
                .writeValue(System.out, output);
            System.out.println();
            break;
        case csv:
            try {
                new CsvExpander()
                    .template(outputTemplate)
                    .data(output)
                    .writeTo(System.out);
            } catch (Exception e) {
                throw new IOException(e);
            }
            break;
        }
    }

    /**
     * Fluent style constructor. Pass the REST api connection to use and
     * use set methods to set options.
     * @param api the REST api connection to use
     */
    public BatchProcessor(REST api) {
        this.api = api;
        this.exportPassword = null;
        this.defaultOperation = Operation.add;
        this.template = null;
        this.traceRequests = false;
        this.generatePasswords = false;
        this.outputFormat = OutputFormat.yaml;
        loadVersaLex();
        reset();
    }
}
