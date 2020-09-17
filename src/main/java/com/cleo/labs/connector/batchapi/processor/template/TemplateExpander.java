package com.cleo.labs.connector.batchapi.processor.template;

import java.io.IOException;
import java.io.StringReader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.script.ScriptException;

import com.cleo.labs.connector.batchapi.processor.Json;
import com.cleo.labs.connector.batchapi.processor.MacroEngine;
import com.cleo.labs.connector.batchapi.processor.ProcessingException;
import com.cleo.labs.connector.batchapi.processor.SquiggleMatcher;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;

public class TemplateExpander {
    private MacroEngine engine;
    private JsonNode template;
    private List<Map<String,String>> data;

    public TemplateExpander() {
        this(new MacroEngine(null));
    }

    public TemplateExpander(MacroEngine engine) {
        this.engine = engine;
        this.data = null;
        this.template = null;
    }

    public TemplateExpander template(String content) throws IOException {
        this.template = Json.mapper.readTree(content);
        return this;
    }

    public TemplateExpander template(Path path) throws IOException {
        return template(new String(Files.readAllBytes(path), Charsets.UTF_8));
    }

    public TemplateExpander template(URL resource) throws IOException {
        return template(Resources.toString(resource, Charsets.UTF_8));
    }

    public TemplateExpander loadCsv(String content) throws IOException {
        CsvMapper mapper = new CsvMapper();
        CsvSchema schema = CsvSchema.emptySchema().withHeader();
        MappingIterator<Map<String,String>> it = mapper.readerFor(Map.class)
                .with(schema)
                .readValues(new StringReader(content));
        data = new ArrayList<>();
        it.forEachRemaining(data::add);
        return this;
    }

    public TemplateExpander loadCsv(Path path) throws IOException {
        return loadCsv(new String(Files.readAllBytes(path), Charsets.UTF_8));
    }

    public TemplateExpander line(Map<String,String> line) {
        if (data == null) {
            data = new ArrayList<>();
        }
        data.add(line);
        return this;
    }

    public List<Map<String,String>> data() {
        return data;
    }

    public TemplateExpander clear() {
        data = null;
        return this;
    }

    private static final Pattern LOOP_PATTERN = Pattern.compile("\\$\\{for\\s+(column\\s+)?([a-zA-Z_]\\w*)\\s*:(.*)\\}");

    /**
     * Looks for a singleton expression of one of the following looping forms:
     * <ul><li>${for id:array expression}</li>
     *     <li>${for column id:regex}</li>
     * </ul>
     * @param key the string to examine
     * @return {@code true} if it matches a looping form
     */
    private static boolean isLoop(String key) {
        if (new SquiggleMatcher(key).singleton() != null) {
            Matcher m = LOOP_PATTERN.matcher(key);
            if (m.matches()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Looks for an object with a single field whose name is a
     * singleton expression of one of the following looping forms:
     * <ul><li>${for id:array expression}</li>
     *     <li>${for column id:regex}</li>
     * </ul>
     * @param node
     * @return
     */
    private static boolean isLoop(JsonNode node) {
        if (node.isObject() && node.size()==1) {
            return isLoop(node.fieldNames().next());
        }
        return false;
    }

    /**
     * For a key for which {@code isLoop} is true, extracts the loop
     * ID and calculates the values (either from a JavaScript expression
     * or from the column names loaded in the engine). It returns a
     * list of strings: the first string is the extracted ID, and the
     * remaining strings are the calculated values.
     * @param key the {@code isLoop} String to evaluate
     * @return a list of ID followed by 0 or more values
     * @throws ScriptException
     */
    private List<String> loopValues(String key) throws ScriptException {
        Matcher matcher = LOOP_PATTERN.matcher(key);
        matcher.matches(); // always true since isLoop was called, right?
        String id = matcher.group(2);
        String expr = matcher.group(3);
        List<String> values = new ArrayList<>();
        values.add(id);
        if (matcher.group(1) != null) {
            // column mode
            Pattern pattern = Pattern.compile(expr);
            for (String column : engine.data().keySet()) {
                Matcher m = pattern.matcher(column);
                if (m.matches()) {
                    if (m.groupCount() > 0) {
                        values.add(m.group(1));
                    } else {
                        values.add(column);
                    }
                }
            }
        } else {
            // JavaScript expression mode
            List<String> array = engine.asArray(expr);
            if (array != null) {
                values.addAll(array);
            }
        }
        return values;
    }

    /**
     * For a node for which {@code isLoop} is true, extracts the loop
     * ID and calculates the values (either from a JavaScript expression
     * or from the column names loaded in the engine). It returns a
     * list of strings: the first string is the extracted ID, and the
     * remaining strings are the calculated values.
     * @param node the {@code isLoop} node to evaluate
     * @return a list of ID followed by 0 or more values
     * @throws ScriptException
     */
    private List<String> loopValues(JsonNode node) throws ScriptException {
        return loopValues(node.fieldNames().next());
    }

    private static final String IF_PREFIX = "${if:";

    /**
     * Looks for a singleton expression of the form ${if:expression}.
     * @param key the string to analyze
     * @return true for conditional keys
     */
    private static boolean isConditional(String key) {
        return new SquiggleMatcher(key).singleton() != null && key.startsWith(IF_PREFIX);
    }

    /**
     * Looks for an object with a single field whose name is a
     * singleton expression of the form ${if:expression}.
     * @param node the node to analyze
     * @return true for conditional nodes
     */
    private static boolean isConditional(JsonNode node) {
        if (node.isObject() && node.size()==1) {
            return isConditional(node.fieldNames().next());
        }
        return false;
    }

    /**
     * Evaluates an isConditional key string and returns {@code true} if
     * it evaluates to a Boolean with a value of {@code true}.
     * @param key the key expression to evaluate
     * @return {@code true} if it evaluates to true
     * @throws ScriptException
     */
    private boolean isTrueCondition(String key) throws ScriptException {
        String expr = "${" + key.substring(IF_PREFIX.length(), key.length()-1) + ":boolean}";
        JsonNode result = engine.expand(expr);
        return result.isBoolean() && result.asBoolean();
    }

    /**
     * Evaluates an isConditional node and returns {@code true} if
     * it evaluates to a Boolean with a value of {@code true}.
     * @param key the node to evaluate
     * @return {@code true} if it evaluates to true
     * @throws ScriptException
     */
    private boolean isTrueCondition(JsonNode node) throws ScriptException {
        return isTrueCondition(node.fieldNames().next());
    }

    private void arrayNodeMerge(ArrayNode result, JsonNode toAdd) {
        if (toAdd.isArray()) {
            result.addAll((ArrayNode)toAdd);
        } else {
            result.add(toAdd);
        }
    }

    private JsonNode expand(JsonNode node) throws Exception {
        if (node.isArray()) {
            ArrayNode result = Json.mapper.createArrayNode();
            for (int i=0; i<node.size(); i++) {
                JsonNode entry = node.get(i);
                if (isLoop(entry)) {
                    JsonNode body = entry.elements().next();
                    List<String> values = loopValues(entry);
                    String id = values.remove(0);
                    for (int v=0; v<values.size(); v++) {
                        engine.datum(id, values.get(v));
                        arrayNodeMerge(result, expand(body));
                    }
                } else if (isConditional(entry)) {
                    if (isTrueCondition(entry)) {
                        arrayNodeMerge(result, expand(entry.elements().next()));
                    }
                } else {
                    JsonNode expanded = expand(entry);
                    if (expanded != null) {
                        result.add(expanded);
                    }
                }
            }
            return result.size() > 0 ? result : null;
        } else if (node.isObject()) {
            ObjectNode result = Json.mapper.createObjectNode();
            Iterator<Entry<String,JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Entry<String,JsonNode> field = fields.next();
                JsonNode entry = field.getValue();
                if (isConditional(field.getKey())) {
                    if (isTrueCondition(field.getKey())) {
                        JsonNode expanded = expand(entry);
                        if (expanded.isObject()) {
                            result.setAll((ObjectNode)expanded);
                        } else {
                            throw new ProcessingException("invalid template condition--\"${if:}\" must be a nested object in an object: "+
                                field.getKey());
                        }
                    }
                } else if (isLoop(field.getKey())) {
                    JsonNode body = field.getValue();
                    List<String> values = loopValues(field.getKey());
                    String id = values.remove(0);
                    for (int v=0; v<values.size(); v++) {
                        engine.datum(id, values.get(v));
                        JsonNode expanded = expand(body);
                        if (expanded.isObject()) {
                            result.setAll((ObjectNode)expanded);
                        } else {
                            throw new ProcessingException("invalid template condition--\"${for}\" must be a nested object in an object: "+
                                field.getKey());
                        }
                    }
                } else {
                    String key = engine.expand(field.getKey()).asText();
                    JsonNode expanded = expand(entry);
                    if (expanded != null) {
                        result.set(key, expanded);
                    }
                }
            }
            return result.size() > 0 ? result : null;
        } else {
            JsonNode x = engine.expand(node.asText());
            return x;
        }
    }

    public static class ExpanderResult {
        private JsonNode expanded;
        private Exception exception;
        public ExpanderResult expanded(JsonNode expanded) {
            this.expanded = expanded;
            return this;
        }
        public JsonNode expanded() {
            return this.expanded;
        }
        public ExpanderResult exception(Exception exception) {
            this.exception = exception;
            return this;
        }
        public Exception exception() {
            return this.exception;
        }
        public boolean success() {
            return this.exception == null;
        }
        public ExpanderResult() {
            this.expanded = null;
            this.exception = null;
        }
        @Override
        public String toString() {
            StringBuilder s = new StringBuilder();
            if (expanded != null) {
                s.append("expanded to ").append(expanded.toString());
            }
            if (exception != null) {
                if (s.length() > 0) {
                    s.append(' ');
                }
                s.append("exception: ").append(exception.getMessage());
            }
            return s.toString();
        }
    }

    public class Expander implements Iterator<ExpanderResult>, Iterable<ExpanderResult> {
        private Iterator<Map<String,String>> dataIterator;

        public Expander() {
            dataIterator = data.iterator();
        }

        @Override
        public boolean hasNext() {
            return dataIterator.hasNext();
        }

        @Override
        public ExpanderResult next() {
            Map<String,String> line = dataIterator.next();
            ExpanderResult result = new ExpanderResult();
            try {
                engine.data(line);
                result.expanded(expand(template));
            } catch (Exception e) {
                result.exception(e);
            }
            return result;
        }

        @Override
        public Iterator<ExpanderResult> iterator() {
            return this;
        }
    }

    public Expander expand() {
        return new Expander();
    }
}