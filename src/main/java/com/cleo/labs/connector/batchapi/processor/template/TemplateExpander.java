package com.cleo.labs.connector.batchapi.processor.template;

import java.io.IOException;
import java.io.StringReader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
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
    private List<ObjectNode> jsondata;

    /*-- constructors --------------------------------------------------------*/

    /**
     * Constructs a new TemplateExpander with a default {@link MacroEngine}
     * that is initialized without any data loaded.
     */
    public TemplateExpander() {
        this(new MacroEngine());
    }

    /**
     * Constructs a new TemplateExpander using the supplied {@link MacroEngine}.
     * @param engine the {@code MacroEngine} to use
     */
    public TemplateExpander(MacroEngine engine) {
        this.engine = engine;
        this.template = null;
        this.data = null;
        this.jsondata = null;
    }

    /*-- the template --------------------------------------------------------*/

    /**
     * Set a YAML template from an already parsed JsonNode.
     * @param template the JsonNode template
     * @return {@code this} for fluent style setup
     * @throws IOException
     */
    public TemplateExpander template(JsonNode template) throws IOException {
        this.template = template;
        return this;
    }

    /**
     * Load a YAML template by parsing it from a String.
     * @param content the String to parse.
     * @return {@code this} for fluent style setup
     * @throws IOException
     */
    public TemplateExpander template(String content) throws IOException {
        return template(Json.mapper.readTree(content));
    }

    /**
     * Load a YAML template by parsing it from a file Path.
     * @param path the Path of the file to load
     * @return {@code this} for fluent style setup
     * @throws IOException
     */
    public TemplateExpander template(Path path) throws IOException {
        return template(new String(Files.readAllBytes(path), Charsets.UTF_8));
    }

    /**
     * Load a YAML template by parsing it from a resource, for example:
     * <pre>
     * template(MyClass.class.getResource("template.yaml"));
     * </pre>
     * @param path the URL of the resource to load
     * @return {@code this} for fluent style setup
     * @throws IOException
     */
    public TemplateExpander template(URL resource) throws IOException {
        return template(Resources.toString(resource, Charsets.UTF_8));
    }

    /*-- the data ------------------------------------------------------------*
     * The core model for the TemplateExpander is expanding a sequence of     *
     * "lines" parsed from a CSV file, each line represented as a Map whose   *
     * keys come from the column headings and values come from the CSV line,  *
     * or the TemplateExpander is expanding a sequence of Json Objects,       *
     * retrieved from a Json ArrayNode.                                       *
     * The Expander Iterator/Iterable iterates over these "lines" or          *
     * "objects", resetting the data in the MacroEngine for each line.        *
     *------------------------------------------------------------------------*/

    /**
     * Set/replace the data lines by parsing CSV content from a String.
     * @param content the String to parse as a CSV file
     * @return {@code this} for fluent style setup
     * @throws IOException
     */
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

    /**
     * Set/replace the data lines by parsing CSV content from a file Path
     * @param path the Path of the file to parse as CSV
     * @return {@code this} for fluent style setup
     * @throws IOException
     */
    public TemplateExpander loadCsv(Path path) throws IOException {
        return loadCsv(new String(Files.readAllBytes(path), Charsets.UTF_8));
    }

    /**
     * Add a single additional line of data from a Map.
     * @param line the Map representing the CSV line
     * @return {@code this} for fluent style setup
     */
    public TemplateExpander line(Map<String,String> line) {
        if (data == null) {
            data = new ArrayList<>();
        }
        data.add(line);
        return this;
    }

    /**
     * Getter providing direct access to the underlying data
     * (it's mutable, so be careful -- someday maybe it will
     * be a copy, so don't count on it or break anything!)
     * @return the underlying data
     */
    public List<Map<String,String>> data() {
        return data;
    }

    /**
     * Use a Json ArrayNode instead of CSV lines as the source
     * for template expansion.
     * @param jsondata the source of template data
     * @return {@code this} for fluent style setup
     */
    public TemplateExpander jsondata(List<ObjectNode> jsondata) {
        this.jsondata = jsondata;
        return this;
    }

    /**
     * Clears the data.
     * @return {@code this} for fluent style setup
     */
    public TemplateExpander clear() {
        data = null;
        jsondata = null;
        return this;
    }

    /*-- hooks for expander functions ----------------------------------------*/

    /**
     * If the engine is set up, inserts a variable into the engine.
     * @param key the variable name
     * @param value the value
     * @throws ScriptException
     */
    public void put(String key, Object value) throws ScriptException {
        if (engine != null) {
            engine.put(key, value);
        }
    }

    /**
     * If the engine is set up, evaluates and expression and returns the result,
     * optionally protecting a variable name from being cleared.
     * @param key the variable name to reserve (or {@code null})
     * @param expr the expression to evaluate
     * @return the result
     * @throws ScriptException
     */
    public Object eval(String key, String expr) throws ScriptException {
        Object value = null;
        if (engine != null) {
            value = engine.eval(key, expr);
        }
        return value;
    }

    /*-- the expander internal implementation --------------------------------*/

    private static final Pattern LOOP_PATTERN = Pattern.compile("\\$\\{for\\s+(column\\s+)?([a-zA-Z_]\\w*)\\s*:(.*)\\}");
    private static final Pattern IF_PATTERN = Pattern.compile("\\$\\{(?:(else\\s+)?if(?:\\s[^:]*)?:(.*)|else(?:\\s.*)?)\\}");
    private static final Pattern VAR_PATTERN = Pattern.compile("\\$\\{var\\s+([a-zA-Z_]\\w*)\\s*}");
    private static final Pattern ERROR_PATTERN = Pattern.compile("\\$\\{error(?:\\s.*)?}");

    /**
     * Looks for a singleton expression of one of the following looping forms:
     * <ul><li>${for id:array expression}</li>
     *     <li>${for column id:regex}</li>
     * </ul>
     * @param key the string to examine
     * @return {@code true} if it matches a looping form
     */
    private static boolean isLoop(String key) {
        return new SquiggleMatcher(key).singleton() != null &&
            LOOP_PATTERN.matcher(key).matches();
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
            if (engine.data() != null) {
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

    private static class ElseTracker {
        private boolean conditionOpen = false;
        private boolean conditionSatisfied = false;
        public boolean checkIf() {
            // ${if} is always ok and starts a new condition block
            conditionOpen = true;
            conditionSatisfied = false;
            return true;
        }
        public boolean checkElseIf() throws ProcessingException {
            // ${else if} requires an open condition and leaves it open
            if (!conditionOpen) {
                throw new ProcessingException("${if} required before ${else if}");
            }
            return !conditionSatisfied;
        }
        public boolean checkElse() throws ProcessingException {
            // ${else} requires an open condition but closes it
            if (!conditionOpen) {
                throw new ProcessingException("${if} required before ${else}");
            }
            conditionOpen = false;
            return !conditionSatisfied;
        }
        public void satisfy() {
            conditionSatisfied = true;
        }
    }

    /**
     * Looks for a singleton expression of the form ${if:expression}.
     * @param key the string to analyze
     * @return true for conditional keys
     */
    private static boolean isConditional(String key) {
        return new SquiggleMatcher(key).singleton() != null &&
            IF_PATTERN.matcher(key).matches();
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
     * @throws ProcessingException
     */
    private boolean isTrueCondition(String key, ElseTracker tracker) throws ScriptException, ProcessingException {
        Matcher matcher = IF_PATTERN.matcher(key);
        matcher.matches(); // always true since isLoop was called, right?
        boolean ifElse = matcher.group(1) != null;
        String expr = matcher.group(2);
        // first check for ${else} -- never anything to evaluate
        // just return true whether the condition is satisfied or not
        if (expr == null) {
            // ${else}
            return tracker.checkElse();
        }

        // now check ${if} and ${else if} to see if we should even evalute
        // if not (condition already satisfied), just return false
        boolean eval;
        if (ifElse) {
            // ${else if}
            eval = tracker.checkElseIf();
        } else {
            // ${if}
            eval = tracker.checkIf();
        }
        if (!eval) {
            return false;
        }

        // so we at least have to evaluate
        // return true (and mark the condition as satisfied) if the expression is true
        boolean result = engine.isTrue(expr);
        if (result) {
            tracker.satisfy();
        }
        return result;
    }

    /**
     * Evaluates an isConditional node and returns {@code true} if
     * it evaluates to a Boolean with a value of {@code true}.
     * @param key the node to evaluate
     * @return {@code true} if it evaluates to true
     * @throws ScriptException
     * @throws ProcessingException
     */
    private boolean isTrueCondition(JsonNode node, ElseTracker tracker) throws ScriptException, ProcessingException {
        return isTrueCondition(node.fieldNames().next(), tracker);
    }

    /**
     * Looks for a singleton expression of the form ${var variable}.
     * @param key the string to analyze
     * @return true for "var" keys
     */
    private static boolean isVar(String key) {
        return new SquiggleMatcher(key).singleton() != null &&
            VAR_PATTERN.matcher(key).matches();
    }

    /**
     * Looks for an object with a single field whose name is a
     * singleton expression of the form ${var variable}.
     * @param node the node to analyze
     * @return true for "var" nodes
     */
    private static boolean isVar(JsonNode node) {
        if (node.isObject() && node.size()==1) {
            return isVar(node.fieldNames().next());
        }
        return false;
    }

    /**
     * For a key for which {@code isVar} is true, extracts the variable
     * name and returns it.
     * @param key the {@code isVar} String to evaluate
     * @return the extracted variable name
     */
    private String varName(String key) {
        Matcher matcher = VAR_PATTERN.matcher(key);
        matcher.matches(); // always true since isLoop was called, right?
        return matcher.group(1);
    }

    /**
     * For a node for which {@code isVar} is true, extracts the variable
     * name and returns it.
     * @param node the {@code isVar} node to evaluate
     * @return the extracted variable name
     */
    private String varName(JsonNode node) {
        return varName(node.fieldNames().next());
    }

    /**
     * Looks for a singleton expression of the form ${error}.
     * @param key the string to analyze
     * @return true for "error" keys
     */
    private static boolean isError(String key) {
        return new SquiggleMatcher(key).singleton() != null &&
            ERROR_PATTERN.matcher(key).matches();
    }

    /**
     * Looks for an object with a single field whose name is a
     * singleton expression of the form ${error}.
     * @param node the node to analyze
     * @return true for "error" nodes
     */
    private static boolean isError(JsonNode node) {
        if (node.isObject() && node.size()==1) {
            return isError(node.fieldNames().next());
        }
        return false;
    }

    private void arrayNodeMerge(ArrayNode result, JsonNode toAdd) {
        if (toAdd == null) {
            // do nothing
        } else if (toAdd.isArray()) {
            result.addAll((ArrayNode)toAdd);
        } else {
            result.add(toAdd);
        }
    }

    private JsonNode expand(JsonNode node) throws Exception {
        if (node.isArray()) {
            ArrayNode result = Json.mapper.createArrayNode();
            ElseTracker tracker = new ElseTracker();
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
                    if (isTrueCondition(entry, tracker)) {
                        arrayNodeMerge(result, expand(entry.elements().next()));
                    }
                } else if (isVar(entry)) {
                    String var = varName(entry);
                    engine.datum(var, Json.asText(expand(entry)));
                } else if (isError(entry)) {
                    throw new ProcessingException(Json.asText(expand(entry)));
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
            ElseTracker tracker = new ElseTracker();
            Iterator<Entry<String,JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Entry<String,JsonNode> field = fields.next();
                JsonNode entry = field.getValue();
                if (isConditional(field.getKey())) {
                    if (isTrueCondition(field.getKey(), tracker)) {
                        JsonNode expanded = expand(entry);
                        if (expanded == null) {
                            // ignore
                        } else if (expanded.isObject()) {
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
                } else if (isVar(field.getKey())) {
                    String var = varName(field.getKey());
                    engine.datum(var, Json.asText(expand(entry)));
                } else if (isError(field.getKey())) {
                    throw new ProcessingException(Json.asText(expand(entry)));
                } else {
                    String key = engine.expand(field.getKey()).asText();
                    JsonNode expanded = expand(entry);
                    if (expanded != null) {
                        result.set(key, expanded);
                    }
                }
            }
            return result.size() > 0 ? result : null;
        } else if (node.isTextual()) {
            JsonNode x = engine.expand(node.asText());
            return x;
        } else {
            return node;
        }
    }

    /*-- the expander public external interface ------------------------------*/

    public static class ExpanderResult {
        private int lineNumber;
        private JsonNode expanded;
        private Exception exception;
        private Map<String,String> line;
        private JsonNode object;
        public ExpanderResult lineNumber(int lineNumber) {
            this.lineNumber = lineNumber;
            return this;
        }
        public int lineNumber() {
            return this.lineNumber;
        }
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
        public ExpanderResult line(Map<String,String> line) {
            this.line = line;
            return this;
        }
        public Map<String,String> line() {
            return this.line;
        }
        public ExpanderResult object(JsonNode object) {
            this.object = object;
            return this;
        }
        public JsonNode object() {
            return this.object;
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
        private Iterator<ObjectNode> jsonIterator;
        private int lineNumber;

        public Expander() {
            if (data != null) {
                dataIterator = data.iterator();
                jsonIterator = null;
                lineNumber = 1; // start at 1 to count the header line
            } else {
                dataIterator = null;
                jsonIterator = jsondata.iterator();
                lineNumber = 0;
            }
        }

        @Override
        public boolean hasNext() {
            if (dataIterator != null) {
                return dataIterator.hasNext();
            } else {
                return jsonIterator.hasNext();
            }
        }

        @Override
        public ExpanderResult next() {
            ExpanderResult result = new ExpanderResult()
                    .lineNumber(++lineNumber);
            if (dataIterator != null) {
                Map<String,String> line = dataIterator.next();
                result.line(line);
                try {
                    engine.data(line);
                    result.expanded(expand(template));
                } catch (Exception e) {
                    result.exception(e);
                }
            } else {
                JsonNode object = jsonIterator.next();
                result.object(object);
                try {
                    engine.object(object);
                    result.expanded(expand(template));
                } catch (Exception e) {
                    result.exception(e);
                }
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

    public static Expander emptyExpander() {
        return new TemplateExpander(null).jsondata(Collections.emptyList()).new Expander();
    }
}
