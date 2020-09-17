package com.cleo.labs.connector.batchapi.processor;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.base.Strings;

import jdk.nashorn.api.scripting.ScriptObjectMirror;

/**
 * Provides a framework for expanding strings with embedded expressions of the
 * form <span style="font-family: monospaced">${expression}</span> in the
 * context of the filename and a map of data bindings.
 */
@SuppressWarnings("restriction")
public class MacroEngine {
    private static final ScriptEngineManager engine_factory = new ScriptEngineManager(null);

    static {
        String[] version = System.getProperty("java.version").split("\\.");
        if (Integer.valueOf(version[0]) > 1 || Integer.valueOf(version[1]) > 8) {
            System.setProperty("nashorn.args", "--no-deprecation-warning");
        }
    }

    private ScriptEngine engine;
    private Date now;
    private Map<String, String> data;

    /**
     * Returns {@code true} if the {@link ScriptEngine} has been started.
     * 
     * @return {@code true} if the {@link ScriptEngine} has been started
     */
    public boolean started() {
        return engine != null;
    }

    /**
     * Starts up the {@link ScriptEngine}, if one has not yet been started.
     */
    private void startEngine() throws ScriptException {
        if (!started()) {
            // Ensure that System.err is not null because otherwise the Script Engine
            // will fail to initialize
            if (System.err == null) {
                System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.err)));
            }

            engine = engine_factory.getEngineByName("JavaScript");
            //engine = engine_factory.getEngineByName("nashorn");
            //engine = new NashornScriptEngineFactory().getScriptEngine();
            engine.eval("load('nashorn:mozilla_compat.js');"
                    + "function date(format) { return new java.text.SimpleDateFormat(format).format(now); }");
            engine.put("now", now);
            data(data);
        }
    }

    /**
     * Creates a new {@code MacroEngine} without starting the real JavaScript
     * {@link ScriptEngine}.
     * 
     */
    public MacroEngine(Map<String, String> data) {
        this.now = new Date();
        this.data = data;
    }

    /**
     * Sets the data for the engine. If the engine is started, sets the variables
     * from the map as well. Entries with null values are removed from the map.
     * 
     * @param data the data Map
     * @return {@code this} to allow for fluent-style setting
     */
    public MacroEngine data(Map<String, String> data) throws ScriptException {
        clear();
        if (data != null) {
            this.data = data;
            Set<String> names = data.keySet();
            names.removeIf(name -> data.get(name)==null);
            if (engine != null) {
                for (Map.Entry<String, String> entry : data.entrySet()) {
                    if (entry.getValue() != null) {
                        engine.put(entry.getKey(), entry.getValue());
                    }
                }
            }
        }
        return this;
    }

    /**
     * Returns the current data for the engine.
     * @return the current data map for the engine (possibly null)
     */
    public Map<String,String> data() {
        return this.data;
    }

    /**
     * Sets one datum for the engine. If the engine is started, set the variable
     * in the engine as well. Null values are ignored.
     * @param name the variable name
     * @param value the value
     * @return {@code this} to allow for fluent-style setting
     * @throws ScriptException
     */
    public MacroEngine datum(String name, String value) throws ScriptException {
        if (value != null) {
            if (this.data == null) {
                this.data = new HashMap<>();
            }
            data.put(name, value);
            if (engine != null) {
                engine.put(name, value);
            }
        }
        return this;
    }

    private static final String INT_SUFFIX = ":int";
    private static final String BOOLEAN_SUFFIX = ":boolean";
    private static final String ARRAY_SUFFIX = ":array";
    private static final String STRING_SUFFIX = ":string";

    /**
     * Returns the {@code input} with any embedded {@code ${expression}} replaced
     * with the results of evaluating them. Usually returns a {@code String}
     * encased in a {@link TextNode}, but if the expression ends in :int or
     * :boolean, it is evaluated appropriately and encased in a {@link IntNode}
     * or {@link BooleanNode} as appropriate.
     * 
     * @param input the string to process
     * @return the resulting JsonNode
     */
    public JsonNode expand(String input) throws ScriptException {
        SquiggleMatcher m = new SquiggleMatcher(input);
        String singleton = m.singleton();
        if (singleton != null) {
            if (singleton.endsWith(INT_SUFFIX)) {
                int value = Integer.valueOf(expr(singleton.substring(0, singleton.length()-INT_SUFFIX.length())));
                return IntNode.valueOf(value);
            } else if (singleton.endsWith(BOOLEAN_SUFFIX)) {
                boolean value = isTrue(singleton.substring(0, singleton.length()-BOOLEAN_SUFFIX.length()));
                return BooleanNode.valueOf(value);
            } else if (singleton.endsWith(ARRAY_SUFFIX)) {
                String expr = singleton.substring(0, singleton.length()-ARRAY_SUFFIX.length());
                List<String> list = asArray(expr);
                if (list != null) {
                    List<TextNode> text = list.stream().map(TextNode::valueOf).collect(Collectors.toList());
                    return Json.mapper.createArrayNode().addAll(text);
                } else {
                    return null;
                }
            } else if (singleton.endsWith(STRING_SUFFIX)) {
                String value = expr(singleton.substring(0, singleton.length()-STRING_SUFFIX.length()));
                return TextNode.valueOf(value);
            } else {
                return asNode(singleton);
            }
                
        } else {
            StringBuilder sb = new StringBuilder();
            while (m.find()) {
                m.appendReplacement(sb, expr(m.expression()));
            }
            m.appendTail(sb);
            String value = sb.toString();
            if (value.matches("(?i)true|false")) {
                return BooleanNode.valueOf(value.matches("(?i)true"));
            }
            return TextNode.valueOf(sb.toString());
        }
    }

    /**
     * {@link Pattern} matching {@code date('...')} with {@code ...} as
     * {@code group(1)} or {@code date("...")} with {@code ...} as {@code group(2)}.
     */
    public static final Pattern DATEFUNCTION = Pattern.compile("date\\((?:'([^']*)'|\"([^\"]*)\")\\)");

    /**
     * Attempts a simple variable lookup for {@code name}, or tries simple
     * evaluation of {@code date} expressions, returning the result, or {@code null}
     * if the expression is not a simple lookup or date expression.
     * 
     * @param name the variable name or date expression
     * @return the result, or {@code null} if the input was not a simple variable
     *         name or date expression
     */
    public String lookup(String name) {
        Matcher date = DATEFUNCTION.matcher(name);
        if (date.matches()) {
            String format = Strings.nullToEmpty(date.group(1)) + Strings.nullToEmpty(date.group(2));
            return new SimpleDateFormat(format).format(now);
        } else {
            if (data!=null && data.containsKey(name)) {
                return data.get(name);
            }
        }
        return null; // no such token
    }

    /**
     * Returns the results of evaluating the input {@code macro} as a JavaScript
     * expression. First, tries to do a simple {@link #lookup(String)} to avoid the
     * JavaScript engine overhead.
     * 
     * @param macro the input expression to evaluate
     * @return the result after lookup or evaluation
     */
    public String expr(String macro) throws ScriptException {
        // first try just looking up a simple value
        String lookup = lookup(macro);
        if (lookup != null) {
            return lookup;
        }
        // ok, now run the JS engine, starting it if needed
        startEngine();
        try {
            Object result = engine.eval(macro);
            return result==null ? null : result.toString();
        } catch (ScriptException e) {
            if (e.getMessage().startsWith("ReferenceError:")) {
                return null;
            }
            throw e;
        }
    }

    /**
     * Returns the result of evaluating the input JavaScript expression and converts
     * it to a boolean. Anything that looks null/empty/0/false is considered
     * {@code false}, as well as any expression that causes an Exception.
     * 
     * @param expr a JavaScript expression
     * @return {@code true} if the expression successfully evaluates to a
     *         non-null/empty/0/false value
     */
    public boolean isTrue(String expr) throws ScriptException {
        startEngine();
        try {
            Object result = engine.eval(expr);
            if (result == null) {
                return false;
            } else if (result instanceof Boolean) {
                return (Boolean) result;
            } else if (result instanceof String) {
                return !((String) result).matches("(?i)no|none|na|false|");
            } else if (result instanceof Integer) {
                return 0 != (Integer) result;
            } else if (result instanceof Double) {
                return 0.0 != (Double) result;
            } else {
                return true;
            }
        } catch (ScriptException e) {
            return false;
        }
    }

    public JsonNode asNode(String expr) throws ScriptException {
        // first try just looking up a simple value
        String lookup = lookup(expr);
        if (lookup != null) {
            return TextNode.valueOf(lookup);
        }
        // ok, now run the JS engine, starting it if needed
        startEngine();
        try {
            Object result = engine.eval(expr);
            if (result == null) {
                return null;
            } else if (result instanceof Boolean) {
                return BooleanNode.valueOf((Boolean)result);
            } else if (result instanceof Integer) {
                return IntNode.valueOf((Integer)result);
            } else if (result instanceof Double) {
                return DoubleNode.valueOf((Double)result);
            } else if (result.toString().matches("(?i)true|false")) {
                return BooleanNode.valueOf(result.toString().matches("(?i)true"));
            } else {
                return TextNode.valueOf(result.toString());
            }
        } catch (ScriptException e) {
            if (e.getMessage().startsWith("ReferenceError:")) {
                return null;
            }
            throw e;
        }
    }

    public List<String> asArray(String expr) throws ScriptException {
        startEngine();
        Object result;
        try {
            result = engine.eval(expr);
        } catch (ScriptException e) {
            if (e.getMessage().startsWith("ReferenceError:")) {
                return null;
            }
            throw e;
        }
        if (result==null) {
            return null;
        }
        if (result instanceof ScriptObjectMirror && ((ScriptObjectMirror)result).isArray()) {
            ScriptObjectMirror array = (ScriptObjectMirror)result;
            if (array.size()==0) {
                return null;
            } else {
                List<String> list = new ArrayList<>(array.size());
                array.values().forEach(v -> {
                    String value = v.toString();
                    if (!Strings.isNullOrEmpty(value)) {
                        list.add(value);
                    }
                });
                return list;
            }
        } else {
            String value = result.toString();
            if (Strings.isNullOrEmpty(value)) {
                return null;
            }
            return Arrays.asList(value);
        }
    }

    public Object eval(String expr) throws ScriptException {
        startEngine();
        return engine.eval(expr);
    }

    public Bindings bindings() {
        return engine == null ? null : engine.getBindings(ScriptContext.ENGINE_SCOPE);
    }

    public void clear() {
        Bindings bindings = bindings();
        if (bindings != null) {
            for (String key : bindings.keySet()) {
                if (key.equals("now") || key.equals("date")) {
                    continue;
                }
                bindings.remove(key);
            }
        }
        data = null;
    }
}
