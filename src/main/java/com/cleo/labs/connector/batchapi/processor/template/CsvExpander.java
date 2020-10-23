package com.cleo.labs.connector.batchapi.processor.template;

import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

import com.cleo.labs.connector.batchapi.processor.BatchProcessor.ResultWriter;
import com.cleo.labs.connector.batchapi.processor.Json;
import com.cleo.labs.connector.batchapi.processor.template.TemplateExpander.ExpanderResult;
import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.Builder;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.ColumnType;

public class CsvExpander {
    private String template;
    private LinkedHashMap<String,ColumnType> headers;

    public CsvExpander() {
        this.template = null;
        this.headers = null;
    }

    public CsvExpander template(String template) {
        this.template = template;
        return this;
    }

    private static final int PREVIEW_THRESHOLD = 100;

    public ResultWriter getWriter(final PrintStream out, final ResultWriter log) throws IOException {
        final List<ObjectNode> data = new ArrayList<>();
        final TemplateExpander[] expander = new TemplateExpander[1];
        final List<ExpanderResult> errors = new ArrayList<>();

        return new ResultWriter() {
            private boolean previewing = true;
            private SequenceWriter seq = null;
            private List<ObjectNode> process() {
                List<ObjectNode> output = new ArrayList<>();
                for (ExpanderResult result : expander[0].expand()) {
                    if (!result.success()) {
                        errors.add(result);
                    } else if (result.expanded() == null) {
                        // skip quietly
                    } else if (result.expanded().isArray()) {
                        result.expanded().forEach(n -> output.add((ObjectNode)n));
                    } else {
                        output.add((ObjectNode)result.expanded());
                    }
                }
                return output;
            }
            private void start() throws IOException {
                // create a new expander and load the data
                expander[0] = new TemplateExpander().jsondata(data);

                // if the template looks like:
                //   columns:
                //   - name:
                //     type:
                //   - ...
                //   template:
                //     the template
                // pre-load the columns into the schema and split off the template
                JsonNode templateNode = Json.mapper.readTree(template);
                if (templateNode.isObject() &&
                        templateNode.size() == 2 &&
                        templateNode.has("columns") &&
                        templateNode.has("template")) {
                    seedHeaders(templateNode.get("columns"));
                    expander[0].template(templateNode.get("template"));
                } else {
                    expander[0].template(templateNode);
                }

                // run the template over the first data batch, collecting output and errors
                List<ObjectNode> output = process();

                // if there is any output, adjust the column headers and set up the writer
                if (output.size() > 0) {
                    extendHeaders(output);
                    CsvSchema schema = getSchema(headers);
                    CsvMapper mapper = new CsvMapper();
                    mapper.configure(Feature.AUTO_CLOSE_TARGET, false);
                    seq = mapper.writerFor(JsonNode.class)
                            .with(schema)
                            .writeValues(out);
                    for (ObjectNode node : output) {
                        seq.write(node);
                    }
                    previewing = false;
                }
            }
            @Override
            public void write(ObjectNode result) throws IOException {
                if (previewing) {
                    data.add(result);
                    if (data.size() >= PREVIEW_THRESHOLD) {
                        start();
                    }
                } else {
                    expander[0].jsondata(Collections.singletonList(result));
                    for (ObjectNode node : process()) {
                        seq.write(node);
                    }
                }
                // write to log, if configured
                if (log != null) {
                    log.write(result);
                }
            }
            @Override
            public void close() throws IOException {
                if (previewing && data.size() > 0) {
                    start();
                }
                if (seq != null) {
                    seq.close();
                }
                // append the errors, if there are any
                if (errors.size() > 0) {
                    PrintWriter writer = new PrintWriter(out);
                    for (ExpanderResult error : errors) {
                        error.exception().printStackTrace(writer);
                    }
                    writer.flush();
                }
                // close the log, if configured
                if (log != null) {
                    log.close();
                }
            }
        };
    }

    private void seedHeaders(JsonNode columns) throws IOException {
        headers = new LinkedHashMap<>();
        if (!columns.isArray()) {
            throw new IOException("template columns should be a list of column names");
        }
        for (JsonNode column : columns) {
            String name = Json.getSubElementAsText(column, "name");
            if (name == null) {
                throw new IOException("missing column name: "+column.toString());
            } else if (headers.containsKey(name)) {
                throw new IOException("duplicate column name: "+name);
            }
            String type = Json.getSubElementAsText(column, "type");
            ColumnType columnType = ColumnType.STRING;
            if (type != null) {
                try {
                    columnType = ColumnType.valueOf(type.trim().toUpperCase());
                } catch (Exception e) {
                    throw new IOException("invalid column type: "+type);
                }
            }
            headers.put(name, columnType);
        }
    }

    private void extendHeaders(List<ObjectNode> data) throws IOException {
        if (headers == null) {
            headers = new LinkedHashMap<>();
        }
        if (data != null && data.size() > 0) {
            for (JsonNode element : data) {
                if (!element.isObject()) {
                    throw new IOException("can't make a CSV out of this element: "+element.toString());
                }
                Iterator<Entry<String,JsonNode>> fields = element.fields();
                while (fields.hasNext()) {
                    Entry<String,JsonNode> field = fields.next();
                    if (!field.getValue().isValueNode()) {
                        throw new IOException("can't make a CSV out of this field: "+field.getValue().toString());
                    }
                    headers.put(field.getKey(), getColumnType(field.getValue(), headers.get(field.getKey())));
                }
            }
        }
    }

    private ColumnType getColumnType(JsonNode value, ColumnType existing) {
        ColumnType valueType;
        if (value.isBoolean()) {
            valueType = ColumnType.BOOLEAN;
        } else if (value.isNumber()) {
            valueType = ColumnType.NUMBER;
        } else {
            valueType = ColumnType.STRING;
        }
        if (existing == null || valueType == existing) {
            return valueType;
        } else if (valueType == ColumnType.STRING || existing == ColumnType.STRING) {
            return ColumnType.NUMBER_OR_STRING;
        } else {
            return ColumnType.NUMBER;
        }
    }

    private CsvSchema getSchema(LinkedHashMap<String,ColumnType> headers) {
        Builder builder = CsvSchema.builder();
        for (Entry<String,ColumnType> header : headers.entrySet()) {
            builder.addColumn(header.getKey(), header.getValue());
        }
        return builder.build().withHeader();
    }

}
