package com.cleo.labs.connector.batchapi.processor.template;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

import com.cleo.labs.connector.batchapi.processor.Json;
import com.cleo.labs.connector.batchapi.processor.ProcessingException;
import com.cleo.labs.connector.batchapi.processor.template.TemplateExpander.ExpanderResult;
import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.Builder;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.ColumnType;

public class CsvExpander {
    private String template;
    private ArrayNode data;
    private LinkedHashMap<String,ColumnType> headers;

    public CsvExpander() {
        this.template = null;
        this.data = null;
        this.headers = null;
    }

    public CsvExpander template(String template) {
        this.template = template;
        return this;
    }

    public CsvExpander data(ArrayNode data) throws ProcessingException {
        this.data = data;
        return this;
    }

    public void writeTo(OutputStream out) throws Exception {
        // data and template are supposed to be set before this
        // create a new expander and load the data
        TemplateExpander expander = new TemplateExpander()
            .jsondata(data);

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
            expander.template(templateNode.get("template"));
        } else {
            expander.template(templateNode);
        }
                
        // run the template over the data and collect output and errors
        ArrayNode output = Json.mapper.createArrayNode();
        List<ExpanderResult> errors = new ArrayList<>();
        for (ExpanderResult result : expander.expand()) {
            if (!result.success()) {
                errors.add(result);
            } else if (result.expanded() == null) {
                // skip quietly
            } else if (result.expanded().isArray()) {
                output.addAll((ArrayNode)result.expanded());
            } else {
                output.add(result.expanded());
            }
        }

        // print the output, if there is any
        if (output.size() > 0) {
            extendHeaders(output);
            CsvSchema schema = getSchema(headers);
            CsvMapper mapper = new CsvMapper();
            mapper.configure(Feature.AUTO_CLOSE_TARGET, false);
            mapper.writerFor(JsonNode.class)
                .with(schema)
                .writeValue(out, output);
            out.flush();
        }

        // append the errors, if there are any
        if (errors.size() > 0) {
            PrintWriter writer = new PrintWriter(out);
            for (ExpanderResult error : errors) {
error.exception().printStackTrace();
                writer.println("ERROR: "+error.exception().getClass().getSimpleName()+" - "+
                    error.exception().getMessage());
            }
            writer.flush();
        }
    }

    private void seedHeaders(JsonNode columns) throws ProcessingException {
        headers = new LinkedHashMap<>();
        if (!columns.isArray()) {
            throw new ProcessingException("template columns should be a list of column names");
        }
        for (JsonNode column : columns) {
            String name = Json.getSubElementAsText(column, "name");
            if (name == null) {
                throw new ProcessingException("missing column name: "+column.toString());
            } else if (headers.containsKey(name)) {
                throw new ProcessingException("duplicate column name: "+name);
            }
            String type = Json.getSubElementAsText(column, "type");
            ColumnType columnType = ColumnType.STRING;
            if (type != null) {
                try {
                    columnType = ColumnType.valueOf(type.trim().toUpperCase());
                } catch (Exception e) {
                    throw new ProcessingException("invalid column type: "+type);
                }
            }
            headers.put(name, columnType);
        }
    }

    private void extendHeaders(ArrayNode data) throws ProcessingException {
        if (headers == null) {
            headers = new LinkedHashMap<>();
        }
        if (data != null && data.size() > 0) {
            for (JsonNode element : data) {
                if (!element.isObject()) {
                    throw new ProcessingException("can't make a CSV out of this element: "+element.toString());
                }
                Iterator<Entry<String,JsonNode>> fields = element.fields();
                while (fields.hasNext()) {
                    Entry<String,JsonNode> field = fields.next();
                    if (!field.getValue().isValueNode()) {
                        throw new ProcessingException("can't make a CSV out of this field: "+field.getValue().toString());
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
