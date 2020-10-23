package com.cleo.labs.connector.batchapi.processor.template;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.Test;

import com.cleo.labs.connector.batchapi.processor.BatchProcessor.ResultWriter;
import com.cleo.labs.connector.batchapi.processor.Json;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class TestCsvExpander {

    @Test
    public void test() throws Exception {
        ArrayNode results = (ArrayNode)Json.mapper.readTree("---\n"+
            "- result:\n"+
            "    status: success\n"+
            "    message: created demo1\n"+
            "  id: 000111222333-1\n"+
            "  username: demo1\n"+
            "  email: demo1@cleo.demo\n"+
            "  password: secret1\n"+
            "  authenticator: Users\n"+
            "- result:\n"+
            "    status: success\n"+
            "    message: created demo2\n"+
            "  id: 000111222333-2\n"+
            "  username: demo2\n"+
            "  email: demo2@cleo.demo\n"+
            "  password: secret2\n"+
            "  authenticator: Users\n"+
            "");
        assertEquals(2, results.size());
        String template = "---\n"+
            "- user: ${data.username}\n"+
            "  email: ${data.email}\n"+
            "  password: ${data.password}\n"+
            "  group: Users\n"+
            "";
        CsvExpander expander = new CsvExpander()
                .template(template);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ResultWriter writer = expander.getWriter(new PrintStream(out), null);
        for (JsonNode node : results) {
            writer.write((ObjectNode)node);
        }
        writer.close();
        out.write("more lines\n".getBytes());
        assertEquals("user,email,password,group\n"
                + "demo1,demo1@cleo.demo,secret1,Users\n"
                + "demo2,demo2@cleo.demo,secret2,Users\n"
                + "more lines\n", new String(out.toByteArray()));
    }

}
