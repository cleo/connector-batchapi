package com.cleo.labs.connector.batchapi.processor.template;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;

import org.junit.Test;

import com.cleo.labs.connector.batchapi.processor.Json;
import com.fasterxml.jackson.databind.node.ArrayNode;

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
                .template(template)
                .data(results);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        expander.writeTo(out);
        System.out.println("more lines");
        
    }

}
