package com.cleo.labs.connector.batchapi.processor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.script.ScriptException;

import org.junit.Test;

import com.cleo.labs.connector.batchapi.processor.template.TemplateExpander;
import com.fasterxml.jackson.databind.JsonNode;

public class TestMacroEngine {

    static private final boolean DIAGNOSTIC = true;

    @Test
    public void testInt() {
        try {
            Map<String,String> data = new HashMap<>();
            data.put("a", "alice");
            data.put("b", "bob");
            MacroEngine engine = new MacroEngine(data);
            JsonNode result;

            result = engine.expand("${a}");
            assertTrue(result.isTextual());
            assertEquals("alice", result.asText());

            result = engine.expand("${a.length}");
            assertTrue(result.isInt());
            assertEquals("alice".length(), result.asInt());

            result = engine.expand("${a.length:int}");
            assertTrue(result.isInt());
            assertEquals("alice".length(), result.asInt());

            result = engine.expand("${a.length>0}");
            assertTrue(result.isBoolean());
            assertEquals(true, result.asBoolean());

            result = engine.expand("${true}");
            assertTrue(result.isBoolean());
            assertEquals(true, result.asBoolean());

            result = engine.expand("${a.length>0:boolean}");
            assertTrue(result.isBoolean());
            assertEquals(true, result.asBoolean());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testClear() throws ScriptException {
        // starts with two bindings: date() and now
        MacroEngine engine = new MacroEngine(null);
        engine.eval("true"); // force a start
        assertEquals(2, engine.bindings().size());

        // add one more
        engine.datum("data1", "value1");
        assertEquals(3, engine.bindings().size());

        // replace that one with 2 more
        Map<String,String> data = new HashMap<>();
        data.put("a", "alice");
        data.put("b", "bob");
        engine.data(data);
        assertEquals(4, engine.bindings().size());
        assertEquals("alice", engine.eval("a"));
        if (DIAGNOSTIC) System.out.println(engine.bindings().entrySet());

        // replace those 2 with 2 more, but one is null
        data.put("a", null);
        engine.data(data);
        assertEquals(3, engine.bindings().size());

        // clear it back to 2
        engine.clear();
        assertEquals(2, engine.bindings().size());

        // add a funny name
        if (DIAGNOSTIC) System.out.println(engine.bindings());
        engine.datum("Carrier/Client", "CLIENT");
        engine.datum("TP#", "TP0");
        assertEquals(4, engine.bindings().size());
        assertEquals("TP0", engine.eval("this['TP#']"));
        if (DIAGNOSTIC) System.out.println(engine.bindings().entrySet());
    }

    @Test
    public void testNull() throws Exception {
        Map<String,String> data = new HashMap<>();
        data.put("a", "alice");
        data.put("b", "bob");
        data.put("d", null);
        MacroEngine engine = new MacroEngine(data);
        JsonNode result;

        result = engine.expand("${c}");
        assertNull(result);

        result = engine.expand("${d}");
        assertNull(result);

        result = engine.expand("${null}");
        assertNull(result);
    }

    @Test
    public void testAuthenticatorTemplate() throws Exception {
        Map<String,String> data = new HashMap<>();
        data.put("UserAlias", "Users_Batch-1");
        data.put("FolderPath", null);
        data.put("HomeDir", "%HarmonyRoot%local/root/Process/%username%");
        data.put("DownloadFolder", "./");
        data.put("UploadFolder", "./");
        data.put("OtherFolder", "test/one;test/two");
        data.put("ArchiveSent", "default");
        data.put("ArchiveReceived", "default");
        data.put("FTP", "Yes");
        data.put("SSHFTP", "No");
        data.put("HTTP", "Yes");
        data.put("Access", "ReadOnly");
        MacroEngine engine = new MacroEngine(data);
        
        TemplateExpander expander = new TemplateExpander(engine)
                .template(TemplateExpander.class.getResource("default/authenticator.yaml"))
                .line(data);
        for (TemplateExpander.ExpanderResult result : expander.expand()) {
            if (result.success()) {
                if (DIAGNOSTIC) Json.mapper.writeValue(System.out, result.expanded());
            } else {
                if (DIAGNOSTIC) result.exception().printStackTrace();
                fail(result.exception().getMessage());
            }
        }
    }

    @Test
    public void testAS2Template() throws Exception {
        Map<String,String> data = new HashMap<>();
        data.put("type", "as2");
        data.put("alias", "ccas2");
        data.put("inbox", "%system%");
        data.put("outbox", "%system%");
        data.put("sentbox", "sent");
        data.put("receivedbox", "received");
        data.put("url", "http://localhost:5080/as2");
        data.put("AS2From", "CFrom");
        data.put("AS2To", "CTo");
        data.put("Subject", "CSubject");
        data.put("https", "no");
        data.put("encrypted", "no");
        data.put("signed", "no");
        data.put("receipt", "no");
        data.put("receipt_sign", "no");
        data.put("receipt_type", "sync");
        data.put("CreateSendName", "sendA");
        data.put("CreateReceiveName", "rcvA");
        data.put("ActionSend", "PUT send");
        data.put("ActionReceive", "PUT rcv");
        data.put("Schedule_Send", "polling");
        data.put("Schedule_Receive", "on Mon-Fri @17:00");
        data.put("action_1_alias", "actnow");
        data.put("action_1_commands", "LCOPY * %inbox%;PUT -DEL foo.txt");
        data.put("action_1_schedule", "polling");
        MacroEngine engine = new MacroEngine(data);
        
        TemplateExpander expander = new TemplateExpander(engine)
                .template(TemplateExpander.class.getResource("default/as2.yaml"))
                .line(data);
        for (TemplateExpander.ExpanderResult result : expander.expand()) {
            if (result.success()) {
                if (DIAGNOSTIC) Json.mapper.writeValue(System.out, result.expanded());
            } else {
                if (DIAGNOSTIC) result.exception().printStackTrace();
                fail(result.exception().getMessage());
            }
        }
    }

    @Test
    public void testSFTPTemplate() throws Exception {
        Map<String,String> data = new HashMap<>();
        data.put("type", "sftp");
        data.put("alias", "test sftp");
        data.put("inbox", "data/inbox");
        data.put("outbox", "data/outbox");
        data.put("sentbox", "sent");
        data.put("receivedbox", "received");
        data.put("host", "mysftp.com");
        data.put("port", "10022");
        data.put("username", "joeuser");
        data.put("password", "pas$W0rd");
        data.put("CreateSendName", "sendA");
        data.put("CreateReceiveName", "rcvA");
        data.put("ActionSend", "PUT send");
        data.put("ActionReceive", "PUT rcv");
        data.put("Schedule_Send", "polling");
        data.put("Schedule_Receive", "on Mon-Fri @17:00");
        data.put("action_1_alias", "actnow");
        data.put("action_1_commands", "LCOPY * %inbox%;PUT -DEL foo.txt");
        data.put("action_1_schedule", "polling");
        MacroEngine engine = new MacroEngine(data);
        
        TemplateExpander expander = new TemplateExpander(engine)
                .template(TemplateExpander.class.getResource("default/sftp.yaml"))
                .line(data);
        for (TemplateExpander.ExpanderResult result : expander.expand()) {
            if (result.success()) {
                if (DIAGNOSTIC) Json.mapper.writeValue(System.out, result.expanded());
            } else {
                if (DIAGNOSTIC) result.exception().printStackTrace();
                fail(result.exception().getMessage());
            }
        }
    }

    @Test
    public void testFTPTemplate() throws Exception {
        Map<String,String> data = new HashMap<>();
        data.put("type", "ftp");
        data.put("alias", "test ftp");
        data.put("inbox", "data/inbox");
        data.put("outbox", "data/outbox");
        data.put("sentbox", "sent");
        data.put("receivedbox", "received");
        data.put("host", "myftp.com");
        data.put("port", "10021");
        data.put("username", "joeuser");
        data.put("password", "pas$W0rd");
        data.put("channelmode", "passive");
        data.put("activelowport", "30000");
        data.put("activehighport", "30099");
        data.put("CreateSendName", "sendA");
        data.put("CreateReceiveName", "rcvA");
        data.put("ActionSend", "PUT send");
        data.put("ActionReceive", "PUT rcv");
        data.put("Schedule_Send", "polling");
        data.put("Schedule_Receive", "on Mon-Fri @17:00");
        data.put("action_1_alias", "actnow");
        data.put("action_1_commands", "LCOPY * %inbox%;PUT -DEL foo.txt");
        data.put("action_1_schedule", "polling");
        MacroEngine engine = new MacroEngine(data);
        
        TemplateExpander expander = new TemplateExpander(engine)
                .template(TemplateExpander.class.getResource("default/ftp.yaml"))
                .line(data);
        for (TemplateExpander.ExpanderResult result : expander.expand()) {
            if (result.success()) {
                if (DIAGNOSTIC) Json.mapper.writeValue(System.out, result.expanded());
            } else {
                if (DIAGNOSTIC) result.exception().printStackTrace();
                fail(result.exception().getMessage());
            }
        }
    }

    @Test
    public void testUserTemplate() throws Exception {
        Map<String,String> data = new HashMap<>();
        data.put("Host", "Users_Batch-1");
        data.put("UserID", "admiralbev");
        data.put("Password", "VH6ti4c89b5i");
        data.put("DefaultHomeDir", "Yes");
        data.put("CustomHomeDir", "%HarmonyRoot%local/root/eProcessing/MillerCoors");
        data.put("WhitelistIP", "127.0.0.1;127.0.0.2");
        data.put("CreateCollectName", "NA");
        data.put("CreateReceiveName", "NA");
        data.put("ActionCollect", "NA");
        data.put("ActionReceive", "NA");
        data.put("Schedule_Send", "no");
        data.put("Schedule_Receive", "no");
        data.put("HostNotes", "NA");
        data.put("OtherFolder", "local/test;local/test1");
        data.put("Email", "admiralbev@cleo.com");
        MacroEngine engine = new MacroEngine(data);
        
        TemplateExpander expander = new TemplateExpander(engine)
                .template(TemplateExpander.class.getResource("default/user.yaml"))
                .line(data);
        for (TemplateExpander.ExpanderResult result : expander.expand()) {
            if (result.success()) {
                if (DIAGNOSTIC) Json.mapper.writeValue(System.out, result.expanded());
            } else {
                if (DIAGNOSTIC) result.exception().printStackTrace();
                fail(result.exception().getMessage());
            }
        }
    }

    @Test
    public void testArray() throws ScriptException {
        MacroEngine engine = new MacroEngine(Collections.emptyMap());
        JsonNode result;

        result = engine.expand("${'foo bar bat'.split(' '):array}");
        assertTrue(result.isArray());
        assertEquals(3, result.size());

        result = engine.expand("${'foobarbat'.split(' '):array}");
        assertTrue(result.isArray());
        assertEquals(1, result.size());

        result = engine.expand("${'foobarbat':array}");
        assertTrue(result.isArray());
        assertEquals(1, result.size());

        result = engine.expand("${3:array}");
        assertTrue(result.isArray());
        assertEquals(1, result.size());
        assertTrue(result.get(0).isTextual());
        assertEquals("3", result.get(0).asText());
    }

    @Test
    public void testArrayIf() throws Exception {
        Map<String,String> data = new HashMap<>();
        data.put("a", "a");
        TemplateExpander expander = new TemplateExpander();
        expander.template("---\n"+
        "- ${if:true}:\n"+
        "  - 1\n"+
        "  - 2\n"+
        "- ${if:true}: 3\n"+
        "- ${if:false}: 4\n"+
        "- ${if:true}:\n"+
        "    result: 5\n"+
        "- 6\n");
        expander.line(data);

        for (TemplateExpander.ExpanderResult result : expander.expand()) {
            if (result.success()) {
                assertTrue(result.expanded().isArray());
                assertEquals(5, result.expanded().size());
                assertTrue(result.expanded().get(3).isObject()); // the result: 5 node
                if (DIAGNOSTIC) Json.mapper.writeValue(System.out, result.expanded());
            } else {
                if (DIAGNOSTIC) result.exception().printStackTrace();
                fail(result.exception().getMessage());
            }
        }
    }

    @Test
    public void testObjectIf() throws Exception {
        Map<String,String> data = new HashMap<>();
        data.put("a", "a");
        TemplateExpander expander = new TemplateExpander();
        expander.template("---\n"+
        "${if:true}:\n"+
        "  field1: 1\n"+
        "  field2: 2\n"+
        "${if:true&&true}:\n"+ // can't have ${if:true} again!
        "  field3: 3\n"+
        "fixed4: 4\n"+
        "${if:false}:\n"+
        "  result: 5\n"+
        "fixed6: 6\n");
        expander.line(data);

        for (TemplateExpander.ExpanderResult result : expander.expand()) {
            if (result.success()) {
                assertTrue(result.expanded().isObject());
                assertEquals(5, result.expanded().size());
                if (DIAGNOSTIC) Json.mapper.writeValue(System.out, result.expanded());
            } else {
                if (DIAGNOSTIC) result.exception().printStackTrace();
                fail(result.exception().getMessage());
            }
        }
    }

}
