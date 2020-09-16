package com.cleo.labs.connector.batchapi.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.google.common.base.Strings;

public class Json {

    public static final YAMLFactory yaml = new YAMLFactory()
            .configure(YAMLGenerator.Feature.MINIMIZE_QUOTES, true)
            .configure(YAMLGenerator.Feature.SPLIT_LINES, false);

    public static final ObjectMapper mapper = new ObjectMapper(yaml);

    public static JsonNode getSubElement(JsonNode node, String jsonKey) {
    	for (String key : jsonKey.split("\\.")) {
    		if (node != null) {
    			node = node.get(key);
    		}
    	}
    	return node;
    }

    public static String getSubElementAsText(JsonNode node, String jsonKey, String ifNull) {
    	JsonNode subElement = getSubElement(node, jsonKey);
    	return subElement == null ? ifNull : subElement.asText();
    }

    public static String getSubElementAsText(JsonNode node, String jsonKey) {
    	return getSubElementAsText(node, jsonKey, null);
    }

    public static String getHref(JsonNode node) {
        return getSubElementAsText(node, "_links.self.href");
    }

    public static String asText(JsonNode node) {
        return node == null ? null : node.asText();
    }

    public static String asText(JsonNode node, String ifNull) {
        return node == null ? ifNull : node.asText(ifNull);
    }

    public static int asInt(JsonNode node) {
        return node == null ? 0 : node.asInt();
    }

    public static int asInt(JsonNode node, int ifNull) {
        return node == null ? ifNull : node.asInt(ifNull);
    }

   public static ObjectNode walkToSubElement(ObjectNode node, String[] key) {
        ObjectNode current = node;
        for (int i=0; i<key.length-1; i++) {
            JsonNode test = current.path(key[i]);
            if (test.isObject()) {
                current = (ObjectNode)test;
            } else {
                current = node.putObject(key[i]);
            }
        }
        return current;
    }

    public static ObjectNode setSubElement(ObjectNode node, String jsonKey, String value) {
        if (!Strings.isNullOrEmpty(value)) {
            if (node == null) {
                node = mapper.createObjectNode();
            }
            String[] key = jsonKey.split("\\.");
            walkToSubElement(node, key).put(key[key.length-1], value);
        }
        return node;
    }

    public static ObjectNode setSubElement(ObjectNode node, String jsonKey, Integer value) {
        if (value != null) {
            if (node == null) {
                node = mapper.createObjectNode();
            }
            String[] key = jsonKey.split("\\.");
            walkToSubElement(node, key).put(key[key.length-1], value);
        }
        return node;
    }

    public static ObjectNode setSubElement(ObjectNode node, String jsonKey, Boolean value) {
        if (value != null) {
            if (node == null) {
                node = mapper.createObjectNode();
            }
            String[] key = jsonKey.split("\\.");
            walkToSubElement(node, key).put(key[key.length-1], value);
        }
        return node;
    }

    public static ObjectNode setSubElement(ObjectNode node, String jsonKey, JsonNode value) {
        if (value != null) {
            if (node == null) {
                node = mapper.createObjectNode();
            }
            String[] key = jsonKey.split("\\.");
            walkToSubElement(node, key).replace(key[key.length-1], value);
        }
        return node;
    }

    public static ObjectNode removeElements(ObjectNode node, String...elements) {
        for (String element : elements) {
            String[] kv = element.split("=", 2);
            String[] keys = kv[0].split("\\.");
            JsonNode value;
            try {
                value = kv.length > 1 ? mapper.readTree(kv[1]) : null;
            } catch (Exception e) {
                // swallow this exception since the "elements" are hard-coded, not up to the user
                throw new RuntimeException(e);
            }
            // walk down to the targeted node
            ObjectNode subnode = node;
            ObjectNode[] parent = new ObjectNode[keys.length];
            for (int i=0; i < keys.length-1 && subnode != null; i++) {
                JsonNode test = subnode.get(keys[i]);
                if (test != null && test.isObject()) {
                    parent[i] = subnode;
                    subnode = (ObjectNode)test;
                } else {
                    subnode = null;
                }
            }
            // if found, remove it and walk back up removing empty objects
            if (subnode != null) {
                if (value == null || value.equals(subnode.get(keys[keys.length-1]))) {
                    subnode.remove(keys[keys.length-1]);
                    for (int i=keys.length-2; !subnode.elements().hasNext() && i >= 0; i--) {
                        subnode = parent[i];
                        subnode.remove(keys[i]);
                    }
                }
            }
        }
        return node;
    }

}
