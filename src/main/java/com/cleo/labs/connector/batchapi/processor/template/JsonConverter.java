package com.cleo.labs.connector.batchapi.processor.template;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.cleo.labs.connector.batchapi.processor.Json;
import com.cleo.labs.connector.batchapi.processor.template.pojo.CSVAS2;
import com.cleo.labs.connector.batchapi.processor.template.pojo.CSVAction;
import com.cleo.labs.connector.batchapi.processor.template.pojo.CSVConnection;
import com.cleo.labs.connector.batchapi.processor.template.pojo.CSVFTP;
import com.cleo.labs.connector.batchapi.processor.template.pojo.CSVMailbox;
import com.cleo.labs.connector.batchapi.processor.template.pojo.CSVSFTP;
import com.cleo.labs.connector.batchapi.processor.template.pojo.JsonAS2;
import com.cleo.labs.connector.batchapi.processor.template.pojo.JsonAction;
import com.cleo.labs.connector.batchapi.processor.template.pojo.JsonFTP;
import com.cleo.labs.connector.batchapi.processor.template.pojo.JsonSFTP;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.opencsv.CSVReader;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.HeaderColumnNameMappingStrategy;


public class JsonConverter {

	public static String actionSeparatorRegex = "[\\|;]";

	public static enum  Type {
	    GROUP,
	    USER,
		AS2,
		SFTP,
		FTP
	}
	
    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

	public static ArrayNode parseCSVFile(String content) throws Exception {
	    ArrayNode file = mapper.createArrayNode();
        //try {
            switch (getCSVFileType(content)) {
            case AS2:
                {
                    List<JsonAS2> connections = createAS2Hosts(content);
                    for (JsonAS2 connection : connections) {
                        file.add(mapper.valueToTree(connection));
                    }
                }
                break;
            case SFTP:
                {
                    List<JsonSFTP> connections = createSFTPHosts(content);
                    for (JsonSFTP connection : connections) {
                        file.add(mapper.valueToTree(connection));
                    }
                }
                break;
            case FTP:
                {
                    List<JsonFTP> connections = createFTPHosts(content);
                    for (JsonFTP connection : connections) {
                        file.add(mapper.valueToTree(connection));
                    }
                }
                break;
            case GROUP:
                CSVReader grpReader = new CSVReader(new StringReader(content));
                List<String[]> grpElements = grpReader.readAll();
                for (String[] lines : grpElements) {
                    if (!lines[0].equals("UserAlias")) {
                        file.add(constructAuthenticator(lines));
                    }
                }
                break;
            case USER:
                HeaderColumnNameMappingStrategy<CSVMailbox> mailboxStrategy = new HeaderColumnNameMappingStrategy<>();
                mailboxStrategy.setType(CSVMailbox.class);
                CSVReader reader2 = new CSVReader(new StringReader(content));
                CsvToBean<CSVMailbox> csvToBean = new CsvToBean<>();
                csvToBean.setCsvReader(reader2);
                csvToBean.setMappingStrategy(mailboxStrategy);
                List<CSVMailbox> mailboxCSVList = csvToBean.parse();

                for (CSVMailbox mailboxCSV : mailboxCSVList) {
                    file.add(constructUser(mailboxCSV));
                }
                break;
            }
        //} catch (Exception e) {
            //System.out.println("error: skipping file: "+e.getMessage());
        //}
	    return file;
	}

    public static Type getCSVFileType(String content) throws Exception {
        String line = content.split("\n", 2)[0];
        if (line.toLowerCase().startsWith("useralias")) {
            return Type.GROUP;
        } else if (!line.toLowerCase().contains("type")) {
            return Type.USER;
        }

        List<CSVConnection> clientCsvList;
        try (StringReader r = new StringReader(content)) {
            HeaderColumnNameMappingStrategy<CSVConnection> mailboxStrategy = new HeaderColumnNameMappingStrategy<>();
            mailboxStrategy.setType(CSVConnection.class);
            CSVReader reader2 = new CSVReader(r);
            CsvToBean<CSVConnection> csvToBean = new CsvToBean<>();
            csvToBean.setCsvReader(reader2);
            csvToBean.setMappingStrategy(mailboxStrategy);
            clientCsvList = csvToBean.parse();
        }
        String type = null;
        for (CSVConnection clientHost : clientCsvList) {
            if (type == null) {
                type = clientHost.getType();
            } else {
                if (!clientHost.getType().equalsIgnoreCase(type)) {
                    throw new Exception("all connections must be the same type ("+type+")");
                }
            }
        }
        if (type.equalsIgnoreCase(Type.AS2.name())) {
            return Type.AS2;
        } else if (type.equalsIgnoreCase(Type.SFTP.name())) {
            return Type.SFTP;
        } else if (type.equalsIgnoreCase(Type.FTP.name())) {
            return Type.FTP;
        }
        throw new Exception("unrecognized type \""+type+"\"");
    }
	   
	private static <T> List<T> parseConnectionCSVFile(String content, Class<T> type) {
		com.cleo.labs.connector.batchapi.processor.template.CSVReader<T> reader;
		try {
			reader = new com.cleo.labs.connector.batchapi.processor.template.CSVReader<>(content, type);
			return reader.readFile();
		} catch (Exception e) {
			return new ArrayList<>();
		}
	}

	protected static List<JsonAS2> createAS2Hosts(String content) {
	    List<JsonAS2> connections = new ArrayList<>();
		for (CSVAS2 csv : parseConnectionCSVFile(content, CSVAS2.class)) {
			JsonAS2 as2Host = new JsonAS2();
			as2Host.connection = csv.getAlias();
			as2Host.connect.url = csv.getUrl();
			as2Host.localName = csv.getAS2From();
			as2Host.partnerName = csv.getAS2To();
			as2Host.outgoing.subject = csv.getSubject();
			as2Host.outgoing.encrypt = Boolean.valueOf(csv.getEncrypted());
			as2Host.outgoing.sign = Boolean.valueOf(csv.getSigned());
			as2Host.outgoing.receipt.type = csv.getReceipt_type();
			as2Host.outgoing.receipt.sign = Boolean.valueOf(csv.getReceipt_sign());
			as2Host.outgoing.storage.outbox = csv.getOutbox();
			as2Host.outgoing.storage.sentbox = csv.getSentbox();
			as2Host.incoming.storage.inbox = csv.getInbox();
			as2Host.incoming.storage.receivedbox = csv.getReceivedbox();
			Map<String,JsonAction> actions = new HashMap<>();
			if (csv.getCreateSendName() != null && !csv.getCreateSendName().isEmpty()
				&& csv.getActionSend() != null && ! csv.getActionSend().isEmpty()){
				JsonAction sendAction = new JsonAction();
				sendAction.alias = csv.getCreateSendName();
				sendAction.commands = csv.getActionSend().split(actionSeparatorRegex);
				sendAction.schedule = "on file continuously"; //TODO:
				actions.put(sendAction.alias, sendAction);
			}
			if (csv.getCreateReceiveName() != null && !csv.getCreateReceiveName().isEmpty()
							&& csv.getActionReceive() != null && ! csv.getActionReceive().isEmpty()){
				JsonAction recAction = new JsonAction();
				recAction.alias = csv.getCreateReceiveName();
				recAction.commands = csv.getActionReceive().split(actionSeparatorRegex);
				recAction.schedule = "on file continuously"; //TODO:
				actions.put(recAction.alias, recAction);
			}
			for (CSVAction action : csv.getActions()) {
				JsonAction actionPOJO = new JsonAction();
				actionPOJO.alias = action.getAlias();
				actionPOJO.commands = action.getCommands().split(actionSeparatorRegex);
				actionPOJO.schedule = "on file continuously"; // TODO: action.getSchedule();
				actions.put(actionPOJO.alias, actionPOJO);
			}
			as2Host.actions = actions;
			connections.add(as2Host);
		}
		return connections;
	}

	protected static List<JsonSFTP> createSFTPHosts(String content) {
	    List<JsonSFTP> connections = new ArrayList<>();
		for (CSVSFTP csv : parseConnectionCSVFile(content, CSVSFTP.class)) {
			JsonSFTP sftpHost = new JsonSFTP();
			sftpHost.connection = csv.getAlias();
			sftpHost.connect.host = csv.getHost();
			sftpHost.connect.port = csv.getPort();
			sftpHost.connect.username = csv.getUsername();
			sftpHost.connect.password = csv.getPassword();
			sftpHost.outgoing.storage.outbox = csv.getOutbox();
			sftpHost.outgoing.storage.sentbox = csv.getSentbox();
			sftpHost.incoming.storage.inbox = csv.getInbox();
			sftpHost.incoming.storage.receivedbox = csv.getReceivedbox();
			ArrayList<JsonAction> actions = new ArrayList<>();
			if (csv.getCreateSendName() != null && !csv.getCreateSendName().isEmpty()
							&& csv.getActionSend() != null && ! csv.getActionSend().isEmpty()){
				JsonAction sendAction = new JsonAction();
				sendAction.alias = csv.getCreateSendName();
				sendAction.commands = csv.getActionSend().split(actionSeparatorRegex);;
				actions.add(sendAction);
			}
			if (csv.getCreateReceiveName() != null && !csv.getCreateReceiveName().isEmpty()
							&& csv.getActionReceive() != null && ! csv.getActionReceive().isEmpty()){
				JsonAction recAction = new JsonAction();
				recAction.alias = csv.getCreateReceiveName();
				recAction.commands = csv.getActionReceive().split(actionSeparatorRegex);;
				actions.add(recAction);
			}
			sftpHost.actions = actions.toArray(new JsonAction[]{});
			connections.add(sftpHost);
		}
		return connections;
	}

	protected static List<JsonFTP> createFTPHosts(String content) {
	    List<JsonFTP> connections = new ArrayList<>();
		for (CSVFTP csv : parseConnectionCSVFile(content, CSVFTP.class)) {
			JsonFTP ftpHost = new JsonFTP();
			ftpHost.connection = csv.getAlias();
			ftpHost.connect.host = csv.getHost();
			ftpHost.connect.port = csv.getPort();
			ftpHost.connect.username = csv.getUsername();
			ftpHost.connect.password = csv.getPassword();
			ftpHost.connect.defaultContentType = csv.getDataType();
			ftpHost.connect.dataChannel.mode = csv.getChannelMode();
			ftpHost.connect.dataChannel.lowPort = csv.getActiveLowPort();
			ftpHost.connect.dataChannel.highPort = csv.getActiveHighPort();
			ftpHost.outgoing.storage.outbox = csv.getOutbox();
			ftpHost.outgoing.storage.sentbox = csv.getSentbox();
			ftpHost.incoming.storage.inbox = csv.getInbox();
			ftpHost.incoming.storage.receivedbox = csv.getReceivedbox();
			ArrayList<JsonAction> actions = new ArrayList<>();
			if (csv.getCreateSendName() != null && !csv.getCreateSendName().isEmpty()
							&& csv.getActionSend() != null && ! csv.getActionSend().isEmpty()){
				JsonAction sendAction = new JsonAction();
				sendAction.alias = csv.getCreateSendName();
				sendAction.commands = csv.getActionSend().split(actionSeparatorRegex);;
				actions.add(sendAction);
			}
			if (csv.getCreateReceiveName() != null && !csv.getCreateReceiveName().isEmpty()
							&& csv.getActionReceive() != null && ! csv.getActionReceive().isEmpty()){
				JsonAction recAction = new JsonAction();
				recAction.alias = csv.getCreateReceiveName();
				recAction.commands = csv.getActionReceive().split(actionSeparatorRegex);;
				actions.add(recAction);
			}
			ftpHost.actions = actions.toArray(new JsonAction[]{});
			connections.add(ftpHost);
		}
		return connections;
	}

    private static ObjectNode loadTemplate(String template) throws Exception {
        return (ObjectNode) mapper.readTree(Resources.toString(Resources.getResource(template), Charsets.UTF_8));
    }

    private static ObjectNode constructAuthenticator(String[] line) throws Exception {
        ObjectNode authenticator = loadTemplate("csv.template.group.yaml");
        authenticator.put("authenticator", line[0]);
        Json.setSubElement(authenticator, "resourceFolder", line[1]);
        Json.setSubElement(authenticator, "home.dir.default", line[2]);
        // process subfolders
        JsonNode existing = authenticator.path("home").path("subfolders").path("default");
        ArrayNode subfolders = existing.isArray() ? (ArrayNode)existing : mapper.createArrayNode();
        if(!line[3].isEmpty()) {
            subfolders.add( mapper.createObjectNode()
                    .put("usage", "download")
                    .put("path", line[3]));
        }
        if(!line[4].isEmpty()) {
            subfolders.add( mapper.createObjectNode()
                    .put("usage", "upload")
                    .put("path", line[4]));
        }
        if(!line[5].isEmpty()) {
            for(String path :line[5].split(";")) {
                subfolders.add( mapper.createObjectNode()
                        .put("usage", "other")
                        .put("path", path));
            }
        }
        if (subfolders.size() > 0) {
            Json.setSubElement(authenticator, "home.subfolders.default", subfolders);
        }
        // done with subfolders
        Json.setSubElement(authenticator, "outgoing.storage.sentbox", line[6]);
        Json.setSubElement(authenticator, "incoming.storage.receivedbox", line[7]);
        if (line[8].equals("Yes")) {
            Json.setSubElement(authenticator, "accept.ftp.enabled", true);
        }
        if (line[9].equals("Yes")) {
            Json.setSubElement(authenticator, "accept.sftp.enabled", true);
        }
        if (line[10].equals("Yes")) {
            Json.setSubElement(authenticator, "accept.http.enabled", true);
        }
        Json.setSubElement(authenticator, "home.access", line[11].toLowerCase());
        return authenticator;
    }

    private static ObjectNode constructActions(CSVMailbox mailboxCSV) {
        String actionSeparatorRegex = "[\\|;]";
        String collectAlias = mailboxCSV.getCreateCollectName();
        String[] collectCommands = mailboxCSV.getActionCollect().split(actionSeparatorRegex);
        String receiveAlias = mailboxCSV.getCreateReceiveName();
        String[] receiveCommands = mailboxCSV.getActionReceive().split(actionSeparatorRegex);
        ObjectNode actions = mapper.createObjectNode();

        if (!collectAlias.equalsIgnoreCase("NA")) {
            ObjectNode action = mapper.createObjectNode();
            action.put("alias", collectAlias);
            ArrayNode commands = action.putArray("commands");
            for (String command : collectCommands) commands.add(command);
            String schedule = mailboxCSV.getSchedule_Collect();
            if (!schedule.isEmpty() && !schedule.equalsIgnoreCase("none") && !schedule.equalsIgnoreCase("no")) {
                if (schedule.equalsIgnoreCase("polling")) {
                    action.put("schedule", "on file continuously");
                } else {
                    action.put("schedule", schedule);
                }
            }
            actions.set(collectAlias, action);
        }

        if (!receiveAlias.equalsIgnoreCase("NA")) {
            ObjectNode action = mapper.createObjectNode();
            action.put("alias", receiveAlias);
            ArrayNode commands = action.putArray("commands");
            for (String command : receiveCommands) commands.add(command);
            String schedule = mailboxCSV.getSchedule_Receive();
            if (!schedule.isEmpty() && !schedule.equalsIgnoreCase("none") && !schedule.equalsIgnoreCase("no")) {
                if (schedule.equalsIgnoreCase("polling")) {
                    action.put("schedule", "on file continuously");
                } else {
                    action.put("schedule", schedule);
                }
            }
            actions.set(receiveAlias, action);
        }

        return actions;
    }

    private static ObjectNode constructUser(CSVMailbox mailboxCSV) throws Exception {
        ObjectNode user = loadTemplate("csv.template.user.yaml");
        user.put("authenticator", mailboxCSV.getHost());
        user.put("username", mailboxCSV.getUserID());
        Json.setSubElement(user, "accept.password", mailboxCSV.getPassword());
        if (mailboxCSV.getDefaultHomeDir().equalsIgnoreCase("Yes")) {
            Json.setSubElement(user, "home.dir.default", mailboxCSV.getCustomHomeDir());
        } else {
            Json.setSubElement(user, "home.dir.override", mailboxCSV.getCustomHomeDir());
        }
        if (!mailboxCSV.getWhitelistIP().isEmpty()) {
            ArrayNode whitelist = mapper.createArrayNode();
            for (String ipaddr : mailboxCSV.getWhitelistIP().split(";")) {
                whitelist.add(mapper.createObjectNode().put("ipAddress", ipaddr));
            }
            Json.setSubElement(user, "accept.whitelist", whitelist);
        }
        Json.setSubElement(user, "notes", mailboxCSV.getHostNotes());
        if (!mailboxCSV.getOtherFolder().isEmpty()) {
            JsonNode existing = user.path("home").path("subfolders").path("default");
            ArrayNode subfolders = existing.isArray() ? (ArrayNode)existing : mapper.createArrayNode();
            for (String path : mailboxCSV.getOtherFolder().split(";")) {
                subfolders.add( mapper.createObjectNode()
                        .put("usage", "other")
                        .put("path", path));
            }
            Json.setSubElement(user, "home.subfolders.default", subfolders);
        }
        Json.setSubElement(user, "email", mailboxCSV.getEmail());
        ObjectNode actions = constructActions(mailboxCSV);
        if (actions.size() > 0) {
            user.replace("actions", actions);
        }
        return user;
    }
}
