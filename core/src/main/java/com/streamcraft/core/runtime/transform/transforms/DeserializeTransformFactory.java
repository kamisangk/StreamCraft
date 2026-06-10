package com.streamcraft.core.runtime.transform.transforms;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.runtime.transform.TransformFactory;
import com.streamcraft.core.runtime.transform.TransformOutputs;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

public class DeserializeTransformFactory implements TransformFactory {

    @Override
    public TransformOutputs apply(DataStream<DataEntity> input, PipelineNode node) {
        String field = node.config().path("field").asText();
        String targetField = node.config().path("targetField").asText();
        String format = node.config().path("format").asText("JSON");
        String delimiter = node.config().path("delimiter").asText(",");
        List<String> fieldNames = new ArrayList<>();
        node.config().path("fieldNames").forEach(item -> fieldNames.add(item.asText()));

        return TransformOutputs.single(input.map(new RichMapFunction<DataEntity, DataEntity>() {
            private static final long serialVersionUID = 1L;
            private transient ObjectMapper objectMapper;

            @Override
            public void open(OpenContext openContext) {
                objectMapper = new ObjectMapper();
            }

            @Override
            public DataEntity map(DataEntity entity) throws Exception {
                Object value = entity.fields().get(field);
                if (!(value instanceof String stringValue)) {
                    return entity;
                }
                try {
                    Object parsed = parseValue(stringValue, format, fieldNames, delimiter);
                    if (parsed == null) {
                        return entity;
                    }
                    return entity.withField(targetField, parsed);
                } catch (Exception exception) {
                    return entity;
                }
            }
        }).name(node.name()));
    }

    private static Object parseValue(String value, String format, List<String> fieldNames, String delimiter)
            throws Exception {
        return switch (format) {
            case "JSON" -> parseJsonObject(value);
            case "KV" -> parseKv(value);
            case "CSV" -> parseCsv(value, fieldNames, delimiter);
            case "XML" -> parseXml(value);
            default -> null;
        };
    }

    private static Map<String, Object> parseJsonObject(String value) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode json = objectMapper.readTree(value);
        if (!json.isObject()) {
            return null;
        }
        return objectMapper.convertValue(json, new TypeReference<Map<String, Object>>() {});
    }

    private static Map<String, Object> parseKv(String value) {
        Map<String, Object> parsed = new LinkedHashMap<>();
        if (value.isBlank()) {
            return parsed;
        }
        String separator = value.contains("&") ? "&" : ",";
        for (String token : value.split(Pattern.quote(separator))) {
            String pair = token.trim();
            int equalsIndex = pair.indexOf('=');
            if (equalsIndex <= 0 || equalsIndex == pair.length() - 1) {
                return null;
            }
            String key = pair.substring(0, equalsIndex).trim();
            String itemValue = pair.substring(equalsIndex + 1).trim();
            if (key.isBlank()) {
                return null;
            }
            parsed.put(key, itemValue);
        }
        return parsed;
    }

    private static Map<String, Object> parseCsv(String value, List<String> fieldNames, String delimiter) {
        if (fieldNames == null || fieldNames.isEmpty()) {
            return null;
        }
        String[] values = value.split(Pattern.quote(resolveDelimiter(delimiter)), -1);
        if (values.length != fieldNames.size()) {
            return null;
        }
        Map<String, Object> parsed = new LinkedHashMap<>();
        for (int index = 0; index < fieldNames.size(); index++) {
            String fieldName = fieldNames.get(index) == null ? "" : fieldNames.get(index).trim();
            if (fieldName.isBlank()) {
                return null;
            }
            parsed.put(fieldName, values[index].trim());
        }
        return parsed;
    }

    private static String resolveDelimiter(String delimiter) {
        return delimiter == null || delimiter.isEmpty() ? "," : delimiter;
    }

    private static Map<String, Object> parseXml(String value) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(false);
        factory.setExpandEntityReferences(false);
        Document document = factory.newDocumentBuilder().parse(new InputSource(new StringReader(value)));
        Element root = document.getDocumentElement();
        if (root == null) {
            return null;
        }

        Map<String, Object> parsed = new LinkedHashMap<>();
        NodeList children = root.getChildNodes();
        for (int index = 0; index < children.getLength(); index++) {
            Node child = children.item(index);
            if (child.getNodeType() != Node.ELEMENT_NODE) {
                continue;
            }
            Element element = (Element) child;
            if (element.hasAttributes()) {
                return null;
            }
            if (hasNestedElement(element)) {
                return null;
            }
            parsed.put(element.getTagName(), element.getTextContent());
        }
        return parsed;
    }

    private static boolean hasNestedElement(Element element) {
        NodeList children = element.getChildNodes();
        for (int index = 0; index < children.getLength(); index++) {
            if (children.item(index).getNodeType() == Node.ELEMENT_NODE) {
                return true;
            }
        }
        return false;
    }
}
