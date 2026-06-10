package com.streamcraft.shared.casewhen;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class CaseWhenConfigParserTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void parseTreatsDefaultModeNoneAsNoDefaultValue() throws Exception {
        CaseWhenConfig config = CaseWhenConfigParser.parse(json("""
                {
                  "targetField": "bucket",
                  "cases": [
                    {"condition": "amount >= 100", "value": "large"}
                  ],
                  "defaultMode": "NONE",
                  "defaultValue": "",
                  "defaultExpression": ""
                }
                """), IllegalArgumentException::new);

        assertTrue(config.defaultValue().emptyValue());
    }

    @Test
    void parseSupportsDefaultExpressionMode() throws Exception {
        CaseWhenConfig config = CaseWhenConfigParser.parse(json("""
                {
                  "targetField": "bucket",
                  "cases": [
                    {"condition": "amount >= 100", "value": "large"}
                  ],
                  "defaultMode": "EXPRESSION",
                  "defaultExpression": "amount >= 50 ? 'medium' : 'small'"
                }
                """), IllegalArgumentException::new);

        assertEquals("amount >= 50 ? 'medium' : 'small'", config.defaultValue().expression());
    }

    @Test
    void parseRejectsDefaultValueModeWithoutValue() throws Exception {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> CaseWhenConfigParser.parse(json("""
                        {
                          "targetField": "bucket",
                          "cases": [
                            {"condition": "amount >= 100", "value": "large"}
                          ],
                          "defaultMode": "VALUE"
                        }
                        """), IllegalArgumentException::new));

        assertEquals("Case when config case value or expression is required.", exception.getMessage());
    }

    private JsonNode json(String value) throws Exception {
        return objectMapper.readTree(value);
    }
}
