package com.streamcraft.shared.dataquality;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class DataQualityConfigParserTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void parsesValidDataQualityConfigWithDefaults() {
        DataQualityConfig config = assertDoesNotThrow(() -> DataQualityConfigParser.parse(
                objectMapper.readTree("""
                        {
                          "rules": [
                            {
                              "field": "age",
                              "ruleType": "NOT_NULL",
                              "customMessage": "age is required"
                            },
                            {
                              "field": "age",
                              "ruleType": "TYPE",
                              "valueType": "INTEGER"
                            },
                            {
                              "field": "age",
                              "ruleType": "RANGE",
                              "min": 18,
                              "max": 65
                            },
                            {
                              "field": "status",
                              "ruleType": "ENUM",
                              "enumValues": ["active", "paused"]
                            },
                            {
                              "field": "status",
                              "ruleType": "REGEX",
                              "pattern": "^[a-z]+$"
                            },
                            {
                              "field": "comment",
                              "ruleType": "LENGTH",
                              "minLength": 2,
                              "maxLength": 120
                            }
                          ]
                        }
                        """),
                IllegalArgumentException::new));

        assertEquals(DataQualityConfig.Mode.DIRTY_PORT, config.mode());
        assertEquals(DataQualityConfig.DEFAULT_ERROR_FIELD, config.errorField());
        assertEquals(6, config.rules().size());
        assertEquals("age", config.rules().get(0).field());
        assertEquals(DataQualityConfig.RuleType.NOT_NULL, config.rules().get(0).ruleType());
        assertEquals("age is required", config.rules().get(0).customMessage());
        assertEquals("age", config.rules().get(1).field());
        assertEquals(DataQualityConfig.RuleType.TYPE, config.rules().get(1).ruleType());
        assertEquals(DataQualityConfig.ValueType.INTEGER, config.rules().get(1).valueType());
        assertEquals(2, config.rules().get(5).minLength());
        assertEquals(120, config.rules().get(5).maxLength());
    }

    @Test
    void parsesFailModeAndAllowsMultipleRulesForSameField() {
        DataQualityConfig config = assertDoesNotThrow(() -> DataQualityConfigParser.parse(
                objectMapper.readTree("""
                        {
                          "mode": "FAIL",
                          "rules": [
                            {
                              "field": "amount",
                              "ruleType": "NOT_NULL"
                            },
                            {
                              "field": "amount",
                              "ruleType": "RANGE",
                              "min": 0
                            }
                          ]
                        }
                        """),
                IllegalArgumentException::new));

        assertEquals(DataQualityConfig.Mode.FAIL, config.mode());
        assertEquals(2, config.rules().size());
        assertEquals("amount", config.rules().get(0).field());
        assertEquals("amount", config.rules().get(1).field());
    }

    @Test
    void rejectsEmptyRuleList() throws Exception {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> DataQualityConfigParser.parse(
                        objectMapper.readTree("""
                                {
                                  "rules": []
                                }
                                """),
                        IllegalArgumentException::new));

        assertTrue(exception.getMessage().contains("rules"));
    }

    @Test
    void rejectsInvalidRulePattern() throws Exception {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> DataQualityConfigParser.parse(
                        objectMapper.readTree("""
                                {
                                  "rules": [
                                    {
                                      "field": "status",
                                      "ruleType": "REGEX",
                                      "pattern": "["
                                    }
                                  ]
                                }
                                """),
                        IllegalArgumentException::new));

        assertTrue(exception.getMessage().contains("pattern"));
    }
}
