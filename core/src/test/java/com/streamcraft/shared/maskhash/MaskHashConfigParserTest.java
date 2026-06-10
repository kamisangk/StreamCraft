package com.streamcraft.shared.maskhash;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.shared.maskhash.MaskHashConfig.Action;
import com.streamcraft.shared.maskhash.MaskHashConfig.Algorithm;
import com.streamcraft.shared.maskhash.MaskHashConfig.Rule;
import org.junit.jupiter.api.Test;

class MaskHashConfigParserTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void parseUsesStudioMaskDefaultsWhenOptionalFieldsAreMissing() throws Exception {
        MaskHashConfig config = MaskHashConfigParser.parse(json("""
                {
                  "rules": [
                    {"sourceField": "phone", "targetField": "phoneMasked"}
                  ]
                }
                """), IllegalArgumentException::new);

        Rule rule = config.rules().get(0);
        assertEquals("phone", rule.sourceField());
        assertEquals("phoneMasked", rule.targetField());
        assertEquals(Action.MASK, rule.action());
        assertEquals(Algorithm.SHA256, rule.algorithm());
        assertEquals("*", rule.maskChar());
        assertEquals(3, rule.keepFirst());
        assertEquals(4, rule.keepLast());
    }

    @Test
    void parseRejectsMissingTargetField() throws Exception {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> MaskHashConfigParser.parse(json("""
                        {
                          "rules": [
                            {"sourceField": "phone"}
                          ]
                        }
                        """), IllegalArgumentException::new));

        assertEquals("Mask/hash config rule targetField is required.", exception.getMessage());
    }

    private JsonNode json(String value) throws Exception {
        return objectMapper.readTree(value);
    }
}
