package com.streamcraft.shared.timederive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.shared.timederive.TimeDeriveConfig.Derivation;
import com.streamcraft.shared.timederive.TimeDeriveConfig.DerivationType;
import com.streamcraft.shared.timederive.TimeDeriveConfig.ParseErrorStrategy;
import com.streamcraft.shared.timederive.TimeDeriveConfig.SourceFormat;
import org.junit.jupiter.api.Test;

class TimeDeriveConfigParserTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void parseSupportsStructuredTimeDeriveOptions() throws Exception {
        TimeDeriveConfig config = TimeDeriveConfigParser.parse(json("""
                {
                  "sourceField": "eventTime",
                  "sourceFormat": "PATTERN",
                  "sourcePattern": "yyyy-MM-dd HH:mm:ss",
                  "sourceTimeZone": "Asia/Shanghai",
                  "outputTimeZone": "UTC",
                  "parseErrorStrategy": "SET_NULL",
                  "derivations": [
                    {"outputField": "year", "type": "YEAR"},
                    {"outputField": "week", "type": "WEEK"},
                    {"outputField": "epochSecond", "type": "EPOCH_SECONDS"},
                    {"outputField": "hourKey", "type": "FORMAT", "pattern": "yyyyMMddHH"}
                  ]
                }
                """), IllegalArgumentException::new);

        assertEquals("eventTime", config.sourceField());
        assertEquals(SourceFormat.PATTERN, config.sourceFormat());
        assertEquals("yyyy-MM-dd HH:mm:ss", config.sourcePattern());
        assertEquals("Asia/Shanghai", config.sourceTimeZone());
        assertEquals("UTC", config.outputTimeZone());
        assertEquals(ParseErrorStrategy.SET_NULL, config.parseErrorStrategy());
        assertEquals(4, config.derivations().size());
        assertEquals(DerivationType.YEAR, config.derivations().get(0).type());
        assertEquals(DerivationType.WEEK, config.derivations().get(1).type());
        assertEquals(DerivationType.EPOCH_SECONDS, config.derivations().get(2).type());
    }

    @Test
    void parseRejectsDuplicateOutputFields() throws Exception {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> TimeDeriveConfigParser.parse(json("""
                        {
                          "sourceField": "eventTime",
                          "derivations": [
                            {"outputField": "dt", "type": "DATE"},
                            {"outputField": "dt", "type": "HOUR"}
                          ]
                        }
                        """), IllegalArgumentException::new));

        assertEquals("Time derive config derivation outputField must be unique: dt", exception.getMessage());
    }

    @Test
    void parseRejectsMissingFormatPattern() throws Exception {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> TimeDeriveConfigParser.parse(json("""
                        {
                          "sourceField": "eventTime",
                          "derivations": [
                            {"outputField": "hourKey", "type": "FORMAT"}
                          ]
                        }
                        """), IllegalArgumentException::new));

        assertEquals("Time derive config pattern is required for PATTERN or FORMAT.", exception.getMessage());
    }

    private JsonNode json(String value) throws Exception {
        return objectMapper.readTree(value);
    }
}
