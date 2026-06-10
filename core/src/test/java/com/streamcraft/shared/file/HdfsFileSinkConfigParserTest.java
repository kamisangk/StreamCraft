package com.streamcraft.shared.file;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.junit.jupiter.api.Test;

class HdfsFileSinkConfigParserTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void parsesSeaTunnelStyleHdfsSinkOptions() throws Exception {
        HdfsFileSinkConfig config = HdfsFileSinkConfigParser.parse(json("""
                {
                  "fs.defaultFS": "hdfs://nameservice1",
                  "path": "/warehouse/dwd/orders",
                  "tmp_path": "/tmp/streamcraft/orders",
                  "file_format_type": "json",
                  "sink_columns": ["order_id", "amount", "dt"],
                  "partition_by": ["dt"],
                  "partition_dir_expression": "${dt}",
                  "is_partition_field_write_in_file": false,
                  "custom_filename": true,
                  "file_name_expression": "orders-${now}",
                  "filename_time_format": "yyyyMMddHHmm",
                  "batch_size": 200,
                  "field_delimiter": "|",
                  "row_delimiter": "\\n",
                  "encoding": "UTF-8",
                  "compress_codec": "none"
                }
                """), IllegalArgumentException::new);

        assertEquals("hdfs://nameservice1", config.defaultFs());
        assertEquals("/warehouse/dwd/orders", config.path());
        assertEquals("/tmp/streamcraft/orders", config.tmpPath());
        assertEquals(HdfsFileFormatType.JSON, config.fileFormatType());
        assertEquals(List.of("order_id", "amount", "dt"), config.sinkColumns());
        assertEquals(List.of("dt"), config.partitionBy());
        assertEquals("${dt}", config.partitionDirExpression());
        assertFalse(config.partitionFieldWriteInFile());
        assertTrue(config.customFilename());
        assertEquals("orders-${now}", config.fileNameExpression());
        assertEquals("yyyyMMddHHmm", config.filenameTimeFormat());
        assertEquals(200, config.batchSize());
        assertEquals("|", config.fieldDelimiter());
        assertEquals("\n", config.rowDelimiter());
    }

    @Test
    void rejectsSinkWithoutPath() throws Exception {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> HdfsFileSinkConfigParser.parse(json("""
                        {
                          "fs.defaultFS": "hdfs://nameservice1",
                          "file_format_type": "json"
                        }
                        """), IllegalArgumentException::new));

        assertEquals("HDFS File Sink config path is required.", exception.getMessage());
    }

    @Test
    void rejectsDuplicateSinkColumns() throws Exception {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> HdfsFileSinkConfigParser.parse(json("""
                        {
                          "fs.defaultFS": "hdfs://nameservice1",
                          "path": "/warehouse/dwd/orders",
                          "file_format_type": "json",
                          "sink_columns": ["id", "id"]
                        }
                        """), IllegalArgumentException::new));

        assertTrue(exception.getMessage().contains("sink_columns"));
    }

    private JsonNode json(String value) throws Exception {
        return objectMapper.readTree(value);
    }
}
