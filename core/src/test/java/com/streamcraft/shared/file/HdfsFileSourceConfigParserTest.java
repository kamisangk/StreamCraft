package com.streamcraft.shared.file;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.junit.jupiter.api.Test;

class HdfsFileSourceConfigParserTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void parsesSeaTunnelStyleHdfsSourceOptions() throws Exception {
        HdfsFileSourceConfig config = HdfsFileSourceConfigParser.parse(json("""
                {
                  "fs.defaultFS": "hdfs://nameservice1",
                  "path": "/warehouse/ods/orders",
                  "file_format_type": "csv",
                  "schema": {
                    "fields": {
                      "order_id": "string",
                      "amount": "double"
                    }
                  },
                  "field_delimiter": ",",
                  "row_delimiter": "\\n",
                  "skip_header_row_number": 1,
                  "encoding": "UTF-8",
                  "compress_codec": "none",
                  "parse_partition_from_path": true,
                  "file_filter_pattern": ".*\\\\.csv",
                  "readMode": "INCREMENTAL",
                  "pollIntervalMillis": 3000,
                  "maxPolls": 5,
                  "idField": "order_id",
                  "timestampField": "event_time",
                  "hdfs_site_path": "/etc/hadoop/conf/hdfs-site.xml",
                  "kerberos_principal": "streamcraft@EXAMPLE.COM",
                  "kerberos_keytab_path": "/etc/security/keytabs/streamcraft.keytab"
                }
                """), IllegalArgumentException::new);

        assertEquals("hdfs://nameservice1", config.defaultFs());
        assertEquals("/warehouse/ods/orders", config.path());
        assertEquals(HdfsFileFormatType.CSV, config.fileFormatType());
        assertEquals(List.of("order_id", "amount"), config.fieldNames());
        assertEquals(",", config.fieldDelimiter());
        assertEquals("\n", config.rowDelimiter());
        assertEquals(1, config.skipHeaderRowNumber());
        assertEquals("UTF-8", config.encoding());
        assertEquals("none", config.compressCodec());
        assertTrue(config.parsePartitionFromPath());
        assertEquals(".*\\.csv", config.fileFilterPattern());
        assertEquals(HdfsFileSourceConfig.ReadMode.INCREMENTAL, config.readMode());
        assertEquals(3000, config.pollIntervalMillis());
        assertEquals(5, config.maxPolls());
        assertEquals("order_id", config.idField());
        assertEquals("event_time", config.timestampField());
        assertEquals("/etc/hadoop/conf/hdfs-site.xml", config.hdfsSitePath());
        assertEquals("streamcraft@EXAMPLE.COM", config.kerberosPrincipal());
        assertEquals("/etc/security/keytabs/streamcraft.keytab", config.kerberosKeytabPath());
    }

    @Test
    void rejectsSourceWithoutDefaultFs() throws Exception {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> HdfsFileSourceConfigParser.parse(json("""
                        {
                          "path": "/warehouse/ods/orders",
                          "file_format_type": "json"
                        }
                        """), IllegalArgumentException::new));

        assertEquals("HDFS File Source config fs.defaultFS is required.", exception.getMessage());
    }

    @Test
    void rejectsInvalidFileFilterPattern() throws Exception {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> HdfsFileSourceConfigParser.parse(json("""
                        {
                          "fs.defaultFS": "hdfs://nameservice1",
                          "path": "/warehouse/ods/orders",
                          "file_format_type": "json",
                          "file_filter_pattern": "*["
                        }
                        """), IllegalArgumentException::new));

        assertTrue(exception.getMessage().contains("file_filter_pattern"));
    }

    private JsonNode json(String value) throws Exception {
        return objectMapper.readTree(value);
    }
}
