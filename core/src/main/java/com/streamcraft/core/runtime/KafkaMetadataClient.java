package com.streamcraft.core.runtime;

import java.io.IOException;
import java.util.List;

interface KafkaMetadataClient extends AutoCloseable {

    List<KafkaPartitionSplit> discover(KafkaSourceConfig config) throws Exception;

    @Override
    void close() throws IOException;
}
