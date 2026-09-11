package com.streamcraft.core.runtime;
import com.streamcraft.core.model.DataEntity;
import com.streamcraft.shared.influxdb.InfluxDbSourceConfig;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
final class InfluxDbSource implements Source<DataEntity, InfluxDbSourceSplit, InfluxDbSourceEnumeratorState> {
    private static final long serialVersionUID = 1L; private final InfluxDbSourceConfig config;
    InfluxDbSource(InfluxDbSourceConfig config){this.config=config;}
    public Boundedness getBoundedness(){return config.readMode()==InfluxDbSourceConfig.ReadMode.FULL?Boundedness.BOUNDED:Boundedness.CONTINUOUS_UNBOUNDED;}
    public SourceReader<DataEntity,InfluxDbSourceSplit> createReader(SourceReaderContext c){return new InfluxDbSourceReader(c,config);}
    public SplitEnumerator<InfluxDbSourceSplit,InfluxDbSourceEnumeratorState> createEnumerator(SplitEnumeratorContext<InfluxDbSourceSplit> c){return new InfluxDbSourceEnumerator(c,initial());}
    public SplitEnumerator<InfluxDbSourceSplit,InfluxDbSourceEnumeratorState> restoreEnumerator(SplitEnumeratorContext<InfluxDbSourceSplit> c, InfluxDbSourceEnumeratorState s){return new InfluxDbSourceEnumerator(c,s==null?initial():s);}
    public SimpleVersionedSerializer<InfluxDbSourceSplit> getSplitSerializer(){return new InfluxDbSourceSplitSerializer();}
    public SimpleVersionedSerializer<InfluxDbSourceEnumeratorState> getEnumeratorCheckpointSerializer(){return new InfluxDbSourceEnumeratorStateSerializer();}
    private InfluxDbSourceEnumeratorState initial(){return new InfluxDbSourceEnumeratorState(new InfluxDbSourceSplit("influxdb-source",new IncrementalSourceState(config.cursorType().name(),config.initialCursorValue().isBlank()?null:config.initialCursorValue(),config.idField().isBlank()?null:config.idField(),null),0));}
}
