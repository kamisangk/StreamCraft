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
    public SplitEnumerator<InfluxDbSourceSplit,InfluxDbSourceEnumeratorState> createEnumerator(SplitEnumeratorContext<InfluxDbSourceSplit> c){return new InfluxDbSourceEnumerator(c,initial(),config);}
    public SplitEnumerator<InfluxDbSourceSplit,InfluxDbSourceEnumeratorState> restoreEnumerator(SplitEnumeratorContext<InfluxDbSourceSplit> c, InfluxDbSourceEnumeratorState s){return new InfluxDbSourceEnumerator(c,s==null?initial():s,config);}
    public SimpleVersionedSerializer<InfluxDbSourceSplit> getSplitSerializer(){return new InfluxDbSourceSplitSerializer();}
    public SimpleVersionedSerializer<InfluxDbSourceEnumeratorState> getEnumeratorCheckpointSerializer(){return new InfluxDbSourceEnumeratorStateSerializer();}
    private InfluxDbSourceEnumeratorState initial(){return new InfluxDbSourceEnumeratorState(splitPlan(config));}
    static java.util.List<InfluxDbSourceSplit> splitPlan(InfluxDbSourceConfig config){
        IncrementalSourceState state=new IncrementalSourceState(config.cursorType().name(),config.cursorField(),config.initialCursorValue().isBlank()?null:config.initialCursorValue(),config.idField().isBlank()?null:config.idField(),null);
        java.util.List<InfluxDbSourceSplit> result=new java.util.ArrayList<>();
        if(!config.splitEnabled()) return java.util.List.of(new InfluxDbSourceSplit("influxdb-source",state,0));
        if(!config.splitMeasurements().isEmpty()){
            int index=0; for(String measurement:config.splitMeasurements()) result.add(new InfluxDbSourceSplit("influxdb:measurement:"+measurement, state,index++,measurement,"","",false));
            return java.util.List.copyOf(result);
        }
        if(config.cursorType()!=InfluxDbSourceConfig.CursorType.LONG && config.cursorType()!=InfluxDbSourceConfig.CursorType.TIMESTAMP) throw new IllegalArgumentException("InfluxDB time range splitting requires LONG or TIMESTAMP cursorType.");
        long start=parseTime(config.splitStart(),config), end=parseTime(config.splitEnd(),config);
        if(end<=start) throw new IllegalArgumentException("InfluxDB splitEnd must be greater than splitStart.");
        long width=Math.max(1,(end-start)/config.splitCount());
        for(int i=0;i<config.splitCount();i++){long lo=i==0?start:start+width*i;long hi=i==config.splitCount()-1?end:start+width*(i+1);if(i>0)lo=Math.max(start,lo-config.splitOverlapMillis());result.add(new InfluxDbSourceSplit("influxdb:"+config.splitField()+":"+i+"/"+config.splitCount(),state,i,"",String.valueOf(lo),String.valueOf(hi),i==config.splitCount()-1));}
        return java.util.List.copyOf(result);
    }
    private static long parseTime(String value,InfluxDbSourceConfig config){try{return config.cursorType()==InfluxDbSourceConfig.CursorType.TIMESTAMP?java.time.Instant.parse(value).toEpochMilli():Long.parseLong(value);}catch(Exception e){throw new IllegalArgumentException("InfluxDB split bounds must be valid timestamps.",e);}}
}
