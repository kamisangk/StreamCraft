package com.streamcraft.core.runtime;
import java.io.IOException; import java.util.*; import org.apache.flink.api.connector.source.*;
final class InfluxDbSourceEnumerator implements SplitEnumerator<InfluxDbSourceSplit,InfluxDbSourceEnumeratorState>{
 private final SplitEnumeratorContext<InfluxDbSourceSplit> context; private InfluxDbSourceSplit pending; private final Set<Integer> waiting=new TreeSet<>();
 InfluxDbSourceEnumerator(SplitEnumeratorContext<InfluxDbSourceSplit> c,InfluxDbSourceEnumeratorState s){context=c;pending=s==null?null:s.pendingSplit();}
 public void start(){assign();} public void handleSplitRequest(int id,String h){waiting.add(id);assign();} public void addReader(int id){waiting.add(id);assign();}
 public void addSplitsBack(List<InfluxDbSourceSplit> s,int id){if(!s.isEmpty())pending=s.get(0);waiting.add(id);assign();} public InfluxDbSourceEnumeratorState snapshotState(long id){return new InfluxDbSourceEnumeratorState(pending);} public void close() throws IOException{}
 private void assign(){if(pending==null)return; Map<Integer,?> readers=context.registeredReaders(); Integer id=waiting.stream().filter(readers::containsKey).findFirst().orElse(readers.keySet().stream().sorted().findFirst().orElse(null)); if(id==null)return; context.assignSplits(new SplitsAssignment<>(Map.of(id,List.of(pending)))); pending=null; waiting.remove(id);}
}
