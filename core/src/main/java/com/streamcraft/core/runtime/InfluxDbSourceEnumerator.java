package com.streamcraft.core.runtime;
import java.io.IOException; import java.util.*; import org.apache.flink.api.connector.source.*;
final class InfluxDbSourceEnumerator implements SplitEnumerator<InfluxDbSourceSplit,InfluxDbSourceEnumeratorState>{
 private final SplitEnumeratorContext<InfluxDbSourceSplit> context; private final ArrayDeque<InfluxDbSourceSplit> pending=new ArrayDeque<>(); private final Set<Integer> waiting=new TreeSet<>(); private final boolean bounded; private final Set<Integer> signaled=new HashSet<>();
 InfluxDbSourceEnumerator(SplitEnumeratorContext<InfluxDbSourceSplit> c,InfluxDbSourceEnumeratorState s,com.streamcraft.shared.influxdb.InfluxDbSourceConfig cfg){context=c;bounded=cfg.readMode()==com.streamcraft.shared.influxdb.InfluxDbSourceConfig.ReadMode.FULL;if(s!=null)pending.addAll(s.pendingSplits());}
 public void start(){assign();} public void handleSplitRequest(int id,String h){waiting.add(id);assign();} public void addReader(int id){waiting.add(id);assign();}
 public void addSplitsBack(List<InfluxDbSourceSplit> s,int id){if(s!=null)s.stream().sorted(Comparator.comparing(InfluxDbSourceSplit::splitId)).forEach(pending::addFirst);waiting.add(id);assign();}
 public InfluxDbSourceEnumeratorState snapshotState(long id){return new InfluxDbSourceEnumeratorState(new ArrayList<>(pending));} public void close()throws IOException{}
 private void assign(){if(pending.isEmpty()){signal();return;}Map<Integer,?> readers=context.registeredReaders();List<Integer> targets=waiting.stream().filter(readers::containsKey).toList();if(targets.isEmpty())targets=readers.keySet().stream().sorted().toList();if(targets.isEmpty())return;Map<Integer,List<InfluxDbSourceSplit>> a=new LinkedHashMap<>();int i=0;while(!pending.isEmpty()){InfluxDbSourceSplit s=pending.removeFirst();int t=targets.get(i++%targets.size());a.computeIfAbsent(t,k->new ArrayList<>()).add(s);}context.assignSplits(new SplitsAssignment<>(a));waiting.removeAll(a.keySet());signal();}
 private void signal(){if(!bounded||!pending.isEmpty())return;context.registeredReaders().keySet().stream().sorted().filter(signaled::add).forEach(context::signalNoMoreSplits);}
}
