package com.streamcraft.core.runtime;
import java.io.Serializable;
record InfluxDbSourceEnumeratorState(java.util.List<InfluxDbSourceSplit> pendingSplits) implements Serializable {
 private static final long serialVersionUID = 1L;
 InfluxDbSourceEnumeratorState{pendingSplits=pendingSplits==null?java.util.List.of():java.util.List.copyOf(pendingSplits);}
 InfluxDbSourceEnumeratorState(InfluxDbSourceSplit pendingSplit){this(pendingSplit==null?java.util.List.of():java.util.List.of(pendingSplit));}
 InfluxDbSourceSplit pendingSplit(){return pendingSplits.isEmpty()?null:pendingSplits.get(0);}
}
