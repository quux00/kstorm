package quux00.wordcount.nonacking;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordCountBolt extends BaseRichBolt {

  private static final long serialVersionUID = 6843364678084556655L;
  private static final Long ONE = Long.valueOf(1);
  
  private OutputCollector collector;
  private Map<String, Long> countMap;
  
  @Override @SuppressWarnings("rawtypes")
  public void prepare(Map stormConf, TopologyContext context, OutputCollector outCollector) {
    collector = outCollector;
    countMap = new HashMap<>();
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word", "count"));
  }

  @Override
  public void execute(Tuple tuple) {
    String word = tuple.getString(0);
    Long cnt = countMap.get(word);
    if (cnt == null) {
      cnt = ONE;
    } else {
      cnt++;
    }
    countMap.put(word, cnt);
    collector.emit(new Values(word, cnt));
  }
}
