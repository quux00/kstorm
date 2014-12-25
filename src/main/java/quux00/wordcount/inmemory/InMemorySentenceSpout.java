package quux00.wordcount.inmemory;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class InMemorySentenceSpout extends BaseRichSpout {

  private static final long serialVersionUID = -462261441867084611L;
  
  private SpoutOutputCollector collector;
  private String[] sentences = {
      "my dog has fleas",
      "i like cold beverages",
      "the dog ate my homework",
      "don't have a cow man",
      "i don't think i like fleas"
  };
  private int idx = 0;
  
  
  @Override
  public void open(@SuppressWarnings("rawtypes") Map cfg, TopologyContext topology, 
      SpoutOutputCollector outCollector) {
    collector = outCollector;
  }
  
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("sentence"));
  }

  @Override
  public void nextTuple() {
    collector.emit(new Values(sentences[idx]));
    idx++;
    if (idx >= sentences.length) {
      idx = 0;
    }
    Utils.sleep(1);
  }
}
