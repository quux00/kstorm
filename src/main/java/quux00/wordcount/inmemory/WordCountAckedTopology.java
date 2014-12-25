package quux00.wordcount.inmemory;

import quux00.wordcount.acking.ReportBolt;
import quux00.wordcount.acking.SplitSentenceBolt;
import quux00.wordcount.acking.WordCountBolt;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class WordCountAckedTopology {
  
  private static final String SENTENCE_SPOUT_ID = "sentence-spout";
  private static final String SPLIT_BOLT_ID = "split-bolt";
  private static final String COUNT_BOLT_ID = "count-bolt";
  private static final String REPORT_BOLT_ID = "report-bolt";
  private static final String TOPOLOGY_NAME = "word-count-topology";

  public static void main(String[] args) throws Exception {
    InMemorySentenceSpout spout = new InMemorySentenceSpout();
    SplitSentenceBolt splitBolt = new SplitSentenceBolt();
    WordCountBolt countBolt = new WordCountBolt();
    ReportBolt reportBolt = new ReportBolt();
    
    TopologyBuilder builder = new TopologyBuilder();
    
    builder.setSpout(SENTENCE_SPOUT_ID, spout);
    builder.setBolt(SPLIT_BOLT_ID, splitBolt).shuffleGrouping(SENTENCE_SPOUT_ID);
    builder.setBolt(COUNT_BOLT_ID, countBolt).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
    builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLT_ID);
    
    Config cfg = new Config();
    
    boolean localMode = Boolean.parseBoolean(System.getProperty("localmode", "false"));
    
    if (localMode) {
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology(TOPOLOGY_NAME, cfg, builder.createTopology());
      Utils.sleep(10 * 1000);
      cluster.killTopology(TOPOLOGY_NAME);
      cluster.shutdown();
      
    } else {
      StormSubmitter.submitTopology(TOPOLOGY_NAME, cfg, builder.createTopology());
    }
  }
}
