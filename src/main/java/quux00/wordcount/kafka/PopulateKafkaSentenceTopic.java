package quux00.wordcount.kafka;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class PopulateKafkaSentenceTopic {
  
  private static final String KAFKA_HOST_PORT = "localhost:9092";
  private static final String TOPIC = "sentences";
  private static final int NUM_TO_PUBLISH = 10000;
  
  private Producer<Object, String> kproducer;
  
  public static void main(String[] args) {
    PopulateKafkaSentenceTopic source = new PopulateKafkaSentenceTopic();
    source.init();
    source.populate(NUM_TO_PUBLISH);
    source.close();
  }

  private void init() {
    Properties props = new Properties();
    props.put("metadata.broker.list", KAFKA_HOST_PORT);
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("request.required.acks", "1");

    ProducerConfig config = new ProducerConfig(props);

    // first type: partition key
    // second type: type of message
    kproducer = new Producer<>(config);    
  }

  private void populate(int nSentences) {
    int idx = 0;
    for (int i = 0; i < nSentences; i++) {
      String sentence = SENTENCES[idx];
      
      kproducer.send(new KeyedMessage<>(TOPIC, sentence));
      
      idx++;
      if (idx >= SENTENCES.length) {
        idx = 0;
      }
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {}
    }
  }

  private void close() {
    kproducer.close();
  }

  static String[] SENTENCES = new String[]{
    "a is for apple",
    "b is for boy",
    "c is for charlie",
    "d is for a dog",
    "e is for an eagle",
    "f is for foo of course",
    "g is for goody goody gumdrop",
    "h is for haskell",
    "i is for idris",
    "j is for java",
    "k is for kotlin",
    "l is for latin squares",
    "m is for midnight",
    "n is for netty",
    "o is for ocaml",
    "and finally p is for python perl and that abdomination php"    
  }; 
}
