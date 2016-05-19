package topology;

import java.util.Arrays;
import java.util.UUID;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import bolt.KafkaWordSplitter;
import bolt.WordCounter;

public class StormkafkaTopology {
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		TopologyBuilder builder = new TopologyBuilder();

		BrokerHosts hosts = new ZkHosts("172.28.20.103:2181,172.28.20.104:2181,172.28.20.105:2181");
//		SpoutConfig spoutConfig = new SpoutConfig(hosts, "test12", "/brokers", UUID.randomUUID().toString());
		SpoutConfig spoutConfig = new SpoutConfig(hosts, "test12", "/storm", UUID.randomUUID().toString());
		spoutConfig.scheme  = new SchemeAsMultiScheme(new StringScheme());
		spoutConfig.zkServers = Arrays.asList(new String[] {"172.28.20.103", "172.28.20.104", "172.28.20.105"});
		spoutConfig.zkPort = 2181;
		spoutConfig.ignoreZkOffsets = true;
		spoutConfig.startOffsetTime = -2;

		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		
		builder.setSpout("test", kafkaSpout);
		builder.setBolt("bolted", new KafkaWordSplitter()).shuffleGrouping("test");
//		builder.setBolt("Output", new WordCounter()).shuffleGrouping("bolted");
		Config conf = new Config();
		conf.setDebug(false);
		
//		LocalCluster cluster = new LocalCluster();
//		cluster.submitTopology("testTopology", conf, builder.createTopology());
//		try {
//			Thread.sleep(300000);
//		} catch (InterruptedException e) {
//		}
//		cluster.shutdown();
		
		//远程模式
		conf.setNumWorkers(20);
		conf.setMaxSpoutPending(5000);
		StormSubmitter.submitTopology("kafka_topology", conf, builder.createTopology());
		
	}
}
