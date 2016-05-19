package bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class KafkaWordSplitter extends BaseRichBolt {
/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	//	private static final Logger LOG = LoggerFactory.getLogger(KafkaWordSplitter.class);
	OutputCollector collector;

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		
	}

	public void execute(Tuple input) {
		System.out.println(input.getString(0));
//		LOG.debug(input.getString(0));
		this.collector.emit(new Values(input.getString(0)));  
		this.collector.ack(input);
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
