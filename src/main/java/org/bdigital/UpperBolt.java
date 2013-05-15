package org.bdigital;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class UpperBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    private Logger log = LoggerFactory.getLogger(UpperBolt.class);
    
    private OutputCollector collector; // El Rich te un collector unic

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
	
	this.collector = collector; // rebem el principi el mateix collector que es fara servir per totes les tuples

    }

    public void execute(Tuple input) {
	
	this.collector.emit(	input, // Afegim la tupla antiga per enclarla i afegir a la cadena de fiabilitat
				new Values(	input.getValue(0), // posem tupla nova
					input.getValue(1),
					input.getString(2).toUpperCase() // passem missatge a majuscules (p ex)
					));
	if (Math.random() < 0.9){
	    collector.ack(input);
	}

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	// definim tupla
	declarer.declare(new Fields("id", "timestamp", "message"));

    }

}
