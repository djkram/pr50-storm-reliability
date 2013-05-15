package org.bdigital;

import java.io.Serializable;
import java.text.MessageFormat;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * Spout - agafa dades de la font i ho tonra en tuples pels Bolts
 * 
 * @author mplanaguma
 * 
 */
public class FakeTweetSpout extends BaseRichSpout implements Serializable {

    private static final long serialVersionUID = 1L;

    private Logger log = LoggerFactory.getLogger(FakeTweetSpout.class);

    private static final Random rnd = new Random();
    private static final String[] MSSG = { "A", "B", "C", "D", "E" };
    private SpoutOutputCollector collector;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	// el primer que s'executa: info de topologia
	log.info("Opening...");
	this.collector = collector;
    }

    public void nextTuple() {
	// Bucle controlat per storm per anar generant tuples
	// agafarem les dades de kafka
	try {
	    Thread.sleep(rnd.nextInt(500) + 500); // alentim per veure sense ser
						  // Jedi
	} catch (InterruptedException e) {
	    log.error(e.getMessage());
	}

	// Enviem Tupla amb id legacy per fiabilitat
	int idx = rnd.nextInt(MSSG.length);
	String message = MSSG[idx];
	log.info(MessageFormat.format("E {0}", message));
	
	UUID id = UUID.randomUUID();
	collector.emit(
		new Values( // Primer parametre = Valor
			id, // aquest id no ens val per fiabilitat, només és un camp de la tupla
			System.currentTimeMillis(), 
			message),
		id+":"+idx // Segon Valor = ID per fiabilitat (p ex id tupla mes idx valor)
		);

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	// definim tuples
	declarer.declare(new Fields("id", "timestamp", "message"));

    }
    
    @Override
    public void fail(Object msgId) {
	
        String idxStr = (String) msgId; // idcomplet tupla Fail
        
        String id = idxStr.substring(0, idxStr.lastIndexOf(":")); // Recuperem part id
        
        idxStr = idxStr.substring(idxStr.lastIndexOf(":")+1); // recuperem part idx
        int idx = Integer.parseInt(idxStr);
        

        
        String message = MSSG[idx]; // recuperem missatge
        log.info(MessageFormat.format("R {0}", message));
        
        collector.emit(new Values(id,System.currentTimeMillis(),message), id+":"+idx); // Reenviem :)
    }

}
