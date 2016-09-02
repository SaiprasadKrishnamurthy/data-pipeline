package college.bolts

import java.util

import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.{Fields, Tuple, Values}

/**
  * Created by saipkri on 30/08/16.
  */
class InputSplitBolt extends BaseRichBolt {

  private[this] var outputCollector: OutputCollector = null
  private[this] var boltId: Int = 0

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = outputFieldsDeclarer.declare(new Fields("state", "pageNumber"))

  override def execute(tuple: Tuple): Unit = {
    val state = tuple.getStringByField("state")
    val totalPageSize = tuple.getStringByField("totalPageSize").toInt
    (1 until totalPageSize.toInt).foreach(pageNumber => outputCollector.emit(tuple, new Values(state, new Integer(pageNumber))))
    outputCollector.ack(tuple)
  }

  override def prepare(map: util.Map[_, _], topologyContext: TopologyContext, outputCollector: OutputCollector): Unit = {
    this.outputCollector = outputCollector
    this.boltId = topologyContext.getThisTaskId
  }
}
