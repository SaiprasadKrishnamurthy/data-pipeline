package college.bolts

import java.util

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.{Fields, Tuple, Values}

/**
  * Created by saipkri on 30/08/16.
  */
class InputReceiverBolt extends BaseRichBolt {

  private[this] var outputCollector: OutputCollector = null
  private[this] var boltId: Int = 0
  private[this] val mapper = new ObjectMapper()

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = outputFieldsDeclarer.declare(new Fields("state", "totalPageSize"))

  override def execute(tuple: Tuple): Unit = {
    val request = tuple.getStringByField("request")
    def requestAsMap = mapper.readValue(request, classOf[java.util.Map[String, String]])
    outputCollector.emit(tuple, new Values(requestAsMap.get("state"), requestAsMap.get("totalPageSize")))
    outputCollector.ack(tuple)
  }

  override def prepare(map: util.Map[_, _], topologyContext: TopologyContext, outputCollector: OutputCollector): Unit = {
    this.outputCollector = outputCollector
    this.boltId = topologyContext.getThisTaskId
  }
}
