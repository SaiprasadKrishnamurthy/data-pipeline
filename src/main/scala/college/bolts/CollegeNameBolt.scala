package college.bolts

import java.util

import college.collector.CollegeCollector
import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.{Fields, Tuple, Values}

/**
  * Created by saipkri on 30/08/16.
  */
class CollegeNameBolt extends BaseRichBolt {

  private[this] var outputCollector: OutputCollector = null
  private[this] var boltId: Int = 0

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = outputFieldsDeclarer.declare(new Fields("state", "name", "apiLink"))

  override def execute(tuple: Tuple): Unit = {
    val state = tuple.getStringByField("state")
    val pageNumber = tuple.getIntegerByField("pageNumber")
    val namesAndLinks = CollegeCollector.nameAndLinkPerPage(state, pageNumber)
    namesAndLinks.foreach(nameAndApiLink => outputCollector.emit(tuple, new Values(state, nameAndApiLink._1, nameAndApiLink._2)))
    outputCollector.ack(tuple)
  }

  override def prepare(map: util.Map[_, _], topologyContext: TopologyContext, outputCollector: OutputCollector): Unit = {
    this.outputCollector = outputCollector
    this.boltId = topologyContext.getThisTaskId
  }
}
