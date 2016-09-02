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
class CollegeDetailsBolt extends BaseRichBolt {

  private[this] var outputCollector: OutputCollector = null
  private[this] var boltId: Int = 0

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = outputFieldsDeclarer.declare(new Fields("state", "name", "address", "courses", "city"))

  override def execute(tuple: Tuple): Unit = {
    val state = tuple.getStringByField("state")
    val name = tuple.getStringByField("name")
    val apiLink = tuple.getStringByField("apiLink")
    val addressAndCourses = CollegeCollector.nameAddressCourses(name, apiLink)
    val city = if (addressAndCourses._2.split(",").size > 2) addressAndCourses._2.split(",").reverse(2) else "not available"
    outputCollector.emit(tuple, new Values(state, addressAndCourses._1, addressAndCourses._2, addressAndCourses._3, city))
    outputCollector.ack(tuple)
  }

  override def prepare(map: util.Map[_, _], topologyContext: TopologyContext, outputCollector: OutputCollector): Unit = {
    this.outputCollector = outputCollector
    this.boltId = topologyContext.getThisTaskId
  }
}
