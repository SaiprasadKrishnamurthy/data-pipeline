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
class PersistenceBolt extends BaseRichBolt {

  private[this] var outputCollector: OutputCollector = null
  private[this] var boltId: Int = 0

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = outputFieldsDeclarer.declare(new Fields("state", "name", "address", "courses"))

  override def execute(tuple: Tuple): Unit = {
    val state = tuple.getStringByField("state")
    val name = tuple.getStringByField("name")
    val address = tuple.getStringByField("address")
    val courses = tuple.getValueByField("courses").asInstanceOf[List[String]]
   /* println("\t\tState: " + state)
    println("\t\tName: " + name)
    println("\t\tAddress: " + address)
    println("\t\tCourses: " + courses)
    println(" --------------------------------------------\n ")*/
    outputCollector.ack(tuple)
  }

  override def prepare(map: util.Map[_, _], topologyContext: TopologyContext, outputCollector: OutputCollector): Unit = {
    this.outputCollector = outputCollector
    this.boltId = topologyContext.getThisTaskId
  }
}
