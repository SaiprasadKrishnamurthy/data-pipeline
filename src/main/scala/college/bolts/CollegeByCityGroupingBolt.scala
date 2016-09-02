package college.bolts

import java.util.concurrent.ConcurrentHashMap

import org.apache.storm.topology.base.BaseBasicBolt
import org.apache.storm.topology.{BasicOutputCollector, OutputFieldsDeclarer}
import org.apache.storm.tuple.{Fields, Tuple, Values}
import scala.collection.JavaConverters._

/**
  * Created by saipkri on 30/08/16.
  */
class CollegeByCityGroupingBolt extends BaseBasicBolt {

  private[this] val map = new ConcurrentHashMap[String, List[String]]().asScala

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = outputFieldsDeclarer.declare(new Fields("city", "partialCollegeGroup"))


  override def execute(tuple: Tuple, basicOutputCollector: BasicOutputCollector): Unit = {
    val city = tuple.getStringByField("city")
    val name = tuple.getStringByField("name")
    val added = map.putIfAbsent(city, List(name)).orElse(map.get(city))
    if (added.isDefined) map.put(city, added.get ::: List(name))
    map.foreach(kv => {
      println(s"City: ${kv._1}  (${kv._2.size})")
      println(s"\t ${kv._2.mkString("\n")}")
    })
    println("\n\n")
  }
}
