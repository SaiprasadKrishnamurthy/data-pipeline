package college.topology

import javax.jms.Session

import college.bolts._
import college.jms.{AppJmsProvider, JsonTupleProducer}
import college.spouts.CollegeCollectorInputSpout
import extensions.AppJmsSpout
import org.apache.storm.topology.TopologyBuilder
import org.apache.storm.tuple.Fields
import org.apache.storm.{Config, LocalCluster}

/**
  * Created by saipkri on 30/08/16.
  */
object CollegeCollectorTopology extends App {

  val noOfExecutors = Runtime.getRuntime.availableProcessors

  // JMS Queue Provider
  val jmsQueueProvider = new AppJmsProvider("jmsContext.xml", "jmsConnectionFactory", "notificationQueue")

  val config = new Config
  config.put(Config.TOPOLOGY_DEBUG, Boolean.box(false))
  config.put(Config.TOPOLOGY_WORKERS, new Integer(30))
  config.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, new Integer(10000))


  val builder = new TopologyBuilder

  val queueSpout = new AppJmsSpout()
  queueSpout.setJmsProvider(jmsQueueProvider)
  queueSpout.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE)
  queueSpout.setDistributed(true); // allow multiple instances
  queueSpout.setJmsTupleProducer(new JsonTupleProducer)
  builder.setSpout("queueSpout", queueSpout, noOfExecutors)

  val collegeCollectorInputSpout = new CollegeCollectorInputSpout
  val inputReceiverBolt = new InputReceiverBolt
  val inputSplitBolt = new InputSplitBolt
  val collegeNameBolt = new CollegeNameBolt
  val collegeDetailsBolt = new CollegeDetailsBolt
  val collegeByCityPartialGroupBolt = new CollegeByCityGroupingBolt
  val persistenceBolt = new PersistenceBolt

  // Wiring.
  builder.setBolt("inputReceiverBolt", inputReceiverBolt, noOfExecutors).shuffleGrouping("queueSpout")
  builder.setBolt("inputSplitBolt", inputSplitBolt, noOfExecutors).shuffleGrouping("inputReceiverBolt")
  builder.setBolt("collegeNameBolt", collegeNameBolt, noOfExecutors).shuffleGrouping("inputSplitBolt")
  builder.setBolt("collegeDetailsBolt", collegeDetailsBolt, noOfExecutors).shuffleGrouping("collegeNameBolt")
  builder.setBolt("collegeByCityPartialGroupBolt", collegeByCityPartialGroupBolt, 1).fieldsGrouping("collegeDetailsBolt", new Fields("city"))
  builder.setBolt("persistenceBolt", persistenceBolt, noOfExecutors).shuffleGrouping("collegeDetailsBolt")

  val cluster = new LocalCluster
  cluster.submitTopology("CollegeDetailCollector", config, builder.createTopology)

  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
    override def run(): Unit = cluster.shutdown
  }))
}
