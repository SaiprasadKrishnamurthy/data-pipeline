package college.spouts

import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichSpout
import org.apache.storm.tuple.{Fields, Values}

/**
  * Created by saipkri on 30/08/16.
  */
class CollegeCollectorInputSpout extends BaseRichSpout {
  private var _collector: SpoutOutputCollector = null

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("state", "totalPageSize"))
  }

  override def open(map: java.util.Map[_, _], topologyContext: TopologyContext, spoutOutputCollector: SpoutOutputCollector) {
    _collector = spoutOutputCollector
  }

  override def nextTuple {
    _collector.emit(new Values("delhi", new Integer(5)))
  }
}
