package college.scratchpad

import javax.jms.{Message, Session}

import org.apache.xbean.spring.context.ClassPathXmlApplicationContext
import org.springframework.jms.core.{JmsTemplate, MessageCreator}

/**
  * Created by saipkri on 30/08/16.
  */
object Trigger extends App {

  val requests = List(
    "{\"state\":\"delhi\", \"totalPageSize\":\"5\"}",
    "{\"state\":\"karnataka\", \"totalPageSize\":\"16\"}",
    "{\"state\":\"tamil-nadu\", \"totalPageSize\":\"62\"}"
  )
  val appContext = new ClassPathXmlApplicationContext("jmsContext.xml")
  val producer = appContext.getBean("queueTemplate").asInstanceOf[JmsTemplate]

  def send(rq: String) =
    producer.send("college.collector.input.queue", new MessageCreator {
      override def createMessage(session: Session): Message = session.createTextMessage(rq)
    })

  // Fire the requests!
  requests.foreach(send)
}
