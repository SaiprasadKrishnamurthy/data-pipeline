package college.jms

import javax.jms.{ConnectionFactory, Destination}

import org.apache.storm.jms.JmsProvider
import org.springframework.context.ApplicationContext
import org.springframework.context.support.ClassPathXmlApplicationContext

class AppJmsProvider extends JmsProvider {
  private[this] var _connectionFactory: ConnectionFactory = null
  private[this] var _destination: Destination = null

  /**
    * Constructs a <code>SpringJmsProvider</code> object given the name of a
    * classpath resource (the spring application context file), and the bean
    * names of a JMS connection factory and destination.
    *
    * @param appContextClasspathResource - the spring configuration file (classpath resource)
    * @param connectionFactoryBean       - the JMS connection factory bean name
    * @param destinationBean             - the JMS destination bean name
    */
  def this(appContextClasspathResource: String, connectionFactoryBean: String, destinationBean: String) {
    this()
    val context: ApplicationContext = new ClassPathXmlApplicationContext(appContextClasspathResource)
    this._connectionFactory = context.getBean(connectionFactoryBean).asInstanceOf[ConnectionFactory]
    this._destination = context.getBean(destinationBean).asInstanceOf[Destination]
  }

  override def connectionFactory(): ConnectionFactory = this._connectionFactory

  override def destination(): Destination = this._destination
}