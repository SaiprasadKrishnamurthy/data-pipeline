A simple data processing pipeline using Storm
=============================================

The problem is this:

I want to collect the college details in India (State-Wise).

The college details are:

College Name, Address, List of courses offered.

The college details are obtained through a simple scrapping service from an external website.

This is an example of the process that is IO intensive (mostly). As it makes a lot of calls to the external website and waits until the response.

But the architecture is highly scalable and fault tolerant.

src/main/scala/topology/CollegeCollectorTopology.scala is the starting point.

###Prerequisites for the code to run:###

* Active MQ for message input
* Storm 1.0.2 (if you want to run in a non-local cluster).
* Zookeeper (if you want to run in a non-local cluster)

To run it on a local cluster, simply run the CollegeCollectorTopology.scala to start the topology and run Trigger.scala to fire an event to 
start the process.
 
Have fun! :-)
