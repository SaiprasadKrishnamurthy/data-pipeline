package college.collector

import com.jaunt.UserAgent

import scala.collection.JavaConversions._


/**
  * Created by saipkri on 30/08/16.
  */
object CollegeCollector {

  /**
    * Returns a list of tuples with first String as the college name and second as the college api url to mine the data in the further nodes.
    *
    * @param state      the state in which the search must be performed.
    * @param pageNumber the page number.
    * @return list of tuples.
    */
  def nameAndLinkPerPage(state: String, pageNumber: Int): List[(String, String)] = {
    val userAgent = new UserAgent()
    val doc = userAgent.visit(s"https://www.collegesearch.in/engineering/colleges-${state.toLowerCase.trim}?page=${pageNumber}")
    doc.findEvery("<a target=\"_blank\">")
      .filter(e => e.hasAttribute("title") && e.hasAttribute("href"))
      .map(e => (e.getAt("title").trim, e.getAt("href").trim))
      .toMap
      .toList
  }

  def nameAddressCourses(collegeName: String, collegeApiUrl: String): (String, String, List[String]) = {
    val userAgent1 = new UserAgent()
    val doc1 = userAgent1.visit(collegeApiUrl)
    val _address = doc1.findEvery("<div class=\"bold\" style=\"color:#777;\">Address</div>")
      .map(e => e.nextSiblingElement().getText)
      .toList
    val address = if (_address.isEmpty) "" else _address(0)
    val courses = doc1.findEvery("<strong>")
      .filter(e => e.getText.toLowerCase.trim.contains("master") || e.getText.toLowerCase.trim.contains("bachelor") || e.getText.toLowerCase.trim.contains("diploma"))
      .map(e => e.getText.trim)
      .toList
    (collegeName, address, courses)
  }
}
