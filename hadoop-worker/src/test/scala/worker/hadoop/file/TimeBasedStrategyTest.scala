package worker.hadoop.file

import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite

/**
  * Created by tiennt4 on 19/12/2016.
  */
class TimeBasedStrategyTest extends FunSuite {
  val fileNamingConf = ConfigFactory.parseString(
    """
      |{
      | "interval": 60m,
      | "filename_pattern": "${topic}/${yyyy}/${MM}/${dd}/${HH}/${topic}-v${version}"
      |}
    """.stripMargin)

  val strategy = new TimeBasedStrategy(fileNamingConf)

  test("IsSectionEnd should return correct value") {
    val section1 = DataSection("", "src/test/resources", "", 1l, System.currentTimeMillis(), null, null)
    assert(strategy.isSectionEnd(section1))
    val section2 = DataSection("", "src/test/resources", "", System.currentTimeMillis(), System.currentTimeMillis(), null, null)
    assert(!strategy.isSectionEnd(section2))
    val section3 = DataSection("", "src/test/resources", "", System.currentTimeMillis() - strategy.interval, System.currentTimeMillis(), null, null)
    assert(strategy.isSectionEnd(section3))
  }

}
