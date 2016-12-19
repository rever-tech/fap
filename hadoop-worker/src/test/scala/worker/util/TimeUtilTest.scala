package worker.util

import org.scalatest.FunSuite
import worker.hadoop.util.TimeUtil

/**
  * Created by tiennt4 on 13/12/2016.
  */
class TimeUtilTest extends FunSuite {
  test("roundLevelByInterval should return correct value with interval 1h") {
    assert(TimeUtil.roundTimeByInterval(1481601072000l, 3600000) == 1481598000000l)
    assert(TimeUtil.roundTimeByInterval(1481598000000l, 3600000) == 1481598000000l)
    assert(TimeUtil.roundTimeByInterval(1482144178849l, 3600000) == 1482141600000l)
  }
  test("roundLevelByInterval should return correct value with interval 0.5h") {
    assert(TimeUtil.roundTimeByInterval(1481601072000l, 1800000) == 1481599800000l)
    assert(TimeUtil.roundTimeByInterval(1481599800000l, 1800000) == 1481599800000l)
    assert(TimeUtil.roundTimeByInterval(1481598000000l, 1800000) == 1481598000000l)
    assert(TimeUtil.roundTimeByInterval(1481598010000l, 1800000) == 1481598000000l)
  }
  test("roundLevelByInterval should return correct value with interval 0.25h") {
    assert(TimeUtil.roundTimeByInterval(1481598000000l, 900000) == 1481598000000l)
    assert(TimeUtil.roundTimeByInterval(1481600460000l, 900000) == 1481599800000l)
  }
}
