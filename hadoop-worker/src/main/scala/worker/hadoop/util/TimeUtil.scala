package worker.hadoop.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import java.util.concurrent.TimeUnit

/**
  * Created by tiennt4 on 12/12/2016.
  */
object TimeUtil {

  val timeUnits = Seq(
    (TimeUnit.SECONDS.toMillis(1), Calendar.SECOND),
    (TimeUnit.MINUTES.toMillis(1), Calendar.MINUTE),
    (TimeUnit.HOURS.toMillis(1), Calendar.HOUR_OF_DAY),
    (TimeUnit.DAYS.toMillis(1), Calendar.DAY_OF_MONTH)
  )

  val toMinuteTimeFormat = new SimpleDateFormat("yyyy_MM_dd_HH_mm")

  def roundTimeByInterval(time: Long, interval: Long): Long = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(time)
    roundLevelByInterval(interval).foreach(i => cal.set(i, 0))
    incByItv(cal.getTimeInMillis, interval)(_ + interval <= time)
  }

  def incByItv(initValue: Long, step: Long)(condition: Long => Boolean): Long =
    if (!condition(initValue))
      initValue
    else
      incByItv(initValue + step, step)(condition)

  def formatDateToMinute(time: Long): String = toMinuteTimeFormat.format(new Date(time))

  private def roundLevelByInterval(interval: Long): Seq[Int] = {
    timeUnits.filter(_._1 <= interval).map(_._2)
  }
}
