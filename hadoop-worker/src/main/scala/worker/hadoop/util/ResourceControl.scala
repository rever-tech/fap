package worker.hadoop.util

/**
  * Created by tiennt4 on 10/12/2016.
  */
object ResourceControl {
  def using[A <: {def close() : Unit}, B](param: A)(f: A => B): B =
    try {
      f(param)
    } finally {
      param.close()
    }
}
