package worker.hadoop

import com.google.inject.Guice
import com.twitter.inject.Injector
import worker.hadoop.module.HadoopWorkerModule
import worker.hadoop.service.HadoopStringWorker

/**
  * Created by SangDang on 9/8/
  **/
object MainApp extends Server

class Server extends App {
  val injector: Injector = Injector(Guice.createInjector(HadoopWorkerModule))
  val worker = injector.instance(classOf[HadoopStringWorker])
  worker.start()
}
