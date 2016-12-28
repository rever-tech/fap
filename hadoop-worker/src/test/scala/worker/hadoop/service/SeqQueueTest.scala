package worker.hadoop.service

import java.util.UUID

import org.scalatest.FunSuite

/**
  * Created by tiennt4 on 18/12/2016.
  */
class SeqQueueTest extends FunSuite{

  test("test get and commit single thread") {
    val queue = new SeqQueue[String]
    val fstUUID = UUID.randomUUID().toString
    val sndUUID = UUID.randomUUID().toString
    queue.addTail(fstUUID)
    queue.addTail(sndUUID)
    assert(queue.getHead.get == fstUUID)
    assert(queue.getHead.isEmpty)
    assert(queue.size == 2)
    queue.commitProcessSucceed()
    assert(queue.size == 1)
    assert(queue.getHead.get == sndUUID)
    queue.commitProcessFailure()
    assert(queue.size == 1)
    assert(queue.getHead.get == sndUUID)
    queue.commitProcessSucceed()
    assert(queue.size == 0)
    assert(queue.getHead.isEmpty)
  }
}
