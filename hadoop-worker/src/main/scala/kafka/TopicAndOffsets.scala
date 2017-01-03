package kafka

/**
  * Created by tiennt4 on 03/01/2017.
  */
case class TopicAndOffsets(topicName: String, offsets: Map[Int, Long])
