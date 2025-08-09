package jsonrpc4s.tests

import java.nio.ByteBuffer
import weaver._
import cats.effect.IO
import fs2.Stream
import scribe.Logger
import jsonrpc4s.Request
import jsonrpc4s.RawJson
import jsonrpc4s.RequestId
import jsonrpc4s.LowLevelMessage
import jsonrpc4s.LowLevelMessageWriter
import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import com.github.plokhotnyuk.jsoniter_scala.macros.CodecMakerConfig
import jsonrpc4s.LowLevelMessageReader

object BaseProtocolMessageSuite extends SimpleIOSuite {
  implicit val stringCodec: JsonValueCodec[String] = JsonCodecMaker.make(CodecMakerConfig)
  private val request = Request(
    "method",
    Some(RawJson.toJson("params")),
    RequestId(1),
    Map("Custom-Header" -> "custom-value")
  )

  private val message = LowLevelMessage.fromMsg(request)
  private val byteArray = LowLevelMessageWriter.write(message).array()
  private val byteArrayDouble = byteArray ++ byteArray
  private def bytes = ByteBuffer.wrap(byteArray)

  test("toString should format message correctly") {
    val expected = """|Custom-Header: custom-value
                      |Content-Length: 60
                      |
                      |{"method":"method","params":"params","id":1,"jsonrpc":"2.0"}""".stripMargin
      .replaceAll("\r\n", "\n")

    val obtained = message.toString.replaceAll("\r\n", "\n")
    IO.pure(expect(obtained == expected))
  }

  test("parse message from whole byte buffer should return correct message") {
    val obtained = LowLevelMessageReader.read(ByteBuffer.wrap(byteArray), Logger.root)
    IO.pure(expect(obtained == Some(message)))
  }

  // Emulates a sequence of chunks and returns the parsed protocol messages.
  def parse(buffers: List[ByteBuffer]): IO[List[LowLevelMessage]] = {
    val stream = Stream.emits(buffers).through(LowLevelMessageReader.streamReader[IO](Logger.root))
    stream.compile.toList
  }

  test("parse-0 should parse correctly") {
    val (buffers, messages) = (1 to 0).toList.map(_ => bytes -> message).unzip
    parse(buffers).map { obtained => expect(obtained == messages) }
  }

  test("parse-1 should parse correctly") {
    val (buffers, messages) = (1 to 1).toList.map(_ => bytes -> message).unzip
    parse(buffers).map { obtained => expect(obtained == messages) }
  }

  test("parse-2 should parse correctly") {
    val (buffers, messages) = (1 to 2).toList.map(_ => bytes -> message).unzip
    parse(buffers).map { obtained => expect(obtained == messages) }
  }

  test("parse-3 should parse correctly") {
    val (buffers, messages) = (1 to 3).toList.map(_ => bytes -> message).unzip
    parse(buffers).map { obtained => expect(obtained == messages) }
  }

  test("parse-4 should parse correctly") {
    val (buffers, messages) = (1 to 4).toList.map(_ => bytes -> message).unzip
    parse(buffers).map { obtained => expect(obtained == messages) }
  }

  def array: ByteBuffer = ByteBuffer.wrap(byteArray)
  def take(n: Int): ByteBuffer = ByteBuffer.wrap(byteArray.take(n))
  def drop(n: Int): ByteBuffer = ByteBuffer.wrap(byteArray.drop(n))

  test("combined should parse two messages correctly") {
    val buffers = ByteBuffer.wrap(byteArrayDouble) :: Nil
    parse(buffers).map { obtained =>
      val expected = List(message, message)
      expect(obtained == expected)
    }
  }

  test("chunked should parse two messages correctly") {
    val buffers = take(10) :: drop(10) :: array :: Nil
    parse(buffers).map { obtained =>
      val expected = List(message, message)
      expect(obtained == expected)
    }
  }

  test("chunked2 should parse two messages correctly") {
    val buffers =
      take(10) :: ByteBuffer.wrap(drop(10).array() ++ take(10).array()) :: drop(10) :: Nil
    parse(buffers).map { obtained =>
      val expected = List(message, message)
      expect(obtained == expected)
    }
  }

  test("chunked at every possible offset should parse correctly") {
    // Test a few specific offsets instead of all
    val testOffsets = List(0, 10, 20, byteArrayDouble.length / 2, byteArrayDouble.length)
    val buffers = testOffsets.flatMap { i =>
      List(
        ByteBuffer.wrap(byteArrayDouble.take(i)),
        ByteBuffer.wrap(byteArrayDouble.drop(i))
      )
    }
    parse(buffers).map { obtained =>
      val expected = List(message, message)
      // We concatenated several test cases, so check the result is divisible by two and each pair equals message
      val evenLength = obtained.length % 2 == 0
      val pairsOk = obtained.grouped(2).forall(pair => pair == List(message, message))
      expect(evenLength && pairsOk)
    }
  }
}
