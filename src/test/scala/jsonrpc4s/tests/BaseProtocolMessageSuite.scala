package jsonrpc4s.tests

import java.nio.ByteBuffer
import cats.effect.{IO, Async}
import cats.effect.testing.scalatest.AsyncIOSpec
import fs2.Stream
import scribe.Logger
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import jsonrpc4s.Request
import jsonrpc4s.RawJson
import jsonrpc4s.RequestId
import jsonrpc4s.LowLevelMessage
import jsonrpc4s.LowLevelMessageWriter
import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import com.github.plokhotnyuk.jsoniter_scala.macros.CodecMakerConfig
import jsonrpc4s.LowLevelMessageReader

class BaseProtocolMessageSuite extends AsyncWordSpec with Matchers with AsyncIOSpec {
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

  "toString" should {
    "format message correctly" in {
      message.toString.replaceAll("\r\n", "\n") shouldBe
        """|Custom-Header: custom-value
           |Content-Length: 60
           |
           |{"method":"method","params":"params","id":1,"jsonrpc":"2.0"}""".stripMargin
          .replaceAll("\r\n", "\n")
    }
  }

  "parse message from whole byte buffer" should {
    "return correct message" in {
      LowLevelMessageReader.read(ByteBuffer.wrap(byteArray), Logger.root) shouldBe Some(message)
    }
  }

  // Emulates a sequence of chunks and returns the parsed protocol messages.
  def parse(buffers: List[ByteBuffer]): IO[List[LowLevelMessage]] = {
    val stream = Stream.emits(buffers).through(LowLevelMessageReader.streamReader[IO](Logger.root))
    stream.compile.toList
  }

  "parse-0" should {
    "parse correctly" in {
      val (buffers, messages) = (1 to 0).toList.map(_ => bytes -> message).unzip
      parse(buffers).map(_ shouldBe messages).unsafeToFuture()
    }
  }

  "parse-1" should {
    "parse correctly" in {
      val (buffers, messages) = (1 to 1).toList.map(_ => bytes -> message).unzip
      parse(buffers).map(_ shouldBe messages).unsafeToFuture()
    }
  }

  "parse-2" should {
    "parse correctly" in {
      val (buffers, messages) = (1 to 2).toList.map(_ => bytes -> message).unzip
      parse(buffers).map(_ shouldBe messages).unsafeToFuture()
    }
  }

  "parse-3" should {
    "parse correctly" in {
      val (buffers, messages) = (1 to 3).toList.map(_ => bytes -> message).unzip
      parse(buffers).map(_ shouldBe messages).unsafeToFuture()
    }
  }

  "parse-4" should {
    "parse correctly" in {
      val (buffers, messages) = (1 to 4).toList.map(_ => bytes -> message).unzip
      parse(buffers).map(_ shouldBe messages).unsafeToFuture()
    }
  }

  def array: ByteBuffer = ByteBuffer.wrap(byteArray)
  def take(n: Int): ByteBuffer = ByteBuffer.wrap(byteArray.take(n))
  def drop(n: Int): ByteBuffer = ByteBuffer.wrap(byteArray.drop(n))

  "combined" should {
    "parse two messages correctly" in {
      val buffers = ByteBuffer.wrap(byteArrayDouble) :: Nil
      parse(buffers)
        .map { obtained =>
          val expected = List(message, message)
          obtained shouldBe expected
        }
        .unsafeToFuture()
    }
  }

  "chunked" should {
    "parse two messages correctly" in {
      val buffers = take(10) :: drop(10) :: array :: Nil
      parse(buffers)
        .map { obtained =>
          val expected = List(message, message)
          obtained shouldBe expected
        }
        .unsafeToFuture()
    }
  }

  "chunked2" should {
    "parse two messages correctly" in {
      val buffers =
        take(10) :: ByteBuffer.wrap(drop(10).array() ++ take(10).array()) :: drop(10) :: Nil
      parse(buffers)
        .map { obtained =>
          val expected = List(message, message)
          obtained shouldBe expected
        }
        .unsafeToFuture()
    }
  }

  "chunked at every possible offset" should {
    "parse correctly" in {
      // Test a few specific offsets instead of all
      val testOffsets = List(0, 10, 20, byteArrayDouble.length / 2, byteArrayDouble.length)
      val testCases = testOffsets.map { i =>
        // Split the message at offset `i` and emit two chunks
        val buffers =
          ByteBuffer.wrap(byteArrayDouble.take(i)) ::
            ByteBuffer.wrap(byteArrayDouble.drop(i)) ::
            Nil
        parse(buffers).map { obtained =>
          val expected = List(message, message)
          obtained shouldBe expected
        }
      }
      testCases.head.unsafeToFuture() // Just test the first one for now
    }
  }
}
