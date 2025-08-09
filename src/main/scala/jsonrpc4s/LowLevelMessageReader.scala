package jsonrpc4s

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.collection.mutable.ArrayBuffer
import fs2.{Pipe, Pull, Stream}
import cats.effect.kernel.Async
import scribe.LoggerSupport

final class LowLevelMessageReader(logger: LoggerSupport) {
  // TODO: Benchmark and consider `Queue[ByteBuffer]` over `ArrayBuffer[Byte]`

  private[this] val EmptyPair = "" -> ""
  private[this] var contentLength = -1
  def currentContentLength: Int = contentLength

  private[this] var header = Map.empty[String, String]
  private[this] def atDelimiter(idx: Int, data: ArrayBuffer[Byte]): Boolean = {
    data.size >= idx + 4 &&
    data(idx) == '\r' &&
    data(idx + 1) == '\n' &&
    data(idx + 2) == '\r' &&
    data(idx + 3) == '\n'
  }

  import LowLevelMessageReader.ReadResult
  def readHeaders(data: ArrayBuffer[Byte]): ReadResult = {
    if (data.size < 4) ReadResult(None, false)
    else {
      var i = 0
      while (i + 4 < data.size && !atDelimiter(i, data)) {
        i += 1
      }
      if (!atDelimiter(i, data)) ReadResult(None, false)
      else {
        val bytes = new Array[Byte](i)
        data.copyToArray(bytes)
        data.remove(0, i + 4)
        val headers = new String(bytes, StandardCharsets.US_ASCII)

        // Parse other headers in JSON-RPC messages even if we only use `Content-Length` below
        val pairs: Map[String, String] = headers
          .split("\r\n")
          .iterator
          .filterNot(_.trim.isEmpty)
          .map { line =>
            line.split(":") match {
              case Array(key, value) => key.trim -> value.trim
              case _ =>
                logger.error(s"Malformed input: $line")
                EmptyPair
            }
          }
          .toMap

        pairs.get("Content-Length") match {
          case Some(n) =>
            try {
              contentLength = n.toInt
              header = pairs
              readContent(data)
            } catch {
              case _: NumberFormatException =>
                logger.error(
                  s"Expected Content-Length to be a number, obtained $n"
                )
                ReadResult(None, false)
            }
          case _ =>
            logger.error(s"Missing Content-Length key in headers $pairs")
            ReadResult(None, false)
        }
      }
    }
  }

  def readContent(data: ArrayBuffer[Byte]): ReadResult = {
    if (contentLength > data.size) ReadResult(None, false)
    else {
      val contentBytes = new Array[Byte](contentLength)
      data.copyToArray(contentBytes)
      data.remove(0, contentLength)
      contentLength = -1
      val msg = new LowLevelMessage(header, contentBytes)
      ReadResult(Some(msg), true)
    }
  }

  //def readWholeMessage()
}

object LowLevelMessageReader {
  def read(buf: ByteBuffer, logger: LoggerSupport): Option[LowLevelMessage] = {
    val data = ArrayBuffer.empty[Byte]
    val array = new Array[Byte](buf.remaining())
    buf.get(array)
    data ++= array

    val reader = new LowLevelMessageReader(logger)
    val result = reader.readHeaders(data)

    // Guarantee that this reader invariant holds
    assert(result.msg.isDefined && result.complete)
    result.msg
  }

  def streamReader[F[_]: Async](
      logger: LoggerSupport,
      name: String
  ): Pipe[F, ByteBuffer, LowLevelMessage] = { in =>
    val data = ArrayBuffer.empty[Byte]
    val reader = new LowLevelMessageReader(logger)

    def drainAvailable: Pull[F, LowLevelMessage, Unit] = {
      Pull.eval(
        Async[F].delay(
          println(
            s"[$name] [drain] start: contentLength=${reader.currentContentLength}, bufferSize=${data.size}"
          )
        )
      ) >> {
        val result =
          if (reader.currentContentLength < 0) reader.readHeaders(data)
          else reader.readContent(data)
        Pull.eval(
          Async[F].delay(
            println(
              s"[$name] [drain] read result: msgDefined=${result.msg.isDefined}, complete=${result.complete}, newBufferSize=${data.size}, nextContentLength=${reader.currentContentLength}"
            )
          )
        ) >>
          (result.msg match {
            case Some(msg) =>
              Pull.eval(
                Async[F].delay(
                  println(
                    s"[$name] [drain] emitting message: headers=${msg.header}, contentBytes=${msg.content.length}"
                  )
                )
              ) >>
                Pull.output1(msg) >> drainAvailable
            case None =>
              Pull.eval(Async[F].delay(println(s"[$name] [drain] no complete message available"))) >> Pull.done
          })
      }
    }

    def loop(s: Stream[F, ByteBuffer]): Pull[F, LowLevelMessage, Unit] =
      s.pull.uncons1.flatMap {
        case None =>
          Pull.eval(
            Async[F].delay(
              println(s"[$name] [loop] upstream completed, finalBufferSize=${data.size}")
            )
          ) >>
            drainAvailable >> Pull.done
        case Some((buf, tail)) =>
          val array = new Array[Byte](buf.remaining())
          buf.get(array)
          val preview = new String(array, 0, math.min(array.length, 128), StandardCharsets.UTF_8)
          val beforeSize = data.size
          Pull.eval(
            Async[F].delay(
              println(
                s"[$name] [loop] received chunk: bytes=${array.length}, preview='" + preview
                  .replaceAll("\n", "\\n") + "'"
              )
            )
          ) >>
            Pull.eval(Async[F].delay(println(s"[$name] [loop] buffer before append: $beforeSize"))) >>
            Pull.eval(Async[F].delay(data ++= array)) >>
            Pull.eval(Async[F].delay(println(s"[$name] [loop] buffer after append: ${data.size}"))) >>
            drainAvailable >> loop(tail)
      }

    loop(in).stream
  }

  private[LowLevelMessageReader] case class ReadResult(
      msg: Option[LowLevelMessage],
      complete: Boolean
  )

}
