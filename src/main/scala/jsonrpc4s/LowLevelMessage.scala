package jsonrpc4s

import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util

import fs2.Stream
import cats.effect.kernel.Async
import scribe.LoggerSupport

import scala.util.Try
import scala.util.Success
import scala.util.Failure

final class LowLevelMessage(
    val header: Map[String, String],
    val content: Array[Byte]
) {

  override def equals(obj: scala.Any): Boolean =
    this.eq(obj.asInstanceOf[Object]) || {
      obj match {
        case m: LowLevelMessage =>
          header.equals(m.header) &&
            util.Arrays.equals(content, m.content)
      }
    }

  override def toString: String = {
    val bytes = LowLevelMessageWriter.write(this)
    StandardCharsets.UTF_8.decode(bytes).toString
  }
}

object LowLevelMessage {
  object io {
    import cats.effect._
    import cats.syntax.all._
    import fs2.Chunk

    private def readBytesFromInputStream[F[_]](is: InputStream, buf: Array[Byte], offset: Int)(
        read: (InputStream, Array[Byte], Int) => F[Int]
    )(
        implicit
        F: Sync[F]
    ): F[Option[(Chunk[Byte], Option[(Array[Byte], Int)])]] =
      read(is, buf, offset).map { numBytes =>
        if (numBytes < 0) None
        else if (offset + numBytes == buf.size) Some(Chunk.array(buf, offset, numBytes) -> None)
        else Some(Chunk.array(buf, offset, numBytes) -> Some(buf -> (offset + numBytes)))
      }

    def readInputStream[F[_]](
        fis: F[InputStream],
        chunkSize: Int
    )(implicit F: Sync[F]): Stream[F, Byte] = {
      val buf = F.delay(new Array[Byte](chunkSize))
      val read = (is: InputStream, buf: Array[Byte], off: Int) =>
        F.interruptible(is.read(buf, off, buf.length - off))

      def useIs(is: InputStream) = Stream.unfoldChunkEval(Option.empty[(Array[Byte], Int)]) {
        case None => buf.flatMap(b => readBytesFromInputStream(is, b, 0)(read))
        case Some((b, offset)) => readBytesFromInputStream(is, b, offset)(read)
      }

      Stream.bracket(fis)(is => Sync[F].blocking(is.close())).flatMap(useIs)
    }
  }

  import com.github.plokhotnyuk.jsoniter_scala.core.writeToArray

  def fromMsg(msg: Message): LowLevelMessage = fromBytes(msg.headers, writeToArray(msg))

  def fromBytes(userHeaders: Map[String, String], bytes: Array[Byte]): LowLevelMessage = {
    val headers = userHeaders.filterNot {
      case (key, _) => key.toLowerCase == "content-length"
    } + ("Content-Length" -> bytes.length.toString)
    new LowLevelMessage(headers, bytes)
  }

  def fromInputStream[F[_]: Async](
      in: InputStream,
      logger: LoggerSupport,
      name: String
  ): Stream[F, LowLevelMessage] = {
    // FIXME: Use bracket to handle this resource correctly if something fails
    fromByteBuffers(
      io.readInputStream[F](Async[F].pure(in), 4096)
        .chunks
        .map(_.toByteBuffer)
        .onFinalize(Async[F].delay(println(s"input stream closed for $name"))),
      logger,
      name
    ).onFinalize(Async[F].delay(println(s"fromByteBuffers closed for $name")))
  }

  def fromBytes[F[_]: Async](
      in: Stream[F, Array[Byte]],
      logger: LoggerSupport
  ): Stream[F, LowLevelMessage] = {
    fromByteBuffers(in.map(ByteBuffer.wrap), logger, "chuj")
  }

  def fromByteBuffers[F[_]: Async](
      in: Stream[F, ByteBuffer],
      logger: LoggerSupport,
      name: String
  ): Stream[F, LowLevelMessage] = {
    in.through(LowLevelMessageReader.streamReader(logger, name))
      .evalTap(msg => Async[F].delay(println(s"Received after LowLevelMessageReader: $msg")))
  }

  def toMsg(message: LowLevelMessage): Message = {
    import com.github.plokhotnyuk.jsoniter_scala.core.readFromArray
    println(s"Content:\n'${new String(message.content, StandardCharsets.UTF_8)}'\n")
    // Make sure we propagate headers from the transport to the read message before handling
    Try(readFromArray[Message](message.content)) match {
      case Success(msg: Request) => msg.copy(headers = message.header)
      case Success(msg: Notification) => msg.copy(headers = message.header)
      case Success(msg: Response.Error) => msg.copy(headers = message.header)
      case Success(msg: Response.Success) => msg.copy(headers = message.header)
      case Success(msg: Response.None.type) => msg
      case Failure(err) => Response.parseError(err.toString)
    }
  }
}
