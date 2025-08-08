package jsonrpc4s

import java.io.OutputStream
import cats.effect.{Async, Deferred, Ref}
import cats.effect.kernel.Sync
import cats.syntax.all._
import fs2.concurrent.Channel
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration
import scribe.LoggerSupport
import scala.util.Try

import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import com.github.plokhotnyuk.jsoniter_scala.core.readFromArray
import com.github.plokhotnyuk.jsoniter_scala.core.writeToArray
import java.nio.channels.WritableByteChannel

class RpcClient[F[_]](
    channel: Channel[F, Message],
    logger: LoggerSupport
)(implicit F: Async[F])
    extends RpcActions[F] {

  protected val counter: Ref[F, Int] = Ref.unsafe[F, Int](1)
  protected val activeServerRequests = TrieMap.empty[RequestId, Deferred[F, Response]]

  protected val notificationsLock = new Object()
  protected def toJson[R: JsonValueCodec](r: R): RawJson = RawJson(writeToArray(r))

  def serverRespond(response: Response): F[Unit] = {
    response match {
      case Response.None => F.unit
      case x: Response.Success => channel.send(x).void
      case x: Response.Error => channel.send(x).void
    }
  }

  def clientRespond(response: Response): F[Unit] = {
    for {
      id <- F.delay(response match {
        case Response.None => None
        case Response.Success(_, requestId, jsonrpc, _) => Some(requestId)
        case Response.Error(_, requestId, jsonrpc, _) => Some(requestId)
      })
      _ <- id match {
        case None => F.unit
        case Some(requestId) =>
          F.delay(activeServerRequests.remove(requestId)).flatMap {
            case None =>
              F.delay(logger.error(s"Response to unknown request: $response"))
            case Some(callback) =>
              callback.complete(response).void
          }
      }
    } yield ()
  }

  def notify[A](
      endpoint: Endpoint[A, Unit],
      params: A,
      headers: Map[String, String] = Map.empty
  ): F[Unit] = {
    import endpoint.codecA
    val msg = Notification(endpoint.method, Some(toJson(params)), headers)

    // Send notifications in the order they are sent by the caller
    F.delay {
        notificationsLock.synchronized {
          channel.send(msg)
        }
      }
      .flatten
      .void
  }

  def request[A, B](
      endpoint: Endpoint[A, B],
      params: A,
      headers: Map[String, String] = Map.empty
  ): F[RpcResponse[B]] = {
    import endpoint.{codecA, codecB}
    for {
      reqId <- counter.updateAndGet(_ + 1).map(RequestId.apply)
      deferred <- Deferred[F, Response]
      _ <- F.delay(activeServerRequests.put(reqId, deferred))
      _ <- F
        .delay {
          val json = Request(endpoint.method, Some(toJson(params)), reqId, headers)
          channel.send(json)
        }
        .flatten
        .void
      response <- deferred.get
      result <- response match {
        // This case can never happen given that no response isn't a valid JSON-RPC message
        case Response.None =>
          F.raiseError(new RuntimeException("Fatal error: obtained `Response.None`!"))
        case err: Response.Error => F.pure(RpcFailure(endpoint.method, err): RpcResponse[B])
        case suc: Response.Success =>
          F.delay(Try(readFromArray[B](suc.result.value)).toEither).map {
            case Right(value) => RpcSuccess(value, suc): RpcResponse[B]
            case Left(err) =>
              RpcFailure(endpoint.method, Response.invalidParams(err.toString, reqId)): RpcResponse[
                B
              ]
          }
      }
    } yield result
  }
}

object RpcClient {
  def fromOutputStream[F[_]: Async](out: OutputStream, logger: LoggerSupport): F[RpcClient[F]] = {
    for {
      channel <- Channel.unbounded[F, Message]
      outputChannel <- Message.messagesToOutput(Left(out), logger)
      _ <- Async[F].start {
        channel.stream
          .evalMap { msg =>
            outputChannel.send(msg).flatMap {
              case Left(_) => Async[F].unit
              case Right(_) => Async[F].unit
            }
          }
          .compile
          .drain
      }
    } yield new RpcClient[F](channel, logger)
  }

  def fromChannel[F[_]: Async](
      channel: WritableByteChannel,
      logger: LoggerSupport
  ): F[RpcClient[F]] = {
    for {
      msgChannel <- Channel.unbounded[F, Message]
      outputChannel <- Message.messagesToOutput(Right(channel), logger)
      _ <- Async[F].start {
        msgChannel.stream
          .evalMap { msg =>
            outputChannel.send(msg).flatMap {
              case Left(_) => Async[F].unit
              case Right(_) => Async[F].unit
            }
          }
          .compile
          .drain
      }
    } yield new RpcClient[F](msgChannel, logger)
  }
}
