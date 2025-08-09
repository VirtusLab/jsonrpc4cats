package jsonrpc4s

import java.io.OutputStream
import cats.effect.{Async, Deferred, Ref}
import cats.syntax.all._
import fs2.concurrent.Channel
import scala.collection.concurrent.TrieMap
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
    println(s"RpcClient responding to $response from server")
    response match {
      case Response.None => F.unit
      case x: Response.Success => channel.send(x).void
      case x: Response.Error => channel.send(x).void
    }
  }

  def clientRespond(response: Response): F[Unit] = {
    for {
      _ <- F.delay(println(s"RpcClient responding to $response from server"))
      id <- F.delay(response match {
        case Response.None => None
        case Response.Success(_, requestId, _, _) => Some(requestId)
        case Response.Error(_, requestId, _, _) => Some(requestId)
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
      _ <- F.delay(println(s"Requesting $endpoint with params $params, reqId $reqId"))
      resEff: F[RpcResponse[B]] @unchecked = for {
        deferred <- Deferred[F, Response]
        _ <- F.delay(activeServerRequests.put(reqId, deferred))
        _ <- F
          .delay {
            val json = Request(endpoint.method, Some(toJson(params)), reqId, headers)
            channel.send(json) *> F.delay(println(s"Sent request $json"))
          }
          .flatten
          .void
        _ <- F.delay(println(s"Request sent, waiting for response"))
        response <- deferred.get
        result <- response match {
          // This case can never happen given that no response isn't a valid JSON-RPC message
          case Response.None =>
            F.raiseError(new RuntimeException("Fatal error: obtained `Response.None`!"))

          // This is dumb as bricks but that's how the original library worked.
          // Let me elaborate:
          // Response is a sum type, it has 3 constructors: None, Success, Error
          // Cancellation is expressed as a Response.Error with code RequestCancelled.
          // RpcResponse is also a sum type, it has 2 constructors: RpcSuccess, RpcFailure.
          // We model the return value of request function as F[RpcResponse[B]] (Task[RpcResponse[B]] in original).
          // Since we return a sum type wrapped in async effect F/Task, we could just return the RpcResponse.
          // But no, the original library fails the effect on cancellation! Why? I don't know why!
          // So we have a Task[RpcResponse[B]] but if we cancel the task, the Task will be failed with...
          // you guessed it, a RpcResponse.RpcFailure which extends, coincidentally, an exception!
          // This shit is equivalent to throwing an exception containing an Integer from a method returning an Integer!
          // But we have to respect the contract because stuff has been built on this hot garbage,
          // so now we have to behave the same way.
          case err: Response.Error if err.isCancelled =>
            F.raiseError(RpcFailure(endpoint.method, err))
          case err: Response.Error => F.pure(RpcFailure(endpoint.method, err))
          case suc: Response.Success =>
            F.delay(Try(readFromArray[B](suc.result.value)).toEither).map {
              case Right(value) => RpcSuccess[B](value, suc)
              case Left(err) =>
                RpcFailure(endpoint.method, Response.invalidParams(err.toString, reqId))
            }
        }
      } yield result
      result <- F.onCancel(
        resEff,
        F.delay {
          activeServerRequests.remove(reqId).foreach { deferred =>
            deferred.complete(Response.cancelled(reqId)) *>
              notify(RpcActions.cancelRequest, CancelParams(reqId))
          }
        }
      )
    } yield result
  }
}

object RpcClient {
  def fromOutputStream[F[_]: Async](
      out: OutputStream,
      logger: LoggerSupport
  ): F[RpcClient[F]] = {
    for {
      channel <- Channel.unbounded[F, Message]
      outputChannel <- Message.messagesToOutput(Left(out), logger)
      _ <- Async[F].start {
        channel.stream
          .evalMap { msg =>
            Async[F].delay(println(s"Sending message $msg to output channel")) *>
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
