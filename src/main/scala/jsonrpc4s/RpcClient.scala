package jsonrpc4cats

import java.io.OutputStream
import cats.effect.{Async, Deferred, Ref, Resource}
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
    logger: LoggerSupport[Unit]
)(implicit F: Async[F])
    extends RpcActions[F] {

  protected val counter: Ref[F, Int] = Ref.unsafe[F, Int](1)
  protected val activeServerRequests = TrieMap.empty[RequestId, Deferred[F, Response]]

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
        case Response.Success(_, requestId, _, _) => Some(requestId)
        // we get a Response.Error if the request is cancelled
        // but since it's us who cancelled the request, the callback
        // is already gone, so we can just ignore it
        case err: Response.Error if err.isCancelled => None
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

    channel.send(msg).void
  }

  def request[A, B](
      endpoint: Endpoint[A, B],
      params: A,
      headers: Map[String, String] = Map.empty
  ): F[RpcResponse[B]] = {
    import endpoint.{codecA, codecB}
    for {
      reqId <- counter.updateAndGet(_ + 1).map(RequestId.apply)
      fireRequest: F[RpcResponse[B]] @unchecked = for {
        deferred <- Deferred[F, Response]
        _ <- F.delay(activeServerRequests.put(reqId, deferred))
        _ <- channel.send(Request(endpoint.method, Some(toJson(params)), reqId, headers)).void
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
          //
          // it's actually not doing anything with CE3 because the carrier fiber must have been
          // cancelled already but for parity - we raise an exception
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
        fireRequest,
        F.delay {
          activeServerRequests.remove(reqId) match {
            case Some(deferred) =>
              deferred.complete(Response.cancelled(reqId)) *>
                notify(RpcActions.cancelRequest, CancelParams(reqId))
            case None =>
              F.unit
          }
        }.flatten
      )
    } yield result
  }
}

object RpcClient {

  def apply[F[_]: Async](
      channel: Channel[F, Message],
      logger: LoggerSupport[Unit]
  ): F[RpcClient[F]] = Async[F].delay(new RpcClient[F](channel, logger))

  def fromOutputStream[F[_]: Async](
      out: OutputStream,
      logger: LoggerSupport[Unit]
  ): Resource[F, RpcClient[F]] = {
    for {
      channel <- Resource.eval(Channel.unbounded[F, Message])
      outputChannel <- Resource.eval(Message.messagesToOutput(Left(out), logger))
      writerFiber <- Resource.eval(Async[F].start {
        channel.stream
          .evalMap { msg => outputChannel.send(msg).void }
          .compile
          .drain
      })
      rpcClient <- Resource.make(RpcClient(channel, logger))(_ => writerFiber.cancel)
    } yield rpcClient
  }

  def fromChannel[F[_]: Async](
      channel: WritableByteChannel,
      logger: LoggerSupport[Unit]
  ): Resource[F, RpcClient[F]] = {
    for {
      msgChannel <- Resource.eval(Channel.unbounded[F, Message])
      outputChannel <- Resource.eval(Message.messagesToOutput(Right(channel), logger))
      writerFiber <- Resource.eval(Async[F].start {
        msgChannel.stream
          .evalMap { msg => outputChannel.send(msg).void }
          .compile
          .drain
      })
      rpcClient <- Resource.make(RpcClient(msgChannel, logger))(_ => writerFiber.cancel)
    } yield rpcClient
  }
}
