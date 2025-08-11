package jsonrpc4cats

import cats.effect.Async
import cats.effect.kernel.{Fiber, Outcome}
import fs2.Stream
import scala.collection.concurrent.TrieMap
import scala.util.control.NonFatal
import scribe.LoggerSupport
import scribe.Scribe
import scribe.cats.effect
import cats.syntax.all._

class RpcServer[F[_]] protected (
    in: Stream[F, Message],
    client: RpcClient[F],
    services: Services[F],
    logger: LoggerSupport[Unit]
)(implicit F: Async[F], S: Scribe[F]) {
  protected val activeClientRequests: TrieMap[RequestId, Fiber[F, Throwable, Response]] =
    TrieMap.empty
  protected val cancelNotification = {
    Service.notification(RpcActions.cancelRequest, logger) {
      new Service[F, CancelParams, Unit] {
        def handle(params: CancelParams): F[Unit] = {
          val id = params.id
          F.delay(activeClientRequests.get(id)).flatMap {
            case None =>
              S.warn(s"Can't cancel request $id, no active request found.")
            case Some(request) =>
              S.info(s"Cancelling request $id") *>
                request.cancel *>
                F.delay {
                  activeClientRequests.remove(id)
                }.void
          }
        }
      }
    }
  }

  protected val handlersByMethodName: Map[String, NamedJsonRpcService[F]] =
    services.addService(cancelNotification).byMethodName

  def cancelActiveClientRequests(): F[Unit] =
    activeClientRequests.values.toVector.map(_.cancel).sequence_

  def waitForActiveClientRequests: F[Unit] = {
    val fibers = activeClientRequests.values.map(fiber => fiber.join.void)
    // Await until completion and ignore task results
    fibers.toList.sequence_
  }

  protected def handleResponse(response: Response): F[Response] =
    client.clientRespond(response).map(_ => Response.None)

  protected def handleRequest(request: Request): F[Response] = {
    val Request(method, _, id, _, _) = request
    F.delay(handlersByMethodName.get(method)).flatMap {
      case None =>
        F.delay {
          logger.info(s"Method not found '$method'")
          Response.methodNotFound(method, id)
        }

      case Some(handler) =>
        val response = handler.handle(request).handleErrorWith {
          case NonFatal(e) =>
            F.delay {
              logger.error(s"Unhandled JSON-RPC error handling request $request", e)
              Response.internalError(e.getMessage, request.id)
            }
        }
        for {
          fiber <- Async[F].start(response)
          _ <- F.delay(activeClientRequests.put(request.id, fiber))
          result <- fiber.join.flatMap {
            case Outcome.Succeeded(fa) => fa
            case Outcome.Errored(e) => F.raiseError(e)
            case Outcome.Canceled() => F.pure(Response.cancelled(id))
          }: F[Response]
        } yield result
    }
  }

  protected def handleNotification(notification: Notification): F[Response] = {
    val Notification(method, _, _, _) = notification
    F.delay(handlersByMethodName.get(method)).flatMap {
      case None =>
        // Can't respond to invalid notifications
        S.error(s"Unknown method '$method'").as(Response.None)

      case Some(handler) =>
        val response = handler
          .handle(notification)
          .handleErrorWith {
            case NonFatal(e) =>
              S.error(s"Error handling notification $notification: ${e}").as(Response.None)
          }

        response.flatMap {
          case Response.None => F.pure(Response.None)
          case nonEmpty =>
            S.error(s"Obtained non-empty response $nonEmpty for notification $notification!") *> F
              .pure(Response.None)
        }
    }
  }

  protected def handleValidMessage(message: Message): F[Response] = {
    message match {
      case response: Response => handleResponse(response)
      case notification: Notification => handleNotification(notification)
      case request: Request => handleRequest(request)
    }
  }

  def startTask: F[Unit] = {
    in.parEvalMapUnordered(32) { msg => // TODO make server parallelism configurable
        handleValidMessage(msg)
          .flatMap {
            case Response.None => F.unit
            case response => client.serverRespond(response)
          }
          .handleErrorWith {
            case NonFatal(e) =>
              F.delay(logger.error("Unhandled error responding to JSON-RPC client", e))
          }
      }
      .compile
      .drain
  }
}

object RpcServer {
  def apply[F[_]: Async](
      in: Stream[F, Message],
      client: RpcClient[F],
      services: Services[F],
      logger: LoggerSupport[Unit]
  ): RpcServer[F] = new RpcServer(in, client, services, logger)
}
