package jsonrpc4s

import cats.effect.{Async, Deferred}
import cats.effect.kernel.{Fiber, Outcome}
import fs2.Stream
import scala.collection.concurrent.TrieMap
import scala.util.control.NonFatal
import scribe.LoggerSupport
import cats.syntax.all._

class RpcServer[F[_]] protected (
    in: Stream[F, Message],
    client: RpcClient[F],
    services: Services[F],
    logger: LoggerSupport
)(implicit F: Async[F]) {
  protected val activeClientRequests: TrieMap[RequestId, Fiber[F, Throwable, Response]] =
    TrieMap.empty
  protected val cancelNotification = {
    Service.notification(RpcActions.cancelRequest, logger) {
      new Service[F, CancelParams, Unit] {
        def handle(params: CancelParams): F[Unit] = {
          val id = params.id
          F.delay(activeClientRequests.get(id)).flatMap {
            case None =>
              F.delay {
                logger.warn(
                  s"Can't cancel request $id, no active request found."
                )
              }
            case Some(request) =>
              F.delay {
                logger.info(s"Cancelling request $id")
                request.cancel
                activeClientRequests.remove(id)
                Response.cancelled(id)
              }.void
          }
        }
      }
    }
  }

  protected val handlersByMethodName: Map[String, NamedJsonRpcService[F]] =
    services.addService(cancelNotification).byMethodName

  def cancelActiveClientRequests(): F[Unit] =
    F.delay(activeClientRequests.values.foreach(_.cancel))

  def waitForActiveClientRequests: F[Unit] = {
    val fibers = activeClientRequests.values.map(fiber => fiber.join.void)
    // Await until completion and ignore task results
    fibers.toList.sequence.void
  }

  protected def handleResponse(response: Response): F[Response] = {
    F.delay {
      client.clientRespond(response)
      Response.None
    }
  }

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
            case Outcome.Canceled() => F.raiseError(new RuntimeException("Request was canceled"))
          }: F[Response]
        } yield result
    }
  }

  protected def handleNotification(notification: Notification): F[Response] = {
    val Notification(method, _, _, _) = notification
    F.delay(handlersByMethodName.get(method)).flatMap {
      case None =>
        F.delay {
          // Can't respond to invalid notifications
          logger.error(s"Unknown method '$method'")
          Response.None
        }

      case Some(handler) =>
        val response = handler
          .handle(notification)
          .handleErrorWith {
            case NonFatal(e) =>
              F.delay {
                logger.error(s"Error handling notification $notification", e)
                Response.None
              }
          }

        response.map {
          case Response.None => Response.None
          case nonEmpty =>
            logger.error(s"Obtained non-empty response $nonEmpty for notification $notification!")
            Response.None
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

  def startTask(afterSubscribe: F[Unit]): F[Unit] = {
    afterSubscribe >> in
      .evalMap { msg =>
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
      logger: LoggerSupport
  ): RpcServer[F] = {
    new RpcServer(in, client, services, logger)
  }
}
