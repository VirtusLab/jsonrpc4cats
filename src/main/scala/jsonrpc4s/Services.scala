package jsonrpc4cats

import cats.effect.kernel.Async
import cats.syntax.all._
import scribe.LoggerSupport

import scala.util.{Try, Success, Failure}
import com.github.plokhotnyuk.jsoniter_scala.core.readFromArray

trait Service[F[_], A, B] {
  def handle(request: A): F[B]
}

trait MethodName {
  def methodName: String
}

trait JsonRpcService[F[_]] extends Service[F, Message, Response]
trait NamedJsonRpcService[F[_]] extends JsonRpcService[F] with MethodName

object Service {
  def request[F[_]: Async, A, B](endpoint: Endpoint[A, B])(
      f: Service[F, A, B]
  ): NamedJsonRpcService[F] = new NamedJsonRpcService[F] {
    override def methodName: String = endpoint.method
    override def handle(message: Message): F[Response] = {
      import endpoint.{codecA, codecB}
      val method = endpoint.method
      message match {
        case Request(`method`, params, id, _, _) =>
          val paramsJson = params.getOrElse(RawJson.nullValue)
          Try(readFromArray[A](paramsJson.value)) match {
            case Success(value) =>
              Async[F].attempt(f.handle(value)).flatMap {
                case Right(response) =>
                  Async[F].pure(Response.ok(RawJson.toJson(response), id))
                // Errors always have a null id because services don't have access to the real id
                case Left(err: Response.Error) => Async[F].pure(err.copy(id = id))
                case Left(err) => Async[F].pure(Response.internalError(err.toString, id))
              }
            case Failure(err) => Async[F].pure(Response.invalidParams(err.toString, id))
          }

        case Request(invalidMethod, _, id, _, _) =>
          Async[F].pure(Response.methodNotFound(invalidMethod, id))
        case _ => Async[F].pure(Response.invalidRequest(s"Expected request, obtained $message"))
      }
    }
  }

  def notification[F[_]: Async, A](
      endpoint: Endpoint[A, Unit],
      logger: LoggerSupport[Unit]
  )(
      f: Service[F, A, Unit]
  ): NamedJsonRpcService[F] = {
    new NamedJsonRpcService[F] {
      override def methodName: String = endpoint.method
      private def fail(msg: String): F[Response] = Async[F].delay {
        logger.error(msg)
        Response.None
      }

      override def handle(message: Message): F[Response] = {
        import endpoint.codecA
        val method = endpoint.method
        message match {
          case Notification(`method`, params, _, _) =>
            val paramsJson = params.getOrElse(RawJson.nullValue)
            Try(readFromArray[A](paramsJson.value)) match {
              case Success(value) => f.handle(value).map(_ => Response.None)
              case Failure(err) => fail(s"Failed to parse notification $message. Errors: $err")
            }
          case Notification(invalidMethod, _, _, _) =>
            fail(s"Expected method '$method', obtained '$invalidMethod'")
          case _ => fail(s"Expected notification, obtained $message")
        }
      }
    }
  }
}

object Services {
  def empty[F[_]](logger: LoggerSupport[Unit]): Services[F] = new Services(Nil, logger)
}

class Services[F[_]] private (
    val services: List[NamedJsonRpcService[F]],
    logger: LoggerSupport[Unit]
) {
  def request[A, B](endpoint: Endpoint[A, B])(f: A => B)(implicit F: Async[F]): Services[F] = {
    requestAsync[A, B](endpoint)(new Service[F, A, B] {
      def handle(request: A): F[B] = F.delay(f(request))
    })
  }

  def requestAsync[A, B](
      endpoint: Endpoint[A, B]
  )(f: Service[F, A, B])(implicit F: Async[F]): Services[F] = {
    addService(Service.request[F, A, B](endpoint)(f))
  }

  def notification[A](
      endpoint: Endpoint[A, Unit]
  )(f: A => Unit)(implicit F: Async[F]): Services[F] = {
    notificationAsync[A](endpoint)(new Service[F, A, Unit] {
      def handle(request: A): F[Unit] = F.delay(f(request))
    })
  }

  def notificationAsync[A](
      endpoint: Endpoint[A, Unit]
  )(f: Service[F, A, Unit])(implicit F: Async[F]): Services[F] = {
    addService(Service.notification[F, A](endpoint, logger)(f))
  }

  def byMethodName: Map[String, NamedJsonRpcService[F]] =
    services.iterator.map(s => s.methodName -> s).toMap

  def addService(service: NamedJsonRpcService[F]): Services[F] = {
    val duplicate = services.find(_.methodName == service.methodName)
    require(
      duplicate.isEmpty,
      s"Duplicate service handler for method ${duplicate.get.methodName}"
    )
    new Services(service :: services, logger)
  }
}
