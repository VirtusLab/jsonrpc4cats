package jsonrpc4s

import cats.effect.{Async, Resource}
import cats.effect.kernel.Fiber
import scribe.Logger
import scribe.LoggerSupport
import cats.syntax.all._

/**
 * A connection with another JSON-RPC entity.
 *
 * @param client used to send requests/notification to the other entity.
 * @param server server on this side listening to input streams from the other entity.
 */
final case class Connection[F[_]](
    client: RpcClient[F],
    server: Fiber[F, Throwable, Unit]
) {
  def cancel: F[Unit] = server.cancel
}

object Connection {

  def simple[F[_]: Async](io: InputOutput[F], name: String)(
      f: RpcClient[F] => Services[F]
  ): Resource[F, Connection[F]] =
    Connection(io, Logger(s"$name-server"), Logger(s"$name-client"), name)(f)

  def apply[F[_]: Async](
      io: InputOutput[F],
      serverLogger: LoggerSupport,
      clientLogger: LoggerSupport,
      name: String
  )(
      f: RpcClient[F] => Services[F]
  ): Resource[F, Connection[F]] =
    for {
      client <- Resource.eval(RpcClient.fromOutputStream(io.out, clientLogger))
      messages = LowLevelMessage
        .fromInputStream(io.in, serverLogger)
        .evalMap(msg => Async[F].delay(LowLevelMessage.toMsg(msg)))
        .evalTap(msg => Async[F].delay(println(s"Received message after toMsg:\n$msg\n")))
      server = RpcServer(messages, client, f(client), serverLogger)
      fiber <- Resource.make(
        Async[F].start(server.startTask)
      ) { fiber =>
        Async[F].delay(println(s"Cancelling connection $name")) *> fiber.cancel *>
          Async[F].delay(println(s"Connection $name cancelled"))
      }
    } yield Connection(client, fiber)
}
