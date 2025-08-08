package jsonrpc4s

import cats.effect.{IO, Async, Resource}
import cats.effect.kernel.Fiber
import scribe.Logger
import scribe.LoggerSupport

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
  def cancel(implicit F: Async[F]): F[Unit] = server.cancel
}

object Connection {

  def simple[F[_]: Async](io: InputOutput[F], name: String)(
      f: RpcClient[F] => Services[F]
  ): Resource[F, Connection[F]] = {
    Connection(io, Logger(s"$name-server"), Logger(s"$name-client"))(f)
  }

  def apply[F[_]: Async](
      io: InputOutput[F],
      serverLogger: LoggerSupport,
      clientLogger: LoggerSupport
  )(
      f: RpcClient[F] => Services[F]
  ): Resource[F, Connection[F]] = {
    for {
      client <- Resource.eval(RpcClient.fromOutputStream(io.out, clientLogger))
      messages = LowLevelMessage
        .fromInputStream(io.in, serverLogger)
        .evalMap(msg => Async[F].delay(LowLevelMessage.toMsg(msg)))
      server = RpcServer(messages, client, f(client), serverLogger)
      fiber <- Resource.make(
        Async[F].start(server.startTask(Async[F].unit))
      ) { fiber => fiber.cancel }
    } yield Connection(client, fiber)
  }
}
