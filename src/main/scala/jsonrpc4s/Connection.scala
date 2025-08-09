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
        .fromInputStream(io.in, serverLogger, name)
        .evalMap(msg => Async[F].delay(LowLevelMessage.toMsg(msg)))
        .evalTap(msg => Async[F].delay(println(s"Received message after toMsg:\n$msg\n")))
        .onFinalize(Async[F].delay(println(s"input stream closed for connection $name")))
      server = RpcServer(messages, client, f(client), serverLogger)
      fiber <- Resource.make(
        Async[F].start(server.startTask)
      ) { fiber =>
        val closeOutputFirst =
          Async[F].delay(println(s"[$name] Closing OUTPUT stream to unblock peer input: $io")) *>
            Async[F].delay(io.out.close()).attempt.void *>
            Async[F].delay(println(s"[$name] Closed OUTPUT stream"))

        val attemptCancel =
          Async[F].delay(println(s"[$name] Cancelling server fiber")) *> fiber.cancel
        // Fire-and-forget cancel to avoid deadlocks between paired connections
        // Async[F]
        //   .start(
        //     Async[F]
        //       .timeoutTo(
        //         fiber.cancel *> Async[F].delay(println(s"[$name] Server fiber cancelled")),
        //         scala.concurrent.duration.DurationInt(1).second,
        //         Async[F].delay(println(s"[$name] Cancel timed out; proceeding with teardown"))
        //       )
        //   )
        //   .void

        val closeInputLast =
          Async[F].delay(
            println(s"[$name] Closing INPUT stream (should be unblocked by peer output close)")
          ) *>
            Async[F].delay(io.in.close()).attempt.void *>
            Async[F].delay(println(s"[$name] Closed INPUT stream"))

        closeOutputFirst *> attemptCancel *> closeInputLast *>
          Async[F].delay(println(s"[$name] Connection release finished"))
      }
    } yield Connection(client, fiber)
}
