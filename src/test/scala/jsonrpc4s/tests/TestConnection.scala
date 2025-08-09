package jsonrpc4s.testkit

import java.io.PipedInputStream
import java.io.PipedOutputStream
import cats.effect.Resource
import cats.syntax.all._
import jsonrpc4s.Connection
import jsonrpc4s.InputOutput
import jsonrpc4s.RpcClient
import jsonrpc4s.Services

/**
 * A bi-directional connection between two running JSON-RPC entities named Alice and Bob.
 *
 * @param alice the running instance for Alice.
 * @param aliceIO the input/output streams for Alice.
 * @param bob the running instance for Bob.
 * @param bobIO the input/output streams for Bob.
 */
final class TestConnection[F[_]](
    val alice: Connection[F],
    val aliceIO: InputOutput[F],
    val bob: Connection[F],
    val bobIO: InputOutput[F]
) {
  def cancel(implicit F: cats.effect.kernel.Async[F]): F[Unit] = {
    aliceIO.close *> bobIO.close *> alice.cancel *> bob.cancel
  }
}

object TestConnection {

  /**
   * Instantiate bi-directional communication between a client and server.
   *
   * Useful for testing purposes.
   *
   * @param clientServices services implemented by the client.
   * @param serverServices services implemented by the server.
   */
  def apply[F[_]: cats.effect.kernel.Async](
      clientServices: RpcClient[F] => Services[F],
      serverServices: RpcClient[F] => Services[F]
  ): Resource[F, TestConnection[F]] = {
    val inAlice = new PipedInputStream()
    val inBob = new PipedInputStream()
    val outAlice = new PipedOutputStream(inBob)
    val outBob = new PipedOutputStream(inAlice)
    for {
      aliceIO <- InputOutput.resource[F](inAlice, outAlice)
      bobIO <- InputOutput.resource[F](inBob, outBob)
      aliceConn <- Connection.simple(aliceIO, "alice")(clientServices)
      bobConn <- Connection.simple(bobIO, "bob")(serverServices)
    } yield new TestConnection[F](aliceConn, aliceIO, bobConn, bobIO)
  }

}
