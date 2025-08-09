package jsonrpc4s

import java.io.InputStream
import java.io.OutputStream
import cats.effect.{Resource, Sync}
import cats.syntax.all._

/** Wrapper around a pair of input/output streams. */
final class InputOutput[F[_]](val in: InputStream, val out: OutputStream) {
  def close(implicit F: Sync[F]): F[Unit] = {
    Sync[F].delay {
      println(s"Closing input/output streams for $this")
      in.close()
      out.close()
      println(s"Input/output streams closed for $this")
    }
  }
}

object InputOutput {
  def apply[F[_]](in: InputStream, out: OutputStream): InputOutput[F] =
    new InputOutput[F](in, out)

  def resource[F[_]: Sync](in: InputStream, out: OutputStream): Resource[F, InputOutput[F]] = {
    Resource.make(
      Sync[F].delay(new InputOutput[F](in, out))
    ) { io => Sync[F].delay(println("Closing input/output streams (resource)")) *> io.close }
  }
}
