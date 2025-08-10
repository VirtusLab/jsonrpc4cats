package jsonrpc4cats.tests

import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import weaver._
import cats.effect.IO
import scribe.file.FileWriter

object FileLoggerSuite extends SimpleIOSuite {

  test("logs should not go to stdout") {
    IO {
      val path = Files.createTempFile("lsp4s", ".log")
      val baos = new ByteArrayOutputStream()

      val writer = new FileWriter(path).flushAlways
      Console.withOut(new PrintStream(baos)) {
        val logger = scribe.Logger("lsp4s").orphan().withHandler(writer = writer)
        logger.info("This is info")
        logger.warn("This is warning")
        logger.error("This is error")
      }
      val obtainedOut = baos.toString()
      val obtainedLogs =
        new String(Files.readAllBytes(path), StandardCharsets.UTF_8)

      val outIsEmpty = obtainedOut.isEmpty
      val logsContainMessages = List("info", "warning", "error").forall { message =>
        obtainedLogs.contains(s"This is $message")
      }

      expect(outIsEmpty) and expect(logsContainMessages)
    }
  }
}
