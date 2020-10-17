package jsonstreamer

import java.io.InputStream
import java.nio.ByteBuffer

import jsonstreamer.Streamer.Row
import org.jsfr.json.{JsonSurfer, ParsingContext, SurfingConfiguration, TypedJsonPathListener}
import zio.stream.{Stream, Take, ZStream}
import zio.{Cause, Promise, Queue, Runtime, Task, ZIO}

import scala.reflect.ClassTag

trait Streamer[T] {
  def apply(origin: Stream[Throwable, ByteBuffer]): Task[ZStream[Any, Throwable, T]]
}

final case class JsonStreamer(surfer: JsonSurfer, jsonPath: String, rts: Runtime[Any])(
  additionalBindings: Seq[Binding[_]] = Seq.empty
) extends Streamer[Row] {
  import Streamer._

  override def apply(origin: Stream[Throwable, ByteBuffer]): Task[ZStream[Any, Throwable, Row]] =
    for {
      queue        <- Queue.unbounded[Take[Throwable, Row]]
      status       <- Promise.make[Throwable, Int]
      jsonProducer = origin.mapConcat(_.array())
      _ <- (jsonProducer.toInputStream.use { in =>
            streamJson(rts, in)(
              additionalBindings ++ Seq(
                Binding[Row](jsonPath)(t => queue.offer(Take.Value(t))),
                Binding[Int](StatusJsonPath) {
                  case responseStatus if responseStatus != 200 =>
                    val error =
                      new RuntimeException(s"REST endpoint respond with $responseStatus status code")
                    status.fail(error) <* queue.offer(Take.Fail(Cause.fail(error)))
                  case responseStatus => status.succeed(responseStatus)
                }
              )
            )
          } <* queue.offer(Take.End)).fork
      jsonConsumer = Stream
        .fromQueueWithShutdown(queue)
        .takeWhile {
          case Take.Value(_)    => true
          case Take.End         => false
          case Take.Fail(cause) => throw cause.squash
        }
    } yield
      jsonConsumer.map {
        case Take.Value(js) => js
      }

  private def streamJson(rts: Runtime[Any], in: InputStream)(bindings: Seq[Binding[_]]): Task[Unit] =
    ZIO.effect {
      bindings
        .foldLeft(surfer.configBuilder()) {
          case (config, binding) => binding(rts, config)
        }
        .buildAndSurf(in)
    }
}

object Streamer {
  type Schema = Map[String, Map[String, String]]
  type Row    = Map[String, String]

  /**
   * Utility method for schema names normalization to make the data a bit avro-friendly.
   * @param schema - Map[ String, Map[String, String] ] representation of json like {"c1":{"name": "column1", "type":"java.lang.String"}}
   * @return schema
   */
  def normalizeSchema(schema: Schema): Schema =
    schema.mapValues(_.map {
      case (k, v) if k == "name" =>
        k -> v.toLowerCase.replace(" ", "_").replace(".", "")
      case pair => pair
    })
}

/**
 *
 * @param jsonPath
 * @param f
 * @param C
 * @tparam T - json object type to convert to.
 */
final case class Binding[T](jsonPath: String)(f: T => Task[_])(implicit C: ClassTag[T])
  extends ((Runtime[Any], SurfingConfiguration.Builder) => SurfingConfiguration.Builder) {

  override def apply(rts: Runtime[Any], v1: SurfingConfiguration.Builder): SurfingConfiguration.Builder =
    v1.bind(
      jsonPath,
      C.runtimeClass.asInstanceOf[Class[T]],
      new TypedJsonPathListener[T] {
        override def onTypedValue(t: T, parsingContext: ParsingContext): Unit = rts.unsafeRun(f(t))
      }
    )
}
