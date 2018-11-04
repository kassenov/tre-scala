package utls

import java.io._
import java.util.Base64
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}

import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write

import scala.io.Source


class Serializer() {

  def exists(name: String): Boolean =
    Files.exists(Paths.get(s"serialized/$name"))

  def serialize(value: Any, name: String): Unit = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(value)
    oos.close
    val str = new String(
      Base64.getEncoder().encode(stream.toByteArray),
      UTF_8
    )
    new PrintWriter(s"serialized/$name") { write(str); close }

  }

  def deserialize(name: String): Any = {
    val str = Source.fromFile(s"serialized/$name").getLines.mkString
    val bytes = Base64.getDecoder().decode(str.getBytes(UTF_8))
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes)) {
      override def resolveClass(desc: java.io.ObjectStreamClass): Class[_] = {
        try { Class.forName(desc.getName, false, getClass.getClassLoader) }
        catch { case ex: ClassNotFoundException => super.resolveClass(desc) }
      }
    }
    val value = ois.readObject
    ois.close
    value
  }

  def saveAsJson(value: Any, name: String, folder: String = "json"): Unit = {
    implicit val formats = DefaultFormats
    val jsonString = write(value)
    new PrintWriter(s"$folder/$name.json") { write(jsonString); close }
  }

}
