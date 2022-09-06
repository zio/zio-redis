/*
 * Copyright 2021 John A. De Goes and the ZIO contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.redis.options

import zio.schema.ast.SchemaAst
import zio.schema.{DeriveSchema, DynamicValue, Schema, StandardType, TypeId}
import zio.stream.ZPipeline
import zio.{Chunk, Duration, ZIO}

import java.net.InetAddress
import scala.collection.immutable.ListMap
import scala.util.Try

trait Connection {
  sealed case class Address(ip: InetAddress, port: Int) {
    private[redis] final def stringify: String = s"${ip.getHostAddress}:$port"
  }

  sealed case class ClientEvents(readable: Boolean = false, writable: Boolean = false)

  sealed trait ClientFlag {
    private[redis] def flag: Char
    private[redis] def stringify: String = flag.toString
  }

  object ClientFlag {
    private[redis] def apply(f: Char): ClientFlag = new ClientFlag {
      val flag: Char = f
    }

    case object ToBeClosedAsap extends ClientFlag {
      val flag = 'a'
    }

    case object Blocked extends ClientFlag {
      val flag = 'b'
    }

    case object ToBeClosedAfterReply extends ClientFlag {
      val flag = 'c'
    }

    case object WatchedKeysModified extends ClientFlag {
      val flag = 'd'
    }

    case object IsMaster extends ClientFlag {
      val flag = 'M'
    }

    case object NoSpecificFlagSet extends ClientFlag {
      val flag = 'N'
    }

    case object MonitorMode extends ClientFlag {
      val flag = 'O'
    }

    case object PubSub extends ClientFlag {
      val flag = 'P'
    }

    case object ReadOnlyMode extends ClientFlag {
      val flag = 'r'
    }

    case object Replica extends ClientFlag {
      val flag = 'S'
    }

    case object Unblocked extends ClientFlag {
      val flag = 'u'
    }

    case object UnixDomainSocket extends ClientFlag {
      val flag = 'U'
    }

    case object MultiExecContext extends ClientFlag {
      val flag = 'X'
    }

    case object KeysTrackingEnabled extends ClientFlag {
      val flag = 't'
    }

    case object TrackingTargetClientInvalid extends ClientFlag {
      val flag = 'R'
    }

    case object BroadcastTrackingMode extends ClientFlag {
      val flag = 'B'
    }
  }

  sealed case class ClientInfo(
    id: Long,
    name: Option[String] = None,
    address: Option[Address] = None,
    localAddress: Option[Address] = None,
    fileDescriptor: Option[Long] = None,
    age: Option[Duration] = None,
    idle: Option[Duration] = None,
    flags: Set[ClientFlag] = Set.empty,
    databaseId: Option[Long] = None,
    subscriptions: Int = 0,
    patternSubscriptions: Int = 0,
    multiCommands: Int = 0,
    queryBufferLength: Option[Int] = None,
    queryBufferFree: Option[Int] = None,
    outputBufferLength: Option[Int] = None,
    outputListLength: Option[Int] = None,
    outputBufferMem: Option[Long] = None,
    events: ClientEvents = ClientEvents(),
    lastCommand: Option[String] = None,
    argvMemory: Option[Long] = None,
    multiMemory: Option[Long] = None,
    totalMemory: Option[Long] = None,
    replyBufferSize: Option[Long] = None,
    replyBufferPeakPosition: Option[Long] = None,
    redirectionClientId: Option[Long] = None,
    respProtocolVersion: Option[String] = None,
    user: Option[String] = None
  )

  object ClientInfoCodec extends zio.schema.codec.Codec {
    def encoder[A](schema: Schema[A]): ZPipeline[Any, Nothing, A, Byte] =
      ZPipeline.fromPush(
        ZIO.succeed((opt: Option[Chunk[A]]) =>
          ZIO
            .attempt(opt.map(values => values.flatMap(encode(schema))).getOrElse(Chunk.empty))
            .orDie
        )
      )

    def decoder[A](schema: Schema[A]): ZPipeline[Any, String, Byte, A] =
      ZPipeline.utfDecode.mapError(_.getMessage) >>> ZPipeline.mapZIO((s: String) =>
        ZIO.fromEither(decode(schema)(Chunk.fromArray(s.getBytes)))
      )

    def encode[A](schema: Schema[A]): A => Chunk[Byte] = { in =>
      val values = schema.toDynamic(in)
      val res    = encodeValue(schema, values)

      Chunk.fromArray(res.getBytes)
    }

    def decode[A](schema: Schema[A]): Chunk[Byte] => Either[String, A] =
      in => {
        val params = paramsToDynamicValue(schema, new String(in.toArray))

        params.flatMap(dv => schema.fromDynamic(dv))
      }

    private def encodeValue[A](schema: Schema[A], v: DynamicValue): String =
      (schema, v) match {
        case (Schema.Transform(f, _, _, _, _), _) => encodeValue(f, v)
        case (Schema.GenericRecord(_, fields, _), DynamicValue.Record(_, values)) =>
          fields.toChunk.map { f =>
            val value = values(f.label)
            val title = fieldNames.find(_._2 == f.label).map(_._1).getOrElse(f.label)

            s"$title=${encodeValue(f.schema, value)}"
          }
            .mkString(" ")
        case (Schema.CaseClass1(_, _, _, extractF, _), DynamicValue.Record(_, _)) =>
          val v1 = schema.fromDynamic(v).toOption.map(extractF)

          v1.map(_.toString).getOrElse("")
        case (Schema.CaseClass2(_, _, _, _, extractF1, extractF2, _), DynamicValue.Record(_, _)) =>
          val obj = schema.fromDynamic(v).toOption.get

          val v1 = extractF1(obj)
          val v2 = extractF2(obj)

          s"$v1$v2"
        case (Schema.Lazy(s), _)         => encodeValue(s(), v)
        case (_, DynamicValue.NoneValue) => ""
        case (
              Schema.Primitive(StandardType.DurationType, _),
              DynamicValue.Primitive(d: java.time.Duration, _)
            ) =>
          d.getSeconds.toString
        case (_, DynamicValue.SomeValue(value))          => encodeValue(schema, value)
        case (_, DynamicValue.Primitive(None, _))        => ""
        case (_, DynamicValue.Primitive(Some(value), _)) => value.toString
        case (_, DynamicValue.Primitive(value, _))       => value.toString
        case x                                           => throw new IllegalArgumentException(s"Unsupported schema: $x $v")
      }

    private def createDynamicValue(value: DynamicValue, optional: Boolean): DynamicValue =
      if (optional) DynamicValue.SomeValue(value) else value

    // Creates a dynamicValue from a string and a standardType
    private def parseDynamicValue[A](
      value: String,
      standardType: StandardType[A],
      optional: Boolean
    ): Either[String, DynamicValue] = standardType match {
      case _ if value == "" && optional => Right(DynamicValue.NoneValue)
      case StandardType.StringType =>
        Right(createDynamicValue(DynamicValue.Primitive(value, StandardType.StringType), optional))
      case StandardType.DurationType =>
        Try(value.toLong)
          .map(s => DynamicValue.Primitive(java.time.Duration.ofSeconds(s), StandardType.DurationType))
          .toEither
          .left
          .map(_ => s"Can not convert $value to java.time.Duration")
          .map(createDynamicValue(_, optional))
      case StandardType.LongType =>
        Try(value.toLong)
          .map(DynamicValue.Primitive(_, StandardType.LongType))
          .toEither
          .left
          .map(_ => s"Can not convert $value to Long")
          .map(createDynamicValue(_, optional))
      case StandardType.IntType =>
        Try(value.toInt)
          .map(DynamicValue.Primitive(_, StandardType.IntType))
          .toEither
          .left
          .map(_ => s"Can not convert $value to Int")
          .map(createDynamicValue(_, optional))
      case _ => Left(s"Unsupported type $standardType")
    }

    // try to parse dynamic value from string and a schema
    private def getDynamicValue[A](schema: Schema[A], name: String, value: String): Either[String, DynamicValue] =
      schema.ast match {
        case SchemaAst.Product(_, _, fields, _) =>
          val field = fields.find(_._1 == name)
          field match {
            case Some((_, SchemaAst.Value(fieldType, _, optional))) =>
              parseDynamicValue(value, fieldType, optional)
            case Some((_, p @ SchemaAst.Product(_, _, fields, optional))) =>
              val values = ListMap(fields.toList.map { case (fieldName, _) =>
                (fieldName, getDynamicValue(p.toSchema, fieldName, value).getOrElse(DynamicValue.NoneValue))
              }: _*)
              if (optional) {
                Right(DynamicValue.SomeValue(DynamicValue.Record(p.id, values)))
              } else {
                Right(DynamicValue.Record(p.id, values))
              }

            case Some((_, SchemaAst.ListNode(fieldType, _, _))) =>
              val s = fieldType.toSchema
              getDynamicValue(s, name, value)
            case Some((_, x)) => Left(s"Unsupported type: $x")
            case None         => Left(s"Field $name not found")
          }
        case SchemaAst.ListNode(item, _, _) => Left(s"ListNode is not supported: $item")
        case x                              => Left(s"Unsupported type: $x")
      }

    // ClientInfo case class fields does not exactly match with redis output (https://redis.io/commands/client-list/)
    private val fieldNames = Map(
      "addr"      -> "address",
      "laddr"     -> "localAddress",
      "fd"        -> "fileDescriptor",
      "db"        -> "databaseId",
      "sub"       -> "subscriptions",
      "psub"      -> "patternSubscriptions",
      "multi"     -> "multiCommands",
      "qbuf"      -> "queryBufferLength",
      "qbuf-free" -> "queryBufferFree",
      "argv-mem"  -> "argvMemory",
      "multi-mem" -> "multiMemory",
      "rbs"       -> "replyBufferSize",
      "rbp"       -> "replyBufferPeakPosition",
      "obl"       -> "outputBufferLength",
      "oll"       -> "outputListLength",
      "omem"      -> "outputBufferMem",
      "tot-mem"   -> "totalMemory",
      "cmd"       -> "lastCommand",
      "redir"     -> "redirectionClientId",
      "resp"      -> "respProtocolVersion"
    )

    private def paramsToDynamicValue[A](schema: Schema[A], str: String): Either[String, DynamicValue] = {
      // fields are encoded as "fieldName=value" separated with whitespaces
      val allFields = str
        .split("\\s|=")
        .grouped(2)
        .map { v =>
          val fieldName = fieldNames.getOrElse(v(0), v(0))
          val value     = getDynamicValue(schema, fieldName, if (v.length == 2) v(1) else "")

          value.map((fieldName, _))
        }
        .toList
        .groupBy(_.isRight)
      val fields = allFields(true).flatMap(_.toOption)

      // only match known fields and ignore the unknown ones
      val values = ListMap(fields: _*)
      Right(DynamicValue.Record(TypeId.fromTypeName("ClientInfo"), values))
    }
  }

  object ClientInfo {
    implicit val addressSchema: Schema[Option[Address]] = Schema.CaseClass1[String, Option[Address]](
      TypeId.fromTypeName("Address"),
      field = Schema.Field[String]("ip", Schema.primitive[String]),
      construct = ip => {
        if (ip.contains(":")) {
          val parts   = ip.split(":")
          val addr    = parts(0)
          val portStr = parts(1)
          Some(Address(InetAddress.getByName(addr), portStr.toInt))
        } else {
          None
        }
      },
      extractField = address => address.map(c => c.ip.getHostAddress + ":" + c.port).getOrElse("")
    )
    implicit val clientEventsSchema: Schema[ClientEvents] = Schema.CaseClass2(
      TypeId.fromTypeName("ClientEvents"),
      field1 = Schema.Field[String]("readable", Schema.primitive[String]),
      field2 = Schema.Field[String]("writable", Schema.primitive[String]),
      construct = (readable: String, writable: String) => ClientEvents(readable == "r", writable == "w"),
      extractField1 = c => if (c.readable) "r" else "",
      extractField2 = c => if (c.writable) "w" else ""
    )
    implicit val clientFlagSchema: Schema[ClientFlag] = Schema
      .Primitive(StandardType.StringType)
      .transform[ClientFlag](
        s => ClientFlag.apply(s.head),
        _.stringify
      )
    implicit val clientFlagsSchema: Schema[Set[ClientFlag]] = Schema
      .Primitive(StandardType.StringType)
      .transform[Set[ClientFlag]](
        s => s.toCharArray.map(ClientFlag.apply).toSet,
        s => s.map(_.stringify).mkString("")
      )
    implicit val schema: Schema[ClientInfo] = DeriveSchema.gen[ClientInfo]

    private val decoder = ClientInfoCodec.decode(schema)
    private val encoder = ClientInfoCodec.encode(schema)

    def decode(in: String): Either[String, ClientInfo] = decoder(Chunk.fromArray(in.getBytes))
    def encode(info: ClientInfo): String               = new String(encoder(info).toArray)
  }

  sealed trait ClientListType { self =>
    private[redis] final def stringify: String =
      self match {
        case ClientListType.Normal  => "NORMAL"
        case ClientListType.Master  => "MASTER"
        case ClientListType.Replica => "REPLICA"
        case ClientListType.PubSub  => "PUBSUB"
      }
  }

  object ClientListType {
    case object Normal  extends ClientListType
    case object Master  extends ClientListType
    case object Replica extends ClientListType
    case object PubSub  extends ClientListType
  }

  sealed trait ClientListFilter {
    private[redis] def stringify: String
  }

  object ClientListFilter {
    case object All extends ClientListFilter {
      private[redis] def stringify: String = "all"
    }

    sealed case class Type(clientType: ClientListType) extends ClientListFilter {
      private[redis] final def stringify: String = s"TYPE"
    }

    sealed case class Id(id: Long, ids: Long*) extends ClientListFilter {
      private[redis] final def stringify: String = "ID"
    }
  }

  sealed trait ClientKillFilter

  object ClientKillFilter {
    sealed case class Address(ip: InetAddress, port: Int) extends ClientKillFilter {
      private[redis] final def stringify: String = s"${ip.getHostAddress}:$port"
    }
    sealed case class LocalAddress(ip: InetAddress, port: Int) extends ClientKillFilter {
      private[redis] final def stringify: String = s"${ip.getHostAddress}:$port"
    }
    sealed case class Id(id: Long)                 extends ClientKillFilter
    sealed case class Type(clientType: ClientType) extends ClientKillFilter
    sealed case class User(username: String)       extends ClientKillFilter
    sealed case class SkipMe(skip: Boolean)        extends ClientKillFilter
  }

  sealed trait ClientPauseMode { self =>
    private[redis] final def stringify: String =
      self match {
        case ClientPauseMode.All   => "ALL"
        case ClientPauseMode.Write => "WRITE"
      }
  }

  object ClientPauseMode {
    case object All   extends ClientPauseMode
    case object Write extends ClientPauseMode
  }

  sealed case class ClientTrackingFlags(
    clientSideCaching: Boolean,
    trackingMode: Option[ClientTrackingMode] = None,
    noLoop: Boolean = false,
    caching: Option[Boolean] = None,
    brokenRedirect: Boolean = false
  )

  sealed case class ClientTrackingInfo(
    flags: ClientTrackingFlags,
    redirect: ClientTrackingRedirect,
    prefixes: Set[String] = Set.empty
  )

  sealed trait ClientTrackingMode { self =>
    private[redis] final def stringify: String =
      self match {
        case ClientTrackingMode.OptIn     => "OPTIN"
        case ClientTrackingMode.OptOut    => "OPTOUT"
        case ClientTrackingMode.Broadcast => "BCAST"
      }

  }

  object ClientTrackingMode {
    case object OptIn     extends ClientTrackingMode
    case object OptOut    extends ClientTrackingMode
    case object Broadcast extends ClientTrackingMode
  }

  sealed trait ClientTrackingRedirect

  object ClientTrackingRedirect {
    case object NotEnabled                         extends ClientTrackingRedirect
    case object NotRedirected                      extends ClientTrackingRedirect
    sealed case class RedirectedTo(clientId: Long) extends ClientTrackingRedirect
  }

  sealed trait ClientType { self =>
    private[redis] final def stringify: String =
      self match {
        case ClientType.Normal  => "normal"
        case ClientType.Master  => "master"
        case ClientType.Replica => "replica"
        case ClientType.PubSub  => "pubsub"
      }
  }

  object ClientType {
    case object Normal  extends ClientType
    case object Master  extends ClientType
    case object Replica extends ClientType
    case object PubSub  extends ClientType
  }

  sealed trait UnblockBehavior { self =>
    private[redis] final def stringify: String =
      self match {
        case UnblockBehavior.Timeout => "TIMEOUT"
        case UnblockBehavior.Error   => "ERROR"
      }
  }

  object UnblockBehavior {
    case object Timeout extends UnblockBehavior
    case object Error   extends UnblockBehavior
  }

}
