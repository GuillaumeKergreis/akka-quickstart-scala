package com.example

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior, PostStop, Signal}

object IotDevice {
  def apply(groupId: String, deviceId: String): Behavior[Command] =
    Behaviors.setup(context => new IotDevice(context, groupId, deviceId))

  sealed trait Command

  final case class ReadTemperature(requestId: Long, replyTo: ActorRef[RespondTemperature]) extends Command

  final case class RespondTemperature(requestId: Long, deviceId: String, value: Option[Double])

  final case class RecordTemperature(requestId: Long, value: Double, replyTo: ActorRef[TemperatureRecorded]) extends Command

  final case class TemperatureRecorded(requestId: Long)

  case object Passivate extends Command

}

class IotDevice(context: ActorContext[IotDevice.Command], groupId: String, deviceId: String)
  extends AbstractBehavior[IotDevice.Command](context) {

  import IotDevice._

  var lastTemperatureReading: Option[Double] = None

  context.log.info(s"Device actor $groupId-$deviceId started")

  override def onMessage(msg: Command): Behavior[Command] = {
    msg match {
      case RecordTemperature(id, value, replyTo) =>
        context.log.info(s"Recorded temperature reading $value with $id")
        lastTemperatureReading = Some(value)
        replyTo ! TemperatureRecorded(id)
        this

      case ReadTemperature(id, replyTo) =>
        replyTo ! RespondTemperature(id, deviceId, lastTemperatureReading)
        this

      case Passivate =>
        Behaviors.stopped
    }
  }

  override def onSignal: PartialFunction[Signal, Behavior[Command]] = {
    case PostStop =>
      context.log.info(s"Device actor $groupId-$deviceId stopped")
      this
  }

}