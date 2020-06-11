package com.example

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, TimerScheduler}

import scala.concurrent.duration.FiniteDuration

object IotDeviceGroupQuery {

  def apply(
             deviceIdToActor: Map[String, ActorRef[IotDevice.Command]],
             requestId: Long,
             requester: ActorRef[IotDeviceManager.RespondAllTemperatures],
             timeout: FiniteDuration): Behavior[Command] = {
    Behaviors.setup { context =>
      Behaviors.withTimers { timers =>
        new IotDeviceGroupQuery(deviceIdToActor, requestId, requester, timeout, context, timers)
      }
    }
  }

  trait Command

  private case object CollectionTimeout extends Command

  final case class WrappedRespondTemperature(response: IotDevice.RespondTemperature) extends Command

  private final case class DeviceTerminated(deviceId: String) extends Command
}

class IotDeviceGroupQuery(
                        deviceIdToActor: Map[String, ActorRef[IotDevice.Command]],
                        requestId: Long,
                        requester: ActorRef[IotDeviceManager.RespondAllTemperatures],
                        timeout: FiniteDuration,
                        context: ActorContext[IotDeviceGroupQuery.Command],
                        timers: TimerScheduler[IotDeviceGroupQuery.Command])
  extends AbstractBehavior[IotDeviceGroupQuery.Command](context) {

  import IotDeviceGroupQuery._
  import IotDeviceManager.DeviceNotAvailable
  import IotDeviceManager.DeviceTimedOut
  import IotDeviceManager.RespondAllTemperatures
  import IotDeviceManager.Temperature
  import IotDeviceManager.TemperatureNotAvailable
  import IotDeviceManager.TemperatureReading

  timers.startSingleTimer(CollectionTimeout, CollectionTimeout, timeout)

  private val respondTemperatureAdapter = context.messageAdapter(WrappedRespondTemperature.apply)


  deviceIdToActor.foreach {
    case (deviceId, device) =>
      context.watchWith(device, DeviceTerminated(deviceId))
      device ! IotDevice.ReadTemperature(0, respondTemperatureAdapter)
  }

  private var repliesSoFar = Map.empty[String, TemperatureReading]
  private var stillWaiting = deviceIdToActor.keySet

  override def onMessage(msg: Command): Behavior[Command] =
    msg match {
      case WrappedRespondTemperature(response) => onRespondTemperature(response)
      case DeviceTerminated(deviceId)          => onDeviceTerminated(deviceId)
      case CollectionTimeout                   => onCollectionTimout()
    }

  private def onRespondTemperature(response: IotDevice.RespondTemperature): Behavior[Command] = {
    val reading = response.value match {
      case Some(value) => Temperature(value)
      case None        => TemperatureNotAvailable
    }

    val deviceId = response.deviceId
    repliesSoFar += (deviceId -> reading)
    stillWaiting -= deviceId

    respondWhenAllCollected()
  }

  private def onDeviceTerminated(deviceId: String): Behavior[Command] = {
    if (stillWaiting(deviceId)) {
      repliesSoFar += (deviceId -> DeviceNotAvailable)
      stillWaiting -= deviceId
    }
    respondWhenAllCollected()
  }

  private def onCollectionTimout(): Behavior[Command] = {
    repliesSoFar ++= stillWaiting.map(deviceId => deviceId -> DeviceTimedOut)
    stillWaiting = Set.empty
    respondWhenAllCollected()
  }

  private def respondWhenAllCollected(): Behavior[Command] = {
    if (stillWaiting.isEmpty) {
      requester ! RespondAllTemperatures(requestId, repliesSoFar)
      Behaviors.stopped
    } else {
      this
    }
  }

}
