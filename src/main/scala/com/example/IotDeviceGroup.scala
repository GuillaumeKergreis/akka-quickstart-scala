package com.example

import java.util.concurrent.TimeUnit

import akka.actor.typed.{ActorRef, Behavior, PostStop, Signal}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import com.example.IotDeviceManager.RequestAllTemperatures

import scala.concurrent.duration.FiniteDuration

object IotDeviceGroup {
  def apply(groupId: String): Behavior[Command] =
    Behaviors.setup(context => new IotDeviceGroup(context, groupId))

  trait Command

  private final case class DeviceTerminated(device: ActorRef[IotDevice.Command], groupId: String, deviceId: String)
    extends Command

}

class IotDeviceGroup(context: ActorContext[IotDeviceGroup.Command], groupId: String)
  extends AbstractBehavior[IotDeviceGroup.Command](context) {
  import IotDeviceGroup._
  import IotDeviceManager.{ DeviceRegistered, ReplyDeviceList, RequestDeviceList, RequestTrackDevice }

  private var deviceIdToActor = Map.empty[String, ActorRef[IotDevice.Command]]

  context.log.info(s"IotDeviceGroup $groupId started")

  override def onMessage(msg: Command): Behavior[Command] =
    msg match {
      case trackMsg @ RequestTrackDevice(`groupId`, deviceId, replyTo) =>
        deviceIdToActor.get(deviceId) match {
          case Some(deviceActor) =>
            replyTo ! DeviceRegistered(deviceActor)
          case None =>
            context.log.info("Creating device actor for {}", trackMsg.deviceId)
            val deviceActor = context.spawn(IotDevice(groupId, deviceId), s"device-$deviceId")
            context.watchWith(deviceActor, DeviceTerminated(deviceActor, groupId, deviceId))
            deviceIdToActor += deviceId -> deviceActor
            replyTo ! DeviceRegistered(deviceActor)
        }
        this

      case RequestTrackDevice(gId, _, _) =>
        context.log.warn(s"Ignoring TrackDevice request for $gId. This actor is responsible for $groupId.")
        this

      case RequestDeviceList(requestId, gId, replyTo) =>
        if (gId == groupId) {
          replyTo ! ReplyDeviceList(requestId, deviceIdToActor.keySet)
          this
        } else
          Behaviors.unhandled

      case DeviceTerminated(_, _, deviceId) =>
        context.log.info(s"Device actor for $deviceId has been terminated")
        deviceIdToActor -= deviceId
        this

      case RequestAllTemperatures(requestId, gId, replyTo) =>
        if (gId == groupId) {
          context.spawnAnonymous(
            IotDeviceGroupQuery(deviceIdToActor, requestId = requestId, requester = replyTo, FiniteDuration(3, TimeUnit.SECONDS)))
          this
        } else
          Behaviors.unhandled
    }

  override def onSignal: PartialFunction[Signal, Behavior[Command]] = {
    case PostStop =>
      context.log.info(s"IotDeviceGroup $groupId stopped")
      this
  }
}
