package com.example

import java.util.concurrent.TimeUnit

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import com.example.IotDeviceGroupQuery.WrappedRespondTemperature
import com.example.IotDeviceManager.{DeviceNotAvailable, DeviceRegistered, DeviceTimedOut, ReplyDeviceList, RequestAllTemperatures, RequestDeviceList, RequestTrackDevice, RespondAllTemperatures, Temperature, TemperatureNotAvailable}
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration.FiniteDuration

class IotDeviceSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike {

  import IotDevice._

  "IotDevice actor" must {

    "reply with empty reading if no temperature is known" in {
      val probe = createTestProbe[RespondTemperature]()
      val iotDeviceActor = spawn(IotDevice("group", "device"))

      iotDeviceActor ! IotDevice.ReadTemperature(requestId = 42, probe.ref)
      val response = probe.receiveMessage()
      response.requestId should ===(42)
      response.value should ===(None)
    }

    "reply with latest temperature reading" in {
      val recordProbe = createTestProbe[TemperatureRecorded]()
      val readProbe = createTestProbe[RespondTemperature]()
      val iotDeviceActor = spawn(IotDevice("group", "device"))

      iotDeviceActor ! IotDevice.RecordTemperature(requestId = 1, 24.0, recordProbe.ref)
      recordProbe.expectMessage(IotDevice.TemperatureRecorded(requestId = 1))

      iotDeviceActor ! IotDevice.ReadTemperature(requestId = 2, readProbe.ref)
      val response1 = readProbe.receiveMessage()
      response1.requestId should ===(2)
      response1.value should ===(Some(24.0))

      iotDeviceActor ! IotDevice.RecordTemperature(requestId = 3, 55.0, recordProbe.ref)
      recordProbe.expectMessage(IotDevice.TemperatureRecorded(requestId = 3))

      iotDeviceActor ! IotDevice.ReadTemperature(requestId = 4, readProbe.ref)
      val response2 = readProbe.receiveMessage()
      response2.requestId should ===(4)
      response2.value should ===(Some(55.0))
    }
  }

  "be able to register a device actor" in {
    val probe = createTestProbe[DeviceRegistered]()
    val groupActor = spawn(IotDeviceGroup("group"))

    groupActor ! RequestTrackDevice("group", "device1", probe.ref)
    val registered1 = probe.receiveMessage()
    val deviceActor1 = registered1.device

    // another deviceId
    groupActor ! RequestTrackDevice("group", "device2", probe.ref)
    val registered2 = probe.receiveMessage()
    val deviceActor2 = registered2.device
    deviceActor1 should !==(deviceActor2)

    // Check that the device actors are working
    val recordProbe = createTestProbe[TemperatureRecorded]()
    deviceActor1 ! RecordTemperature(requestId = 0, 1.0, recordProbe.ref)
    recordProbe.expectMessage(TemperatureRecorded(requestId = 0))
    deviceActor2 ! IotDevice.RecordTemperature(requestId = 1, 2.0, recordProbe.ref)
    recordProbe.expectMessage(IotDevice.TemperatureRecorded(requestId = 1))
  }

  "ignore requests for wrong groupId" in {
    val probe = createTestProbe[DeviceRegistered]()
    val groupActor = spawn(IotDeviceGroup("group"))

    groupActor ! RequestTrackDevice("wrongGroup", "device1", probe.ref)
    probe.expectNoMessage(FiniteDuration(500, TimeUnit.MILLISECONDS))
  }

  "return same actor for same deviceId" in {
    val probe = createTestProbe[DeviceRegistered]()
    val groupActor = spawn(IotDeviceGroup("group"))

    groupActor ! RequestTrackDevice("group", "device1", probe.ref)
    val registered1 = probe.receiveMessage()

    // registering same again should be idempotent
    groupActor ! RequestTrackDevice("group", "device1", probe.ref)
    val registered2 = probe.receiveMessage()

    registered1.device should ===(registered2.device)
  }

  "be able to list active devices" in {
    val registeredProbe = createTestProbe[DeviceRegistered]()
    val groupActor = spawn(IotDeviceGroup("group"))

    groupActor ! RequestTrackDevice("group", "device1", registeredProbe.ref)
    registeredProbe.receiveMessage()

    groupActor ! RequestTrackDevice("group", "device2", registeredProbe.ref)
    registeredProbe.receiveMessage()

    val deviceListProbe = createTestProbe[ReplyDeviceList]()
    groupActor ! RequestDeviceList(requestId = 0, groupId = "group", deviceListProbe.ref)
    deviceListProbe.expectMessage(ReplyDeviceList(requestId = 0, Set("device1", "device2")))
  }

  "be able to list active devices after one shuts down" in {
    val registeredProbe = createTestProbe[DeviceRegistered]()
    val groupActor = spawn(IotDeviceGroup("group"))

    groupActor ! RequestTrackDevice("group", "device1", registeredProbe.ref)
    val registered1 = registeredProbe.receiveMessage()
    val toShutDown = registered1.device

    groupActor ! RequestTrackDevice("group", "device2", registeredProbe.ref)
    registeredProbe.receiveMessage()

    val deviceListProbe = createTestProbe[ReplyDeviceList]()
    groupActor ! RequestDeviceList(requestId = 0, groupId = "group", deviceListProbe.ref)
    deviceListProbe.expectMessage(ReplyDeviceList(requestId = 0, Set("device1", "device2")))

    toShutDown ! Passivate
    registeredProbe.expectTerminated(toShutDown, registeredProbe.remainingOrDefault)

    // using awaitAssert to retry because it might take longer for the groupActor
    // to see the Terminated, that order is undefined
    registeredProbe.awaitAssert {
      groupActor ! RequestDeviceList(requestId = 1, groupId = "group", deviceListProbe.ref)
      deviceListProbe.expectMessage(ReplyDeviceList(requestId = 1, Set("device2")))
    }
  }

  "return temperature value for working devices" in {
    val requester = createTestProbe[RespondAllTemperatures]()

    val device1 = createTestProbe[Command]()
    val device2 = createTestProbe[Command]()

    val deviceIdToActor = Map("device1" -> device1.ref, "device2" -> device2.ref)

    val queryActor =
      spawn(IotDeviceGroupQuery(deviceIdToActor, requestId = 1, requester = requester.ref, timeout = FiniteDuration(3, TimeUnit.SECONDS)))

    device1.expectMessageType[IotDevice.ReadTemperature]
    device2.expectMessageType[IotDevice.ReadTemperature]

    queryActor ! WrappedRespondTemperature(IotDevice.RespondTemperature(requestId = 0, "device1", Some(1.0)))
    queryActor ! WrappedRespondTemperature(IotDevice.RespondTemperature(requestId = 0, "device2", Some(2.0)))

    requester.expectMessage(
      RespondAllTemperatures(
        requestId = 1,
        temperatures = Map("device1" -> Temperature(1.0), "device2" -> Temperature(2.0))))
  }

  "return TemperatureNotAvailable for devices with no readings" in {
    val requester = createTestProbe[RespondAllTemperatures]()

    val device1 = createTestProbe[Command]()
    val device2 = createTestProbe[Command]()

    val deviceIdToActor = Map("device1" -> device1.ref, "device2" -> device2.ref)

    val queryActor =
      spawn(IotDeviceGroupQuery(deviceIdToActor, requestId = 1, requester = requester.ref, timeout = FiniteDuration(3, TimeUnit.SECONDS)))

    device1.expectMessageType[IotDevice.ReadTemperature]
    device2.expectMessageType[IotDevice.ReadTemperature]

    queryActor ! WrappedRespondTemperature(IotDevice.RespondTemperature(requestId = 0, "device1", None))
    queryActor ! WrappedRespondTemperature(IotDevice.RespondTemperature(requestId = 0, "device2", Some(2.0)))

    requester.expectMessage(
      RespondAllTemperatures(
        requestId = 1,
        temperatures = Map("device1" -> TemperatureNotAvailable, "device2" -> Temperature(2.0))))
  }

  "return DeviceNotAvailable if device stops before answering" in {
    val requester = createTestProbe[RespondAllTemperatures]()

    val device1 = createTestProbe[Command]()
    val device2 = createTestProbe[Command]()

    val deviceIdToActor = Map("device1" -> device1.ref, "device2" -> device2.ref)

    val queryActor =
      spawn(IotDeviceGroupQuery(deviceIdToActor, requestId = 1, requester = requester.ref, timeout = FiniteDuration(3, TimeUnit.SECONDS)))

    device1.expectMessageType[IotDevice.ReadTemperature]
    device2.expectMessageType[IotDevice.ReadTemperature]

    queryActor ! WrappedRespondTemperature(IotDevice.RespondTemperature(requestId = 0, "device1", Some(2.0)))

    device2.stop()

    requester.expectMessage(
      RespondAllTemperatures(
        requestId = 1,
        temperatures = Map("device1" -> Temperature(2.0), "device2" -> DeviceNotAvailable)))
  }

  "return temperature reading even if device stops after answering" in {
    val requester = createTestProbe[RespondAllTemperatures]()

    val device1 = createTestProbe[Command]()
    val device2 = createTestProbe[Command]()

    val deviceIdToActor = Map("device1" -> device1.ref, "device2" -> device2.ref)

    val queryActor =
      spawn(IotDeviceGroupQuery(deviceIdToActor, requestId = 1, requester = requester.ref, timeout = FiniteDuration(3, TimeUnit.SECONDS)))

    device1.expectMessageType[IotDevice.ReadTemperature]
    device2.expectMessageType[IotDevice.ReadTemperature]

    queryActor ! WrappedRespondTemperature(IotDevice.RespondTemperature(requestId = 0, "device1", Some(1.0)))
    queryActor ! WrappedRespondTemperature(IotDevice.RespondTemperature(requestId = 0, "device2", Some(2.0)))

    device2.stop()

    requester.expectMessage(
      RespondAllTemperatures(
        requestId = 1,
        temperatures = Map("device1" -> Temperature(1.0), "device2" -> Temperature(2.0))))
  }

  "return DeviceTimedOut if device does not answer in time" in {
    val requester = createTestProbe[RespondAllTemperatures]()

    val device1 = createTestProbe[Command]()
    val device2 = createTestProbe[Command]()

    val deviceIdToActor = Map("device1" -> device1.ref, "device2" -> device2.ref)

    val queryActor =
      spawn(IotDeviceGroupQuery(deviceIdToActor, requestId = 1, requester = requester.ref, timeout = FiniteDuration(200, TimeUnit.MILLISECONDS)))

    device1.expectMessageType[IotDevice.ReadTemperature]
    device2.expectMessageType[IotDevice.ReadTemperature]

    queryActor ! WrappedRespondTemperature(IotDevice.RespondTemperature(requestId = 0, "device1", Some(1.0)))

    // no reply from device2

    requester.expectMessage(
      RespondAllTemperatures(
        requestId = 1,
        temperatures = Map("device1" -> Temperature(1.0), "device2" -> DeviceTimedOut)))
  }

  "be able to collect temperatures from all active devices" in {
    val registeredProbe = createTestProbe[DeviceRegistered]()
    val groupActor = spawn(IotDeviceGroup("group"))

    groupActor ! RequestTrackDevice("group", "device1", registeredProbe.ref)
    val deviceActor1 = registeredProbe.receiveMessage().device

    groupActor ! RequestTrackDevice("group", "device2", registeredProbe.ref)
    val deviceActor2 = registeredProbe.receiveMessage().device

    groupActor ! RequestTrackDevice("group", "device3", registeredProbe.ref)
    registeredProbe.receiveMessage()

    // Check that the device actors are working
    val recordProbe = createTestProbe[TemperatureRecorded]()
    deviceActor1 ! RecordTemperature(requestId = 0, 1.0, recordProbe.ref)
    recordProbe.expectMessage(TemperatureRecorded(requestId = 0))
    deviceActor2 ! RecordTemperature(requestId = 1, 2.0, recordProbe.ref)
    recordProbe.expectMessage(TemperatureRecorded(requestId = 1))
    // No temperature for device3

    val allTempProbe = createTestProbe[RespondAllTemperatures]()
    groupActor ! RequestAllTemperatures(requestId = 0, groupId = "group", allTempProbe.ref)
    allTempProbe.expectMessage(
      RespondAllTemperatures(
        requestId = 0,
        temperatures =
          Map("device1" -> Temperature(1.0), "device2" -> Temperature(2.0), "device3" -> TemperatureNotAvailable)))
  }

}
