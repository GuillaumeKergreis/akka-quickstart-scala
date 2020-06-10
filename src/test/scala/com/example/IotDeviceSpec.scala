package com.example

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import com.example.IotDeviceManager.{DeviceRegistered, ReplyDeviceList, RequestDeviceList, RequestTrackDevice}
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
    probe.expectNoMessage(FiniteDuration(500, "milliseconds"))
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
}
