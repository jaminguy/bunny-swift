// This source code is dual-licensed under the Apache License, version 2.0,
// and the MIT license.
//
// SPDX-License-Identifier: Apache-2.0 OR MIT
//
// Copyright (c) 2025-2026 Michael S. Klishin

import AMQPProtocol
import Foundation
import NIOConcurrencyHelpers
import Recovery
import Testing

@testable import BunnySwift

/// Stands in for `Connection` with a broker that answers `basic.consume` and,
/// still inside the write call, delivers a message for the new consumer. A
/// real broker sends consume-ok followed at once by up to prefetch deliveries
/// when the queue has ready messages, and the dispatcher can hand those to the
/// channel before `basicConsume` resumes; the stub makes that happen every time.
/// Each `basic.consume` takes the next queued answer, and `okThenDeliver` once
/// the queue is empty.
private actor EarlyDeliveryStubConnection: ChannelConnection {
  enum ConsumeAnswer {
    /// consume-ok echoing the request's tag (a server tag when it is empty),
    /// then one delivery for that tag.
    case okThenDeliver
    /// consume-ok carrying a tag other than the one requested.
    case okWithTag(String)
    /// The write of `basic.consume` throws, as it does when the socket is lost.
    case writeFails
  }

  nonisolated let topologyRegistry = TopologyRegistry()
  var frameMax: UInt32 { FrameDefaults.maxSize }

  private var channel: Channel?
  private var consumeAnswers: [ConsumeAnswer]
  private(set) var sentMethods: [AMQPMethod] = []

  init(consumeAnswers: [ConsumeAnswer] = []) {
    self.consumeAnswers = consumeAnswers
  }

  func attach(_ channel: Channel) {
    self.channel = channel
  }

  func send(_ frame: Frame) async throws {
    guard case .method(let channelID, let method) = frame, let channel else { return }
    var consumeAnswer = ConsumeAnswer.okThenDeliver
    if case .basicConsume = method, !consumeAnswers.isEmpty {
      consumeAnswer = consumeAnswers.removeFirst()
    }
    if case .basicConsume = method, case .writeFails = consumeAnswer {
      throw ConnectionError.notConnected
    }
    sentMethods.append(method)
    switch method {
    case .channelOpen:
      await channel.handleFrame(
        .method(channelID: channelID, method: .channelOpenOk(ChannelOpenOk())))
    case .basicConsume(let consume):
      switch consumeAnswer {
      case .okThenDeliver:
        let tag = consume.consumerTag.isEmpty ? "amq.ctag-server" : consume.consumerTag
        await channel.handleFrame(
          .method(
            channelID: channelID, method: .basicConsumeOk(BasicConsumeOk(consumerTag: tag))))
        await deliver(7, consumerTag: tag, to: channel, channelID: channelID)
      case .okWithTag(let tag):
        await channel.handleFrame(
          .method(
            channelID: channelID, method: .basicConsumeOk(BasicConsumeOk(consumerTag: tag))))
      case .writeFails:
        break
      }
    case .basicCancel(let cancel):
      await channel.handleFrame(
        .method(
          channelID: channelID,
          method: .basicCancelOk(BasicCancelOk(consumerTag: cancel.consumerTag))))
    case .basicGet:
      await channel.handleFrame(
        .method(channelID: channelID, method: .basicGetEmpty(BasicGetEmpty())))
    default:
      break
    }
  }

  func writeBatch(_ frames: [Frame]) async throws {}

  func flush() async {}

  func channelClosed(_ channelID: UInt16) async {}

  var sentConsumeTags: [String] {
    sentMethods.compactMap {
      if case .basicConsume(let consume) = $0 { consume.consumerTag } else { nil }
    }
  }

  var sentNacks: [BasicNack] {
    sentMethods.compactMap {
      if case .basicNack(let nack) = $0 { nack } else { nil }
    }
  }
}

/// The outcome of one task, readable without awaiting the task, so a call the
/// code under test parks forever, or a delivery the channel dropped, shows up
/// as "no outcome within the deadline" rather than hanging the suite.
private final class Outcome<Value: Sendable>: Sendable {
  private let value = NIOLockedValueBox<Result<Value, any Error>?>(nil)

  var result: Result<Value, any Error>? { value.withLockedValue { $0 } }

  func record(_ body: @escaping @Sendable () async throws -> Value) {
    Task {
      do {
        let produced = try await body()
        value.withLockedValue { $0 = .success(produced) }
      } catch {
        value.withLockedValue { $0 = .failure(error) }
      }
    }
  }
}

private func eventually(
  within seconds: Double = 2, _ condition: @escaping @Sendable () async -> Bool
) async -> Bool {
  let deadline = ContinuousClock.now + .seconds(seconds)
  while ContinuousClock.now < deadline {
    if await condition() { return true }
    try? await Task.sleep(for: .milliseconds(5))
  }
  return await condition()
}

@Suite("Consumer registration ordering")
struct ChannelConsumeRegistrationTests {

  /// Catches a consumer registered only after consume-ok returns: the broker's
  /// first deliveries find no consumer and are dropped, and the broker holds
  /// each one unacked until the channel closes.
  @Test("A delivery handled before basicConsume resumes reaches the consumer")
  func deliveryBeforeConsumeResumesReachesTheConsumer() async throws {
    let channelID: UInt16 = 1
    let stub = EarlyDeliveryStubConnection()
    let channel = Channel(connection: stub, channelID: channelID)
    await stub.attach(channel)
    try await channel.open()

    let first = Outcome<UInt64?>()
    first.record {
      let stream = try await channel.basicConsume(queue: "q")
      var iterator = stream.makeAsyncIterator()
      return await iterator.next()?.deliveryInfo.deliveryTag
    }

    let delivered = await eventually { first.result != nil }
    #expect(delivered, "the delivery that followed consume-ok never reached the consumer stream")
    guard delivered else { return }
    #expect(try first.result?.get() == 7)
    #expect(await stub.sentNacks.isEmpty)
  }

  /// Catches a consumer registered under a tag other than the one sent: the
  /// tag must be chosen and registered before `basic.consume` is written.
  @Test("basicConsume without a tag sends a unique client-chosen tag")
  func basicConsumeSendsAClientChosenTag() async throws {
    let stub = EarlyDeliveryStubConnection()
    let channel = Channel(connection: stub, channelID: 1)
    await stub.attach(channel)
    try await channel.open()

    let first = try await channel.basicConsume(queue: "q")
    let second = try await channel.basicConsume(queue: "q")

    let sent = await stub.sentConsumeTags
    #expect(sent.count == 2)
    #expect(sent.allSatisfy { !$0.isEmpty }, "basic.consume went out with an empty tag")
    #expect(sent == [first.consumerTag, second.consumerTag])
    #expect(first.consumerTag != second.consumerTag)
  }

  /// Catches a registration left behind when `basic.consume` fails: the
  /// caller never gets that stream, and a retry with the same tag on the
  /// channel is refused as a duplicate.
  @Test("A failed basic.consume removes its registration")
  func failedConsumeRemovesItsRegistration() async throws {
    let stub = EarlyDeliveryStubConnection(consumeAnswers: [.writeFails])
    let channel = Channel(connection: stub, channelID: 1)
    await stub.attach(channel)
    try await channel.open()

    await #expect(throws: ConnectionError.self) {
      _ = try await channel.basicConsume(queue: "q", consumerTag: "worker")
    }

    try await expectConsumeWithTagReceivesItsDelivery(channel, tag: "worker")
  }

  /// Catches a consume-ok whose tag differs from the one registered being
  /// accepted, or the registration under the requested tag being kept after
  /// it: the stream would wait under a tag the broker never delivers to, and a
  /// retry with the same tag is refused as a duplicate.
  @Test("A consume-ok carrying another tag fails the consume and removes its registration")
  func mismatchedConsumeOkTagFailsAndRemovesRegistration() async throws {
    let stub = EarlyDeliveryStubConnection(consumeAnswers: [.okWithTag("amq.ctag-other")])
    let channel = Channel(connection: stub, channelID: 1)
    await stub.attach(channel)
    try await channel.open()

    await #expect(throws: ConnectionError.self) {
      _ = try await channel.basicConsume(queue: "q", consumerTag: "worker")
    }

    try await expectConsumeWithTagReceivesItsDelivery(channel, tag: "worker")
  }

  /// A consume with `tag` succeeds on `channel` and its stream receives the
  /// delivery the stub sends behind consume-ok.
  private func expectConsumeWithTagReceivesItsDelivery(_ channel: Channel, tag: String)
    async throws
  {
    let retry = Outcome<UInt64?>()
    retry.record {
      let stream = try await channel.basicConsume(queue: "q", consumerTag: tag)
      var iterator = stream.makeAsyncIterator()
      return await iterator.next()?.deliveryInfo.deliveryTag
    }
    let completed = await eventually { retry.result != nil }
    #expect(completed, "the retry with the same tag never received its delivery")
    guard completed else { return }
    #expect(try retry.result?.get() == 7, "the retry with the same tag was refused")
  }

  /// Catches the basic.get slot being queued only after the request is
  /// written: a get-ok or get-empty handled first found no slot and was
  /// dropped, and basicGet parked forever.
  @Test("A get-empty handled before basicGet resumes completes the call")
  func getReplyBeforeResumeCompletesTheCall() async throws {
    let stub = EarlyDeliveryStubConnection()
    let channel = Channel(connection: stub, channelID: 1)
    await stub.attach(channel)
    try await channel.open()

    let outcome = Outcome<Bool>()
    outcome.record { try await channel.basicGet(queue: "q") == nil }

    let completed = await eventually { outcome.result != nil }
    #expect(completed, "basicGet never completed: the get-empty that arrived first was dropped")
    guard completed else { return }
    #expect(try outcome.result?.get() == true)
  }
}
