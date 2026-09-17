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

/// Stands in for `Connection` with a broker that confirms a publish before the
/// write call returns: the ack reaches the channel before the publisher has
/// advanced past its flush hop and installed its confirm waiter. A real broker
/// produces this ordering whenever its ack is dispatched onto the channel
/// actor ahead of the publisher's own resumption; the stub makes it happen on
/// every publish.
private actor EagerAckStubConnection: ChannelConnection {
  nonisolated let topologyRegistry = TopologyRegistry()
  var frameMax: UInt32 { FrameDefaults.maxSize }

  private var channel: Channel?
  private(set) var batches: [[Frame]] = []
  /// Sequence numbers the stub answers with a nack instead of an ack.
  private var nackedSeqNos: Set<UInt64> = []

  func attach(_ channel: Channel) {
    self.channel = channel
  }

  func nack(seqNo: UInt64) {
    nackedSeqNos.insert(seqNo)
  }

  func send(_ frame: Frame) async throws {
    guard case .method(let channelID, let method) = frame, let answer = reply(to: method) else {
      return
    }
    await channel?.handleFrame(.method(channelID: channelID, method: answer))
  }

  func writeBatch(_ frames: [Frame]) async throws {
    batches.append(frames)
    let seqNo = UInt64(batches.count)
    let confirm: AMQPMethod =
      nackedSeqNos.contains(seqNo)
      ? .basicNack(BasicNack(deliveryTag: seqNo, multiple: false, requeue: false))
      : .basicAck(BasicAck(deliveryTag: seqNo))
    await channel?.handleFrame(.method(channelID: channelID, method: confirm))
  }

  func flush() async {}

  func channelClosed(_ channelID: UInt16) async {}
}

private let channelID: UInt16 = 1

private func reply(to request: AMQPMethod) -> AMQPMethod? {
  switch request {
  case .channelOpen: .channelOpenOk(ChannelOpenOk())
  case .confirmSelect: .confirmSelectOk
  case .channelClose: .channelCloseOk
  default: nil
  }
}

/// The outcome of one task, readable without awaiting the task, so a publish
/// the code under test parks forever shows up as "no outcome within the
/// deadline" rather than hanging the suite.
private final class Outcome: Sendable {
  private let value = NIOLockedValueBox<Result<Void, any Error>?>(nil)

  var result: Result<Void, any Error>? { value.withLockedValue { $0 } }

  func record(_ body: @escaping @Sendable () async throws -> Void) {
    Task {
      do {
        try await body()
        value.withLockedValue { $0 = .success(()) }
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

private func openTrackingChannel(
  on stub: EagerAckStubConnection, outstandingLimit: Int = 0
) async throws -> Channel {
  let channel = Channel(connection: stub, channelID: channelID)
  await stub.attach(channel)
  try await channel.open()
  try await channel.confirmSelect(tracking: true, outstandingLimit: outstandingLimit)
  return channel
}

@Suite("Channel confirm reply ordering")
struct ChannelConfirmReplyOrderTests {

  @Test("An ack that lands before the publisher parks its waiter still completes the publish")
  func earlyAckCompletesThePublish() async throws {
    let stub = EagerAckStubConnection()
    let channel = try await openTrackingChannel(on: stub)

    let outcome = Outcome()
    outcome.record { try await channel.basicPublish(body: Data("early".utf8), routingKey: "k") }

    let completed = await eventually { outcome.result != nil }
    #expect(
      completed,
      "basicPublish never completed: the ack that arrived before awaitConfirmation installed its waiter was dropped"
    )
    guard completed else { return }
    try outcome.result?.get()
  }

  @Test("An early ack completes the buffered publish variant too")
  func earlyAckCompletesTheBufferedPublish() async throws {
    let stub = EagerAckStubConnection()
    let channel = try await openTrackingChannel(on: stub)

    let outcome = Outcome()
    outcome.record { try await channel.publish(body: Data("early".utf8), routingKey: "k") }

    let completed = await eventually { outcome.result != nil }
    #expect(completed, "publish never completed after an early ack")
    guard completed else { return }
    try outcome.result?.get()
  }

  @Test("An early nack is reported to the publisher, not lost")
  func earlyNackIsReported() async throws {
    let stub = EagerAckStubConnection()
    let channel = try await openTrackingChannel(on: stub)
    await stub.nack(seqNo: 1)

    let outcome = Outcome()
    outcome.record { try await channel.basicPublish(body: Data("early".utf8), routingKey: "k") }

    let completed = await eventually { outcome.result != nil }
    #expect(completed, "basicPublish never completed after an early nack")
    guard completed, case .failure(let error)? = outcome.result else {
      Issue.record("expected publisherNack, got \(String(describing: outcome.result))")
      return
    }
    guard case ConnectionError.publisherNack(let seqNo)? = error as? ConnectionError else {
      Issue.record("expected publisherNack, got \(error)")
      return
    }
    #expect(seqNo == 1)
  }

  @Test("An early ack frees its outstanding-confirms slot")
  func earlyAckFreesTheOutstandingSlot() async throws {
    let stub = EagerAckStubConnection()
    let channel = try await openTrackingChannel(on: stub, outstandingLimit: 1)

    let first = Outcome()
    first.record { try await channel.basicPublish(body: Data("one".utf8), routingKey: "k") }
    let firstDone = await eventually { first.result != nil }
    #expect(firstDone, "first publish never completed")
    guard firstDone else { return }
    try first.result?.get()

    // With a limit of one, the second publish can only proceed if the first
    // ack was counted; a dropped ack leaves the slot taken forever.
    let second = Outcome()
    second.record { try await channel.basicPublish(body: Data("two".utf8), routingKey: "k") }
    let secondDone = await eventually { second.result != nil }
    #expect(secondDone, "second publish never completed: the early ack did not free its slot")
    guard secondDone else { return }
    try second.result?.get()
    #expect(await stub.batches.count == 2)
  }
}
