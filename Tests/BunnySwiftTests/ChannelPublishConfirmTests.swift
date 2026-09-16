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

/// Stands in for `Connection`: records what a channel writes and can hold every
/// write open until released, so a test can put two publishes in flight on one
/// channel at the same time.
private actor StubConnection: ChannelConnection {
  nonisolated let topologyRegistry = TopologyRegistry()
  var frameMax: UInt32 { FrameDefaults.maxSize }

  private(set) var sent: [Frame] = []
  private(set) var batches: [[Frame]] = []
  private(set) var closedChannels: [UInt16] = []

  private var holding = false
  private var heldWriters: [CheckedContinuation<Void, Never>] = []

  /// Every `writeBatch` call from now on suspends until `releaseWrites()`.
  func holdWrites() {
    holding = true
  }

  func releaseWrites() {
    holding = false
    let writers = heldWriters
    heldWriters.removeAll()
    for writer in writers {
      writer.resume()
    }
  }

  func send(_ frame: Frame) async throws {
    sent.append(frame)
  }

  func writeBatch(_ frames: [Frame]) async throws {
    batches.append(frames)
    guard holding else { return }
    await withCheckedContinuation { cont in
      heldWriters.append(cont)
    }
  }

  func flush() async {}

  func channelClosed(_ channelID: UInt16) async {
    closedChannels.append(channelID)
  }

  /// The body carried by each written batch, in write order.
  var writtenBodies: [Data] {
    batches.map { batch in
      batch.reduce(into: Data()) { body, frame in
        if case .body(channelID: _, let payload) = frame { body.append(payload) }
      }
    }
  }

  /// The RPC methods the channel has sent, in order.
  var sentMethods: [AMQPMethod] {
    sent.compactMap { frame in
      if case .method(channelID: _, let method) = frame { return method }
      return nil
    }
  }
}

/// The outcome of one task, readable from the test without awaiting the task,
/// so a task the code under test parks forever shows up as "no outcome within
/// the deadline" rather than hanging the suite.
private final class Outcome: Sendable {
  private let value = NIOLockedValueBox<Result<Void, any Error>?>(nil)

  var result: Result<Void, any Error>? { value.withLockedValue { $0 } }
  var succeeded: Bool { (try? result?.get()) != nil }

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

private let channelID: UInt16 = 1

/// Polls `condition` until it holds or `seconds` elapse.
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

/// The broker's answer to a channel RPC.
private func reply(to request: AMQPMethod) -> AMQPMethod? {
  switch request {
  case .channelOpen: .channelOpenOk(ChannelOpenOk())
  case .confirmSelect: .confirmSelectOk
  case .channelClose: .channelCloseOk
  default: nil
  }
}

/// Runs a channel operation and answers each RPC it sends. The channel parks
/// its response continuation only after `send` returns and drops a reply that
/// lands earlier, so a request is answered only once the channel is listening.
private func perform(
  _ operation: @escaping @Sendable () async throws -> Void,
  on channel: Channel,
  answeredBy stub: StubConnection
) async throws {
  let outcome = Outcome()
  outcome.record(operation)
  let answered = await eventually {
    if outcome.result != nil { return true }
    if await channel.awaitingResponses > 0,
      let request = await stub.sentMethods.last, let answer = reply(to: request)
    {
      await channel.handleFrame(.method(channelID: channelID, method: answer))
    }
    return outcome.result != nil
  }
  #expect(answered, "operation did not complete")
  try outcome.result?.get()
}

private func openConfirmChannel(on stub: StubConnection) async throws -> Channel {
  let channel = Channel(connection: stub, channelID: channelID)
  try await perform({ try await channel.open() }, on: channel, answeredBy: stub)
  try await perform(
    { try await channel.confirmSelect(tracking: true) }, on: channel, answeredBy: stub)
  return channel
}

private func ack(_ tag: UInt64) -> Frame {
  .method(channelID: channelID, method: .basicAck(BasicAck(deliveryTag: tag)))
}

private func publish(_ body: String, on channel: Channel) -> Outcome {
  let outcome = Outcome()
  outcome.record { try await channel.basicPublish(body: Data(body.utf8), routingKey: "k") }
  return outcome
}

private func connectionError(in result: Result<Void, any Error>?) -> ConnectionError? {
  guard case .failure(let error)? = result else { return nil }
  return error as? ConnectionError
}

private func isChannelClosed(_ result: Result<Void, any Error>?, replyCode: UInt16) -> Bool {
  guard case .channelClosed(let code, _, _, _)? = connectionError(in: result) else { return false }
  return code == replyCode
}

@Suite("Channel publisher confirms")
struct ChannelPublishConfirmTests {
  @Test("Two publishes in flight on one channel are numbered and written one at a time, and both resume")
  func concurrentPublishesAreNumberedAndWrittenInOrder() async throws {
    let stub = StubConnection()
    let channel = try await openConfirmChannel(on: stub)
    await stub.holdWrites()

    let first = publish("a", on: channel)
    #expect(await eventually { await stub.batches.count == 1 })

    // The second publish waits for the first to finish writing before it reads
    // the sequence number, so the broker's tags and the channel's numbers agree.
    let second = publish("b", on: channel)
    try await Task.sleep(for: .milliseconds(50))
    #expect(await stub.batches.count == 1)

    await stub.releaseWrites()
    #expect(await eventually { await stub.batches.count == 2 })
    #expect(await stub.writtenBodies == [Data("a".utf8), Data("b".utf8)])
    #expect(await channel.publishSeqNo == 3)

    // The broker tags them 1 and 2 in write order and confirms each on its own.
    await channel.handleFrame(ack(1))
    await channel.handleFrame(ack(2))

    #expect(await eventually { first.result != nil && second.result != nil })
    #expect(first.succeeded)
    #expect(second.succeeded)
  }

  @Test("A nack fails the publisher with publisherNack")
  func nackFailsThePublisher() async throws {
    let stub = StubConnection()
    let channel = try await openConfirmChannel(on: stub)

    let publish = publish("a", on: channel)
    #expect(await eventually { await stub.batches.count == 1 })
    await channel.handleFrame(
      .method(channelID: channelID, method: .basicNack(BasicNack(deliveryTag: 1))))

    #expect(await eventually { publish.result != nil })
    guard case .publisherNack(let seqNo)? = connectionError(in: publish.result) else {
      Issue.record("expected publisherNack, got \(String(describing: publish.result))")
      return
    }
    #expect(seqNo == 1)
  }

  @Test("close() fails a parked publisher with channelClosed instead of leaving it suspended")
  func closeFailsParkedPublisher() async throws {
    let stub = StubConnection()
    let channel = try await openConfirmChannel(on: stub)

    let publish = publish("a", on: channel)
    #expect(await eventually { await stub.batches.count == 1 })
    #expect(publish.result == nil)

    try await perform({ try await channel.close() }, on: channel, answeredBy: stub)

    #expect(await eventually { publish.result != nil })
    #expect(isChannelClosed(publish.result, replyCode: 200))
  }

  @Test("close() during a write refuses that publish and the one queued behind it instead of parking them")
  func closeDuringAWriteRefusesInFlightPublishes() async throws {
    let stub = StubConnection()
    let channel = try await openConfirmChannel(on: stub)
    await stub.holdWrites()

    let writing = publish("a", on: channel)
    #expect(await eventually { await stub.batches.count == 1 })
    let queued = publish("b", on: channel)
    try await Task.sleep(for: .milliseconds(50))
    #expect(queued.result == nil)

    try await perform({ try await channel.close() }, on: channel, answeredBy: stub)
    await stub.releaseWrites()

    // Neither may install a confirm handler on the closed channel: the write
    // that finished is refused before it parks, the queued one at the gate.
    #expect(await eventually { writing.result != nil && queued.result != nil })
    #expect(connectionError(in: writing.result) != nil)
    #expect(connectionError(in: queued.result) != nil)
    #expect(await stub.batches.count == 1)
  }

  @Test("A broker-initiated Channel.Close fails a parked publisher with its reply code")
  func brokerCloseFailsParkedPublisher() async throws {
    let stub = StubConnection()
    let channel = try await openConfirmChannel(on: stub)

    let publish = publish("a", on: channel)
    #expect(await eventually { await stub.batches.count == 1 })

    let close = ChannelClose(replyCode: 404, replyText: "NOT_FOUND", classId: 60, methodId: 40)
    await channel.handleFrame(.method(channelID: channelID, method: .channelClose(close)))

    #expect(await eventually { publish.result != nil })
    #expect(isChannelClosed(publish.result, replyCode: 404))
    #expect(await stub.closedChannels == [channelID])
  }

  @Test("A repeated confirmSelect sends nothing and keeps the sequence where it was")
  func repeatedConfirmSelectIsANoOp() async throws {
    let stub = StubConnection()
    let channel = try await openConfirmChannel(on: stub)

    let first = publish("a", on: channel)
    #expect(await eventually { await stub.batches.count == 1 })
    await channel.handleFrame(ack(1))
    #expect(await eventually { first.result != nil })

    // Nothing answers this RPC; it must return on its own.
    let reselect = Outcome()
    reselect.record { try await channel.confirmSelect(tracking: true) }
    #expect(await eventually { reselect.result != nil })
    #expect(reselect.succeeded)
    let selects = await stub.sentMethods.filter { method in
      if case .confirmSelect = method { return true }
      return false
    }
    #expect(selects.count == 1)

    // The broker numbers the next delivery 2, and so does the channel.
    let second = publish("b", on: channel)
    #expect(await eventually { await stub.batches.count == 2 })
    #expect(await channel.publishSeqNo == 3)
    await channel.handleFrame(ack(2))
    #expect(await eventually { second.result != nil })
    #expect(second.succeeded)
  }
}
