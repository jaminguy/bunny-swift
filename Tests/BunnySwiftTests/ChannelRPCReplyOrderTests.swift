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

/// Stands in for `Connection` with a broker whose reply reaches the channel
/// before the write call returns to the caller. A real broker produces this
/// ordering whenever its reply is dispatched onto the channel actor ahead of
/// the sender's own resumption; the stub makes it happen every time.
private actor EagerStubConnection: ChannelConnection {
  nonisolated let topologyRegistry = TopologyRegistry()
  var frameMax: UInt32 { FrameDefaults.maxSize }

  private var channel: Channel?
  private(set) var answered: [AMQPMethod] = []
  var failSends = false

  func attach(_ channel: Channel) {
    self.channel = channel
  }

  func setFailSends(_ value: Bool) {
    failSends = value
  }

  func send(_ frame: Frame) async throws {
    if failSends { throw ConnectionError.notConnected }
    guard case .method(let channelID, let method) = frame, let answer = reply(to: method) else {
      return
    }
    answered.append(method)
    await channel?.handleFrame(.method(channelID: channelID, method: answer))
  }

  func writeBatch(_ frames: [Frame]) async throws {}

  func flush() async {}

  func channelClosed(_ channelID: UInt16) async {}
}

/// The broker's answer to a channel RPC.
private func reply(to request: AMQPMethod) -> AMQPMethod? {
  switch request {
  case .channelOpen: .channelOpenOk(ChannelOpenOk())
  case .confirmSelect: .confirmSelectOk
  case .channelClose: .channelCloseOk
  case .queueDeclare(let declare):
    .queueDeclareOk(QueueDeclareOk(queue: declare.queue, messageCount: 0, consumerCount: 0))
  default: nil
  }
}

/// The outcome of one task, readable without awaiting the task, so a call the
/// code under test parks forever shows up as "no outcome within the deadline"
/// rather than hanging the suite.
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

@Suite("Channel RPC reply ordering")
struct ChannelRPCReplyOrderTests {

  @Test("A reply that lands before the caller parks its continuation still completes the call")
  func replyBeforeParkCompletesTheCall() async throws {
    let stub = EagerStubConnection()
    let channel = Channel(connection: stub, channelID: 1)
    await stub.attach(channel)

    let outcome = Outcome()
    outcome.record { try await channel.open() }

    let completed = await eventually { outcome.result != nil }
    #expect(
      completed,
      "open() never completed: the Channel.OpenOk that arrived before waitForResponse() parked was dropped"
    )
    guard completed else { return }
    try outcome.result?.get()
    #expect(await channel.open)
    #expect(await channel.awaitingResponses == 0)
  }

  @Test("Consecutive RPCs each receive their own early reply")
  func consecutiveRPCsEachReceiveTheirOwnEarlyReply() async throws {
    let stub = EagerStubConnection()
    let channel = Channel(connection: stub, channelID: 1)
    await stub.attach(channel)

    let outcome = Outcome()
    outcome.record {
      try await channel.open()
      try await channel.confirmSelect(tracking: true)
      _ = try await channel.queue("q.early", durable: false)
    }

    let completed = await eventually { outcome.result != nil }
    #expect(completed, "a chain of RPCs did not complete when every reply arrived early")
    guard completed else { return }
    try outcome.result?.get()
    #expect(await stub.answered.count == 3)
    #expect(await channel.awaitingResponses == 0)
  }

  @Test("A failed send leaves no response slot behind")
  func failedSendLeavesNoSlotBehind() async throws {
    let stub = EagerStubConnection()
    let channel = Channel(connection: stub, channelID: 1)
    await stub.attach(channel)
    await stub.setFailSends(true)

    await #expect(throws: ConnectionError.self) {
      try await channel.open()
    }
    #expect(await channel.awaitingResponses == 0)
    #expect(await !channel.open)

    // The channel is usable once sends work again: the next reply must land
    // on the next request, not on a slot the failed send left behind.
    await stub.setFailSends(false)
    let outcome = Outcome()
    outcome.record { try await channel.open() }
    let completed = await eventually { outcome.result != nil }
    #expect(completed, "open() after a failed send never completed")
    guard completed else { return }
    try outcome.result?.get()
    #expect(await channel.open)
  }
}
