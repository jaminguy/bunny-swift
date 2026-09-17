// This source code is dual-licensed under the Apache License, version 2.0,
// and the MIT license.
//
// SPDX-License-Identifier: Apache-2.0 OR MIT
//
// Copyright (c) 2025-2026 Michael S. Klishin

import Foundation
import NIOConcurrencyHelpers
import Testing

@testable import BunnySwift

/// Probes, against a real broker, whether a publisher confirm can reach the
/// channel before the publisher has installed its waiter: many confirm-mode
/// publishes in flight on one channel, each bounded by a deadline the test
/// observes from outside so a parked publish shows up as a count rather than
/// a hung suite.
///
/// The second test skews task priorities the way a host process might: the
/// connection (and so its frame-dispatch loop) is opened from a high-priority
/// task while the publishers run at background priority, which is the
/// ordering under which an ack's job can be scheduled ahead of the
/// publisher's resumption from its flush hop.

private final class Outcome: Sendable {
  private let value = NIOLockedValueBox<Result<Void, any Error>?>(nil)

  var result: Result<Void, any Error>? { value.withLockedValue { $0 } }

  func record(priority: TaskPriority? = nil, _ body: @escaping @Sendable () async throws -> Void) {
    Task(priority: priority) {
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
  within seconds: Double, _ condition: @escaping @Sendable () async -> Bool
) async -> Bool {
  let deadline = ContinuousClock.now + .seconds(seconds)
  while ContinuousClock.now < deadline {
    if await condition() { return true }
    try? await Task.sleep(for: .milliseconds(10))
  }
  return await condition()
}

/// A routing key nothing is bound to on the default exchange: the broker drops
/// the message and still confirms it, so the probe leaves nothing behind.
private let unroutedKey = "bunnyswift.confirm.stress.unrouted"

/// Runs `rounds` batches of `width` concurrent confirm-mode publishes on one
/// channel and returns how many never settled within `deadline` seconds of
/// their batch start.
private func runBatches(
  on channel: Channel, rounds: Int, width: Int, deadline: Double,
  publisherPriority: TaskPriority? = nil
) async throws -> Int {
  for round in 0..<rounds {
    let outcomes = (0..<width).map { _ in Outcome() }
    for (index, outcome) in outcomes.enumerated() {
      outcome.record(priority: publisherPriority) {
        try await channel.basicPublish(
          body: Data("round \(round) publish \(index)".utf8), routingKey: unroutedKey)
      }
    }
    let settled = await eventually(within: deadline) {
      outcomes.allSatisfy { $0.result != nil }
    }
    if !settled {
      return outcomes.filter { $0.result == nil }.count
    }
    for outcome in outcomes {
      try outcome.result?.get()
    }
  }
  return 0
}

/// Like `runBatches`, but every publish in a batch goes out on its own
/// confirm-mode channel, so the per-channel publish gate serializes nothing
/// and the connection and transport actors carry `width` writes at once, the
/// shape of a worker process whose consumer channels ack on the same
/// connection while the publish channel confirms.
private func runMultiChannelBatches(
  on channels: [Channel], rounds: Int, deadline: Double,
  publisherPriority: TaskPriority? = nil
) async throws -> Int {
  for round in 0..<rounds {
    let outcomes = channels.map { _ in Outcome() }
    for (channel, outcome) in zip(channels, outcomes) {
      outcome.record(priority: publisherPriority) {
        try await channel.basicPublish(
          body: Data("round \(round)".utf8), routingKey: unroutedKey)
      }
    }
    let settled = await eventually(within: deadline) {
      outcomes.allSatisfy { $0.result != nil }
    }
    if !settled {
      return outcomes.filter { $0.result == nil }.count
    }
    for outcome in outcomes {
      try outcome.result?.get()
    }
  }
  return 0
}

private func openTrackingChannels(on connection: Connection, count: Int) async throws -> [Channel] {
  var channels: [Channel] = []
  for _ in 0..<count {
    let channel = try await connection.openChannel()
    try await channel.confirmSelect(tracking: true)
    channels.append(channel)
  }
  return channels
}

@Suite("Channel confirm ordering under load", .disabled(if: TestConfig.skipIntegrationTests), .serialized)
struct ChannelConfirmStressTests {

  @Test("Concurrent confirm-mode publishes on many channels all settle", .timeLimit(.minutes(3)))
  func concurrentPublishesOnManyChannelsSettle() async throws {
    let connection = try await TestConfig.openConnection()
    let channels = try await openTrackingChannels(on: connection, count: 64)
    let parked = try await runMultiChannelBatches(on: channels, rounds: 40, deadline: 30)
    #expect(parked == 0, "\(parked) confirm-mode publishes on separate channels never settled")
    if parked == 0 { try await connection.close() }
  }

  @Test(
    "Concurrent confirm-mode publishes on many channels settle when publishers run below the dispatcher's priority",
    .timeLimit(.minutes(3)))
  func concurrentPublishesOnManyChannelsSettleAcrossPriorities() async throws {
    let opened = NIOLockedValueBox<Connection?>(nil)
    let openTask = Task(priority: .high) {
      let connection = try await TestConfig.openConnection()
      opened.withLockedValue { $0 = connection }
    }
    try await openTask.value
    guard let connection = opened.withLockedValue({ $0 }) else {
      Issue.record("connection did not open")
      return
    }
    let channels = try await openTrackingChannels(on: connection, count: 64)
    let parked = try await runMultiChannelBatches(
      on: channels, rounds: 40, deadline: 30, publisherPriority: .background)
    #expect(
      parked == 0,
      "\(parked) confirm-mode publishes on separate channels never settled with skewed priorities")
    if parked == 0 { try await connection.close() }
  }

  @Test("Concurrent confirm-mode publishes on one channel all settle", .timeLimit(.minutes(3)))
  func concurrentPublishesSettle() async throws {
    let connection = try await TestConfig.openConnection()
    let channel = try await connection.openChannel()
    try await channel.confirmSelect(tracking: true)
    let parked = try await runBatches(on: channel, rounds: 40, width: 64, deadline: 30)
    #expect(parked == 0, "\(parked) confirm-mode publishes never settled")
    if parked == 0 { try await connection.close() }
  }

  @Test(
    "Concurrent confirm-mode publishes settle when publishers run below the dispatcher's priority",
    .timeLimit(.minutes(3)))
  func concurrentPublishesSettleAcrossPriorities() async throws {
    let opened = NIOLockedValueBox<Connection?>(nil)
    let openTask = Task(priority: .high) {
      let connection = try await TestConfig.openConnection()
      opened.withLockedValue { $0 = connection }
    }
    try await openTask.value
    guard let connection = opened.withLockedValue({ $0 }) else {
      Issue.record("connection did not open")
      return
    }
    let channel = try await connection.openChannel()
    try await channel.confirmSelect(tracking: true)
    let parked = try await runBatches(
      on: channel, rounds: 40, width: 64, deadline: 30, publisherPriority: .background)
    #expect(parked == 0, "\(parked) confirm-mode publishes never settled with skewed priorities")
    if parked == 0 { try await connection.close() }
  }
}
