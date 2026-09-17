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

/// Probes, against a real broker, whether an RPC reply can reach the channel
/// before the caller parks its continuation: many channel RPCs in flight on
/// one connection, each bounded by a deadline the test observes from outside
/// so a parked call shows up as a count rather than a hung suite.
///
/// The second test skews task priorities the way a host process might: the
/// connection (and so its frame-dispatch loop) is opened from a high-priority
/// task while the RPC callers run at background priority, which is the
/// ordering under which a reply's job can be scheduled ahead of the caller's
/// resumption on the same actor.

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

/// One unit of work: four RPCs on a fresh channel.
private func channelRoundTrip(on connection: Connection) async throws {
  let channel = try await connection.openChannel()
  let queue = try await channel.temporaryQueue()
  _ = try await queue.delete()
  try await channel.close()
}

/// Runs `rounds` batches of `width` concurrent round trips and returns how
/// many calls never settled within `deadline` seconds of their batch start.
private func runBatches(
  on connection: Connection, rounds: Int, width: Int, deadline: Double,
  callerPriority: TaskPriority? = nil
) async throws -> Int {
  for _ in 0..<rounds {
    let outcomes = (0..<width).map { _ in Outcome() }
    for outcome in outcomes {
      outcome.record(priority: callerPriority) { try await channelRoundTrip(on: connection) }
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

@Suite("Channel RPC ordering under load", .disabled(if: TestConfig.skipIntegrationTests), .serialized)
struct ChannelRPCStressTests {

  @Test("Concurrent channel RPCs on one connection all settle", .timeLimit(.minutes(3)))
  func concurrentRPCsSettle() async throws {
    let connection = try await TestConfig.openConnection()
    let parked = try await runBatches(on: connection, rounds: 40, width: 64, deadline: 30)
    #expect(parked == 0, "\(parked) channel RPC calls never settled")
    if parked == 0 { try await connection.close() }
  }

  @Test(
    "Concurrent channel RPCs settle when callers run below the dispatcher's priority",
    .timeLimit(.minutes(3)))
  func concurrentRPCsSettleAcrossPriorities() async throws {
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
    let parked = try await runBatches(
      on: connection, rounds: 40, width: 64, deadline: 30, callerPriority: .background)
    #expect(parked == 0, "\(parked) channel RPC calls never settled with skewed priorities")
    if parked == 0 { try await connection.close() }
  }
}
