// This source code is dual-licensed under the Apache License, version 2.0,
// and the MIT license.
//
// SPDX-License-Identifier: Apache-2.0 OR MIT
//
// Copyright (c) 2025-2026 Michael S. Klishin

import AMQPProtocol
import Foundation
import NIO
import NIOConcurrencyHelpers
import Testing

@testable import BunnySwift
@testable import Transport

/// Reports a channel's `channel.open` written without sending it, as a broker
/// that never answers does: the RPC waits on an open-ok that never comes.
private final class DropChannelOpenHandler: ChannelOutboundHandler, @unchecked Sendable {
  typealias OutboundIn = Frame
  typealias OutboundOut = Frame

  func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
    if case .method(let channelID, .channelOpen) = unwrapOutboundIn(data), channelID != 0 {
      promise?.succeed(())
      return
    }
    context.write(data, promise: promise)
  }
}

private func dropChannelOpens(on connection: Connection) async throws {
  let socket = try #require(await connection.transport.channel)
  try await socket.pipeline.addHandler(DropChannelOpenHandler()).get()
}

/// Runs `body` and records how it ended, so a test can poll for the outcome
/// without awaiting a call that may never return.
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

  /// The outcome once there is one, or nil if there is none within `bound`.
  func settled(within bound: Duration) async -> Result<Void, any Error>? {
    let deadline = ContinuousClock.now + bound
    while ContinuousClock.now < deadline {
      if let result { return result }
      try? await Task.sleep(for: .milliseconds(10))
    }
    return result
  }
}

/// Polls until `channel` has an RPC parked for a broker reply.
private func awaitParkedRPC(on channel: BunnySwift.Channel) async -> Bool {
  let deadline = ContinuousClock.now + .seconds(2)
  while ContinuousClock.now < deadline {
    if await channel.awaitingResponses > 0 { return true }
    try? await Task.sleep(for: .milliseconds(10))
  }
  return false
}

/// True if the stream ends within `bound`.
private func finishes(_ stream: MessageStream, within bound: Duration) async -> Bool {
  await withTaskGroup(of: Bool.self) { group in
    group.addTask {
      var iterator = stream.makeAsyncIterator()
      return await iterator.next() == nil
    }
    group.addTask {
      try? await Task.sleep(for: bound)
      return false
    }
    let first = await group.next() ?? false
    group.cancelAll()
    return first
  }
}

@Suite(
  "Connection close fails pending channel work",
  .disabled(if: TestConfig.skipIntegrationTests),
  .timeLimit(.minutes(1))
)
struct ConnectionClosePendingWorkTests {

  @Test("A client close fails a channel recovery's pending RPC and ends the channel's consumers")
  func closeFailsRecoveryRPCAndEndsConsumers() async throws {
    let connection = try await TestConfig.openConnection()
    let channel = try await connection.openChannel()
    let queue = try await channel.queue("", exclusive: true)
    let stream = try await queue.consume()

    // The state recovery leaves a channel in once the connection is open
    // again: the channel is not open, its consumers are kept for recovery, and
    // its re-open waits on the broker with the publish gate held.
    try await dropChannelOpens(on: connection)
    await channel.handleConnectionLost()
    let recovery = Outcome()
    recovery.record { try await channel.recoverOnNewConnection() }
    #expect(await awaitParkedRPC(on: channel), "the recovery's channel.open never went out")

    try await connection.close()

    #expect(await finishes(stream, within: .seconds(2)), "the consumer stream did not finish")
    // The recovery returning, by a throw, is also what releases the publish
    // gate it holds.
    let result = try #require(
      await recovery.settled(within: .seconds(2)),
      "the recovery RPC was still waiting after the connection closed")
    guard case .failure = result else {
      Issue.record("the recovery completed on a connection the client closed")
      return
    }
  }

  @Test("A client close fails a channel open still waiting on the broker")
  func closeFailsPendingChannelOpen() async throws {
    let connection = try await TestConfig.openConnection()
    try await dropChannelOpens(on: connection)

    let opening = Outcome()
    opening.record { _ = try await connection.openChannel() }
    // Nothing exposes the channel before `openChannel` returns, so wait for
    // its request to have reached the pipeline instead.
    try await Task.sleep(for: .milliseconds(200))
    #expect(opening.result == nil, "channel.open was answered despite being dropped")

    try await connection.close()

    let result = try #require(
      await opening.settled(within: .seconds(2)),
      "openChannel was still waiting after the connection closed")
    guard case .failure = result else {
      Issue.record("openChannel succeeded on a connection the client closed")
      return
    }
  }
}
