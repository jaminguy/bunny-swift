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

/// Stands in for `Connection` across a recovery: answers the channel's RPCs,
/// records publish frames, and lets a test run something at the moment the
/// channel re-selects confirm mode during `recoverOnNewConnection` — after
/// `open()` has marked the channel open and before the sequence is reset.
private actor RecoveryStubConnection: ChannelConnection {
  nonisolated let topologyRegistry = TopologyRegistry()
  var frameMax: UInt32 { FrameDefaults.maxSize }

  private var channel: Channel?
  private(set) var batches: [[Frame]] = []
  private var beforeConfirmSelectOk: (@Sendable () async -> Void)?

  func attach(_ channel: Channel) {
    self.channel = channel
  }

  /// Runs once, when the channel next sends `confirm.select`, before the
  /// stub answers it.
  func onNextConfirmSelect(_ hook: @escaping @Sendable () async -> Void) {
    beforeConfirmSelectOk = hook
  }

  func send(_ frame: Frame) async throws {
    guard case .method(let channelID, let method) = frame else { return }
    switch method {
    case .channelOpen:
      await channel?.handleFrame(.method(channelID: channelID, method: .channelOpenOk(ChannelOpenOk())))
    case .confirmSelect:
      if let hook = beforeConfirmSelectOk {
        beforeConfirmSelectOk = nil
        await hook()
      }
      await channel?.handleFrame(.method(channelID: channelID, method: .confirmSelectOk))
    case .channelClose:
      await channel?.handleFrame(.method(channelID: channelID, method: .channelCloseOk))
    default:
      break
    }
  }

  func writeBatch(_ frames: [Frame]) async throws {
    batches.append(frames)
  }

  func flush() async {}

  func channelClosed(_ channelID: UInt16) async {}
}

private let channelID: UInt16 = 1

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

private func ack(_ tag: UInt64) -> Frame {
  .method(channelID: channelID, method: .basicAck(BasicAck(deliveryTag: tag)))
}

@Suite("Channel recovery window")
struct ChannelRecoveryWindowTests {

  @Test("A publish admitted while the channel recovers goes out on the fresh sequence and settles")
  func publishDuringRecoveryUsesTheFreshSequence() async throws {
    let stub = RecoveryStubConnection()
    let channel = Channel(connection: stub, channelID: channelID)
    await stub.attach(channel)
    try await channel.open()
    try await channel.confirmSelect(tracking: true)

    // One publish before the drop, so the old sequence is past 1.
    let first = Outcome()
    first.record { try await channel.basicPublish(body: Data("one".utf8), routingKey: "k") }
    #expect(await eventually { await stub.batches.count == 1 })
    await channel.handleFrame(ack(1))
    #expect(await eventually { first.result != nil })
    try first.result?.get()
    #expect(await channel.publishSeqNo == 2)

    // The link drops.
    await channel.handleConnectionLost()

    // Recovery re-opens the channel. Inside the window between `open()` and
    // the confirm re-select, a publisher that never stopped publishing tries
    // again. The nudge only gives it time to run; the assertions below do
    // not depend on where it lands.
    let window = Outcome()
    await stub.onNextConfirmSelect {
      window.record { try await channel.basicPublish(body: Data("two".utf8), routingKey: "k") }
      try? await Task.sleep(for: .milliseconds(50))
    }
    try await channel.recoverOnNewConnection()

    // The recovered channel is a fresh sequence on the broker's side: the
    // publish must be its first delivery, so the ack for tag 1 settles it.
    #expect(await eventually { await stub.batches.count == 2 }, "the publish never went out")
    await channel.handleFrame(ack(1))
    let settled = await eventually { window.result != nil }
    #expect(
      settled,
      "the publish admitted during recovery never settled: it was numbered from the old sequence and its slot is not the one the broker's ack names"
    )
    guard settled else { return }
    try window.result?.get()
    #expect(await channel.publishSeqNo == 2)
  }

  @Test("A slot left on the interim channel is failed when recovery starts over")
  func slotLeftFromAnInterruptedRecoveryIsFailed() async throws {
    let stub = RecoveryStubConnection()
    let channel = Channel(connection: stub, channelID: channelID)
    await stub.attach(channel)
    try await channel.open()
    try await channel.confirmSelect(tracking: true)

    // A publish whose frames went out but whose ack never comes: the link
    // dropped again and nothing told the channel.
    let stranded = Outcome()
    stranded.record { try await channel.basicPublish(body: Data("lost".utf8), routingKey: "k") }
    #expect(await eventually { await stub.batches.count == 1 })

    // Recovery runs again on a new connection; the stranded publish must be
    // failed, not left parked on a slot no ack will ever name.
    try await channel.recoverOnNewConnection()
    let settled = await eventually { stranded.result != nil }
    #expect(settled, "a publish stranded across a restarted recovery never settled")
    guard settled, case .failure(let error)? = stranded.result else {
      Issue.record("expected the stranded publish to fail, got \(String(describing: stranded.result))")
      return
    }
    #expect(error is ConnectionError)
  }
}
