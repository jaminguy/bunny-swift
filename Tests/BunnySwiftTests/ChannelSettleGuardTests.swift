// This source code is dual-licensed under the Apache License, version 2.0,
// and the MIT license.
//
// SPDX-License-Identifier: Apache-2.0 OR MIT
//
// Copyright (c) 2025-2026 Michael S. Klishin

import AMQPProtocol
import Foundation
import Testing

@testable import BunnySwift

private let channelID: UInt16 = 1

private func consumingChannel() async throws -> (
  RecordingChannelConnection, Channel, MessageStream.AsyncIterator, String
) {
  let stub = RecordingChannelConnection()
  let channel = Channel(connection: stub, channelID: channelID)
  await stub.attach(channel)
  try await channel.open()
  let stream = try await channel.basicConsume(queue: "q")
  return (stub, channel, stream.makeAsyncIterator(), stream.consumerTag)
}

@Suite("Channel settle guard")
struct ChannelSettleGuardTests {

  @Test("An ack after the broker closed the channel throws instead of going out")
  func ackAfterBrokerChannelCloseThrowsLocally() async throws {
    var (stub, channel, iterator, consumerTag) = try await consumingChannel()
    await deliver(1, consumerTag: consumerTag, to: channel, channelID: channelID)
    let message = try #require(await iterator.next())

    await channel.handleFrame(
      .method(
        channelID: channelID,
        method: .channelClose(ChannelClose(replyCode: 404, replyText: "NOT_FOUND"))))

    // A method on a channel number the broker has released earns a
    // connection-level 504, which takes every other channel down with it.
    await #expect(throws: ConnectionError.self) { try await message.ack() }
    await #expect(throws: ConnectionError.self) { try await message.nack() }
    await #expect(throws: ConnectionError.self) { try await message.reject() }
    await #expect(throws: ConnectionError.self) { try await channel.basicAck(deliveryTag: 1) }

    let settles = await stub.sentMethods.filter {
      switch $0 {
      case .basicAck, .basicNack, .basicReject: true
      default: false
      }
    }
    #expect(settles.isEmpty, "settled on a closed channel: \(settles)")
  }

  @Test("An ack for a delivery from before recovery is refused; a fresh one goes out")
  func ackFromBeforeRecoveryIsRefused() async throws {
    var (stub, channel, iterator, consumerTag) = try await consumingChannel()
    await deliver(1, consumerTag: consumerTag, to: channel, channelID: channelID)
    let stale = try #require(await iterator.next())

    await channel.handleConnectionLost()
    try await channel.recoverOnNewConnection()

    // The recovered broker channel numbers its deliveries from 1 again, so
    // the stale tag would name this fresh delivery.
    await deliver(1, consumerTag: consumerTag, to: channel, channelID: channelID)
    let fresh = try #require(await iterator.next())

    await #expect(throws: ConnectionError.self) { try await stale.ack() }
    #expect(await stub.sentAcks.isEmpty, "the stale ack went out on the recovered channel")

    try await fresh.ack()
    #expect(await stub.sentAcks == [1])
  }
}
