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

@Suite("Broker-initiated consumer cancellation")
struct ChannelServerCancelTests {

  @Test("A basic.cancel from the broker ends the consumer and is not answered")
  func brokerCancelIsNotAnswered() async throws {
    let channelID: UInt16 = 1
    let stub = RecordingChannelConnection()
    let channel = Channel(connection: stub, channelID: channelID)
    await stub.attach(channel)
    try await channel.open()
    let stream = try await channel.basicConsume(queue: "q")

    await channel.handleFrame(
      .method(
        channelID: channelID,
        method: .basicCancel(BasicCancel(consumerTag: stream.consumerTag, noWait: true))))

    // RabbitMQ has already dropped the tag; a cancel-ok for it crashes the
    // broker's channel writer and closes the whole connection with 541.
    let replied = await stub.sentMethods.contains {
      if case .basicCancelOk = $0 { true } else { false }
    }
    #expect(!replied, "the client answered the broker's no-wait basic.cancel")

    var iterator = stream.makeAsyncIterator()
    let next = await iterator.next()
    #expect(next == nil, "the consumer stream did not finish")
  }
}
