// This source code is dual-licensed under the Apache License, version 2.0,
// and the MIT license.
//
// SPDX-License-Identifier: Apache-2.0 OR MIT
//
// Copyright (c) 2025-2026 Michael S. Klishin

import AMQPProtocol
import Foundation
import Recovery

@testable import BunnySwift

/// Stands in for `Connection`: answers the channel RPCs a consumer needs and
/// records every method the channel sends, so a test can assert what would
/// have reached the broker.
actor RecordingChannelConnection: ChannelConnection {
  nonisolated let topologyRegistry = TopologyRegistry()
  var frameMax: UInt32 { FrameDefaults.maxSize }

  private var channel: Channel?
  private(set) var sentMethods: [AMQPMethod] = []

  func attach(_ channel: Channel) {
    self.channel = channel
  }

  func send(_ frame: Frame) async throws {
    guard case .method(let channelID, let method) = frame else { return }
    sentMethods.append(method)
    switch method {
    case .channelOpen:
      await channel?.handleFrame(
        .method(channelID: channelID, method: .channelOpenOk(ChannelOpenOk())))
    case .basicConsume(let consume):
      let tag = consume.consumerTag.isEmpty ? "ctag-1" : consume.consumerTag
      await channel?.handleFrame(
        .method(channelID: channelID, method: .basicConsumeOk(BasicConsumeOk(consumerTag: tag))))
    case .channelClose:
      await channel?.handleFrame(.method(channelID: channelID, method: .channelCloseOk))
    default:
      break
    }
  }

  func writeBatch(_ frames: [Frame]) async throws {}

  func flush() async {}

  func channelClosed(_ channelID: UInt16) async {}

  var sentAcks: [UInt64] {
    sentMethods.compactMap {
      if case .basicAck(let ack) = $0 { ack.deliveryTag } else { nil }
    }
  }
}

/// Feeds the channel a complete, empty-bodied `basic.deliver`.
func deliver(_ tag: UInt64, consumerTag: String, to channel: Channel, channelID: UInt16) async {
  let deliver = BasicDeliver(
    consumerTag: consumerTag, deliveryTag: tag, redelivered: false, exchange: "",
    routingKey: "q")
  await channel.handleFrame(.method(channelID: channelID, method: .basicDeliver(deliver)))
  await channel.handleFrame(
    .header(channelID: channelID, classID: 60, bodySize: 0, properties: BasicProperties()))
}
