// This source code is dual-licensed under the Apache License, version 2.0,
// and the MIT license.
//
// SPDX-License-Identifier: Apache-2.0 OR MIT
//
// Copyright (c) 2025-2026 Michael S. Klishin

import Foundation
import Testing

@testable import BunnySwift

@Suite("Consumer cancel notification", .disabled(if: TestConfig.skipIntegrationTests))
struct ConsumerCancelNotificationTests {

  @Test("Deleting a consumed queue ends the consumer and leaves the connection open")
  func brokerCancelLeavesTheConnectionOpen() async throws {
    var config = TestConfig.connectionConfiguration()
    config.automaticRecovery = false
    let connection = try await Connection.open(config)
    defer { Task { try? await connection.close() } }

    let consumerChannel = try await connection.openChannel()
    let queue = try await consumerChannel.queue("", exclusive: true)
    let stream = try await queue.consume()

    let otherChannel = try await connection.openChannel()
    _ = try await otherChannel.queueDelete(queue.name)
    try await Task.sleep(for: .milliseconds(500))

    #expect(await connection.connected, "the broker closed the connection")
    let declared = try? await otherChannel.queue("", exclusive: true)
    #expect(declared != nil, "the connection no longer serves other channels")

    var iterator = stream.makeAsyncIterator()
    let next = await iterator.next()
    #expect(next == nil, "the consumer stream did not finish")
  }
}
