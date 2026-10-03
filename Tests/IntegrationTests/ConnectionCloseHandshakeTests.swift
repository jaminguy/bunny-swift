// This source code is dual-licensed under the Apache License, version 2.0,
// and the MIT license.
//
// SPDX-License-Identifier: Apache-2.0 OR MIT
//
// Copyright (c) 2025-2026 Michael S. Klishin

import Foundation
import Testing

@testable import Transport

@Suite("Connection close handshake", .disabled(if: TestConfig.skipIntegrationTests))
struct ConnectionCloseHandshakeTests {

  @Test("A client close writes connection.close and waits for the broker's close-ok")
  func clientCloseCompletesTheCloseHandshake() async throws {
    let transport = AMQPTransport()
    _ = try await transport.connect(configuration: TestConfig.connectionConfiguration())
    await transport.setFrameHandler({ _ in }, onDisconnect: {})

    let started = ContinuousClock.now
    await transport.close()
    let elapsed = ContinuousClock.now - started

    // Without close-ok the broker logs the socket close as an abrupt one.
    #expect(await transport.receivedCloseOk, "the broker never answered connection.close")
    #expect(elapsed < .seconds(1), "close waited out its bound: \(elapsed)")
    #expect(await !transport.connected)
  }
}
