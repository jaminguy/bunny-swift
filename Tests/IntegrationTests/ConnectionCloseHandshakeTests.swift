// This source code is dual-licensed under the Apache License, version 2.0,
// and the MIT license.
//
// SPDX-License-Identifier: Apache-2.0 OR MIT
//
// Copyright (c) 2025-2026 Michael S. Klishin

import AMQPProtocol
import Foundation
import NIO
import Testing

@testable import Transport

/// Withholds the client's `connection.close` from the socket. `.stall` leaves
/// its write pending, as a socket whose peer has stopped reading does once the
/// send buffer is full; `.drop` reports it written, as a broker that never
/// answers does. A withheld write fails when the channel closes, as NIO fails
/// a socket's pending writes.
private final class WithheldCloseHandler: ChannelOutboundHandler, @unchecked Sendable {
  typealias OutboundIn = Frame
  typealias OutboundOut = Frame

  enum Mode { case stall, drop }

  private let mode: Mode
  private var stalled: [EventLoopPromise<Void>] = []

  init(_ mode: Mode) {
    self.mode = mode
  }

  func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
    guard case .method(channelID: 0, method: .connectionClose) = unwrapOutboundIn(data) else {
      context.write(data, promise: promise)
      return
    }
    switch mode {
    case .stall:
      if let promise { stalled.append(promise) }
    case .drop:
      promise?.succeed(())
    }
  }

  func close(context: ChannelHandlerContext, mode: CloseMode, promise: EventLoopPromise<Void>?) {
    failStalled()
    context.close(mode: mode, promise: promise)
  }

  func handlerRemoved(context: ChannelHandlerContext) {
    failStalled()
  }

  private func failStalled() {
    let pending = stalled
    stalled.removeAll()
    for promise in pending {
      promise.fail(ChannelError.ioOnClosedChannel)
    }
  }
}

private func connectWithheld(_ mode: WithheldCloseHandler.Mode) async throws -> AMQPTransport {
  let transport = AMQPTransport()
  _ = try await transport.connect(configuration: TestConfig.connectionConfiguration())
  await transport.setFrameHandler({ _ in }, onDisconnect: {})
  let channel = try #require(await transport.channel)
  try await channel.pipeline.addHandler(WithheldCloseHandler(mode)).get()
  return transport
}

/// Runs `close`, calling `release` after `delay` while it is parked, and
/// returns how long `close` took, or nil if it had not returned within 3 s. A
/// close still parked then is cut loose with `forceClose`, so the test ends.
private func timedClose(
  _ transport: AMQPTransport,
  releasingAfter delay: Duration? = nil,
  with release: @escaping @Sendable () async -> Void = {}
) async -> Duration? {
  let closer = Task { () -> Duration in
    let started = ContinuousClock.now
    await transport.close()
    return ContinuousClock.now - started
  }
  if let delay {
    try? await Task.sleep(for: delay)
    await release()
  }
  return await withTaskGroup(of: Duration?.self) { group in
    group.addTask { await closer.value }
    group.addTask {
      try? await Task.sleep(for: .seconds(3))
      return nil
    }
    let first = await group.next() ?? nil
    if first == nil {
      await transport.forceClose()
    }
    group.cancelAll()
    return first
  }
}

/// Accepts TCP connections and never writes to them, so a client handshake
/// against it waits on `connection.start` until the client gives up.
private func startSilentServer() async throws -> NIO.Channel {
  try await ServerBootstrap(group: SharedEventLoopGroup.shared)
    .serverChannelOption(.socketOption(.so_reuseaddr), value: 1)
    .bind(host: "127.0.0.1", port: 0)
    .get()
}

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

  @Test("A close whose connection.close write never completes still returns within its bound")
  func closeIsBoundedWhenTheWriteStalls() async throws {
    let transport = try await connectWithheld(.stall)

    let elapsed = await timedClose(transport)

    let took = try #require(elapsed, "close parked on a write that never completes")
    #expect(took < .milliseconds(1500), "close overran its 1 s bound: \(took)")
    #expect(await !transport.connected)
  }

  @Test("forceClose releases a close parked on the broker's reply")
  func forceCloseReleasesAParkedClose() async throws {
    let transport = try await connectWithheld(.drop)

    let elapsed = await timedClose(transport, releasingAfter: .milliseconds(200)) {
      await transport.forceClose()
    }

    let took = try #require(elapsed, "close never returned")
    #expect(took < .milliseconds(800), "close waited out its bound after forceClose: \(took)")
  }

  @Test("resetForRecovery releases a close parked on the broker's reply")
  func resetForRecoveryReleasesAParkedClose() async throws {
    let transport = try await connectWithheld(.drop)

    let elapsed = await timedClose(transport, releasingAfter: .milliseconds(200)) {
      await transport.resetForRecovery()
    }

    let took = try #require(elapsed, "close never returned")
    #expect(took < .milliseconds(800), "close waited out its bound after a reset: \(took)")
  }

  @Test("connect releases a close parked on the broker's reply")
  func connectReleasesAParkedClose() async throws {
    let transport = try await connectWithheld(.drop)
    let unreachable = {
      var config = TestConfig.connectionConfiguration()
      config.port = 1
      return config
    }()

    let elapsed = await timedClose(transport, releasingAfter: .milliseconds(200)) {
      _ = try? await transport.connect(configuration: unreachable)
    }

    let took = try #require(elapsed, "close never returned")
    #expect(took < .milliseconds(800), "close waited out its bound after a connect: \(took)")
  }

  @Test("A close released by a connect leaves the new socket and frame stream alone")
  func closeReleasedByAConnectLeavesTheNewConnectionAlone() async throws {
    let server = try await startSilentServer()
    defer { server.close(promise: nil) }
    let silentPort = try #require(server.localAddress?.port)
    let transport = try await connectWithheld(.drop)
    var silent = TestConfig.connectionConfiguration()
    silent.port = silentPort

    let closer = Task { await transport.close() }
    try await Task.sleep(for: .milliseconds(200))
    let connectEnded = ManagedAtomic(false)
    let connecting = Task {
      _ = try? await transport.connect(configuration: silent)
      connectEnded.store(true)
    }
    await closer.value

    // The new socket is up and its handshake waits on the silent server; the
    // old close must neither drop the socket nor end the stream it reads.
    var reachedServer = false
    let deadline = ContinuousClock.now + .seconds(3)
    while !reachedServer, ContinuousClock.now < deadline {
      reachedServer = await transport.channel?.remoteAddress?.port == silentPort
      if !reachedServer { try await Task.sleep(for: .milliseconds(50)) }
    }
    try await Task.sleep(for: .milliseconds(300))
    let channel = await transport.channel
    #expect(reachedServer, "the new connection's socket never came up")
    #expect(channel?.remoteAddress?.port == silentPort, "the old close cleared the new socket")
    #expect(channel?.isActive == true, "the new socket was closed")
    #expect(!connectEnded.load(), "the new handshake ended: its frame stream was finished")

    await transport.forceClose()
    await connecting.value
  }
}
