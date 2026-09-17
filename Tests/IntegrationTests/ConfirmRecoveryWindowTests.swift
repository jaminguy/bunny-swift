// This source code is dual-licensed under the Apache License, version 2.0,
// and the MIT license.
//
// SPDX-License-Identifier: Apache-2.0 OR MIT
//
// Copyright (c) 2025-2026 Michael S. Klishin

import Foundation
import NIOConcurrencyHelpers
import RabbitMQHTTPAPIClient
import Testing

@testable import BunnySwift

/// Live probes for the recovery window of a confirm-mode channel, against a
/// real broker:
///
/// 1. Publishers that never stop publishing across a forced close. A publish
///    admitted between the recovered channel's `open()` and its confirm
///    re-select is numbered from the old sequence, so no ack ever names its
///    slot and it parks; every later publish is then off by one and parks
///    too. Every publish must settle, and a fresh publish after recovery must
///    settle.
/// 2. A second forced close while channel recovery is still running. The
///    connection ignores every disconnect while it is recovering, so the
///    recovery RPC that was in flight parks, recovery never finishes, and the
///    connection reports open with dead channels. The probe widens the window
///    with many channels and fires the second close from `rabbitmqctl`, which
///    closes connections without the management listing's statistics lag; it
///    reports whether the close landed inside the window.

private let httpAPI = Client()

private enum WindowTestConfig {
  static let recoveryInterval: TimeInterval = 0.5
  static let recoveryTimeout: TimeInterval = 20.0

  static func openConnection(name: String) async throws -> Connection {
    var config = ConnectionConfiguration(
      automaticRecovery: true,
      networkRecoveryInterval: recoveryInterval,
      topologyRecovery: true
    )
    config.heartbeat = 4
    config.connectionName = name
    return try await Connection.open(config)
  }
}

/// A routing key nothing is bound to on the default exchange: dropped and
/// still confirmed.
private let unroutedKey = "bunnyswift.recovery.window.unrouted"

private func pollUntil(
  timeout: TimeInterval, interval: TimeInterval = 0.05,
  _ condition: () async throws -> Bool
) async rethrows -> Bool {
  let deadline = Date().addingTimeInterval(timeout)
  while Date() < deadline {
    if try await condition() { return true }
    try? await Task.sleep(for: .milliseconds(Int(interval * 1000)))
  }
  return try await condition()
}

/// Closes every broker connection whose client-provided name matches,
/// retrying while the management listing catches up.
private func closeAllConnectionsWithName(
  _ connectionName: String, timeout: TimeInterval = 10, interval: TimeInterval = 0.2
) async throws {
  let deadline = Date().addingTimeInterval(timeout)
  while true {
    let connections = try await httpAPI.listConnections()
    let matching = connections.filter { $0.clientProperties?.connectionName == connectionName }
    if !matching.isEmpty {
      for conn in matching {
        try await httpAPI.closeConnection(conn.name, reason: "Closed by a test via the HTTP API")
      }
      return
    }
    if Date() >= deadline {
      throw WindowTestError.connectionNotFound(connectionName)
    }
    try await Task.sleep(for: .milliseconds(Int(interval * 1000)))
  }
}

private enum WindowTestError: Error, CustomStringConvertible {
  case connectionNotFound(String)

  var description: String {
    switch self {
    case .connectionNotFound(let name): "Connection with client name '\(name)' not found in listing"
    }
  }
}

/// Publishers that never stop: each loop publishes until told to stop,
/// counting attempts started and attempts settled (by success or by error).
/// A parked publish is a started attempt that never settles.
private final class PublisherLoops: Sendable {
  private let started = NIOLockedValueBox(0)
  private let settled = NIOLockedValueBox(0)
  private let stopped = NIOLockedValueBox(false)

  var startedCount: Int { started.withLockedValue { $0 } }
  var settledCount: Int { settled.withLockedValue { $0 } }

  func run(count: Int, on channel: Channel) {
    for index in 0..<count {
      Task {
        var attempt = 0
        while !stopped.withLockedValue({ $0 }) {
          attempt += 1
          started.withLockedValue { $0 += 1 }
          _ = try? await channel.basicPublish(
            body: Data("loop \(index) attempt \(attempt)".utf8), routingKey: unroutedKey)
          settled.withLockedValue { $0 += 1 }
          await Task.yield()
        }
      }
    }
  }

  func stop() {
    stopped.withLockedValue { $0 = true }
  }
}

@Suite("Confirm recovery window", .disabled(if: TestConfig.skipIntegrationTests), .serialized)
struct ConfirmRecoveryWindowTests {

  @Test("Tracked publishes issued across a forced close all settle", .timeLimit(.minutes(2)))
  func publishesAcrossAForcedCloseSettle() async throws {
    let name = "test.confirm.window.\(UUID().uuidString.prefix(8))"
    let connection = try await WindowTestConfig.openConnection(name: name)
    let channel = try await connection.openChannel()
    try await channel.confirmSelect(tracking: true)
    for _ in 0..<3 {
      try await channel.basicPublish(body: Data("before".utf8), routingKey: unroutedKey)
    }

    let loops = PublisherLoops()
    loops.run(count: 16, on: channel)

    try await closeAllConnectionsWithName(name)
    #expect(await pollUntil(timeout: 5) { await !connection.connected }, "the forced close was not detected")
    #expect(
      await pollUntil(timeout: WindowTestConfig.recoveryTimeout) { await connection.connected },
      "the connection did not recover")
    // Let the loops run on the recovered channel for a while.
    try await Task.sleep(for: .seconds(1))
    loops.stop()

    let drained = await pollUntil(timeout: 10) { loops.settledCount == loops.startedCount }
    #expect(
      drained,
      "\(loops.startedCount - loops.settledCount) publish(es) never settled after recovery (started \(loops.startedCount), settled \(loops.settledCount))"
    )

    // A fresh publish on the recovered channel must settle: it proves the
    // client's sequence still matches the broker's delivery tags.
    let fresh = NIOLockedValueBox(false)
    Task {
      _ = try? await channel.basicPublish(body: Data("after".utf8), routingKey: unroutedKey)
      fresh.withLockedValue { $0 = true }
    }
    #expect(await pollUntil(timeout: 5) { fresh.withLockedValue { $0 } }, "a publish after recovery never settled")
    if drained { try? await connection.close() }
  }

  @Test("Recovery survives a second forced close that lands while channels are recovering", .timeLimit(.minutes(3)))
  func recoverySurvivesASecondCloseInsideTheWindow() async throws {
    // The probe's connection belongs to a user created for this test, so the
    // broker can close it through connection tracking — immediate, unlike the
    // management listing, which lags creation by seconds — without touching
    // any other suite's connections.
    let user = "test.second.drop.\(UUID().uuidString.prefix(8))"
    try await httpAPI.createUser(.withPassword(user, password: user))
    try await httpAPI.grantPermissions(
      PermissionParams(user: user, vhost: "/", configure: ".*", write: ".*", read: ".*"))
    defer { Task { try? await httpAPI.deleteUser(user, idempotently: true) } }

    var config = ConnectionConfiguration(
      automaticRecovery: true,
      networkRecoveryInterval: WindowTestConfig.recoveryInterval,
      topologyRecovery: true
    )
    config.username = user
    config.password = user
    config.heartbeat = 4
    config.connectionName = user
    let connection = try await Connection.open(config)
    // Enough confirm-mode channels to make channel recovery outlast the
    // tracked close's round trip, and no more: every channel here is two
    // RPCs per recovery on a broker the other suites are using at the same
    // time.
    var channels: [Channel] = []
    for _ in 0..<300 {
      let channel = try await connection.openChannel()
      try await channel.confirmSelect(tracking: true)
      channels.append(channel)
    }
    let recoveries = NIOLockedValueBox(0)
    await connection.onRecovery { recoveries.withLockedValue { $0 += 1 } }

    let loops = PublisherLoops()
    loops.run(count: 4, on: channels[0])

    try await httpAPI.closeUserConnections(user, reason: "Closed by a test via the HTTP API (first drop)")
    #expect(await pollUntil(timeout: 5) { await !connection.connected }, "the first forced close was not detected")
    // `connected` flips back the moment the socket is up, before channel
    // recovery starts; the second close is issued at that moment and lands
    // while the channels are being recovered. It counts as inside the window
    // when no recovery had completed by the time it was issued.
    #expect(
      await pollUntil(timeout: WindowTestConfig.recoveryTimeout, interval: 0.005) { await connection.connected },
      "the connection did not reconnect")
    try await httpAPI.closeUserConnections(user, reason: "Closed by a test via the HTTP API (second drop)")
    let landedInsideTheWindow = recoveries.withLockedValue { $0 } == 0

    // Whether or not the close landed inside the window, the connection must
    // end up recovered with live channels and every publish settled.
    let recoveredAgain = await pollUntil(timeout: WindowTestConfig.recoveryTimeout) {
      recoveries.withLockedValue { $0 } >= (landedInsideTheWindow ? 1 : 2)
    }
    #expect(
      recoveredAgain,
      "recovery never completed after the second close (landed inside the window: \(landedInsideTheWindow), recoveries: \(recoveries.withLockedValue { $0 }))"
    )
    loops.stop()
    let drained = await pollUntil(timeout: 10) { loops.settledCount == loops.startedCount }
    #expect(
      drained,
      "\(loops.startedCount - loops.settledCount) publish(es) never settled (landed inside the window: \(landedInsideTheWindow))"
    )
    let fresh = NIOLockedValueBox(false)
    Task {
      _ = try? await channels[0].basicPublish(body: Data("after".utf8), routingKey: unroutedKey)
      fresh.withLockedValue { $0 = true }
    }
    #expect(await pollUntil(timeout: 5) { fresh.withLockedValue { $0 } }, "a publish after the second close never settled")
    if !landedInsideTheWindow {
      Issue.record("probe note: the second close landed after channel recovery had completed; the wedge window was not exercised this run", severity: .warning)
    }
    if recoveredAgain && drained { try? await connection.close() }
  }
}
