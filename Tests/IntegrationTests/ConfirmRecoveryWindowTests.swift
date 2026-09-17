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

/// Creates a broker user for one probe, runs the body, and deletes the user
/// before returning, on success and on failure alike. The deletion is
/// awaited: an unstructured task spawned at exit can be outlived by the test
/// process, and a leftover user is a footprint on whatever broker ran the
/// suite.
private func withProbeUser(
  prefix: String, _ body: (String) async throws -> Void
) async throws {
  let user = "\(prefix).\(UUID().uuidString.prefix(8))"
  try await httpAPI.createUser(.withPassword(user, password: user))
  try await httpAPI.grantPermissions(
    PermissionParams(user: user, vhost: "/", configure: ".*", write: ".*", read: ".*"))
  do {
    try await body(user)
  } catch {
    try? await httpAPI.deleteUser(user, idempotently: true)
    throw error
  }
  try await httpAPI.deleteUser(user, idempotently: true)
}

private func openProbeConnection(user: String) async throws -> Connection {
  var config = ConnectionConfiguration(
    automaticRecovery: true,
    networkRecoveryInterval: WindowTestConfig.recoveryInterval,
    topologyRecovery: true
  )
  config.username = user
  config.password = user
  config.heartbeat = 4
  config.connectionName = user
  return try await Connection.open(config)
}

/// Publishes once on a recovered channel and reports whether the broker
/// confirmed it within the deadline. A publish that throws counts as not
/// confirmed: a dead channel fails fast, and "settled" alone would accept it.
private func freshPublishConfirmed(on channel: Channel, timeout: TimeInterval = 5) async -> Bool {
  let result = NIOLockedValueBox<Bool?>(nil)
  Task {
    do {
      try await channel.basicPublish(body: Data("after".utf8), routingKey: unroutedKey)
      result.withLockedValue { $0 = true }
    } catch {
      result.withLockedValue { $0 = false }
    }
  }
  _ = await pollUntil(timeout: timeout) { result.withLockedValue { $0 } != nil }
  return result.withLockedValue { $0 } == true
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

    // A fresh publish on the recovered channel must be confirmed: it proves
    // the client's sequence still matches the broker's delivery tags and
    // that the channel is alive, not failing fast.
    let confirmed = await freshPublishConfirmed(on: channel)
    #expect(confirmed, "a publish after recovery was not confirmed")
    if drained && confirmed { try? await connection.close() }
  }

  @Test("Recovery survives a second forced close that lands while channels are recovering", .timeLimit(.minutes(3)))
  func recoverySurvivesASecondCloseInsideTheWindow() async throws {
    // The probe's connection belongs to a user created for this test, so the
    // broker can close it through connection tracking — immediate, unlike the
    // management listing, which lags creation by seconds — without touching
    // any other suite's connections.
    try await withProbeUser(prefix: "test.second.drop") { user in
      try await secondCloseInsideChannelRecovery(user: user)
    }
  }

  private func secondCloseInsideChannelRecovery(user: String) async throws {
    let connection = try await openProbeConnection(user: user)
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

    // `connected` flips back the moment the socket is up, before channel
    // recovery starts; the second close is issued at that moment and lands
    // while the channels are being recovered. It counts as inside the window
    // when no recovery had completed by the time it was issued. A close that
    // misses the window (recovery already finished) exercises nothing this
    // test is for, so the drop is repeated until one lands; a run in which
    // none lands is a failure, not a pass.
    var landedInsideTheWindow = false
    var attempts = 0
    while !landedInsideTheWindow && attempts < 3 {
      attempts += 1
      let recoveriesBefore = recoveries.withLockedValue { $0 }
      try await httpAPI.closeUserConnections(user, reason: "Closed by a test via the HTTP API (first drop, attempt \(attempts))")
      #expect(await pollUntil(timeout: 5) { await !connection.connected }, "the forced close was not detected")
      #expect(
        await pollUntil(timeout: WindowTestConfig.recoveryTimeout, interval: 0.005) { await connection.connected },
        "the connection did not reconnect")
      try await httpAPI.closeUserConnections(user, reason: "Closed by a test via the HTTP API (second drop, attempt \(attempts))")
      landedInsideTheWindow = recoveries.withLockedValue { $0 } == recoveriesBefore
      if !landedInsideTheWindow {
        // Let this attempt's second recovery finish before trying again.
        _ = await pollUntil(timeout: WindowTestConfig.recoveryTimeout) {
          recoveries.withLockedValue { $0 } >= recoveriesBefore + 2
        }
      }
    }
    #expect(landedInsideTheWindow, "the second close never landed inside channel recovery in \(attempts) attempts; the wedge window was not exercised")
    guard landedInsideTheWindow else {
      loops.stop()
      try? await connection.close()
      return
    }

    // The close landed while channels were recovering: the connection must
    // still end up recovered with live channels and every publish settled.
    let recoveriesAtLanding = recoveries.withLockedValue { $0 }
    let recoveredAgain = await pollUntil(timeout: WindowTestConfig.recoveryTimeout) {
      recoveries.withLockedValue { $0 } > recoveriesAtLanding
    }
    #expect(
      recoveredAgain,
      "recovery never completed after a second close inside channel recovery (recoveries: \(recoveries.withLockedValue { $0 }))"
    )
    loops.stop()
    let drained = await pollUntil(timeout: 10) { loops.settledCount == loops.startedCount }
    #expect(drained, "\(loops.startedCount - loops.settledCount) publish(es) never settled after the second close")
    let confirmed = await freshPublishConfirmed(on: channels[0])
    #expect(confirmed, "a publish after the second close was not confirmed: the channel is dead or misnumbered")
    if recoveredAgain && drained && confirmed { try? await connection.close() }
  }

  /// The second close lands after every channel is back but while the
  /// topology is still being redeclared. Topology recovery swallows RPC
  /// errors, so a recovery that loses its socket there must still notice
  /// and try again rather than report success on a dead connection.
  @Test("Recovery survives a second forced close that lands while the topology is being recovered", .timeLimit(.minutes(3)))
  func recoverySurvivesASecondCloseDuringTopologyRecovery() async throws {
    try await withProbeUser(prefix: "test.topology.drop") { user in
      try await secondCloseDuringTopologyRecovery(user: user)
    }
  }

  private func secondCloseDuringTopologyRecovery(user: String) async throws {
    let connection = try await openProbeConnection(user: user)
    let channel = try await connection.openChannel()
    try await channel.confirmSelect(tracking: true)
    // One channel, so channel recovery is instant, and enough recorded
    // queues that redeclaring them outlasts the close's round trip.
    let prefix = "bunnyswift.recovery.topology.\(UUID().uuidString.prefix(8))"
    for index in 0..<400 {
      _ = try await channel.queue("\(prefix).\(index)", durable: true)
    }
    defer {
      Task {
        for index in 0..<400 { try? await httpAPI.deleteQueue("\(prefix).\(index)", in: "/", idempotently: true) }
      }
    }
    let recoveries = NIOLockedValueBox(0)
    await connection.onRecovery { recoveries.withLockedValue { $0 += 1 } }

    let loops = PublisherLoops()
    loops.run(count: 4, on: channel)

    var landedInsideTopologyRecovery = false
    var attempts = 0
    while !landedInsideTopologyRecovery && attempts < 3 {
      attempts += 1
      let recoveriesBefore = recoveries.withLockedValue { $0 }
      try await httpAPI.closeUserConnections(user, reason: "Closed by a test via the HTTP API (first drop, attempt \(attempts))")
      #expect(await pollUntil(timeout: 5) { await !connection.connected }, "the forced close was not detected")
      #expect(
        await pollUntil(timeout: WindowTestConfig.recoveryTimeout, interval: 0.005) { await connection.connected },
        "the connection did not reconnect")
      // The single channel is back within a round trip of the reconnect;
      // the redeclares are what the close is meant to interrupt.
      try await httpAPI.closeUserConnections(user, reason: "Closed by a test via the HTTP API (second drop, attempt \(attempts))")
      landedInsideTopologyRecovery = recoveries.withLockedValue { $0 } == recoveriesBefore
      if !landedInsideTopologyRecovery {
        _ = await pollUntil(timeout: WindowTestConfig.recoveryTimeout) {
          recoveries.withLockedValue { $0 } >= recoveriesBefore + 2
        }
      }
    }
    #expect(landedInsideTopologyRecovery, "the second close never landed inside recovery in \(attempts) attempts")
    guard landedInsideTopologyRecovery else {
      loops.stop()
      try? await connection.close()
      return
    }

    // A recovery that lost its socket must not be reported as success: when
    // `onRecovery` fires, the connection must actually be open, and publishes
    // must settle on it.
    let recoveriesAtLanding = recoveries.withLockedValue { $0 }
    let recoveredAgain = await pollUntil(timeout: WindowTestConfig.recoveryTimeout) {
      recoveries.withLockedValue { $0 } > recoveriesAtLanding
    }
    #expect(recoveredAgain, "recovery never completed after a second close during topology recovery")
    let openAfterRecovery = await pollUntil(timeout: WindowTestConfig.recoveryTimeout) { await connection.connected }
    #expect(openAfterRecovery, "recovery reported success on a connection that is not open")
    loops.stop()
    let drained = await pollUntil(timeout: 10) { loops.settledCount == loops.startedCount }
    #expect(drained, "\(loops.startedCount - loops.settledCount) publish(es) never settled after the second close")
    let confirmed = await freshPublishConfirmed(on: channel)
    #expect(confirmed, "a publish after the second close was not confirmed: the channel is dead or misnumbered")
    if recoveredAgain && openAfterRecovery && drained && confirmed { try? await connection.close() }
  }
}
