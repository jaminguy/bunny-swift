// This source code is dual-licensed under the Apache License, version 2.0,
// and the MIT license.
//
// SPDX-License-Identifier: Apache-2.0 OR MIT
//
// Copyright (c) 2025-2026 Michael S. Klishin

@_exported import AMQPProtocol
import Foundation
import NIO
import Recovery
@_exported import Transport

/// Type alias to disambiguate from Objective-C Method
public typealias AMQPMethod = AMQPProtocol.Method

/// The connection surface a `Channel` drives. `Connection` is the production
/// conformer; tests stand in a stub so channel-level interleavings can be forced
/// without a broker.
internal protocol ChannelConnection: AnyObject, Sendable {
  var topologyRegistry: TopologyRegistry { get }
  var frameMax: UInt32 { get async }
  func send(_ frame: Frame) async throws
  func writeBatch(_ frames: [Frame]) async throws
  func flush() async
  func channelClosed(_ channelID: UInt16) async
}

extension Connection: ChannelConnection {}

public actor Channel {
  private weak var connection: (any ChannelConnection)?
  private let channelID: UInt16
  private var isOpen = false
  private var confirmMode = false
  private var transactionMode = false

  // QoS settings (recorded for recovery)
  private var prefetchCount: UInt16 = 0
  private var prefetchSize: UInt32 = 0
  private var prefetchGlobal: Bool = false

  // Pending RPC responses, in request order. A slot is added before its request
  // is written, so a reply that lands before the caller parks its continuation
  // is kept for it instead of dropped (see `rpc`).
  private var pendingResponses: [PendingResponse] = []
  private var pendingGetResponses: [CheckedContinuation<GetResponse?, Error>] = []

  // Message assembly
  private var incomingMessage: IncomingMessage?

  // Consumers
  private var consumers: [String: AsyncStream<Message>.Continuation] = [:]
  private var returnHandlers: [@Sendable (ReturnedMessage) -> Void] = []

  // Channel close event handlers
  private var closeHandlers: [@Sendable (ChannelCloseInfo) -> Void] = []

  // Channel recovery event handlers
  private var recoveryHandlers: [@Sendable () async -> Void] = []

  // Publisher confirms
  private var nextPublishSeqNo: UInt64 = 1
  /// One slot per in-flight confirm-mode publish, keyed by sequence number
  /// and created before the frames are written, so an ack that lands before
  /// the publisher parks its waiter is kept for it (see `awaitConfirmation`).
  private var confirmHandlers: [UInt64: PendingConfirm] = [:]
  private var publisherConfirmationTracking = false
  private var outstandingConfirmsLimit: Int = 0
  private var outstandingConfirmsCount: Int = 0
  private var confirmLimitWaiters: [CheckedContinuation<Void, Never>] = []

  /// In confirm mode one publish at a time holds this gate from reading its
  /// sequence number until its frames are written and the number advanced.
  /// `Channel` is a reentrant actor: without the gate two publishes interleave
  /// at the awaits in between, take the same number, and one confirm waiter
  /// overwrites the other. The broker numbers deliveries in the order frames
  /// arrive, so the number and the write must also be one unit against every
  /// other publisher. The gate hands on in arrival order and is released before
  /// the confirm is awaited, so confirm waits still overlap.
  private var publishGateHeld = false
  private var publishGateWaiters: [CheckedContinuation<Void, Never>] = []

  // MARK: - Initialization

  internal init(connection: any ChannelConnection, channelID: UInt16) {
    self.connection = connection
    self.channelID = channelID
  }

  internal func open() async throws {
    let response = try await rpc(.channelOpen(ChannelOpen()))
    guard case .channelOpenOk = response else {
      throw ConnectionError.protocolError("Expected Channel.OpenOk, got \(response)")
    }
    isOpen = true
  }

  // MARK: - Exchange Operations

  public func exchange(
    _ name: String,
    type: ExchangeType,
    durable: Bool = false,
    autoDelete: Bool = false,
    internal: Bool = false,
    arguments: Table = [:]
  ) async throws -> Exchange {
    let declare = ExchangeDeclare(
      reserved1: 0,
      exchange: name,
      type: type.rawValue,
      passive: false,
      durable: durable,
      autoDelete: autoDelete,
      internal: `internal`,
      noWait: false,
      arguments: arguments
    )
    let response = try await rpc(.exchangeDeclare(declare))
    guard case .exchangeDeclareOk = response else {
      throw ConnectionError.protocolError("Expected Exchange.DeclareOk, got \(response)")
    }

    await connection?.topologyRegistry.recordExchange(
      RecordedExchange(
        name: name,
        type: type.rawValue,
        durable: durable,
        autoDelete: autoDelete,
        internal: `internal`,
        arguments: arguments
      ))

    return Exchange(channel: self, name: name, type: type)
  }

  public func direct(_ name: String, durable: Bool = false, autoDelete: Bool = false) async throws
    -> Exchange
  {
    try await exchange(name, type: .direct, durable: durable, autoDelete: autoDelete)
  }

  public func fanout(_ name: String, durable: Bool = false, autoDelete: Bool = false) async throws
    -> Exchange
  {
    try await exchange(name, type: .fanout, durable: durable, autoDelete: autoDelete)
  }

  public func topic(_ name: String, durable: Bool = false, autoDelete: Bool = false) async throws
    -> Exchange
  {
    try await exchange(name, type: .topic, durable: durable, autoDelete: autoDelete)
  }

  public func headers(_ name: String, durable: Bool = false, autoDelete: Bool = false) async throws
    -> Exchange
  {
    try await exchange(name, type: .headers, durable: durable, autoDelete: autoDelete)
  }

  public var defaultExchange: Exchange {
    Exchange(channel: self, name: "", type: .direct)
  }

  public func exchangeDelete(_ name: String, ifUnused: Bool = false) async throws {
    let delete = ExchangeDelete(
      reserved1: 0,
      exchange: name,
      ifUnused: ifUnused,
      noWait: false
    )
    let response = try await rpc(.exchangeDelete(delete))
    guard case .exchangeDeleteOk = response else {
      throw ConnectionError.protocolError("Expected Exchange.DeleteOk, got \(response)")
    }

    await connection?.topologyRegistry.deleteExchange(named: name)
  }

  public func exchangeBind(
    destination: String,
    source: String,
    routingKey: String = "",
    arguments: Table = [:]
  ) async throws {
    try await exchangeBindWithoutRecording(
      destination: destination, source: source, routingKey: routingKey, arguments: arguments)

    await connection?.topologyRegistry.recordExchangeBinding(
      RecordedExchangeBinding(
        destination: destination,
        source: source,
        routingKey: routingKey,
        arguments: arguments
      ))
  }

  internal func exchangeBindWithoutRecording(
    destination: String,
    source: String,
    routingKey: String = "",
    arguments: Table = [:]
  ) async throws {
    let bind = ExchangeBind(
      reserved1: 0,
      destination: destination,
      source: source,
      routingKey: routingKey,
      noWait: false,
      arguments: arguments
    )
    let response = try await rpc(.exchangeBind(bind))
    guard case .exchangeBindOk = response else {
      throw ConnectionError.protocolError("Expected Exchange.BindOk, got \(response)")
    }
  }

  public func exchangeUnbind(
    destination: String,
    source: String,
    routingKey: String = "",
    arguments: Table = [:]
  ) async throws {
    let unbind = ExchangeUnbind(
      reserved1: 0,
      destination: destination,
      source: source,
      routingKey: routingKey,
      noWait: false,
      arguments: arguments
    )
    let response = try await rpc(.exchangeUnbind(unbind))
    guard case .exchangeUnbindOk = response else {
      throw ConnectionError.protocolError("Expected Exchange.UnbindOk, got \(response)")
    }

    await connection?.topologyRegistry.deleteExchangeBinding(
      RecordedExchangeBinding(
        destination: destination,
        source: source,
        routingKey: routingKey,
        arguments: arguments
      ))
  }

  // MARK: - Queue Operations

  /// Declares a server-named, exclusive queue.
  public func temporaryQueue(
    arguments: Table = [:]
  ) async throws -> Queue {
    try await queue("", exclusive: true, arguments: arguments)
  }

  public func queue(
    _ name: String = "",
    type: QueueType? = nil,
    durable: Bool = false,
    exclusive: Bool = false,
    autoDelete: Bool = false,
    arguments: Table = [:]
  ) async throws -> Queue {
    var args = arguments
    if let queueType = type {
      args[XArguments.queueType] = queueType.asTableValue
    }
    let declare = QueueDeclare(
      reserved1: 0,
      queue: name,
      passive: false,
      durable: durable,
      exclusive: exclusive,
      autoDelete: autoDelete,
      noWait: false,
      arguments: args
    )
    let response = try await rpc(.queueDeclare(declare))
    guard case .queueDeclareOk(let ok) = response else {
      throw ConnectionError.protocolError("Expected Queue.DeclareOk, got \(response)")
    }

    let serverNamed = name.isEmpty
    await connection?.topologyRegistry.recordQueue(
      RecordedQueue(
        name: ok.queue,
        durable: durable,
        exclusive: exclusive,
        autoDelete: autoDelete,
        arguments: args,
        serverNamed: serverNamed
      ))

    return Queue(
      channel: self, name: ok.queue, messageCount: ok.messageCount, consumerCount: ok.consumerCount)
  }

  /// Declares a Tanzu RabbitMQ delayed queue (durable by default).
  public func delayedQueue(
    _ name: String,
    retryType: DelayedRetryType? = nil,
    retryMin: Duration? = nil,
    retryMax: Duration? = nil,
    consumerDisconnectedTimeout: Duration? = nil,
    durable: Bool = true,
    arguments: Table = [:]
  ) async throws -> Queue {
    var args = arguments
    if let retryType {
      args[XArguments.delayedRetryType] = retryType.asFieldValue
    }
    if let retryMin {
      args[XArguments.delayedRetryMin] = .int64(durationToMillis(retryMin))
    }
    if let retryMax {
      args[XArguments.delayedRetryMax] = .int64(durationToMillis(retryMax))
    }
    if let consumerDisconnectedTimeout {
      args[XArguments.consumerDisconnectedTimeout] =
        .int64(durationToMillis(consumerDisconnectedTimeout))
    }
    return try await queue(name, type: .delayed, durable: durable, arguments: args)
  }

  /// Declares a Tanzu RabbitMQ JMS queue (durable by default).
  public func jmsQueue(
    _ name: String,
    selectorFields: [String]? = nil,
    selectorFieldMaxBytes: Int? = nil,
    consumerDisconnectedTimeout: Duration? = nil,
    durable: Bool = true,
    arguments: Table = [:]
  ) async throws -> Queue {
    var args = arguments
    if let selectorFields {
      args[XArguments.selectorFields] = .array(selectorFields.map { .string($0) })
    }
    if let selectorFieldMaxBytes {
      args[XArguments.selectorFieldMaxBytes] = .int64(Int64(selectorFieldMaxBytes))
    }
    if let consumerDisconnectedTimeout {
      args[XArguments.consumerDisconnectedTimeout] =
        .int64(durationToMillis(consumerDisconnectedTimeout))
    }
    return try await queue(name, type: .jms, durable: durable, arguments: args)
  }

  public func queueDelete(
    _ name: String,
    ifUnused: Bool = false,
    ifEmpty: Bool = false
  ) async throws -> UInt32 {
    let delete = QueueDelete(
      reserved1: 0,
      queue: name,
      ifUnused: ifUnused,
      ifEmpty: ifEmpty,
      noWait: false
    )
    let response = try await rpc(.queueDelete(delete))
    guard case .queueDeleteOk(let ok) = response else {
      throw ConnectionError.protocolError("Expected Queue.DeleteOk, got \(response)")
    }

    await connection?.topologyRegistry.deleteQueue(named: name)

    return ok.messageCount
  }

  public func queuePurge(_ name: String) async throws -> UInt32 {
    let purge = QueuePurge(reserved1: 0, queue: name, noWait: false)
    let response = try await rpc(.queuePurge(purge))
    guard case .queuePurgeOk(let ok) = response else {
      throw ConnectionError.protocolError("Expected Queue.PurgeOk, got \(response)")
    }
    return ok.messageCount
  }

  public func queueBind(
    queue: String,
    exchange: String,
    routingKey: String = "",
    arguments: Table = [:]
  ) async throws {
    try await queueBindWithoutRecording(
      queue: queue, exchange: exchange, routingKey: routingKey, arguments: arguments)

    await connection?.topologyRegistry.recordQueueBinding(
      RecordedQueueBinding(
        queue: queue,
        exchange: exchange,
        routingKey: routingKey,
        arguments: arguments
      ))
  }

  internal func queueBindWithoutRecording(
    queue: String,
    exchange: String,
    routingKey: String = "",
    arguments: Table = [:]
  ) async throws {
    let bind = QueueBind(
      reserved1: 0,
      queue: queue,
      exchange: exchange,
      routingKey: routingKey,
      noWait: false,
      arguments: arguments
    )
    let response = try await rpc(.queueBind(bind))
    guard case .queueBindOk = response else {
      throw ConnectionError.protocolError("Expected Queue.BindOk, got \(response)")
    }
  }

  public func queueUnbind(
    queue: String,
    exchange: String,
    routingKey: String = "",
    arguments: Table = [:]
  ) async throws {
    let unbind = QueueUnbind(
      reserved1: 0,
      queue: queue,
      exchange: exchange,
      routingKey: routingKey,
      arguments: arguments
    )
    let response = try await rpc(.queueUnbind(unbind))
    guard case .queueUnbindOk = response else {
      throw ConnectionError.protocolError("Expected Queue.UnbindOk, got \(response)")
    }

    await connection?.topologyRegistry.deleteQueueBinding(
      RecordedQueueBinding(
        queue: queue,
        exchange: exchange,
        routingKey: routingKey,
        arguments: arguments
      ))
  }

  // MARK: - Publishing

  /// Immediate flush for low-latency single messages.
  ///
  /// When publisher confirmation tracking is enabled, this method waits for broker
  /// confirmation before returning. If the broker nacks the message, throws an error.
  public func basicPublish(
    body: Data,
    exchange: String = "",
    routingKey: String,
    mandatory: Bool = false,
    immediate: Bool = false,
    properties: BasicProperties = .persistent
  ) async throws {
    guard isOpen, let connection = connection else {
      throw ConnectionError.notConnected
    }

    if publisherConfirmationTracking {
      try await waitForConfirmSlot()
    }

    if confirmMode {
      await acquirePublishGate()
      // The channel may have closed while this publish waited its turn.
      guard isOpen else {
        releasePublishGate()
        throw ConnectionError.notConnected
      }
    }
    let seqNo = confirmMode ? nextPublishSeqNo : 0
    let pending = registerConfirmSlot(seqNo: seqNo)

    let frames = buildPublishFrames(
      body: body,
      exchange: exchange,
      routingKey: routingKey,
      mandatory: mandatory,
      immediate: immediate,
      properties: properties,
      frameMax: await connection.frameMax
    )

    do {
      try await connection.writeBatch(frames)
    } catch {
      if confirmMode {
        confirmHandlers.removeValue(forKey: seqNo)
        releasePublishGate()
      }
      throw error
    }
    await connection.flush()

    if confirmMode {
      nextPublishSeqNo += 1
      releasePublishGate()
      if let pending {
        try await awaitConfirmation(pending, seqNo: seqNo)
      }
    }
  }

  /// Buffers writes for high throughput; call flush after batch.
  ///
  /// When publisher confirmation tracking is enabled, this method waits for broker
  /// confirmation before returning. If the broker nacks the message, throws an error.
  public func publish(
    body: Data,
    exchange: String = "",
    routingKey: String,
    mandatory: Bool = false,
    properties: BasicProperties = .persistent
  ) async throws {
    guard isOpen, let connection = connection else {
      throw ConnectionError.notConnected
    }

    if publisherConfirmationTracking {
      try await waitForConfirmSlot()
    }

    if confirmMode {
      await acquirePublishGate()
      // The channel may have closed while this publish waited its turn.
      guard isOpen else {
        releasePublishGate()
        throw ConnectionError.notConnected
      }
    }
    let seqNo = confirmMode ? nextPublishSeqNo : 0
    let pending = registerConfirmSlot(seqNo: seqNo)

    let frames = buildPublishFrames(
      body: body,
      exchange: exchange,
      routingKey: routingKey,
      mandatory: mandatory,
      immediate: false,
      properties: properties,
      frameMax: await connection.frameMax
    )

    do {
      try await connection.writeBatch(frames)
    } catch {
      if confirmMode {
        confirmHandlers.removeValue(forKey: seqNo)
        releasePublishGate()
      }
      throw error
    }

    if confirmMode {
      nextPublishSeqNo += 1
      releasePublishGate()
      if let pending {
        await connection.flush()
        try await awaitConfirmation(pending, seqNo: seqNo)
      }
    }
  }

  private func waitForConfirmSlot() async throws {
    guard outstandingConfirmsLimit > 0 else { return }
    while outstandingConfirmsCount >= outstandingConfirmsLimit {
      await withCheckedContinuation { cont in
        confirmLimitWaiters.append(cont)
      }
      // Woken by the channel going away rather than by a freed slot.
      guard isOpen else {
        throw ConnectionError.notConnected
      }
    }
    outstandingConfirmsCount += 1
  }

  /// Takes the publish gate, waiting behind every publish that asked first. A
  /// releasing holder hands the gate straight to the first waiter, so
  /// `publishGateHeld` stays true across the handoff.
  private func acquirePublishGate() async {
    guard publishGateHeld else {
      publishGateHeld = true
      return
    }
    await withCheckedContinuation { cont in
      publishGateWaiters.append(cont)
    }
  }

  private func releasePublishGate() {
    guard !publishGateWaiters.isEmpty else {
      publishGateHeld = false
      return
    }
    publishGateWaiters.removeFirst().resume()
  }

  /// Registers the confirm slot for a tracked publish. Called under the
  /// publish gate, in the same synchronous section that took `seqNo`, so the
  /// slot exists before any frame is written: the broker's ack travels back
  /// as its own job and nothing orders it after the publisher's resumption
  /// from the write and flush hops. With the slot installed only in
  /// `awaitConfirmation`, an ack that won that race found no waiter, was
  /// dropped, and the publisher parked forever while its outstanding-confirms
  /// slot stayed taken. Returns nil when nothing will await (no tracking).
  private func registerConfirmSlot(seqNo: UInt64) -> PendingConfirm? {
    guard confirmMode, publisherConfirmationTracking else { return nil }
    let pending = PendingConfirm()
    confirmHandlers[seqNo] = pending
    return pending
  }

  private func awaitConfirmation(_ pending: PendingConfirm, seqNo: UInt64) async throws {
    // A close or connection loss during the write has already settled the
    // slot with its error, and an early ack has already settled it with the
    // answer; either resumes here without parking.
    let confirmed = try await withCheckedThrowingContinuation { cont in
      if let outcome = pending.outcome {
        cont.resume(with: outcome)
      } else {
        pending.continuation = cont
      }
    }
    if !confirmed {
      throw ConnectionError.publisherNack(seqNo: seqNo)
    }
  }

  public func flush() async {
    await connection?.flush()
  }

  private func buildPublishFrames(
    body: Data,
    exchange: String,
    routingKey: String,
    mandatory: Bool,
    immediate: Bool,
    properties: BasicProperties,
    frameMax: UInt32
  ) -> [Frame] {
    let maxBodySize = Int(frameMax) - 8
    let bodyFrameCount = body.isEmpty ? 0 : (body.count + maxBodySize - 1) / maxBodySize
    var frames: [Frame] = []
    frames.reserveCapacity(2 + bodyFrameCount)

    let publish = BasicPublish(
      reserved1: 0,
      exchange: exchange,
      routingKey: routingKey,
      mandatory: mandatory,
      immediate: immediate
    )
    frames.append(.method(channelID: channelID, method: .basicPublish(publish)))

    frames.append(
      .header(
        channelID: channelID,
        classID: ClassID.basic.rawValue,
        bodySize: UInt64(body.count),
        properties: properties
      ))

    // AMQP 0-9-1: no body frames when bodySize is zero
    if !body.isEmpty {
      if body.count <= maxBodySize {
        // Fast path: body fits in single frame, no copy needed
        frames.append(.body(channelID: channelID, payload: body))
      } else {
        // Chunked: split into multiple frames
        var offset = 0
        while offset < body.count {
          let end = min(offset + maxBodySize, body.count)
          frames.append(.body(channelID: channelID, payload: body[offset..<end]))
          offset = end
        }
      }
    }

    return frames
  }

  // MARK: - Consuming

  public func basicConsume(
    queue: String,
    consumerTag: String = "",
    acknowledgementMode: ConsumerAcknowledgementMode = .manual,
    exclusive: Bool = false,
    arguments: Table = [:]
  ) async throws -> MessageStream {
    let consume = BasicConsume(
      reserved1: 0,
      queue: queue,
      consumerTag: consumerTag,
      noLocal: false,
      noAck: acknowledgementMode.noAckFieldValue,
      exclusive: exclusive,
      noWait: false,
      arguments: arguments
    )
    let response = try await rpc(.basicConsume(consume))
    guard case .basicConsumeOk(let ok) = response else {
      throw ConnectionError.protocolError("Expected Basic.ConsumeOk, got \(response)")
    }

    let (stream, continuation) = AsyncStream<Message>.makeStream()
    consumers[ok.consumerTag] = continuation

    await connection?.topologyRegistry.recordConsumer(
      RecordedConsumer(
        consumerTag: ok.consumerTag,
        queue: queue,
        acknowledgementMode: acknowledgementMode,
        exclusive: exclusive,
        arguments: arguments
      ))

    return MessageStream(channel: self, consumerTag: ok.consumerTag, stream: stream)
  }

  public func basicCancel(_ consumerTag: String) async throws {
    let cancel = BasicCancel(consumerTag: consumerTag, noWait: false)
    let response = try await rpc(.basicCancel(cancel))
    guard case .basicCancelOk = response else {
      throw ConnectionError.protocolError("Expected Basic.CancelOk, got \(response)")
    }
    removeConsumer(consumerTag)

    await connection?.topologyRegistry.deleteConsumer(tag: consumerTag)
  }

  /// Synchronously fetch a single message from a queue (pull API).
  ///
  /// - Warning: This method is inefficient and strongly discouraged for production use.
  ///   It requires a network round-trip per message, resulting in very low throughput.
  ///   Use ``basicConsume(queue:consumerTag:acknowledgementMode:exclusive:arguments:)`` instead for
  ///   efficient push-based message delivery.
  ///
  /// This method is suitable only for tests, debugging, or administrative tools
  /// where occasional single-message retrieval is acceptable.
  ///
  /// - Parameters:
  ///   - queue: The queue name to fetch from
  ///   - acknowledgementMode: Whether messages require manual acknowledgement (default) or are auto-acknowledged.
  /// - Returns: A ``GetResponse`` if a message was available, or `nil` if the queue was empty.
  @_documentation(visibility: private)
  public func basicGet(queue: String, acknowledgementMode: ConsumerAcknowledgementMode = .manual)
    async throws -> GetResponse?
  {
    let get = BasicGet(reserved1: 0, queue: queue, noAck: acknowledgementMode.noAckFieldValue)
    try await sendMethod(.basicGet(get))

    return try await withCheckedThrowingContinuation { cont in
      pendingGetResponses.append(cont)
    }
  }

  // MARK: - Acknowledgements

  public func basicAck(deliveryTag: UInt64, multiple: Bool = false) async throws {
    let ack = BasicAck(deliveryTag: deliveryTag, multiple: multiple)
    try await sendMethod(.basicAck(ack))
  }

  public func basicNack(deliveryTag: UInt64, multiple: Bool = false, requeue: Bool = true)
    async throws
  {
    let nack = BasicNack(deliveryTag: deliveryTag, multiple: multiple, requeue: requeue)
    try await sendMethod(.basicNack(nack))
  }

  public func basicReject(deliveryTag: UInt64, requeue: Bool = true) async throws {
    let reject = BasicReject(deliveryTag: deliveryTag, requeue: requeue)
    try await sendMethod(.basicReject(reject))
  }

  // MARK: - QoS

  public func basicQos(prefetchSize: UInt32 = 0, prefetchCount: UInt16 = 0, global: Bool = false)
    async throws
  {
    let qos = BasicQos(prefetchSize: prefetchSize, prefetchCount: prefetchCount, global: global)
    let response = try await rpc(.basicQos(qos))
    guard case .basicQosOk = response else {
      throw ConnectionError.protocolError("Expected Basic.QosOk, got \(response)")
    }

    self.prefetchSize = prefetchSize
    self.prefetchCount = prefetchCount
    self.prefetchGlobal = global
  }

  // MARK: - Publisher Confirms

  /// Enable publisher confirms mode.
  ///
  /// - Parameter tracking: If `true`, publish methods will automatically wait for broker
  ///   confirmation before returning. Defaults to `false`.
  /// - Parameter outstandingLimit: Maximum number of unconfirmed messages before publish
  ///   methods block. Only applies when `tracking` is `true`. Use `0` for unlimited.
  public func confirmSelect(tracking: Bool = false, outstandingLimit: Int = 0) async throws {
    // The broker treats a repeated Confirm.Select as a no-op and keeps numbering
    // deliveries where it was; restarting the sequence here would put every
    // later publish one or more tags away from the answer meant for it. The
    // second check covers callers that overlapped while the RPC was in flight.
    guard !confirmMode else { return }
    let select = ConfirmSelect(noWait: false)
    let response = try await rpc(.confirmSelect(select))
    guard case .confirmSelectOk = response else {
      throw ConnectionError.protocolError("Expected Confirm.SelectOk, got \(response)")
    }
    guard !confirmMode else { return }
    confirmMode = true
    publisherConfirmationTracking = tracking
    outstandingConfirmsLimit = outstandingLimit
    nextPublishSeqNo = 1
  }

  /// Whether publisher confirmation tracking is enabled.
  public var isPublisherConfirmationTrackingEnabled: Bool {
    publisherConfirmationTracking
  }

  public var publishSeqNo: UInt64 {
    nextPublishSeqNo
  }

  /// How many RPCs are parked for a broker reply. A stub-driven test answers a
  /// request only once the channel is listening for the answer; a reply that
  /// lands earlier is dropped.
  internal var awaitingResponses: Int {
    pendingResponses.filter(\.requestSent).count
  }

  /// Wait for all outstanding publisher confirmations to complete.
  public func waitForConfirms() async throws {
    guard confirmMode else { return }
    while !confirmHandlers.isEmpty {
      try await Task.sleep(for: .milliseconds(10))
    }
  }

  // MARK: - Transactions

  public func txSelect() async throws {
    let response = try await rpc(.txSelect)
    guard case .txSelectOk = response else {
      throw ConnectionError.protocolError("Expected Tx.SelectOk, got \(response)")
    }
    transactionMode = true
  }

  public func txCommit() async throws {
    let response = try await rpc(.txCommit)
    guard case .txCommitOk = response else {
      throw ConnectionError.protocolError("Expected Tx.CommitOk, got \(response)")
    }
  }

  public func txRollback() async throws {
    let response = try await rpc(.txRollback)
    guard case .txRollbackOk = response else {
      throw ConnectionError.protocolError("Expected Tx.RollbackOk, got \(response)")
    }
  }

  // MARK: - Returns

  public func onReturn(_ handler: @escaping @Sendable (ReturnedMessage) -> Void) {
    returnHandlers.append(handler)
  }

  // MARK: - Channel Close Events

  /// Register a handler for channel close events (server or client initiated).
  public func onClose(_ handler: @escaping @Sendable (ChannelCloseInfo) -> Void) {
    closeHandlers.append(handler)
  }

  /// Register a handler called after this channel is recovered.
  public func onRecovery(_ handler: @escaping @Sendable () async -> Void) {
    recoveryHandlers.append(handler)
  }

  /// Invoke channel recovery handlers.
  internal func notifyRecovered() async {
    for handler in recoveryHandlers {
      await handler()
    }
  }

  // MARK: - Frame Handling

  internal func handleFrame(_ frame: Frame) async {
    switch frame {
    case .method(channelID: _, let method):
      await handleMethod(method)
    case .header(channelID: _, classID: _, let bodySize, let properties):
      if var msg = incomingMessage {
        msg.properties = properties
        msg.bodySize = bodySize
        if bodySize == 0 {
          await completeMessage(msg)
        } else {
          incomingMessage = msg
        }
      }
    case .body(channelID: _, payload: let chunk):
      if var msg = incomingMessage {
        msg.appendBody(chunk)
        if UInt64(msg.receivedBytes) >= msg.bodySize {
          await completeMessage(msg)
        } else {
          incomingMessage = msg
        }
      }
    case .heartbeat:
      break
    }
  }

  private func handleMethod(_ method: AMQPMethod) async {
    switch method {
    case .channelOpenOk, .channelCloseOk, .channelFlowOk,
      .exchangeDeclareOk, .exchangeDeleteOk, .exchangeBindOk, .exchangeUnbindOk,
      .queueDeclareOk, .queueDeleteOk, .queuePurgeOk, .queueBindOk, .queueUnbindOk,
      .basicQosOk, .basicConsumeOk, .basicCancelOk, .basicRecoverOk,
      .txSelectOk, .txCommitOk, .txRollbackOk,
      .confirmSelectOk:
      if !pendingResponses.isEmpty {
        pendingResponses.removeFirst().settle(.success(method))
      }

    case .channelClose(let close):
      isOpen = false
      try? await sendMethod(.channelCloseOk)
      let error = ConnectionError.channelClosed(
        replyCode: close.replyCode,
        replyText: close.replyText,
        classID: close.classId,
        methodID: close.methodId
      )
      let closeInfo = ChannelCloseInfo(
        replyCode: close.replyCode,
        replyText: close.replyText,
        classID: close.classId,
        methodID: close.methodId,
        initiatedByServer: true
      )
      for handler in closeHandlers {
        handler(closeInfo)
      }
      for pending in pendingResponses {
        pending.settle(.failure(error))
      }
      pendingResponses.removeAll()
      for cont in pendingGetResponses {
        cont.resume(throwing: error)
      }
      pendingGetResponses.removeAll()
      failOutstandingConfirms(with: error)
      await connection?.channelClosed(channelID)

    case .basicDeliver(let deliver):
      incomingMessage = IncomingMessage(
        properties: BasicProperties(),
        bodySize: 0,
        deliveryInfo: DeliveryInfo(
          consumerTag: deliver.consumerTag,
          deliveryTag: deliver.deliveryTag,
          redelivered: deliver.redelivered,
          exchange: deliver.exchange,
          routingKey: deliver.routingKey
        )
      )

    case .basicGetOk(let ok):
      incomingMessage = IncomingMessage(
        properties: BasicProperties(),
        bodySize: 0,
        getInfo: GetInfo(
          deliveryTag: ok.deliveryTag,
          redelivered: ok.redelivered,
          exchange: ok.exchange,
          routingKey: ok.routingKey,
          messageCount: ok.messageCount
        )
      )

    case .basicGetEmpty:
      if let cont = pendingGetResponses.first {
        pendingGetResponses.removeFirst()
        cont.resume(returning: nil)
      }

    case .basicReturn(let ret):
      incomingMessage = IncomingMessage(
        properties: BasicProperties(),
        bodySize: 0,
        returnInfo: ReturnInfo(
          replyCode: ret.replyCode,
          replyText: ret.replyText,
          exchange: ret.exchange,
          routingKey: ret.routingKey
        )
      )

    case .basicAck(let ack):
      handleConfirm(deliveryTag: ack.deliveryTag, multiple: ack.multiple, ack: true)

    case .basicNack(let nack):
      handleConfirm(deliveryTag: nack.deliveryTag, multiple: nack.multiple, ack: false)

    case .basicCancel(let cancel):
      removeConsumer(cancel.consumerTag)
      let cancelOk = BasicCancelOk(consumerTag: cancel.consumerTag)
      try? await sendMethod(.basicCancelOk(cancelOk))

      // An auto-delete queue's last consumer was cancelled by the server.
      // Remove the consumer from topology so it won't be recovered.
      await connection?.topologyRegistry.deleteConsumer(tag: cancel.consumerTag)

    default:
      break
    }
  }

  /// Delivers a fully assembled message and clears the incoming message slot.
  private func completeMessage(_ msg: IncomingMessage) async {
    await deliverMessage(msg)
    incomingMessage = nil
  }

  private func deliverMessage(_ incoming: IncomingMessage) async {
    let body = incoming.assembleBody()
    if let deliveryInfo = incoming.deliveryInfo {
      let message = Message(
        body: body,
        properties: incoming.properties,
        deliveryInfo: deliveryInfo,
        channel: self
      )
      consumers[deliveryInfo.consumerTag]?.yield(message)
    } else if let getInfo = incoming.getInfo {
      let response = GetResponse(
        body: body,
        properties: incoming.properties,
        deliveryTag: getInfo.deliveryTag,
        redelivered: getInfo.redelivered,
        exchange: getInfo.exchange,
        routingKey: getInfo.routingKey,
        messageCount: getInfo.messageCount,
        channel: self
      )
      if let cont = pendingGetResponses.first {
        pendingGetResponses.removeFirst()
        cont.resume(returning: response)
      }
    } else if let returnInfo = incoming.returnInfo {
      let returned = ReturnedMessage(
        body: body,
        properties: incoming.properties,
        replyCode: returnInfo.replyCode,
        replyText: returnInfo.replyText,
        exchange: returnInfo.exchange,
        routingKey: returnInfo.routingKey
      )
      for handler in returnHandlers {
        handler(returned)
      }
    }
  }

  private func handleConfirm(deliveryTag: UInt64, multiple: Bool, ack: Bool) {
    var confirmedCount = 0
    if multiple {
      let matched = confirmHandlers.filter { $0.key <= deliveryTag }
      for (tag, pending) in matched {
        confirmHandlers.removeValue(forKey: tag)
        pending.settle(.success(ack))
        confirmedCount += 1
      }
    } else if let pending = confirmHandlers.removeValue(forKey: deliveryTag) {
      pending.settle(.success(ack))
      confirmedCount += 1
    }

    // Release waiters if we freed up slots
    if publisherConfirmationTracking && outstandingConfirmsLimit > 0 {
      outstandingConfirmsCount -= confirmedCount
      while !confirmLimitWaiters.isEmpty && outstandingConfirmsCount < outstandingConfirmsLimit {
        let waiter = confirmLimitWaiters.removeFirst()
        waiter.resume()
      }
    }
  }

  // MARK: - Recovery

  /// Fail all pending RPCs on connection loss.
  /// Consumer continuations are kept alive so `for await` loops
  /// resume transparently after recovery.
  internal func handleConnectionLost() {
    let error = ConnectionError.notConnected
    for pending in pendingResponses {
      pending.settle(.failure(error))
    }
    pendingResponses.removeAll()
    for cont in pendingGetResponses {
      cont.resume(throwing: error)
    }
    pendingGetResponses.removeAll()
    isOpen = false
    failOutstandingConfirms(with: error)
    incomingMessage = nil
  }

  /// Fails every publish still waiting on the broker and wakes the publishers
  /// parked on the outstanding-confirms limit so they observe the closed
  /// channel. Every path that ends the channel's life calls this: a waiter
  /// nobody resumes is destroyed unresumed and its publisher stays suspended
  /// for the life of the process.
  private func failOutstandingConfirms(with error: any Error) {
    for (_, pending) in confirmHandlers {
      pending.settle(.failure(error))
    }
    confirmHandlers.removeAll()
    for waiter in confirmLimitWaiters {
      waiter.resume()
    }
    confirmLimitWaiters.removeAll()
    outstandingConfirmsCount = 0
  }

  /// Finish all consumer streams permanently. Called when
  /// recovery is disabled or all retry attempts are exhausted.
  internal func terminateConsumers() {
    for continuation in consumers.values {
      continuation.finish()
    }
    consumers.removeAll()
  }

  internal func recoverOnNewConnection() async throws {
    // Clear any pending state from a previous failed recovery attempt
    let error = ConnectionError.notConnected
    for pending in pendingResponses { pending.settle(.failure(error)) }
    pendingResponses.removeAll()
    for cont in pendingGetResponses { cont.resume(throwing: error) }
    pendingGetResponses.removeAll()

    try await open()

    // Restore QoS
    if prefetchCount > 0 || prefetchSize > 0 {
      let qos = BasicQos(
        prefetchSize: prefetchSize, prefetchCount: prefetchCount, global: prefetchGlobal)
      let response = try await rpc(.basicQos(qos))
      guard case .basicQosOk = response else {
        throw ConnectionError.protocolError("Expected Basic.QosOk during recovery")
      }
    }

    // Restore publisher confirms
    if confirmMode {
      let select = ConfirmSelect(noWait: false)
      let response = try await rpc(.confirmSelect(select))
      guard case .confirmSelectOk = response else {
        throw ConnectionError.protocolError("Expected Confirm.SelectOk during recovery")
      }
      nextPublishSeqNo = 1
    }

    // Restore transaction mode
    if transactionMode {
      let response = try await rpc(.txSelect)
      guard case .txSelectOk = response else {
        throw ConnectionError.protocolError("Expected Tx.SelectOk during recovery")
      }
    }
  }

  internal func redeclareExchange(_ exchange: RecordedExchange) async throws {
    let declare = ExchangeDeclare(
      reserved1: 0,
      exchange: exchange.name,
      type: exchange.type,
      passive: false,
      durable: exchange.durable,
      autoDelete: exchange.autoDelete,
      internal: exchange.internal,
      noWait: false,
      arguments: exchange.arguments
    )
    let response = try await rpc(.exchangeDeclare(declare))
    guard case .exchangeDeclareOk = response else {
      throw ConnectionError.protocolError("Expected Exchange.DeclareOk during recovery")
    }
  }

  internal func redeclareQueue(_ queue: RecordedQueue) async throws -> String {
    let nameForRecovery = queue.serverNamed ? "" : queue.name
    let declare = QueueDeclare(
      reserved1: 0,
      queue: nameForRecovery,
      passive: false,
      durable: queue.durable,
      exclusive: queue.exclusive,
      autoDelete: queue.autoDelete,
      noWait: false,
      arguments: queue.arguments
    )
    let response = try await rpc(.queueDeclare(declare))
    guard case .queueDeclareOk(let ok) = response else {
      throw ConnectionError.protocolError("Expected Queue.DeclareOk during recovery")
    }
    return ok.queue
  }

  /// Re-register this channel's consumers on the server.
  /// Returns (oldTag, newTag) pairs for any tags that changed.
  internal func recoverOwnConsumers(
    from registry: TopologyRegistry,
    filter: (@Sendable (RecordedConsumer) -> Bool)? = nil
  ) async -> [(String, String)] {
    var tagChanges: [(String, String)] = []
    // Snapshot keys to avoid mutating the dictionary during iteration
    let tags = Array(consumers.keys)
    for tag in tags {
      guard let recorded = await registry.consumer(forTag: tag) else { continue }
      if let filter, !filter(recorded) { continue }
      if let change = await recoverConsumer(recorded) {
        tagChanges.append(change)
      }
    }
    return tagChanges
  }

  /// Re-register a single consumer, reusing the existing continuation
  /// so the caller's `for await` loop resumes transparently.
  private func recoverConsumer(_ recorded: RecordedConsumer) async -> (String, String)? {
    let consume = BasicConsume(
      reserved1: 0,
      queue: recorded.queue,
      consumerTag: recorded.consumerTag,
      noLocal: false,
      noAck: recorded.acknowledgementMode.noAckFieldValue,
      exclusive: recorded.exclusive,
      noWait: false,
      arguments: recorded.arguments
    )

    do {
      let response = try await rpc(.basicConsume(consume))
      guard case .basicConsumeOk(let ok) = response else { return nil }

      if ok.consumerTag != recorded.consumerTag {
        // Server assigned a different tag; move the continuation
        if let continuation = consumers.removeValue(forKey: recorded.consumerTag) {
          consumers[ok.consumerTag] = continuation
        }
        await connection?.topologyRegistry.deleteConsumer(tag: recorded.consumerTag)
        await connection?.topologyRegistry.recordConsumer(
          RecordedConsumer(
            consumerTag: ok.consumerTag,
            queue: recorded.queue,
            acknowledgementMode: recorded.acknowledgementMode,
            exclusive: recorded.exclusive,
            arguments: recorded.arguments
          ))
        return (recorded.consumerTag, ok.consumerTag)
      }
      return nil
    } catch {
      // Consumer recovery failure: terminate its stream
      if let continuation = consumers.removeValue(forKey: recorded.consumerTag) {
        continuation.finish()
      }
      return nil
    }
  }

  // MARK: - Private Helpers

  private func removeConsumer(_ tag: String) {
    consumers[tag]?.finish()
    consumers.removeValue(forKey: tag)
  }

  private func sendMethod(_ method: AMQPMethod) async throws {
    guard let connection = connection else {
      throw ConnectionError.notConnected
    }
    try await connection.send(.method(channelID: channelID, method: method))
  }

  /// Sends `method` and returns the broker's reply.
  ///
  /// The response slot is queued before the request is written. The reply
  /// travels back through the transport and `Connection` as its own job, and
  /// nothing orders that job after this caller's resumption from `send`: with
  /// the slot queued afterwards, a reply that won the race found nothing to
  /// resume, was dropped, and the caller then parked forever on a continuation
  /// nothing would complete. The slot keeps an early reply until the caller
  /// parks. A `send` that throws has written nothing, so no reply follows and
  /// the slot is removed — unless a close or connection loss already settled
  /// it, in which case it has left the queue already.
  private func rpc(_ method: AMQPMethod) async throws -> AMQPMethod {
    guard let connection = connection else {
      throw ConnectionError.notConnected
    }
    let pending = PendingResponse()
    pendingResponses.append(pending)
    do {
      try await connection.send(.method(channelID: channelID, method: method))
    } catch {
      if let index = pendingResponses.firstIndex(where: { $0 === pending }) {
        pendingResponses.remove(at: index)
      }
      throw error
    }
    pending.requestSent = true
    return try await withCheckedThrowingContinuation { cont in
      if let outcome = pending.outcome {
        cont.resume(with: outcome)
      } else {
        pending.continuation = cont
      }
    }
  }

  // MARK: - Close

  public func close() async throws {
    guard isOpen else { return }
    isOpen = false

    for continuation in consumers.values {
      continuation.finish()
    }
    consumers.removeAll()

    let close = ChannelClose(replyCode: 200, replyText: "Normal shutdown", classId: 0, methodId: 0)
    failOutstandingConfirms(
      with: ConnectionError.channelClosed(
        replyCode: close.replyCode,
        replyText: close.replyText,
        classID: close.classId,
        methodID: close.methodId
      ))
    _ = try? await rpc(.channelClose(close))

    await connection?.channelClosed(channelID)
  }

  // MARK: - Properties

  public var open: Bool { isOpen }
  public var number: UInt16 { channelID }
}

/// One in-flight tracked publish, confined to the channel actor. The broker's
/// ack or nack, or the error that ends the wait, goes to the parked
/// continuation, or is held until the publisher parks when it arrives first.
private final class PendingConfirm {
  var continuation: CheckedContinuation<Bool, Error>?
  var outcome: Result<Bool, Error>?

  func settle(_ result: Result<Bool, Error>) {
    if let continuation {
      self.continuation = nil
      continuation.resume(with: result)
    } else if outcome == nil {
      outcome = result
    }
  }
}

/// One in-flight channel RPC, confined to the channel actor. The reply, or the
/// error that ends the wait, goes to the parked continuation, or is held until
/// the caller parks when it arrives first.
private final class PendingResponse {
  var continuation: CheckedContinuation<AMQPMethod, Error>?
  var outcome: Result<AMQPMethod, Error>?
  /// Set once the request has been written; tests answer only sent requests.
  var requestSent = false

  func settle(_ result: Result<AMQPMethod, Error>) {
    if let continuation {
      self.continuation = nil
      continuation.resume(with: result)
    } else if outcome == nil {
      outcome = result
    }
  }
}

// MARK: - Supporting Types

public enum ExchangeType: String, Sendable {
  case direct
  case fanout
  case topic
  case headers
}

/// Converts a `Duration` to milliseconds as `Int64` for AMQP 0-9-1 table values.
private func durationToMillis(_ d: Duration) -> Int64 {
  let (seconds, attoseconds) = d.components
  return Int64(seconds) * 1_000 + Int64(attoseconds) / 1_000_000_000_000_000
}

private struct IncomingMessage {
  var properties: BasicProperties
  var bodySize: UInt64
  var bodyChunks: [Data] = []
  var receivedBytes: Int = 0
  var deliveryInfo: DeliveryInfo?
  var getInfo: GetInfo?
  var returnInfo: ReturnInfo?

  mutating func appendBody(_ chunk: Data) {
    bodyChunks.append(chunk)
    receivedBytes += chunk.count
  }

  func assembleBody() -> Data {
    guard bodyChunks.count > 1 else {
      return bodyChunks.first ?? Data()
    }
    var result = Data(capacity: receivedBytes)
    for chunk in bodyChunks {
      result.append(chunk)
    }
    return result
  }
}

public struct DeliveryInfo: Sendable {
  public let consumerTag: String
  public let deliveryTag: UInt64
  public let redelivered: Bool
  public let exchange: String
  public let routingKey: String
}

private struct GetInfo {
  let deliveryTag: UInt64
  let redelivered: Bool
  let exchange: String
  let routingKey: String
  let messageCount: UInt32
}

private struct ReturnInfo {
  let replyCode: UInt16
  let replyText: String
  let exchange: String
  let routingKey: String
}

public struct ChannelCloseInfo: Sendable {
  public let replyCode: UInt16
  public let replyText: String
  public let classID: UInt16
  public let methodID: UInt16
  public let initiatedByServer: Bool

  public var isSoftError: Bool {
    AMQPReplyCode(rawValue: replyCode)?.isSoftError ?? false
  }

  public var amqpReplyCode: AMQPReplyCode? {
    AMQPReplyCode(rawValue: replyCode)
  }
}
