//===----------------------------------------------------------------------===//
//
// This source file is part of the AsyncHTTPClient open source project
//
// Copyright (c) 2026 Apple Inc. and the AsyncHTTPClient project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of AsyncHTTPClient project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import AsyncHTTPClient
import NIOCore
import NIOHPACK
import NIOHTTP1
import NIOHTTP2
import NIOPosix
import NIOSSL
import XCTest

// Reproduces https://github.com/swift-server/async-http-client/issues/922 end to end.
// The client streams a request body over TLS with ALPN h2. The server sends a complete 200 response
// (HEADERS with END_STREAM) before the request body ends. Then the server resets the stream, sends
// GOAWAY and closes the connection, or closes the connection.
final class Issue922ReproTests: XCTestCase {
    func testRSTStreamAfterEarlyResponseEnd() async throws {
        try await self.runRepro(.rstStream)
    }

    func testGoAwayAfterEarlyResponseEnd() async throws {
        try await self.runRepro(.goAwayAndClose)
    }

    func testConnectionCloseAfterEarlyResponseEnd() async throws {
        try await self.runRepro(.closeConnection)
    }

    private func runRepro(_ action: EarlyResponseThenAbortHandler.Action) async throws {
        let bin = HTTPBin(.http2(compress: false)) { _ in EarlyResponseThenAbortHandler(action: action) }
        defer { XCTAssertNoThrow(try bin.shutdown()) }

        var config = HTTPClient.Configuration()
        config.tlsConfiguration = .clientDefault
        config.tlsConfiguration?.certificateVerification = .none
        config.httpVersion = .automatic
        let client = HTTPClient(eventLoopGroupProvider: .singleton, configuration: config)

        let (body, bodyContinuation) = AsyncStream<ByteBuffer>.makeStream()
        bodyContinuation.yield(ByteBuffer(string: "first part"))

        var request = HTTPClientRequest(url: "https://localhost:\(bin.port)/upload")
        request.method = .POST
        request.body = .stream(body, length: .unknown)

        let response = try await client.execute(request, timeout: .seconds(10))
        XCTAssertEqual(response.status, .ok)
        XCTAssertEqual(response.version, .http2)
        _ = try await response.body.collect(upTo: 1024)

        // Give the client time to receive the reset, the GOAWAY or the close.
        try await Task.sleep(for: .milliseconds(300))
        bodyContinuation.yield(ByteBuffer(string: "second part"))
        bodyContinuation.finish()
        try await Task.sleep(for: .milliseconds(300))

        try await client.shutdown()
    }
}

final class EarlyResponseThenAbortHandler: ChannelInboundHandler {
    typealias InboundIn = HTTPServerRequestPart
    typealias OutboundOut = HTTPServerResponsePart

    enum Action: Sendable {
        case rstStream
        case goAwayAndClose
        case closeConnection
    }

    private let action: Action

    init(action: Action) {
        self.action = action
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        guard case .head = self.unwrapInboundIn(data) else { return }

        // Write one HEADERS frame with END_STREAM. Write it below the HTTP/1 codec, because the codec
        // sends the response end as a separate DATA frame.
        let codecContext = try! context.pipeline.syncOperations.context(
            handlerType: HTTP2FramePayloadToHTTP1ServerCodec.self
        )
        let headers = HTTP2Frame.FramePayload.headers(.init(headers: [":status": "200"], endStream: true))
        let action = self.action

        // Abort only after the HEADERS frame is on the wire.
        codecContext.writeAndFlush(NIOAny(headers)).assumeIsolated().whenComplete { _ in
            switch action {
            case .rstStream:
                context.close(promise: nil)
            case .goAwayAndClose:
                let goAway = HTTP2Frame(
                    streamID: .rootStream,
                    payload: .goAway(lastStreamID: .rootStream, errorCode: .noError, opaqueData: nil)
                )
                let parent = context.channel.parent!
                parent.writeAndFlush(goAway).assumeIsolated().whenComplete { _ in
                    parent.close(promise: nil)
                }
            case .closeConnection:
                context.channel.parent?.close(promise: nil)
            }
        }
    }
}
