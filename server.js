const http = require("http");
const WebSocket = require("ws");

const PORT = Number(process.env.PORT || 10000);
const BACKEND_WS_URL =
  process.env.BACKEND_WS_URL ||
  "ws://taeserveroflife1.falixsrv.me:27355/";

const server = http.createServer((request, response) => {
  response.writeHead(200, {
    "Content-Type": "text/plain",
    "Cache-Control": "no-store"
  });

  response.end(
    "Eaglercraft WebSocket relay is running.\n" +
    `Backend: ${BACKEND_WS_URL}\n`
  );
});

const frontendServer = new WebSocket.Server({
  noServer: true,
  perMessageDeflate: false
});

server.on("upgrade", (request, socket, head) => {
  frontendServer.handleUpgrade(request, socket, head, (clientSocket) => {
    frontendServer.emit("connection", clientSocket, request);
  });
});

frontendServer.on("connection", (clientSocket, request) => {
  console.log(
    `Eaglercraft connection received from ${request.socket.remoteAddress}`
  );

  const protocolHeader = request.headers["sec-websocket-protocol"];

  const backendOptions = {
    perMessageDeflate: false,
    handshakeTimeout: 15000,
    rejectUnauthorized: true
  };

  const backendSocket = protocolHeader
    ? new WebSocket(
        BACKEND_WS_URL,
        protocolHeader.split(",").map((item) => item.trim()),
        backendOptions
      )
    : new WebSocket(BACKEND_WS_URL, backendOptions);

  const queuedMessages = [];

  clientSocket.on("message", (data, isBinary) => {
    if (backendSocket.readyState === WebSocket.OPEN) {
      backendSocket.send(data, { binary: isBinary });
    } else if (backendSocket.readyState === WebSocket.CONNECTING) {
      queuedMessages.push({ data, isBinary });
    }
  });

  backendSocket.on("open", () => {
    console.log("Connected to FalixNode EaglerXServer");

    for (const message of queuedMessages) {
      backendSocket.send(message.data, {
        binary: message.isBinary
      });
    }

    queuedMessages.length = 0;
  });

  backendSocket.on("message", (data, isBinary) => {
    if (clientSocket.readyState === WebSocket.OPEN) {
      clientSocket.send(data, { binary: isBinary });
    }
  });

  backendSocket.on("error", (error) => {
    console.error(`FalixNode connection error: ${error.message}`);

    if (clientSocket.readyState === WebSocket.OPEN) {
      clientSocket.close(1011, "Backend connection failed");
    }
  });

  clientSocket.on("error", (error) => {
    console.error(`Client WebSocket error: ${error.message}`);
  });

  clientSocket.on("close", () => {
    if (
      backendSocket.readyState === WebSocket.OPEN ||
      backendSocket.readyState === WebSocket.CONNECTING
    ) {
      backendSocket.close();
    }
  });

  backendSocket.on("close", (code, reason) => {
    console.log(
      `FalixNode connection closed: ${code} ${reason.toString()}`
    );

    if (clientSocket.readyState === WebSocket.OPEN) {
      clientSocket.close(code === 1000 ? 1000 : 1011);
    }
  });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Render relay listening on port ${PORT}`);
  console.log(`FalixNode backend: ${BACKEND_WS_URL}`);
});
    
