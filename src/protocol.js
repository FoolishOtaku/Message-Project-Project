/**
 * Wire protocol: newline-delimited JSON (NDJSON) over TCP.
 *
 * TCP is a *stream* of bytes, not a sequence of messages. One `data` event
 * might contain half a message, or three messages glued together. So we
 * need "framing": we end every message with "\n" and buffer incoming bytes
 * until we see a full line.
 */

/**
 * Listen to a socket and call `onMessage(err, msg)` once per complete JSON line.
 * @param {import('node:net').Socket} socket
 * @param {(err: Error | null, msg?: any) => void} onMessage
 */
function attachJsonLines(socket, onMessage) {
  let buffer = '';
  socket.setEncoding('utf8');

  socket.on('data', (chunk) => {
    buffer += chunk;

    let newlineIndex;
    while ((newlineIndex = buffer.indexOf('\n')) !== -1) {
      const line = buffer.slice(0, newlineIndex).trim();
      buffer = buffer.slice(newlineIndex + 1);
      if (!line) continue;

      let msg;
      try {
        msg = JSON.parse(line);
      } catch {
        onMessage(new Error(`Invalid JSON: ${line.slice(0, 80)}`));
        continue;
      }
      onMessage(null, msg);
    }

    // Protect against a client that never sends a newline.
    if (buffer.length > 1_000_000) {
      onMessage(new Error('Frame too large (max 1 MB)'));
      socket.destroy();
    }
  });
}

/**
 * Send one JSON message as a single line.
 * @param {import('node:net').Socket} socket
 * @param {object} obj
 */
function send(socket, obj) {
  if (!socket.destroyed && socket.writable) {
    socket.write(JSON.stringify(obj) + '\n');
  }
}

/** Turn CLI/form text into a payload: JSON if it parses, otherwise a plain string. */
function parsePayload(text) {
  if (text === undefined || text === '') return null;
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

module.exports = { attachJsonLines, send, parsePayload };
