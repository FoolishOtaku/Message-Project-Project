/**
 * Persistent SQLite storage for Kurir using Node.js built-in `node:sqlite`.
 * Requires zero npm dependencies (available natively in Node.js 22+).
 *
 * Persists:
 *   1. All published messages & routing outcomes
 *   2. Deliveries history per consumer (with full readable payloads)
 *   3. Waiting/unconsumed queue items so queues survive broker restarts
 */

const { DatabaseSync } = require('node:sqlite');
const path = require('node:path');
const fs = require('node:fs');

const DEFAULT_DB_DIR = path.join(__dirname, '..', 'data');
const DEFAULT_DB_PATH = path.join(DEFAULT_DB_DIR, 'kurir.db');

class Storage {
  constructor(dbPath = process.env.DB_PATH || DEFAULT_DB_PATH) {
    const dir = path.dirname(dbPath);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }

    this.dbPath = dbPath;
    this.db = new DatabaseSync(dbPath);
    this.init();
  }

  init() {
    // Enable Write-Ahead Logging for fast concurrent reads and writes
    this.db.exec('PRAGMA journal_mode = WAL;');
    this.db.exec('PRAGMA foreign_keys = ON;');

    // 1. Messages table
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS messages (
        id TEXT PRIMARY KEY,
        routing_key TEXT NOT NULL,
        payload TEXT,
        published_by TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        status TEXT NOT NULL DEFAULT 'published',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      );
      CREATE INDEX IF NOT EXISTS idx_messages_key ON messages(routing_key);
      CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages(timestamp DESC);
    `);

    // 2. Deliveries table: historical log of what each consumer received
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS deliveries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message_id TEXT NOT NULL,
        queue_name TEXT NOT NULL,
        consumer_id TEXT NOT NULL,
        consumer_name TEXT NOT NULL,
        delivered_at INTEGER NOT NULL,
        FOREIGN KEY (message_id) REFERENCES messages(id) ON DELETE CASCADE
      );
      CREATE INDEX IF NOT EXISTS idx_deliveries_consumer ON deliveries(consumer_name);
      CREATE INDEX IF NOT EXISTS idx_deliveries_queue ON deliveries(queue_name);
      CREATE INDEX IF NOT EXISTS idx_deliveries_delivered_at ON deliveries(delivered_at DESC);
    `);

    // 3. Pending Queue table: holds messages currently waiting in queues
    // This allows queue contents to survive a broker restart!
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS pending_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        queue_name TEXT NOT NULL,
        message_id TEXT NOT NULL,
        enqueued_at INTEGER NOT NULL,
        FOREIGN KEY (message_id) REFERENCES messages(id) ON DELETE CASCADE
      );
      CREATE INDEX IF NOT EXISTS idx_pending_queue ON pending_queue(queue_name, id ASC);
    `);

    // Prepare statements for optimal performance
    this.stmtInsertMessage = this.db.prepare(`
      INSERT OR REPLACE INTO messages (id, routing_key, payload, published_by, timestamp, status)
      VALUES (?, ?, ?, ?, ?, ?)
    `);

    this.stmtUpdateMessageStatus = this.db.prepare(`
      UPDATE messages SET status = ? WHERE id = ?
    `);

    this.stmtEnqueue = this.db.prepare(`
      INSERT INTO pending_queue (queue_name, message_id, enqueued_at)
      VALUES (?, ?, ?)
    `);

    this.stmtDequeue = this.db.prepare(`
      DELETE FROM pending_queue
      WHERE rowid = (
        SELECT rowid FROM pending_queue
        WHERE queue_name = ? AND message_id = ?
        LIMIT 1
      )
    `);

    this.stmtInsertDelivery = this.db.prepare(`
      INSERT INTO deliveries (message_id, queue_name, consumer_id, consumer_name, delivered_at)
      VALUES (?, ?, ?, ?, ?)
    `);

    this.stmtGetPending = this.db.prepare(`
      SELECT pq.queue_name, m.id, m.routing_key, m.payload, m.published_by, m.timestamp
      FROM pending_queue pq
      JOIN messages m ON pq.message_id = m.id
      ORDER BY pq.id ASC
    `);
  }

  // ───────────────────────── Write Operations ─────────────────────────

  savePublishedMessage(message, status = 'published') {
    const payloadStr = typeof message.payload === 'string'
      ? message.payload
      : JSON.stringify(message.payload);

    this.stmtInsertMessage.run(
      message.id,
      message.routingKey,
      payloadStr,
      message.publishedBy,
      message.timestamp,
      status
    );
  }

  updateMessageStatus(messageId, status) {
    this.stmtUpdateMessageStatus.run(status, messageId);
  }

  enqueue(queueName, messageId, timestamp = Date.now()) {
    this.stmtEnqueue.run(queueName, messageId, timestamp);
  }

  dequeue(queueName, messageId) {
    this.stmtDequeue.run(queueName, messageId);
  }

  recordDelivery(messageId, queueName, consumerId, consumerName, deliveredAt = Date.now()) {
    this.stmtInsertDelivery.run(messageId, queueName, consumerId, consumerName, deliveredAt);
    this.updateMessageStatus(messageId, 'delivered');
  }

  // ───────────────────────── Read Operations ─────────────────────────

  /**
   * Load all pending messages across all queues on broker startup
   */
  loadPendingQueues() {
    const rows = this.stmtGetPending.all();
    const queues = new Map();
    for (const r of rows) {
      if (!queues.has(r.queue_name)) {
        queues.set(r.queue_name, []);
      }
      let parsedPayload = null;
      try {
        parsedPayload = r.payload ? JSON.parse(r.payload) : null;
      } catch {
        parsedPayload = r.payload;
      }
      queues.get(r.queue_name).push({
        id: r.id,
        routingKey: r.routing_key,
        payload: parsedPayload,
        publishedBy: r.published_by,
        timestamp: r.timestamp,
      });
    }
    return queues;
  }

  /**
   * Retrieve deliveries log with formatted message payloads for inspection
   */
  getDeliveries({ consumer, queue, search, limit = 50, offset = 0 } = {}) {
    let sql = `
      SELECT
        d.id as delivery_id,
        d.message_id,
        d.queue_name,
        d.consumer_id,
        d.consumer_name,
        d.delivered_at,
        m.routing_key,
        m.payload,
        m.published_by,
        m.timestamp as published_at
      FROM deliveries d
      JOIN messages m ON d.message_id = m.id
      WHERE 1=1
    `;
    const params = [];

    if (consumer && consumer !== 'all') {
      sql += ` AND d.consumer_name = ?`;
      params.push(consumer);
    }
    if (queue && queue !== 'all') {
      sql += ` AND d.queue_name = ?`;
      params.push(queue);
    }
    if (search && search.trim()) {
      sql += ` AND (m.routing_key LIKE ? OR m.payload LIKE ? OR m.id LIKE ? OR d.consumer_name LIKE ?)`;
      const term = `%${search.trim()}%`;
      params.push(term, term, term, term);
    }

    sql += ` ORDER BY d.delivered_at DESC LIMIT ? OFFSET ?`;
    params.push(Number(limit) || 50, Number(offset) || 0);

    const rows = this.db.prepare(sql).all(...params);
    return rows.map((r) => {
      let parsed = null;
      try {
        parsed = r.payload ? JSON.parse(r.payload) : null;
      } catch {
        parsed = r.payload;
      }
      return {
        id: r.delivery_id,
        messageId: r.message_id,
        queue: r.queue_name,
        consumerId: r.consumer_id,
        consumerName: r.consumer_name,
        deliveredAt: r.delivered_at,
        routingKey: r.routing_key,
        publishedBy: r.published_by,
        publishedAt: r.published_at,
        payload: parsed,
      };
    });
  }

  /**
   * Get list of unique consumer names that have ever processed data
   */
  getKnownConsumers() {
    const rows = this.db.prepare(`
      SELECT DISTINCT consumer_name FROM deliveries ORDER BY consumer_name ASC
    `).all();
    return rows.map((r) => r.consumer_name);
  }

  /**
   * Get overview statistics from database
   */
  getStats() {
    const messagesCount = this.db.prepare(`SELECT count(*) as count FROM messages`).get().count;
    const deliveriesCount = this.db.prepare(`SELECT count(*) as count FROM deliveries`).get().count;
    const pendingCount = this.db.prepare(`SELECT count(*) as count FROM pending_queue`).get().count;
    const unroutableCount = this.db.prepare(`SELECT count(*) as count FROM messages WHERE status = 'unroutable'`).get().count;

    return {
      totalStoredMessages: messagesCount,
      totalDeliveries: deliveriesCount,
      totalPendingInQueues: pendingCount,
      unroutable: unroutableCount,
    };
  }
}

module.exports = { Storage };
