// controllers/wdispatch.controller.js
const { Pool } = require('pg');

const pool = new Pool({
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  host: process.env.PGHOST,
  port: process.env.PGPORT,
  database: process.env.PGDATABASEW,
});

exports.createDispatch = async (req, res) => {
  const client = await pool.connect();
  try {
    const { booking_id, dispatches, through, lr_number } = req.body;

    if (!booking_id || !Array.isArray(dispatches) || dispatches.length === 0) {
      return res.status(400).json({ message: "Invalid dispatch data" });
    }

    await client.query('BEGIN');

    for (const d of dispatches) {
      await client.query(
        `INSERT INTO public.dispatch_logs 
         (booking_id, product_index, product_name, brand, dispatched_cases, dispatched_qty,
          rate_per_box, discount_percent, godown, transport_name, lr_number, dispatched_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW())`,
        [
          booking_id,
          d.product_index,
          d.product_name || "Unknown",
          d.brand || null,
          d.dispatched_cases,
          d.dispatched_qty,
          d.rate_per_box || 0,
          d.discount_percent || 0,
          d.godown || "Main Godown",
          through || "Own Transport",
          lr_number || null
        ]
      );
    }

    await client.query('COMMIT');
    res.json({ message: "Dispatched successfully" });
  } catch (err) {
    await client.query('ROLLBACK');
    console.error("Dispatch Error:", err);
    res.status(500).json({ message: err.message || "Dispatch failed" });
  } finally {
    client.release();
  }
};

// GET ALL LOGS — Used to show dispatched cases
exports.getAllDispatchLogs = async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        dl.booking_id,
        dl.product_index,
        dl.dispatched_cases,
        dl.dispatched_qty,
        dl.rate_per_box,
        dl.discount_percent,
        dl.brand,
        dl.godown,
        dl.transport_name,
        dl.lr_number,
        dl.dispatched_at
      FROM public.dispatch_logs dl
      ORDER BY dl.dispatched_at DESC
    `);
    res.json(result.rows);
  } catch (err) {
    console.error("getAllDispatchLogs Error:", err);
    res.status(500).json({ message: "Failed to fetch logs" });
  }
};

// Optional: Get dispatch logs for a specific booking (for future use)
exports.getDispatchLogsByBooking = async (req, res) => {
  const { booking_id } = req.params;
  try {
    const { rows } = await pool.query(`
      SELECT 
        product_index,
        product_name,
        dispatched_cases,
        dispatched_qty,
        transport_type,
        lr_number,
        dispatched_at
      FROM public.dispatch_logs 
      WHERE booking_id = $1 
      ORDER BY dispatched_at DESC
    `, [booking_id]);

    res.json({ dispatch_logs: rows });
  } catch (err) {
    console.error("getDispatchLogsByBooking Error:", err);
    res.status(500).json({ message: "Failed to fetch logs" });
  }
};