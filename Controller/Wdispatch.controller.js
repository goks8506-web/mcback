// controllers/Booking.controller.js
const { Pool } = require('pg');

const pool = new Pool({
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  host: process.env.PGHOST,
  port: process.env.PGPORT,
  database: process.env.PGDATABASE,
});
// controllers/Booking.controller.js
exports.createDispatch = async (req, res) => {
  const client = await pool.connect();
  try {
    const { booking_id, dispatches, transport_type, lr_number } = req.body;
    await client.query('BEGIN');

    for (const d of dispatches) {
      await client.query(
        `INSERT INTO public.dispatch_logs 
         (booking_id, product_index, product_name, dispatched_cases, dispatched_qty, amount, transport_type, lr_number)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
        [
          booking_id,
          d.product_index,
          d.product_name,           // <-- now stored
          d.dispatched_cases,
          d.dispatched_qty,
          d.amount,
          transport_type || 'Own',
          lr_number || null
        ]
      );
    }

    await client.query('COMMIT');
    res.json({ message: 'Dispatched successfully' });
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('createDispatch error:', err);
    res.status(500).json({ message: err.message });
  } finally {
    client.release();
  }
};

exports.getAllDispatchLogs = async (req, res) => {
  const result = await pool.query(`
    SELECT booking_id, product_index, dispatched_cases, dispatched_qty, amount
    FROM public.dispatch_logs
  `);
  res.json(result.rows);
};

exports.getDispatchLogsByBooking = async (req, res) => {
  const { booking_id } = req.params;
  try {
    const { rows } = await pool.query(`
      SELECT 
        *
      FROM public.dispatch_logs dl
      WHERE dl.booking_id = $1
      ORDER BY dl.dispatched_at
    `, [booking_id]);

    res.json({ dispatch_logs: rows });
  } catch (err) {
    console.error('getDispatchLogsByBooking error:', err);
    res.status(500).json({ error: err.message });
  }
};