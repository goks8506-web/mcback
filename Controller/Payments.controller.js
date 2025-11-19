// controllers/Payments.controller.js
const { Pool } = require('pg');

const pool = new Pool({
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  host: process.env.PGHOST,
  port: process.env.PGPORT,
  database: process.env.PGDATABASE,
});

exports.createAdmin = async (req, res) => {
  const { username } = req.body;
  if (!username) return res.status(400).json({ error: "Username required" });

  try {
    const result = await pool.query(
      "INSERT INTO admins (username) VALUES ($1) RETURNING id",
      [username]
    );
    res.status(201).json({ id: result.rows[0].id, username });
  } catch (err) {
    if (err.code === '23505') return res.status(400).json({ error: "Admin exists" });
    console.error("createAdmin error:", err);
    res.status(500).json({ error: err.message });
  }
};

exports.getAdmins = async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        a.id, a.username,
        STRING_AGG(DISTINCT ab.bank_name, ',') as bank_names,
        COALESCE(SUM(p.amount_paid), 0)::NUMERIC as total
      FROM admins a
      LEFT JOIN admin_banks ab ON a.username = ab.username
      LEFT JOIN payments p ON a.id = p.admin_id
      GROUP BY a.id
    `);

    res.json(result.rows.map(r => ({
      ...r,
      total: parseFloat(r.total),
      bank_name: r.bank_names ? r.bank_names.split(',').filter(Boolean) : []
    })));
  } catch (err) {
    console.error("getAdmins error:", err);
    res.status(500).json({ error: err.message });
  }
};

exports.getAdminTransactions = async (req, res) => {
  const { id } = req.params;
  try {
    const result = await pool.query(`
      SELECT p.*, b.customer_name, b.bill_number
      FROM payments p
      JOIN book b ON p.booking_id = b.id
      WHERE p.admin_id = $1
      ORDER BY p.transaction_date DESC
    `, [id]);
    res.json(result.rows);
  } catch (err) {
    console.error("getAdminTransactions error:", err);
    res.status(500).json({ error: err.message });
  }
};

exports.addBankAccount = async (req, res) => {
  const { username, bank_name } = req.body;
  if (!username || !bank_name) {
    return res.status(400).json({ error: "Username and bank_name required" });
  }
  try {
    await pool.query(
      "INSERT INTO admin_banks (username, bank_name) VALUES ($1, $2)",
      [username, bank_name]
    );
    res.status(201).json({ message: "Bank added" });
  } catch (err) {
    console.error("addBankAccount error:", err);
    res.status(500).json({ error: err.message });
  }
};

exports.getBankAccounts = async (req, res) => {
  const { username } = req.params;
  try {
    const result = await pool.query(
      "SELECT bank_name FROM admin_banks WHERE username = $1",
      [username]
    );
    res.json(result.rows.map(r => r.bank_name));
  } catch (err) {
    console.error("getBankAccounts error:", err);
    res.status(500).json({ error: err.message });
  }
};

exports.getPending = async (req, res) => {
  try {
    const query = `
      SELECT
        b.id,
        b.bill_number,
        b.customer_name,
        b.total,
        COALESCE(SUM(p.amount_paid), 0)::NUMERIC AS paid,
        (b.total - COALESCE(SUM(p.amount_paid), 0))::NUMERIC AS balance,
        b.bill_date
      FROM public.book b
      LEFT JOIN payments p ON p.booking_id = b.id
      GROUP BY b.id, b.bill_number, b.customer_name, b.total, b.bill_date
      HAVING (b.total - COALESCE(SUM(p.amount_paid), 0)) > 0
      ORDER BY b.bill_date DESC;
    `;
    const { rows } = await pool.query(query);
    res.json(rows);
  } catch (err) {
    console.error('getPending error:', err);
    res.status(500).json({ error: err.message });
  }
};

exports.recordPayment = async (req, res) => {
  const {
    booking_id,
    amount_paid,
    payment_method,
    transaction_date = new Date(),
    bank_name,
    admin_id
  } = req.body;

  if (!booking_id || !amount_paid || !payment_method || !admin_id) {
    return res.status(400).json({ error: 'Missing required fields' });
  }

  try {
    await pool.query(
      `INSERT INTO public.payments (
         booking_id, amount_paid, payment_method,
         bank_name, transaction_date, admin_id
       ) VALUES ($1, $2, $3, $4, $5, $6)`,
      [booking_id, amount_paid, payment_method, bank_name || null, transaction_date, admin_id]
    );
    res.json({ success: true });
  } catch (err) {
    console.error('recordPayment error:', err);
    res.status(500).json({ error: err.message });
  }
};

exports.getPaymentHistory = async (req, res) => {
  const { id } = req.params;
  try {
    const result = await pool.query(`
      SELECT p.*, a.username as admin_name
      FROM payments p
      LEFT JOIN admins a ON p.admin_id = a.id
      WHERE p.booking_id = $1
      ORDER BY p.transaction_date
    `, [id]);
    res.json(result.rows);
  } catch (err) {
    console.error("getPaymentHistory error:", err);
    res.status(500).json({ error: err.message });
  }
};

exports.getsBookings = async (req, res) => {
  try {
    const query = `
      SELECT
        b.id,
        b.bill_number,
        b.customer_name,
        b.address,
        b.through,
        b.lr_number,
        b.total,
        b.created_at,
        b.items,
        b.extra_charges,

        COALESCE((
          SELECT SUM(dl.amount)::NUMERIC
          FROM dispatch_logs dl
          WHERE dl.booking_id = b.id
        ), 0) AS dispatched_total,

        COALESCE(SUM(p.amount_paid), 0)::NUMERIC AS paid,

        (
          SELECT a.username
          FROM payments p2
          LEFT JOIN admins a ON p2.admin_id = a.id
          WHERE p2.booking_id = b.id
          LIMIT 1
        ) AS admin_username,

        (
          SELECT json_agg(
            json_build_object(
              'product_index', dl.product_index,
              'product_name', dl.product_name,
              'dispatched_qty', dl.dispatched_qty,
              'dispatched_cases', dl.dispatched_cases,
              'amount', dl.amount,
              'dispatched_at', dl.dispatched_at,
              'transport_name', b.through,
              'lr_number', b.lr_number,
              -- === NEW: Include original price from items ===
              'price_per_box', (
                SELECT (i->>'rate_per_box')::NUMERIC
                FROM jsonb_array_elements(b.items) WITH ORDINALITY AS t(i, idx)
                WHERE (i->>'s_no')::INT - 1 = dl.product_index
              ),
              'discount_percent', (
                SELECT (i->>'discount_percent')::NUMERIC
                FROM jsonb_array_elements(b.items) WITH ORDINALITY AS t(i, idx)
                WHERE (i->>'s_no')::INT - 1 = dl.product_index
              )
            ) ORDER BY dl.dispatched_at
          )
          FROM dispatch_logs dl
          WHERE dl.booking_id = b.id
        ) AS dispatch_logs,

        (
          SELECT json_agg(
            json_build_object(
              'id', p.id,
              'amount_paid', p.amount_paid,
              'payment_method', p.payment_method,
              'bank_name', p.bank_name,
              'transaction_date', p.transaction_date,
              'admin_id', p.admin_id,
              'admin_username', a2.username
            ) ORDER BY p.transaction_date
          )
          FROM payments p
          LEFT JOIN admins a2 ON p.admin_id = a2.id
          WHERE p.booking_id = b.id
        ) AS payments

      FROM book b
      LEFT JOIN payments p ON p.booking_id = b.id
      GROUP BY b.id
      ORDER BY b.created_at DESC;
    `;

    const { rows } = await pool.query(query);

    const result = rows.map(r => ({
      ...r,
      total: parseFloat(r.total) || 0,
      dispatched_total: parseFloat(r.dispatched_total) || 0,
      paid: parseFloat(r.paid) || 0,
      balance: (parseFloat(r.dispatched_total) || 0) - (parseFloat(r.paid) || 0),

      items: typeof r.items === "string" ? JSON.parse(r.items || "[]") : r.items || [],
      extra_charges: typeof r.extra_charges === "string" ? JSON.parse(r.extra_charges) : r.extra_charges || {},

      dispatch_logs: (r.dispatch_logs || []).map(log => ({
        ...log,
        price_per_box: parseFloat(log.price_per_box) || 0,
        discount_percent: parseFloat(log.discount_percent) || 0
      })),
      payments: r.payments || [],
      admin_username: r.admin_username || null
    }));

    res.json(result);
  } catch (err) {
    console.error("getsBookings error:", err);
    res.status(500).json({ error: err.message });
  }
};

exports.getDispatchLogs = async (req, res) => {
  const { booking_id } = req.params;
  try {
    const result = await pool.query(`
      SELECT 
        product_index,
        product_name,
        dispatched_qty,
        dispatched_cases,
        amount,
        dispatched_at,
        transport_type,
        transport_name,
        lr_number
      FROM dispatch_logs 
      WHERE booking_id = $1
      ORDER BY dispatched_at
    `, [booking_id]);
    res.json({ dispatch_logs: result.rows });
  } catch (err) {
    console.error("getDispatchLogs error:", err);
    res.status(500).json({ error: err.message });
  }
};

exports.getTransactions = async (req, res) => {
  const { id } = req.params;
  try {
    const result = await pool.query(`
      SELECT p.*, a.username as admin_username
      FROM payments p
      LEFT JOIN admins a ON p.admin_id = a.id
      WHERE p.booking_id = $1
    `, [id]);
    res.json(result.rows);
  } catch (err) {
    console.error("getTransactions error:", err);
    res.status(500).json({ error: err.message });
  }
};