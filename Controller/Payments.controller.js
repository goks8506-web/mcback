// controllers/Payments.controller.js
const { Pool } = require('pg');

const pool = new Pool({
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  host: process.env.PGHOST,
  port: process.env.PGPORT,
  database: process.env.PGDATABASEW,
});

// ────────────────────────────────────────────────
// Admin & Bank Account Management
// ────────────────────────────────────────────────

exports.createAdmin = async (req, res) => {
  const { username } = req.body;
  if (!username?.trim()) return res.status(400).json({ error: "Username required" });

  try {
    const result = await pool.query(
      "INSERT INTO admins (username) VALUES ($1) RETURNING id, username",
      [username.trim().toUpperCase()]
    );
    res.status(201).json(result.rows[0]);
  } catch (err) {
    if (err.code === '23505') return res.status(409).json({ error: "Admin username already exists" });
    console.error("createAdmin error:", err);
    res.status(500).json({ error: err.message });
  }
};

exports.getAdmins = async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        a.id, a.username,
        STRING_AGG(DISTINCT ab.bank_name, ', ') as bank_names,
        COALESCE(SUM(p.amount_paid), 0)::NUMERIC as total_collected
      FROM admins a
      LEFT JOIN admin_banks ab ON a.username = ab.username
      LEFT JOIN payments p ON a.id = p.admin_id
      GROUP BY a.id
      ORDER BY a.username
    `);

    res.json(result.rows.map(r => ({
      ...r,
      total_collected: parseFloat(r.total_collected || 0),
      bank_names: r.bank_names ? r.bank_names.split(', ').filter(Boolean) : []
    })));
  } catch (err) {
    console.error("getAdmins error:", err);
    res.status(500).json({ error: err.message });
  }
};

exports.addBankAccount = async (req, res) => {
  const { username, bank_name } = req.body;
  if (!username || !bank_name) return res.status(400).json({ error: "username and bank_name required" });

  try {
    await pool.query(
      "INSERT INTO admin_banks (username, bank_name) VALUES ($1, $2) ON CONFLICT DO NOTHING",
      [username.trim().toUpperCase(), bank_name.trim()]
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
      [username.trim().toUpperCase()]
    );
    res.json(result.rows.map(r => r.bank_name));
  } catch (err) {
    console.error("getBankAccounts error:", err);
    res.status(500).json({ error: err.message });
  }
};

// ────────────────────────────────────────────────
// Record Payment
// ────────────────────────────────────────────────

exports.recordPayment = async (req, res) => {
  const {
    booking_id,
    amount_paid,
    payment_method,
    transaction_date = new Date().toISOString().split('T')[0],
    bank_name,
    admin_id
  } = req.body;

  if (!booking_id || !amount_paid || !payment_method || !admin_id) {
    return res.status(400).json({ error: 'booking_id, amount_paid, payment_method, admin_id required' });
  }

  try {
    const result = await pool.query(
      `INSERT INTO public.payments (
         booking_id, amount_paid, payment_method,
         bank_name, transaction_date, admin_id
       ) VALUES ($1, $2, $3, $4, $5, $6)
       RETURNING id, transaction_date`,
      [booking_id, parseFloat(amount_paid), payment_method.trim(), bank_name?.trim() || null, transaction_date, admin_id]
    );

    res.json({
      success: true,
      payment_id: result.rows[0].id,
      message: "Payment recorded"
    });
  } catch (err) {
    console.error('recordPayment error:', err);
    res.status(500).json({ error: err.message || 'Failed to record payment' });
  }
};

exports.getCustomerLedger = async (req, res) => {
  const { customer_name, from_date, to_date } = req.query;

  if (!customer_name?.trim()) {
    return res.status(400).json({ error: "customer_name is required" });
  }

  const searchName = `%${customer_name.trim().toLowerCase()}%`;
  const startDate = from_date || '2020-01-01';
  const endDate   = to_date   || '2030-12-31';

  try {
    const result = await pool.query(`
      WITH events AS (
        -- Invoices (bookings) → increases receivable
        SELECT
          b.created_at AS event_date,
          'INVOICE' AS event_type,
          b.bill_no AS ref,
          b.customer_name,
          b.net_amount AS amount,
          0 AS paid_amount,
          b.net_amount AS balance_impact,
          NULL AS payment_method,
          NULL AS bank_name,
          NULL AS admin_username,
          b.id AS booking_id,
          NULL AS payment_id
        FROM bookings b
        WHERE LOWER(b.customer_name) LIKE $1
          AND b.created_at BETWEEN $2 AND $3

        UNION ALL

        -- Payments → decreases receivable
        SELECT
          p.transaction_date::timestamp AS event_date,
          'PAYMENT' AS event_type,
          b.bill_no AS ref,
          b.customer_name,
          0 AS amount,
          p.amount_paid AS paid_amount,
          -p.amount_paid AS balance_impact,
          p.payment_method,
          p.bank_name,
          a.username AS admin_username,
          p.booking_id,
          p.id AS payment_id
        FROM payments p
        JOIN bookings b ON p.booking_id = b.id
        LEFT JOIN admins a ON p.admin_id = a.id
        WHERE LOWER(b.customer_name) LIKE $1
          AND p.transaction_date BETWEEN $2 AND $3

        UNION ALL

        -- Dispatches → informational only (no balance impact)
        SELECT
          dl.dispatched_at AS event_date,
          'DISPATCH' AS event_type,
          b.bill_no AS ref,
          b.customer_name,
          (dl.dispatched_cases * dl.rate_per_box * (1 - COALESCE(dl.discount_percent, 0) / 100)) AS amount,
          0 AS paid_amount,
          0 AS balance_impact,
          NULL AS payment_method,
          dl.transport_name AS bank_name,
          NULL AS admin_username,
          dl.booking_id,
          NULL AS payment_id
        FROM dispatch_logs dl
        JOIN bookings b ON dl.booking_id = b.id
        WHERE LOWER(b.customer_name) LIKE $1
          AND dl.dispatched_at BETWEEN $2 AND $3
      )
      SELECT
        event_date,
        event_type,
        ref,
        customer_name,
        amount,
        paid_amount,
        balance_impact,
        payment_method,
        bank_name,
        admin_username,
        booking_id,
        payment_id
      FROM events
      ORDER BY event_date ASC, event_type ASC, payment_id ASC
    `, [searchName, startDate, endDate]);

    let runningBalance = 0;
    const transactions = result.rows.map(row => {
      runningBalance += Number(row.balance_impact || 0);
      return {
        ...row,
        amount: parseFloat(row.amount || 0),
        paid_amount: parseFloat(row.paid_amount || 0),
        running_balance: parseFloat(runningBalance.toFixed(2)),
      };
    });

    // SAFE SUMMARY - using correlated subqueries to avoid duplication
    const summaryResult = await pool.query(`
      SELECT 
        (SELECT COALESCE(SUM(net_amount), 0) 
         FROM bookings 
         WHERE LOWER(customer_name) LIKE $1 
           AND created_at BETWEEN $2 AND $3) AS total_invoiced,

        (SELECT COALESCE(SUM(
           dl.dispatched_cases * dl.rate_per_box * (1 - COALESCE(dl.discount_percent, 0) / 100)
         ), 0) 
         FROM dispatch_logs dl 
         JOIN bookings b ON dl.booking_id = b.id
         WHERE LOWER(b.customer_name) LIKE $1 
           AND b.created_at BETWEEN $2 AND $3) AS total_dispatched,

        (SELECT COALESCE(SUM(p.amount_paid), 0) 
         FROM payments p 
         JOIN bookings b ON p.booking_id = b.id
         WHERE LOWER(b.customer_name) LIKE $1 
           AND p.transaction_date BETWEEN $2 AND $3) AS total_paid
    `, [searchName, startDate, endDate]);

    const totals = summaryResult.rows[0] || { total_invoiced: 0, total_dispatched: 0, total_paid: 0 };

    const totalInvoiced   = Number(totals.total_invoiced);
    const totalPaid       = Number(totals.total_paid);
    const totalDispatched = Number(totals.total_dispatched);
    const currentOutstanding = totalInvoiced - totalPaid;

    res.json({
      customer_name: customer_name.trim(),
      period: { from: startDate, to: endDate },
      summary: {
        total_invoiced:   totalInvoiced,
        total_dispatched: totalDispatched,
        total_paid:       totalPaid,
        current_outstanding: Number(currentOutstanding.toFixed(2)),
      },
      transactions
    });
  } catch (err) {
    console.error("getCustomerLedger error:", err);
    res.status(500).json({ error: err.message || "Failed to generate ledger" });
  }
};

exports.getOutstandingCustomers = async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        b.customer_name,
        b.customer_gstin,
        b.customer_place,
        COUNT(DISTINCT b.id) AS bill_count,
        COALESCE(SUM(b.net_amount), 0) AS total_invoiced,
        COALESCE(SUM(dl.amount), 0)    AS total_dispatched,
        COALESCE(SUM(p.amount_paid), 0) AS total_collected,
        (COALESCE(SUM(dl.amount), 0) - COALESCE(SUM(p.amount_paid), 0)) AS outstanding
      FROM bookings b
      LEFT JOIN payments p ON p.booking_id = b.id
      LEFT JOIN (
        SELECT booking_id, SUM(amount) amount
        FROM dispatch_logs
        GROUP BY booking_id
      ) dl ON dl.booking_id = b.id
      GROUP BY b.customer_name, b.customer_gstin, b.customer_place
      HAVING (COALESCE(SUM(dl.amount), 0) - COALESCE(SUM(p.amount_paid), 0)) > 1
      ORDER BY outstanding DESC
    `);

    res.json(result.rows.map(r => ({
      customer_name: r.customer_name,
      gstin: r.customer_gstin,
      place: r.customer_place,
      bill_count: Number(r.bill_count),
      total_invoiced: parseFloat(r.total_invoiced),
      total_dispatched: parseFloat(r.total_dispatched),
      total_collected: parseFloat(r.total_collected),
      outstanding: parseFloat(r.outstanding),
    })));
  } catch (err) {
    console.error("getOutstandingCustomers error:", err);
    res.status(500).json({ error: err.message });
  }
};

exports.getPending = async (req, res) => {
  try {
    const query = `
      SELECT
        b.id,
        b.bill_no                  AS bill_number,
        b.customer_name,
        b.created_at               AS bill_date,          -- or use actual bill_date if you have it
        b.net_amount               AS total_net_amount,   -- full invoice incl. all taxes

        COALESCE(SUM(p.amount_paid), 0)::NUMERIC AS total_paid,

        -- Outstanding = full net amount - payments received
        GREATEST(0, 
          b.net_amount - COALESCE(SUM(p.amount_paid), 0)
        )::NUMERIC AS balance

      FROM public.bookings b
      LEFT JOIN payments p ON p.booking_id = b.id

      GROUP BY 
        b.id, b.bill_no, b.customer_name, 
        b.net_amount, b.created_at

      HAVING 
        GREATEST(0, b.net_amount - COALESCE(SUM(p.amount_paid), 0)) > 0

      ORDER BY b.created_at DESC
    `;

    const { rows } = await pool.query(query);

    res.json(rows.map(r => ({
      id:               r.id,
      bill_number:      r.bill_number,
      customer_name:    r.customer_name,
      bill_date:        r.bill_date,
      total_net_amount: parseFloat(r.total_net_amount || 0),
      total_paid:       parseFloat(r.total_paid || 0),
      balance:          Number(r.balance).toFixed(2) * 1,   // safe number with 2 decimals
    })));
  } catch (err) {
    console.error('getPending error:', err);
    res.status(500).json({ error: err.message || "Failed to fetch pending payments" });
  }
};

// ────────────────────────────────────────────────
// Keep your original detailed booking view if needed
// (you can rename or keep as getsBookings / getBookingsWithDetails)
// ────────────────────────────────────────────────

exports.getBookingsWithDetails = async (req, res) => {
  try {
    const query = `
      SELECT
        b.id,
        b.bill_no,
        b.customer_name,
        b.customer_address,
        b.customer_gstin,
        b.customer_place,
        b.customer_state_code,
        b.through,
        b.destination,
        b.no_of_cases,
        b.subtotal,
        b.packing_amount,
        b.extra_amount,
        b.cgst_amount,
        b.sgst_amount,
        b.igst_amount,
        b.net_amount,
        b.items,
        b.created_at,

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
              'transport_name', dl.transport_name,
              'lr_number', dl.lr_number
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

      FROM bookings b
      LEFT JOIN payments p ON p.booking_id = b.id
      GROUP BY b.id
      ORDER BY b.created_at DESC
    `;

    const { rows } = await pool.query(query);

    const result = rows.map(r => ({
      ...r,
      net_amount: parseFloat(r.net_amount || 0),
      dispatched_total: parseFloat(r.dispatched_total || 0),
      paid: parseFloat(r.paid || 0),
      balance: parseFloat((r.dispatched_total - r.paid).toFixed(2)),

      items: typeof r.items === "string" ? JSON.parse(r.items || "[]") : r.items || [],
      dispatch_logs: r.dispatch_logs || [],
      payments: r.payments || [],
      admin_username: r.admin_username || null
    }));

    res.json(result);
  } catch (err) {
    console.error("getBookingsWithDetails error:", err);
    res.status(500).json({ error: err.message });
  }
};

exports.getPaymentsByBooking = async (req, res) => {
  const { id } = req.params; // id = booking_id

  try {
    const result = await pool.query(`
      SELECT 
        p.id,
        p.amount_paid,
        p.payment_method,
        p.bank_name,
        p.transaction_date,
        p.admin_id,
        a.username AS admin_username
      FROM payments p
      LEFT JOIN admins a ON p.admin_id = a.id
      WHERE p.booking_id = $1
      ORDER BY p.transaction_date DESC
    `, [id]);

    res.json(result.rows);
  } catch (err) {
    console.error('getPaymentsByBooking error:', err);
    res.status(500).json({ error: err.message || 'Failed to fetch payment history' });
  }
};