// controllers/Booking.controller.js
const { Pool } = require('pg');
const pool = new Pool({
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  host: process.env.PGHOST,
  port: process.env.PGPORT,
  database: process.env.PGDATABASEW,
});

const getCompanyInitials = (name) => {
  if (!name) return 'XX';
  return name
    .trim()
    .split(/\s+/)
    .map(word => word.charAt(0).toUpperCase())
    .slice(0, 2)
    .join('');
};

// GET latest bill number for a company prefix (e.g. NT, GC)
exports.getLatestBillNo = async (req, res) => {
  try {
    const { prefix } = req.query;
    if (!prefix) return res.status(400).json({ message: 'Prefix required' });

    const cleanPrefix = prefix.trim().toUpperCase();

    // Extract numeric part and find the MAX number used with this prefix
    const result = await pool.query(`
      SELECT bill_no
      FROM public.bookings
      WHERE bill_no ~ $1
      ORDER BY 
        (regexp_matches(bill_no, '^' || $2 || '-(\\d+)$'))[1]::INTEGER DESC
      LIMIT 1
    `, [
      `^${cleanPrefix}-\\d+$`,           // Exact match: starts with prefix + dash + digits only
      cleanPrefix                         // Safe for regex
    ]);

    let nextNumber = 1;

    if (result.rows.length > 0) {
      const lastBill = result.rows[0].bill_no;
      const match = lastBill.match(new RegExp(`^${cleanPrefix}-(\\d+)$`, 'i'));
      if (match) {
        nextNumber = parseInt(match[1], 10) + 1;
      }
    }

    const nextBillNo = `${cleanPrefix}-${String(nextNumber).padStart(3, '0')}`;
    res.json({ bill_no: nextBillNo });

  } catch (err) {
    console.error('Error in getLatestBillNo:', err);
    res.status(500).json({ message: 'Failed to get next bill number', error: err.message });
  }
};

exports.checkBillNoExists = async (req, res) => {
  try {
    const { bill_no } = req.query;
    if (!bill_no) return res.status(400).json({ exists: false });

    const cleaned = bill_no.trim().toUpperCase();
    const result = await pool.query('SELECT 1 FROM public.bookings WHERE UPPER(bill_no) = $1', [cleaned]);

    res.json({ exists: result.rows.length > 0 });
  } catch (err) {
    console.error(err);
    res.status(500).json({ exists: false });
  }
};

// NEW: Get recent customers (for autocomplete)
exports.getRecentCustomers = async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT DISTINCT ON (customer_name) 
        customer_name, 
        customer_address, 
        customer_gstin, 
        customer_place,
        customer_state_code
      FROM bookings 
      WHERE customer_name IS NOT NULL AND customer_name != ''
      ORDER BY customer_name, created_at DESC
      LIMIT 50
    `);
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Failed to fetch customers' });
  }
};

// Updated createBooking to save PDF + proper bill_no
exports.createBooking = async (req, res) => {
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    const {
      customer_name,
      customer_address = '',
      customer_gstin = '',
      customer_place = '',
      customer_state_code = '33',
      through = 'DIRECT',
      destination = '',
      items,
      subtotal = 0,
      packing_amount = 0,
      extra_amount = 0,
      cgst_amount = 0,
      sgst_amount = 0,
      igst_amount = 0,
      net_amount = 0,
      bill_no: providedBillNo = '',
      company_name: companyName = 'NISHA TRADERS'
    } = req.body || {};

    if (!customer_name || !items) {
      return res.status(400).json({ message: 'Customer name and items required' });
    }

    let finalBillNo = providedBillNo.trim().toUpperCase();

    if (!finalBillNo) {
      const prefix = getCompanyInitials(companyName);
      const latestRes = await client.query(`
        SELECT bill_no FROM bookings 
        WHERE bill_no ILIKE $1 
        ORDER BY LENGTH(bill_no) DESC, bill_no DESC LIMIT 1
      `, [`${prefix}-%`]);

      let nextNum = 1;
      if (latestRes.rows.length > 0) {
        const match = latestRes.rows[0].bill_no.match(new RegExp(`^${prefix}-(\\d+)$`, 'i'));
        if (match) nextNum = parseInt(match[1]) + 1;
      }
      finalBillNo = `${prefix}-${String(nextNum).padStart(3, '0')}`;
    }

    const dupCheck = await client.query('SELECT id FROM public.bookings WHERE UPPER(bill_no) = $1', [finalBillNo]);
    if (dupCheck.rows.length > 0) {
      await client.query('ROLLBACK');
      return res.status(409).json({
        message: 'This bill number already exists!',
        used_bill_no: finalBillNo
      });
    }

    let itemsArray;
    try {
      itemsArray = typeof items === 'string' ? JSON.parse(items) : items;
    } catch (e) {
      await client.query('ROLLBACK');
      return res.status(400).json({ message: 'Invalid items JSON' });
    }

    const totalCases = itemsArray.reduce((sum, item) => sum + (parseInt(item.cases) || 0), 0);

    // INSERT WITHOUT pdf_data
    const insertQuery = `
      INSERT INTO public.bookings (
        bill_no, customer_name, customer_address, customer_gstin, customer_place,
        customer_state_code, through, destination, no_of_cases, subtotal, packing_amount,
        extra_amount, cgst_amount, sgst_amount, igst_amount, net_amount, items, company_name, created_at
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,NOW())
      RETURNING id, bill_no, created_at
    `;

    const values = [
      finalBillNo, customer_name, customer_address, customer_gstin, customer_place,
      customer_state_code, through, destination || '', totalCases,
      parseFloat(subtotal), parseFloat(packing_amount), parseFloat(extra_amount),
      parseFloat(cgst_amount), parseFloat(sgst_amount), parseFloat(igst_amount),
      parseFloat(net_amount), JSON.stringify(itemsArray), companyName  // ← Added
    ];

    const result = await client.query(insertQuery, values);
    await client.query('COMMIT');

    res.json({
      message: 'Bill saved successfully!',
      booking: result.rows[0]
    });

  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Create Booking Error:', err);
    res.status(500).json({ error: err.message || 'Server error' });
  } finally {
    client.release();
  }
};

// GET ALL BOOKINGS
exports.getAllBookings = async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT id, bill_no, customer_name, customer_address, customer_gstin, 
             customer_place, customer_state_code, through, destination, no_of_cases,
             subtotal, packing_amount, extra_amount, cgst_amount, sgst_amount,
             igst_amount, net_amount, items, created_at
      FROM public.bookings 
      ORDER BY created_at DESC
    `);
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Failed to fetch bills' });
  }
};

// GET SINGLE BILL (for printing)
exports.getBookingById = async (req, res) => {
  try {
    const { id } = req.params;
    const result = await pool.query('SELECT * FROM bookings WHERE id = $1', [id]);
    if (!result.rows.length) return res.status(404).json({ message: 'Bill not found' });

    res.json(result.rows[0]);
  } catch (err) {
    res.status(500).json({ message: 'Server error' });
  }
};

exports.getStatesForSupply = async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT code, state_name 
      FROM public.codestate 
      ORDER BY state_name ASC
    `);

    res.status(200).json(result.rows);
  } catch (err) {
    console.error('Error fetching states:', err);
    res.status(500).json({ error: 'Failed to fetch states' });
  }
};
