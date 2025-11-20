// controllers/Booking.controller.js
const { Pool } = require('pg');
const PDFDocument = require('pdfkit');
const fs = require('fs');
const path = require('path');

const pool = new Pool({
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  host: process.env.PGHOST,
  port: process.env.PGPORT,
  database: process.env.PGDATABASE,
});

const generateBillNumber = async () => {
  const result = await pool.query('SELECT COUNT(*) as count FROM public.book');
  return `BILL-${String(parseInt(result.rows[0].count, 10) + 1).padStart(3, '0')}`;
};

const formatDate = (dateStr) => {
  const [y, m, d] = dateStr.split('-');
  return `${d}/${m}/${y}`;
};

exports.createBooking = async (req, res) => {
  const client = await pool.connect();
  try {
    // ── 1. Extract ALL fields (including apply_* flags) ──
    const {
      customer_name, address, gstin, lr_number, agent_name,
      from: fromLoc, to: toLoc, through,
      additional_discount = 0,
      packing_percent = 3.0,
      taxable_value,
      stock_from,
      items = [],
      apply_processing_fee = false,
      apply_cgst = false,
      apply_sgst = false,
      apply_igst = false
    } = req.body;

    if (!customer_name || !items.length || !fromLoc || !toLoc || !through) {
      return res.status(400).json({ message: 'Missing required fields' });
    }

    await client.query('BEGIN');

    const bill_number = await generateBillNumber();
    const bill_date = new Date().toISOString().split('T')[0];

    let subtotal = 0;
    let totalCases = 0;
    const processedItems = [];

    // ── 2. Process items (stock reduce + history) ──
    for (const [idx, item] of items.entries()) {
      const {
        id: stock_id,
        productname, brand,
        cases, per_case, discount_percent = 0, godown, rate_per_box
      } = item;

      if (!stock_id || !productname || !brand || !cases || !per_case || rate_per_box === undefined) {
        throw new Error(`Invalid item at index ${idx}: Missing stock_id or data`);
      }

      const stockCheck = await client.query(
        'SELECT current_cases, per_case, taken_cases FROM public.stock WHERE id = $1 FOR UPDATE',
        [stock_id]
      );

      if (stockCheck.rows.length === 0) {
        throw new Error(`Stock entry not found for ID: ${stock_id}`);
      }

      const { current_cases, taken_cases } = stockCheck.rows[0];
      if (cases > current_cases) {
        throw new Error(`Insufficient stock: ${productname} (Available: ${current_cases}, Requested: ${cases})`);
      }

      const qty = cases * per_case;
      const amountBefore = qty * rate_per_box;
      const discountAmt = amountBefore * (discount_percent / 100);
      const finalAmt = amountBefore - discountAmt;

      subtotal += finalAmt;
      totalCases += cases;

      const newCases = current_cases - cases;
      const newTakenCases = (taken_cases || 0) + cases;

      await client.query(
        'UPDATE public.stock SET current_cases = $1, taken_cases = $2, last_taken_date = CURRENT_TIMESTAMP WHERE id = $3',
        [newCases, newTakenCases, stock_id]
      );

      await client.query(
        `INSERT INTO public.stock_history 
         (stock_id, action, cases, per_case_total, date, customer_name) 
         VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP, $5)`,
        [stock_id, 'taken', cases, cases * per_case, customer_name]
      );

      processedItems.push({
        s_no: idx + 1,
        productname,
        brand,
        cases,
        per_case,
        quantity: qty,
        rate_per_box,
        discount_percent: parseFloat(discount_percent),
        amount: parseFloat(finalAmt.toFixed(2)),
        godown: godown || stock_from
      });
    }

    // ── 3. CALCULATE TOTALS (WITH TAX LOGIC) ──
    const packingCharges = apply_processing_fee ? subtotal * (packing_percent / 100) : 0;
    const subtotalWithPacking = subtotal + packingCharges;

    // Taxable value = subtotalWithPacking + user-added taxable_value
    const userTaxable = taxable_value ? parseFloat(taxable_value) : 0;
    const taxableUsed = subtotalWithPacking + userTaxable;

    const addlDiscountAmt = taxableUsed * (additional_discount / 100);
    const netBeforeTax = taxableUsed - addlDiscountAmt;

    let cgstAmt = 0, sgstAmt = 0, igstAmt = 0;

    // ── TAX LOGIC: IGST overrides CGST+SGST ──
    if (apply_igst) {
      igstAmt = netBeforeTax * 0.18;
    } else if (apply_cgst && apply_sgst) {
      cgstAmt = netBeforeTax * 0.09;
      sgstAmt = netBeforeTax * 0.09;
    }
    // If only one of CGST/SGST is true → ignore (invalid)

    const totalTax = cgstAmt + sgstAmt + igstAmt;
    const grandTotal = Math.round(netBeforeTax + totalTax);
    const roundOff = grandTotal - (netBeforeTax + totalTax);

    // ── 4. GENERATE PDF ──
    const pdfFileName = `bill_${bill_number}.pdf`;
    const pdfPath = path.join(__dirname, '..', 'uploads', 'pdfs', pdfFileName);
    fs.mkdirSync(path.dirname(pdfPath), { recursive: true });

    await generatePDF({
      bill_number, bill_date, customer_name, address, gstin, lr_number, agent_name,
      from: fromLoc, to: toLoc, through, items: processedItems,
      subtotal, packingCharges, subtotalWithPacking, taxableUsed, addlDiscountAmt,
      roundOff, grandTotal, totalCases, stock_from, packing_percent,
      cgstAmt, sgstAmt, igstAmt
    }, pdfPath);

    const relativePdfPath = `/uploads/pdfs/${pdfFileName}`;

    // ── 5. SAVE TO DB (extra_charges includes all) ──
    await client.query(
      `INSERT INTO public.book (
        bill_number, bill_date, customer_name, address, gstin, lr_number, agent_name,
        "from", "to", "through", stock_from, pdf_path, items, total, extra_charges
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`,
      [
        bill_number, bill_date, customer_name, address, gstin, lr_number, agent_name,
        fromLoc, toLoc, through, stock_from, relativePdfPath,
        JSON.stringify(processedItems), grandTotal,
        JSON.stringify({
          packing_percent: parseFloat(packing_percent) || 0,
          additional_discount: parseFloat(additional_discount) || 0,
          taxable_value: userTaxable,
          apply_processing_fee,
          apply_cgst,
          apply_sgst,
          apply_igst
        })
      ]
    );

    await client.query('COMMIT');
    res.json({ message: 'Booking created', bill_number, pdfPath: relativePdfPath });
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Booking Error:', err.message);
    res.status(500).json({ message: err.message });
  } finally {
    client.release();
  }
};

// ── PDF GENERATOR (updated to show CGST/SGST/IGST) ──
const generatePDF = (data, outputPath) => {
  return new Promise((resolve, reject) => {
    const doc = new PDFDocument({ margin: 40, size: 'A4' });
    const stream = fs.createWriteStream(outputPath);
    doc.pipe(stream);

    // === TITLE ===
    doc.fontSize(16).font('Helvetica-Bold').text('Estimate', { align: 'center' }).moveDown(1.5);

    const leftX = 50;
    const rightX = 350;
    const tableStartX = leftX;
    const tableWidth = 490;
    const colWidths = [35, 130, 45, 45, 55, 65, 65, 50];
    const rowHeight = 20;
    const cellPadding = 4;

    // === CUSTOMER & BILL INFO ===
    const startY = 100;
    doc.font('Helvetica-Bold').fontSize(10);
    doc.text('Customer Information', leftX, startY);
    doc.font('Helvetica').fontSize(10);
    doc.text(`Party Name : ${data.customer_name || ''}`, leftX, startY + 15);
    doc.text(`Address    : ${data.address || ''}`, leftX, startY + 30);
    doc.text(`GSTIN      : ${data.gstin || ''}`, leftX, startY + 45);

    doc.font('Helvetica-Bold').fontSize(10);
    doc.text('Bill Details', rightX, startY);
    doc.font('Helvetica').fontSize(10);
    doc.text(`Bill NO     : ${data.bill_number}`, rightX, startY + 15);
    doc.text(`Bill DATE   : ${formatDate(data.bill_date)}`, rightX, startY + 30);
    doc.text(`Agent Name : ${data.agent_name || 'DIRECT'}`, rightX, startY + 45);
    doc.text(`L.R. NUMBER : ${data.lr_number || ''}`, rightX, startY + 60);
    doc.text(`No. of Cases : ${data.totalCases}`, rightX, startY + 75).fontSize(15);

    // === TABLE ===
    let y = startY + 90;
    const headers = ['S.No', 'Product', 'Case', 'Per', 'Qty', 'Rate', 'Amount', 'From'];
    const verticalLines = [tableStartX];
    colWidths.forEach(w => verticalLines.push(verticalLines[verticalLines.length - 1] + w));
    let x = tableStartX;

    // Header
    const headerTop = y;
    const headerBottom = y + rowHeight;
    doc.lineWidth(0.8).strokeColor('black');
    doc.moveTo(tableStartX, headerTop).lineTo(tableStartX + tableWidth, headerTop).stroke();
    doc.moveTo(tableStartX, headerBottom).lineTo(tableStartX + tableWidth, headerBottom).stroke();
    verticalLines.forEach(vx => {
      doc.moveTo(vx, headerTop).lineTo(vx, headerBottom).stroke();
    });
    doc.font('Helvetica-Bold').fontSize(9);
    headers.forEach((h, i) => {
      doc.text(h, x + cellPadding, y + cellPadding, {
        width: colWidths[i] - 2 * cellPadding,
        align: 'center'
      });
      x += colWidths[i];
    });
    y += rowHeight + 1;

    // Rows
    doc.font('Helvetica').fontSize(9);
    data.items.forEach((item) => {
      x = tableStartX;
      const row = [
        item.s_no.toString(),
        item.productname,
        item.cases.toString(),
        item.per_case.toString(),
        item.quantity.toString(),
        `${item.rate_per_box.toFixed(2)}`,
        item.amount.toFixed(2),
        item.godown
      ];
      const rowTop = y;
      const rowBottom = y + rowHeight;
      doc.lineWidth(0.4).strokeColor('black');
      doc.moveTo(tableStartX, rowTop).lineTo(tableStartX + tableWidth, rowTop).stroke();
      doc.moveTo(tableStartX, rowBottom).lineTo(tableStartX + tableWidth, rowBottom).stroke();
      verticalLines.forEach(vx => {
        doc.moveTo(vx, rowTop).lineTo(vx, rowBottom).stroke();
      });
      row.forEach((text, i) => {
        doc.text(text, x + cellPadding, y + cellPadding, {
          width: colWidths[i] - 2 * cellPadding,
          align: 'center'
        });
        x += colWidths[i];
      });
      y += rowHeight + 1;
    });

    // Bottom border
    doc.lineWidth(0.8)
       .moveTo(tableStartX, y - 1)
       .lineTo(tableStartX + tableWidth, y - 1)
       .strokeColor('black')
       .stroke();

    // === TOTALS ===
    y += 15;
    const transportStartY = y;
    doc.font('Helvetica-Bold').fontSize(10);
    doc.text('Transport Details', leftX, transportStartY);
    doc.font('Helvetica').fontSize(10);
    doc.text(`From         : ${data.from}`, leftX, transportStartY + 15);
    doc.text(`To           : ${data.to}`, leftX, transportStartY + 30);
    doc.text(`Through      : ${data.through}`, leftX, transportStartY + 45);

    const totals = [
      ['GOODS VALUE', data.subtotal.toFixed(2)],
      ['SPECIAL DISCOUNT', `-${data.addlDiscountAmt.toFixed(2)}`],
      ['SUB TOTAL', data.subtotal.toFixed(2)],
      [`PACKING @ ${data.packing_percent}%`, data.packingCharges.toFixed(2)],
      ['SUB TOTAL', data.subtotalWithPacking.toFixed(2)],
      ['TAXABLE VALUE', data.taxableUsed.toFixed(2)],
      ...(data.apply_cgst ? [['CGST @ 9%', data.cgstAmt.toFixed(2)]] : []),
      ...(data.apply_sgst ? [['SGST @ 9%', data.sgstAmt.toFixed(2)]] : []),
      ...(data.apply_igst ? [['IGST @ 18%', data.igstAmt.toFixed(2)]] : []),
      ['ROUND OFF', data.roundOff.toFixed(2)],
      [''],
    ];

    let ty = transportStartY;
    const labelX = rightX;
    const valueX = rightX + 110;
    const valueWidth = 70;

    doc.font('Helvetica').fontSize(10);
    totals.forEach(([label, value]) => {
      const lineY = ty + 15;
      doc.text(label, labelX, lineY, { align: 'left' });
      doc.text(value, valueX, lineY, { width: valueWidth, align: 'right' });
      ty += 15;
    });

    // NET AMOUNT
    const netY = ty;
    doc.font('Helvetica-Bold').fontSize(11)
       .text('NET AMOUNT', labelX, netY)
       .text(data.grandTotal.toFixed(2), valueX, netY, { width: valueWidth, align: 'right' });

    // FOOTER
    y = Math.max(y, ty) + 35;
    doc.fontSize(8).font('Helvetica')
       .text('Note:', leftX, y)
       .text('1. Company not responsible for transit loss/damage', leftX + 10, y + 12)
       .text('2. Subject to Sivakasi jurisdiction. E.& O.E', leftX + 10, y + 24);

    doc.end();
    stream.on('finish', resolve);
    stream.on('error', reject);
  });
};

// ── OTHER EXPORTS (unchanged) ──
exports.getBookings = async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT id, bill_number, bill_date, customer_name, "from", "to", items, pdf_path
      FROM public.book ORDER BY created_at DESC
    `);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ message: 'Failed to fetch bookings' });
  }
};

exports.getCustomers = async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT DISTINCT ON (customer_name)
        customer_name, address, gstin, lr_number, agent_name, "from", "to", "through"
      FROM public.book
      WHERE customer_name IS NOT NULL AND customer_name != ''
      ORDER BY customer_name, created_at DESC
    `);

    const customers = result.rows.map(row => ({
      label: row.customer_name,
      value: {
        name: row.customer_name,
        address: row.address || '',
        gstin: row.gstin || '',
        lr_number: row.lr_number || '',
        agent_name: row.agent_name || '',
        from: row.from || '',
        to: row.to || '',
        through: row.through || ''
      }
    }));

    res.json(customers);
  } catch (err) {
    console.error('Get Customers Error:', err);
    res.status(500).json({ message: 'Failed to fetch customers' });
  }
};

exports.searchProductsGlobal = async (req, res) => {
  const { name } = req.query;
  const searchTerm = `%${name.trim().toLowerCase()}%`;

  try {
    const godownsRes = await pool.query(`SELECT id, name FROM public.godown`);
    const godowns = godownsRes.rows;
    const allResults = [];

    for (const godown of godowns) {
      const godownId = godown.id;
      const typesRes = await pool.query(`
        SELECT DISTINCT product_type
        FROM public.stock 
        WHERE godown_id = $1 AND current_cases > 0
          AND (LOWER(productname) LIKE $2 OR LOWER(brand) LIKE $2)
      `, [godownId, searchTerm]);

      if (typesRes.rows.length === 0) continue;

      const productTypes = typesRes.rows.map(r => r.product_type);
      let joins = '';
      const params = [godownId, searchTerm];
      let idx = 3;

      productTypes.forEach(type => {
        const table = type.toLowerCase().replace(/\s+/g, '_');
        joins += `
          LEFT JOIN public."${table}" p${idx}
            ON LOWER(s.productname) = LOWER(p${idx}.productname)
            AND LOWER(s.brand) = LOWER(p${idx}.brand)
        `;
        idx++;
      });

      const finalQuery = `
        SELECT 
          s.id,
          s.product_type,
          s.productname,
          s.brand,
          s.per_case,
          s.current_cases,
          COALESCE(
            ${productTypes.map((_, i) => `CAST(p${i + 3}.price AS NUMERIC)`).join(', ')}, 
            0
          )::NUMERIC AS rate_per_box,
          $1::INTEGER AS godown_id,
          '${godown.name}' AS godown_name
        FROM public.stock s
        ${joins}
        WHERE s.godown_id = $1 
          AND s.current_cases > 0
          AND (LOWER(s.productname) LIKE $2 OR LOWER(s.brand) LIKE $2)
        ORDER BY s.product_type, s.productname
      `;

      const result = await pool.query(finalQuery, params);
      allResults.push(...result.rows);
    }

    res.json(allResults);
  } catch (err) {
    console.error('searchProductsGlobal:', err.message);
    res.status(500).json({ message: 'Search failed' });
  }
};