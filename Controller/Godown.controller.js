// src/controllers/godownController.js
const { Pool } = require('pg');
const ExcelJS = require('exceljs');

const pool = new Pool({
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  host: process.env.PGHOST,
  port: process.env.PGPORT,
  database: process.env.PGDATABASE,
  max: 30,
});

/* ---------- CACHES (shared with inventory) ---------- */
let productTypeCache = { data: null, timestamp: 0 };
let brandCache = { data: null, timestamp: 0 };

async function getCachedProductTypes() {
  const now = Date.now();
  if (!productTypeCache.data || now - productTypeCache.timestamp > 300000) {
    const client = await pool.connect();
    try {
      const res = await client.query('SELECT product_type FROM public.products');
      productTypeCache = { data: res.rows.map(r => r.product_type), timestamp: now };
    } finally {
      client.release();
    }
  }
  return productTypeCache.data;
}

async function getCachedBrands() {
  const now = Date.now();
  if (!brandCache.data || now - brandCache.timestamp > 300000) {
    const client = await pool.connect();
    try {
      const res = await client.query('SELECT id, name, agent_name FROM public.brand');
      brandCache = { data: res.rows, timestamp: now };
    } finally {
      client.release();
    }
  }
  return brandCache.data;
}

/* ---------- GODOWN CRUD ---------- */
exports.addGodown = async (req, res) => {
  try {
    const { name } = req.body;
    if (!name) return res.status(400).json({ message: 'Godown name is required' });

    const formatted = name.toLowerCase().replace(/\s+/g, '_');
    await pool.query(`
      CREATE TABLE IF NOT EXISTS public.godown (
        id BIGSERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL UNIQUE
      )
    `);

    const exists = await pool.query('SELECT 1 FROM public.godown WHERE name = $1', [formatted]);
    if (exists.rows.length) return res.status(400).json({ message: 'Godown already exists' });

    const { rows } = await pool.query('INSERT INTO public.godown (name) VALUES ($1) RETURNING id', [formatted]);
    res.status(201).json({ message: 'Godown created', id: rows[0].id });
  } catch (e) {
    console.error(e);
    res.status(500).json({ message: 'Failed to create godown' });
  }
};

exports.getGodowns = async (req, res) => {
  try {
    const { rows: godowns } = await pool.query('SELECT id, name FROM public.godown ORDER BY name');

    for (const g of godowns) {
      const { rows: stocks } = await pool.query(
        `SELECT s.id, s.product_type, s.productname, s.brand,
                s.current_cases, s.per_case, s.date_added,
                s.last_taken_date, s.taken_cases,
                COALESCE(b.agent_name,'-') AS agent_name
         FROM public.stock s
         LEFT JOIN public.brand b ON s.brand = b.name
         WHERE s.godown_id = $1
         ORDER BY s.productname`,
        [g.id]
      );
      g.stocks = stocks;
    }
    res.json(godowns);
  } catch (e) {
    console.error(e);
    res.status(500).json({ message: 'Failed to fetch godowns' });
  }
};

exports.deleteGodown = async (req, res) => {
  try {
    const { id } = req.params;
    const { rowCount } = await pool.query('DELETE FROM public.godown WHERE id = $1', [id]);
    if (!rowCount) return res.status(404).json({ message: 'Godown not found' });
    res.json({ message: 'Godown deleted' });
  } catch (e) {
    console.error(e);
    res.status(500).json({ message: 'Failed to delete godown' });
  }
};

/* ---------- STOCK OPERATIONS ---------- */
exports.addStockToGodown = async (req, res) => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const { godown_id, product_type, productname, brand, cases_added } = req.body;
    if (!godown_id || !product_type || !productname || !brand || !cases_added)
      return res.status(400).json({ message: 'All fields required' });

    const cases = parseInt(cases_added, 10);
    if (isNaN(cases) || cases <= 0) return res.status(400).json({ message: 'Cases must be >0' });

    const godown = await client.query('SELECT 1 FROM public.godown WHERE id = $1', [godown_id]);
    if (!godown.rowCount) return res.status(404).json({ message: 'Godown not found' });

    const table = product_type.toLowerCase().replace(/\s+/g, '_');
    const prod = await client.query(
      `SELECT id, per_case, wprice FROM public."${table}" WHERE productname = $1 AND brand = $2`,
      [productname, brand]
    );
    if (!prod.rowCount) return res.status(404).json({ message: 'Product not found' });
    const { per_case, wprice } = prod.rows[0];

    await client.query(`
      CREATE TABLE IF NOT EXISTS public.stock (
        id BIGSERIAL PRIMARY KEY,
        godown_id INTEGER REFERENCES public.godown(id) ON DELETE CASCADE,
        product_type VARCHAR(100) NOT NULL,
        productname VARCHAR(255) NOT NULL,
        brand VARCHAR(100) NOT NULL,
        current_cases INTEGER NOT NULL DEFAULT 0,
        per_case INTEGER NOT NULL,
        date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_taken_date TIMESTAMP,
        taken_cases INTEGER DEFAULT 0,
        brand_id INTEGER REFERENCES public.brand(id),
        CONSTRAINT uq_stock UNIQUE(godown_id,product_type,productname,brand)
      )
    `);

    const brands = await getCachedBrands();
    const brandRec = brands.find(b => b.name.toLowerCase() === brand.toLowerCase());
    const brand_id = brandRec?.id ?? (await client.query(
      'INSERT INTO public.brand (name) VALUES ($1) ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING id',
      [brand.toLowerCase()]
    )).rows[0].id;

    const existing = await client.query(
      'SELECT id, current_cases FROM public.stock WHERE godown_id=$1 AND product_type=$2 AND productname=$3 AND brand=$4',
      [godown_id, product_type, productname, brand]
    );

    let stockId;
    if (existing.rowCount) {
      stockId = existing.rows[0].id;
      const newCases = existing.rows[0].current_cases + cases;
      await client.query(
        'UPDATE public.stock SET current_cases=$1, date_added=CURRENT_TIMESTAMP, brand_id=$2 WHERE id=$3',
        [newCases, brand_id, stockId]
      );
    } else {
      const ins = await client.query(
        `INSERT INTO public.stock
         (godown_id,product_type,productname,brand,brand_id,current_cases,per_case)
         VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id`,
        [godown_id, product_type, productname, brand, brand_id, cases, per_case]
      );
      stockId = ins.rows[0].id;
    }

    await client.query(`
      CREATE TABLE IF NOT EXISTS public.stock_history (
        id BIGSERIAL PRIMARY KEY,
        stock_id INTEGER REFERENCES public.stock(id) ON DELETE CASCADE,
        action VARCHAR(10) CHECK (action IN ('added','taken')),
        cases INTEGER NOT NULL,
        per_case_total INTEGER NOT NULL,
        date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    await client.query(
      'INSERT INTO public.stock_history (stock_id,action,cases,per_case_total) VALUES ($1,$2,$3,$4)',
      [stockId, 'added', cases, cases * per_case]
    );

    await client.query('COMMIT');
    res.status(201).json({ message: 'Stock added', stock_id: stockId });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error(e);
    res.status(500).json({ message: 'Failed to add stock' });
  } finally {
    client.release();
  }
};

exports.getStockByGodown = async (req, res) => {
  const { godown_id } = req.params;
  try {
    const stockRes = await pool.query(
      `SELECT s.id, s.product_type, s.productname, s.brand,
              s.per_case, s.current_cases,
              COALESCE(b.agent_name, '-') AS agent_name,
              g.name AS godown_name
       FROM public.stock s
       JOIN public.godown g ON s.godown_id = g.id
       LEFT JOIN public.brand b ON s.brand = b.name
       WHERE s.godown_id = $1
       ORDER BY s.product_type, s.productname`,
      [godown_id]
    );

    if (stockRes.rows.length === 0) return res.json([]);

    const stocks = stockRes.rows;
    const enrichedStocks = await Promise.all(
      stocks.map(async (s) => {
        let wprice = 0;
        const tableName = s.product_type.toLowerCase().replace(/\s+/g, '_');
        try {
          const hasBrandRes = await pool.query(
            `SELECT column_name FROM information_schema.columns 
             WHERE table_schema = 'public' AND table_name = $1 AND column_name = 'brand'`,
            [tableName]
          );
          const hasBrand = hasBrandRes.rows.length > 0;

          let query = `SELECT wprice FROM public."${tableName}" WHERE LOWER(productname) = LOWER($1)`;
          const params = [s.productname];

          if (hasBrand && s.brand) {
            query += ` AND LOWER(brand) = LOWER($2)`;
            params.push(s.brand);
          }

          query += ` LIMIT 1`;

          const prodRes = await pool.query(query, params);
          if (prodRes.rows.length > 0) {
            wprice = parseFloat(prodRes.rows[0].wprice) || 0;
          }
        } catch (e) {
          console.warn(`wprice fetch failed for ${tableName}/${s.productname}`, e.message);
        }

        return { ...s, wprice: wprice.toFixed(2) };
      })
    );

    res.json(enrichedStocks);
  } catch (e) {
    console.error('getStockByGodown error:', e);
    res.status(500).json({ message: 'Failed to fetch stock' });
  }
};

exports.takeStockFromGodown = async (req, res) => {
  const client = await pool.connect();
  try {
    const { stock_id, cases_taken } = req.body;
    if (!stock_id || !cases_taken || parseInt(cases_taken) <= 0)
      return res.status(400).json({ message: 'Valid stock_id & cases required' });

    await client.query('BEGIN');
    const stock = await client.query(
      'SELECT current_cases, per_case, taken_cases FROM public.stock WHERE id=$1 FOR UPDATE',
      [stock_id]
    );
    if (!stock.rowCount) return res.status(404).json({ message: 'Stock not found' });

    const { current_cases, per_case, taken_cases = 0 } = stock.rows[0];
    const taken = parseInt(cases_taken);
    if (taken > current_cases) return res.status(400).json({ message: 'Insufficient stock' });

    const newCases = current_cases - taken;
    const newTaken = taken_cases + taken;

    await client.query(
      'UPDATE public.stock SET current_cases=$1, taken_cases=$2, last_taken_date=CURRENT_TIMESTAMP WHERE id=$3',
      [newCases, newTaken, stock_id]
    );
    await client.query(
      'INSERT INTO public.stock_history (stock_id,action,cases,per_case_total) VALUES ($1,$2,$3,$4)',
      [stock_id, 'taken', taken, taken * per_case]
    );

    await client.query('COMMIT');
    res.json({ message: 'Stock taken', new_cases: newCases });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error(e);
    res.status(500).json({ message: 'Failed to take stock' });
  } finally {
    client.release();
  }
};

exports.addStockToExisting = async (req, res) => {
  try {
    const { stock_id, cases_added } = req.body;
    if (!stock_id || !cases_added || parseInt(cases_added) <= 0)
      return res.status(400).json({ message: 'Valid data required' });

    const added = parseInt(cases_added);
    const stock = await pool.query(
      'SELECT current_cases, per_case FROM public.stock WHERE id=$1',
      [stock_id]
    );
    if (!stock.rowCount) return res.status(404).json({ message: 'Stock not found' });

    const { current_cases, per_case } = stock.rows[0];
    const newCases = current_cases + added;

    await pool.query(
      'UPDATE public.stock SET current_cases=$1, date_added=CURRENT_TIMESTAMP WHERE id=$2',
      [newCases, stock_id]
    );
    await pool.query(
      'INSERT INTO public.stock_history (stock_id,action,cases,per_case_total) VALUES ($1,$2,$3,$4)',
      [stock_id, 'added', added, added * per_case]
    );

    res.json({ message: 'Stock added', new_cases: newCases });
  } catch (e) {
    console.error(e);
    res.status(500).json({ message: 'Failed to add stock' });
  }
};

exports.getStockHistory = async (req, res) => {
  try {
    const { stock_id } = req.params;
    const { rows } = await pool.query(
      `SELECT h.*, s.productname, s.brand, s.product_type,
              s.per_case * h.cases AS per_case_total,
              COALESCE(b.agent_name,'-') AS agent_name
       FROM public.stock_history h
       JOIN public.stock s ON h.stock_id=s.id
       LEFT JOIN public.brand b ON s.brand=b.name
       WHERE h.stock_id=$1
       ORDER BY h.date DESC`,
      [stock_id]
    );
    res.json(rows);
  } catch (e) {
    console.error(e);
    res.status(500).json({ message: 'Failed to fetch history' });
  }
};

/* ---------- EXCEL EXPORT (uses wprice) ---------- */
exports.exportGodownStockToExcel = async (req, res) => {
  try {
    const { rows: godowns } = await pool.query('SELECT id, name FROM public.godown ORDER BY name');
    const workbook = new ExcelJS.Workbook();
    workbook.creator = 'Admin System';

    for (const g of godowns) {
      const { rows: stocks } = await pool.query(
        `SELECT s.*, g.name AS godown_name,
                COALESCE(b.agent_name,'-') AS agent_name,
                COALESCE(latest.customer_name,'-') AS last_customer_name,
                COALESCE(latest.agent_name,'-') AS last_agent_name,
                COALESCE(p.wprice,0) AS wprice
         FROM public.stock s
         JOIN public.godown g ON s.godown_id=g.id
         LEFT JOIN public.brand b ON s.brand=b.name
         LEFT JOIN LATERAL (
           SELECT h.customer_name, h.agent_name
           FROM public.stock_history h
           WHERE h.stock_id=s.id AND h.action='taken'
           ORDER BY h.date DESC LIMIT 1
         ) latest ON TRUE
         LEFT JOIN public."${g.name.toLowerCase().replace(/\s+/g, '_')}" p
           ON LOWER(s.productname)=LOWER(p.productname)
           AND LOWER(s.brand)=LOWER(p.brand)
         WHERE s.godown_id=$1
         ORDER BY s.productname`,
        [g.id]
      );

      const ws = workbook.addWorksheet(g.name, { properties: { defaultColWidth: 15 } });
      ws.columns = [
        { header: 'Product Type', key: 'product_type', width: 20 },
        { header: 'Product Name', key: 'productname', width: 30 },
        { header: 'Brand', key: 'brand', width: 15 },
        { header: 'Last Customer/Agent', key: 'display_name', width: 22 },
        { header: 'Current Cases', key: 'current_cases', width: 15 },
        { header: 'Per Case', key: 'per_case', width: 10 },
        { header: 'Taken Cases', key: 'taken_cases', width: 15 },
        { header: 'Rate per Box (Wholesale)', key: 'wprice', width: 22 },
        { header: 'Date Added', key: 'date_added', width: 20 },
        { header: 'Last Taken', key: 'last_taken_date', width: 20 },
      ];

      stocks.forEach(r => {
        ws.addRow({
          product_type: r.product_type,
          productname: r.productname,
          brand: r.brand,
          display_name: r.last_customer_name !== '-' ? r.last_customer_name : r.last_agent_name,
          current_cases: r.current_cases,
          per_case: r.per_case,
          taken_cases: r.taken_cases || 0,
          wprice: r.wprice,
          date_added: r.date_added,
          last_taken_date: r.last_taken_date || '',
        });
      });

      ws.getRow(1).font = { bold: true };
      ws.getRow(1).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFCCCCCC' } };
    }

    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', 'attachment; filename=godown_stocks.xlsx');
    await workbook.xlsx.write(res);
    res.end();
  } catch (e) {
    console.error(e);
    res.status(500).json({ message: 'Export failed' });
  }
};

/* ---------- MISC ---------- */
exports.editGodown = async (req, res) => {
  try {
    const { id } = req.params;
    const { name } = req.body;
    if (!name?.trim()) return res.status(400).json({ message: 'Name required' });

    const fmt = name.toLowerCase().trim().replace(/\s+/g, '_');
    const dup = await pool.query('SELECT 1 FROM public.godown WHERE name=$1 AND id!=$2', [fmt, id]);
    if (dup.rowCount) return res.status(400).json({ message: 'Name taken' });

    const { rowCount } = await pool.query('UPDATE public.godown SET name=$1 WHERE id=$2', [fmt, id]);
    if (!rowCount) return res.status(404).json({ message: 'Godown not found' });

    res.json({ message: 'Updated', name: fmt });
  } catch (e) {
    console.error(e);
    res.status(500).json({ message: 'Update failed' });
  }
};

exports.getGodownsFast = async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT g.id, g.name,
             COALESCE(SUM(s.current_cases),0) AS total_cases,
             COUNT(s.id) AS stock_items
      FROM public.godown g
      LEFT JOIN public.stock s ON s.godown_id=g.id
      GROUP BY g.id,g.name
      ORDER BY g.name`);
    res.json(rows);
  } catch (e) {
    console.error(e);
    res.status(500).json({ message: 'Failed' });
  }
};

exports.bulkAllocate = async (req, res) => {
  const client = await pool.connect();
  try {
    const { allocations = [] } = req.body;
    if (!allocations.length) return res.status(400).json({ message: 'No allocations' });

    await client.query('BEGIN');
    const results = [];

    for (const a of allocations) {
      const { godown_id, product_type, productname, brand, per_case, cases_added } = a;
      const cases = parseInt(cases_added, 10);
      if (isNaN(cases) || cases <= 0) continue;

      const fmtBrand = brand.toLowerCase();
      let brandRes = await client.query('SELECT id FROM public.brand WHERE name=$1', [fmtBrand]);
      if (!brandRes.rowCount) {
        brandRes = await client.query('INSERT INTO public.brand (name) VALUES ($1) RETURNING id', [fmtBrand]);
      }
      const brand_id = brandRes.rows[0].id;

      const exist = await client.query(
        `SELECT id, current_cases FROM public.stock
         WHERE godown_id=$1 AND product_type=$2 AND productname=$3 AND brand=$4`,
        [godown_id, product_type, productname, brand]
      );

      let stockId;
      if (exist.rowCount) {
        stockId = exist.rows[0].id;
        const newC = exist.rows[0].current_cases + cases;
        await client.query(
          'UPDATE public.stock SET current_cases=$1, date_added=CURRENT_TIMESTAMP, brand_id=$2 WHERE id=$3',
          [newC, brand_id, stockId]
        );
      } else {
        const ins = await client.query(
          `INSERT INTO public.stock
           (godown_id,product_type,productname,brand,brand_id,current_cases,per_case)
           VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id`,
          [godown_id, product_type, productname, brand, brand_id, cases, per_case]
        );
        stockId = ins.rows[0].id;
      }

      await client.query(
        'INSERT INTO public.stock_history (stock_id,action,cases,per_case_total) VALUES ($1,$2,$3,$4)',
        [stockId, 'added', cases, cases * per_case]
      );

      results.push({ godown_id, productname, brand, cases_added: cases });
    }

    await client.query('COMMIT');
    res.status(201).json({ message: 'Bulk allocation done', added: results.length, details: results });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error(e);
    res.status(500).json({ message: 'Bulk allocation failed' });
  } finally {
    client.release();
  }
};