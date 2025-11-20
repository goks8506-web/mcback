// Controller/StockAnalysis.controller.js
const { Pool } = require('pg');
require('dotenv').config();

const pool = new Pool({
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  host: process.env.PGHOST,
  port: process.env.PGPORT,
  database: process.env.PGDATABASE,
});

exports.getStockAnalysis = async (req, res) => {
  const client = await pool.connect();
  try {
    // ────────────────────── 1. All individual rows ──────────────────────
    const allRes = await client.query(`
      SELECT 
        COALESCE(g.name, 'Unknown') AS godown_name,
        COALESCE(s.product_type, 'Unknown') AS product_type,
        s.productname,
        COALESCE(s.brand, 'Unknown') AS brand,
        COALESCE(b.agent_name, '-') AS agent_name,
        s.current_cases AS cases,
        s.per_case,
        (s.current_cases * s.per_case) AS total_qty
      FROM public.stock s
      JOIN public.godown g ON s.godown_id = g.id
      LEFT JOIN public.brand b ON s.brand = b.name
      ORDER BY g.name, s.product_type, s.productname
    `);

    // ────────────────────── 2. Low stock (< 3 cases total) ──────────────────────
    const lowRes = await client.query(`
      SELECT 
        s.product_type,
        s.productname,
        s.brand,
        COALESCE(b.agent_name, '-') AS agent_name,
        SUM(s.current_cases) AS total_cases,
        SUM(s.current_cases * s.per_case) AS total_qty
      FROM public.stock s
      LEFT JOIN public.brand b ON s.brand = b.name
      GROUP BY s.product_type, s.productname, s.brand, b.agent_name
      HAVING SUM(s.current_cases) < 3
      ORDER BY total_cases ASC
    `);

    // ────────────────────── 3. Godown-wise total cases ──────────────────────
    const godownRes = await client.query(`
      SELECT 
        g.name AS godown_name,
        SUM(s.current_cases) AS total_cases
      FROM public.stock s
      JOIN public.godown g ON s.godown_id = g.id
      GROUP BY g.name
      ORDER BY total_cases DESC
    `);

    // ────────────────────── 4. Product-wise total cases (all godowns) ──────────────────────
    const productRes = await client.query(`
      SELECT 
        s.product_type,
        s.productname,
        s.brand,
        COALESCE(b.agent_name, '-') AS agent_name,
        SUM(s.current_cases) AS total_cases,
        SUM(s.current_cases * s.per_case) AS total_qty
      FROM public.stock s
      LEFT JOIN public.brand b ON s.brand = b.name
      GROUP BY s.product_type, s.productname, s.brand, b.agent_name
      ORDER BY total_cases DESC
    `);

    // ────────────────────── 5. Grand total ──────────────────────
    const grandRes = await client.query(`
      SELECT 
        COUNT(DISTINCT s.product_type || s.productname || s.brand) AS unique_products,
        SUM(s.current_cases) AS total_cases,
        SUM(s.current_cases * s.per_case) AS total_quantity
      FROM public.stock s
    `);

    res.json({
      allRows: allRes.rows,
      lowStock: lowRes.rows,
      godownSummary: godownRes.rows,
      productSummary: productRes.rows,
      grandTotal: grandRes.rows[0] || { unique_products: 0, total_cases: 0, total_quantity: 0 }
    });
  } catch (err) {
    console.error('StockAnalysis error:', err);
    res.status(500).json({ error: 'Failed to fetch analysis' });
  } finally {
    client.release();
  }
};