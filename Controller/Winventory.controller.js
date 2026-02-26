// controllers/Inventory.controller.js (Updated with brand, hsn_code, states)
const { Pool } = require('pg');

const pool = new Pool({
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  host: process.env.PGHOST,
  port: process.env.PGPORT,
  database: process.env.PGDATABASEW,
});

/* ──────────────────────  PRODUCT TYPE (Category)  ────────────────────── */
exports.addProductType = async (req, res) => {
  try {
    let { product_type } = req.body;
    if (!product_type) return res.status(400).json({ message: 'Product type required' });

    product_type = product_type.toLowerCase().trim().replace(/\s+/g, '_');
    const exists = await pool.query('SELECT 1 FROM public.product_categories WHERE name = $1', [product_type]);
    if (exists.rows.length) return res.status(400).json({ message: 'Category already exists' });

    // Create category + dynamic table
    await pool.query('INSERT INTO public.product_categories (name) VALUES ($1)', [product_type]);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS public.${product_type} (
        id BIGSERIAL PRIMARY KEY,
        productname TEXT NOT NULL,
        brand TEXT,
        hsn_code VARCHAR(10),
        price NUMERIC(12,2) NOT NULL,
        per_case INTEGER NOT NULL
      )
    `);
    res.status(201).json({ message: 'Category created successfully' });
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Server error' });
  }
};

exports.getProductTypes = async (req, res) => {
  try {
    const result = await pool.query('SELECT name FROM public.product_categories ORDER BY name');
    res.json(result.rows.map(r => r.name));
  } catch (err) {
    res.status(500).json({ message: 'Failed to fetch categories' });
  }
};

/* ──────────────────────  PRODUCTS  ────────────────────── */
exports.addProduct = async (req, res) => {
  try {
    const { productname, brand, hsn_code, price, per_case, product_type } = req.body;
    if (!productname || !price || !per_case || !product_type)
      return res.status(400).json({ message: 'Required fields missing' });

    const table = product_type.toLowerCase().trim().replace(/\s+/g, '_');
    const checkTable = await pool.query(`SELECT to_regclass('public.${table}')`);
    if (!checkTable.rows[0].to_regclass)
      return res.status(400).json({ message: 'Invalid product category' });

    const dup = await pool.query(
      `SELECT id FROM public.${table} WHERE LOWER(productname) = LOWER($1) AND LOWER(brand) = LOWER($2)`,
      [productname.trim(), (brand || '').trim()]
    );
    if (dup.rows.length) return res.status(400).json({ message: 'Product already exists' });

    const result = await pool.query(
      `INSERT INTO public.${table} (productname, brand, hsn_code, price, per_case)
       VALUES ($1, $2, $3, $4, $5) RETURNING id`,
      [productname.trim(), (brand || null), (hsn_code || null), parseFloat(price), parseInt(per_case)]
    );

    res.status(201).json({ message: 'Product added', id: result.rows[0].id });
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Failed to add product' });
  }
};

exports.getAllProducts = async (req, res) => {
  try {
    const types = await pool.query('SELECT name FROM public.product_categories');
    const all = [];

    for (const { name } of types.rows) {
      const table = name.toLowerCase().replace(/\s+/g, '_');
      const rows = await pool.query(`
        SELECT id, productname, price AS rate_per_box, per_case, '${name}' as product_type 
        FROM public.${table} ORDER BY productname
      `);
      all.push(...rows.rows);
    }
    res.json(all);
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Failed to fetch products' });
  }
};

exports.searchProducts = async (req, res) => {
  const { name } = req.query;
  const searchTerm = `%${name.trim().toLowerCase()}%`;

  try {
    const types = await pool.query('SELECT name FROM public.product_categories');
    const all = [];

    for (const { name } of types.rows) {
      const table = name.toLowerCase().replace(/\s+/g, '_');
      const rows = await pool.query(`
        SELECT id, productname, brand, hsn_code, price AS rate_per_box, per_case, '${name}' as product_type 
        FROM public.${table} 
        WHERE LOWER(productname) LIKE $1 OR LOWER(brand) LIKE $1 ORDER BY productname
      `, [searchTerm]);
      all.push(...rows.rows);
    }
    res.json(all);
  } catch (err) {
    console.error('Search Products Error:', err);
    res.status(500).json({ message: 'Search failed' });
  }
};

exports.updateProduct = async (req, res) => {
  try {
    const { product_type, id } = req.params;
    const { productname, brand, hsn_code, price, per_case } = req.body;

    const table = product_type.toLowerCase().replace(/\s+/g, '_');
    await pool.query(
      `UPDATE public.${table} 
       SET productname = $1, brand = $2, hsn_code = $3, price = $4, per_case = $5 
       WHERE id = $6`,
      [productname.trim(), brand || null, hsn_code || null, parseFloat(price), parseInt(per_case), id]
    );
    res.json({ message: 'Product updated' });
  } catch (err) {
    res.status(500).json({ message: 'Update failed' });
  }
};

exports.deleteProduct = async (req, res) => {
  try {
    const { product_type, id } = req.params;
    const table = product_type.toLowerCase().replace(/\s+/g, '_');
    await pool.query(`DELETE FROM public.${table} WHERE id = $1`, [id]);
    res.json({ message: 'Product deleted' });
  } catch (err) {
    res.status(500).json({ message: 'Delete failed' });
  }
};

/* ──────────────────────  STATES  ────────────────────── */
exports.getStates = async (req, res) => {
  try {
    const result = await pool.query('SELECT code, state_name FROM codestate ORDER BY code');
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ message: 'Failed to fetch states' });
  }
};