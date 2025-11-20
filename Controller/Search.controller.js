// Controller/Search.controller.js
const { Pool } = require('pg');

const pool = new Pool({
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  host: process.env.PGHOST,
  port: process.env.PGPORT,
  database: process.env.PGDATABASE,
});

exports.searchProducts = async (req, res) => {
  try {
    const { type = 'all', name = '' } = req.query;
    const searchName = `%${name}%`;

    let query = `
        SELECT 
            s.product_type,
            s.productname,
            s.brand,
            COALESCE(b.agent_name, '-') AS agent_name,
            g.name AS godown_name,
            g.id AS godown_id,
            s.current_cases
        FROM public.stock s
        JOIN public.godown g ON s.godown_id = g.id
        LEFT JOIN public.brand b ON s.brand = b.name
        WHERE s.productname ILIKE $1
    `;
    const params = [searchName];

    if (type !== 'all') {
      query += ` AND s.product_type = $2`;
      params.push(type);
    }

    query += ` ORDER BY s.productname, s.brand, g.name`;

    const result = await pool.query(query, params);
    res.status(200).json(result.rows);
  } catch (err) {
    console.error('Error in searchProducts:', err.message);
    res.status(500).json({ message: 'Failed to search products' });
  }
};