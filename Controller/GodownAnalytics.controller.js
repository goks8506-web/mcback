// backend/Controller/GodownAnalytics.controller.js
const { Pool } = require('pg');
const ExcelJS = require('exceljs');

const pool = new Pool({
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  host: process.env.PGHOST,
  port: process.env.PGPORT,
  database: process.env.PGDATABASE,
});

exports.getAllGodownsAnalytics = async (req, res) => {
  const { period = 'month' } = req.query;

  try {
    const dateFormat = period === 'day' ? 'YYYY-MM-DD' :
                      period === 'year' ? 'YYYY' : 'YYYY-MM';

    // 1. Time-based Intake vs Outtake
    const history = await pool.query(`
      SELECT 
        g.id AS godown_id,
        g.name AS godown_name,
        TO_CHAR(h.date, $1) AS period,
        h.action,
        SUM(h.cases) AS cases
      FROM public.stock_history h
      JOIN public.stock s ON h.stock_id = s.id
      JOIN public.godown g ON s.godown_id = g.id
      GROUP BY g.id, g.name, period, h.action
      ORDER BY g.name, period
    `, [dateFormat]);

    // Build chart data per godown
    const chartData = {};
    const totals = {};

    history.rows.forEach(r => {
      const { godown_id, godown_name, period, action, cases } = r;
      const key = `${godown_id}-${godown_name}`;

      if (!chartData[key]) {
        chartData[key] = { 
          id: godown_id, 
          name: godown_name, 
          labels: new Set(), 
          intake: {}, 
          outtake: {},
          productIntake: {},   // NEW
          productOuttake: {}   // NEW
        };
        totals[key] = { intake: 0, outtake: 0 };
      }

      chartData[key].labels.add(period);
      const val = parseInt(cases);
      if (action === 'added') {
        chartData[key].intake[period] = (chartData[key].intake[period] || 0) + val;
        totals[key].intake += val;
      } else {
        chartData[key].outtake[period] = (chartData[key].outtake[period] || 0) + val;
        totals[key].outtake += val;
      }
    });

    // 2. NEW: Product-wise Intake & Outtake per Godown
    const productQuery = await pool.query(`
      SELECT 
        g.id AS godown_id,
        g.name AS godown_name,
        s.productname,
        h.action,
        SUM(h.cases) AS cases
      FROM public.stock_history h
      JOIN public.stock s ON h.stock_id = s.id
      JOIN public.godown g ON s.godown_id = g.id
      GROUP BY g.id, g.name, s.productname, h.action
    `);

    productQuery.rows.forEach(r => {
      const key = `${r.godown_id}-${r.godown_name}`;
      if (!chartData[key]) return; // safety

      const val = parseInt(r.cases);
      if (r.action === 'added') {
        chartData[key].productIntake[r.productname] = 
          (chartData[key].productIntake[r.productname] || 0) + val;
      } else {
        chartData[key].productOuttake[r.productname] = 
          (chartData[key].productOuttake[r.productname] || 0) + val;
      }
    });

    // Convert to arrays
    const chart = Object.values(chartData).map(g => {
      const labels = Array.from(g.labels).sort();
      const productNames = Object.keys({ ...g.productIntake, ...g.productOuttake });

      return {
        godownId: g.id,
        godownName: g.name,
        labels,
        intake: labels.map(l => g.intake[l] || 0),
        outtake: labels.map(l => g.outtake[l] || 0),
        // NEW: Product-wise data
        productIntake: productNames.map(p => g.productIntake[p] || 0),
        productOuttake: productNames.map(p => g.productOuttake[p] || 0),
        productNames
      };
    });

    // 3. Top 5 Products (global) — unchanged
    const topProducts = await pool.query(`
      SELECT 
        s.productname,
        s.brand,
        SUM(h.cases) AS cases_taken
      FROM public.stock_history h
      JOIN public.stock s ON h.stock_id = s.id
      WHERE h.action = 'taken'
      GROUP BY s.productname, s.brand
      ORDER BY cases_taken DESC
      LIMIT 5
    `);

    // 4. Agent Performance (global) — unchanged
    const agentPerf = await pool.query(`
      SELECT 
        COALESCE(bn.agent_name, 'Unknown') AS agent,
        h.action,
        SUM(h.cases) AS cases
      FROM public.stock_history h
      JOIN public.stock s ON h.stock_id = s.id
      LEFT JOIN public.brand bn ON s.brand = bn.name
      GROUP BY agent, h.action
    `);

    const agents = {};
    agentPerf.rows.forEach(r => {
      const a = r.agent;
      if (!agents[a]) agents[a] = { added: 0, taken: 0 };
      if (r.action === 'added') agents[a].added += parseInt(r.cases);
      else agents[a].taken += parseInt(r.cases);
    });

    res.status(200).json({
      chart,
      totals: Object.entries(totals).map(([key, t]) => {
        const [id, name] = key.split('-');
        return { godownId: parseInt(id), godownName: name, ...t };
      }),
      topProducts: topProducts.rows.map(r => ({
        product: `${r.productname} (${r.brand})`,
        cases: parseInt(r.cases_taken)
      })),
      agentPerformance: Object.entries(agents).map(([agent, d]) => ({
        agent, added: d.added, taken: d.taken
      }))
    });

  } catch (err) {
    console.error('All Godowns Analytics Error:', err);
    res.status(500).json({ message: 'Server error' });
  }
};

exports.exportAllToExcel = async (req, res) => {
  try {
    const history = await pool.query(`
      SELECT 
        g.id AS godown_id,
        g.name AS godown_name,
        h.date,
        s.productname,
        s.brand,
        COALESCE(bn.agent_name, '-') AS agent_name,
        h.action,
        h.cases,
        h.per_case_total
      FROM public.stock_history h
      JOIN public.stock s ON h.stock_id = s.id
      JOIN public.godown g ON s.godown_id = g.id
      LEFT JOIN public.brand bn ON s.brand = bn.name
      ORDER BY g.name, h.date DESC
    `);

    const wb = new ExcelJS.Workbook();

    // Group by godown
    const godownMap = {};
    history.rows.forEach(r => {
      const key = `${r.godown_id}|${r.godown_name}`;
      if (!godownMap[key]) {
        godownMap[key] = { name: r.godown_name, rows: [] };
      }
      godownMap[key].rows.push(r);
    });

    // Create one sheet per godown
    Object.values(godownMap).forEach(g => {
      const ws = wb.addWorksheet(g.name.replace(/[^a-zA-Z0-9]/g, '_').slice(0, 31));

      ws.columns = [
        { header: 'Date', key: 'date', width: 20 },
        { header: 'Product', key: 'product', width: 25 },
        { header: 'Brand', key: 'brand', width: 15 },
        { header: 'Agent', key: 'agent', width: 15 },
        { header: 'Action', key: 'action', width: 10 },
        { header: 'Cases', key: 'cases', width: 10 },
        { header: 'Total Qty', key: 'total', width: 12 },
      ];

      g.rows.forEach(r => {
        ws.addRow({
          date: new Date(r.date).toLocaleString(),
          product: r.productname,
          brand: r.brand,
          agent: r.agent_name,
          action: r.action === 'added' ? 'IN' : 'OUT',
          cases: r.cases,
          total: r.per_case_total
        });
      });

      ws.getRow(1).font = { bold: true };
    });

    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', 'attachment; filename=all_godowns_analytics.xlsx');
    await wb.xlsx.write(res);
    res.end();
  } catch (err) {
    console.error('Export Error:', err);
    res.status(500).json({ message: 'Export failed' });
  }
};