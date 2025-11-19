// Router/StockAnalysis.router.js
const express = require('express');
const router = express.Router();
const { getStockAnalysis } = require('../Controller/Analysis.controller');

router.get('/stock-analysis', getStockAnalysis);

module.exports = router;