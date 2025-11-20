// backend/Router/GodownAnalytics.router.js
const express = require('express');
const router = express.Router();
const {
  getAllGodownsAnalytics,
  exportAllToExcel
} = require('../Controller/GodownAnalytics.controller');

router.get('/analytics/all', getAllGodownsAnalytics);
router.get('/analytics/all/export', exportAllToExcel);

module.exports = router;