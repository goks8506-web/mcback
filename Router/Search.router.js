// Router/Search.router.js
const express = require('express');
const router = express.Router();
const { searchProducts } = require('../Controller/Search.controller');

router.get('/search', searchProducts);

module.exports = router;