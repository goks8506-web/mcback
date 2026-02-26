// Router/Inventory.router.js
const express = require('express');
const router = express.Router();
const {
  addProduct,
  getAllProducts, 
  updateProduct,
  deleteProduct,
  addProductType,
  getProductTypes,
  getStates
} = require('../Controller/Winventory.controller');

// ==================== PRODUCT ROUTES ====================
router.post('/products', addProduct);                    // Add new product
router.get('/products', getAllProducts);                 // Get ALL products from all categories
router.put('/products/:product_type/:id', updateProduct); // Update product
router.delete('/products/:product_type/:id', deleteProduct); // Delete product

router.post('/product-types', addProductType);
router.get('/product-types', getProductTypes);
router.get('/states',getStates);

module.exports = router;