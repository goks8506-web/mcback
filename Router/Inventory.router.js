const express = require('express');
const router = express.Router();
const {
  addProduct, getProducts, addProductType, getProductTypes,
  updateProduct, deleteProduct, toggleProductStatus, toggleFastRunning,
  deleteProductType,
  addBrand, getBrands, updateBrand, deleteBrand
} = require('../Controller/Inventory.controller');
const multer = require('multer');
const { storage } = require('../Config/cloudinary');

const upload = multer({
  storage,
  limits: { fileSize: 5 * 1024 * 1024 },
});

// Products
router.post('/products', upload.array('images'), addProduct);
router.get('/products', getProducts);
router.put('/products/:tableName/:id', upload.array('images'), updateProduct);
router.delete('/products/:tableName/:id', deleteProduct);
router.patch('/products/:tableName/:id/toggle-status', toggleProductStatus);
router.patch('/products/:tableName/:id/toggle-fast-running', toggleFastRunning);

// Product Types
router.post('/product-types', addProductType);
router.get('/product-types', getProductTypes);
router.delete('/product-types/:productType', deleteProductType);

// Brands
router.post('/brands', addBrand);
router.get('/brands', getBrands);
router.put('/brands/:id', updateBrand);
router.delete('/brands/:id', deleteBrand);

module.exports = router;