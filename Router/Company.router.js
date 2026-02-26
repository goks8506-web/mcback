const express = require('express');
const router = express.Router();
const {
  getCompanyDetails,
  createCompany,
  updateCompany,
  getAllCompanies,
  deleteCompany
} = require('../Controller/Company.controller');

router.get('/company', getCompanyDetails);     // Load form
router.get('/companies', getAllCompanies);
router.post('/company', createCompany);        // Add new
router.put('/company', updateCompany);         // Edit latest
router.delete('/company/:id', deleteCompany);

module.exports = router;