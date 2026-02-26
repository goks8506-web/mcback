// routes/payments.routes.js
const express = require('express');
const router = express.Router();

const ctrl = require('../Controller/Payments.controller');

// Admin & Banks
router.post('/admins', ctrl.createAdmin);
router.get('/admins', ctrl.getAdmins);
router.post('/admins/banks', ctrl.addBankAccount);
router.get('/admins/:username/banks', ctrl.getBankAccounts);

// Payments
router.post('/payments', ctrl.recordPayment);
router.get('/payments/:id', ctrl.getPaymentsByBooking);

// Ledger & Reports
router.get('/ledger', ctrl.getCustomerLedger);
router.get('/outstanding', ctrl.getOutstandingCustomers);
router.get('/pendingpay', ctrl.getPending);

// Detailed bookings view (with payments + dispatches)
router.get('/bookings', ctrl.getBookingsWithDetails);
// Optional alias if you prefer the old name:
// router.get('/sbooking', ctrl.getBookingsWithDetails);

module.exports = router;