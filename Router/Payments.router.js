// routes/payments.js (unchanged)
const express = require('express');
const router = express.Router();
const ctrl = require('../Controller/Payments.controller');

router.post('/admins', ctrl.createAdmin);
router.get('/admins', ctrl.getAdmins);
router.get('/admins/:id/transactions', ctrl.getAdminTransactions);
router.post('/admins/bank-accounts', ctrl.addBankAccount);
router.get('/admins/:username/bank-accounts', ctrl.getBankAccounts);

router.get('/pending', ctrl.getPending);
router.post('/payment', ctrl.recordPayment);
router.get('/payments/:id', ctrl.getPaymentHistory);

router.get('/sbooking', ctrl.getsBookings);
router.get('/dispatch_logs/:order_id', ctrl.getDispatchLogs);
router.get('/transactions/:id', ctrl.getTransactions);

module.exports = router;