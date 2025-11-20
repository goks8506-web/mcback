// routes/booking.js
const express = require('express');
const router = express.Router();
const { createDispatch, getAllDispatchLogs, getDispatchLogsByBooking} = require('../Controller/Wdispatch.controller');

router.post('/dispatch',createDispatch);
router.get('/dispatch_logs/all',getAllDispatchLogs);
router.get('/dispatch_logs/:booking_id', getDispatchLogsByBooking);

module.exports = router;