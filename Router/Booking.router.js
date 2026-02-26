// routes/booking.routes.js
const express = require('express');
const router = express.Router();
const multer = require('multer');

const upload = multer({ limits: { fileSize: 60 * 1024 * 1024 } });
const {
  createBooking,
  getAllBookings,
  getBookingById,
  getRecentCustomers,
  getStatesForSupply,
  getLatestBillNo
} = require('../Controller/Booking.controller');

router.post('/bookings', upload.single('pdf'), createBooking);
router.get('/bookings', getAllBookings);
router.get('/bookings/:id', getBookingById);
router.get('/customers/recent', getRecentCustomers);
router.get('/states', getStatesForSupply);
router.get('/latest', getLatestBillNo);

module.exports = router;