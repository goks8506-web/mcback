const { Pool } = require('pg');
const multer = require('multer');
const cloudinary = require("cloudinary").v2;
const { CloudinaryStorage } = require("multer-storage-cloudinary");

// Cloudinary Config (same as inventory.controller)
cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
  api_key: process.env.CLOUDINARY_API_KEY.trim(),
  api_secret: process.env.CLOUDINARY_API_SECRET.trim(),
});

// Storage folder: mnc_company
const storage = new CloudinaryStorage({
  cloudinary,
  params: {
    folder: "mnc_company",
    allowed_formats: ["jpg", "png", "jpeg", "webp"],
  },
});

const pool = new Pool({
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  host: process.env.PGHOST,
  port: process.env.PGPORT,
  database: process.env.PGDATABASEW,
});

const upload = multer({ storage });
const uploadFields = upload.fields([
  { name: 'logo', maxCount: 1 },
  { name: 'signature', maxCount: 1 }
]);

exports.getAllCompanies = async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        id, company_name, address, gstin, email,
        logo_url, signature_url,
        bank_name, branch, account_no, ifsc_code,
        created_at, updated_at
      FROM public.company_details 
      ORDER BY id DESC
    `);

    res.json(result.rows);
  } catch (err) {
    console.error('Get all companies error:', err);
    res.status(500).json({ message: 'Server error' });
  }
};

// GET latest company (for edit form & booking page)
exports.getCompanyDetails = async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT * FROM public.company_details 
      ORDER BY id DESC LIMIT 1
    `);

    if (result.rows.length === 0) {
      return res.json({
        id: null,
        company_name: 'NISHA TRADERS',
        address: '', gstin: '', email: '',
        logo_url: '', signature_url: '',
        bank_name: 'Tamilnad Mercantile Bank Ltd.',
        branch: 'SIVAKASI',
        account_no: '', ifsc_code: ''
      });
    }

    res.json(result.rows[0]);
  } catch (err) {
    console.error('Get latest company error:', err);
    res.status(500).json({ message: 'Server error' });
  }
};

// CREATE new company
exports.createCompany = async (req, res) => {
  uploadFields(req, res, async (err) => {
    if (err) return res.status(400).json({ message: err.message });

    const {
      company_name = 'NISHA TRADERS',
      address = '', gstin = '', email = '',
      bank_name = 'Tamilnad Mercantile Bank Ltd.',
      branch = 'SIVAKASI', account_no = '', ifsc_code = ''
    } = req.body;

    const logo_url = req.files?.logo?.[0]?.path || '';
    const signature_url = req.files?.signature?.[0]?.path || '';

    try {
      await pool.query(`
        INSERT INTO public.company_details
          (company_name, address, gstin, email, logo_url, signature_url,
           bank_name, branch, account_no, ifsc_code)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
      `, [
        company_name.trim(),
        address.trim(),
        gstin.trim().toUpperCase(),
        email.trim(),
        logo_url,
        signature_url,
        bank_name.trim(),
        branch.trim(),
        account_no.trim(),
        ifsc_code.trim()
      ]);

      res.json({ message: 'New company created successfully!', success: true });
    } catch (err) {
      console.error('Create error:', err);
      res.status(500).json({ message: 'Failed to create company' });
    }
  });
};

// UPDATE existing company
exports.updateCompany = async (req, res) => {
  uploadFields(req, res, async (err) => {
    if (err) return res.status(400).json({ message: err.message });

    const {
      company_name, address, gstin, email,
      bank_name, branch, account_no, ifsc_code
    } = req.body;

    const logo_url = req.files?.logo?.[0]?.path || null;
    const signature_url = req.files?.signature?.[0]?.path || null;

    try {
      await pool.query(`
        UPDATE public.company_details SET
          company_name = $1, address = $2, gstin = $3, email = $4,
          logo_url = COALESCE($5, logo_url),
          signature_url = COALESCE($6, signature_url),
          bank_name = $7, branch = $8, account_no = $9, ifsc_code = $10,
          updated_at = NOW()
        WHERE id = (SELECT id FROM public.company_details ORDER BY id DESC LIMIT 1)
      `, [
        company_name.trim(),
        address.trim(),
        gstin.trim().toUpperCase(),
        email.trim(),
        logo_url,
        signature_url,
        bank_name.trim(),
        branch.trim(),
        account_no.trim(),
        ifsc_code.trim()
      ]);

      res.json({ message: 'Company updated successfully!', success: true });
    } catch (err) {
      console.error('Update error:', err);
      res.status(500).json({ message: 'Failed to update' });
    }
  });
};

// DELETE company (and its images from Cloudinary)
exports.deleteCompany = async (req, res) => {
  const { id } = req.params;

  try {
    // 1. Get the company to retrieve image URLs
    const companyRes = await pool.query(
      `SELECT logo_url, signature_url FROM public.company_details WHERE id = $1`,
      [id]
    );

    if (companyRes.rows.length === 0) {
      return res.status(404).json({ message: 'Company not found' });
    }

    const { logo_url, signature_url } = companyRes.rows[0];

    // 2. Delete images from Cloudinary (if they exist)
    const deleteFromCloudinary = async (url) => {
      if (!url) return;
      try {
        const publicId = url.split('/').pop().split('.')[0]; // extracts public_id
        await cloudinary.uploader.destroy(`mnc_company/${publicId}`);
      } catch (cloudErr) {
        console.warn('Failed to delete image from Cloudinary:', cloudErr.message);
        // We don't fail the whole delete just because Cloudinary cleanup failed
      }
    };

    await Promise.all([
      deleteFromCloudinary(logo_url),
      deleteFromCloudinary(signature_url),
    ]);

    // 3. Delete record from database
    await pool.query(`DELETE FROM public.company_details WHERE id = $1`, [id]);

    res.json({ message: 'Company deleted successfully!', success: true });
  } catch (err) {
    console.error('Delete company error:', err);
    res.status(500).json({ message: 'Failed to delete company' });
  }
};