const { Pool } = require("pg");
const cloudinary = require("cloudinary").v2;

// Configure Cloudinary
cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
  api_key: process.env.CLOUDINARY_API_KEY,
  api_secret: process.env.CLOUDINARY_API_SECRET,
});

const pool = new Pool({
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  host: process.env.PGHOST,
  port: process.env.PGPORT,
  database: process.env.PGDATABASE,
  max: 30,
});

// Caches
let productTypeCache = { data: null, timestamp: 0 };
let brandCache = { data: null, timestamp: 0 };

// === HELPER: Get Cached Product Types ===
async function getCachedProductTypes() {
  const now = Date.now();
  if (!productTypeCache.data || now - productTypeCache.timestamp > 300000) {
    const client = await pool.connect();
    try {
      const result = await client.query("SELECT product_type FROM public.products");
      productTypeCache = {
        data: result.rows.map((r) => r.product_type),
        timestamp: now,
      };
    } finally {
      client.release();
    }
  }
  return productTypeCache.data;
}

// === HELPER: Get Cached Brands ===
async function getCachedBrands() {
  const now = Date.now();
  if (!brandCache.data || now - brandCache.timestamp > 300000) {
    const client = await pool.connect();
    try {
      const result = await client.query("SELECT id, name, agent_name FROM public.brand");
      brandCache = {
        data: result.rows,
        timestamp: now,
      };
    } finally {
      client.release();
    }
  }
  return brandCache.data;
}

// === CREATE BRAND TABLE (Run once or on startup) ===
async function ensureBrandsTable() {
  const client = await pool.connect();
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS public.brand (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) UNIQUE NOT NULL,
        agent_name VARCHAR(100)
      )
    `);
  } finally {
    client.release();
  }
}
ensureBrandsTable();

// === ADD PRODUCT TYPE ===
exports.addProductType = async (req, res) => {
  const client = await pool.connect();
  try {
    const { product_type } = req.body;
    if (!product_type) return res.status(400).json({ message: "Product type is required" });

    const formatted = product_type.toLowerCase().replace(/\s+/g, "_");
    const exists = await client.query("SELECT 1 FROM public.products WHERE product_type = $1", [formatted]);
    if (exists.rows.length > 0) return res.status(400).json({ message: "Product type already exists" });

    await client.query("BEGIN");

    await client.query("INSERT INTO public.products (product_type) VALUES ($1)", [formatted]);

    const tableName = formatted;
    await client.query(`
      CREATE TABLE IF NOT EXISTS public.${tableName} (
        id SERIAL PRIMARY KEY,
        serial_number VARCHAR(50) NOT NULL,
        productname VARCHAR(100) NOT NULL,
        price NUMERIC(10,2) NOT NULL,
        wprice NUMERIC(10,2) NOT NULL DEFAULT 0.00,
        per VARCHAR(10) NOT NULL CHECK (per IN ('pieces', 'box', 'pkt')),
        per_case INTEGER NOT NULL DEFAULT 1,
        discount NUMERIC(5,2) NOT NULL,
        brand_id INTEGER REFERENCES public.brand(id) ON DELETE RESTRICT,
        image TEXT,
        description TEXT,
        status VARCHAR(10) NOT NULL DEFAULT 'off' CHECK (status IN ('on', 'off')),
        fast_running BOOLEAN DEFAULT false
      )
    `);

    // Indexes (kept – they don’t enforce uniqueness, just speed up queries)
    await client.query(`CREATE INDEX IF NOT EXISTS idx_serial_${tableName} ON public.${tableName}(serial_number)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_name_${tableName} ON public.${tableName}(productname)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_brand_${tableName} ON public.${tableName}(brand_id)`);

    productTypeCache.data = [...(productTypeCache.data || []), formatted];
    productTypeCache.timestamp = Date.now();

    await client.query("COMMIT");
    res.status(201).json({ message: "Product type created successfully" });
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("Error in addProductType:", err);
    res.status(500).json({ message: "Failed to create product type", error: err.message });
  } finally {
    client.release();
  }
};

// === ADD PRODUCT ===
exports.addProduct = async (req, res) => {
  const client = await pool.connect();
  try {
    const {
      serial_number, productname, price, wprice, per, discount,
      product_type, description = "", per_case, brand
    } = req.body;

    const existingImages = req.body.existingImages ? JSON.parse(req.body.existingImages) : [];
    const files = req.files || [];

    // Validation
    if (!serial_number || !productname || !price || !wprice || !per || !discount || !product_type || !per_case || !brand) {
      return res.status(400).json({ message: "All required fields must be provided" });
    }

    if (!["pieces", "box", "pkt"].includes(per)) {
      return res.status(400).json({ message: "Valid per value (pieces, box, pkt)" });
    }

    const priceNum = parseFloat(price);
    const wpriceNum = parseFloat(wprice);
    const discountNum = parseFloat(discount);
    const perCaseNum = parseInt(per_case);

    if (isNaN(priceNum) || priceNum < 0) return res.status(400).json({ message: "Invalid price" });
    if (isNaN(wpriceNum) || wpriceNum < 0) return res.status(400).json({ message: "Invalid wholesale price" });
    if (isNaN(discountNum) || discountNum < 0 || discountNum > 100) return res.status(400).json({ message: "Discount 0-100%" });
    if (isNaN(perCaseNum) || perCaseNum < 1) return res.status(400).json({ message: "per_case >= 1" });

    const tableName = product_type.toLowerCase().replace(/\s+/g, "_");
    const types = await getCachedProductTypes();
    if (!types.includes(product_type)) return res.status(400).json({ message: "Invalid product type" });

    // Validate Brand
    const brands = await getCachedBrands();
    const brandRec = brands.find(b => b.name === brand);
    if (!brandRec) return res.status(400).json({ message: "Brand not found" });

    // Check duplicate
    const dup = await client.query(
      `SELECT 1 FROM public.${tableName} WHERE serial_number = $1 OR productname = $2`,
      [serial_number, productname]
    );
    if (dup.rows.length > 0) return res.status(400).json({ message: "Product already exists" });

    // Final Images
    const finalImages = [
      ...existingImages,
      ...files.map(f => f.path)
    ];

    const result = await client.query(`
      INSERT INTO public.${tableName}
      (serial_number, productname, price, wprice, per, per_case, discount, brand_id, image, description, status)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'off')
      RETURNING id
    `, [
      serial_number, productname, priceNum, wpriceNum, per, perCaseNum,
      discountNum, brandRec.id,
      finalImages.length > 0 ? JSON.stringify(finalImages) : null,
      description
    ]);

    res.status(201).json({ message: "Product saved", id: result.rows[0].id });
  } catch (err) {
    console.error("addProduct error:", err);
    res.status(500).json({ message: "Failed to save", error: err.message });
  } finally {
    client.release();
  }
};

// === UPDATE PRODUCT ===
exports.updateProduct = async (req, res) => {
  const client = await pool.connect();
  try {
    const { tableName, id } = req.params;
    const {
      serial_number, productname, price, wprice, per, discount, status,
      description = "", per_case, brand, existingImages
    } = req.body;
    const files = req.files || [];

    if (!serial_number || !productname || !price || !wprice || !per || !discount || !per_case || !brand) {
      return res.status(400).json({ message: "Required fields missing" });
    }

    const priceNum = parseFloat(price);
    const wpriceNum = parseFloat(wprice);
    const discountNum = parseFloat(discount);
    const perCaseNum = parseInt(per_case);
    if (isNaN(priceNum) || isNaN(wpriceNum) || isNaN(discountNum) || isNaN(perCaseNum)) {
      return res.status(400).json({ message: "Invalid numeric values" });
    }

    const brands = await getCachedBrands();
    const brandRec = brands.find(b => b.name === brand);
    if (!brandRec) return res.status(400).json({ message: "Invalid brand" });

    let finalImages = [];
    if (existingImages) {
      finalImages = typeof existingImages === "string" ? JSON.parse(existingImages) : existingImages;
    }
    finalImages = [...finalImages, ...files.map(f => f.path)];

    // Delete removed images
    const current = await client.query(`SELECT image FROM public.${tableName} WHERE id = $1`, [id]);
    if (current.rows[0]?.image) {
      const old = JSON.parse(current.rows[0].image) || [];
      const toDelete = old.filter(url => !finalImages.includes(url));
      for (const url of toDelete) {
        const publicId = url.match(/\/mnc_products\/(.+?)\./)?.[1];
        if (publicId) {
          await cloudinary.uploader.destroy(`mnc_products/${publicId}`, {
            resource_type: url.includes("/video/") ? "video" : "image"
          });
        }
      }
    }

    const query = `
      UPDATE public.${tableName} SET
        serial_number = $1, productname = $2, price = $3, wprice = $4, per = $5,
        per_case = $6, discount = $7, brand_id = $8, image = $9, description = $10
        ${status ? ', status = $11' : ''}
      WHERE id = $${status ? 12 : 11} RETURNING id
    `;

    const values = [
      serial_number, productname, priceNum, wpriceNum, per, perCaseNum,
      discountNum, brandRec.id,
      finalImages.length > 0 ? JSON.stringify(finalImages) : null,
      description
    ];
    if (status) values.push(status);
    values.push(id);

    const result = await client.query(query, values);
    if (result.rows.length === 0) return res.status(404).json({ message: "Not found" });

    res.status(200).json({ message: "Updated" });
  } catch (err) {
    console.error("updateProduct:", err);
    res.status(500).json({ message: "Update failed", error: err.message });
  } finally {
    client.release();
  }
};

// === GET PRODUCTS ===
exports.getProducts = async (req, res) => {
  try {
    const { page = 1, limit = 50 } = req.query;
    const offset = (page - 1) * limit;
    const types = await getCachedProductTypes();

    const queries = types.map(async (type) => {
      const table = type.toLowerCase().replace(/\s+/g, "_");
      const client = await pool.connect();
      try {
        const result = await client.query(`
          SELECT p.*, b.name AS brand_name, b.agent_name
          FROM public.${table} p
          LEFT JOIN public.brand b ON p.brand_id = b.id
          ORDER BY p.id LIMIT $1 OFFSET $2
        `, [limit, offset]);

        return result.rows.map(r => ({
          id: r.id,
          product_type: type,
          serial_number: r.serial_number,
          productname: r.productname,
          price: r.price,
          wprice: r.wprice,
          per: r.per,
          per_case: r.per_case,
          discount: r.discount,
          brand: r.brand_name,
          agent_name: r.agent_name,
          image: r.image ? JSON.parse(r.image) : [],
          description: r.description || "",
          status: r.status,
          fast_running: r.fast_running
        }));
      } finally {
        client.release();
      }
    });

    const data = (await Promise.all(queries)).flat();
    res.json({ data, page: +page, limit: +limit, total: data.length });
  } catch (err) {
    res.status(500).json({ message: "Fetch failed", error: err.message });
  }
};

// === BRAND CRUD ===
exports.addBrand = async (req, res) => {
  try {
    const { brand, agent_name } = req.body;
    if (!brand) return res.status(400).json({ message: "Brand name required" });

    const formatted = brand.toLowerCase().replace(/\s+/g, "_");
    const exists = await pool.query("SELECT 1 FROM public.brand WHERE name = $1", [formatted]);
    if (exists.rows.length > 0) return res.status(400).json({ message: "Brand exists" });

    const result = await pool.query(
      "INSERT INTO public.brand (name, agent_name) VALUES ($1, $2) RETURNING id",
      [formatted, agent_name || null]
    );

    brandCache.data = null; // invalidate
    res.status(201).json({ message: "Brand created", id: result.rows[0].id });
  } catch (err) {
    res.status(500).json({ message: "Failed", error: err.message });
  }
};

exports.getBrands = async (req, res) => {
  try {
    const brands = await getCachedBrands();
    res.json(brands.map(b => ({ id: b.id, name: b.name, agent_name: b.agent_name })));
  } catch (err) {
    res.status(500).json({ message: "Failed", error: err.message });
  }
};

exports.updateBrand = async (req, res) => {
  const client = await pool.connect();
  try {
    const { id } = req.params;
    const { brand, agent_name } = req.body;
    const formatted = brand.toLowerCase().replace(/\s+/g, "_");

    await client.query(
      "UPDATE public.brand SET name = $1, agent_name = $2 WHERE id = $3",
      [formatted, agent_name || null, id]
    );

    brandCache.data = null;
    res.json({ message: "Brand updated" });
  } catch (err) {
    res.status(500).json({ message: "Update failed", error: err.message });
  } finally {
    client.release();
  }
};

exports.deleteBrand = async (req, res) => {
  const client = await pool.connect();
  try {
    const { id } = req.params;

    const types = await getCachedProductTypes();
    for (const type of types) {
      const table = type.toLowerCase().replace(/\s+/g, "_");
      const inUse = await client.query(`SELECT 1 FROM public.${table} WHERE brand_id = $1 LIMIT 1`, [id]);
      if (inUse.rows.length > 0) {
        return res.status(400).json({ message: "Cannot delete: Brand is in use" });
      }
    }

    await client.query("DELETE FROM public.brand WHERE id = $1", [id]);
    brandCache.data = null;
    res.json({ message: "Brand deleted" });
  } catch (err) {
    res.status(500).json({ message: "Delete failed", error: err.message });
  } finally {
    client.release();
  }
};

// === GET PRODUCT TYPES ===
exports.getProductTypes = async (req, res) => {
  try {
    const result = await pool.query("SELECT product_type FROM public.products");
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ message: "Failed", error: err.message });
  }
};

// === DELETE PRODUCT TYPE ===
exports.deleteProductType = async (req, res) => {
  const client = await pool.connect();
  try {
    const { productType } = req.params;
    const formatted = productType.toLowerCase().replace(/\s+/g, "_");

    await client.query("BEGIN");
    const products = await client.query(`SELECT image FROM public.${formatted}`);
    for (const p of products.rows) {
      if (p.image) {
        const urls = JSON.parse(p.image);
        for (const url of urls) {
          const publicId = url.match(/\/mnc_products\/(.+?)\./)?.[1];
          if (publicId) {
            await cloudinary.uploader.destroy(`mnc_products/${publicId}`, {
              resource_type: url.includes("/video/") ? "video" : "image"
            });
          }
        }
      }
    }

    await client.query(`DROP TABLE IF EXISTS public.${formatted}`);
    await client.query("DELETE FROM public.products WHERE product_type = $1", [formatted]);

    productTypeCache.data = productTypeCache.data.filter(t => t !== formatted);
    await client.query("COMMIT");
    res.json({ message: "Product type deleted" });
  } catch (err) {
    await client.query("ROLLBACK");
    res.status(500).json({ message: "Failed", error: err.message });
  } finally {
    client.release();
  }
};

// === DELETE PRODUCT ===
exports.deleteProduct = async (req, res) => {
  const client = await pool.connect();
  try {
    const { tableName, id } = req.params;

    const types = await getCachedProductTypes();
    if (!types.includes(tableName)) {
      return res.status(400).json({ message: "Invalid product type" });
    }

    const result = await client.query(
      `SELECT image FROM public.${tableName} WHERE id = $1`,
      [id]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ message: "Product not found" });
    }

    const imageJson = result.rows[0].image;

    if (imageJson) {
      const images = JSON.parse(imageJson) || [];
      for (const url of images) {
        const publicId = url.match(/\/mnc_products\/(.+?)\./)?.[1];
        if (publicId) {
          try {
            await cloudinary.uploader.destroy(`mnc_products/${publicId}`, {
              resource_type: url.includes("/video/") ? "video" : "image",
            });
          } catch (cloudErr) {
            console.warn("Cloudinary delete failed:", publicId, cloudErr.message);
          }
        }
      }
    }

    const deleteResult = await client.query(
      `DELETE FROM public.${tableName} WHERE id = $1 RETURNING id`,
      [id]
    );

    if (deleteResult.rows.length === 0) {
      return res.status(404).json({ message: "Product not found" });
    }

    res.status(200).json({ message: "Product deleted successfully" });
  } catch (err) {
    console.error("Error in deleteProduct:", err);
    res.status(500).json({ message: "Failed to delete product", error: err.message });
  } finally {
    client.release();
  }
};

// === TOGGLE PRODUCT STATUS ===
exports.toggleProductStatus = async (req, res) => {
  const client = await pool.connect();
  try {
    const { tableName, id } = req.params;

    const types = await getCachedProductTypes();
    if (!types.includes(tableName)) {
      return res.status(400).json({ message: "Invalid product type" });
    }

    const result = await client.query(
      `SELECT status FROM public.${tableName} WHERE id = $1`,
      [id]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ message: "Product not found" });
    }

    const currentStatus = result.rows[0].status;
    const newStatus = currentStatus === "on" ? "off" : "on";

    await client.query(
      `UPDATE public.${tableName} SET status = $1 WHERE id = $2`,
      [newStatus, id]
    );

    res.status(200).json({ message: "Status toggled", status: newStatus });
  } catch (err) {
    console.error("Error in toggleProductStatus:", err);
    res.status(500).json({ message: "Failed to toggle status", error: err.message });
  } finally {
    client.release();
  }
};

// === TOGGLE FAST RUNNING ===
exports.toggleFastRunning = async (req, res) => {
  const client = await pool.connect();
  try {
    const { tableName, id } = req.params;

    const types = await getCachedProductTypes();
    if (!types.includes(tableName)) {
      return res.status(400).json({ message: "Invalid product type" });
    }

    const result = await client.query(
      `SELECT fast_running FROM public.${tableName} WHERE id = $1`,
      [id]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ message: "Product not found" });
    }

    const updated = !result.rows[0].fast_running;

    await client.query(
      `UPDATE public.${tableName} SET fast_running = $1 WHERE id = $2`,
      [updated, id]
    );

    res.status(200).json({ message: "Fast running updated", fast_running: updated });
  } catch (err) {
    console.error("Error in toggleFastRunning:", err);
    res.status(500).json({ message: "Failed to update", error: err.message });
  } finally {
    client.release();
  }
};