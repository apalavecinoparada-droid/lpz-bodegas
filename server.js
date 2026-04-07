'use strict';
// ============================================================
//  LPZ Bodegas - Servidor para Render.com
//  Adaptado para funcionar sin Docker, sin Nginx, sin archivos
//  SQL externos. Se conecta a PostgreSQL via DATABASE_URL.
// ============================================================
require('dotenv').config();
const express  = require('express');
const { Pool } = require('pg');
const cors     = require('cors');
const bcrypt   = require('bcryptjs');
const jwt      = require('jsonwebtoken');
const path     = require('path');

const app        = express();
const PORT       = process.env.PORT || 3000;
const JWT_SECRET = process.env.JWT_SECRET || 'lpz_bodegas_secret_2025';

// ── Conexión a base de datos ───────────────────────────────
// Render provee DATABASE_URL automáticamente.
// Para desarrollo local se pueden usar variables individuales.
const pool = new Pool(
  process.env.DATABASE_URL
    ? {
        connectionString: process.env.DATABASE_URL,
        ssl: { rejectUnauthorized: false }
      }
    : {
        host:     process.env.DB_HOST     || 'localhost',
        port:     parseInt(process.env.DB_PORT || '5432'),
        database: process.env.DB_NAME     || 'lpz_bodegas',
        user:     process.env.DB_USER     || 'postgres',
        password: process.env.DB_PASSWORD || 'postgres',
      }
);

// ── Middleware ─────────────────────────────────────────────
app.use(cors());
app.use(express.json({ limit: '5mb' }));
app.use(express.static(path.join(__dirname, 'frontend')));

// ── Auth helpers ───────────────────────────────────────────
function auth(req, res, next) {
  const h = req.headers.authorization;
  if (!h) return res.status(401).json({ error: 'No autorizado' });
  try { req.user = jwt.verify(h.split(' ')[1], JWT_SECRET); next(); }
  catch { res.status(401).json({ error: 'Token inválido' }); }
}

// ── Audit ──────────────────────────────────────────────────
async function audit(tabla, id, accion, antes, despues, usr) {
  try {
    await pool.query(
      `INSERT INTO auditoria(tabla_afectada,registro_id,accion,datos_anteriores,datos_nuevos,usuario)
       VALUES($1,$2,$3,$4,$5,$6)`,
      [tabla, id, accion, antes ? JSON.stringify(antes) : null,
       despues ? JSON.stringify(despues) : null, usr]
    );
  } catch {}
}

// ════════════════════════════════════════════════════════════
//  AUTO-SETUP: crea tablas y datos iniciales si no existen
// ════════════════════════════════════════════════════════════
async function autoSetup() {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // ── Tablas maestras ─────────────────────────────────────
    await client.query(`
      CREATE TABLE IF NOT EXISTS bodegas (
        bodega_id   SERIAL PRIMARY KEY,
        codigo      VARCHAR(20)  NOT NULL UNIQUE,
        nombre      VARCHAR(100) NOT NULL,
        ubicacion   VARCHAR(200),
        responsable VARCHAR(100),
        activo      BOOLEAN NOT NULL DEFAULT true,
        creado_en   TIMESTAMP DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS categorias (
        categoria_id SERIAL PRIMARY KEY,
        nombre       VARCHAR(80) NOT NULL UNIQUE,
        descripcion  TEXT,
        activo       BOOLEAN NOT NULL DEFAULT true,
        creado_en    TIMESTAMP DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS subcategorias (
        subcategoria_id SERIAL PRIMARY KEY,
        categoria_id    INT NOT NULL REFERENCES categorias(categoria_id),
        nombre          VARCHAR(80) NOT NULL,
        activo          BOOLEAN NOT NULL DEFAULT true,
        creado_en       TIMESTAMP DEFAULT NOW(),
        UNIQUE(categoria_id, nombre)
      );
      CREATE TABLE IF NOT EXISTS proveedores (
        proveedor_id  SERIAL PRIMARY KEY,
        rut           VARCHAR(12)  NOT NULL UNIQUE,
        nombre        VARCHAR(150) NOT NULL,
        contacto      VARCHAR(100),
        telefono      VARCHAR(30),
        email         VARCHAR(100),
        activo        BOOLEAN NOT NULL DEFAULT true,
        creado_en     TIMESTAMP DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS productos (
        producto_id        SERIAL PRIMARY KEY,
        codigo             VARCHAR(30)  NOT NULL UNIQUE,
        codigo_alternativo VARCHAR(50),
        nombre             VARCHAR(150) NOT NULL,
        descripcion        TEXT,
        subcategoria_id    INT NOT NULL REFERENCES subcategorias(subcategoria_id),
        unidad_medida      VARCHAR(20)  NOT NULL DEFAULT 'UN',
        stock_minimo       NUMERIC(12,3) DEFAULT 0,
        stock_maximo       NUMERIC(12,3),
        costo_referencia   NUMERIC(14,2) DEFAULT 0,
        activo             BOOLEAN NOT NULL DEFAULT true,
        creado_en          TIMESTAMP DEFAULT NOW(),
        modificado_en      TIMESTAMP DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS faenas (
        faena_id    SERIAL PRIMARY KEY,
        codigo      VARCHAR(20)  NOT NULL UNIQUE,
        nombre      VARCHAR(100) NOT NULL,
        descripcion TEXT,
        activo      BOOLEAN NOT NULL DEFAULT true,
        creado_en   TIMESTAMP DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS equipos (
        equipo_id    SERIAL PRIMARY KEY,
        codigo       VARCHAR(30)  NOT NULL UNIQUE,
        nombre       VARCHAR(100) NOT NULL,
        tipo         VARCHAR(50),
        faena_id     INT REFERENCES faenas(faena_id),
        patente_serie VARCHAR(50),
        activo       BOOLEAN NOT NULL DEFAULT true,
        creado_en    TIMESTAMP DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS tipos_documento (
        tipo_doc_id SERIAL PRIMARY KEY,
        codigo      VARCHAR(10) NOT NULL UNIQUE,
        nombre      VARCHAR(80) NOT NULL,
        activo      BOOLEAN NOT NULL DEFAULT true
      );
      CREATE TABLE IF NOT EXISTS motivos_movimiento (
        motivo_id SERIAL PRIMARY KEY,
        nombre    VARCHAR(100) NOT NULL,
        tipo      VARCHAR(20)  NOT NULL,
        activo    BOOLEAN NOT NULL DEFAULT true
      );
      CREATE TABLE IF NOT EXISTS ordenes_compra (
        oc_id         SERIAL PRIMARY KEY,
        numero_oc     VARCHAR(30) NOT NULL UNIQUE,
        fecha         DATE NOT NULL,
        proveedor_id  INT REFERENCES proveedores(proveedor_id),
        estado        VARCHAR(20) NOT NULL DEFAULT 'PENDIENTE',
        observaciones TEXT,
        creado_en     TIMESTAMP DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS usuarios (
        usuario_id    SERIAL PRIMARY KEY,
        email         VARCHAR(100) NOT NULL UNIQUE,
        nombre        VARCHAR(100) NOT NULL,
        password_hash VARCHAR(255) NOT NULL,
        rol           VARCHAR(30)  NOT NULL DEFAULT 'BODEGUERO',
        activo        BOOLEAN NOT NULL DEFAULT true,
        creado_en     TIMESTAMP DEFAULT NOW()
      );
    `);

    // ── Movimientos ─────────────────────────────────────────
    await client.query(`
      CREATE TABLE IF NOT EXISTS movimiento_encabezado (
        movimiento_id          SERIAL PRIMARY KEY,
        tipo_movimiento        VARCHAR(20)  NOT NULL,
        fecha                  DATE NOT NULL,
        bodega_id              INT NOT NULL REFERENCES bodegas(bodega_id),
        bodega_destino_id      INT REFERENCES bodegas(bodega_id),
        faena_id               INT REFERENCES faenas(faena_id),
        equipo_id              INT REFERENCES equipos(equipo_id),
        proveedor_id           INT REFERENCES proveedores(proveedor_id),
        tipo_doc_id            INT REFERENCES tipos_documento(tipo_doc_id),
        numero_documento       VARCHAR(30),
        fecha_documento        DATE,
        oc_referencia          VARCHAR(50),
        motivo_id              INT REFERENCES motivos_movimiento(motivo_id),
        estado                 VARCHAR(20) NOT NULL DEFAULT 'ACTIVO',
        observaciones          TEXT,
        responsable_entrega    VARCHAR(100),
        responsable_recepcion  VARCHAR(100),
        usuario                VARCHAR(100) NOT NULL DEFAULT 'sistema',
        referencia_transfer_id INT REFERENCES movimiento_encabezado(movimiento_id),
        creado_en              TIMESTAMP DEFAULT NOW(),
        anulado_en             TIMESTAMP,
        anulado_por            VARCHAR(100),
        motivo_anulacion       TEXT
      );
      CREATE TABLE IF NOT EXISTS movimiento_detalle (
        detalle_id     SERIAL PRIMARY KEY,
        movimiento_id  INT NOT NULL REFERENCES movimiento_encabezado(movimiento_id),
        producto_id    INT NOT NULL REFERENCES productos(producto_id),
        cantidad       NUMERIC(12,3) NOT NULL,
        unidad_medida  VARCHAR(20),
        costo_unitario NUMERIC(14,4) NOT NULL DEFAULT 0,
        costo_total    NUMERIC(14,2) GENERATED ALWAYS AS (cantidad * costo_unitario) STORED,
        lote           VARCHAR(50),
        observacion    TEXT
      );
      CREATE TABLE IF NOT EXISTS stock_actual (
        producto_id             INT NOT NULL REFERENCES productos(producto_id),
        bodega_id               INT NOT NULL REFERENCES bodegas(bodega_id),
        cantidad_disponible     NUMERIC(12,3) NOT NULL DEFAULT 0,
        costo_promedio_actual   NUMERIC(14,4) NOT NULL DEFAULT 0,
        ultima_actualizacion    TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (producto_id, bodega_id)
      );
      CREATE TABLE IF NOT EXISTS auditoria (
        auditoria_id     BIGSERIAL PRIMARY KEY,
        tabla_afectada   VARCHAR(60) NOT NULL,
        registro_id      INT,
        accion           VARCHAR(20) NOT NULL,
        datos_anteriores JSONB,
        datos_nuevos     JSONB,
        usuario          VARCHAR(100) NOT NULL,
        ip_origen        VARCHAR(45),
        fecha_hora       TIMESTAMP DEFAULT NOW()
      );
    `);

    // ── Índices ─────────────────────────────────────────────
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_mov_fecha   ON movimiento_encabezado(fecha);
      CREATE INDEX IF NOT EXISTS idx_mov_tipo    ON movimiento_encabezado(tipo_movimiento);
      CREATE INDEX IF NOT EXISTS idx_mov_bodega  ON movimiento_encabezado(bodega_id);
      CREATE INDEX IF NOT EXISTS idx_mov_faena   ON movimiento_encabezado(faena_id);
      CREATE INDEX IF NOT EXISTS idx_mov_equipo  ON movimiento_encabezado(equipo_id);
      CREATE INDEX IF NOT EXISTS idx_det_mov     ON movimiento_detalle(movimiento_id);
      CREATE INDEX IF NOT EXISTS idx_det_prod    ON movimiento_detalle(producto_id);
    `);

    await client.query('COMMIT');
    console.log('  [OK] Tablas verificadas/creadas');

    // ── Datos iniciales (solo si la BD está vacía) ──────────
    const { rows } = await client.query('SELECT COUNT(*) FROM bodegas');
    if (parseInt(rows[0].count) === 0) {
      await insertarDatosIniciales(client);
    }

    // ── Usuario admin inicial ───────────────────────────────
    const u = await pool.query('SELECT COUNT(*) FROM usuarios');
    if (parseInt(u.rows[0].count) === 0) {
      const hash = await bcrypt.hash('admin123', 10);
      await pool.query(
        "INSERT INTO usuarios(email,nombre,password_hash,rol) VALUES('admin@lpz.cl','Administrador',$1,'ADMINISTRADOR')",
        [hash]
      );
      console.log('  [OK] Usuario admin creado: admin@lpz.cl / admin123');
    }

  } catch (e) {
    await client.query('ROLLBACK');
    console.error('  [ERROR] autoSetup:', e.message);
  } finally {
    client.release();
  }
}

async function insertarDatosIniciales(client) {
  // Tipos de documento
  await client.query(`
    INSERT INTO tipos_documento (codigo, nombre) VALUES
      ('FAC','Factura Electrónica'),('GD','Guía de Despacho'),
      ('NC','Nota de Crédito'),('CL','Compra Local'),('AJ','Ajuste Inicial')
    ON CONFLICT DO NOTHING;
  `);
  // Motivos
  await client.query(`
    INSERT INTO motivos_movimiento (nombre, tipo) VALUES
      ('Mantención Correctiva','SALIDA'),('Mantención Preventiva','SALIDA'),
      ('Consumo Operacional','SALIDA'),('Consumo Taller','SALIDA'),
      ('Pérdida / Merma','AJUSTE'),('Diferencia Inventario Físico','AJUSTE'),
      ('Ajuste de Apertura','AJUSTE')
    ON CONFLICT DO NOTHING;
  `);
  // Bodegas
  await client.query(`
    INSERT INTO bodegas (codigo,nombre,ubicacion,responsable) VALUES
      ('BC','Bodega Central','Planta Principal','Juan Pérez'),
      ('BT','Bodega Taller','Taller Central','Pedro González'),
      ('BF3','Bodega Faena Mec 3','Faena Mecánica 3','Luis Torres'),
      ('BL','Bodega Lubricantes','Planta Principal','Carlos Muñoz')
    ON CONFLICT DO NOTHING;
  `);
  // Categorías
  await client.query(`
    INSERT INTO categorias (nombre) VALUES
      ('Repuestos'),('Insumos'),('Lubricantes'),('Herramientas'),('Consumibles')
    ON CONFLICT DO NOTHING;
  `);
  // Subcategorías
  await client.query(`
    INSERT INTO subcategorias (categoria_id, nombre)
    SELECT id, sc FROM (VALUES
      (1,'Filtros'),(1,'Sellos y Retenes'),(1,'Rodamientos'),(1,'Mangueras'),
      (2,'Soldaduras'),(2,'Discos de Corte'),(2,'Abrasivos'),
      (3,'Aceite Hidráulico'),(3,'Aceite de Motor'),(3,'Grasas'),(3,'Refrigerantes'),
      (4,'Herramientas Manuales'),(5,'Elementos de Limpieza')
    ) AS t(id,sc)
    JOIN categorias c ON c.categoria_id = t.id
    ON CONFLICT DO NOTHING;
  `);
  // Proveedores
  await client.query(`
    INSERT INTO proveedores (rut,nombre,contacto,telefono) VALUES
      ('76.543.210-5','Comercial Hidráulica Sur Ltda.','Roberto Araya','+56 9 1234 5678'),
      ('76.111.222-3','Lubricantes y Filtros del Sur S.A.','Ana Morales','+56 9 8765 4321'),
      ('77.333.444-1','Ferretería Industrial Los Ángeles','Miguel Castro','+56 43 234 5678'),
      ('76.888.999-0','Distribuidora Técnica Sur','Sandra López','+56 9 5555 1234')
    ON CONFLICT DO NOTHING;
  `);
  // Faenas
  await client.query(`
    INSERT INTO faenas (codigo,nombre,descripcion) VALUES
      ('FAE-MEC3','Faena Mec 3','Cosecha mecanizada sector 3'),
      ('FAE-MEC4','Faena Mec 4','Cosecha mecanizada sector 4'),
      ('FAE-MEC5','Faena Mec 5','Cosecha mecanizada sector 5'),
      ('TALL','Taller Central','Taller central de mantención')
    ON CONFLICT DO NOTHING;
  `);
  // Equipos
  await client.query(`
    INSERT INTO equipos (codigo,nombre,tipo,faena_id)
    SELECT cod,nom,tip,f.faena_id FROM (VALUES
      ('HARV-01','Harvester 01','Cosechador','FAE-MEC3'),
      ('HARV-02','Harvester 02','Cosechador','FAE-MEC4'),
      ('SKID-11','Skidder 11','Arrastrador','FAE-MEC3'),
      ('PROC-11','Procesadora 11','Procesador','FAE-MEC3'),
      ('EXC-PC210','Excavadora PC210','Excavadora','TALL'),
      ('CAM-LUB','Camión Lubricador','Camión','TALL'),
      ('TALL-GEN','Taller Central','Taller','TALL')
    ) AS t(cod,nom,tip,fcod)
    JOIN faenas f ON f.codigo = t.fcod
    ON CONFLICT DO NOTHING;
  `);
  // Productos
  await client.query(`
    INSERT INTO productos (codigo,nombre,subcategoria_id,unidad_medida,stock_minimo,costo_referencia)
    SELECT cod,nom,sc.subcategoria_id,um,smin::numeric,cref::numeric
    FROM (VALUES
      ('FLTR-HID-001','Filtro Hidráulico 90L',  'Filtros',          'UN',  3, 38500),
      ('FLTR-MOT-002','Filtro Aceite Motor D6E', 'Filtros',          'UN',  4, 24900),
      ('FLTR-AIR-003','Filtro Aire Primario',    'Filtros',          'UN',  2, 45000),
      ('SELL-ORB-001','Kit Sellos Orbitrol',     'Sellos y Retenes', 'KIT', 2, 67800),
      ('ROD-SKF-6205','Rodamiento SKF 6205',     'Rodamientos',      'UN',  5, 18500),
      ('MANG-HID-3/4','Manguera Hidráulica 3/4"','Mangueras',        'MT', 10,  8900),
      ('ACE-HID-68-20','Aceite Hidráulico ISO 68 (20L)','Aceite Hidráulico','BID',5,42000),
      ('ACE-MOT-15W40','Aceite Motor 15W-40 (20L)','Aceite de Motor','BID', 8, 38000),
      ('GRAS-EP2-18KG','Grasa Litio EP-2 (18kg)','Grasas',           'BAL', 3, 28500),
      ('REFR-DEX-5L',  'Refrigerante DexCool (5L)','Refrigerantes',  'GL',  6, 12500),
      ('DISC-COR-4.5', 'Disco de Corte 4.5"',   'Discos de Corte',  'UN', 20,  2200),
      ('SOLD-E6011-KG','Electrodos E6011 1/8" (kg)','Soldaduras',    'KG', 10,  4800)
    ) AS t(cod,nom,scnom,um,smin,cref)
    JOIN subcategorias sc ON sc.nombre = t.scnom
    ON CONFLICT DO NOTHING;
  `);

  // Ingresos de ejemplo
  await client.query(`
    WITH mov1 AS (
      INSERT INTO movimiento_encabezado (tipo_movimiento,fecha,bodega_id,proveedor_id,tipo_doc_id,numero_documento,fecha_documento,oc_referencia,responsable_recepcion,usuario)
      SELECT 'INGRESO','2025-01-10',b.bodega_id,p.proveedor_id,td.tipo_doc_id,'00045312','2025-01-10','OC-2025-0003','Juan Pérez','sistema'
      FROM bodegas b, proveedores p, tipos_documento td
      WHERE b.codigo='BC' AND p.rut='76.543.210-5' AND td.codigo='FAC'
      RETURNING movimiento_id
    )
    INSERT INTO movimiento_detalle (movimiento_id,producto_id,cantidad,costo_unitario)
    SELECT m.movimiento_id, p.producto_id, qty, cu
    FROM mov1 m
    CROSS JOIN (VALUES
      ('FLTR-HID-001',6,38500),('FLTR-MOT-002',8,24900),
      ('SELL-ORB-001',3,67800),('ROD-SKF-6205',10,18500)
    ) AS d(cod,qty,cu)
    JOIN productos p ON p.codigo = d.cod;
  `).catch(() => {});

  await client.query(`
    WITH mov2 AS (
      INSERT INTO movimiento_encabezado (tipo_movimiento,fecha,bodega_id,proveedor_id,tipo_doc_id,numero_documento,fecha_documento,oc_referencia,responsable_recepcion,usuario)
      SELECT 'INGRESO','2025-01-12',b.bodega_id,p.proveedor_id,td.tipo_doc_id,'00012450','2025-01-12','OC-2025-0004','Juan Pérez','sistema'
      FROM bodegas b, proveedores p, tipos_documento td
      WHERE b.codigo='BC' AND p.rut='76.111.222-3' AND td.codigo='FAC'
      RETURNING movimiento_id
    )
    INSERT INTO movimiento_detalle (movimiento_id,producto_id,cantidad,costo_unitario)
    SELECT m.movimiento_id, p.producto_id, qty, cu
    FROM mov2 m
    CROSS JOIN (VALUES
      ('ACE-HID-68-20',10,42000),('ACE-MOT-15W40',12,38000),
      ('GRAS-EP2-18KG',4,28500),('REFR-DEX-5L',8,12500)
    ) AS d(cod,qty,cu)
    JOIN productos p ON p.codigo = d.cod;
  `).catch(() => {});

  await client.query(`
    WITH mov3 AS (
      INSERT INTO movimiento_encabezado (tipo_movimiento,fecha,bodega_id,proveedor_id,tipo_doc_id,numero_documento,fecha_documento,oc_referencia,responsable_recepcion,usuario)
      SELECT 'INGRESO','2025-01-15',b.bodega_id,p.proveedor_id,td.tipo_doc_id,'00089123','2025-01-15','OC-2025-0005','Pedro González','sistema'
      FROM bodegas b, proveedores p, tipos_documento td
      WHERE b.codigo='BT' AND p.rut='77.333.444-1' AND td.codigo='FAC'
      RETURNING movimiento_id
    )
    INSERT INTO movimiento_detalle (movimiento_id,producto_id,cantidad,costo_unitario)
    SELECT m.movimiento_id, p.producto_id, qty, cu
    FROM mov3 m
    CROSS JOIN (VALUES
      ('DISC-COR-4.5',50,2200),('SOLD-E6011-KG',15,4800)
    ) AS d(cod,qty,cu)
    JOIN productos p ON p.codigo = d.cod;
  `).catch(() => {});

  // Stock inicial calculado desde los ingresos
  await client.query(`
    INSERT INTO stock_actual (producto_id, bodega_id, cantidad_disponible, costo_promedio_actual)
    SELECT md.producto_id, me.bodega_id,
           SUM(md.cantidad),
           SUM(md.cantidad * md.costo_unitario) / SUM(md.cantidad)
    FROM movimiento_detalle md
    JOIN movimiento_encabezado me ON md.movimiento_id = me.movimiento_id
    WHERE me.tipo_movimiento = 'INGRESO' AND me.estado = 'ACTIVO'
    GROUP BY md.producto_id, me.bodega_id
    ON CONFLICT (producto_id, bodega_id) DO UPDATE
      SET cantidad_disponible   = EXCLUDED.cantidad_disponible,
          costo_promedio_actual = EXCLUDED.costo_promedio_actual,
          ultima_actualizacion  = NOW();
  `).catch(() => {});

  // Salidas de ejemplo
  await client.query(`
    WITH sal1 AS (
      INSERT INTO movimiento_encabezado (tipo_movimiento,fecha,bodega_id,faena_id,equipo_id,motivo_id,observaciones,responsable_entrega,responsable_recepcion,usuario)
      SELECT 'SALIDA','2025-01-16',b.bodega_id,f.faena_id,e.equipo_id,m.motivo_id,'Cambio filtros programado','Juan Pérez','Carlos Muñoz','sistema'
      FROM bodegas b, faenas f, equipos e, motivos_movimiento m
      WHERE b.codigo='BC' AND f.codigo='FAE-MEC3' AND e.codigo='HARV-01' AND m.nombre='Mantención Correctiva'
      RETURNING movimiento_id, bodega_id
    )
    INSERT INTO movimiento_detalle (movimiento_id,producto_id,cantidad,costo_unitario)
    SELECT s.movimiento_id, p.producto_id, qty,
           COALESCE(sa.costo_promedio_actual, p.costo_referencia)
    FROM sal1 s
    CROSS JOIN (VALUES ('FLTR-HID-001',2),('FLTR-MOT-002',2)) AS d(cod,qty)
    JOIN productos p ON p.codigo = d.cod
    LEFT JOIN stock_actual sa ON sa.producto_id = p.producto_id AND sa.bodega_id = s.bodega_id;
  `).catch(() => {});

  // Actualizar stock después de salidas
  await client.query(`
    UPDATE stock_actual sa
    SET cantidad_disponible = GREATEST(0,
      sa.cantidad_disponible - COALESCE((
        SELECT SUM(md.cantidad)
        FROM movimiento_detalle md
        JOIN movimiento_encabezado me ON md.movimiento_id = me.movimiento_id
        WHERE me.tipo_movimiento = 'SALIDA' AND me.estado = 'ACTIVO'
          AND md.producto_id = sa.producto_id AND me.bodega_id = sa.bodega_id
      ),0)
    ),
    ultima_actualizacion = NOW();
  `).catch(() => {});

  console.log('  [OK] Datos iniciales insertados');
}

// ════════════════════════════════════════════════════════════
//  RUTAS AUTH
// ════════════════════════════════════════════════════════════
app.post('/api/auth/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    const r = await pool.query('SELECT * FROM usuarios WHERE email=$1 AND activo=true', [email]);
    if (!r.rows.length) return res.status(401).json({ error: 'Credenciales inválidas' });
    const ok = await bcrypt.compare(password, r.rows[0].password_hash);
    if (!ok) return res.status(401).json({ error: 'Credenciales inválidas' });
    const u = r.rows[0];
    const token = jwt.sign({ id: u.usuario_id, email: u.email, nombre: u.nombre, rol: u.rol }, JWT_SECRET, { expiresIn: '8h' });
    res.json({ token, usuario: { id: u.usuario_id, email: u.email, nombre: u.nombre, rol: u.rol } });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/api/auth/me', auth, (req, res) => res.json(req.user));

// ════════════════════════════════════════════════════════════
//  RUTAS MAESTROS
// ════════════════════════════════════════════════════════════
function crud(tabla, pk, campos) {
  const r = express.Router();
  r.get('/', auth, async (req, res) => {
    try { res.json((await pool.query(`SELECT * FROM ${tabla} ORDER BY ${pk}`)).rows); }
    catch (e) { res.status(500).json({ error: e.message }); }
  });
  r.get('/:id', auth, async (req, res) => {
    try {
      const r2 = await pool.query(`SELECT * FROM ${tabla} WHERE ${pk}=$1`, [req.params.id]);
      if (!r2.rows.length) return res.status(404).json({ error: 'No encontrado' });
      res.json(r2.rows[0]);
    } catch (e) { res.status(500).json({ error: e.message }); }
  });
  r.post('/', auth, async (req, res) => {
    try {
      const vals = campos.map(c => req.body[c]);
      const r2 = await pool.query(
        `INSERT INTO ${tabla}(${campos.join(',')}) VALUES(${campos.map((_,i)=>`$${i+1}`).join(',')}) RETURNING *`,
        vals
      );
      res.status(201).json(r2.rows[0]);
    } catch (e) { res.status(400).json({ error: e.message }); }
  });
  r.put('/:id', auth, async (req, res) => {
    try {
      const vals = [...campos.map(c => req.body[c]), req.params.id];
      const sets = campos.map((c,i)=>`${c}=$${i+1}`).join(',');
      const r2 = await pool.query(
        `UPDATE ${tabla} SET ${sets} WHERE ${pk}=$${vals.length} RETURNING *`, vals
      );
      res.json(r2.rows[0]);
    } catch (e) { res.status(400).json({ error: e.message }); }
  });
  r.patch('/:id/activo', auth, async (req, res) => {
    try {
      const r2 = await pool.query(`UPDATE ${tabla} SET activo=NOT activo WHERE ${pk}=$1 RETURNING *`, [req.params.id]);
      res.json(r2.rows[0]);
    } catch (e) { res.status(400).json({ error: e.message }); }
  });
  return r;
}

app.use('/api/bodegas',     crud('bodegas',    'bodega_id',    ['codigo','nombre','ubicacion','responsable']));
app.use('/api/categorias',  crud('categorias', 'categoria_id', ['nombre','descripcion']));
app.use('/api/subcategorias', crud('subcategorias','subcategoria_id',['categoria_id','nombre']));
app.use('/api/faenas',      crud('faenas',     'faena_id',     ['codigo','nombre','descripcion']));
app.use('/api/equipos',     crud('equipos',    'equipo_id',    ['codigo','nombre','tipo','faena_id','patente_serie']));
app.use('/api/proveedores', crud('proveedores','proveedor_id', ['rut','nombre','contacto','telefono','email']));
app.use('/api/tipos-documento', crud('tipos_documento','tipo_doc_id',['codigo','nombre']));
app.use('/api/motivos',     crud('motivos_movimiento','motivo_id',['nombre','tipo']));

// Productos (con joins)
const pr = express.Router();
pr.get('/', auth, async (req, res) => {
  try {
    const r = await pool.query(`
      SELECT p.*, sc.nombre AS subcategoria_nombre, ca.categoria_id, ca.nombre AS categoria_nombre
      FROM productos p
      JOIN subcategorias sc ON p.subcategoria_id=sc.subcategoria_id
      JOIN categorias    ca ON sc.categoria_id=ca.categoria_id
      ORDER BY p.nombre`);
    res.json(r.rows);
  } catch(e){ res.status(500).json({error:e.message}); }
});
pr.get('/:id', auth, async(req,res)=>{
  try { res.json((await pool.query('SELECT * FROM productos WHERE producto_id=$1',[req.params.id])).rows[0]); }
  catch(e){ res.status(500).json({error:e.message}); }
});
pr.post('/', auth, async(req,res)=>{
  try {
    const {codigo,nombre,descripcion,subcategoria_id,unidad_medida,stock_minimo,costo_referencia}=req.body;
    const r=await pool.query(
      'INSERT INTO productos(codigo,nombre,descripcion,subcategoria_id,unidad_medida,stock_minimo,costo_referencia) VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING *',
      [codigo,nombre,descripcion||null,subcategoria_id,unidad_medida||'UN',stock_minimo||0,costo_referencia||0]
    );
    res.status(201).json(r.rows[0]);
  } catch(e){ res.status(400).json({error:e.message}); }
});
pr.put('/:id', auth, async(req,res)=>{
  try {
    const {codigo,nombre,descripcion,subcategoria_id,unidad_medida,stock_minimo,costo_referencia}=req.body;
    const r=await pool.query(
      'UPDATE productos SET codigo=$1,nombre=$2,descripcion=$3,subcategoria_id=$4,unidad_medida=$5,stock_minimo=$6,costo_referencia=$7,modificado_en=NOW() WHERE producto_id=$8 RETURNING *',
      [codigo,nombre,descripcion||null,subcategoria_id,unidad_medida||'UN',stock_minimo||0,costo_referencia||0,req.params.id]
    );
    res.json(r.rows[0]);
  } catch(e){ res.status(400).json({error:e.message}); }
});
pr.patch('/:id/activo', auth, async(req,res)=>{
  try { res.json((await pool.query('UPDATE productos SET activo=NOT activo WHERE producto_id=$1 RETURNING *',[req.params.id])).rows[0]); }
  catch(e){ res.status(400).json({error:e.message}); }
});
app.use('/api/productos', pr);

// ════════════════════════════════════════════════════════════
//  MOVIMIENTOS
// ════════════════════════════════════════════════════════════
const mv = express.Router();
mv.get('/', auth, async (req,res) => {
  try {
    const {tipo,bodega_id,faena_id,equipo_id,desde,hasta}=req.query;
    let where=['1=1'], vals=[];
    if(tipo){vals.push(tipo);where.push(`me.tipo_movimiento=$${vals.length}`);}
    if(bodega_id){vals.push(bodega_id);where.push(`me.bodega_id=$${vals.length}`);}
    if(faena_id){vals.push(faena_id);where.push(`me.faena_id=$${vals.length}`);}
    if(equipo_id){vals.push(equipo_id);where.push(`me.equipo_id=$${vals.length}`);}
    if(desde){vals.push(desde);where.push(`me.fecha>=$${vals.length}`);}
    if(hasta){vals.push(hasta);where.push(`me.fecha<=$${vals.length}`);}
    const r=await pool.query(`
      SELECT me.*,
        b.nombre AS bodega_nombre, f.nombre AS faena_nombre,
        e.nombre AS equipo_nombre, pr.nombre AS proveedor_nombre,
        td.nombre AS tipo_doc_nombre, mot.nombre AS motivo_nombre,
        (SELECT SUM(md.costo_total) FROM movimiento_detalle md WHERE md.movimiento_id=me.movimiento_id) AS total,
        (SELECT COUNT(*) FROM movimiento_detalle md WHERE md.movimiento_id=me.movimiento_id) AS num_lineas
      FROM movimiento_encabezado me
      LEFT JOIN bodegas b ON me.bodega_id=b.bodega_id
      LEFT JOIN faenas f ON me.faena_id=f.faena_id
      LEFT JOIN equipos e ON me.equipo_id=e.equipo_id
      LEFT JOIN proveedores pr ON me.proveedor_id=pr.proveedor_id
      LEFT JOIN tipos_documento td ON me.tipo_doc_id=td.tipo_doc_id
      LEFT JOIN motivos_movimiento mot ON me.motivo_id=mot.motivo_id
      WHERE ${where.join(' AND ')}
      ORDER BY me.movimiento_id DESC`, vals);
    res.json(r.rows);
  } catch(e){ res.status(500).json({error:e.message}); }
});

mv.get('/:id/detalles', auth, async(req,res)=>{
  try {
    const r=await pool.query(`
      SELECT md.*, p.nombre AS producto_nombre, p.codigo AS producto_codigo, p.unidad_medida
      FROM movimiento_detalle md JOIN productos p ON md.producto_id=p.producto_id
      WHERE md.movimiento_id=$1`,[req.params.id]);
    res.json(r.rows);
  } catch(e){ res.status(500).json({error:e.message}); }
});

mv.post('/', auth, async(req,res)=>{
  const client=await pool.connect();
  try {
    await client.query('BEGIN');
    const{tipo_movimiento,fecha,bodega_id,faena_id,equipo_id,proveedor_id,tipo_doc_id,numero_documento,fecha_documento,oc_referencia,motivo_id,observaciones,responsable_entrega,responsable_recepcion,lineas}=req.body;
    if(!tipo_movimiento||!fecha||!bodega_id) throw new Error('Tipo, fecha y bodega son obligatorios');
    if(!lineas||!lineas.length) throw new Error('Debe incluir al menos una línea');
    if(tipo_movimiento==='SALIDA'){
      for(const l of lineas){
        const sr=await client.query('SELECT cantidad_disponible FROM stock_actual WHERE producto_id=$1 AND bodega_id=$2',[l.producto_id,bodega_id]);
        const disp=parseFloat(sr.rows[0]?.cantidad_disponible||0);
        if(parseFloat(l.cantidad)>disp){
          const pn=(await client.query('SELECT nombre FROM productos WHERE producto_id=$1',[l.producto_id])).rows[0]?.nombre||l.producto_id;
          throw new Error(`Stock insuficiente: "${pn}" — disponible: ${disp}`);
        }
      }
    }
    const mr=await client.query(`
      INSERT INTO movimiento_encabezado(tipo_movimiento,fecha,bodega_id,faena_id,equipo_id,proveedor_id,tipo_doc_id,numero_documento,fecha_documento,oc_referencia,motivo_id,observaciones,responsable_entrega,responsable_recepcion,usuario)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) RETURNING movimiento_id`,
      [tipo_movimiento,fecha,bodega_id,faena_id||null,equipo_id||null,proveedor_id||null,tipo_doc_id||null,numero_documento||null,fecha_documento||null,oc_referencia||null,motivo_id||null,observaciones||null,responsable_entrega||null,responsable_recepcion||null,req.user.email]);
    const movId=mr.rows[0].movimiento_id;
    for(const l of lineas){
      const pid=parseInt(l.producto_id), bidI=parseInt(bodega_id), qty=parseFloat(l.cantidad), cuIn=parseFloat(l.costo_unitario||0);
      const sr=await client.query('SELECT cantidad_disponible,costo_promedio_actual FROM stock_actual WHERE producto_id=$1 AND bodega_id=$2',[pid,bidI]);
      const cur=sr.rows[0]||{cantidad_disponible:0,costo_promedio_actual:0};
      const curQ=parseFloat(cur.cantidad_disponible), curCpp=parseFloat(cur.costo_promedio_actual);
      let newQ,newCpp,cu;
      if(tipo_movimiento==='INGRESO'){
        cu=cuIn; newQ=curQ+qty;
        newCpp=newQ>0?(curQ*curCpp+qty*cu)/newQ:cu;
      } else if(tipo_movimiento==='SALIDA'){
        cu=curCpp; newQ=Math.max(0,curQ-qty); newCpp=curCpp;
      } else {
        cu=cuIn||curCpp; const qA=qty;
        newQ=Math.max(0,curQ+qA);
        newCpp=qA>0&&newQ>0?(curQ*curCpp+qA*cu)/newQ:curCpp;
      }
      await client.query('INSERT INTO movimiento_detalle(movimiento_id,producto_id,cantidad,costo_unitario) VALUES($1,$2,$3,$4)',[movId,pid,qty,cu]);
      await client.query(`
        INSERT INTO stock_actual(producto_id,bodega_id,cantidad_disponible,costo_promedio_actual,ultima_actualizacion)
        VALUES($1,$2,$3,$4,NOW())
        ON CONFLICT(producto_id,bodega_id) DO UPDATE
          SET cantidad_disponible=$3,costo_promedio_actual=$4,ultima_actualizacion=NOW()`,
        [pid,bidI,newQ,newCpp]);
    }
    await client.query('COMMIT');
    res.status(201).json({ok:true,movimiento_id:movId});
  } catch(e){ await client.query('ROLLBACK'); res.status(400).json({error:e.message}); }
  finally{ client.release(); }
});

mv.patch('/:id/anular', auth, async(req,res)=>{
  try {
    const r=await pool.query(`UPDATE movimiento_encabezado SET estado='ANULADO',anulado_en=NOW(),anulado_por=$1,motivo_anulacion=$2 WHERE movimiento_id=$3 AND estado='ACTIVO' RETURNING *`,
      [req.user.email,req.body.motivo_anulacion||'Sin motivo',req.params.id]);
    if(!r.rows.length) return res.status(400).json({error:'No encontrado o ya anulado'});
    res.json({ok:true});
  } catch(e){ res.status(400).json({error:e.message}); }
});
app.use('/api/movimientos', mv);

// ════════════════════════════════════════════════════════════
//  STOCK
// ════════════════════════════════════════════════════════════
app.get('/api/stock', auth, async(req,res)=>{
  try {
    const r=await pool.query(`
      SELECT sa.producto_id, p.codigo, p.nombre AS producto_nombre, p.unidad_medida, p.stock_minimo,
             sc.nombre AS subcategoria, ca.nombre AS categoria, ca.categoria_id,
             sa.bodega_id, b.nombre AS bodega_nombre, b.codigo AS bodega_codigo,
             sa.cantidad_disponible, sa.costo_promedio_actual,
             ROUND(sa.cantidad_disponible*sa.costo_promedio_actual,0) AS valor_total,
             CASE WHEN sa.cantidad_disponible<=p.stock_minimo THEN true ELSE false END AS bajo_minimo
      FROM stock_actual sa
      JOIN productos p ON sa.producto_id=p.producto_id
      JOIN subcategorias sc ON p.subcategoria_id=sc.subcategoria_id
      JOIN categorias ca ON sc.categoria_id=ca.categoria_id
      JOIN bodegas b ON sa.bodega_id=b.bodega_id
      WHERE p.activo=true AND b.activo=true
      ORDER BY ca.nombre,sc.nombre,p.nombre`);
    res.json(r.rows);
  } catch(e){ res.status(500).json({error:e.message}); }
});

app.get('/api/stock/alertas', auth, async(req,res)=>{
  try {
    const r=await pool.query(`
      SELECT sa.producto_id, p.codigo, p.nombre AS producto_nombre,
             sa.bodega_id, b.nombre AS bodega_nombre,
             sa.cantidad_disponible, p.stock_minimo, p.unidad_medida
      FROM stock_actual sa
      JOIN productos p ON sa.producto_id=p.producto_id
      JOIN bodegas b ON sa.bodega_id=b.bodega_id
      WHERE sa.cantidad_disponible<=p.stock_minimo AND p.activo=true
      ORDER BY p.nombre`);
    res.json(r.rows);
  } catch(e){ res.status(500).json({error:e.message}); }
});

app.get('/api/stock/consolidado', auth, async(req,res)=>{
  try {
    const r=await pool.query(`
      SELECT sa.producto_id, p.codigo, p.nombre AS producto_nombre, p.unidad_medida, p.stock_minimo,
             sc.nombre AS subcategoria, ca.nombre AS categoria,
             SUM(sa.cantidad_disponible) AS cantidad_total,
             CASE WHEN SUM(sa.cantidad_disponible)>0
               THEN SUM(sa.cantidad_disponible*sa.costo_promedio_actual)/SUM(sa.cantidad_disponible)
               ELSE 0 END AS cpp_promedio,
             SUM(sa.cantidad_disponible*sa.costo_promedio_actual) AS valor_total
      FROM stock_actual sa
      JOIN productos p ON sa.producto_id=p.producto_id
      JOIN subcategorias sc ON p.subcategoria_id=sc.subcategoria_id
      JOIN categorias ca ON sc.categoria_id=ca.categoria_id
      WHERE p.activo=true
      GROUP BY sa.producto_id,p.codigo,p.nombre,p.unidad_medida,p.stock_minimo,sc.nombre,ca.nombre
      ORDER BY p.nombre`);
    res.json(r.rows);
  } catch(e){ res.status(500).json({error:e.message}); }
});

// ════════════════════════════════════════════════════════════
//  KARDEX
// ════════════════════════════════════════════════════════════
app.get('/api/kardex', auth, async(req,res)=>{
  try {
    const{producto_id,bodega_id}=req.query;
    if(!producto_id) return res.status(400).json({error:'producto_id requerido'});
    let where='md.producto_id=$1', vals=[producto_id];
    if(bodega_id){vals.push(bodega_id);where+=` AND me.bodega_id=$${vals.length}`;}
    const r=await pool.query(`
      SELECT me.movimiento_id, me.tipo_movimiento, me.fecha, me.bodega_id,
             b.nombre AS bodega_nombre,
             md.producto_id, p.codigo AS producto_codigo, p.nombre AS producto_nombre, p.unidad_medida,
             CASE WHEN me.tipo_movimiento='INGRESO' THEN md.cantidad ELSE 0 END AS entrada,
             CASE WHEN me.tipo_movimiento='SALIDA'  THEN md.cantidad ELSE 0 END AS salida,
             md.costo_unitario, md.costo_total,
             me.faena_id, f.nombre AS faena_nombre,
             me.equipo_id, e.nombre AS equipo_nombre,
             me.observaciones, me.estado
      FROM movimiento_detalle md
      JOIN movimiento_encabezado me ON md.movimiento_id=me.movimiento_id
      JOIN bodegas b ON me.bodega_id=b.bodega_id
      JOIN productos p ON md.producto_id=p.producto_id
      LEFT JOIN faenas f ON me.faena_id=f.faena_id
      LEFT JOIN equipos e ON me.equipo_id=e.equipo_id
      WHERE ${where} AND me.estado='ACTIVO'
      ORDER BY me.movimiento_id`,vals);
    res.json(r.rows);
  } catch(e){ res.status(500).json({error:e.message}); }
});

// ════════════════════════════════════════════════════════════
//  REPORTES
// ════════════════════════════════════════════════════════════
app.get('/api/reportes/consumo', auth, async(req,res)=>{
  try {
    const{desde,hasta,faena_id,equipo_id,bodega_id}=req.query;
    let where=["me.tipo_movimiento='SALIDA'","me.estado='ACTIVO'"],vals=[];
    if(desde){vals.push(desde);where.push(`me.fecha>=$${vals.length}`);}
    if(hasta){vals.push(hasta);where.push(`me.fecha<=$${vals.length}`);}
    if(faena_id){vals.push(faena_id);where.push(`me.faena_id=$${vals.length}`);}
    if(equipo_id){vals.push(equipo_id);where.push(`me.equipo_id=$${vals.length}`);}
    if(bodega_id){vals.push(bodega_id);where.push(`me.bodega_id=$${vals.length}`);}
    const r=await pool.query(`
      SELECT me.fecha,b.nombre AS bodega,f.nombre AS faena,e.nombre AS equipo,
             p.codigo AS producto_codigo,p.nombre AS producto,
             sc.nombre AS subcategoria,ca.nombre AS categoria,
             md.cantidad,p.unidad_medida,md.costo_unitario,md.costo_total
      FROM movimiento_encabezado me
      JOIN movimiento_detalle md ON me.movimiento_id=md.movimiento_id
      JOIN productos p ON md.producto_id=p.producto_id
      JOIN subcategorias sc ON p.subcategoria_id=sc.subcategoria_id
      JOIN categorias ca ON sc.categoria_id=ca.categoria_id
      JOIN bodegas b ON me.bodega_id=b.bodega_id
      LEFT JOIN faenas f ON me.faena_id=f.faena_id
      LEFT JOIN equipos e ON me.equipo_id=e.equipo_id
      WHERE ${where.join(' AND ')}
      ORDER BY me.fecha,e.nombre,p.nombre`,vals);
    res.json(r.rows);
  } catch(e){ res.status(500).json({error:e.message}); }
});

app.get('/api/reportes/ranking-productos', auth, async(req,res)=>{
  try {
    const{desde,hasta}=req.query;
    let where=["me.tipo_movimiento='SALIDA'","me.estado='ACTIVO'"],vals=[];
    if(desde){vals.push(desde);where.push(`me.fecha>=$${vals.length}`);}
    if(hasta){vals.push(hasta);where.push(`me.fecha<=$${vals.length}`);}
    const r=await pool.query(`
      SELECT p.codigo,p.nombre AS producto_nombre,p.unidad_medida,
             SUM(md.cantidad) AS cantidad_total,SUM(md.costo_total) AS costo_total,COUNT(DISTINCT me.movimiento_id) AS n
      FROM movimiento_encabezado me
      JOIN movimiento_detalle md ON me.movimiento_id=md.movimiento_id
      JOIN productos p ON md.producto_id=p.producto_id
      WHERE ${where.join(' AND ')}
      GROUP BY p.producto_id,p.codigo,p.nombre,p.unidad_medida
      ORDER BY costo_total DESC LIMIT 15`,vals);
    res.json(r.rows);
  } catch(e){ res.status(500).json({error:e.message}); }
});

app.get('/api/reportes/ranking-equipos', auth, async(req,res)=>{
  try {
    const{desde,hasta}=req.query;
    let where=["me.tipo_movimiento='SALIDA'","me.estado='ACTIVO'","me.equipo_id IS NOT NULL"],vals=[];
    if(desde){vals.push(desde);where.push(`me.fecha>=$${vals.length}`);}
    if(hasta){vals.push(hasta);where.push(`me.fecha<=$${vals.length}`);}
    const r=await pool.query(`
      SELECT e.codigo,e.nombre AS equipo_nombre,e.tipo,f.nombre AS faena_nombre,
             SUM(md.costo_total) AS costo_total,COUNT(DISTINCT me.movimiento_id) AS n
      FROM movimiento_encabezado me
      JOIN movimiento_detalle md ON me.movimiento_id=md.movimiento_id
      JOIN equipos e ON me.equipo_id=e.equipo_id
      LEFT JOIN faenas f ON e.faena_id=f.faena_id
      WHERE ${where.join(' AND ')}
      GROUP BY e.equipo_id,e.codigo,e.nombre,e.tipo,f.nombre
      ORDER BY costo_total DESC`,vals);
    res.json(r.rows);
  } catch(e){ res.status(500).json({error:e.message}); }
});

app.get('/api/reportes/ingresos', auth, async(req,res)=>{
  try {
    const{desde,hasta,proveedor_id}=req.query;
    let where=["me.tipo_movimiento='INGRESO'","me.estado='ACTIVO'"],vals=[];
    if(desde){vals.push(desde);where.push(`me.fecha>=$${vals.length}`);}
    if(hasta){vals.push(hasta);where.push(`me.fecha<=$${vals.length}`);}
    if(proveedor_id){vals.push(proveedor_id);where.push(`me.proveedor_id=$${vals.length}`);}
    const r=await pool.query(`
      SELECT me.movimiento_id,me.fecha,me.numero_documento,me.oc_referencia,
             b.nombre AS bodega_nombre,pr.nombre AS proveedor_nombre,td.nombre AS tipo_doc_nombre,
             SUM(md.costo_total) AS total_ingreso,COUNT(md.detalle_id) AS num_lineas
      FROM movimiento_encabezado me
      JOIN bodegas b ON me.bodega_id=b.bodega_id
      JOIN movimiento_detalle md ON me.movimiento_id=md.movimiento_id
      LEFT JOIN proveedores pr ON me.proveedor_id=pr.proveedor_id
      LEFT JOIN tipos_documento td ON me.tipo_doc_id=td.tipo_doc_id
      WHERE ${where.join(' AND ')}
      GROUP BY me.movimiento_id,me.fecha,me.numero_documento,me.oc_referencia,b.nombre,pr.nombre,td.nombre
      ORDER BY me.fecha DESC`,vals);
    res.json(r.rows);
  } catch(e){ res.status(500).json({error:e.message}); }
});

// ════════════════════════════════════════════════════════════
//  USUARIOS
// ════════════════════════════════════════════════════════════
app.get('/api/usuarios', auth, async(req,res)=>{
  try { res.json((await pool.query('SELECT usuario_id,email,nombre,rol,activo,creado_en FROM usuarios ORDER BY nombre')).rows); }
  catch(e){ res.status(500).json({error:e.message}); }
});
app.post('/api/usuarios', auth, async(req,res)=>{
  try {
    const{email,nombre,password,rol}=req.body;
    const hash=await bcrypt.hash(password,10);
    const r=await pool.query('INSERT INTO usuarios(email,nombre,password_hash,rol) VALUES($1,$2,$3,$4) RETURNING usuario_id,email,nombre,rol,activo',[email,nombre,hash,rol||'BODEGUERO']);
    res.status(201).json(r.rows[0]);
  } catch(e){ res.status(400).json({error:e.message}); }
});
app.patch('/api/usuarios/:id/activo', auth, async(req,res)=>{
  try { res.json((await pool.query('UPDATE usuarios SET activo=NOT activo WHERE usuario_id=$1 RETURNING *',[req.params.id])).rows[0]); }
  catch(e){ res.status(400).json({error:e.message}); }
});

// ════════════════════════════════════════════════════════════
//  HEALTH
// ════════════════════════════════════════════════════════════
app.get('/api/ping', async(req,res)=>{
  try { await pool.query('SELECT 1'); res.json({ok:true,time:new Date().toISOString()}); }
  catch(e){ res.status(500).json({ok:false,error:e.message}); }
});
app.get('*', (req,res)=>res.sendFile(path.join(__dirname,'frontend','index.html')));

// ════════════════════════════════════════════════════════════
//  ARRANQUE
// ════════════════════════════════════════════════════════════
app.listen(PORT, '0.0.0.0', async()=>{
  console.log('\n============================================================');
  console.log('  LPZ Bodegas iniciando en puerto', PORT);
  console.log('============================================================');
  let intentos = 0;
  while (intentos < 10) {
    try {
      await pool.query('SELECT 1');
      console.log('  [OK] Conexión a base de datos establecida');
      break;
    } catch(e) {
      intentos++;
      console.log(`  [ESPERA] Base de datos no lista, intento ${intentos}/10...`);
      await new Promise(r=>setTimeout(r,3000));
    }
  }
  await autoSetup();
  console.log('  [OK] Sistema listo');
  console.log('  Credenciales: admin@lpz.cl / admin123');
  console.log('============================================================\n');
});
