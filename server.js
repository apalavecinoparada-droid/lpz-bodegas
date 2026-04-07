'use strict';
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

const pool = new Pool(
  process.env.DATABASE_URL
    ? { connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } }
    : { host: process.env.DB_HOST||'localhost', port: parseInt(process.env.DB_PORT||'5432'),
        database: process.env.DB_NAME||'lpz_bodegas', user: process.env.DB_USER||'postgres',
        password: process.env.DB_PASSWORD||'postgres' }
);

app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.static(path.join(__dirname, 'frontend')));

function auth(req, res, next) {
  const h = req.headers.authorization;
  if (!h) return res.status(401).json({ error: 'No autorizado' });
  try { req.user = jwt.verify(h.split(' ')[1], JWT_SECRET); next(); }
  catch { res.status(401).json({ error: 'Token invalido' }); }
}

async function audit(tabla, id, accion, antes, despues, usr) {
  try { await pool.query('INSERT INTO auditoria(tabla_afectada,registro_id,accion,datos_anteriores,datos_nuevos,usuario) VALUES($1,$2,$3,$4,$5,$6)',[tabla,id,accion,antes?JSON.stringify(antes):null,despues?JSON.stringify(despues):null,usr]); } catch {}
}

async function autoSetup() {
  // AUTO-REPARACION: si ordenes_compra existe con estructura incorrecta, la elimina
  try {
    const cols = await pool.query(`SELECT column_name FROM information_schema.columns WHERE table_name='ordenes_compra' AND table_schema='public'`);
    if (cols.rows.length > 0) {
      const names = cols.rows.map(function(r){return r.column_name;});
      if (!names.includes('fecha_emision')) {
        await pool.query('DROP TABLE IF EXISTS ordenes_compra_detalle CASCADE');
        await pool.query('DROP TABLE IF EXISTS ordenes_compra CASCADE');
        console.log('  [FIX] Tablas OC con estructura incorrecta eliminadas — se recrearan');
      }
    }
  } catch(e) { console.log('  [WARN] Check OC:', e.message); }

  // Cada DDL corre de forma independiente — un fallo no afecta a los demas
  async function q(sql) {
    try { await pool.query(sql); } catch(e) { /* ignore: IF NOT EXISTS handles duplicates */ }
  }

  // ── Tablas base ──────────────────────────────────────────
  await q(`CREATE TABLE IF NOT EXISTS bodegas (bodega_id SERIAL PRIMARY KEY, codigo VARCHAR(20) NOT NULL UNIQUE, nombre VARCHAR(100) NOT NULL, ubicacion VARCHAR(200), responsable VARCHAR(100), activo BOOLEAN NOT NULL DEFAULT true, creado_en TIMESTAMP DEFAULT NOW())`);
  await q(`CREATE TABLE IF NOT EXISTS categorias (categoria_id SERIAL PRIMARY KEY, nombre VARCHAR(80) NOT NULL UNIQUE, descripcion TEXT, activo BOOLEAN NOT NULL DEFAULT true, creado_en TIMESTAMP DEFAULT NOW())`);
  await q(`CREATE TABLE IF NOT EXISTS subcategorias (subcategoria_id SERIAL PRIMARY KEY, categoria_id INT NOT NULL REFERENCES categorias(categoria_id), nombre VARCHAR(80) NOT NULL, activo BOOLEAN NOT NULL DEFAULT true, creado_en TIMESTAMP DEFAULT NOW(), UNIQUE(categoria_id, nombre))`);
  await q(`CREATE TABLE IF NOT EXISTS proveedores (proveedor_id SERIAL PRIMARY KEY, rut VARCHAR(12) NOT NULL UNIQUE, nombre VARCHAR(150) NOT NULL, contacto VARCHAR(100), telefono VARCHAR(30), email VARCHAR(100), activo BOOLEAN NOT NULL DEFAULT true, creado_en TIMESTAMP DEFAULT NOW())`);
  await q(`ALTER TABLE proveedores ADD COLUMN IF NOT EXISTS direccion VARCHAR(200)`);
  await q(`ALTER TABLE proveedores ADD COLUMN IF NOT EXISTS giro VARCHAR(200)`);
  await q(`CREATE TABLE IF NOT EXISTS productos (producto_id SERIAL PRIMARY KEY, codigo VARCHAR(30) NOT NULL UNIQUE, codigo_alternativo VARCHAR(50), nombre VARCHAR(150) NOT NULL, descripcion TEXT, subcategoria_id INT NOT NULL REFERENCES subcategorias(subcategoria_id), unidad_medida VARCHAR(20) NOT NULL DEFAULT 'UN', stock_minimo NUMERIC(12,3) DEFAULT 0, stock_maximo NUMERIC(12,3), costo_referencia NUMERIC(14,2) DEFAULT 0, activo BOOLEAN NOT NULL DEFAULT true, creado_en TIMESTAMP DEFAULT NOW(), modificado_en TIMESTAMP DEFAULT NOW())`);
  await q(`CREATE TABLE IF NOT EXISTS faenas (faena_id SERIAL PRIMARY KEY, codigo VARCHAR(20) NOT NULL UNIQUE, nombre VARCHAR(100) NOT NULL, descripcion TEXT, activo BOOLEAN NOT NULL DEFAULT true, creado_en TIMESTAMP DEFAULT NOW())`);
  await q(`CREATE TABLE IF NOT EXISTS equipos (equipo_id SERIAL PRIMARY KEY, codigo VARCHAR(30) NOT NULL UNIQUE, nombre VARCHAR(100) NOT NULL, tipo VARCHAR(50), faena_id INT REFERENCES faenas(faena_id), patente_serie VARCHAR(50), activo BOOLEAN NOT NULL DEFAULT true, creado_en TIMESTAMP DEFAULT NOW())`);
  await q(`ALTER TABLE equipos ADD COLUMN IF NOT EXISTS marca VARCHAR(80)`);
  await q(`ALTER TABLE equipos ADD COLUMN IF NOT EXISTS modelo VARCHAR(80)`);
  await q(`ALTER TABLE equipos ADD COLUMN IF NOT EXISTS anio INT`);
  await q(`ALTER TABLE equipos ADD COLUMN IF NOT EXISTS placa_patente VARCHAR(30)`);
  await q(`ALTER TABLE equipos ADD COLUMN IF NOT EXISTS num_chasis VARCHAR(50)`);
  await q(`ALTER TABLE equipos ADD COLUMN IF NOT EXISTS empresa_id INT REFERENCES empresas(empresa_id)`);
  // Faenas por empresa (v2.1)
  await q(`ALTER TABLE faenas ADD COLUMN IF NOT EXISTS empresa_id INT REFERENCES empresas(empresa_id)`);
  // Logo empresa (v2.1)
  await q(`ALTER TABLE empresas ADD COLUMN IF NOT EXISTS logo_base64 TEXT`);
  // OC auditoria reapertura (v2.1)
  await q(`ALTER TABLE ordenes_compra ADD COLUMN IF NOT EXISTS reabierto_en TIMESTAMP`);
  await q(`ALTER TABLE ordenes_compra ADD COLUMN IF NOT EXISTS reabierto_por VARCHAR(100)`);
  await q(`ALTER TABLE ordenes_compra ADD COLUMN IF NOT EXISTS motivo_reapertura TEXT`);
  await q(`CREATE TABLE IF NOT EXISTS tipos_documento (tipo_doc_id SERIAL PRIMARY KEY, codigo VARCHAR(10) NOT NULL UNIQUE, nombre VARCHAR(80) NOT NULL, activo BOOLEAN NOT NULL DEFAULT true)`);
  await q(`CREATE TABLE IF NOT EXISTS motivos_movimiento (motivo_id SERIAL PRIMARY KEY, nombre VARCHAR(100) NOT NULL, tipo VARCHAR(20) NOT NULL, activo BOOLEAN NOT NULL DEFAULT true)`);
  await q(`CREATE TABLE IF NOT EXISTS usuarios (usuario_id SERIAL PRIMARY KEY, email VARCHAR(100) NOT NULL UNIQUE, nombre VARCHAR(100) NOT NULL, password_hash VARCHAR(255) NOT NULL, rol VARCHAR(30) NOT NULL DEFAULT 'BODEGUERO', activo BOOLEAN NOT NULL DEFAULT true, creado_en TIMESTAMP DEFAULT NOW())`);
  // ── Tablas nuevas v2 ─────────────────────────────────────
  await q(`CREATE TABLE IF NOT EXISTS empresas (empresa_id SERIAL PRIMARY KEY, rut VARCHAR(15) NOT NULL UNIQUE, razon_social VARCHAR(150) NOT NULL, direccion VARCHAR(200), ciudad VARCHAR(100), giro VARCHAR(200), telefono VARCHAR(30), email VARCHAR(100), activo BOOLEAN NOT NULL DEFAULT true, creado_en TIMESTAMP DEFAULT NOW(), modificado_en TIMESTAMP DEFAULT NOW())`);
  await q(`CREATE TABLE IF NOT EXISTS condiciones_pago (condicion_id SERIAL PRIMARY KEY, nombre VARCHAR(100) NOT NULL UNIQUE, descripcion TEXT, activo BOOLEAN NOT NULL DEFAULT true, creado_en TIMESTAMP DEFAULT NOW())`);
  // ── Movimientos ──────────────────────────────────────────
  await q(`CREATE TABLE IF NOT EXISTS movimiento_encabezado (movimiento_id SERIAL PRIMARY KEY, tipo_movimiento VARCHAR(20) NOT NULL, fecha DATE NOT NULL, bodega_id INT NOT NULL REFERENCES bodegas(bodega_id), bodega_destino_id INT REFERENCES bodegas(bodega_id), faena_id INT REFERENCES faenas(faena_id), equipo_id INT REFERENCES equipos(equipo_id), proveedor_id INT REFERENCES proveedores(proveedor_id), tipo_doc_id INT REFERENCES tipos_documento(tipo_doc_id), numero_documento VARCHAR(30), fecha_documento DATE, oc_referencia VARCHAR(50), motivo_id INT REFERENCES motivos_movimiento(motivo_id), estado VARCHAR(20) NOT NULL DEFAULT 'ACTIVO', observaciones TEXT, responsable_entrega VARCHAR(100), responsable_recepcion VARCHAR(100), usuario VARCHAR(100) NOT NULL DEFAULT 'sistema', referencia_transfer_id INT REFERENCES movimiento_encabezado(movimiento_id), creado_en TIMESTAMP DEFAULT NOW(), anulado_en TIMESTAMP, anulado_por VARCHAR(100), motivo_anulacion TEXT)`);
  await q(`CREATE TABLE IF NOT EXISTS movimiento_detalle (detalle_id SERIAL PRIMARY KEY, movimiento_id INT NOT NULL REFERENCES movimiento_encabezado(movimiento_id), producto_id INT NOT NULL REFERENCES productos(producto_id), cantidad NUMERIC(12,3) NOT NULL, unidad_medida VARCHAR(20), costo_unitario NUMERIC(14,4) NOT NULL DEFAULT 0, costo_total NUMERIC(14,2) GENERATED ALWAYS AS (cantidad * costo_unitario) STORED, lote VARCHAR(50), observacion TEXT)`);
  await q(`CREATE TABLE IF NOT EXISTS stock_actual (producto_id INT NOT NULL REFERENCES productos(producto_id), bodega_id INT NOT NULL REFERENCES bodegas(bodega_id), cantidad_disponible NUMERIC(12,3) NOT NULL DEFAULT 0, costo_promedio_actual NUMERIC(14,4) NOT NULL DEFAULT 0, ultima_actualizacion TIMESTAMP DEFAULT NOW(), PRIMARY KEY (producto_id, bodega_id))`);
  await q(`CREATE TABLE IF NOT EXISTS auditoria (auditoria_id BIGSERIAL PRIMARY KEY, tabla_afectada VARCHAR(60) NOT NULL, registro_id INT, accion VARCHAR(20) NOT NULL, datos_anteriores JSONB, datos_nuevos JSONB, usuario VARCHAR(100) NOT NULL, ip_origen VARCHAR(45), fecha_hora TIMESTAMP DEFAULT NOW())`);
  // ── OC ───────────────────────────────────────────────────
  await q(`CREATE SEQUENCE IF NOT EXISTS seq_oc_num START 1`);
  await q(`CREATE TABLE IF NOT EXISTS ordenes_compra (oc_id SERIAL PRIMARY KEY, numero_oc VARCHAR(30) NOT NULL UNIQUE, empresa_id INT REFERENCES empresas(empresa_id), proveedor_id INT REFERENCES proveedores(proveedor_id), fecha_emision DATE, solicitante VARCHAR(100), retira VARCHAR(100), condicion_id INT REFERENCES condiciones_pago(condicion_id), estado VARCHAR(20) NOT NULL DEFAULT 'PENDIENTE', impuesto_adicional NUMERIC(14,2) DEFAULT 0, neto NUMERIC(14,2) DEFAULT 0, iva NUMERIC(14,2) DEFAULT 0, total NUMERIC(14,2) DEFAULT 0, tipo_doc_id INT REFERENCES tipos_documento(tipo_doc_id), numero_documento VARCHAR(30), fecha_documento DATE, bodega_ingreso_id INT REFERENCES bodegas(bodega_id), movimiento_id INT REFERENCES movimiento_encabezado(movimiento_id), observaciones TEXT, usuario VARCHAR(100), creado_en TIMESTAMP DEFAULT NOW(), modificado_en TIMESTAMP DEFAULT NOW(), anulado_en TIMESTAMP, anulado_por VARCHAR(100))`);
  await q(`CREATE TABLE IF NOT EXISTS ordenes_compra_detalle (detalle_id SERIAL PRIMARY KEY, oc_id INT NOT NULL REFERENCES ordenes_compra(oc_id) ON DELETE CASCADE, linea_num INT, descripcion TEXT, producto_id INT REFERENCES productos(producto_id), subcategoria_id INT REFERENCES subcategorias(subcategoria_id), faena_id INT REFERENCES faenas(faena_id), equipo_id INT REFERENCES equipos(equipo_id), cantidad NUMERIC(12,3) NOT NULL DEFAULT 0, precio_unitario NUMERIC(14,4) DEFAULT 0, total_linea NUMERIC(14,2) GENERATED ALWAYS AS (cantidad * precio_unitario) STORED, ingresa_bodega BOOLEAN DEFAULT false, bodega_destino_id INT REFERENCES bodegas(bodega_id))`);
  // ── Patch columnas OC faltantes (BD con tabla incompleta) ─
  const ocPatch = [
    "ALTER TABLE ordenes_compra ADD COLUMN IF NOT EXISTS fecha_emision DATE",
    "ALTER TABLE ordenes_compra ADD COLUMN IF NOT EXISTS empresa_id INT",
    "ALTER TABLE ordenes_compra ADD COLUMN IF NOT EXISTS solicitante VARCHAR(100)",
    "ALTER TABLE ordenes_compra ADD COLUMN IF NOT EXISTS retira VARCHAR(100)",
    "ALTER TABLE ordenes_compra ADD COLUMN IF NOT EXISTS condicion_id INT",
    "ALTER TABLE ordenes_compra ADD COLUMN IF NOT EXISTS estado VARCHAR(20) DEFAULT 'PENDIENTE'",
    "ALTER TABLE ordenes_compra ADD COLUMN IF NOT EXISTS impuesto_adicional NUMERIC(14,2) DEFAULT 0",
    "ALTER TABLE ordenes_compra ADD COLUMN IF NOT EXISTS neto NUMERIC(14,2) DEFAULT 0",
    "ALTER TABLE ordenes_compra ADD COLUMN IF NOT EXISTS iva NUMERIC(14,2) DEFAULT 0",
    "ALTER TABLE ordenes_compra ADD COLUMN IF NOT EXISTS total NUMERIC(14,2) DEFAULT 0",
    "ALTER TABLE ordenes_compra ADD COLUMN IF NOT EXISTS tipo_doc_id INT",
    "ALTER TABLE ordenes_compra ADD COLUMN IF NOT EXISTS numero_documento VARCHAR(30)",
    "ALTER TABLE ordenes_compra ADD COLUMN IF NOT EXISTS fecha_documento DATE",
    "ALTER TABLE ordenes_compra ADD COLUMN IF NOT EXISTS bodega_ingreso_id INT",
    "ALTER TABLE ordenes_compra ADD COLUMN IF NOT EXISTS movimiento_id INT",
    "ALTER TABLE ordenes_compra ADD COLUMN IF NOT EXISTS observaciones TEXT",
    "ALTER TABLE ordenes_compra ADD COLUMN IF NOT EXISTS usuario VARCHAR(100)",
    "ALTER TABLE ordenes_compra ADD COLUMN IF NOT EXISTS creado_en TIMESTAMP DEFAULT NOW()",
    "ALTER TABLE ordenes_compra ADD COLUMN IF NOT EXISTS modificado_en TIMESTAMP DEFAULT NOW()",
    "ALTER TABLE ordenes_compra ADD COLUMN IF NOT EXISTS anulado_en TIMESTAMP",
    "ALTER TABLE ordenes_compra ADD COLUMN IF NOT EXISTS anulado_por VARCHAR(100)",
    "ALTER TABLE ordenes_compra_detalle ADD COLUMN IF NOT EXISTS linea_num INT",
    "ALTER TABLE ordenes_compra_detalle ADD COLUMN IF NOT EXISTS descripcion TEXT",
    "ALTER TABLE ordenes_compra_detalle ADD COLUMN IF NOT EXISTS subcategoria_id INT",
    "ALTER TABLE ordenes_compra_detalle ADD COLUMN IF NOT EXISTS faena_id INT",
    "ALTER TABLE ordenes_compra_detalle ADD COLUMN IF NOT EXISTS equipo_id INT",
    "ALTER TABLE ordenes_compra_detalle ADD COLUMN IF NOT EXISTS ingresa_bodega BOOLEAN DEFAULT false",
    "ALTER TABLE ordenes_compra_detalle ADD COLUMN IF NOT EXISTS bodega_destino_id INT",
  ];
  for (const sql of ocPatch) { await q(sql); }
  // ── Indices (cada uno independiente) ─────────────────────
  const idxList = [
    "CREATE INDEX IF NOT EXISTS idx_mov_fecha ON movimiento_encabezado(fecha)",
    "CREATE INDEX IF NOT EXISTS idx_mov_tipo ON movimiento_encabezado(tipo_movimiento)",
    "CREATE INDEX IF NOT EXISTS idx_mov_bodega ON movimiento_encabezado(bodega_id)",
    "CREATE INDEX IF NOT EXISTS idx_mov_faena ON movimiento_encabezado(faena_id)",
    "CREATE INDEX IF NOT EXISTS idx_mov_equipo ON movimiento_encabezado(equipo_id)",
    "CREATE INDEX IF NOT EXISTS idx_det_mov ON movimiento_detalle(movimiento_id)",
    "CREATE INDEX IF NOT EXISTS idx_det_prod ON movimiento_detalle(producto_id)",
    "CREATE INDEX IF NOT EXISTS idx_oc_estado ON ordenes_compra(estado)",
    "CREATE INDEX IF NOT EXISTS idx_oc_prov ON ordenes_compra(proveedor_id)",
    "CREATE INDEX IF NOT EXISTS idx_oc_fecha ON ordenes_compra(fecha_emision)",
  ];
  for (const sql of idxList) { await q(sql); }
  console.log('  [OK] Tablas verificadas');
  // ── Datos iniciales ───────────────────────────────────────
  try {
    const {rows} = await pool.query('SELECT COUNT(*) FROM bodegas');
    if (parseInt(rows[0].count) === 0) {
      const client = await pool.connect();
      try { await insertarDatosIniciales(client); } finally { client.release(); }
    }
  } catch(e) { console.error('  [WARN] Datos iniciales:', e.message); }
  try {
    const u = await pool.query('SELECT COUNT(*) FROM usuarios');
    if (parseInt(u.rows[0].count) === 0) {
      const hash = await bcrypt.hash('admin123', 10);
      await pool.query("INSERT INTO usuarios(email,nombre,password_hash,rol) VALUES('admin@lpz.cl','Administrador',$1,'ADMINISTRADOR')",[hash]);
    }
  } catch(e) {}
  try {
    const emp = await pool.query('SELECT COUNT(*) FROM empresas');
    if (parseInt(emp.rows[0].count) === 0) {
      await pool.query("INSERT INTO empresas(rut,razon_social,giro) VALUES('76.543.210-1','Leonidas Poo Zenteno y Cia. Ltda.','Explotacion Forestal') ON CONFLICT DO NOTHING");
    }
  } catch(e) {}
  try {
    const cp = await pool.query('SELECT COUNT(*) FROM condiciones_pago');
    if (parseInt(cp.rows[0].count) === 0) {
      await pool.query("INSERT INTO condiciones_pago(nombre) VALUES('Contado'),('30 dias'),('60 dias'),('90 dias') ON CONFLICT DO NOTHING");
    }
  } catch(e) {}
}

async function insertarDatosIniciales(client) {
  await client.query(`INSERT INTO tipos_documento(codigo,nombre) VALUES('FAC','Factura Electronica'),('GD','Guia de Despacho'),('NC','Nota de Credito'),('CL','Compra Local'),('AJ','Ajuste Inicial') ON CONFLICT DO NOTHING`);
  await client.query(`INSERT INTO motivos_movimiento(nombre,tipo) VALUES('Mantencion Correctiva','SALIDA'),('Mantencion Preventiva','SALIDA'),('Consumo Operacional','SALIDA'),('Consumo Taller','SALIDA'),('Perdida / Merma','AJUSTE'),('Diferencia Inventario Fisico','AJUSTE'),('Ajuste de Apertura','AJUSTE') ON CONFLICT DO NOTHING`);
  await client.query(`INSERT INTO bodegas(codigo,nombre,ubicacion,responsable) VALUES('BC','Bodega Central','Planta Principal','Juan Perez'),('BT','Bodega Taller','Taller Central','Pedro Gonzalez'),('BF3','Bodega Faena Mec 3','Faena Mecanica 3','Luis Torres'),('BL','Bodega Lubricantes','Planta Principal','Carlos Munoz') ON CONFLICT DO NOTHING`);
  await client.query(`INSERT INTO categorias(nombre) VALUES('Repuestos'),('Insumos'),('Lubricantes'),('Herramientas'),('Consumibles') ON CONFLICT DO NOTHING`);
  await client.query(`INSERT INTO subcategorias(categoria_id,nombre) SELECT id,sc FROM (VALUES(1,'Filtros'),(1,'Sellos y Retenes'),(1,'Rodamientos'),(1,'Mangueras'),(2,'Soldaduras'),(2,'Discos de Corte'),(2,'Abrasivos'),(3,'Aceite Hidraulico'),(3,'Aceite de Motor'),(3,'Grasas'),(3,'Refrigerantes'),(4,'Herramientas Manuales'),(5,'Elementos de Limpieza'))AS t(id,sc) JOIN categorias c ON c.categoria_id=t.id ON CONFLICT DO NOTHING`);
  await client.query(`INSERT INTO proveedores(rut,nombre,contacto,telefono) VALUES('76.543.210-5','Comercial Hidraulica Sur Ltda.','Roberto Araya','+56 9 1234 5678'),('76.111.222-3','Lubricantes y Filtros del Sur S.A.','Ana Morales','+56 9 8765 4321'),('77.333.444-1','Ferreteria Industrial Los Angeles','Miguel Castro','+56 43 234 5678'),('76.888.999-0','Distribuidora Tecnica Sur','Sandra Lopez','+56 9 5555 1234') ON CONFLICT DO NOTHING`);
  await client.query(`INSERT INTO faenas(codigo,nombre,descripcion) VALUES('FAE-MEC3','Faena Mec 3','Cosecha mecanizada sector 3'),('FAE-MEC4','Faena Mec 4','Cosecha mecanizada sector 4'),('FAE-MEC5','Faena Mec 5','Cosecha mecanizada sector 5'),('TALL','Taller Central','Taller central de mantencion') ON CONFLICT DO NOTHING`);
  await client.query(`INSERT INTO equipos(codigo,nombre,tipo,faena_id) SELECT cod,nom,tip,f.faena_id FROM(VALUES('HARV-01','Harvester 01','Cosechador','FAE-MEC3'),('HARV-02','Harvester 02','Cosechador','FAE-MEC4'),('SKID-11','Skidder 11','Arrastrador','FAE-MEC3'),('PROC-11','Procesadora 11','Procesador','FAE-MEC3'),('EXC-PC210','Excavadora PC210','Excavadora','TALL'),('CAM-LUB','Camion Lubricador','Camion','TALL'),('TALL-GEN','Taller Central','Taller','TALL'))AS t(cod,nom,tip,fcod) JOIN faenas f ON f.codigo=t.fcod ON CONFLICT DO NOTHING`);
  await client.query(`INSERT INTO productos(codigo,nombre,subcategoria_id,unidad_medida,stock_minimo,costo_referencia) SELECT cod,nom,sc.subcategoria_id,um,smin::numeric,cref::numeric FROM(VALUES('FLTR-HID-001','Filtro Hidraulico 90L','Filtros','UN',3,38500),('FLTR-MOT-002','Filtro Aceite Motor D6E','Filtros','UN',4,24900),('FLTR-AIR-003','Filtro Aire Primario','Filtros','UN',2,45000),('SELL-ORB-001','Kit Sellos Orbitrol','Sellos y Retenes','KIT',2,67800),('ROD-SKF-6205','Rodamiento SKF 6205','Rodamientos','UN',5,18500),('MANG-HID-3/4','Manguera Hidraulica 3/4','Mangueras','MT',10,8900),('ACE-HID-68-20','Aceite Hidraulico ISO 68 (20L)','Aceite Hidraulico','BID',5,42000),('ACE-MOT-15W40','Aceite Motor 15W-40 (20L)','Aceite de Motor','BID',8,38000),('GRAS-EP2-18KG','Grasa Litio EP-2 (18kg)','Grasas','BAL',3,28500),('REFR-DEX-5L','Refrigerante DexCool (5L)','Refrigerantes','GL',6,12500),('DISC-COR-4.5','Disco de Corte 4.5','Discos de Corte','UN',20,2200),('SOLD-E6011-KG','Electrodos E6011 1/8 (kg)','Soldaduras','KG',10,4800))AS t(cod,nom,scnom,um,smin,cref) JOIN subcategorias sc ON sc.nombre=t.scnom ON CONFLICT DO NOTHING`);
  await client.query(`WITH m1 AS(INSERT INTO movimiento_encabezado(tipo_movimiento,fecha,bodega_id,proveedor_id,tipo_doc_id,numero_documento,fecha_documento,responsable_recepcion,usuario) SELECT 'INGRESO','2025-01-10',b.bodega_id,p.proveedor_id,td.tipo_doc_id,'00045312','2025-01-10','Juan Perez','sistema' FROM bodegas b,proveedores p,tipos_documento td WHERE b.codigo='BC' AND p.rut='76.543.210-5' AND td.codigo='FAC' RETURNING movimiento_id) INSERT INTO movimiento_detalle(movimiento_id,producto_id,cantidad,costo_unitario) SELECT m.movimiento_id,p.producto_id,qty,cu FROM m1 m CROSS JOIN(VALUES('FLTR-HID-001',6,38500),('FLTR-MOT-002',8,24900),('SELL-ORB-001',3,67800),('ROD-SKF-6205',10,18500))AS d(cod,qty,cu) JOIN productos p ON p.codigo=d.cod`).catch(()=>{});
  await client.query(`WITH m2 AS(INSERT INTO movimiento_encabezado(tipo_movimiento,fecha,bodega_id,proveedor_id,tipo_doc_id,numero_documento,fecha_documento,responsable_recepcion,usuario) SELECT 'INGRESO','2025-01-12',b.bodega_id,p.proveedor_id,td.tipo_doc_id,'00012450','2025-01-12','Juan Perez','sistema' FROM bodegas b,proveedores p,tipos_documento td WHERE b.codigo='BC' AND p.rut='76.111.222-3' AND td.codigo='FAC' RETURNING movimiento_id) INSERT INTO movimiento_detalle(movimiento_id,producto_id,cantidad,costo_unitario) SELECT m.movimiento_id,p.producto_id,qty,cu FROM m2 m CROSS JOIN(VALUES('ACE-HID-68-20',10,42000),('ACE-MOT-15W40',12,38000),('GRAS-EP2-18KG',4,28500),('REFR-DEX-5L',8,12500))AS d(cod,qty,cu) JOIN productos p ON p.codigo=d.cod`).catch(()=>{});
  await client.query(`INSERT INTO stock_actual(producto_id,bodega_id,cantidad_disponible,costo_promedio_actual) SELECT md.producto_id,me.bodega_id,SUM(md.cantidad),SUM(md.cantidad*md.costo_unitario)/SUM(md.cantidad) FROM movimiento_detalle md JOIN movimiento_encabezado me ON md.movimiento_id=me.movimiento_id WHERE me.tipo_movimiento='INGRESO' AND me.estado='ACTIVO' GROUP BY md.producto_id,me.bodega_id ON CONFLICT(producto_id,bodega_id) DO UPDATE SET cantidad_disponible=EXCLUDED.cantidad_disponible,costo_promedio_actual=EXCLUDED.costo_promedio_actual`).catch(()=>{});
  await client.query(`WITH s1 AS(INSERT INTO movimiento_encabezado(tipo_movimiento,fecha,bodega_id,faena_id,equipo_id,motivo_id,observaciones,responsable_entrega,responsable_recepcion,usuario) SELECT 'SALIDA','2025-01-16',b.bodega_id,f.faena_id,e.equipo_id,m.motivo_id,'Cambio filtros','Juan Perez','Carlos Munoz','sistema' FROM bodegas b,faenas f,equipos e,motivos_movimiento m WHERE b.codigo='BC' AND f.codigo='FAE-MEC3' AND e.codigo='HARV-01' AND m.nombre='Mantencion Correctiva' RETURNING movimiento_id,bodega_id) INSERT INTO movimiento_detalle(movimiento_id,producto_id,cantidad,costo_unitario) SELECT s.movimiento_id,p.producto_id,qty,COALESCE(sa.costo_promedio_actual,p.costo_referencia) FROM s1 s CROSS JOIN(VALUES('FLTR-HID-001',2),('FLTR-MOT-002',2))AS d(cod,qty) JOIN productos p ON p.codigo=d.cod LEFT JOIN stock_actual sa ON sa.producto_id=p.producto_id AND sa.bodega_id=s.bodega_id`).catch(()=>{});
  await client.query(`UPDATE stock_actual sa SET cantidad_disponible=GREATEST(0,sa.cantidad_disponible-COALESCE((SELECT SUM(md.cantidad) FROM movimiento_detalle md JOIN movimiento_encabezado me ON md.movimiento_id=me.movimiento_id WHERE me.tipo_movimiento='SALIDA' AND me.estado='ACTIVO' AND md.producto_id=sa.producto_id AND me.bodega_id=sa.bodega_id),0)),ultima_actualizacion=NOW()`).catch(()=>{});
  console.log('  [OK] Datos iniciales insertados');
}

// ── AUTH ──
app.post('/api/auth/login', async(req,res)=>{
  try{
    const{email,password}=req.body;
    const r=await pool.query('SELECT * FROM usuarios WHERE email=$1 AND activo=true',[email]);
    if(!r.rows.length) return res.status(401).json({error:'Credenciales invalidas'});
    const ok=await bcrypt.compare(password,r.rows[0].password_hash);
    if(!ok) return res.status(401).json({error:'Credenciales invalidas'});
    const u=r.rows[0];
    const token=jwt.sign({id:u.usuario_id,email:u.email,nombre:u.nombre,rol:u.rol},JWT_SECRET,{expiresIn:'8h'});
    res.json({token,usuario:{id:u.usuario_id,email:u.email,nombre:u.nombre,rol:u.rol}});
  }catch(e){res.status(500).json({error:e.message});}
});
app.get('/api/auth/me', auth, (req,res)=>res.json(req.user));

// ── CRUD HELPER ──
function crud(tabla, pk, campos) {
  const r = express.Router();
  r.get('/', auth, async(req,res)=>{try{res.json((await pool.query(`SELECT * FROM ${tabla} ORDER BY ${pk}`)).rows);}catch(e){res.status(500).json({error:e.message});}});
  r.get('/:id', auth, async(req,res)=>{try{const r2=await pool.query(`SELECT * FROM ${tabla} WHERE ${pk}=$1`,[req.params.id]);if(!r2.rows.length)return res.status(404).json({error:'No encontrado'});res.json(r2.rows[0]);}catch(e){res.status(500).json({error:e.message});}});
  r.post('/', auth, async(req,res)=>{
    try{
      const intFields=['empresa_id','faena_id','equipo_id','categoria_id','subcategoria_id','bodega_id','proveedor_id','tipo_doc_id','motivo_id','condicion_id'];
      const vals=campos.map(function(c){
        const v=req.body[c];
        if(v===''||v===null||v===undefined)return null;
        if(intFields.includes(c)&&!isNaN(v))return parseInt(v);
        return v;
      });
      const r2=await pool.query(`INSERT INTO ${tabla}(${campos.join(',')}) VALUES(${campos.map((_,i)=>`$${i+1}`).join(',')}) RETURNING *`,vals);
      res.status(201).json(r2.rows[0]);
    }catch(e){res.status(400).json({error:e.message});}
  });
  r.put('/:id', auth, async(req,res)=>{try{const vals=[...campos.map(c=>req.body[c]),req.params.id];const sets=campos.map((c,i)=>`${c}=$${i+1}`).join(',');const r2=await pool.query(`UPDATE ${tabla} SET ${sets} WHERE ${pk}=$${vals.length} RETURNING *`,vals);res.json(r2.rows[0]);}catch(e){res.status(400).json({error:e.message});}});
  r.patch('/:id/activo', auth, async(req,res)=>{try{const r2=await pool.query(`UPDATE ${tabla} SET activo=NOT activo WHERE ${pk}=$1 RETURNING *`,[req.params.id]);res.json(r2.rows[0]);}catch(e){res.status(400).json({error:e.message});}});
  r.delete('/:id', auth, async(req,res)=>{
    try{
      await pool.query(`DELETE FROM ${tabla} WHERE ${pk}=$1`,[req.params.id]);
      res.json({ok:true});
    }catch(e){
      if(e.code==='23503') return res.status(409).json({error:'No se puede eliminar: este registro esta en uso. Use Inactivar en su lugar.'});
      res.status(400).json({error:e.message});
    }
  });
  return r;
}

app.use('/api/bodegas',     crud('bodegas',    'bodega_id',    ['codigo','nombre','ubicacion','responsable']));
app.use('/api/categorias',  crud('categorias', 'categoria_id', ['nombre','descripcion']));
app.use('/api/subcategorias', crud('subcategorias','subcategoria_id',['categoria_id','nombre']));
// Faenas con resolución automática de empresa_id desde RUT
const faenaRouter = express.Router();
faenaRouter.get('/', auth, async(req,res)=>{
  try{res.json((await pool.query('SELECT f.*,e.razon_social AS empresa_nombre FROM faenas f LEFT JOIN empresas e ON f.empresa_id=e.empresa_id ORDER BY e.razon_social NULLS LAST,f.nombre')).rows);}
  catch(e){res.status(500).json({error:e.message});}
});
faenaRouter.post('/', auth, async(req,res)=>{
  try{
    let{codigo,nombre,descripcion,empresa_id}=req.body;
    // Auto-resolver: si empresa_id contiene letras o guiones, es un RUT
    if(empresa_id&&!/^\d+$/.test(String(empresa_id).trim())){
      const rut=String(empresa_id).replace(/\./g,'').replace(/-/g,'').trim();
      const er=await pool.query("SELECT empresa_id FROM empresas WHERE REPLACE(REPLACE(rut,'.',''),'-','')=REPLACE(REPLACE($1,'.',''),'-','') LIMIT 1",[String(empresa_id)]);
      if(!er.rows.length)throw new Error('Empresa no encontrada con RUT: '+empresa_id);
      empresa_id=er.rows[0].empresa_id;
    }
    empresa_id=empresa_id?parseInt(empresa_id):null;
    const r=await pool.query('INSERT INTO faenas(codigo,nombre,descripcion,empresa_id) VALUES($1,$2,$3,$4) RETURNING *',[codigo,nombre||null,descripcion||null,empresa_id]);
    res.status(201).json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
faenaRouter.put('/:id', auth, async(req,res)=>{
  try{
    let{codigo,nombre,descripcion,empresa_id}=req.body;
    if(empresa_id&&!/^\d+$/.test(String(empresa_id).trim())){
      const er=await pool.query("SELECT empresa_id FROM empresas WHERE REPLACE(REPLACE(rut,'.',''),'-','')=REPLACE(REPLACE($1,'.',''),'-','') LIMIT 1",[String(empresa_id)]);
      if(!er.rows.length)throw new Error('Empresa no encontrada con RUT: '+empresa_id);
      empresa_id=er.rows[0].empresa_id;
    }
    empresa_id=empresa_id?parseInt(empresa_id):null;
    const r=await pool.query('UPDATE faenas SET codigo=$1,nombre=$2,descripcion=$3,empresa_id=$4 WHERE faena_id=$5 RETURNING *',[codigo,nombre||null,descripcion||null,empresa_id,req.params.id]);
    res.json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
faenaRouter.patch('/:id/activo', auth, async(req,res)=>{
  try{res.json((await pool.query('UPDATE faenas SET activo=NOT activo WHERE faena_id=$1 RETURNING *',[req.params.id])).rows[0]);}
  catch(e){res.status(400).json({error:e.message});}
});
faenaRouter.delete('/:id', auth, async(req,res)=>{
  try{
    await pool.query('DELETE FROM faenas WHERE faena_id=$1',[req.params.id]);
    res.json({ok:true});
  }catch(e){
    if(e.code==='23503')return res.status(409).json({error:'No se puede eliminar: esta en uso. Use Inactivar.'});
    res.status(400).json({error:e.message});
  }
});
app.use('/api/faenas', faenaRouter);
app.use('/api/tipos-documento', crud('tipos_documento','tipo_doc_id',['codigo','nombre']));
app.use('/api/motivos',     crud('motivos_movimiento','motivo_id',['nombre','tipo']));
app.use('/api/condiciones-pago', crud('condiciones_pago','condicion_id',['nombre','descripcion']));

// DELETE con restricciones
app.delete('/api/categorias/:id', auth, async(req,res)=>{
  try{
    const sc=await pool.query('SELECT COUNT(*) FROM subcategorias WHERE categoria_id=$1',[req.params.id]);
    if(parseInt(sc.rows[0].count)>0) return res.status(409).json({error:'No se puede eliminar: tiene subcategorias asociadas.'});
    await pool.query('DELETE FROM categorias WHERE categoria_id=$1',[req.params.id]);
    res.json({ok:true});
  }catch(e){res.status(400).json({error:e.message});}
});
app.delete('/api/subcategorias/:id', auth, async(req,res)=>{
  try{
    const pr=await pool.query('SELECT COUNT(*) FROM productos WHERE subcategoria_id=$1',[req.params.id]);
    if(parseInt(pr.rows[0].count)>0) return res.status(409).json({error:'No se puede eliminar: tiene productos asociados. Inactivelos primero.'});
    await pool.query('DELETE FROM subcategorias WHERE subcategoria_id=$1',[req.params.id]);
    res.json({ok:true});
  }catch(e){res.status(400).json({error:e.message});}
});
app.delete('/api/condiciones-pago/:id', auth, async(req,res)=>{
  try{
    const oc=await pool.query('SELECT COUNT(*) FROM ordenes_compra WHERE condicion_id=$1',[req.params.id]);
    if(parseInt(oc.rows[0].count)>0) return res.status(409).json({error:'No se puede eliminar: esta en uso en ordenes de compra.'});
    await pool.query('DELETE FROM condiciones_pago WHERE condicion_id=$1',[req.params.id]);
    res.json({ok:true});
  }catch(e){res.status(400).json({error:e.message});}
});

// EMPRESAS
const empR=express.Router();
empR.get('/', auth, async(req,res)=>{try{res.json((await pool.query('SELECT * FROM empresas ORDER BY empresa_id')).rows);}catch(e){res.status(500).json({error:e.message});}});
empR.post('/', auth, async(req,res)=>{
  try{
    const{rut,razon_social,direccion,ciudad,giro,telefono,email}=req.body;
    const r=await pool.query('INSERT INTO empresas(rut,razon_social,direccion,ciudad,giro,telefono,email,logo_base64) VALUES($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *',[rut,razon_social,direccion||null,ciudad||null,giro||null,telefono||null,email||null,req.body.logo_base64||null]);
    res.status(201).json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
empR.put('/:id', auth, async(req,res)=>{
  try{
    const{rut,razon_social,direccion,ciudad,giro,telefono,email}=req.body;
    const r=await pool.query('UPDATE empresas SET rut=$1,razon_social=$2,direccion=$3,ciudad=$4,giro=$5,telefono=$6,email=$7,logo_base64=$8,modificado_en=NOW() WHERE empresa_id=$9 RETURNING *',[rut,razon_social,direccion||null,ciudad||null,giro||null,telefono||null,email||null,req.body.logo_base64||null,req.params.id]);
    res.json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
empR.patch('/:id/activo', auth, async(req,res)=>{try{res.json((await pool.query('UPDATE empresas SET activo=NOT activo WHERE empresa_id=$1 RETURNING *',[req.params.id])).rows[0]);}catch(e){res.status(400).json({error:e.message});}});
empR.delete('/:id', auth, async(req,res)=>{
  try{
    const oc=await pool.query('SELECT COUNT(*) FROM ordenes_compra WHERE empresa_id=$1',[req.params.id]);
    if(parseInt(oc.rows[0].count)>0) return res.status(409).json({error:'No se puede eliminar: tiene ordenes de compra asociadas.'});
    await pool.query('DELETE FROM empresas WHERE empresa_id=$1',[req.params.id]);
    res.json({ok:true});
  }catch(e){res.status(400).json({error:e.message});}
});
// Lookup empresa_id by RUT (for imports)
empR.get('/lookup/rut/:rut', auth, async(req,res)=>{
  try{
    const r=await pool.query("SELECT empresa_id FROM empresas WHERE REPLACE(REPLACE(rut,'.',''),'-','')=REPLACE(REPLACE($1,'.',''),'-','') AND activo=true LIMIT 1",[req.params.rut]);
    if(!r.rows.length) return res.status(404).json({error:'Empresa no encontrada con RUT: '+req.params.rut});
    res.json({empresa_id:r.rows[0].empresa_id});
  }catch(e){res.status(500).json({error:e.message});}
});
app.use('/api/empresas', empR);

// EQUIPOS con empresa (v2.2)
const eqR=express.Router();
async function resolveEmpresaId(val){
  if(!val)return null;
  const s=String(val).trim();
  if(/^\d+$/.test(s))return parseInt(s);
  const r=await pool.query("SELECT empresa_id FROM empresas WHERE REPLACE(REPLACE(rut,'.',''),'-','')=REPLACE(REPLACE($1,'.',''),'-','') LIMIT 1",[s]);
  if(!r.rows.length)throw new Error('Empresa no encontrada con RUT: '+s);
  return r.rows[0].empresa_id;
}
eqR.get('/', auth, async(req,res)=>{
  try{
    const r=await pool.query('SELECT e.*,f.nombre AS faena_nombre,emp.razon_social AS empresa_nombre FROM equipos e LEFT JOIN faenas f ON e.faena_id=f.faena_id LEFT JOIN empresas emp ON e.empresa_id=emp.empresa_id ORDER BY emp.razon_social NULLS LAST,e.nombre');
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});
eqR.post('/', auth, async(req,res)=>{
  try{
    const{codigo,nombre,tipo,faena_id,patente_serie,marca,modelo,anio,placa_patente,num_chasis}=req.body;
    const empresa_id=await resolveEmpresaId(req.body.empresa_id);
    const r=await pool.query('INSERT INTO equipos(codigo,nombre,tipo,faena_id,patente_serie,marca,modelo,anio,placa_patente,num_chasis,empresa_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING *',[codigo,nombre,tipo||null,faena_id||null,patente_serie||null,marca||null,modelo||null,anio||null,placa_patente||null,num_chasis||null,empresa_id]);
    res.status(201).json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
eqR.put('/:id', auth, async(req,res)=>{
  try{
    const{codigo,nombre,tipo,faena_id,patente_serie,marca,modelo,anio,placa_patente,num_chasis}=req.body;
    const empresa_id=await resolveEmpresaId(req.body.empresa_id);
    const r=await pool.query('UPDATE equipos SET codigo=$1,nombre=$2,tipo=$3,faena_id=$4,patente_serie=$5,marca=$6,modelo=$7,anio=$8,placa_patente=$9,num_chasis=$10,empresa_id=$11 WHERE equipo_id=$12 RETURNING *',[codigo,nombre,tipo||null,faena_id||null,patente_serie||null,marca||null,modelo||null,anio||null,placa_patente||null,num_chasis||null,empresa_id,req.params.id]);
    res.json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
eqR.patch('/:id/activo', auth, async(req,res)=>{try{res.json((await pool.query('UPDATE equipos SET activo=NOT activo WHERE equipo_id=$1 RETURNING *',[req.params.id])).rows[0]);}catch(e){res.status(400).json({error:e.message});}});
eqR.delete('/:id', auth, async(req,res)=>{
  try{await pool.query('DELETE FROM equipos WHERE equipo_id=$1',[req.params.id]);res.json({ok:true});}
  catch(e){if(e.code==='23503')return res.status(409).json({error:'No se puede eliminar: equipo en uso.'});res.status(400).json({error:e.message});}
});
app.use('/api/equipos', eqR);

// PROVEEDORES con nuevos campos
const prvR=express.Router();
prvR.get('/', auth, async(req,res)=>{try{res.json((await pool.query('SELECT * FROM proveedores ORDER BY nombre')).rows);}catch(e){res.status(500).json({error:e.message});}});
prvR.post('/', auth, async(req,res)=>{
  try{
    const{rut,nombre,contacto,telefono,email,direccion,giro}=req.body;
    const r=await pool.query('INSERT INTO proveedores(rut,nombre,contacto,telefono,email,direccion,giro) VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING *',[rut,nombre,contacto||null,telefono||null,email||null,direccion||null,giro||null]);
    res.status(201).json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
prvR.put('/:id', auth, async(req,res)=>{
  try{
    const{rut,nombre,contacto,telefono,email,direccion,giro}=req.body;
    const r=await pool.query('UPDATE proveedores SET rut=$1,nombre=$2,contacto=$3,telefono=$4,email=$5,direccion=$6,giro=$7 WHERE proveedor_id=$8 RETURNING *',[rut,nombre,contacto||null,telefono||null,email||null,direccion||null,giro||null,req.params.id]);
    res.json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
prvR.patch('/:id/activo', auth, async(req,res)=>{try{res.json((await pool.query('UPDATE proveedores SET activo=NOT activo WHERE proveedor_id=$1 RETURNING *',[req.params.id])).rows[0]);}catch(e){res.status(400).json({error:e.message});}});
app.use('/api/proveedores', prvR);

// PRODUCTOS con delete
const prR=express.Router();
prR.get('/', auth, async(req,res)=>{try{res.json((await pool.query('SELECT p.*,sc.nombre AS subcategoria_nombre,ca.categoria_id,ca.nombre AS categoria_nombre FROM productos p JOIN subcategorias sc ON p.subcategoria_id=sc.subcategoria_id JOIN categorias ca ON sc.categoria_id=ca.categoria_id ORDER BY p.nombre')).rows);}catch(e){res.status(500).json({error:e.message});}});
prR.get('/:id', auth, async(req,res)=>{try{res.json((await pool.query('SELECT * FROM productos WHERE producto_id=$1',[req.params.id])).rows[0]);}catch(e){res.status(500).json({error:e.message});}});
prR.post('/', auth, async(req,res)=>{
  try{
    const{codigo,nombre,descripcion,subcategoria_id,unidad_medida,stock_minimo,stock_maximo,costo_referencia}=req.body;
    const r=await pool.query('INSERT INTO productos(codigo,nombre,descripcion,subcategoria_id,unidad_medida,stock_minimo,stock_maximo,costo_referencia) VALUES($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *',[codigo,nombre,descripcion||null,subcategoria_id,unidad_medida||'UN',stock_minimo||0,stock_maximo||null,costo_referencia||0]);
    res.status(201).json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
prR.put('/:id', auth, async(req,res)=>{
  try{
    const{codigo,nombre,descripcion,subcategoria_id,unidad_medida,stock_minimo,stock_maximo,costo_referencia}=req.body;
    const r=await pool.query('UPDATE productos SET codigo=$1,nombre=$2,descripcion=$3,subcategoria_id=$4,unidad_medida=$5,stock_minimo=$6,stock_maximo=$7,costo_referencia=$8,modificado_en=NOW() WHERE producto_id=$9 RETURNING *',[codigo,nombre,descripcion||null,subcategoria_id,unidad_medida||'UN',stock_minimo||0,stock_maximo||null,costo_referencia||0,req.params.id]);
    res.json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
prR.patch('/:id/activo', auth, async(req,res)=>{try{res.json((await pool.query('UPDATE productos SET activo=NOT activo WHERE producto_id=$1 RETURNING *',[req.params.id])).rows[0]);}catch(e){res.status(400).json({error:e.message});}});
prR.delete('/:id', auth, async(req,res)=>{
  try{
    const mv=await pool.query('SELECT COUNT(*) FROM movimiento_detalle WHERE producto_id=$1',[req.params.id]);
    if(parseInt(mv.rows[0].count)>0) return res.status(409).json({error:'No se puede eliminar: el producto tiene movimientos registrados. Use Inactivar.'});
    const oc=await pool.query('SELECT COUNT(*) FROM ordenes_compra_detalle WHERE producto_id=$1',[req.params.id]);
    if(parseInt(oc.rows[0].count)>0) return res.status(409).json({error:'No se puede eliminar: el producto esta en ordenes de compra.'});
    await pool.query('DELETE FROM stock_actual WHERE producto_id=$1',[req.params.id]);
    await pool.query('DELETE FROM productos WHERE producto_id=$1',[req.params.id]);
    res.json({ok:true});
  }catch(e){res.status(400).json({error:e.message});}
});
app.use('/api/productos', prR);

// MOVIMIENTOS
const mvR=express.Router();
mvR.get('/', auth, async(req,res)=>{
  try{
    const{tipo,bodega_id,faena_id,equipo_id,desde,hasta}=req.query;
    let where=['1=1'],vals=[];
    if(tipo){vals.push(tipo);where.push(`me.tipo_movimiento=$${vals.length}`);}
    if(bodega_id){vals.push(bodega_id);where.push(`me.bodega_id=$${vals.length}`);}
    if(faena_id){vals.push(faena_id);where.push(`me.faena_id=$${vals.length}`);}
    if(equipo_id){vals.push(equipo_id);where.push(`me.equipo_id=$${vals.length}`);}
    if(desde){vals.push(desde);where.push(`me.fecha>=$${vals.length}`);}
    if(hasta){vals.push(hasta);where.push(`me.fecha<=$${vals.length}`);}
    const r=await pool.query(`SELECT me.*,b.nombre AS bodega_nombre,f.nombre AS faena_nombre,e.nombre AS equipo_nombre,pr.nombre AS proveedor_nombre,td.nombre AS tipo_doc_nombre,mot.nombre AS motivo_nombre,(SELECT SUM(md.costo_total) FROM movimiento_detalle md WHERE md.movimiento_id=me.movimiento_id) AS total,(SELECT SUM(md.costo_total) FROM movimiento_detalle md WHERE md.movimiento_id=me.movimiento_id) AS total_ingreso,(SELECT COUNT(*) FROM movimiento_detalle md WHERE md.movimiento_id=me.movimiento_id) AS num_lineas FROM movimiento_encabezado me LEFT JOIN bodegas b ON me.bodega_id=b.bodega_id LEFT JOIN faenas f ON me.faena_id=f.faena_id LEFT JOIN equipos e ON me.equipo_id=e.equipo_id LEFT JOIN proveedores pr ON me.proveedor_id=pr.proveedor_id LEFT JOIN tipos_documento td ON me.tipo_doc_id=td.tipo_doc_id LEFT JOIN motivos_movimiento mot ON me.motivo_id=mot.motivo_id WHERE ${where.join(' AND ')} ORDER BY me.movimiento_id DESC`,vals);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});
mvR.get('/:id/detalles', auth, async(req,res)=>{
  try{const r=await pool.query('SELECT md.*,p.nombre AS producto_nombre,p.codigo AS producto_codigo,p.unidad_medida FROM movimiento_detalle md JOIN productos p ON md.producto_id=p.producto_id WHERE md.movimiento_id=$1',[req.params.id]);res.json(r.rows);}catch(e){res.status(500).json({error:e.message});}
});
mvR.post('/', auth, async(req,res)=>{
  const client=await pool.connect();
  try{
    await client.query('BEGIN');
    const{tipo_movimiento,fecha,bodega_id,faena_id,equipo_id,proveedor_id,tipo_doc_id,numero_documento,fecha_documento,oc_referencia,motivo_id,observaciones,responsable_entrega,responsable_recepcion,lineas}=req.body;
    if(!tipo_movimiento||!fecha||!bodega_id) throw new Error('Tipo, fecha y bodega son obligatorios');
    if(!lineas||!lineas.length) throw new Error('Debe incluir al menos una linea');
    if(tipo_movimiento==='SALIDA'){
      for(const l of lineas){
        const sr=await client.query('SELECT cantidad_disponible FROM stock_actual WHERE producto_id=$1 AND bodega_id=$2',[l.producto_id,bodega_id]);
        const disp=parseFloat(sr.rows[0]?.cantidad_disponible||0);
        if(parseFloat(l.cantidad)>disp){const pn=(await client.query('SELECT nombre FROM productos WHERE producto_id=$1',[l.producto_id])).rows[0]?.nombre||l.producto_id;throw new Error(`Stock insuficiente: "${pn}" disponible: ${disp}`);}
      }
    }
    const mr=await client.query('INSERT INTO movimiento_encabezado(tipo_movimiento,fecha,bodega_id,faena_id,equipo_id,proveedor_id,tipo_doc_id,numero_documento,fecha_documento,oc_referencia,motivo_id,observaciones,responsable_entrega,responsable_recepcion,usuario) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) RETURNING movimiento_id',[tipo_movimiento,fecha,bodega_id,faena_id||null,equipo_id||null,proveedor_id||null,tipo_doc_id||null,numero_documento||null,fecha_documento||null,oc_referencia||null,motivo_id||null,observaciones||null,responsable_entrega||null,responsable_recepcion||null,req.user.email]);
    const movId=mr.rows[0].movimiento_id;
    for(const l of lineas){
      const pid=parseInt(l.producto_id),bidI=parseInt(bodega_id),qty=parseFloat(l.cantidad),cuIn=parseFloat(l.costo_unitario||0);
      const sr=await client.query('SELECT cantidad_disponible,costo_promedio_actual FROM stock_actual WHERE producto_id=$1 AND bodega_id=$2',[pid,bidI]);
      const cur=sr.rows[0]||{cantidad_disponible:0,costo_promedio_actual:0};
      const curQ=parseFloat(cur.cantidad_disponible),curCpp=parseFloat(cur.costo_promedio_actual);
      let newQ,newCpp,cu;
      if(tipo_movimiento==='INGRESO'){cu=cuIn;newQ=curQ+qty;newCpp=newQ>0?(curQ*curCpp+qty*cu)/newQ:cu;}
      else if(tipo_movimiento==='SALIDA'){cu=curCpp;newQ=Math.max(0,curQ-qty);newCpp=curCpp;}
      else{cu=cuIn||curCpp;newQ=Math.max(0,curQ+qty);newCpp=qty>0&&newQ>0?(curQ*curCpp+qty*cu)/newQ:curCpp;}
      await client.query('INSERT INTO movimiento_detalle(movimiento_id,producto_id,cantidad,costo_unitario) VALUES($1,$2,$3,$4)',[movId,pid,qty,cu]);
      await client.query('INSERT INTO stock_actual(producto_id,bodega_id,cantidad_disponible,costo_promedio_actual,ultima_actualizacion) VALUES($1,$2,$3,$4,NOW()) ON CONFLICT(producto_id,bodega_id) DO UPDATE SET cantidad_disponible=$3,costo_promedio_actual=$4,ultima_actualizacion=NOW()',[pid,bidI,newQ,newCpp]);
    }
    await client.query('COMMIT');
    res.status(201).json({ok:true,movimiento_id:movId});
  }catch(e){await client.query('ROLLBACK');res.status(400).json({error:e.message});}
  finally{client.release();}
});
mvR.patch('/:id/anular', auth, async(req,res)=>{
  const client=await pool.connect();
  try{
    await client.query('BEGIN');
    const{motivo_anulacion}=req.body;
    const mr=await client.query("SELECT * FROM movimiento_encabezado WHERE movimiento_id=$1 AND estado='ACTIVO'",[req.params.id]);
    if(!mr.rows.length) return res.status(400).json({error:'Movimiento no encontrado o ya anulado'});
    const mov=mr.rows[0];
    const dets=await client.query('SELECT * FROM movimiento_detalle WHERE movimiento_id=$1',[req.params.id]);
    for(const d of dets.rows){
      const sr=await client.query('SELECT cantidad_disponible,costo_promedio_actual FROM stock_actual WHERE producto_id=$1 AND bodega_id=$2',[d.producto_id,mov.bodega_id]);
      const cur=sr.rows[0]||{cantidad_disponible:0,costo_promedio_actual:0};
      const curQ=parseFloat(cur.cantidad_disponible),curCpp=parseFloat(cur.costo_promedio_actual);
      const qty=parseFloat(d.cantidad),cu=parseFloat(d.costo_unitario);
      let newQ=curQ,newCpp=curCpp;
      if(mov.tipo_movimiento==='INGRESO'){newQ=Math.max(0,curQ-qty);newCpp=newQ>0&&curQ>0?Math.max(0,(curQ*curCpp-qty*cu)/newQ):0;}
      else if(mov.tipo_movimiento==='SALIDA'){newQ=curQ+qty;newCpp=curCpp;}
      await client.query('UPDATE stock_actual SET cantidad_disponible=$1,costo_promedio_actual=$2,ultima_actualizacion=NOW() WHERE producto_id=$3 AND bodega_id=$4',[Math.max(0,newQ),Math.max(0,newCpp),d.producto_id,mov.bodega_id]);
    }
    await client.query("UPDATE movimiento_encabezado SET estado='ANULADO',anulado_en=NOW(),anulado_por=$1,motivo_anulacion=$2 WHERE movimiento_id=$3",[req.user.email,motivo_anulacion||'Anulado por usuario',req.params.id]);
    await client.query('COMMIT');
    res.json({ok:true});
  }catch(e){await client.query('ROLLBACK');res.status(400).json({error:e.message});}
  finally{client.release();}
});
app.use('/api/movimientos', mvR);

// STOCK
app.get('/api/stock', auth, async(req,res)=>{
  try{const r=await pool.query('SELECT sa.producto_id,p.codigo,p.nombre AS producto_nombre,p.unidad_medida,p.stock_minimo,sc.nombre AS subcategoria,ca.nombre AS categoria,ca.categoria_id,sa.bodega_id,b.nombre AS bodega_nombre,b.codigo AS bodega_codigo,sa.cantidad_disponible,sa.costo_promedio_actual,ROUND(sa.cantidad_disponible*sa.costo_promedio_actual,0) AS valor_total,CASE WHEN sa.cantidad_disponible<=p.stock_minimo THEN true ELSE false END AS bajo_minimo FROM stock_actual sa JOIN productos p ON sa.producto_id=p.producto_id JOIN subcategorias sc ON p.subcategoria_id=sc.subcategoria_id JOIN categorias ca ON sc.categoria_id=ca.categoria_id JOIN bodegas b ON sa.bodega_id=b.bodega_id WHERE p.activo=true AND b.activo=true ORDER BY ca.nombre,sc.nombre,p.nombre');res.json(r.rows);}catch(e){res.status(500).json({error:e.message});}
});
app.get('/api/stock/alertas', auth, async(req,res)=>{
  try{const r=await pool.query('SELECT sa.producto_id,p.codigo,p.nombre AS producto_nombre,sa.bodega_id,b.nombre AS bodega_nombre,sa.cantidad_disponible,p.stock_minimo,p.unidad_medida FROM stock_actual sa JOIN productos p ON sa.producto_id=p.producto_id JOIN bodegas b ON sa.bodega_id=b.bodega_id WHERE sa.cantidad_disponible<=p.stock_minimo AND p.activo=true ORDER BY p.nombre');res.json(r.rows);}catch(e){res.status(500).json({error:e.message});}
});
app.get('/api/stock/consolidado', auth, async(req,res)=>{
  try{const r=await pool.query('SELECT sa.producto_id,p.codigo,p.nombre AS producto_nombre,p.unidad_medida,p.stock_minimo,sc.nombre AS subcategoria,ca.nombre AS categoria,SUM(sa.cantidad_disponible) AS cantidad_total,CASE WHEN SUM(sa.cantidad_disponible)>0 THEN SUM(sa.cantidad_disponible*sa.costo_promedio_actual)/SUM(sa.cantidad_disponible) ELSE 0 END AS cpp_promedio,SUM(sa.cantidad_disponible*sa.costo_promedio_actual) AS valor_total FROM stock_actual sa JOIN productos p ON sa.producto_id=p.producto_id JOIN subcategorias sc ON p.subcategoria_id=sc.subcategoria_id JOIN categorias ca ON sc.categoria_id=ca.categoria_id WHERE p.activo=true GROUP BY sa.producto_id,p.codigo,p.nombre,p.unidad_medida,p.stock_minimo,sc.nombre,ca.nombre ORDER BY p.nombre');res.json(r.rows);}catch(e){res.status(500).json({error:e.message});}
});

// KARDEX
app.get('/api/kardex', auth, async(req,res)=>{
  try{
    const{producto_id,bodega_id}=req.query;
    if(!producto_id) return res.status(400).json({error:'producto_id requerido'});
    let where='md.producto_id=$1',vals=[producto_id];
    if(bodega_id){vals.push(bodega_id);where+=` AND me.bodega_id=$${vals.length}`;}
    const r=await pool.query(`SELECT me.movimiento_id,me.tipo_movimiento,me.fecha,me.bodega_id,b.nombre AS bodega_nombre,md.producto_id,p.codigo AS producto_codigo,p.nombre AS producto_nombre,p.unidad_medida,CASE WHEN me.tipo_movimiento='INGRESO' THEN md.cantidad ELSE 0 END AS entrada,CASE WHEN me.tipo_movimiento='SALIDA' THEN md.cantidad ELSE 0 END AS salida,md.costo_unitario,md.costo_total,me.faena_id,f.nombre AS faena_nombre,me.equipo_id,e.nombre AS equipo_nombre,me.observaciones,me.estado FROM movimiento_detalle md JOIN movimiento_encabezado me ON md.movimiento_id=me.movimiento_id JOIN bodegas b ON me.bodega_id=b.bodega_id JOIN productos p ON md.producto_id=p.producto_id LEFT JOIN faenas f ON me.faena_id=f.faena_id LEFT JOIN equipos e ON me.equipo_id=e.equipo_id WHERE ${where} AND me.estado='ACTIVO' ORDER BY me.movimiento_id`,vals);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});

// REPORTES
app.get('/api/reportes/consumo', auth, async(req,res)=>{
  try{
    const{desde,hasta,faena_id,equipo_id,bodega_id}=req.query;
    let where=["me.tipo_movimiento='SALIDA'","me.estado='ACTIVO'"],vals=[];
    if(desde){vals.push(desde);where.push(`me.fecha>=$${vals.length}`);}
    if(hasta){vals.push(hasta);where.push(`me.fecha<=$${vals.length}`);}
    if(faena_id){vals.push(faena_id);where.push(`me.faena_id=$${vals.length}`);}
    if(equipo_id){vals.push(equipo_id);where.push(`me.equipo_id=$${vals.length}`);}
    if(bodega_id){vals.push(bodega_id);where.push(`me.bodega_id=$${vals.length}`);}
    const r=await pool.query(`SELECT me.fecha,b.nombre AS bodega,f.nombre AS faena,e.nombre AS equipo,p.codigo AS producto_codigo,p.nombre AS producto,sc.nombre AS subcategoria,ca.nombre AS categoria,md.cantidad,p.unidad_medida,md.costo_unitario,md.costo_total FROM movimiento_encabezado me JOIN movimiento_detalle md ON me.movimiento_id=md.movimiento_id JOIN productos p ON md.producto_id=p.producto_id JOIN subcategorias sc ON p.subcategoria_id=sc.subcategoria_id JOIN categorias ca ON sc.categoria_id=ca.categoria_id JOIN bodegas b ON me.bodega_id=b.bodega_id LEFT JOIN faenas f ON me.faena_id=f.faena_id LEFT JOIN equipos e ON me.equipo_id=e.equipo_id WHERE ${where.join(' AND ')} ORDER BY me.fecha,e.nombre,p.nombre`,vals);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});
app.get('/api/reportes/ranking-productos', auth, async(req,res)=>{
  try{const{desde,hasta}=req.query;let where=["me.tipo_movimiento='SALIDA'","me.estado='ACTIVO'"],vals=[];if(desde){vals.push(desde);where.push(`me.fecha>=$${vals.length}`);}if(hasta){vals.push(hasta);where.push(`me.fecha<=$${vals.length}`);}const r=await pool.query(`SELECT p.codigo,p.nombre AS producto_nombre,p.unidad_medida,SUM(md.cantidad) AS cantidad_total,SUM(md.costo_total) AS costo_total,COUNT(DISTINCT me.movimiento_id) AS n FROM movimiento_encabezado me JOIN movimiento_detalle md ON me.movimiento_id=md.movimiento_id JOIN productos p ON md.producto_id=p.producto_id WHERE ${where.join(' AND ')} GROUP BY p.producto_id,p.codigo,p.nombre,p.unidad_medida ORDER BY costo_total DESC LIMIT 15`,vals);res.json(r.rows);}catch(e){res.status(500).json({error:e.message});}
});
app.get('/api/reportes/ranking-equipos', auth, async(req,res)=>{
  try{const{desde,hasta}=req.query;let where=["me.tipo_movimiento='SALIDA'","me.estado='ACTIVO'","me.equipo_id IS NOT NULL"],vals=[];if(desde){vals.push(desde);where.push(`me.fecha>=$${vals.length}`);}if(hasta){vals.push(hasta);where.push(`me.fecha<=$${vals.length}`);}const r=await pool.query(`SELECT e.codigo,e.nombre AS equipo_nombre,e.tipo,f.nombre AS faena_nombre,SUM(md.costo_total) AS costo_total,COUNT(DISTINCT me.movimiento_id) AS n FROM movimiento_encabezado me JOIN movimiento_detalle md ON me.movimiento_id=md.movimiento_id JOIN equipos e ON me.equipo_id=e.equipo_id LEFT JOIN faenas f ON e.faena_id=f.faena_id WHERE ${where.join(' AND ')} GROUP BY e.equipo_id,e.codigo,e.nombre,e.tipo,f.nombre ORDER BY costo_total DESC`,vals);res.json(r.rows);}catch(e){res.status(500).json({error:e.message});}
});
app.get('/api/reportes/ingresos', auth, async(req,res)=>{
  try{const{desde,hasta,proveedor_id}=req.query;let where=["me.tipo_movimiento='INGRESO'","me.estado='ACTIVO'"],vals=[];if(desde){vals.push(desde);where.push(`me.fecha>=$${vals.length}`);}if(hasta){vals.push(hasta);where.push(`me.fecha<=$${vals.length}`);}if(proveedor_id){vals.push(proveedor_id);where.push(`me.proveedor_id=$${vals.length}`);}const r=await pool.query(`SELECT me.movimiento_id,me.fecha,me.numero_documento,me.oc_referencia,b.nombre AS bodega_nombre,pr.nombre AS proveedor_nombre,td.nombre AS tipo_doc_nombre,SUM(md.costo_total) AS total_ingreso,COUNT(md.detalle_id) AS num_lineas FROM movimiento_encabezado me JOIN bodegas b ON me.bodega_id=b.bodega_id JOIN movimiento_detalle md ON me.movimiento_id=md.movimiento_id LEFT JOIN proveedores pr ON me.proveedor_id=pr.proveedor_id LEFT JOIN tipos_documento td ON me.tipo_doc_id=td.tipo_doc_id WHERE ${where.join(' AND ')} GROUP BY me.movimiento_id,me.fecha,me.numero_documento,me.oc_referencia,b.nombre,pr.nombre,td.nombre ORDER BY me.fecha DESC`,vals);res.json(r.rows);}catch(e){res.status(500).json({error:e.message});}
});

// ORDENES DE COMPRA
const ocR=express.Router();
ocR.get('/', auth, async(req,res)=>{
  try{
    const{estado,proveedor_id,desde,hasta,empresa_id,numero_documento}=req.query;
    let where=['1=1'],vals=[];
    if(estado){vals.push(estado);where.push(`oc.estado=$${vals.length}`);}
    if(proveedor_id){vals.push(proveedor_id);where.push(`oc.proveedor_id=$${vals.length}`);}
    if(empresa_id){vals.push(empresa_id);where.push(`oc.empresa_id=$${vals.length}`);}
    if(desde){vals.push(desde);where.push(`oc.fecha_emision>=$${vals.length}`);}
    if(hasta){vals.push(hasta);where.push(`oc.fecha_emision<=$${vals.length}`);}
    if(numero_documento){vals.push('%'+numero_documento+'%');where.push(`oc.numero_documento ILIKE $${vals.length}`);}
    const r=await pool.query(`SELECT oc.*,e.razon_social AS empresa_nombre,pr.nombre AS proveedor_nombre,pr.rut AS proveedor_rut,cp.nombre AS condicion_nombre,td.nombre AS tipo_doc_nombre FROM ordenes_compra oc LEFT JOIN empresas e ON oc.empresa_id=e.empresa_id LEFT JOIN proveedores pr ON oc.proveedor_id=pr.proveedor_id LEFT JOIN condiciones_pago cp ON oc.condicion_id=cp.condicion_id LEFT JOIN tipos_documento td ON oc.tipo_doc_id=td.tipo_doc_id WHERE ${where.join(' AND ')} ORDER BY oc.oc_id DESC`,vals);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});
ocR.get('/reporte/detalle', auth, async(req,res)=>{
  try{
    const{desde,hasta,estado,proveedor_id,empresa_id}=req.query;
    let where=['1=1'],vals=[];
    if(estado){vals.push(estado);where.push(`oc.estado=$${vals.length}`);}
    if(proveedor_id){vals.push(proveedor_id);where.push(`oc.proveedor_id=$${vals.length}`);}
    if(empresa_id){vals.push(empresa_id);where.push(`oc.empresa_id=$${vals.length}`);}
    if(desde){vals.push(desde);where.push(`oc.fecha_emision>=$${vals.length}`);}
    if(hasta){vals.push(hasta);where.push(`oc.fecha_emision<=$${vals.length}`);}
    const r=await pool.query(`SELECT oc.numero_oc,oc.fecha_emision,oc.estado,oc.solicitante,oc.retira,oc.fecha_documento,oc.numero_documento,oc.neto,oc.iva,oc.impuesto_adicional,oc.total,oc.usuario,e.razon_social AS empresa,pr.nombre AS proveedor,pr.rut AS proveedor_rut,cp.nombre AS condicion_pago,td.nombre AS tipo_documento,d.linea_num,d.descripcion,d.cantidad,d.precio_unitario,d.total_linea,d.ingresa_bodega,p.codigo AS producto_codigo,p.nombre AS producto_nombre,sc.nombre AS subcategoria,f.nombre AS faena,eq.nombre AS equipo FROM ordenes_compra oc LEFT JOIN empresas e ON oc.empresa_id=e.empresa_id LEFT JOIN proveedores pr ON oc.proveedor_id=pr.proveedor_id LEFT JOIN condiciones_pago cp ON oc.condicion_id=cp.condicion_id LEFT JOIN tipos_documento td ON oc.tipo_doc_id=td.tipo_doc_id LEFT JOIN ordenes_compra_detalle d ON oc.oc_id=d.oc_id LEFT JOIN productos p ON d.producto_id=p.producto_id LEFT JOIN subcategorias sc ON d.subcategoria_id=sc.subcategoria_id LEFT JOIN faenas f ON d.faena_id=f.faena_id LEFT JOIN equipos eq ON d.equipo_id=eq.equipo_id WHERE ${where.join(' AND ')} ORDER BY oc.oc_id,d.linea_num`,vals);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});
ocR.get('/:id', auth, async(req,res)=>{
  try{
    const oc=await pool.query('SELECT oc.*,e.razon_social AS empresa_nombre,pr.nombre AS proveedor_nombre,pr.rut AS proveedor_rut,cp.nombre AS condicion_nombre,td.nombre AS tipo_doc_nombre,b.nombre AS bodega_ingreso_nombre FROM ordenes_compra oc LEFT JOIN empresas e ON oc.empresa_id=e.empresa_id LEFT JOIN proveedores pr ON oc.proveedor_id=pr.proveedor_id LEFT JOIN condiciones_pago cp ON oc.condicion_id=cp.condicion_id LEFT JOIN tipos_documento td ON oc.tipo_doc_id=td.tipo_doc_id LEFT JOIN bodegas b ON oc.bodega_ingreso_id=b.bodega_id WHERE oc.oc_id=$1',[req.params.id]);
    if(!oc.rows.length) return res.status(404).json({error:'No encontrado'});
    const dets=await pool.query('SELECT d.*,p.nombre AS producto_nombre,p.codigo AS producto_codigo,sc.nombre AS subcategoria_nombre,f.nombre AS faena_nombre,eq.nombre AS equipo_nombre,b.nombre AS bodega_destino_nombre FROM ordenes_compra_detalle d LEFT JOIN productos p ON d.producto_id=p.producto_id LEFT JOIN subcategorias sc ON d.subcategoria_id=sc.subcategoria_id LEFT JOIN faenas f ON d.faena_id=f.faena_id LEFT JOIN equipos eq ON d.equipo_id=eq.equipo_id LEFT JOIN bodegas b ON d.bodega_destino_id=b.bodega_id WHERE d.oc_id=$1 ORDER BY d.linea_num,d.detalle_id',[req.params.id]);
    res.json({...oc.rows[0],lineas:dets.rows});
  }catch(e){res.status(500).json({error:e.message});}
});
ocR.post('/', auth, async(req,res)=>{
  const client=await pool.connect();
  try{
    await client.query('BEGIN');
    const{empresa_id,proveedor_id,fecha_emision,solicitante,retira,condicion_id,impuesto_adicional,observaciones,lineas}=req.body;
    if(!proveedor_id||!fecha_emision) throw new Error('Proveedor y fecha son obligatorios');
    if(!lineas||!lineas.length) throw new Error('Debe ingresar al menos una linea');
    const year=new Date().getFullYear();
    const seq=await client.query("SELECT nextval('seq_oc_num')");
    const numero_oc=`OC-${year}-${String(seq.rows[0].nextval).padStart(4,'0')}`;
    const neto=lineas.reduce((s,l)=>s+(parseFloat(l.cantidad)||0)*(parseFloat(l.precio_unitario)||0),0);
    const iva=Math.round(neto*0.19);
    const imp=parseFloat(impuesto_adicional)||0;
    const total=neto+iva+imp;
    const ocR2=await client.query('INSERT INTO ordenes_compra(numero_oc,empresa_id,proveedor_id,fecha_emision,solicitante,retira,condicion_id,impuesto_adicional,neto,iva,total,observaciones,usuario) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) RETURNING oc_id',[numero_oc,empresa_id||null,proveedor_id,fecha_emision,solicitante||null,retira||null,condicion_id||null,imp,neto,iva,total,observaciones||null,req.user.email]);
    const ocId=ocR2.rows[0].oc_id;
    for(let i=0;i<lineas.length;i++){const l=lineas[i];await client.query('INSERT INTO ordenes_compra_detalle(oc_id,linea_num,descripcion,producto_id,subcategoria_id,faena_id,equipo_id,cantidad,precio_unitario,ingresa_bodega,bodega_destino_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)',[ocId,i+1,l.descripcion||null,l.producto_id||null,l.subcategoria_id||null,l.faena_id||null,l.equipo_id||null,parseFloat(l.cantidad)||0,parseFloat(l.precio_unitario)||0,l.ingresa_bodega||false,l.bodega_destino_id||null]);}
    await client.query('COMMIT');
    res.status(201).json({ok:true,oc_id:ocId,numero_oc});
  }catch(e){await client.query('ROLLBACK');res.status(400).json({error:e.message});}
  finally{client.release();}
});
ocR.put('/:id', auth, async(req,res)=>{
  const client=await pool.connect();
  try{
    await client.query('BEGIN');
    const chk=await client.query('SELECT estado FROM ordenes_compra WHERE oc_id=$1',[req.params.id]);
    if(!chk.rows.length) throw new Error('OC no encontrada');
    if(chk.rows[0].estado!=='PENDIENTE') throw new Error('Solo se pueden editar ordenes PENDIENTES');
    const{empresa_id,proveedor_id,fecha_emision,solicitante,retira,condicion_id,impuesto_adicional,observaciones,lineas}=req.body;
    const neto=lineas.reduce((s,l)=>s+(parseFloat(l.cantidad)||0)*(parseFloat(l.precio_unitario)||0),0);
    const iva=Math.round(neto*0.19);const imp=parseFloat(impuesto_adicional)||0;const total=neto+iva+imp;
    await client.query('UPDATE ordenes_compra SET empresa_id=$1,proveedor_id=$2,fecha_emision=$3,solicitante=$4,retira=$5,condicion_id=$6,impuesto_adicional=$7,neto=$8,iva=$9,total=$10,observaciones=$11,modificado_en=NOW() WHERE oc_id=$12',[empresa_id||null,proveedor_id,fecha_emision,solicitante||null,retira||null,condicion_id||null,imp,neto,iva,total,observaciones||null,req.params.id]);
    await client.query('DELETE FROM ordenes_compra_detalle WHERE oc_id=$1',[req.params.id]);
    for(let i=0;i<lineas.length;i++){const l=lineas[i];await client.query('INSERT INTO ordenes_compra_detalle(oc_id,linea_num,descripcion,producto_id,subcategoria_id,faena_id,equipo_id,cantidad,precio_unitario,ingresa_bodega,bodega_destino_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)',[req.params.id,i+1,l.descripcion||null,l.producto_id||null,l.subcategoria_id||null,l.faena_id||null,l.equipo_id||null,parseFloat(l.cantidad)||0,parseFloat(l.precio_unitario)||0,l.ingresa_bodega||false,l.bodega_destino_id||null]);}
    await client.query('COMMIT');res.json({ok:true});
  }catch(e){await client.query('ROLLBACK');res.status(400).json({error:e.message});}
  finally{client.release();}
});
ocR.patch('/:id/cerrar', auth, async(req,res)=>{
  try{
    const{tipo_doc_id,numero_documento,fecha_documento}=req.body;
    if(!tipo_doc_id||!numero_documento||!fecha_documento) return res.status(400).json({error:'Tipo documento, folio y fecha son obligatorios'});
    const chk=await pool.query('SELECT estado FROM ordenes_compra WHERE oc_id=$1',[req.params.id]);
    if(!chk.rows.length) return res.status(404).json({error:'OC no encontrada'});
    if(chk.rows[0].estado!=='PENDIENTE') return res.status(400).json({error:'Solo se pueden cerrar ordenes PENDIENTES'});
    await pool.query("UPDATE ordenes_compra SET estado='CERRADA',tipo_doc_id=$1,numero_documento=$2,fecha_documento=$3,modificado_en=NOW() WHERE oc_id=$4",[tipo_doc_id,numero_documento,fecha_documento,req.params.id]);
    res.json({ok:true});
  }catch(e){res.status(400).json({error:e.message});}
});
ocR.patch('/:id/anular', auth, async(req,res)=>{
  try{
    const chk=await pool.query('SELECT estado,movimiento_id FROM ordenes_compra WHERE oc_id=$1',[req.params.id]);
    if(!chk.rows.length) return res.status(404).json({error:'OC no encontrada'});
    if(chk.rows[0].estado==='ANULADA') return res.status(400).json({error:'La OC ya esta anulada'});
    if(chk.rows[0].movimiento_id) return res.status(400).json({error:'No se puede anular: ya se recibieron productos en bodega.'});
    await pool.query("UPDATE ordenes_compra SET estado='ANULADA',anulado_en=NOW(),anulado_por=$1 WHERE oc_id=$2",[req.user.email,req.params.id]);
    res.json({ok:true});
  }catch(e){res.status(400).json({error:e.message});}
});
ocR.post('/:id/recibir-bodega', auth, async(req,res)=>{
  const client=await pool.connect();
  try{
    await client.query('BEGIN');
    const ocQ=await client.query('SELECT * FROM ordenes_compra WHERE oc_id=$1',[req.params.id]);
    if(!ocQ.rows.length) throw new Error('OC no encontrada');
    const oc=ocQ.rows[0];
    if(oc.estado!=='CERRADA') throw new Error('Solo se pueden recibir ordenes CERRADAS');
    if(oc.movimiento_id) throw new Error('Esta OC ya fue recibida en bodega');
    const{bodega_id}=req.body;
    const lineas=await client.query('SELECT * FROM ordenes_compra_detalle WHERE oc_id=$1 AND ingresa_bodega=true',[req.params.id]);
    if(!lineas.rows.length) throw new Error('No hay lineas marcadas para ingresar a bodega. Al crear o editar la OC, active el checkbox "A Bodega" en las lineas que deben entrar al inventario.');
    const conProducto=lineas.rows.filter(function(l){return l.producto_id;});
    if(!conProducto.length) throw new Error('Las lineas marcadas "A Bodega" no tienen producto asociado. Edite la OC, asigne un producto a esas lineas y vuelva a intentarlo.');
    const bodegaEfectiva=bodega_id||oc.bodega_ingreso_id;
    if(!bodegaEfectiva) throw new Error('Debe seleccionar la bodega de destino');
    const mr=await client.query('INSERT INTO movimiento_encabezado(tipo_movimiento,fecha,bodega_id,proveedor_id,tipo_doc_id,numero_documento,fecha_documento,oc_referencia,observaciones,usuario) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING movimiento_id',['INGRESO',oc.fecha_documento||new Date().toISOString().split('T')[0],bodegaEfectiva,oc.proveedor_id,oc.tipo_doc_id,oc.numero_documento,oc.fecha_documento,oc.numero_oc,`Recepcion OC ${oc.numero_oc}`,req.user.email]);
    const movId=mr.rows[0].movimiento_id;
    for(const l of conProducto){
      const pid=l.producto_id,qty=parseFloat(l.cantidad),cu=parseFloat(l.precio_unitario)||0;
      const bodDest=l.bodega_destino_id||bodegaEfectiva;
      const sr=await client.query('SELECT cantidad_disponible,costo_promedio_actual FROM stock_actual WHERE producto_id=$1 AND bodega_id=$2',[pid,bodDest]);
      const cur=sr.rows[0]||{cantidad_disponible:0,costo_promedio_actual:0};
      const curQ=parseFloat(cur.cantidad_disponible),curCpp=parseFloat(cur.costo_promedio_actual);
      const newQ=curQ+qty,newCpp=newQ>0?(curQ*curCpp+qty*cu)/newQ:cu;
      await client.query('INSERT INTO movimiento_detalle(movimiento_id,producto_id,cantidad,costo_unitario) VALUES($1,$2,$3,$4)',[movId,pid,qty,cu]);
      await client.query('INSERT INTO stock_actual(producto_id,bodega_id,cantidad_disponible,costo_promedio_actual,ultima_actualizacion) VALUES($1,$2,$3,$4,NOW()) ON CONFLICT(producto_id,bodega_id) DO UPDATE SET cantidad_disponible=$3,costo_promedio_actual=$4,ultima_actualizacion=NOW()',[pid,bodDest,newQ,newCpp]);
    }
    await client.query('UPDATE ordenes_compra SET movimiento_id=$1,bodega_ingreso_id=$2,modificado_en=NOW() WHERE oc_id=$3',[movId,bodegaEfectiva,req.params.id]);
    await client.query('COMMIT');
    res.json({ok:true,movimiento_id:movId});
  }catch(e){await client.query('ROLLBACK');res.status(400).json({error:e.message});}
  finally{client.release();}
});
// REABRIR OC (solo si no generó ingreso a bodega)
ocR.patch('/:id/reabrir', auth, async(req,res)=>{
  const client=await pool.connect();
  try{
    await client.query('BEGIN');
    const chk=await client.query('SELECT estado,movimiento_id FROM ordenes_compra WHERE oc_id=$1',[req.params.id]);
    if(!chk.rows.length) throw new Error('OC no encontrada');
    const oc=chk.rows[0];
    if(oc.estado!=='CERRADA') throw new Error('Solo se pueden reabrir ordenes en estado CERRADA');
    if(oc.movimiento_id) throw new Error('No se puede reabrir: la OC ya genero un ingreso a bodega (movimiento #'+oc.movimiento_id+'). Anule el movimiento primero si desea reabrir la OC.');
    const motivo=req.body.motivo||'Sin motivo especificado';
    await client.query("UPDATE ordenes_compra SET estado='PENDIENTE',tipo_doc_id=NULL,numero_documento=NULL,fecha_documento=NULL,reabierto_en=NOW(),reabierto_por=$1,motivo_reapertura=$2,modificado_en=NOW() WHERE oc_id=$3",[req.user.email,motivo,req.params.id]);
    await client.query('COMMIT');
    res.json({ok:true});
  }catch(e){await client.query('ROLLBACK');res.status(400).json({error:e.message});}
  finally{client.release();}
});

// OC con filtros adicionales (subcategoria, faena, equipo)
ocR.get('/buscar/filtros', auth, async(req,res)=>{
  try{
    const{estado,proveedor_id,empresa_id,desde,hasta,subcategoria_id,faena_id,equipo_id,numero_documento}=req.query;
    let where=['1=1'],vals=[];
    if(estado){vals.push(estado);where.push(`oc.estado=$${vals.length}`);}
    if(proveedor_id){vals.push(proveedor_id);where.push(`oc.proveedor_id=$${vals.length}`);}
    if(empresa_id){vals.push(empresa_id);where.push(`oc.empresa_id=$${vals.length}`);}
    if(desde){vals.push(desde);where.push(`oc.fecha_emision>=$${vals.length}`);}
    if(hasta){vals.push(hasta);where.push(`oc.fecha_emision<=$${vals.length}`);}
    if(subcategoria_id){vals.push(subcategoria_id);where.push(`d.subcategoria_id=$${vals.length}`);}
    if(faena_id){vals.push(faena_id);where.push(`d.faena_id=$${vals.length}`);}
    if(equipo_id){vals.push(equipo_id);where.push(`d.equipo_id=$${vals.length}`);}
    if(numero_documento){vals.push('%'+numero_documento+'%');where.push(`oc.numero_documento ILIKE $${vals.length}`);}
    const r=await pool.query(`
      SELECT DISTINCT oc.oc_id,oc.numero_oc,oc.fecha_emision,oc.estado,oc.solicitante,oc.retira,
             oc.fecha_documento,oc.numero_documento,oc.neto,oc.iva,oc.impuesto_adicional,oc.total,oc.usuario,
             e.razon_social AS empresa,pr.nombre AS proveedor,pr.rut AS proveedor_rut,
             cp.nombre AS condicion_pago,td.nombre AS tipo_documento
      FROM ordenes_compra oc
      LEFT JOIN empresas e ON oc.empresa_id=e.empresa_id
      LEFT JOIN proveedores pr ON oc.proveedor_id=pr.proveedor_id
      LEFT JOIN condiciones_pago cp ON oc.condicion_id=cp.condicion_id
      LEFT JOIN tipos_documento td ON oc.tipo_doc_id=td.tipo_doc_id
      LEFT JOIN ordenes_compra_detalle d ON oc.oc_id=d.oc_id
      WHERE ${where.join(' AND ')}
      ORDER BY oc.oc_id DESC`,vals);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});

app.use('/api/ordenes-compra', ocR);

// USUARIOS
app.get('/api/usuarios', auth, async(req,res)=>{try{res.json((await pool.query('SELECT usuario_id,email,nombre,rol,activo,creado_en FROM usuarios ORDER BY nombre')).rows);}catch(e){res.status(500).json({error:e.message});}});
app.post('/api/usuarios', auth, async(req,res)=>{
  try{const{email,nombre,password,rol}=req.body;const hash=await bcrypt.hash(password,10);const r=await pool.query('INSERT INTO usuarios(email,nombre,password_hash,rol) VALUES($1,$2,$3,$4) RETURNING usuario_id,email,nombre,rol,activo',[email,nombre,hash,rol||'BODEGUERO']);res.status(201).json(r.rows[0]);}catch(e){res.status(400).json({error:e.message});}
});
app.patch('/api/usuarios/:id/activo', auth, async(req,res)=>{try{res.json((await pool.query('UPDATE usuarios SET activo=NOT activo WHERE usuario_id=$1 RETURNING *',[req.params.id])).rows[0]);}catch(e){res.status(400).json({error:e.message});}});

app.get('/api/ping', async(req,res)=>{try{await pool.query('SELECT 1');res.json({ok:true,version:'2.0',time:new Date().toISOString()});}catch(e){res.status(500).json({ok:false,error:e.message});}});
app.get('*', (req,res)=>res.sendFile(path.join(__dirname,'frontend','index.html')));

app.listen(PORT,'0.0.0.0', async()=>{
  console.log('\n============================================================');
  console.log('  LPZ Bodegas v2.0 — Puerto', PORT);
  console.log('============================================================');
  let tries=0;
  while(tries<12){try{await pool.query('SELECT 1');console.log('  [OK] BD conectada');break;}catch{tries++;console.log(`  [ESPERA] BD... ${tries}/12`);await new Promise(r=>setTimeout(r,3000));}}
  await autoSetup();
  console.log('  [OK] Sistema listo — admin@lpz.cl / admin123');
  console.log('============================================================\n');
});
