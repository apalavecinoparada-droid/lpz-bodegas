'use strict';
require('dotenv').config();
const express  = require('express');
const { Pool } = require('pg');
const cors     = require('cors');
const bcrypt   = require('bcryptjs');
const jwt      = require('jsonwebtoken');
const path     = require('path');
let pdfParse=null;
try{pdfParse=require('pdf-parse');console.log('[OK] pdf-parse cargado');}
catch(e){console.log('[WARN] pdf-parse no disponible:',e.message);}

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


// ══════════════════════════════════════════════════════
// MÓDULO MANTENCIÓN — CREACIÓN DE TABLAS
// ══════════════════════════════════════════════════════
async function setupMantenciones(q){
  // Ampliar equipos con campos técnicos de mantenimiento
  const equipoCols=[
    ["tipo_activo","VARCHAR(20) DEFAULT 'maquinaria'"],
    ["familia","VARCHAR(50)"],["marca","VARCHAR(50)"],
    ["modelo_equipo","VARCHAR(100)"],["anio_fabricacion","INT"],
    ["numero_serie","VARCHAR(80)"],["patente","VARCHAR(20)"],
    ["motor_descripcion","VARCHAR(80)"],["transmision_descripcion","VARCHAR(80)"],
    ["cap_aceite_motor","NUMERIC(8,2)"],["cap_aceite_hidraulico","NUMERIC(8,2)"],
    ["cap_transmision","NUMERIC(8,2)"],["cap_refrigerante","NUMERIC(8,2)"],
    ["cap_combustible_equipo","NUMERIC(8,2)"],["tipo_combustible_equipo","VARCHAR(20)"],
    ["horometro_actual","NUMERIC(10,1) DEFAULT 0"],["kilometraje_actual","INT DEFAULT 0"],
    ["fecha_puesta_servicio","DATE"],
    ["estado_operativo","VARCHAR(20) DEFAULT 'operativo'"],
    ["criticidad","VARCHAR(10) DEFAULT 'media'"],
    ["observaciones_tecnicas","TEXT"]
  ];
  for(const [nm,def] of equipoCols){
    try{await q(`ALTER TABLE equipos ADD COLUMN IF NOT EXISTS ${nm} ${def}`);}catch(e){}
  }

  await q(`CREATE TABLE IF NOT EXISTS mant_planes (
    plan_id SERIAL PRIMARY KEY,
    empresa_id INT REFERENCES empresas(empresa_id),
    nombre VARCHAR(150) NOT NULL,
    descripcion TEXT,
    tipo_activo VARCHAR(20) DEFAULT 'todos',
    familia VARCHAR(50),
    marca VARCHAR(50),
    modelo_filtro VARCHAR(100),
    equipo_id INT REFERENCES equipos(equipo_id),
    sistema VARCHAR(60),
    componente VARCHAR(100),
    tipo_mantencion VARCHAR(30) DEFAULT 'preventivo',
    intervalo_horas NUMERIC(8,1),
    intervalo_km INT,
    intervalo_dias INT,
    tolerancia_horas NUMERIC(6,1) DEFAULT 10,
    tolerancia_km INT DEFAULT 200,
    tolerancia_dias INT DEFAULT 5,
    tiempo_estimado_hrs NUMERIC(5,1),
    prioridad VARCHAR(10) DEFAULT 'normal',
    checklist_items JSONB DEFAULT '[]',
    repuestos_sugeridos JSONB DEFAULT '[]',
    lubricantes_sugeridos JSONB DEFAULT '[]',
    activo BOOLEAN DEFAULT true,
    creado_en TIMESTAMP DEFAULT NOW()
  )`);

  await q(`CREATE TABLE IF NOT EXISTS mant_avisos (
    aviso_id SERIAL PRIMARY KEY,
    empresa_id INT REFERENCES empresas(empresa_id),
    equipo_id INT NOT NULL REFERENCES equipos(equipo_id),
    faena_id INT REFERENCES faenas(faena_id),
    fecha DATE NOT NULL DEFAULT CURRENT_DATE,
    reportado_por VARCHAR(100),
    criticidad VARCHAR(10) DEFAULT 'media',
    equipo_detenido BOOLEAN DEFAULT false,
    sistema VARCHAR(60),
    sintoma TEXT NOT NULL,
    observaciones TEXT,
    estado VARCHAR(20) DEFAULT 'pendiente',
    ot_id INT,
    usuario VARCHAR(100),
    creado_en TIMESTAMP DEFAULT NOW()
  )`);

  await q(`CREATE TABLE IF NOT EXISTS mant_ot (
    ot_id SERIAL PRIMARY KEY,
    numero_ot VARCHAR(30) UNIQUE NOT NULL,
    empresa_id INT REFERENCES empresas(empresa_id),
    equipo_id INT NOT NULL REFERENCES equipos(equipo_id),
    faena_id INT REFERENCES faenas(faena_id),
    plan_id INT REFERENCES mant_planes(plan_id),
    aviso_id INT REFERENCES mant_avisos(aviso_id),
    tipo_mantencion VARCHAR(30) NOT NULL DEFAULT 'preventivo',
    origen VARCHAR(30) DEFAULT 'manual',
    fecha_apertura DATE NOT NULL DEFAULT CURRENT_DATE,
    fecha_programada DATE,
    fecha_inicio TIMESTAMP,
    fecha_termino TIMESTAMP,
    horometro_servicio NUMERIC(10,1),
    kilometraje_servicio INT,
    estado VARCHAR(20) DEFAULT 'abierta',
    prioridad VARCHAR(10) DEFAULT 'normal',
    sistema VARCHAR(60),
    sintoma_reportado TEXT,
    diagnostico TEXT,
    causa TEXT,
    trabajo_realizado TEXT,
    observaciones TEXT,
    responsable VARCHAR(100),
    mecanico_asignado VARCHAR(100),
    taller_tipo VARCHAR(15) DEFAULT 'interno',
    taller_nombre VARCHAR(100),
    tiempo_detenido_hrs NUMERIC(7,2) DEFAULT 0,
    costo_repuestos NUMERIC(14,2) DEFAULT 0,
    costo_lubricantes NUMERIC(14,2) DEFAULT 0,
    costo_mano_obra_interna NUMERIC(14,2) DEFAULT 0,
    costo_mano_obra_externa NUMERIC(14,2) DEFAULT 0,
    costo_servicios NUMERIC(14,2) DEFAULT 0,
    costo_traslado NUMERIC(14,2) DEFAULT 0,
    costo_otros NUMERIC(14,2) DEFAULT 0,
    costo_total NUMERIC(14,2) DEFAULT 0,
    usuario VARCHAR(100),
    creado_en TIMESTAMP DEFAULT NOW(),
    actualizado_en TIMESTAMP DEFAULT NOW()
  )`);

  await q(`CREATE TABLE IF NOT EXISTS mant_ot_tareas (
    tarea_id SERIAL PRIMARY KEY,
    ot_id INT NOT NULL REFERENCES mant_ot(ot_id) ON DELETE CASCADE,
    orden INT DEFAULT 0,
    descripcion VARCHAR(300) NOT NULL,
    sistema VARCHAR(60),
    tipo VARCHAR(20) DEFAULT 'tarea',
    estado VARCHAR(20) DEFAULT 'pendiente',
    observacion TEXT,
    desde_plan BOOLEAN DEFAULT false,
    creado_en TIMESTAMP DEFAULT NOW()
  )`);

  await q(`CREATE TABLE IF NOT EXISTS mant_ot_materiales (
    material_id SERIAL PRIMARY KEY,
    ot_id INT NOT NULL REFERENCES mant_ot(ot_id) ON DELETE CASCADE,
    tipo VARCHAR(20) DEFAULT 'repuesto',
    prod_id INT REFERENCES productos(producto_id),
    descripcion VARCHAR(200) NOT NULL,
    cantidad NUMERIC(10,3) NOT NULL,
    unidad VARCHAR(20),
    precio_unitario NUMERIC(14,2) DEFAULT 0,
    costo_total NUMERIC(14,2) DEFAULT 0,
    origen VARCHAR(20) DEFAULT 'inventario',
    creado_en TIMESTAMP DEFAULT NOW()
  )`);

  await q(`CREATE TABLE IF NOT EXISTS mant_lecturas (
    lectura_id SERIAL PRIMARY KEY,
    equipo_id INT NOT NULL REFERENCES equipos(equipo_id),
    fecha DATE NOT NULL DEFAULT CURRENT_DATE,
    horometro NUMERIC(10,1),
    kilometraje INT,
    origen VARCHAR(30) DEFAULT 'manual',
    ot_id INT,
    usuario VARCHAR(100),
    creado_en TIMESTAMP DEFAULT NOW()
  )`);

  await q(`CREATE TABLE IF NOT EXISTS mant_programacion (
    prog_id SERIAL PRIMARY KEY,
    equipo_id INT NOT NULL REFERENCES equipos(equipo_id),
    plan_id INT NOT NULL REFERENCES mant_planes(plan_id),
    empresa_id INT REFERENCES empresas(empresa_id),
    proxima_fecha DATE,
    proxima_horas NUMERIC(10,1),
    proxima_km INT,
    ultima_ejecucion_fecha DATE,
    ultima_ejecucion_horas NUMERIC(10,1),
    ultima_ejecucion_km INT,
    ultima_ot_id INT,
    estado VARCHAR(20) DEFAULT 'vigente',
    creado_en TIMESTAMP DEFAULT NOW(),
    actualizado_en TIMESTAMP DEFAULT NOW(),
    UNIQUE(equipo_id,plan_id)
  )`);

  // ── Tabla personal (maestro general de personas) ──
  await q(`CREATE TABLE IF NOT EXISTS personal (
    persona_id SERIAL PRIMARY KEY,
    empresa_id INT REFERENCES empresas(empresa_id),
    nombre_completo VARCHAR(150) NOT NULL,
    rut VARCHAR(20),
    cargo VARCHAR(80),
    especialidad VARCHAR(80),
    telefono VARCHAR(30),
    correo VARCHAR(100),
    participa_mantencion BOOLEAN DEFAULT false,
    valor_hora_hombre NUMERIC(12,2),
    moneda VARCHAR(5) DEFAULT 'CLP',
    activo BOOLEAN DEFAULT true,
    observaciones TEXT,
    creado_en TIMESTAMP DEFAULT NOW()
  )`);

  // ── OT Sistemas (muchos a muchos OT ↔ sistema) ──
  await q(`CREATE TABLE IF NOT EXISTS mant_ot_sistemas (
    id SERIAL PRIMARY KEY,
    ot_id INT NOT NULL REFERENCES mant_ot(ot_id) ON DELETE CASCADE,
    sistema VARCHAR(60) NOT NULL,
    es_principal BOOLEAN DEFAULT false,
    UNIQUE(ot_id, sistema)
  )`);

  // ── OT Personal (muchos a muchos OT ↔ persona) ──
  await q(`CREATE TABLE IF NOT EXISTS mant_ot_personal (
    id SERIAL PRIMARY KEY,
    ot_id INT NOT NULL REFERENCES mant_ot(ot_id) ON DELETE CASCADE,
    persona_id INT NOT NULL REFERENCES personal(persona_id),
    rol VARCHAR(60) DEFAULT 'ejecutor',
    horas_trabajadas NUMERIC(7,2) DEFAULT 0,
    valor_hora_aplicado NUMERIC(12,2) DEFAULT 0,
    costo_total NUMERIC(14,2) GENERATED ALWAYS AS (horas_trabajadas * valor_hora_aplicado) STORED,
    observacion TEXT,
    UNIQUE(ot_id, persona_id)
  )`);

  // ── Agregar ot_id a ordenes_compra_detalle ──
  try{ await q('ALTER TABLE ordenes_compra_detalle ADD COLUMN IF NOT EXISTS ot_id INT REFERENCES mant_ot(ot_id)'); }catch(e){}

    // Indices
  const idxs=[
    'CREATE INDEX IF NOT EXISTS idx_mant_ot_equipo ON mant_ot(equipo_id)',
    'CREATE INDEX IF NOT EXISTS idx_mant_ot_estado ON mant_ot(estado)',
    'CREATE INDEX IF NOT EXISTS idx_mant_avisos_equipo ON mant_avisos(equipo_id)',
    'CREATE INDEX IF NOT EXISTS idx_mant_lecturas_equipo ON mant_lecturas(equipo_id,fecha DESC)',
    'CREATE INDEX IF NOT EXISTS idx_mant_prog_equipo ON mant_programacion(equipo_id)',
  ];
  for(const i of idxs){try{await q(i);}catch(e){}}
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
    try { await pool.query(sql); } catch(e) { console.error('[SETUP ERR]',sql.substring(0,60),'→',e.message.substring(0,100)); }
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
  // Factor de conversion (v2.3)
  await q(`ALTER TABLE productos ADD COLUMN IF NOT EXISTS unidad_compra VARCHAR(30)`);
  await q(`ALTER TABLE productos ADD COLUMN IF NOT EXISTS factor_conversion NUMERIC(10,4) DEFAULT 1`);
  // ── Control de Combustibles (v3.0) ──────────────────────────────────────
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

  // Control de Combustibles — tablas en orden correcto (después de empresas, faenas, equipos)
  await q(`CREATE TABLE IF NOT EXISTS comb_tipos (tipo_id SERIAL PRIMARY KEY, nombre VARCHAR(50) NOT NULL UNIQUE, activo BOOLEAN DEFAULT true)`);
  await q(`CREATE TABLE IF NOT EXISTS comb_estanques (estanque_id SERIAL PRIMARY KEY, empresa_id INT NOT NULL REFERENCES empresas(empresa_id), codigo VARCHAR(20) NOT NULL UNIQUE, nombre VARCHAR(100) NOT NULL, tipo_estanque VARCHAR(30) NOT NULL DEFAULT 'FIJO', ubicacion VARCHAR(150), capacidad_max NUMERIC(10,2), tipo_combustible_id INT REFERENCES comb_tipos(tipo_id), observaciones TEXT, activo BOOLEAN DEFAULT true, creado_en TIMESTAMP DEFAULT NOW())`);
  await q(`CREATE TABLE IF NOT EXISTS comb_stock (estanque_id INT NOT NULL REFERENCES comb_estanques(estanque_id), tipo_id INT NOT NULL REFERENCES comb_tipos(tipo_id), litros_disponibles NUMERIC(12,3) DEFAULT 0, costo_promedio NUMERIC(14,4) DEFAULT 0, ultima_actualizacion TIMESTAMP DEFAULT NOW(), PRIMARY KEY(estanque_id,tipo_id))`);
  await q(`CREATE TABLE IF NOT EXISTS comb_movimientos (mov_id SERIAL PRIMARY KEY, tipo_mov VARCHAR(20) NOT NULL, empresa_id INT REFERENCES empresas(empresa_id), fecha DATE NOT NULL, tipo_id INT NOT NULL REFERENCES comb_tipos(tipo_id), estanque_origen_id INT REFERENCES comb_estanques(estanque_id), estanque_destino_id INT REFERENCES comb_estanques(estanque_id), equipo_id INT REFERENCES equipos(equipo_id), faena_id INT REFERENCES faenas(faena_id), proveedor_id INT REFERENCES proveedores(proveedor_id), litros NUMERIC(12,3) NOT NULL, precio_unitario NUMERIC(14,4) DEFAULT 0, costo_total NUMERIC(14,2) DEFAULT 0, horometro NUMERIC(10,1), kilometraje NUMERIC(10,1), responsable VARCHAR(100), numero_documento VARCHAR(30), oc_referencia VARCHAR(30), observaciones TEXT, estado VARCHAR(10) DEFAULT 'ACTIVO', motivo_anulacion TEXT, usuario VARCHAR(100), creado_en TIMESTAMP DEFAULT NOW(), anulado_en TIMESTAMP, anulado_por VARCHAR(100))`);
  await q(`CREATE INDEX IF NOT EXISTS idx_comb_mov_fecha ON comb_movimientos(fecha)`);
  await q(`CREATE INDEX IF NOT EXISTS idx_comb_mov_tipo ON comb_movimientos(tipo_mov)`);
  await q(`CREATE INDEX IF NOT EXISTS idx_comb_mov_equipo ON comb_movimientos(equipo_id)`);
  // Cierres quincenales Copec
  await q(`CREATE TABLE IF NOT EXISTS comb_cierres (
    cierre_id SERIAL PRIMARY KEY,
    empresa_id INT REFERENCES empresas(empresa_id),
    proveedor_id INT REFERENCES proveedores(proveedor_id),
    numero_factura VARCHAR(30),
    fecha_factura DATE,
    litros_total NUMERIC(12,3),
    base_afecta NUMERIC(14,2),
    ie_total NUMERIC(14,2),
    iva NUMERIC(14,2),
    total_factura NUMERIC(14,2),
    precio_neto_litro NUMERIC(14,4),
    estado VARCHAR(15) DEFAULT 'PENDIENTE',
    observaciones TEXT,
    usuario VARCHAR(100),
    creado_en TIMESTAMP DEFAULT NOW(),
    procesado_en TIMESTAMP,
    procesado_por VARCHAR(100)
  )`);
  await q(`CREATE TABLE IF NOT EXISTS comb_cierre_guias (
    id SERIAL PRIMARY KEY,
    cierre_id INT NOT NULL REFERENCES comb_cierres(cierre_id) ON DELETE CASCADE,
    mov_id INT NOT NULL REFERENCES comb_movimientos(mov_id),
    litros NUMERIC(12,3),
    precio_provisorio NUMERIC(14,4),
    precio_real NUMERIC(14,4),
    diferencia_total NUMERIC(14,2)
  )`);
  // Campo provisorio en movimientos
  await q(`ALTER TABLE comb_movimientos ADD COLUMN IF NOT EXISTS es_provisorio BOOLEAN DEFAULT false`);
  await q(`ALTER TABLE comb_movimientos ADD COLUMN IF NOT EXISTS cierre_id INT REFERENCES comb_cierres(cierre_id)`);
  // Seed tipos de combustible
  for(const t of ['Diesel','Gasolina 93','Gasolina 95','Gasolina 97']){
    await q(`INSERT INTO comb_tipos(nombre) SELECT '${t}' WHERE NOT EXISTS(SELECT 1 FROM comb_tipos WHERE nombre='${t}')`);
  }

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
  await setupMantenciones(q);
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
  // Mantención module tables
  try{ await setupMantenciones(pool.query.bind(pool)); }catch(e){console.log('[WARN] mant tables:',e.message);}
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
prvR.delete('/:id', auth, async(req,res)=>{
  try{
    await pool.query('DELETE FROM proveedores WHERE proveedor_id=$1',[req.params.id]);
    res.json({ok:true});
  }catch(e){
    if(e.code==='23503') return res.status(409).json({error:'No se puede eliminar: proveedor en uso. Use Inactivar.'});
    res.status(400).json({error:e.message});
  }
});
app.use('/api/proveedores', prvR);

// PRODUCTOS con delete
const prR=express.Router();
prR.get('/', auth, async(req,res)=>{try{res.json((await pool.query('SELECT p.*,sc.nombre AS subcategoria_nombre,ca.categoria_id,ca.nombre AS categoria_nombre FROM productos p JOIN subcategorias sc ON p.subcategoria_id=sc.subcategoria_id JOIN categorias ca ON sc.categoria_id=ca.categoria_id ORDER BY p.nombre')).rows);}catch(e){res.status(500).json({error:e.message});}});
prR.get('/:id', auth, async(req,res)=>{try{res.json((await pool.query('SELECT * FROM productos WHERE producto_id=$1',[req.params.id])).rows[0]);}catch(e){res.status(500).json({error:e.message});}});
prR.post('/', auth, async(req,res)=>{
  try{
    const{codigo,nombre,descripcion,subcategoria_id,unidad_medida,stock_minimo,stock_maximo,costo_referencia,unidad_compra,factor_conversion}=req.body;
    const scId=subcategoria_id?parseInt(subcategoria_id):null;
    if(!scId)throw new Error('Debe seleccionar un Tipo de Producto');
    const r=await pool.query('INSERT INTO productos(codigo,nombre,descripcion,subcategoria_id,unidad_medida,stock_minimo,stock_maximo,costo_referencia,unidad_compra,factor_conversion) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING *',[codigo,nombre,descripcion||null,scId,unidad_medida||'UN',stock_minimo||0,stock_maximo||null,costo_referencia||0,unidad_compra||null,parseFloat(factor_conversion)||1]);
    res.status(201).json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
prR.put('/:id', auth, async(req,res)=>{
  try{
    const{codigo,nombre,descripcion,subcategoria_id,unidad_medida,stock_minimo,stock_maximo,costo_referencia,unidad_compra,factor_conversion}=req.body;
    const scId2=subcategoria_id?parseInt(subcategoria_id):null;
    if(!scId2)throw new Error('Debe seleccionar un Tipo de Producto');
    const r=await pool.query('UPDATE productos SET codigo=$1,nombre=$2,descripcion=$3,subcategoria_id=$4,unidad_medida=$5,stock_minimo=$6,stock_maximo=$7,costo_referencia=$8,unidad_compra=$9,factor_conversion=$10,modificado_en=NOW() WHERE producto_id=$11 RETURNING *',[codigo,nombre,descripcion||null,scId2,unidad_medida||'UN',stock_minimo||0,stock_maximo||null,costo_referencia||0,unidad_compra||null,parseFloat(factor_conversion)||1,req.params.id]);
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
    const prod_map=req.body.prod_map||{};
    const factor_map=req.body.factor_map||{};
    const comb_map=req.body.comb_map||{};
    let lineas=await client.query('SELECT * FROM ordenes_compra_detalle WHERE oc_id=$1 AND ingresa_bodega=true',[req.params.id]);
    if(!lineas.rows.length) lineas=await client.query('SELECT * FROM ordenes_compra_detalle WHERE oc_id=$1 AND (ingresa_bodega IS NULL OR ingresa_bodega=true)',[req.params.id]);
    if(!lineas.rows.length) throw new Error('No hay lineas marcadas para ingresar.');
    const lineasComb=lineas.rows.filter(function(l){return !!comb_map[String(l.detalle_id)];});
    const lineasInv=lineas.rows.filter(function(l){
      const pid=prod_map[String(l.detalle_id)]?parseInt(prod_map[String(l.detalle_id)]):l.producto_id;
      return !!pid&&!comb_map[String(l.detalle_id)];
    }).map(function(l){
      return Object.assign({},l,{producto_id:prod_map[String(l.detalle_id)]?parseInt(prod_map[String(l.detalle_id)]):l.producto_id});
    });
    if(!lineasComb.length&&!lineasInv.length) throw new Error('Debe asignar destino a al menos una linea.');
    let movId=null;
    // Inventory lines → bodega
    if(lineasInv.length>0){
      const bodegaEfectiva=bodega_id||oc.bodega_ingreso_id||(await client.query('SELECT bodega_id FROM bodegas WHERE activo=true ORDER BY bodega_id LIMIT 1')).rows[0]?.bodega_id;
      if(!bodegaEfectiva) throw new Error('Debe seleccionar una bodega de recepcion.');
      const mr=await client.query('INSERT INTO movimiento_encabezado(tipo_movimiento,fecha,bodega_id,proveedor_id,tipo_doc_id,numero_documento,fecha_documento,oc_referencia,observaciones,usuario) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING movimiento_id',['INGRESO',oc.fecha_documento||new Date().toISOString().split('T')[0],bodegaEfectiva,oc.proveedor_id,oc.tipo_doc_id,oc.numero_documento,oc.fecha_documento,oc.numero_oc,'Recepcion OC '+oc.numero_oc,req.user.email]);
      movId=mr.rows[0].movimiento_id;
      for(const l of lineasInv){
        const pid=l.producto_id,cantCompra=parseFloat(l.cantidad),cu=parseFloat(l.precio_unitario)||0;
        const bodDest=l.bodega_destino_id||bodegaEfectiva;
        const pInfo=await client.query('SELECT factor_conversion FROM productos WHERE producto_id=$1',[pid]);
        const factorOverride=factor_map[String(l.detalle_id)];
        const factor=factorOverride?parseFloat(factorOverride)||1:parseFloat((pInfo.rows[0]||{}).factor_conversion)||1;
        const qty=cantCompra*factor,cuBase=factor>1?cu/factor:cu;
        const sr=await client.query('SELECT cantidad_disponible,costo_promedio_actual FROM stock_actual WHERE producto_id=$1 AND bodega_id=$2',[pid,bodDest]);
        const cur=sr.rows[0]||{cantidad_disponible:0,costo_promedio_actual:0};
        const curQ=parseFloat(cur.cantidad_disponible),curCpp=parseFloat(cur.costo_promedio_actual);
        const newQ=curQ+qty,newCpp=newQ>0?(curQ*curCpp+qty*cuBase)/newQ:cuBase;
        await client.query('INSERT INTO movimiento_detalle(movimiento_id,producto_id,cantidad,costo_unitario) VALUES($1,$2,$3,$4)',[movId,pid,qty,cuBase]);
        await client.query('INSERT INTO stock_actual(producto_id,bodega_id,cantidad_disponible,costo_promedio_actual,ultima_actualizacion) VALUES($1,$2,$3,$4,NOW()) ON CONFLICT(producto_id,bodega_id) DO UPDATE SET cantidad_disponible=$3,costo_promedio_actual=$4,ultima_actualizacion=NOW()',[pid,bodDest,newQ,newCpp]);
      }
    }
    // Combustible lines → comb_stock
    for(const l of lineasComb){
      const estanqueId=parseInt(comb_map[String(l.detalle_id)]);
      const lts=parseFloat(l.cantidad),pu=parseFloat(l.precio_unitario)||0;
      const estQ=await client.query('SELECT tipo_combustible_id,empresa_id FROM comb_estanques WHERE estanque_id=$1',[estanqueId]);
      if(!estQ.rows.length) throw new Error('Estanque no encontrado');
      const tipoId=estQ.rows[0].tipo_combustible_id,empresaId=estQ.rows[0].empresa_id;
      if(!tipoId) throw new Error('El estanque no tiene tipo de combustible asignado. Configure el estanque primero.');
      const stk=await client.query('SELECT litros_disponibles,costo_promedio FROM comb_stock WHERE estanque_id=$1 AND tipo_id=$2',[estanqueId,tipoId]);
      if(stk.rows.length){
        const curQ=parseFloat(stk.rows[0].litros_disponibles),curCpp=parseFloat(stk.rows[0].costo_promedio);
        const newQ=curQ+lts,newCpp=newQ>0?(curQ*curCpp+lts*pu)/newQ:pu;
        await client.query('UPDATE comb_stock SET litros_disponibles=$1,costo_promedio=$2,ultima_actualizacion=NOW() WHERE estanque_id=$3 AND tipo_id=$4',[newQ,newCpp,estanqueId,tipoId]);
      }else{
        await client.query('INSERT INTO comb_stock(estanque_id,tipo_id,litros_disponibles,costo_promedio) VALUES($1,$2,$3,$4)',[estanqueId,tipoId,lts,pu]);
      }
      await client.query('INSERT INTO comb_movimientos(tipo_mov,empresa_id,fecha,tipo_id,estanque_destino_id,litros,precio_unitario,costo_total,proveedor_id,numero_documento,oc_referencia,usuario) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)',
        ['INGRESO_STOCK',empresaId,oc.fecha_documento||new Date().toISOString().split('T')[0],tipoId,estanqueId,lts,pu,lts*pu,oc.proveedor_id,oc.numero_documento,oc.numero_oc,req.user.email]);
    }
    await client.query('UPDATE ordenes_compra SET movimiento_id=$1,modificado_en=NOW() WHERE oc_id=$2',[movId,req.params.id]);
    await client.query('COMMIT');
    res.json({ok:true,movimiento_id:movId,comb_lines:lineasComb.length});
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
// catch-all moved to after API routes


// ════════════════════════════════════════════════════
// CONTROL DE COMBUSTIBLES
// ════════════════════════════════════════════════════

// Tipos de combustible
app.get('/api/comb/tipos', auth, async(req,res)=>{
  try{res.json((await pool.query('SELECT * FROM comb_tipos ORDER BY nombre')).rows);}
  catch(e){res.status(500).json({error:e.message});}
});
app.post('/api/comb/tipos', auth, async(req,res)=>{
  try{const r=await pool.query('INSERT INTO comb_tipos(nombre) VALUES($1) RETURNING *',[req.body.nombre]);res.status(201).json(r.rows[0]);}
  catch(e){res.status(400).json({error:e.message});}
});
app.put('/api/comb/tipos/:id', auth, async(req,res)=>{
  try{
    const r=await pool.query('UPDATE comb_tipos SET nombre=$1 WHERE tipo_id=$2 RETURNING *',[req.body.nombre,req.params.id]);
    res.json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
app.patch('/api/comb/tipos/:id/activo', auth, async(req,res)=>{
  try{res.json((await pool.query('UPDATE comb_tipos SET activo=NOT activo WHERE tipo_id=$1 RETURNING *',[req.params.id])).rows[0]);}
  catch(e){res.status(400).json({error:e.message});}
});

// Estanques
app.get('/api/comb/estanques', auth, async(req,res)=>{
  try{
    // Try full query first; if comb_tipos/comb_stock don't exist yet, fallback to simple query
    let rows;
    try{
      const r=await pool.query(`SELECT e.*,emp.razon_social AS empresa_nombre,ct.nombre AS tipo_comb_nombre,cs.litros_disponibles,cs.costo_promedio
        FROM comb_estanques e
        LEFT JOIN empresas emp ON e.empresa_id=emp.empresa_id
        LEFT JOIN comb_tipos ct ON e.tipo_combustible_id=ct.tipo_id
        LEFT JOIN comb_stock cs ON cs.estanque_id=e.estanque_id AND cs.tipo_id=e.tipo_combustible_id
        ORDER BY emp.razon_social NULLS LAST,e.nombre`);
      rows=r.rows;
    }catch(_){
      const r2=await pool.query('SELECT e.*,emp.razon_social AS empresa_nombre FROM comb_estanques e LEFT JOIN empresas emp ON e.empresa_id=emp.empresa_id ORDER BY e.nombre');
      rows=r2.rows;
    }
    res.json(rows);
  }catch(e){res.status(500).json({error:e.message});}
});
app.post('/api/comb/estanques', auth, async(req,res)=>{
  console.log('[COMB-EST POST] body:', JSON.stringify(req.body));
  try{
    const{empresa_id,codigo,nombre,tipo_estanque,ubicacion,capacidad_max,tipo_combustible_id,observaciones}=req.body;
    const empId=parseInt(empresa_id);
    console.log('[COMB-EST POST] empId:', empId, 'codigo:', codigo, 'nombre:', nombre);
    if(!empId) return res.status(400).json({error:'Debe seleccionar una empresa'});
    if(!codigo) return res.status(400).json({error:'El codigo es obligatorio'});
    if(!nombre) return res.status(400).json({error:'El nombre es obligatorio'});
    const r=await pool.query('INSERT INTO comb_estanques(empresa_id,codigo,nombre,tipo_estanque,ubicacion,capacidad_max,tipo_combustible_id,observaciones) VALUES($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *',
      [empId,codigo.trim(),nombre.trim(),tipo_estanque||'FIJO',ubicacion||null,capacidad_max?parseFloat(capacidad_max):null,tipo_combustible_id?parseInt(tipo_combustible_id):null,observaciones||null]);
    console.log('[COMB-EST POST] OK, id:', r.rows[0].estanque_id);
    res.status(201).json(r.rows[0]);
  }catch(e){
    console.error('[COMB-EST POST] ERROR:', e.message);
    res.status(400).json({error:e.message});
  }
});
app.put('/api/comb/estanques/:id', auth, async(req,res)=>{
  try{
    const{empresa_id,codigo,nombre,tipo_estanque,ubicacion,capacidad_max,tipo_combustible_id,observaciones}=req.body;
    const empId=parseInt(empresa_id);
    if(!empId) return res.status(400).json({error:'Debe seleccionar una empresa'});
    const r=await pool.query('UPDATE comb_estanques SET empresa_id=$1,codigo=$2,nombre=$3,tipo_estanque=$4,ubicacion=$5,capacidad_max=$6,tipo_combustible_id=$7,observaciones=$8 WHERE estanque_id=$9 RETURNING *',
      [empId,codigo.trim(),nombre.trim(),tipo_estanque||'FIJO',ubicacion||null,capacidad_max?parseFloat(capacidad_max):null,tipo_combustible_id?parseInt(tipo_combustible_id):null,observaciones||null,req.params.id]);
    res.json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
app.delete('/api/comb/estanques/:id', auth, async(req,res)=>{
  try{
    // Check if estanque has stock
    const stk=await pool.query('SELECT litros_disponibles FROM comb_stock WHERE estanque_id=$1',[req.params.id]);
    if(stk.rows.some(r=>parseFloat(r.litros_disponibles)>0))
      return res.status(409).json({error:'No se puede eliminar: el estanque tiene stock. Primero distribuya o traslade el combustible.'});
    await pool.query('DELETE FROM comb_stock WHERE estanque_id=$1',[req.params.id]);
    await pool.query('DELETE FROM comb_estanques WHERE estanque_id=$1',[req.params.id]);
    res.json({ok:true});
  }catch(e){res.status(400).json({error:e.message});}
});
app.patch('/api/comb/estanques/:id/activo', auth, async(req,res)=>{
  try{res.json((await pool.query('UPDATE comb_estanques SET activo=NOT activo WHERE estanque_id=$1 RETURNING *',[req.params.id])).rows[0]);}
  catch(e){res.status(400).json({error:e.message});}
});

// Stock de combustible por estanque
app.get('/api/comb/stock', auth, async(req,res)=>{
  try{
    const r=await pool.query(`SELECT cs.*,e.nombre AS estanque_nombre,e.codigo AS estanque_codigo,e.tipo_estanque,
      emp.razon_social AS empresa_nombre, ct.nombre AS tipo_nombre,
      ROUND(cs.litros_disponibles*cs.costo_promedio,0) AS valor_total
      FROM comb_stock cs
      JOIN comb_estanques e ON cs.estanque_id=e.estanque_id
      JOIN empresas emp ON e.empresa_id=emp.empresa_id
      JOIN comb_tipos ct ON cs.tipo_id=ct.tipo_id
      WHERE cs.litros_disponibles>0
      ORDER BY emp.razon_social,e.nombre`);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});

// Movimientos (lista)
app.get('/api/comb/movimientos', auth, async(req,res)=>{
  try{
    const{tipo_mov,empresa_id,desde,hasta,estanque_id,equipo_id,tipo_id,faena_id}=req.query;
    let where=["m.estado='ACTIVO'"],vals=[];
    if(tipo_mov){vals.push(tipo_mov);where.push(`m.tipo_mov=$${vals.length}`);}
    if(empresa_id){vals.push(empresa_id);where.push(`m.empresa_id=$${vals.length}`);}
    if(desde){vals.push(desde);where.push(`m.fecha>=$${vals.length}`);}
    if(hasta){vals.push(hasta);where.push(`m.fecha<=$${vals.length}`);}
    if(estanque_id){vals.push(estanque_id);where.push(`(m.estanque_origen_id=$${vals.length} OR m.estanque_destino_id=$${vals.length})`);}
    if(equipo_id){vals.push(equipo_id);where.push(`m.equipo_id=$${vals.length}`);}
    if(tipo_id){vals.push(tipo_id);where.push(`m.tipo_id=$${vals.length}`);}
    if(faena_id){vals.push(faena_id);where.push(`m.faena_id=$${vals.length}`);}
    const r=await pool.query(`
      SELECT m.*,ct.nombre AS tipo_nombre,
        eo.nombre AS estanque_origen,ed.nombre AS estanque_destino,
        eq.nombre AS equipo_nombre,f.nombre AS faena_nombre,
        pr.nombre AS proveedor_nombre,emp.razon_social AS empresa_nombre
      FROM comb_movimientos m
      LEFT JOIN comb_tipos ct ON m.tipo_id=ct.tipo_id
      LEFT JOIN comb_estanques eo ON m.estanque_origen_id=eo.estanque_id
      LEFT JOIN comb_estanques ed ON m.estanque_destino_id=ed.estanque_id
      LEFT JOIN equipos eq ON m.equipo_id=eq.equipo_id
      LEFT JOIN faenas f ON m.faena_id=f.faena_id
      LEFT JOIN proveedores pr ON m.proveedor_id=pr.proveedor_id
      LEFT JOIN empresas emp ON m.empresa_id=emp.empresa_id
      WHERE ${where.join(' AND ')}
      ORDER BY m.fecha DESC,m.mov_id DESC`,vals);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});

// Compra directa (no genera stock)
app.post('/api/comb/compra-directa', auth, async(req,res)=>{
  try{
    const{empresa_id,fecha,tipo_id,litros,precio_unitario,equipo_id,faena_id,proveedor_id,numero_documento,oc_referencia,observaciones}=req.body;
    if(!equipo_id) throw new Error('La compra directa debe asociarse a un equipo o vehículo');
    const costo_total=parseFloat(litros)*parseFloat(precio_unitario||0);
    const r=await pool.query(`INSERT INTO comb_movimientos(tipo_mov,empresa_id,fecha,tipo_id,litros,precio_unitario,costo_total,equipo_id,faena_id,proveedor_id,numero_documento,oc_referencia,observaciones,usuario)
      VALUES('COMPRA_DIRECTA',$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) RETURNING *`,
      [empresa_id||null,fecha,tipo_id,parseFloat(litros),parseFloat(precio_unitario||0),costo_total,equipo_id,faena_id||null,proveedor_id||null,numero_documento||null,oc_referencia||null,observaciones||null,req.user.email]);
    res.status(201).json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});

// Ingreso a stock en estanque
app.post('/api/comb/ingreso-stock', auth, async(req,res)=>{
  const client=await pool.connect();
  try{
    await client.query('BEGIN');
    const{empresa_id,fecha,tipo_id,estanque_destino_id,litros,precio_unitario,proveedor_id,numero_documento,oc_referencia,observaciones,es_provisorio}=req.body;
    if(!estanque_destino_id) throw new Error('Debe seleccionar el estanque destino');
    const lts=parseFloat(litros);
    const pu=parseFloat(precio_unitario||0);
    const costo_total=lts*pu;
    // Verificar capacidad
    const est=await client.query('SELECT capacidad_max FROM comb_estanques WHERE estanque_id=$1',[estanque_destino_id]);
    if(est.rows[0]?.capacidad_max){
      const stk=await client.query('SELECT litros_disponibles FROM comb_stock WHERE estanque_id=$1 AND tipo_id=$2',[estanque_destino_id,tipo_id]);
      const actual=parseFloat(stk.rows[0]?.litros_disponibles||0);
      if(actual+lts>parseFloat(est.rows[0].capacidad_max)) throw new Error(`Capacidad máxima del estanque superada (máx: ${est.rows[0].capacidad_max} lts)`);
    }
    // Actualizar stock (CPP)
    const stk=await client.query('SELECT litros_disponibles,costo_promedio FROM comb_stock WHERE estanque_id=$1 AND tipo_id=$2',[estanque_destino_id,tipo_id]);
    if(stk.rows.length){
      const curQ=parseFloat(stk.rows[0].litros_disponibles),curCpp=parseFloat(stk.rows[0].costo_promedio);
      const newQ=curQ+lts,newCpp=newQ>0?(curQ*curCpp+lts*pu)/newQ:pu;
      await client.query('UPDATE comb_stock SET litros_disponibles=$1,costo_promedio=$2,ultima_actualizacion=NOW() WHERE estanque_id=$3 AND tipo_id=$4',[newQ,newCpp,estanque_destino_id,tipo_id]);
    }else{
      await client.query('INSERT INTO comb_stock(estanque_id,tipo_id,litros_disponibles,costo_promedio) VALUES($1,$2,$3,$4)',[estanque_destino_id,tipo_id,lts,pu]);
    }
    const r=await client.query(`INSERT INTO comb_movimientos(tipo_mov,empresa_id,fecha,tipo_id,estanque_destino_id,litros,precio_unitario,costo_total,proveedor_id,numero_documento,oc_referencia,observaciones,usuario)
      VALUES('INGRESO_STOCK',$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING *`,
      [empresa_id||null,fecha,tipo_id,estanque_destino_id,lts,pu,costo_total,proveedor_id||null,numero_documento||null,oc_referencia||null,observaciones||null,req.user.email]);
    await client.query('COMMIT');
    res.status(201).json(r.rows[0]);
  }catch(e){await client.query('ROLLBACK');res.status(400).json({error:e.message});}
  finally{client.release();}
});

// Traspaso entre estanques
app.post('/api/comb/traspaso', auth, async(req,res)=>{
  const client=await pool.connect();
  try{
    await client.query('BEGIN');
    const{empresa_id,fecha,tipo_id,estanque_origen_id,estanque_destino_id,litros,observaciones}=req.body;
    if(String(estanque_origen_id)===String(estanque_destino_id)) throw new Error('El estanque origen y destino deben ser distintos');
    const lts=parseFloat(litros);
    // Verificar stock origen
    const stk=await client.query('SELECT litros_disponibles,costo_promedio FROM comb_stock WHERE estanque_id=$1 AND tipo_id=$2',[estanque_origen_id,tipo_id]);
    if(!stk.rows.length||parseFloat(stk.rows[0].litros_disponibles)<lts) throw new Error(`Stock insuficiente en estanque origen (disponible: ${parseFloat(stk.rows[0]?.litros_disponibles||0).toFixed(2)} lts)`);
    const cpp=parseFloat(stk.rows[0].costo_promedio);
    // Descontar origen
    await client.query('UPDATE comb_stock SET litros_disponibles=litros_disponibles-$1,ultima_actualizacion=NOW() WHERE estanque_id=$2 AND tipo_id=$3',[lts,estanque_origen_id,tipo_id]);
    // Acreditar destino (mantiene valorización)
    const stkD=await client.query('SELECT litros_disponibles,costo_promedio FROM comb_stock WHERE estanque_id=$1 AND tipo_id=$2',[estanque_destino_id,tipo_id]);
    if(stkD.rows.length){
      const curQ=parseFloat(stkD.rows[0].litros_disponibles),curCpp=parseFloat(stkD.rows[0].costo_promedio);
      const newQ=curQ+lts,newCpp=newQ>0?(curQ*curCpp+lts*cpp)/newQ:cpp;
      await client.query('UPDATE comb_stock SET litros_disponibles=$1,costo_promedio=$2,ultima_actualizacion=NOW() WHERE estanque_id=$3 AND tipo_id=$4',[newQ,newCpp,estanque_destino_id,tipo_id]);
    }else{
      await client.query('INSERT INTO comb_stock(estanque_id,tipo_id,litros_disponibles,costo_promedio) VALUES($1,$2,$3,$4)',[estanque_destino_id,tipo_id,lts,cpp]);
    }
    const r=await client.query(`INSERT INTO comb_movimientos(tipo_mov,empresa_id,fecha,tipo_id,estanque_origen_id,estanque_destino_id,litros,precio_unitario,costo_total,observaciones,usuario)
      VALUES('TRASPASO',$1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING *`,
      [empresa_id||null,fecha,tipo_id,estanque_origen_id,estanque_destino_id,lts,cpp,lts*cpp,observaciones||null,req.user.email]);
    await client.query('COMMIT');
    res.status(201).json(r.rows[0]);
  }catch(e){await client.query('ROLLBACK');res.status(400).json({error:e.message});}
  finally{client.release();}
});

// Distribución a equipo/vehículo
app.post('/api/comb/distribucion', auth, async(req,res)=>{
  const client=await pool.connect();
  try{
    await client.query('BEGIN');
    const{empresa_id,fecha,tipo_id,estanque_origen_id,equipo_id,faena_id,litros,horometro,kilometraje,responsable,observaciones}=req.body;
    if(!equipo_id) throw new Error('Debe seleccionar el equipo o vehículo');
    if(!estanque_origen_id) throw new Error('Debe seleccionar el estanque de origen');
    const lts=parseFloat(litros);
    // Verificar stock
    const stk=await client.query('SELECT litros_disponibles,costo_promedio FROM comb_stock WHERE estanque_id=$1 AND tipo_id=$2',[estanque_origen_id,tipo_id]);
    const disp=parseFloat(stk.rows[0]?.litros_disponibles||0);
    if(disp<lts) throw new Error(`Stock insuficiente (disponible: ${disp.toFixed(2)} lts)`);
    const cpp=parseFloat(stk.rows[0].costo_promedio);
    const costo_total=lts*cpp;
    // Descontar stock
    await client.query('UPDATE comb_stock SET litros_disponibles=litros_disponibles-$1,ultima_actualizacion=NOW() WHERE estanque_id=$2 AND tipo_id=$3',[lts,estanque_origen_id,tipo_id]);
    const r=await client.query(`INSERT INTO comb_movimientos(tipo_mov,empresa_id,fecha,tipo_id,estanque_origen_id,equipo_id,faena_id,litros,precio_unitario,costo_total,horometro,kilometraje,responsable,observaciones,usuario)
      VALUES('DISTRIBUCION',$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) RETURNING *`,
      [empresa_id||null,fecha,tipo_id,estanque_origen_id,equipo_id,faena_id||null,lts,cpp,costo_total,horometro||null,kilometraje||null,responsable||null,observaciones||null,req.user.email]);
    await client.query('COMMIT');
    res.status(201).json(r.rows[0]);
  }catch(e){await client.query('ROLLBACK');res.status(400).json({error:e.message});}
  finally{client.release();}
});

// Anular movimiento
app.patch('/api/comb/movimientos/:id/anular', auth, async(req,res)=>{
  const client=await pool.connect();
  try{
    await client.query('BEGIN');
    const m=await client.query('SELECT * FROM comb_movimientos WHERE mov_id=$1',[req.params.id]);
    if(!m.rows.length) throw new Error('Movimiento no encontrado');
    const mov=m.rows[0];
    if(mov.estado==='ANULADO') throw new Error('El movimiento ya está anulado');
    const motivo=req.body.motivo||'Sin motivo';
    // Revertir stock según tipo
    if(mov.tipo_mov==='INGRESO_STOCK'){
      await client.query('UPDATE comb_stock SET litros_disponibles=litros_disponibles-$1,ultima_actualizacion=NOW() WHERE estanque_id=$2 AND tipo_id=$3',[mov.litros,mov.estanque_destino_id,mov.tipo_id]);
    }else if(mov.tipo_mov==='DISTRIBUCION'){
      await client.query('UPDATE comb_stock SET litros_disponibles=litros_disponibles+$1,ultima_actualizacion=NOW() WHERE estanque_id=$2 AND tipo_id=$3',[mov.litros,mov.estanque_origen_id,mov.tipo_id]);
    }else if(mov.tipo_mov==='TRASPASO'){
      await client.query('UPDATE comb_stock SET litros_disponibles=litros_disponibles+$1,ultima_actualizacion=NOW() WHERE estanque_id=$2 AND tipo_id=$3',[mov.litros,mov.estanque_origen_id,mov.tipo_id]);
      await client.query('UPDATE comb_stock SET litros_disponibles=litros_disponibles-$1,ultima_actualizacion=NOW() WHERE estanque_id=$2 AND tipo_id=$3',[mov.litros,mov.estanque_destino_id,mov.tipo_id]);
    }
    await client.query(`UPDATE comb_movimientos SET estado='ANULADO',motivo_anulacion=$1,anulado_en=NOW(),anulado_por=$2 WHERE mov_id=$3`,[motivo,req.user.email,req.params.id]);
    await client.query('COMMIT');
    res.json({ok:true});
  }catch(e){await client.query('ROLLBACK');res.status(400).json({error:e.message});}
  finally{client.release();}
});

// Reporte kardex por estanque
app.get('/api/comb/kardex', auth, async(req,res)=>{
  try{
    const{estanque_id,tipo_id,desde,hasta}=req.query;
    let where=["m.estado='ACTIVO'"],vals=[];
    if(estanque_id){vals.push(estanque_id);where.push(`(m.estanque_origen_id=$${vals.length} OR m.estanque_destino_id=$${vals.length})`);}
    if(tipo_id){vals.push(tipo_id);where.push(`m.tipo_id=$${vals.length}`);}
    if(desde){vals.push(desde);where.push(`m.fecha>=$${vals.length}`);}
    if(hasta){vals.push(hasta);where.push(`m.fecha<=$${vals.length}`);}
    const r=await pool.query(`SELECT m.*,ct.nombre AS tipo_nombre,eo.nombre AS estanque_origen,ed.nombre AS estanque_destino,eq.nombre AS equipo_nombre,f.nombre AS faena_nombre,pr.nombre AS proveedor_nombre
      FROM comb_movimientos m
      LEFT JOIN comb_tipos ct ON m.tipo_id=ct.tipo_id
      LEFT JOIN comb_estanques eo ON m.estanque_origen_id=eo.estanque_id
      LEFT JOIN comb_estanques ed ON m.estanque_destino_id=ed.estanque_id
      LEFT JOIN equipos eq ON m.equipo_id=eq.equipo_id
      LEFT JOIN faenas f ON m.faena_id=f.faena_id
      LEFT JOIN proveedores pr ON m.proveedor_id=pr.proveedor_id
      WHERE ${where.join(' AND ')} ORDER BY m.fecha,m.mov_id`,vals);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});
// ════════════════════════════════════════════════════

// Endpoint de inicialización manual tablas combustibles
app.post('/api/setup/comb', auth, async(req,res)=>{
  const errors=[];
  async function run(sql){
    try{ await pool.query(sql); }
    catch(e){ errors.push(e.message.substring(0,120)); }
  }
  await run(`CREATE TABLE IF NOT EXISTS comb_tipos (tipo_id SERIAL PRIMARY KEY, nombre VARCHAR(50) NOT NULL UNIQUE, activo BOOLEAN DEFAULT true)`);
  await run(`CREATE TABLE IF NOT EXISTS comb_estanques (estanque_id SERIAL PRIMARY KEY, empresa_id INT NOT NULL REFERENCES empresas(empresa_id), codigo VARCHAR(20) NOT NULL UNIQUE, nombre VARCHAR(100) NOT NULL, tipo_estanque VARCHAR(30) NOT NULL DEFAULT 'FIJO', ubicacion VARCHAR(150), capacidad_max NUMERIC(10,2), tipo_combustible_id INT REFERENCES comb_tipos(tipo_id), observaciones TEXT, activo BOOLEAN DEFAULT true, creado_en TIMESTAMP DEFAULT NOW())`);
  await run(`CREATE TABLE IF NOT EXISTS comb_stock (estanque_id INT NOT NULL REFERENCES comb_estanques(estanque_id), tipo_id INT NOT NULL REFERENCES comb_tipos(tipo_id), litros_disponibles NUMERIC(12,3) DEFAULT 0, costo_promedio NUMERIC(14,4) DEFAULT 0, ultima_actualizacion TIMESTAMP DEFAULT NOW(), PRIMARY KEY(estanque_id,tipo_id))`);
  await run(`CREATE TABLE IF NOT EXISTS comb_movimientos (mov_id SERIAL PRIMARY KEY, tipo_mov VARCHAR(20) NOT NULL, empresa_id INT REFERENCES empresas(empresa_id), fecha DATE NOT NULL, tipo_id INT NOT NULL REFERENCES comb_tipos(tipo_id), estanque_origen_id INT REFERENCES comb_estanques(estanque_id), estanque_destino_id INT REFERENCES comb_estanques(estanque_id), equipo_id INT REFERENCES equipos(equipo_id), faena_id INT REFERENCES faenas(faena_id), litros NUMERIC(12,3) NOT NULL, precio_unitario NUMERIC(14,4) DEFAULT 0, costo_total NUMERIC(14,2) DEFAULT 0, horometro NUMERIC(10,1), kilometraje NUMERIC(10,1), responsable VARCHAR(100), numero_documento VARCHAR(30), estado VARCHAR(10) DEFAULT 'ACTIVO', motivo_anulacion TEXT, usuario VARCHAR(100), creado_en TIMESTAMP DEFAULT NOW(), anulado_en TIMESTAMP, anulado_por VARCHAR(100))`);
  for(const t of ['Diesel','Gasolina 93','Gasolina 95','Gasolina 97']){
    await run(`INSERT INTO comb_tipos(nombre) SELECT '${t}' WHERE NOT EXISTS(SELECT 1 FROM comb_tipos WHERE nombre='${t}')`);
  }
  if(errors.length) res.status(207).json({ok:false,errors});
  else res.json({ok:true,msg:'Tablas de combustibles creadas correctamente'});
});



// ══════════════════════════════════════════════════════
// PANEL DE COMBUSTIBLES — ESTADÍSTICAS POR PERÍODO
// ══════════════════════════════════════════════════════
app.get('/api/comb/panel-stats', auth, async(req,res)=>{
  try{
    const{desde,hasta,empresa_id}=req.query;
    const d=desde||new Date(new Date().getFullYear(),new Date().getMonth(),1).toISOString().split('T')[0];
    const h=hasta||new Date().toISOString().split('T')[0];
    let eWhere='',eVals=[d,h];
    if(empresa_id){eVals.push(empresa_id);eWhere=` AND m.empresa_id=$${eVals.length}`;}

    // Stock actual por estanque (no filtra por fecha — es estado actual)
    const stockQ=await pool.query(`
      SELECT cs.estanque_id,e.nombre AS estanque_nombre,e.tipo_estanque,e.codigo,
        emp.razon_social AS empresa_nombre,ct.nombre AS tipo_nombre,
        cs.litros_disponibles,cs.costo_promedio,
        ROUND(cs.litros_disponibles*cs.costo_promedio,0) AS valor_total
      FROM comb_stock cs
      JOIN comb_estanques e ON cs.estanque_id=e.estanque_id
      JOIN empresas emp ON e.empresa_id=emp.empresa_id
      JOIN comb_tipos ct ON cs.tipo_id=ct.tipo_id
      WHERE cs.litros_disponibles>0
      ORDER BY emp.razon_social,e.nombre`);

    // Compras del período (INGRESO_STOCK)
    const comprasQ=await pool.query(`
      SELECT emp.razon_social AS empresa,ct.nombre AS tipo,
        SUM(m.litros) AS litros,SUM(m.costo_total) AS monto
      FROM comb_movimientos m
      LEFT JOIN empresas emp ON m.empresa_id=emp.empresa_id
      LEFT JOIN comb_tipos ct ON m.tipo_id=ct.tipo_id
      WHERE m.tipo_mov='INGRESO_STOCK' AND m.estado='ACTIVO'
        AND m.fecha>=$1 AND m.fecha<=$2${empresa_id?` AND m.empresa_id=$${eVals.length}`:''}
      GROUP BY emp.razon_social,ct.nombre ORDER BY litros DESC`,eVals);

    // Distribuciones del período
    const distQ=await pool.query(`
      SELECT emp.razon_social AS empresa,ct.nombre AS tipo,
        f.nombre AS faena,eq.nombre AS equipo,
        SUM(m.litros) AS litros,SUM(m.costo_total) AS costo
      FROM comb_movimientos m
      LEFT JOIN empresas emp ON m.empresa_id=emp.empresa_id
      LEFT JOIN comb_tipos ct ON m.tipo_id=ct.tipo_id
      LEFT JOIN faenas f ON m.faena_id=f.faena_id
      LEFT JOIN equipos eq ON m.equipo_id=eq.equipo_id
      WHERE m.tipo_mov='DISTRIBUCION' AND m.estado='ACTIVO'
        AND m.fecha>=$1 AND m.fecha<=$2${empresa_id?` AND m.empresa_id=$${eVals.length}`:''}
      GROUP BY emp.razon_social,ct.nombre,f.nombre,eq.nombre ORDER BY litros DESC`,eVals);

    // Distribuciones por faena
    const faenaQ=await pool.query(`
      SELECT COALESCE(f.nombre,'Sin faena') AS faena,
        SUM(m.litros) AS litros,SUM(m.costo_total) AS costo
      FROM comb_movimientos m
      LEFT JOIN faenas f ON m.faena_id=f.faena_id
      WHERE m.tipo_mov='DISTRIBUCION' AND m.estado='ACTIVO'
        AND m.fecha>=$1 AND m.fecha<=$2${empresa_id?` AND m.empresa_id=$${eVals.length}`:''}
      GROUP BY f.nombre ORDER BY litros DESC`,eVals);

    // Compras por empresa
    const compEmpQ=await pool.query(`
      SELECT COALESCE(emp.razon_social,'Sin empresa') AS empresa,
        SUM(m.litros) AS litros,SUM(m.costo_total) AS monto
      FROM comb_movimientos m
      LEFT JOIN empresas emp ON m.empresa_id=emp.empresa_id
      WHERE m.tipo_mov='INGRESO_STOCK' AND m.estado='ACTIVO'
        AND m.fecha>=$1 AND m.fecha<=$2${empresa_id?` AND m.empresa_id=$${eVals.length}`:''}
      GROUP BY emp.razon_social ORDER BY litros DESC`,eVals);

    // Distribuciones por empresa
    const distEmpQ=await pool.query(`
      SELECT COALESCE(emp.razon_social,'Sin empresa') AS empresa,
        SUM(m.litros) AS litros,SUM(m.costo_total) AS costo
      FROM comb_movimientos m
      LEFT JOIN empresas emp ON m.empresa_id=emp.empresa_id
      WHERE m.tipo_mov='DISTRIBUCION' AND m.estado='ACTIVO'
        AND m.fecha>=$1 AND m.fecha<=$2${empresa_id?` AND m.empresa_id=$${eVals.length}`:''}
      GROUP BY emp.razon_social ORDER BY litros DESC`,eVals);

    res.json({
      periodo:{desde:d,hasta:h},
      stock:stockQ.rows,
      compras:comprasQ.rows,
      distribuciones:distQ.rows,
      porFaena:faenaQ.rows,
      comprasPorEmpresa:compEmpQ.rows,
      distPorEmpresa:distEmpQ.rows
    });
  }catch(e){res.status(500).json({error:e.message});}
});

// KARDEX POR ESTANQUE
app.get('/api/comb/kardex-est', auth, async(req,res)=>{
  try{
    const{estanque_id,tipo_id,desde,hasta}=req.query;
    if(!estanque_id) return res.status(400).json({error:'Debe indicar estanque_id'});
    const estId=parseInt(estanque_id);

    // 1. Calculate opening balance (all movements BEFORE 'desde')
    let saldoInicial=0;
    if(desde){
      const preVals=[estanque_id,desde];
      let preWhere=["m.estado='ACTIVO'","(m.estanque_origen_id=$1 OR m.estanque_destino_id=$1)","m.fecha<$2"];
      if(tipo_id){preVals.push(tipo_id);preWhere.push(`m.tipo_id=$${preVals.length}`);}
      const preMovs=await pool.query(`SELECT tipo_mov,estanque_origen_id,estanque_destino_id,litros FROM comb_movimientos m WHERE ${preWhere.join(' AND ')} ORDER BY m.fecha ASC,m.mov_id ASC`,preVals);
      preMovs.rows.forEach(function(m){
        if(m.tipo_mov==='INGRESO_STOCK'&&m.estanque_destino_id===estId) saldoInicial+=parseFloat(m.litros);
        else if(m.tipo_mov==='DISTRIBUCION'&&m.estanque_origen_id===estId) saldoInicial-=parseFloat(m.litros);
        else if(m.tipo_mov==='TRASPASO'){
          if(m.estanque_destino_id===estId) saldoInicial+=parseFloat(m.litros);
          else if(m.estanque_origen_id===estId) saldoInicial-=parseFloat(m.litros);
        }
      });
    }

    // 2. Get period movements
    let where=["m.estado='ACTIVO'","(m.estanque_origen_id=$1 OR m.estanque_destino_id=$1)"],vals=[estanque_id];
    if(desde){vals.push(desde);where.push(`m.fecha>=$${vals.length}`);}
    if(hasta){vals.push(hasta);where.push(`m.fecha<=$${vals.length}`);}
    if(tipo_id){vals.push(tipo_id);where.push(`m.tipo_id=$${vals.length}`);}
    const movs=await pool.query(`
      SELECT m.*,ct.nombre AS tipo_nombre,
        eo.nombre AS estanque_origen,ed.nombre AS estanque_destino,
        eq.nombre AS equipo_nombre,f.nombre AS faena_nombre,
        pr.nombre AS proveedor_nombre,emp.razon_social AS empresa_nombre
      FROM comb_movimientos m
      LEFT JOIN comb_tipos ct ON m.tipo_id=ct.tipo_id
      LEFT JOIN comb_estanques eo ON m.estanque_origen_id=eo.estanque_id
      LEFT JOIN comb_estanques ed ON m.estanque_destino_id=ed.estanque_id
      LEFT JOIN equipos eq ON m.equipo_id=eq.equipo_id
      LEFT JOIN faenas f ON m.faena_id=f.faena_id
      LEFT JOIN proveedores pr ON m.proveedor_id=pr.proveedor_id
      LEFT JOIN empresas emp ON m.empresa_id=emp.empresa_id
      WHERE ${where.join(' AND ')}
      ORDER BY m.fecha ASC,m.mov_id ASC`,vals);

    // 3. Build kardex with running balance starting from saldoInicial
    let saldo=parseFloat(saldoInicial.toFixed(3));
    const rows=movs.rows.map(function(m){
      let entrada=0,salida=0;
      if(m.tipo_mov==='INGRESO_STOCK'&&m.estanque_destino_id===estId) entrada=parseFloat(m.litros);
      else if(m.tipo_mov==='DISTRIBUCION'&&m.estanque_origen_id===estId) salida=parseFloat(m.litros);
      else if(m.tipo_mov==='TRASPASO'){
        if(m.estanque_destino_id===estId) entrada=parseFloat(m.litros);
        else if(m.estanque_origen_id===estId) salida=parseFloat(m.litros);
      }
      saldo=parseFloat((saldo+entrada-salida).toFixed(3));
      return Object.assign({},m,{entrada,salida,saldo_acumulado:saldo});
    });
    res.json({saldo_inicial:parseFloat(saldoInicial.toFixed(3)),rows});
  }catch(e){res.status(500).json({error:e.message});}
});


// Listar ingresos provisorios pendientes de cierre
app.get('/api/comb/provisorios', auth, async(req,res)=>{
  try{
    const{empresa_id}=req.query;
    let where=["m.tipo_mov='INGRESO_STOCK'","m.estado='ACTIVO'","m.es_provisorio=true","m.cierre_id IS NULL"];
    let vals=[];
    if(empresa_id){vals.push(empresa_id);where.push(`m.empresa_id=$${vals.length}`);}
    const r=await pool.query(`
      SELECT m.*,ct.nombre AS tipo_nombre,
        ed.nombre AS estanque_nombre,
        emp.razon_social AS empresa_nombre,
        pr.nombre AS proveedor_nombre
      FROM comb_movimientos m
      LEFT JOIN comb_tipos ct ON m.tipo_id=ct.tipo_id
      LEFT JOIN comb_estanques ed ON m.estanque_destino_id=ed.estanque_id
      LEFT JOIN empresas emp ON m.empresa_id=emp.empresa_id
      LEFT JOIN proveedores pr ON m.proveedor_id=pr.proveedor_id
      WHERE ${where.join(' AND ')}
      ORDER BY m.fecha DESC,m.mov_id DESC`,vals);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});

// CRUD cierres
app.get('/api/comb/cierres', auth, async(req,res)=>{
  try{
    const r=await pool.query(`
      SELECT c.*,emp.razon_social AS empresa_nombre,pr.nombre AS proveedor_nombre,
        COUNT(cg.id) AS num_guias
      FROM comb_cierres c
      LEFT JOIN empresas emp ON c.empresa_id=emp.empresa_id
      LEFT JOIN proveedores pr ON c.proveedor_id=pr.proveedor_id
      LEFT JOIN comb_cierre_guias cg ON c.cierre_id=cg.cierre_id
      GROUP BY c.cierre_id,emp.razon_social,pr.nombre
      ORDER BY c.creado_en DESC`);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});

app.post('/api/comb/cierres', auth, async(req,res)=>{
  try{
    const{empresa_id,proveedor_id,numero_factura,fecha_factura,litros_total,base_afecta,ie_total,iva,total_factura,observaciones,mov_ids}=req.body;
    if(!mov_ids||!mov_ids.length) return res.status(400).json({error:'Seleccione al menos un ingreso provisorio'});
    const precio_neto_litro=parseFloat(base_afecta)/parseFloat(litros_total);
    const r=await pool.query(`INSERT INTO comb_cierres(empresa_id,proveedor_id,numero_factura,fecha_factura,litros_total,base_afecta,ie_total,iva,total_factura,precio_neto_litro,observaciones,usuario)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING *`,
      [empresa_id||null,proveedor_id||null,numero_factura,fecha_factura,parseFloat(litros_total),parseFloat(base_afecta),parseFloat(ie_total),parseFloat(iva),parseFloat(total_factura),precio_neto_litro,observaciones||null,req.user.email]);
    const cierreId=r.rows[0].cierre_id;
    // Link guias
    for(const mid of mov_ids){
      const mv=await pool.query('SELECT litros,precio_unitario FROM comb_movimientos WHERE mov_id=$1',[mid]);
      if(mv.rows.length){
        const lts=parseFloat(mv.rows[0].litros),pprov=parseFloat(mv.rows[0].precio_unitario);
        const diff=(precio_neto_litro-pprov)*lts;
        await pool.query('INSERT INTO comb_cierre_guias(cierre_id,mov_id,litros,precio_provisorio,precio_real,diferencia_total) VALUES($1,$2,$3,$4,$5,$6)',
          [cierreId,mid,lts,pprov,precio_neto_litro,diff]);
      }
    }
    res.status(201).json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});

// PROCESAR CIERRE — ajusta CPP y recalcula distribuciones retroactivamente
app.post('/api/comb/cierres/:id/procesar', auth, async(req,res)=>{
  const client=await pool.connect();
  try{
    await client.query('BEGIN');
    // Load cierre
    const cQ=await client.query('SELECT * FROM comb_cierres WHERE cierre_id=$1',[req.params.id]);
    if(!cQ.rows.length) throw new Error('Cierre no encontrado');
    const cierre=cQ.rows[0];
    if(cierre.estado==='PROCESADO') throw new Error('Este cierre ya fue procesado');
    const precioReal=parseFloat(cierre.precio_neto_litro);

    // Load guias of this cierre
    const guiasQ=await client.query(`SELECT cg.*,m.estanque_destino_id,m.tipo_id,m.fecha,m.empresa_id
      FROM comb_cierre_guias cg JOIN comb_movimientos m ON cg.mov_id=m.mov_id
      WHERE cg.cierre_id=$1`,[req.params.id]);

    // Group by estanque
    const estanques={};
    guiasQ.rows.forEach(function(g){
      const eid=g.estanque_destino_id;
      if(!estanques[eid]) estanques[eid]={tipo_id:g.tipo_id,empresa_id:g.empresa_id,guias:[],fecha_min:g.fecha};
      estanques[eid].guias.push(g);
      if(g.fecha<estanques[eid].fecha_min) estanques[eid].fecha_min=g.fecha;
    });

    const ajustes=[];

    for(const [estId,est] of Object.entries(estanques)){
      // Update each provisional ingreso with real price
      for(const g of est.guias){
        const costoRealTotal=g.litros*precioReal;
        await client.query('UPDATE comb_movimientos SET precio_unitario=$1,costo_total=$2,es_provisorio=false,cierre_id=$3 WHERE mov_id=$4',
          [precioReal,costoRealTotal,req.params.id,g.mov_id]);
      }

      // Recalculate CPP for this estanque from scratch (all ACTIVE movements)
      const histQ=await client.query(`SELECT tipo_mov,estanque_origen_id,estanque_destino_id,litros,precio_unitario
        FROM comb_movimientos WHERE estado='ACTIVO' AND (estanque_origen_id=$1 OR estanque_destino_id=$1)
        ORDER BY fecha ASC,mov_id ASC`,[estId]);
      let saldo=0,cpp=0;
      histQ.rows.forEach(function(m){
        const eid2=parseInt(estId);
        if(m.tipo_mov==='INGRESO_STOCK'&&m.estanque_destino_id===eid2){
          const q=parseFloat(m.litros),pu=parseFloat(m.precio_unitario);
          const newQ=saldo+q;
          cpp=newQ>0?(saldo*cpp+q*pu)/newQ:pu;
          saldo=newQ;
        } else if(m.tipo_mov==='DISTRIBUCION'&&m.estanque_origen_id===eid2){
          saldo-=parseFloat(m.litros);
        } else if(m.tipo_mov==='TRASPASO'){
          if(m.estanque_destino_id===eid2) saldo+=parseFloat(m.litros);
          else if(m.estanque_origen_id===eid2) saldo-=parseFloat(m.litros);
        }
      });
      // Update comb_stock with recalculated CPP
      await client.query('UPDATE comb_stock SET costo_promedio=$1,ultima_actualizacion=NOW() WHERE estanque_id=$2',[cpp,estId]);

      // Retroactively adjust distributions from fecha_min onwards
      const distQ=await client.query(`SELECT mov_id,litros,costo_total,precio_unitario
        FROM comb_movimientos WHERE tipo_mov='DISTRIBUCION' AND estado='ACTIVO'
          AND estanque_origen_id=$1 AND fecha>=$2`,[estId,est.fecha_min]);

      let difTotal=0;
      for(const d of distQ.rows){
        const litros=parseFloat(d.litros);
        const costoAnterior=parseFloat(d.costo_total);
        const costoNuevo=litros*cpp;
        const dif=costoNuevo-costoAnterior;
        difTotal+=dif;
        if(Math.abs(dif)>1){
          await client.query('UPDATE comb_movimientos SET precio_unitario=$1,costo_total=$2 WHERE mov_id=$3',
            [cpp,costoNuevo,d.mov_id]);
        }
      }

      // Register AJUSTE_VALORIZACION movement
      if(Math.abs(difTotal)>1){
        await client.query(`INSERT INTO comb_movimientos(tipo_mov,empresa_id,fecha,tipo_id,estanque_origen_id,litros,precio_unitario,costo_total,oc_referencia,observaciones,usuario,es_provisorio)
          VALUES('AJUSTE_VALORIZACION',$1,NOW()::date,$2,$3,$4,$5,$6,$7,$8,$9,false)`,
          [est.empresa_id,est.tipo_id,parseInt(estId),0,precioReal-cpp,difTotal,
          'Cierre '+cierre.numero_factura,
          'Ajuste CPP por cierre quincenal. Precio prov: $'+est.guias[0].precio_provisorio+' → real: $'+precioReal.toFixed(2)+'/lt',
          req.user.email]);
        ajustes.push({estanque_id:estId,dif_total:difTotal,cpp_nuevo:cpp});
      }
    }

    // Mark cierre as processed
    await client.query('UPDATE comb_cierres SET estado=$1,procesado_en=NOW(),procesado_por=$2 WHERE cierre_id=$3',
      ['PROCESADO',req.user.email,req.params.id]);

    await client.query('COMMIT');
    res.json({ok:true,ajustes,precio_real:precioReal});
  }catch(e){await client.query('ROLLBACK');res.status(400).json({error:e.message});}
  finally{client.release();}
});


// OCR FACTURA — extracción local PDF/XML + parseo DTE chileno
app.post('/api/ocr/factura', auth, async(req,res)=>{
  try{
    const{base64,mediaType}=req.body;
    if(!base64||!mediaType) return res.status(400).json({error:'Falta base64 o mediaType'});
    const isPdf=mediaType==='application/pdf';
    const isXml=mediaType==='text/xml'||mediaType==='application/xml';
    let textoCompleto='';

    // XML: decode and parse directly
    if(isXml){
      const xmlStr=Buffer.from(base64,'base64').toString('latin1');
      const data=parsearDteXml(xmlStr);
      return res.json({ok:true,data,raw:xmlStr.substring(0,500)+'...'});
    }

    if(isPdf){
      const buf=Buffer.from(base64,'base64');
      if(pdfParse){
        const parsed=await pdfParse(buf);
        textoCompleto=parsed.text||'';
      }else{
        // Fallback sin pdf-parse: extraer texto legible del buffer PDF
        const raw=buf.toString('binary');
        const matches=raw.match(/\(([^)]{2,200})\)/g)||[];
        textoCompleto=matches.map(m=>m.slice(1,-1)).join(' ');
        if(!textoCompleto) textoCompleto=raw.replace(/[^\x20-\x7E\n]/g,' ').replace(/ {3,}/g,'\n');
      }
    }else{
      // Para imágenes necesitamos API externa — intentar con Gemini si está configurado
      const apiKey=process.env.GEMINI_API_KEY||'';
      if(!apiKey) return res.status(400).json({error:'Para imágenes se requiere GEMINI_API_KEY. Suba el documento como PDF.'});
      const geminiContent={parts:[{inline_data:{mime_type:mediaType,data:base64}},{text:'Extrae todo el texto de esta imagen de factura o guía de despacho, tal como aparece, sin formato especial.'}]};
      const geminiUrl='https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key='+apiKey;
      const r=await fetch(geminiUrl,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({contents:[geminiContent]})});
      const d=await r.json();
      textoCompleto=d.candidates?.[0]?.content?.parts?.[0]?.text||'';
      if(!textoCompleto) return res.status(502).json({error:'No se pudo extraer texto de la imagen'});
    }

    // Parse Chilean invoice text with regex
    const data=parsearFacturaChilena(textoCompleto);
    res.json({ok:true,data,raw:textoCompleto});
  }catch(e){
    res.status(500).json({error:e.message});
  }
});

function parsearFacturaChilena(txt){
  const lineas=txt.split('\n').map(l=>l.trim()).filter(Boolean);

  function findFirst(patterns){
    for(const p of patterns){const m=txt.match(p);if(m&&m[1])return m[1].trim();}
    return null;
  }

  function parseMonto(s){
    if(!s)return null;
    s=s.replace(/[$\s]/g,'').replace(/\./g,'');
    return parseFloat(s)||null;
  }

  function cleanRut(r){return r?r.replace(/\s/g,'').toUpperCase():null;}

  const tipoDoc=/FACTURA/i.test(txt)?'FACTURA':/GU[IÍ]A/i.test(txt)?'GUIA':/BOLETA/i.test(txt)?'BOLETA':'FACTURA';
  const ndoc=findFirst([/N\s*[°º]\s*0*(\d{4,})/,/Nº\s*0*(\d{4,})/,/NUMERO\s*:?\s*0*(\d{4,})/i]);

  let fecha=null;
  const fm=txt.match(/(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})/);
  if(fm)fecha=`${fm[3]}-${fm[2].padStart(2,'0')}-${fm[1].padStart(2,'0')}`;

  const rutMatches=[...txt.matchAll(/(\d{1,2}\.?\d{3}\.?\d{3}-[\dkK])/g)];
  const provRut=rutMatches[0]?cleanRut(rutMatches[0][1]):null;
  const clienteRut=rutMatches[1]?cleanRut(rutMatches[1][1]):null;

  let provNombre=null;
  const skipProv=/^(RUT|R\.U\.T|DIREC|Direcc|Calle|colon|GIRO|Giro|SII|S\.I\.I|SEGU|Serv|SERV|VENTAS|FERR|INDU|www\.|http)/i;
  for(const l of lineas){
    if(l.length>4&&!skipProv.test(l)&&/[A-ZÁÉÍÓÚ]{3}/.test(l)){provNombre=l;break;}
  }

  const clienteNombre=findFirst([/[Ss]e[ñn]or(?:es)?\s*:?\s*([A-ZÁÉÍÓÚÑ][^\n]{5,60})/,/NOMBRE\s*:\s*([^\n]{5,60})/]);
  const condPago=findFirst([/[Cc]ondici[oó]n(?:es)?\s*de\s*[Pp]ago\s*:?\s*([^\n]{2,30})/,/CTA\.?\s*CTE\.?\s*(\d+)/i]);

  const neto=parseMonto(findFirst([/[Mm]onto\s*[Nn]eto\s*\$?\s*([\d.]+)/,/NETO\s*\$?\s*([\d.]+)/]));
  const iva=parseMonto(findFirst([/IVA\s*19%?\s*\$?\s*([\d.]+)/,/I\.?V\.?A\.?\s*\$?\s*([\d.]+)/]));
  const total=parseMonto(findFirst([/TOTAL\s*\$?\s*([\d.]+)/,/Total\s*\$?\s*([\d.]+)/]));

  const skipLine=/^(Glosa|DETALLE|Descripci|PRODUCTO|Cantidad|CANTIDAD|Precio|PRECIO|Monto|MONTO|TOTAL|Total|IVA|Neto|NETO|RUT|Fecha|Venc|Cond|Giro|GIRO|Señor|Direcc|Ciudad|Comuna|S\.I\.I|Timbre|Sistema|www\.|Afecto|Imp\.|Desc|Item$|REFERENCIA|Orden de|^I\d+\s|^paulo|Exento|^\*\*)/i;

  // Strategy A: single line — description+qty$price SI $total
  function parseLineaUnica(l){
    const m=l.match(/\$([\d.]+)\s+[A-Z]{2}\s+\$([\d.]+)$/);
    if(!m)return null;
    const pu=parseMonto(m[1]);
    const tot=parseMonto(m[2]);
    if(!pu||!tot||pu<=0||tot<=0)return null;
    const prefix=l.slice(0,m.index).replace(/\s+$/,'');
    const trailMatch=prefix.match(/(\d+)$/);
    if(!trailMatch)return null;
    const trailing=trailMatch[1];
    for(let qLen=1;qLen<=Math.min(3,trailing.length);qLen++){
      const qty=parseInt(trailing.slice(-qLen));
      if(qty<=0)continue;
      const desc=prefix.slice(0,prefix.length-qLen).replace(/\s+$/,'');
      if(desc.length<2)continue;
      const ratio=tot/(pu*qty);
      if(ratio>=0.95&&ratio<=1.05)return{descripcion:desc,cantidad:qty,precio_unitario:pu,total_linea:tot};
    }
    return null;
  }

  // Strategy B: multi-line — qty$price on one line, description on previous line(s)
  // Line format: "4$30.716 SI $122.861" or "4 $30.716 SI $122.861"
  const rePrecioSolo=/^(\d{1,4})\s*\$?([\d.]+)\s+[A-Z]{2}\s+\$([\d.]+)$/;

  const detalleLineas=[];
  for(let i=0;i<lineas.length;i++){
    const l=lineas[i];
    if(l.length<8||l.length>200)continue;
    if(skipLine.test(l))continue;

    // Try single-line strategy first
    const item=parseLineaUnica(l);
    if(item){detalleLineas.push(item);continue;}

    // Try multi-line: is this a price-only line?
    const pm=l.match(rePrecioSolo);
    if(pm){
      const qty=parseInt(pm[1]);
      const pu=parseMonto(pm[2]);
      const tot=parseMonto(pm[3]);
      if(qty>0&&pu>0&&tot>0){
        const ratio=tot/(pu*qty);
        if(ratio>=0.95&&ratio<=1.05){
          // Find description: look back for last non-skip, non-price line
          let desc='';
          for(let j=i-1;j>=Math.max(0,i-3);j--){
            const prev=lineas[j];
            if(!skipLine.test(prev)&&!prev.match(rePrecioSolo)&&prev.length>=3&&!/^\d+$/.test(prev)){
              desc=(desc?prev+' '+desc:prev);
              break; // take just the immediately previous description line
            }
          }
          if(desc.length>=2)detalleLineas.push({descripcion:desc.trim(),cantidad:qty,precio_unitario:pu,total_linea:tot});
        }
      }
    }
  }

  // Fallback: price at end of line
  if(detalleLineas.length===0){
    for(const l of lineas){
      if(skipLine.test(l))continue;
      const m=l.match(/^(.{6,80})\s+\$?([\d.]{4,12})$/);
      if(m){
        const tot=parseMonto(m[2]);
        if(tot&&tot>=500&&!/^(Monto|Total|Neto|IVA|Exento)/i.test(m[1]))
          detalleLineas.push({descripcion:m[1].trim(),cantidad:1,precio_unitario:tot,total_linea:tot});
      }
    }
  }

  return{numero_documento:ndoc,fecha_emision:fecha,tipo_doc:tipoDoc,
    proveedor_rut:provRut,proveedor_nombre:provNombre,
    cliente_rut:clienteRut,cliente_nombre:clienteNombre?clienteNombre.trim():null,
    neto,iva,total,condiciones_pago:condPago,lineas:detalleLineas};
}

function parsearDteXml(xmlStr){
  function tag(name){
    // Extract first occurrence of <name>value</name>
    const m=xmlStr.match(new RegExp('<'+name+'[^>]*>([^<]*)<\/'+name+'>'));
    return m?m[1].trim():null;
  }
  function tagAll(name){
    // Extract all occurrences
    const re=new RegExp('<'+name+'[^>]*>([^<]*)<\/'+name+'>','g');
    return [...xmlStr.matchAll(re)].map(m=>m[1].trim());
  }
  function num(s){return s?parseFloat(s)||null:null;}

  // Tipo DTE: 33=Factura, 52=Guía, 39=Boleta
  const tipoDte=tag('TipoDTE');
  const tipoDoc=tipoDte==='33'||tipoDte==='34'?'FACTURA':tipoDte==='52'?'GUIA':tipoDte==='39'?'BOLETA':'FACTURA';

  // Encabezado
  const ndoc=tag('Folio');
  const fecha=tag('FchEmis');
  const condPago=tag('TermPagoGlosa')||tag('FmaPago');

  // Emisor (proveedor)
  const provRut=tag('RUTEmisor');
  const provNombre=tag('RznSoc');

  // Receptor (cliente)
  const clienteRut=tag('RUTRecep');
  const clienteNombre=tag('RznSocRecep');

  // Totales
  const neto=num(tag('MntNeto'));
  const iva=num(tag('IVA'));
  const total=num(tag('MntTotal'));

  // Líneas de detalle — extraer todos los bloques <Detalle>
  const lineas=[];
  const detalleBlocks=[...xmlStr.matchAll(/<Detalle>([\s\S]*?)<\/Detalle>/g)];
  for(const block of detalleBlocks){
    const b=block[1];
    function tagB(name){const m=b.match(new RegExp('<'+name+'[^>]*>([^<]*)<\/'+name+'>'));return m?m[1].trim():null;}
    const desc=tagB('NmbItem');
    const qty=parseFloat(tagB('QtyItem')||'1')||1;
    const prc=num(tagB('PrcItem'));
    const monto=num(tagB('MontoItem'));
    if(desc&&monto)lineas.push({descripcion:desc,cantidad:qty,precio_unitario:prc||monto,total_linea:monto});
  }

  return{numero_documento:ndoc,fecha_emision:fecha,tipo_doc:tipoDoc,
    proveedor_rut:provRut,proveedor_nombre:provNombre,
    cliente_rut:clienteRut,cliente_nombre:clienteNombre,
    neto,iva,total,condiciones_pago:condPago,lineas};
}



// ══════════════════════════════════════════════════════
// INTEGRACIÓN FACTO/KOYWE — DTE RECIBIDOS
// Base URL: https://api-billing.koywe.com
// Auth: POST /V1/authentication  {apiKey, secret}
// ══════════════════════════════════════════════════════

let factoToken=null, factoTokenExp=0;

async function getFactoToken(){
  if(factoToken&&Date.now()<factoTokenExp) return factoToken;
  const apiKey=process.env.FACTO_CLIENT_ID||'';     // Client Identification
  const secret=process.env.FACTO_CLIENT_SECRET||''; // Client Secret
  const user=process.env.FACTO_USER||apiKey;         // Resource Owner Name
  const pass=process.env.FACTO_PASS||secret;         // Resource Owner Password
  if(!apiKey||!secret) throw new Error('Credenciales Facto/Koywe no configuradas (FACTO_CLIENT_ID, FACTO_CLIENT_SECRET)');

  // The 400 "invalid_request" means endpoint found, body field names wrong
  // Try all credential combinations
  const BASE='https://api-billing.koywe.com/V1/authentication';
  // Try multiple URL + body combinations
  const combos=[
    ['https://api-billing.koywe.com/V1/authentication',{apiKey:user,secret:pass}],
    ['https://api-billing.koywe.com/V1/authentication',{apiKey,secret}],
    ['https://api-billing.koywe.com/V1/auth',{apiKey:user,secret:pass}],
    ['https://api-billing.koywe.com/V1/auth',{apiKey,secret}],
    ['https://api-billing.koywe.com/V1/auth/sign-in',{apiKey:user,secret:pass}],
    ['https://api-billing.koywe.com/V1/auth/sign-in',{apiKey,secret}],
    ['https://api-billing.koywe.com/V1/sign-in',{apiKey:user,secret:pass}],
    ['https://api-billing.koywe.com/V1/login',{apiKey:user,secret:pass}],
    ['https://api-billing.koywe.com/auth',{apiKey:user,secret:pass}],
    ['https://api-billing.koywe.com/authentication',{apiKey:user,secret:pass}],
  ];
  let d=null,lastStatus=0,lastText='';
  for(let i=0;i<combos.length;i++){
    const [url,body]=combos[i];
    const r2=await fetch(url,{method:'POST',headers:{'Content-Type':'application/json','Accept':'application/json'},body:JSON.stringify(body)});
    lastText=await r2.text();
    console.log('[Facto auth]',url,JSON.stringify(body),'→',r2.status,lastText.substring(0,120));
    try{d=JSON.parse(lastText);}catch(e){d={raw:lastText};}
    lastStatus=r2.status;
    if(r2.ok&&d&&d.token){
      factoToken=d.token;factoTokenExp=Date.now()+(23*3600*1000);
      console.log('[Facto] auth OK:',url);
      return factoToken;
    }
  }
  throw new Error(`Auth Koywe HTTP ${lastStatus}: ${(d&&(d.message||d.error||d.title))||lastText.substring(0,150)}`);
  factoToken=d.token;
  factoTokenExp=Date.now()+(23*3600*1000); // JWT dura 24h, renovar a las 23h
  return factoToken;
}

// Listar DTE recibidos (inbox)
app.get('/api/facto/dte-recibidos', auth, async(req,res)=>{
  try{
    if(!process.env.FACTO_CLIENT_ID) return res.status(400).json({error:'Credenciales Facto no configuradas'});
    const{desde,hasta,tipo}=req.query;
    const token=await getFactoToken();
    // Koywe Billing: inbox = documentos recibidos
    let url='https://api-billing.koywe.com/V1/inbox?';
    const params=['per_page=50','page=1'];
    if(desde) params.push('created_at_start='+desde);
    if(hasta) params.push('created_at_end='+hasta);
    url+=params.join('&');
    const r=await fetch(url,{headers:{'Authorization':'Bearer '+token,'Accept':'application/json'}});
    const text=await r.text();
    let d;try{d=JSON.parse(text);}catch(e){d={raw:text};}
    console.log('[Facto inbox] HTTP',r.status,'→',text.substring(0,300));
    if(!r.ok) return res.status(r.status).json({error:`HTTP ${r.status}: ${d.message||d.error||text.substring(0,100)}`,detail:d});
    res.json(d);
  }catch(e){res.status(500).json({error:e.message});}
});

// Descargar XML de un DTE recibido
app.get('/api/facto/dte/:id/xml', auth, async(req,res)=>{
  try{
    if(!process.env.FACTO_CLIENT_ID) return res.status(400).json({error:'Credenciales no configuradas'});
    const token=await getFactoToken();
    // Get the specific inbox document with its XML
    const url='https://api-billing.koywe.com/V1/inbox/'+req.params.id;
    const r=await fetch(url,{headers:{'Authorization':'Bearer '+token,'Accept':'application/json'}});
    const d=await r.json();
    if(!r.ok) return res.status(r.status).json({error:d.message||'Error descargando DTE'});
    // Try to extract XML from the document
    const xmlContent=d.xml||d.electronic_document||d.xml_content||null;
    if(xmlContent){
      const data=parsearDteXml(xmlContent);
      return res.json({ok:true,data,raw_xml:xmlContent});
    }
    // If no XML, build from document data
    const data={
      numero_documento:d.folio||d.document_number||d.number||null,
      fecha_emision:d.issue_date||d.created_at||null,
      tipo_doc:d.document_type_id==='33'?'FACTURA':d.document_type_id==='52'?'GUIA':'FACTURA',
      proveedor_rut:d.issuer?.tax_id||d.issuer_tax_id||null,
      proveedor_nombre:d.issuer?.name||d.issuer_name||null,
      cliente_rut:d.receiver?.tax_id||d.receiver_tax_id||null,
      cliente_nombre:d.receiver?.name||d.receiver_name||null,
      neto:d.net_amount||d.totals?.net||null,
      iva:d.tax_amount||d.totals?.tax||null,
      total:d.total_amount||d.totals?.total||null,
      condiciones_pago:null,
      lineas:(d.items||d.details||[]).map(function(item){
        return{descripcion:item.name||item.description||item.detail||'',
          cantidad:parseFloat(item.quantity||1),
          precio_unitario:parseFloat(item.unit_price||item.price||0),
          total_linea:parseFloat(item.total||item.amount||0)};
      })
    };
    res.json({ok:true,data,raw:d});
  }catch(e){res.status(500).json({error:e.message});}
});

// Test conexión
app.get('/api/facto/test', auth, async(req,res)=>{
  try{
    if(!process.env.FACTO_CLIENT_ID) return res.json({ok:false,msg:'Variables FACTO_CLIENT_ID y FACTO_CLIENT_SECRET no configuradas'});
    const token=await getFactoToken();
    res.json({ok:true,msg:'Conexión Koywe Billing exitosa',token_preview:token.substring(0,20)+'...'});
  }catch(e){res.json({ok:false,msg:e.message});}
});


// ══════════════════════════════════════════════════════
// MÓDULO MANTENCIÓN — ENDPOINTS
// ══════════════════════════════════════════════════════

// ── PLANES MAESTROS ──
app.get('/api/mant/planes', auth, async(req,res)=>{
  try{
    const{empresa_id,tipo_activo,activo}=req.query;
    let where=['1=1'],vals=[];
    if(empresa_id){vals.push(empresa_id);where.push(`p.empresa_id=$${vals.length}`);}
    if(tipo_activo&&tipo_activo!=='todos'){vals.push(tipo_activo);where.push(`(p.tipo_activo=$${vals.length} OR p.tipo_activo='todos')`);}
    if(activo!==undefined){vals.push(activo==='true');where.push(`p.activo=$${vals.length}`);}
    const r=await pool.query(`SELECT p.*,e.razon_social AS empresa_nombre FROM mant_planes p LEFT JOIN empresas e ON p.empresa_id=e.empresa_id WHERE ${where.join(' AND ')} ORDER BY p.nombre`,vals);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});
app.post('/api/mant/planes', auth, async(req,res)=>{
  try{
    const{empresa_id,nombre,descripcion,tipo_activo,familia,marca,modelo_filtro,equipo_id,sistema,componente,tipo_mantencion,intervalo_horas,intervalo_km,intervalo_dias,tolerancia_horas,tolerancia_km,tolerancia_dias,tiempo_estimado_hrs,prioridad,checklist_items,repuestos_sugeridos,lubricantes_sugeridos}=req.body;
    if(!nombre) return res.status(400).json({error:'Nombre requerido'});
    const r=await pool.query(`INSERT INTO mant_planes(empresa_id,nombre,descripcion,tipo_activo,familia,marca,modelo_filtro,equipo_id,sistema,componente,tipo_mantencion,intervalo_horas,intervalo_km,intervalo_dias,tolerancia_horas,tolerancia_km,tolerancia_dias,tiempo_estimado_hrs,prioridad,checklist_items,repuestos_sugeridos,lubricantes_sugeridos) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22) RETURNING *`,
      [empresa_id||null,nombre,descripcion||null,tipo_activo||'todos',familia||null,marca||null,modelo_filtro||null,equipo_id||null,sistema||null,componente||null,tipo_mantencion||'preventivo',intervalo_horas||null,intervalo_km||null,intervalo_dias||null,tolerancia_horas||10,tolerancia_km||200,tolerancia_dias||5,tiempo_estimado_hrs||null,prioridad||'normal',JSON.stringify(checklist_items||[]),JSON.stringify(repuestos_sugeridos||[]),JSON.stringify(lubricantes_sugeridos||[])]);
    res.status(201).json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
app.put('/api/mant/planes/:id', auth, async(req,res)=>{
  try{
    const{nombre,descripcion,tipo_activo,familia,marca,modelo_filtro,equipo_id,sistema,componente,tipo_mantencion,intervalo_horas,intervalo_km,intervalo_dias,tolerancia_horas,tolerancia_km,tolerancia_dias,tiempo_estimado_hrs,prioridad,checklist_items,repuestos_sugeridos,lubricantes_sugeridos,activo}=req.body;
    const r=await pool.query(`UPDATE mant_planes SET nombre=$1,descripcion=$2,tipo_activo=$3,familia=$4,marca=$5,modelo_filtro=$6,equipo_id=$7,sistema=$8,componente=$9,tipo_mantencion=$10,intervalo_horas=$11,intervalo_km=$12,intervalo_dias=$13,tolerancia_horas=$14,tolerancia_km=$15,tolerancia_dias=$16,tiempo_estimado_hrs=$17,prioridad=$18,checklist_items=$19,repuestos_sugeridos=$20,lubricantes_sugeridos=$21,activo=$22 WHERE plan_id=$23 RETURNING *`,
      [nombre,descripcion||null,tipo_activo||'todos',familia||null,marca||null,modelo_filtro||null,equipo_id||null,sistema||null,componente||null,tipo_mantencion||'preventivo',intervalo_horas||null,intervalo_km||null,intervalo_dias||null,tolerancia_horas||10,tolerancia_km||200,tolerancia_dias||5,tiempo_estimado_hrs||null,prioridad||'normal',JSON.stringify(checklist_items||[]),JSON.stringify(repuestos_sugeridos||[]),JSON.stringify(lubricantes_sugeridos||[]),activo!==false,req.params.id]);
    res.json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});

// ── AVISOS DE FALLA ──
app.get('/api/mant/avisos', auth, async(req,res)=>{
  try{
    const{empresa_id,equipo_id,estado}=req.query;
    let where=['1=1'],vals=[];
    if(empresa_id){vals.push(empresa_id);where.push(`a.empresa_id=$${vals.length}`);}
    if(equipo_id){vals.push(equipo_id);where.push(`a.equipo_id=$${vals.length}`);}
    if(estado){vals.push(estado);where.push(`a.estado=$${vals.length}`);}
    const r=await pool.query(`SELECT a.*,eq.nombre AS equipo_nombre,f.nombre AS faena_nombre,emp.razon_social AS empresa_nombre FROM mant_avisos a LEFT JOIN equipos eq ON a.equipo_id=eq.equipo_id LEFT JOIN faenas f ON a.faena_id=f.faena_id LEFT JOIN empresas emp ON a.empresa_id=emp.empresa_id WHERE ${where.join(' AND ')} ORDER BY a.creado_en DESC`,vals);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});
app.post('/api/mant/avisos', auth, async(req,res)=>{
  try{
    const{empresa_id,equipo_id,faena_id,fecha,reportado_por,criticidad,equipo_detenido,sistema,sintoma,observaciones}=req.body;
    if(!equipo_id||!sintoma) return res.status(400).json({error:'Equipo y síntoma requeridos'});
    // Si equipo detenido, actualizar estado_operativo
    if(equipo_detenido) await pool.query("UPDATE equipos SET estado_operativo='detenido' WHERE equipo_id=$1",[equipo_id]);
    const r=await pool.query(`INSERT INTO mant_avisos(empresa_id,equipo_id,faena_id,fecha,reportado_por,criticidad,equipo_detenido,sistema,sintoma,observaciones,usuario) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING *`,
      [empresa_id||null,equipo_id,faena_id||null,fecha||new Date().toISOString().split('T')[0],reportado_por||null,criticidad||'media',equipo_detenido||false,sistema||null,sintoma,observaciones||null,req.user.email]);
    res.status(201).json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
app.patch('/api/mant/avisos/:id', auth, async(req,res)=>{
  try{
    const{estado,ot_id}=req.body;
    const r=await pool.query('UPDATE mant_avisos SET estado=$1,ot_id=$2 WHERE aviso_id=$3 RETURNING *',[estado,ot_id||null,req.params.id]);
    res.json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});

// ── ÓRDENES DE TRABAJO ──
app.get('/api/mant/ot', auth, async(req,res)=>{
  try{
    const{empresa_id,equipo_id,estado,desde,hasta}=req.query;
    let where=['1=1'],vals=[];
    if(empresa_id){vals.push(empresa_id);where.push(`o.empresa_id=$${vals.length}`);}
    if(equipo_id){vals.push(equipo_id);where.push(`o.equipo_id=$${vals.length}`);}
    if(estado){vals.push(estado);where.push(`o.estado=$${vals.length}`);}
    if(desde){vals.push(desde);where.push(`o.fecha_apertura>=$${vals.length}`);}
    if(hasta){vals.push(hasta);where.push(`o.fecha_apertura<=$${vals.length}`);}
    const r=await pool.query(`SELECT o.*,eq.nombre AS equipo_nombre,eq.tipo_activo,eq.familia,f.nombre AS faena_nombre,emp.razon_social AS empresa_nombre FROM mant_ot o LEFT JOIN equipos eq ON o.equipo_id=eq.equipo_id LEFT JOIN faenas f ON o.faena_id=f.faena_id LEFT JOIN empresas emp ON o.empresa_id=emp.empresa_id WHERE ${where.join(' AND ')} ORDER BY o.creado_en DESC`,vals);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});
app.post('/api/mant/ot', auth, async(req,res)=>{
  try{
    const{empresa_id,equipo_id,faena_id,plan_id,aviso_id,tipo_mantencion,origen,fecha_apertura,fecha_programada,horometro_servicio,kilometraje_servicio,estado,prioridad,sistema,sintoma_reportado,responsable,mecanico_asignado,taller_tipo,taller_nombre,observaciones}=req.body;
    if(!equipo_id||!tipo_mantencion) return res.status(400).json({error:'Equipo y tipo de mantención requeridos'});
    // Generate OT number
    const yr=new Date().getFullYear();
    const cnt=await pool.query("SELECT COUNT(*)+1 AS n FROM mant_ot WHERE EXTRACT(YEAR FROM creado_en)=$1",[yr]);
    const num=`OT-${yr}-${String(cnt.rows[0].n).padStart(4,'0')}`;
    const r=await pool.query(`INSERT INTO mant_ot(numero_ot,empresa_id,equipo_id,faena_id,plan_id,aviso_id,tipo_mantencion,origen,fecha_apertura,fecha_programada,horometro_servicio,kilometraje_servicio,estado,prioridad,sistema,sintoma_reportado,responsable,mecanico_asignado,taller_tipo,taller_nombre,observaciones,usuario) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22) RETURNING *`,
      [num,empresa_id||null,equipo_id,faena_id||null,plan_id||null,aviso_id||null,tipo_mantencion,origen||'manual',fecha_apertura||new Date().toISOString().split('T')[0],fecha_programada||null,horometro_servicio||null,kilometraje_servicio||null,estado||'abierta',prioridad||'normal',sistema||null,sintoma_reportado||null,responsable||null,mecanico_asignado||null,taller_tipo||'interno',taller_nombre||null,observaciones||null,req.user.email]);
    const ot=r.rows[0];
    // Si viene de aviso, actualizar aviso
    if(aviso_id) await pool.query("UPDATE mant_avisos SET estado='generado_ot',ot_id=$1 WHERE aviso_id=$2",[ot.ot_id,aviso_id]);
    // Si viene de plan, cargar checklist
    if(plan_id){
      const plan=await pool.query('SELECT checklist_items FROM mant_planes WHERE plan_id=$1',[plan_id]);
      if(plan.rows.length&&plan.rows[0].checklist_items){
        const items=plan.rows[0].checklist_items;
        for(let i=0;i<items.length;i++){
          await pool.query('INSERT INTO mant_ot_tareas(ot_id,orden,descripcion,desde_plan) VALUES($1,$2,$3,true)',[ot.ot_id,i+1,items[i].descripcion||items[i]]);
        }
      }
    }
    res.status(201).json(ot);
  }catch(e){res.status(400).json({error:e.message});}
});
app.put('/api/mant/ot/:id', auth, async(req,res)=>{
  try{
    const{estado,fecha_inicio,fecha_termino,horometro_servicio,kilometraje_servicio,diagnostico,causa,trabajo_realizado,observaciones,responsable,mecanico_asignado,taller_tipo,taller_nombre,tiempo_detenido_hrs,costo_mano_obra_interna,costo_mano_obra_externa,costo_servicios,costo_traslado,costo_otros,prioridad,sistema}=req.body;
    // Recalculate total costs
    const matQ=await pool.query(`SELECT COALESCE(SUM(CASE WHEN tipo IN ('repuesto','filtro') THEN costo_total ELSE 0 END),0) AS rep, COALESCE(SUM(CASE WHEN tipo IN ('lubricante','grasa','refrigerante') THEN costo_total ELSE 0 END),0) AS lub FROM mant_ot_materiales WHERE ot_id=$1`,[req.params.id]);
    const costoRep=parseFloat(matQ.rows[0].rep)||0;
    const costoLub=parseFloat(matQ.rows[0].lub)||0;
    const moInt=parseFloat(costo_mano_obra_interna)||0;
    const moExt=parseFloat(costo_mano_obra_externa)||0;
    const srv2=parseFloat(costo_servicios)||0;
    const tras=parseFloat(costo_traslado)||0;
    const otros=parseFloat(costo_otros)||0;
    const total=costoRep+costoLub+moInt+moExt+srv2+tras+otros;
    const r=await pool.query(`UPDATE mant_ot SET estado=$1,fecha_inicio=$2,fecha_termino=$3,horometro_servicio=$4,kilometraje_servicio=$5,diagnostico=$6,causa=$7,trabajo_realizado=$8,observaciones=$9,responsable=$10,mecanico_asignado=$11,taller_tipo=$12,taller_nombre=$13,tiempo_detenido_hrs=$14,costo_repuestos=$15,costo_lubricantes=$16,costo_mano_obra_interna=$17,costo_mano_obra_externa=$18,costo_servicios=$19,costo_traslado=$20,costo_otros=$21,costo_total=$22,prioridad=$23,sistema=$24,actualizado_en=NOW() WHERE ot_id=$25 RETURNING *`,
      [estado,fecha_inicio||null,fecha_termino||null,horometro_servicio||null,kilometraje_servicio||null,diagnostico||null,causa||null,trabajo_realizado||null,observaciones||null,responsable||null,mecanico_asignado||null,taller_tipo||'interno',taller_nombre||null,tiempo_detenido_hrs||0,costoRep,costoLub,moInt,moExt,srv2,tras,otros,total,prioridad||'normal',sistema||null,req.params.id]);
    const ot=r.rows[0];
    // Si se cierra: actualizar horómetro/km del equipo + recalcular programación
    if(estado==='cerrada'){
      if(horometro_servicio) await pool.query('UPDATE equipos SET horometro_actual=$1 WHERE equipo_id=$2',[horometro_servicio,ot.equipo_id]);
      if(kilometraje_servicio) await pool.query('UPDATE equipos SET kilometraje_actual=$1 WHERE equipo_id=$2',[kilometraje_servicio,ot.equipo_id]);
      await pool.query("UPDATE equipos SET estado_operativo='operativo' WHERE equipo_id=$1 AND estado_operativo='detenido'",[ot.equipo_id]);
      // Registrar lectura
      await pool.query('INSERT INTO mant_lecturas(equipo_id,fecha,horometro,kilometraje,origen,ot_id,usuario) VALUES($1,$2,$3,$4,$5,$6,$7)',[ot.equipo_id,new Date().toISOString().split('T')[0],horometro_servicio||null,kilometraje_servicio||null,'ot',ot.ot_id,req.user.email]);
      // Recalcular programación si viene de plan
      if(ot.plan_id){
        const plan=await pool.query('SELECT * FROM mant_planes WHERE plan_id=$1',[ot.plan_id]);
        if(plan.rows.length){
          const p=plan.rows[0];
          const proxFecha=p.intervalo_dias?new Date(Date.now()+p.intervalo_dias*86400000).toISOString().split('T')[0]:null;
          const proxHoras=p.intervalo_horas&&horometro_servicio?parseFloat(horometro_servicio)+parseFloat(p.intervalo_horas):null;
          const proxKm=p.intervalo_km&&kilometraje_servicio?parseInt(kilometraje_servicio)+parseInt(p.intervalo_km):null;
          await pool.query(`INSERT INTO mant_programacion(equipo_id,plan_id,empresa_id,proxima_fecha,proxima_horas,proxima_km,ultima_ejecucion_fecha,ultima_ejecucion_horas,ultima_ejecucion_km,ultima_ot_id,estado) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'vigente') ON CONFLICT(equipo_id,plan_id) DO UPDATE SET proxima_fecha=$4,proxima_horas=$5,proxima_km=$6,ultima_ejecucion_fecha=$7,ultima_ejecucion_horas=$8,ultima_ejecucion_km=$9,ultima_ot_id=$10,estado='vigente',actualizado_en=NOW()`,
            [ot.equipo_id,ot.plan_id,ot.empresa_id,proxFecha,proxHoras,proxKm,new Date().toISOString().split('T')[0],horometro_servicio||null,kilometraje_servicio||null,ot.ot_id]);
        }
      }
    }
    res.json(ot);
  }catch(e){res.status(400).json({error:e.message});}
});

// OT Tareas
app.get('/api/mant/ot/:id/tareas', auth, async(req,res)=>{
  try{const r=await pool.query('SELECT * FROM mant_ot_tareas WHERE ot_id=$1 ORDER BY orden,tarea_id',[req.params.id]);res.json(r.rows);}catch(e){res.status(500).json({error:e.message});}
});
app.post('/api/mant/ot/:id/tareas', auth, async(req,res)=>{
  try{
    const{descripcion,sistema,tipo,desde_plan}=req.body;
    const cnt=await pool.query('SELECT COUNT(*)+1 AS n FROM mant_ot_tareas WHERE ot_id=$1',[req.params.id]);
    const r=await pool.query('INSERT INTO mant_ot_tareas(ot_id,orden,descripcion,sistema,tipo,desde_plan) VALUES($1,$2,$3,$4,$5,$6) RETURNING *',[req.params.id,cnt.rows[0].n,descripcion,sistema||null,tipo||'tarea',desde_plan||false]);
    res.status(201).json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
app.patch('/api/mant/ot/tareas/:id', auth, async(req,res)=>{
  try{
    const{estado,observacion}=req.body;
    const r=await pool.query('UPDATE mant_ot_tareas SET estado=$1,observacion=$2 WHERE tarea_id=$3 RETURNING *',[estado,observacion||null,req.params.id]);
    res.json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});

// OT Materiales
app.get('/api/mant/ot/:id/materiales', auth, async(req,res)=>{
  try{const r=await pool.query('SELECT m.*,p.nombre AS prod_nombre FROM mant_ot_materiales m LEFT JOIN productos p ON m.prod_id=p.prod_id WHERE m.ot_id=$1 ORDER BY m.creado_en',[req.params.id]);res.json(r.rows);}catch(e){res.status(500).json({error:e.message});}
});
app.post('/api/mant/ot/:id/materiales', auth, async(req,res)=>{
  try{
    const{tipo,prod_id,descripcion,cantidad,unidad,precio_unitario,origen}=req.body;
    const pu=parseFloat(precio_unitario)||0;
    const ct=(parseFloat(cantidad)||0)*pu;
    const r=await pool.query('INSERT INTO mant_ot_materiales(ot_id,tipo,prod_id,descripcion,cantidad,unidad,precio_unitario,costo_total,origen) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING *',
      [req.params.id,tipo||'repuesto',prod_id||null,descripcion,parseFloat(cantidad)||0,unidad||null,pu,ct,origen||'inventario']);
    res.status(201).json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
app.delete('/api/mant/ot/materiales/:id', auth, async(req,res)=>{
  try{await pool.query('DELETE FROM mant_ot_materiales WHERE material_id=$1',[req.params.id]);res.json({ok:true});}catch(e){res.status(400).json({error:e.message});}
});

// ── LECTURAS (horómetro/km) ──
app.post('/api/mant/lecturas', auth, async(req,res)=>{
  try{
    const{equipo_id,fecha,horometro,kilometraje}=req.body;
    if(!equipo_id) return res.status(400).json({error:'Equipo requerido'});
    const r=await pool.query('INSERT INTO mant_lecturas(equipo_id,fecha,horometro,kilometraje,origen,usuario) VALUES($1,$2,$3,$4,$5,$6) RETURNING *',
      [equipo_id,fecha||new Date().toISOString().split('T')[0],horometro||null,kilometraje||null,'manual',req.user.email]);
    // Update equipo
    if(horometro) await pool.query('UPDATE equipos SET horometro_actual=$1 WHERE equipo_id=$2',[horometro,equipo_id]);
    if(kilometraje) await pool.query('UPDATE equipos SET kilometraje_actual=$1 WHERE equipo_id=$2',[kilometraje,equipo_id]);
    res.status(201).json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});

// ── PROGRAMACIÓN / ALERTAS ──
app.get('/api/mant/programacion', auth, async(req,res)=>{
  try{
    const{empresa_id,equipo_id,estado}=req.query;
    let where=['1=1'],vals=[];
    if(empresa_id){vals.push(empresa_id);where.push(`e.empresa_id=$${vals.length}`);}
    if(equipo_id){vals.push(equipo_id);where.push(`p.equipo_id=$${vals.length}`);}
    if(estado){vals.push(estado);where.push(`p.estado=$${vals.length}`);}
    // Update estados based on current readings
    await pool.query(`UPDATE mant_programacion p SET estado=CASE
      WHEN (p.proxima_horas IS NOT NULL AND eq.horometro_actual>=p.proxima_horas) THEN 'vencida'
      WHEN (p.proxima_km IS NOT NULL AND eq.kilometraje_actual>=p.proxima_km) THEN 'vencida'
      WHEN (p.proxima_fecha IS NOT NULL AND p.proxima_fecha<=CURRENT_DATE) THEN 'vencida'
      WHEN (p.proxima_horas IS NOT NULL AND eq.horometro_actual>=p.proxima_horas-50) THEN 'proxima'
      WHEN (p.proxima_km IS NOT NULL AND eq.kilometraje_actual>=p.proxima_km-500) THEN 'proxima'
      WHEN (p.proxima_fecha IS NOT NULL AND p.proxima_fecha<=CURRENT_DATE+15) THEN 'proxima'
      ELSE 'vigente' END
      FROM equipos eq WHERE p.equipo_id=eq.equipo_id AND p.estado!='suspendida'`);
    const r=await pool.query(`SELECT p.*,pl.nombre AS plan_nombre,pl.sistema,pl.tipo_mantencion,pl.intervalo_horas,pl.intervalo_km,pl.intervalo_dias,pl.prioridad AS plan_prioridad,eq.nombre AS equipo_nombre,eq.horometro_actual,eq.kilometraje_actual,eq.tipo_activo,f.nombre AS faena_nombre,emp.razon_social AS empresa_nombre FROM mant_programacion p JOIN mant_planes pl ON p.plan_id=pl.plan_id JOIN equipos eq ON p.equipo_id=eq.equipo_id LEFT JOIN faenas f ON eq.faena_id=f.faena_id LEFT JOIN empresas emp ON p.empresa_id=emp.empresa_id WHERE ${where.join(' AND ')} ORDER BY CASE p.estado WHEN 'vencida' THEN 1 WHEN 'proxima' THEN 2 ELSE 3 END`,vals);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});

// ── PANEL RESUMEN MANTENCIÓN ──
app.get('/api/mant/panel', auth, async(req,res)=>{
  try{
    const{empresa_id}=req.query;
    let emp='';let vals=[];
    if(empresa_id){vals.push(empresa_id);emp=` AND o.empresa_id=$${vals.length}`;}
    const [otEstados,alertas,costoMes]=await Promise.all([
      pool.query(`SELECT estado,COUNT(*) AS n FROM mant_ot o WHERE 1=1${emp} GROUP BY estado`,vals),
      pool.query(`SELECT COUNT(*) FILTER(WHERE estado='vencida') AS vencidas,COUNT(*) FILTER(WHERE estado='proxima') AS proximas FROM mant_programacion p ${empresa_id?'JOIN equipos eq ON p.equipo_id=eq.equipo_id WHERE eq.empresa_id=$1':'WHERE 1=1'}`,empresa_id?[empresa_id]:[]),
      pool.query(`SELECT COALESCE(SUM(costo_total),0) AS total FROM mant_ot o WHERE estado='cerrada' AND fecha_termino>=date_trunc('month',CURRENT_DATE)${emp}`,vals)
    ]);
    res.json({ot_por_estado:otEstados.rows,alertas:alertas.rows[0],costo_mes:costoMes.rows[0].total});
  }catch(e){res.status(500).json({error:e.message});}
});

// ── HISTORIAL POR ACTIVO ──
app.get('/api/mant/historial/:equipo_id', auth, async(req,res)=>{
  try{
    const{desde,hasta}=req.query;
    let where=[`o.equipo_id=$1`],vals=[req.params.equipo_id];
    if(desde){vals.push(desde);where.push(`o.fecha_apertura>=$${vals.length}`);}
    if(hasta){vals.push(hasta);where.push(`o.fecha_apertura<=$${vals.length}`);}
    const[ots,materiales,lecturas]=await Promise.all([
      pool.query(`SELECT o.*,f.nombre AS faena_nombre FROM mant_ot o LEFT JOIN faenas f ON o.faena_id=f.faena_id WHERE ${where.join(' AND ')} ORDER BY o.fecha_apertura DESC`,vals),
      pool.query(`SELECT m.*,o.numero_ot,o.fecha_apertura FROM mant_ot_materiales m JOIN mant_ot o ON m.ot_id=o.ot_id WHERE o.equipo_id=$1 ORDER BY o.fecha_apertura DESC`,[req.params.equipo_id]),
      pool.query(`SELECT * FROM mant_lecturas WHERE equipo_id=$1 ORDER BY fecha DESC LIMIT 20`,[req.params.equipo_id])
    ]);
    const eq=await pool.query('SELECT * FROM equipos WHERE equipo_id=$1',[req.params.equipo_id]);
    res.json({equipo:eq.rows[0],ots:ots.rows,materiales:materiales.rows,lecturas:lecturas.rows});
  }catch(e){res.status(500).json({error:e.message});}
});

// ── REPORTES ──
app.get('/api/mant/reporte/costos', auth, async(req,res)=>{
  try{
    const{empresa_id,desde,hasta,agrupacion}=req.query;
    let where=["o.estado='cerrada'"],vals=[];
    if(empresa_id){vals.push(empresa_id);where.push(`o.empresa_id=$${vals.length}`);}
    if(desde){vals.push(desde);where.push(`o.fecha_apertura>=$${vals.length}`);}
    if(hasta){vals.push(hasta);where.push(`o.fecha_apertura<=$${vals.length}`);}
    let groupBy='eq.nombre';
    if(agrupacion==='faena') groupBy='f.nombre';
    else if(agrupacion==='tipo') groupBy='o.tipo_mantencion';
    else if(agrupacion==='empresa') groupBy='emp.razon_social';
    const r=await pool.query(`SELECT ${groupBy} AS grupo,COUNT(*) AS ots,SUM(o.costo_total) AS costo_total,SUM(o.costo_repuestos) AS repuestos,SUM(o.costo_lubricantes) AS lubricantes,SUM(o.costo_mano_obra_interna+o.costo_mano_obra_externa) AS mano_obra,SUM(o.tiempo_detenido_hrs) AS hrs_detencion FROM mant_ot o LEFT JOIN equipos eq ON o.equipo_id=eq.equipo_id LEFT JOIN faenas f ON o.faena_id=f.faena_id LEFT JOIN empresas emp ON o.empresa_id=emp.empresa_id WHERE ${where.join(' AND ')} GROUP BY ${groupBy} ORDER BY costo_total DESC`,vals);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});


// ══════════════════════════════════════════════════════
// PERSONAL — CRUD
// ══════════════════════════════════════════════════════
app.get('/api/personal', auth, async(req,res)=>{
  try{
    const{empresa_id,activo,mantencion}=req.query;
    let where=['1=1'],vals=[];
    if(empresa_id){vals.push(empresa_id);where.push(`p.empresa_id=$${vals.length}`);}
    if(activo!==undefined){vals.push(activo==='true');where.push(`p.activo=$${vals.length}`);}
    if(mantencion==='true'){where.push('p.participa_mantencion=true');}
    const r=await pool.query(`SELECT p.*,e.razon_social AS empresa_nombre FROM personal p LEFT JOIN empresas e ON p.empresa_id=e.empresa_id WHERE ${where.join(' AND ')} ORDER BY p.nombre_completo`,vals);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});
app.post('/api/personal', auth, async(req,res)=>{
  try{
    const{empresa_id,nombre_completo,rut,cargo,especialidad,telefono,correo,participa_mantencion,valor_hora_hombre,moneda,observaciones}=req.body;
    if(!nombre_completo) return res.status(400).json({error:'Nombre requerido'});
    const r=await pool.query(`INSERT INTO personal(empresa_id,nombre_completo,rut,cargo,especialidad,telefono,correo,participa_mantencion,valor_hora_hombre,moneda,observaciones) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING *`,
      [empresa_id||null,nombre_completo,rut||null,cargo||null,especialidad||null,telefono||null,correo||null,participa_mantencion||false,valor_hora_hombre||null,moneda||'CLP',observaciones||null]);
    res.status(201).json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
app.put('/api/personal/:id', auth, async(req,res)=>{
  try{
    const{empresa_id,nombre_completo,rut,cargo,especialidad,telefono,correo,participa_mantencion,valor_hora_hombre,moneda,activo,observaciones}=req.body;
    const r=await pool.query(`UPDATE personal SET empresa_id=$1,nombre_completo=$2,rut=$3,cargo=$4,especialidad=$5,telefono=$6,correo=$7,participa_mantencion=$8,valor_hora_hombre=$9,moneda=$10,activo=$11,observaciones=$12 WHERE persona_id=$13 RETURNING *`,
      [empresa_id||null,nombre_completo,rut||null,cargo||null,especialidad||null,telefono||null,correo||null,participa_mantencion||false,valor_hora_hombre||null,moneda||'CLP',activo!==false,observaciones||null,req.params.id]);
    res.json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
app.patch('/api/personal/:id/activo', auth, async(req,res)=>{
  try{const r=await pool.query('UPDATE personal SET activo=NOT activo WHERE persona_id=$1 RETURNING *',[req.params.id]);res.json(r.rows[0]);}catch(e){res.status(400).json({error:e.message});}
});

// ══════════════════════════════════════════════════════
// OT SISTEMAS (muchos a muchos)
// ══════════════════════════════════════════════════════
app.get('/api/mant/ot/:id/sistemas', auth, async(req,res)=>{
  try{const r=await pool.query('SELECT * FROM mant_ot_sistemas WHERE ot_id=$1 ORDER BY es_principal DESC,sistema',[req.params.id]);res.json(r.rows);}catch(e){res.status(500).json({error:e.message});}
});
app.post('/api/mant/ot/:id/sistemas', auth, async(req,res)=>{
  try{
    const{sistemas}=req.body; // array: [{sistema:'motor',es_principal:true}, ...]
    if(!Array.isArray(sistemas)||!sistemas.length) return res.status(400).json({error:'Sistemas requeridos'});
    await pool.query('DELETE FROM mant_ot_sistemas WHERE ot_id=$1',[req.params.id]);
    for(const s of sistemas){
      await pool.query('INSERT INTO mant_ot_sistemas(ot_id,sistema,es_principal) VALUES($1,$2,$3) ON CONFLICT DO NOTHING',[req.params.id,s.sistema,s.es_principal||false]);
    }
    // Update sistema field in OT with principal one
    const principal=sistemas.find(function(s){return s.es_principal;})||sistemas[0];
    await pool.query('UPDATE mant_ot SET sistema=$1 WHERE ot_id=$2',[principal.sistema,req.params.id]);
    const r=await pool.query('SELECT * FROM mant_ot_sistemas WHERE ot_id=$1 ORDER BY es_principal DESC',[req.params.id]);
    res.json(r.rows);
  }catch(e){res.status(400).json({error:e.message});}
});

// ══════════════════════════════════════════════════════
// OT PERSONAL (muchos a muchos)
// ══════════════════════════════════════════════════════
app.get('/api/mant/ot/:id/personal', auth, async(req,res)=>{
  try{
    const r=await pool.query(`SELECT op.*,p.nombre_completo,p.cargo,p.especialidad,p.valor_hora_hombre AS valor_hh_maestro FROM mant_ot_personal op JOIN personal p ON op.persona_id=p.persona_id WHERE op.ot_id=$1 ORDER BY op.rol,p.nombre_completo`,[req.params.id]);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});
app.post('/api/mant/ot/:id/personal', auth, async(req,res)=>{
  try{
    const{persona_id,rol,horas_trabajadas,valor_hora_aplicado,observacion}=req.body;
    if(!persona_id) return res.status(400).json({error:'Persona requerida'});
    // Get default valor_hh from maestro if not provided
    let vhh=parseFloat(valor_hora_aplicado)||0;
    if(!vhh){
      const p=await pool.query('SELECT valor_hora_hombre FROM personal WHERE persona_id=$1',[persona_id]);
      if(p.rows.length) vhh=parseFloat(p.rows[0].valor_hora_hombre)||0;
    }
    const r=await pool.query(`INSERT INTO mant_ot_personal(ot_id,persona_id,rol,horas_trabajadas,valor_hora_aplicado,observacion) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(ot_id,persona_id) DO UPDATE SET rol=$3,horas_trabajadas=$4,valor_hora_aplicado=$5,observacion=$6 RETURNING *`,
      [req.params.id,persona_id,rol||'ejecutor',parseFloat(horas_trabajadas)||0,vhh,observacion||null]);
    // Recalculate costo_mano_obra_interna in OT
    const totMO=await pool.query('SELECT COALESCE(SUM(costo_total),0) AS total FROM mant_ot_personal WHERE ot_id=$1',[req.params.id]);
    const moTotal=parseFloat(totMO.rows[0].total)||0;
    await pool.query(`UPDATE mant_ot SET costo_mano_obra_interna=$1, costo_total=costo_repuestos+costo_lubricantes+$1+costo_mano_obra_externa+costo_servicios+costo_traslado+costo_otros WHERE ot_id=$2`,[moTotal,req.params.id]);
    res.status(201).json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
app.delete('/api/mant/ot/personal/:id', auth, async(req,res)=>{
  try{
    const p=await pool.query('SELECT ot_id FROM mant_ot_personal WHERE id=$1',[req.params.id]);
    await pool.query('DELETE FROM mant_ot_personal WHERE id=$1',[req.params.id]);
    if(p.rows.length){
      const ot_id=p.rows[0].ot_id;
      const totMO=await pool.query('SELECT COALESCE(SUM(costo_total),0) AS total FROM mant_ot_personal WHERE ot_id=$1',[ot_id]);
      const moTotal=parseFloat(totMO.rows[0].total)||0;
      await pool.query(`UPDATE mant_ot SET costo_mano_obra_interna=$1, costo_total=costo_repuestos+costo_lubricantes+$1+costo_mano_obra_externa+costo_servicios+costo_traslado+costo_otros WHERE ot_id=$2`,[moTotal,ot_id]);
    }
    res.json({ok:true});
  }catch(e){res.status(400).json({error:e.message});}
});

// ══════════════════════════════════════════════════════
// OC DETALLE — Vincular a OT
// ══════════════════════════════════════════════════════
app.patch('/api/oc/detalle/:id/ot', auth, async(req,res)=>{
  try{
    const{ot_id}=req.body;
    const r=await pool.query('UPDATE ordenes_compra_detalle SET ot_id=$1 WHERE detalle_id=$2 RETURNING *',[ot_id||null,req.params.id]);
    res.json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
// OC líneas asociadas a una OT
app.get('/api/mant/ot/:id/compras', auth, async(req,res)=>{
  try{
    const r=await pool.query(`SELECT d.*,oc.numero_oc,oc.fecha_emision,oc.estado AS oc_estado,prov.razon_social AS proveedor FROM ordenes_compra_detalle d JOIN ordenes_compra oc ON d.oc_id=oc.oc_id LEFT JOIN proveedores prov ON oc.proveedor_id=prov.proveedor_id WHERE d.ot_id=$1 ORDER BY oc.fecha_emision DESC`,[req.params.id]);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});


// Enlazar OC completa (líneas sin ingresa_bodega) a una OT
app.patch('/api/oc/link-ot', auth, async(req,res)=>{
  try{
    const{oc_id,ot_id}=req.body;
    if(!oc_id||!ot_id) return res.status(400).json({error:'oc_id y ot_id requeridos'});
    // Solo enlazar líneas que NO van a inventario (ingresa_bodega=false o null)
    const r=await pool.query(
      `UPDATE ordenes_compra_detalle SET ot_id=$1 WHERE oc_id=$2 AND (ingresa_bodega IS NULL OR ingresa_bodega=false) RETURNING *`,
      [ot_id,oc_id]
    );
    res.json({ok:true,lineas_enlazadas:r.rowCount});
  }catch(e){res.status(400).json({error:e.message});}
});

// SPA fallback — must be AFTER all API routes
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
