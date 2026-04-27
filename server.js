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

  // ── ot_id se agrega en ocPatch después de crear ordenes_compra_detalle ──

  // ── Mantenedor de sistemas técnicos (global) ──
  await q(`CREATE TABLE IF NOT EXISTS mant_sistemas (
    sistema_id SERIAL PRIMARY KEY,
    codigo VARCHAR(10) NOT NULL UNIQUE,
    nombre VARCHAR(100) NOT NULL,
    descripcion TEXT,
    orden INT DEFAULT 0,
    activo BOOLEAN DEFAULT true
  )`);

  // ── Mantenedor de tareas estándar ──
  await q(`CREATE TABLE IF NOT EXISTS mant_tareas_std (
    tarea_std_id SERIAL PRIMARY KEY,
    sistema_id INT NOT NULL REFERENCES mant_sistemas(sistema_id),
    nombre VARCHAR(200) NOT NULL,
    descripcion TEXT,
    tipo_tarea VARCHAR(30) DEFAULT 'preventiva',
    tipo_activo VARCHAR(20) DEFAULT 'todos',
    activo BOOLEAN DEFAULT true,
    UNIQUE(sistema_id, nombre)
  )`);

  // ── Personal por tarea de OT ──
  await q(`CREATE TABLE IF NOT EXISTS mant_ot_tarea_personal (
    id SERIAL PRIMARY KEY,
    tarea_id INT NOT NULL REFERENCES mant_ot_tareas(tarea_id) ON DELETE CASCADE,
    tipo_personal VARCHAR(10) DEFAULT 'interno',
    persona_id INT REFERENCES personal(persona_id),
    nombre_externo VARCHAR(150),
    horas_trabajadas NUMERIC(7,2) DEFAULT 0,
    valor_hora_aplicado NUMERIC(12,2) DEFAULT 0,
    costo_total NUMERIC(14,2) GENERATED ALWAYS AS (horas_trabajadas * valor_hora_aplicado) STORED,
    tiene_costo BOOLEAN DEFAULT true,
    observacion TEXT
  )`);

  // ── ALTER tareas OT: agregar sistema_id y tarea_std_id ──
  try{await q('ALTER TABLE mant_ot_tareas ADD COLUMN IF NOT EXISTS sistema_id INT REFERENCES mant_sistemas(sistema_id)');}catch(e){}
  try{await q('ALTER TABLE mant_ot_tareas ADD COLUMN IF NOT EXISTS tarea_std_id INT REFERENCES mant_tareas_std(tarea_std_id)');}catch(e){}

  // ── ALTER movimiento_detalle: enlace a OT ──
  try{await q('ALTER TABLE movimiento_detalle ADD COLUMN IF NOT EXISTS ot_id INT');}catch(e){}

  // ── ALTER mant_ot: traslado details ──
  try{await q('ALTER TABLE mant_ot ADD COLUMN IF NOT EXISTS vehiculo_traslado VARCHAR(100)');}catch(e){}
  try{await q('ALTER TABLE mant_ot ADD COLUMN IF NOT EXISTS distancia_km NUMERIC(8,1) DEFAULT 0');}catch(e){}
  try{await q('ALTER TABLE mant_ot ADD COLUMN IF NOT EXISTS costo_combustible_traslado NUMERIC(14,2) DEFAULT 0');}catch(e){}

  // ── Maestro de Modelos de Equipo ──
  await q(`CREATE TABLE IF NOT EXISTS modelos_equipo (
    modelo_id SERIAL PRIMARY KEY,
    marca VARCHAR(80) NOT NULL,
    modelo VARCHAR(100) NOT NULL,
    tipo_maquina VARCHAR(40),
    funcion_principal VARCHAR(150),
    motor_descripcion VARCHAR(150),
    potencia_hp NUMERIC(6,1),
    peso_kg NUMERIC(8,0),
    cap_tanque_combustible NUMERIC(8,1),
    cap_aceite_motor NUMERIC(8,1),
    cap_sist_hidraulico NUMERIC(8,1),
    cap_refrigerante NUMERIC(8,1),
    tipo_transmision VARCHAR(80),
    ancho_zapata VARCHAR(60),
    observaciones TEXT,
    activo BOOLEAN DEFAULT true,
    creado_en TIMESTAMP DEFAULT NOW(),
    UNIQUE(marca, modelo)
  )`);

  // ── ALTER equipos: tipo_cargo y modelo_id ──
  try{await q("ALTER TABLE equipos ADD COLUMN IF NOT EXISTS tipo_cargo VARCHAR(30) DEFAULT 'maquinaria'");}catch(e){}
  try{await q('ALTER TABLE equipos ADD COLUMN IF NOT EXISTS modelo_id INT REFERENCES modelos_equipo(modelo_id)');}catch(e){}
  try{await q('ALTER TABLE equipos ADD COLUMN IF NOT EXISTS contacto_terreno VARCHAR(100)');}catch(e){}
  try{await q('ALTER TABLE equipos ADD COLUMN IF NOT EXISTS chasis VARCHAR(80)');}catch(e){}
  try{await q("ALTER TABLE equipos ADD COLUMN IF NOT EXISTS horas_productivas_dia NUMERIC(4,1) DEFAULT 12");}catch(e){}

    // Indices
  const idxs=[
    'CREATE INDEX IF NOT EXISTS idx_mant_ot_equipo ON mant_ot(equipo_id)',
    'CREATE INDEX IF NOT EXISTS idx_mant_ot_estado ON mant_ot(estado)',
    'CREATE INDEX IF NOT EXISTS idx_mant_avisos_equipo ON mant_avisos(equipo_id)',
    'CREATE INDEX IF NOT EXISTS idx_mant_lecturas_equipo ON mant_lecturas(equipo_id,fecha DESC)',
    'CREATE INDEX IF NOT EXISTS idx_mant_prog_equipo ON mant_programacion(equipo_id)',
    'CREATE INDEX IF NOT EXISTS idx_mant_tareas_std_sistema ON mant_tareas_std(sistema_id)',
    'CREATE INDEX IF NOT EXISTS idx_mant_ot_tarea_pers ON mant_ot_tarea_personal(tarea_id)',
    'CREATE INDEX IF NOT EXISTS idx_mov_detalle_ot ON movimiento_detalle(ot_id)',
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
  try{await q('ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS username VARCHAR(50) UNIQUE');}catch(e){}
  try{await q('ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS empresa_id INT REFERENCES empresas(empresa_id)');}catch(e){}
  try{await q('ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS faena_id INT REFERENCES faenas(faena_id)');}catch(e){}
  await q(`CREATE TABLE IF NOT EXISTS roles (rol_id SERIAL PRIMARY KEY, nombre VARCHAR(50) NOT NULL UNIQUE, descripcion VARCHAR(200), modulos JSONB DEFAULT '[]', es_admin BOOLEAN DEFAULT false, activo BOOLEAN DEFAULT true, creado_en TIMESTAMP DEFAULT NOW())`);
  try{await q('ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS rol_id INT REFERENCES roles(rol_id)');}catch(e){}
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
    "ALTER TABLE ordenes_compra ADD COLUMN IF NOT EXISTS recibido_en TIMESTAMP",
    "ALTER TABLE ordenes_compra_detalle ADD COLUMN IF NOT EXISTS linea_num INT",
    "ALTER TABLE ordenes_compra_detalle ADD COLUMN IF NOT EXISTS exenta BOOLEAN DEFAULT false",
    "ALTER TABLE ordenes_compra_detalle ADD COLUMN IF NOT EXISTS descripcion TEXT",
    "ALTER TABLE ordenes_compra_detalle ADD COLUMN IF NOT EXISTS subcategoria_id INT",
    "ALTER TABLE ordenes_compra_detalle ADD COLUMN IF NOT EXISTS faena_id INT",
    "ALTER TABLE ordenes_compra_detalle ADD COLUMN IF NOT EXISTS equipo_id INT",
    "ALTER TABLE ordenes_compra_detalle ADD COLUMN IF NOT EXISTS ingresa_bodega BOOLEAN DEFAULT false",
    "ALTER TABLE ordenes_compra_detalle ADD COLUMN IF NOT EXISTS bodega_destino_id INT",
    "ALTER TABLE ordenes_compra_detalle ADD COLUMN IF NOT EXISTS ot_id INT",
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
  // Seed roles
  try{
    const rc=await pool.query('SELECT COUNT(*) FROM roles');
    if(parseInt(rc.rows[0].count)===0){
      await pool.query(`INSERT INTO roles(nombre,descripcion,modulos,es_admin) VALUES
        ('Administrador','Acceso completo al sistema','[]',true),
        ('Supervisor','Acceso a todos los módulos operativos','[]',false),
        ('Bodeguero','Control de inventario y bodegas','["inventario","reportes"]',false),
        ('Operador','Rendiciones y consultas básicas','["rendiciones","ordenes"]',false)
        ON CONFLICT(nombre) DO NOTHING`);
      // Link admin user to admin role
      const adminRole=await pool.query("SELECT rol_id FROM roles WHERE es_admin=true LIMIT 1");
      if(adminRole.rows.length){
        await pool.query("UPDATE usuarios SET rol_id=$1 WHERE rol IS NOT NULL AND rol_id IS NULL",[adminRole.rows[0].rol_id]);
      }
    }
  }catch(e){console.log('[WARN] seed roles:',e.message);}
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
  try{ await setupRendiciones(pool.query.bind(pool)); }catch(e){console.log('[WARN] rend tables:',e.message);}
  try{ await setupTransporte(pool.query.bind(pool)); }catch(e){console.log('[WARN] transporte tables:',e.message);}
  try{ await setupContratos(pool.query.bind(pool)); }catch(e){console.log('[WARN] contratos tables:',e.message);}
  try{ await setupProgMant(pool.query.bind(pool)); }catch(e){console.log('[WARN] prog mant tables:',e.message);}
  try{ await setupFacturaGuias(pool.query.bind(pool)); }catch(e){console.log('[WARN] factura guias tables:',e.message);}
  try{ await pool.query('ALTER TABLE comb_estanques ALTER COLUMN empresa_id DROP NOT NULL'); }catch(e){}
  // Seed sistemas y tareas estándar
  try{
    const sc=await pool.query('SELECT COUNT(*) FROM mant_sistemas');
    if(parseInt(sc.rows[0].count)===0){
      await pool.query(`INSERT INTO mant_sistemas(codigo,nombre,descripcion,orden) VALUES
        ('MOT','Motor','Motor diésel, inyección, alimentación',1),
        ('HID','Sistema hidráulico','Bombas, válvulas, cilindros, mangueras',2),
        ('ELE','Sistema eléctrico','Alternador, baterías, cableado, sensores',3),
        ('TRA','Transmisión','Caja de cambios, convertidor, diferenciales',4),
        ('ENF','Enfriamiento','Radiador, termostato, ventilador, refrigerante',5),
        ('COM','Combustible','Tanque, filtros, líneas, bombas de combustible',6),
        ('FRE','Frenos','Discos, pastillas, líquido, ABS',7),
        ('DIR','Dirección','Orbitrol, cilindros, bomba, barras',8),
        ('EST','Estructura/Chasis','Bastidor, cabina, guardabarros, protecciones',9),
        ('CAB','Cabezal forestal','Sierra, rodillos, cuchillas, alimentador',10),
        ('ROD','Tren de rodado','Cadenas, rodillos, ruedas guía, tensores',11),
        ('LUB','Lubricación','Sistema centralizado, engrase, puntos',12),
        ('NEU','Neumáticos','Cubiertas, llantas, presión, alineación',13),
        ('SUS','Suspensión','Amortiguadores, resortes, bujes',14),
        ('CAR','Carrocería','Plataforma, tolva, baranda, accesorios',15),
        ('CLI','Climatización','A/C, calefacción, filtro cabina',16),
        ('SEG','Seguridad','Extintor, alarmas, luces, cinturones',17)
      ON CONFLICT DO NOTHING`);
      // Tareas estándar por sistema
      const tareas=[
        ['MOT','Cambio de aceite de motor'],['MOT','Cambio de filtro de aceite'],['MOT','Cambio de filtro de combustible'],['MOT','Cambio de filtro separador de agua'],['MOT','Revisión de fugas de aceite'],['MOT','Ajuste/cambio de correas'],['MOT','Inspección de inyectores'],['MOT','Medición de compresión'],['MOT','Limpieza de respiradero de cárter'],['MOT','Revisión de turbocompresor'],['MOT','Cambio de filtro de aire primario'],['MOT','Cambio de filtro de aire secundario'],['MOT','Revisión de soportes de motor'],
        ['HID','Cambio de aceite hidráulico'],['HID','Cambio de filtro hidráulico de retorno'],['HID','Cambio de filtro hidráulico de presión'],['HID','Revisión de mangueras hidráulicas'],['HID','Inspección de fugas hidráulicas'],['HID','Cambio de sellos de cilindro'],['HID','Purga de circuito hidráulico'],['HID','Revisión de bomba hidráulica'],['HID','Ajuste de presiones de sistema'],['HID','Cambio de acumulador hidráulico'],['HID','Revisión de válvulas de control'],['HID','Cambio de acoples rápidos'],
        ['ELE','Revisión de baterías'],['ELE','Limpieza de bornes'],['ELE','Revisión de alternador'],['ELE','Inspección de cableado general'],['ELE','Revisión de sensores'],['ELE','Diagnóstico de códigos de falla'],['ELE','Cambio de fusibles/relés'],['ELE','Revisión de luces de trabajo'],['ELE','Revisión de motor de arranque'],['ELE','Actualización de software/ECU'],
        ['TRA','Cambio de aceite de transmisión'],['TRA','Revisión de convertidor de torque'],['TRA','Ajuste de embrague'],['TRA','Revisión de diferenciales'],['TRA','Cambio de filtro de transmisión'],['TRA','Revisión de mandos finales'],['TRA','Inspección de juntas universales'],
        ['ENF','Revisión de nivel de refrigerante'],['ENF','Limpieza de radiador'],['ENF','Cambio de termostato'],['ENF','Revisión de ventilador'],['ENF','Cambio de mangueras de refrigeración'],['ENF','Revisión de bomba de agua'],['ENF','Cambio de refrigerante'],
        ['COM','Drenaje de tanque de combustible'],['COM','Cambio de prefiltro de combustible'],['COM','Revisión de líneas de combustible'],['COM','Limpieza de tanque'],['COM','Revisión de bomba de alimentación'],
        ['FRE','Cambio de pastillas de freno'],['FRE','Cambio de discos de freno'],['FRE','Revisión de líquido de frenos'],['FRE','Purga de sistema de frenos'],['FRE','Revisión de freno de estacionamiento'],['FRE','Inspección de cañerías de freno'],['FRE','Ajuste de frenos'],
        ['DIR','Revisión de dirección hidráulica'],['DIR','Cambio de aceite de dirección'],['DIR','Revisión de barras de dirección'],['DIR','Ajuste de convergencia'],['DIR','Revisión de cilindros de dirección'],
        ['EST','Inspección estructural general'],['EST','Reparación de soldaduras'],['EST','Revisión de cabina'],['EST','Cambio de vidrios/espejos'],['EST','Revisión de protecciones'],
        ['CAB','Afilado de sierra/cuchillas'],['CAB','Cambio de cadena de sierra'],['CAB','Revisión de rodillos de alimentación'],['CAB','Cambio de cuchillas desramadoras'],['CAB','Ajuste de presión de rodillos'],['CAB','Revisión de motor de sierra'],['CAB','Lubricación de barra de sierra'],['CAB','Inspección de mangueras de cabezal'],['CAB','Cambio de espada de sierra'],['CAB','Calibración de diámetros'],
        ['ROD','Revisión de tensión de cadenas'],['ROD','Cambio de zapatas'],['ROD','Revisión de rodillos superiores'],['ROD','Revisión de rodillos inferiores'],['ROD','Cambio de rueda guía'],['ROD','Revisión de sprocket'],['ROD','Ajuste de tensor de cadena'],['ROD','Inspección de pernos de zapata'],
        ['LUB','Engrase general de puntos'],['LUB','Revisión de sistema de lubricación centralizado'],['LUB','Cambio de grasa de rodamientos'],['LUB','Lubricación de articulaciones'],['LUB','Revisión de niveles de aceite'],
        ['NEU','Rotación de neumáticos'],['NEU','Revisión de presión'],['NEU','Alineación y balanceo'],['NEU','Cambio de neumáticos'],['NEU','Revisión de desgaste'],['NEU','Reparación de pinchazos'],
        ['SUS','Revisión de amortiguadores'],['SUS','Cambio de bujes'],['SUS','Revisión de hojas de resorte'],['SUS','Inspección de brazos de suspensión'],
        ['CAR','Revisión de plataforma/tolva'],['CAR','Reparación de barandas'],['CAR','Revisión de sistema de volteo'],['CAR','Inspección de anclajes'],
        ['CLI','Revisión de A/C'],['CLI','Cambio de filtro de cabina'],['CLI','Recarga de gas refrigerante'],['CLI','Revisión de calefacción'],
        ['SEG','Revisión de extintor'],['SEG','Revisión de alarma de retroceso'],['SEG','Revisión de cinturón de seguridad'],['SEG','Revisión de luces de emergencia'],['SEG','Inspección de sistema ROPS/FOPS']
      ];
      for(const[cod,nom] of tareas){
        await pool.query(`INSERT INTO mant_tareas_std(sistema_id,nombre) SELECT sistema_id,$2 FROM mant_sistemas WHERE codigo=$1 ON CONFLICT DO NOTHING`,[cod,nom]);
      }
      console.log('  [OK] Sistemas ('+17+') y tareas estándar ('+tareas.length+') cargados');
    }
  }catch(e){console.log('[WARN] seed sistemas:',e.message);}

  // ── Seed modelos de equipo desde gestión de flota ──
  try{
    const mc=await pool.query('SELECT COUNT(*) FROM modelos_equipo');
    if(parseInt(mc.rows[0].count)===0){
      const modelos=[
        ['KOMATSU','PC210LC-10MO','EXCAVADORA','Excavación/Carguío','SAA6D107E-1 Turbo Intercooled',165,22300,400,22,190,15,'Hidráulica','800 mm triple grouser'],
        ['KOMATSU','PC200LC','EXCAVADORA','Excavación/Carguío','SAA6D107E-1 Turbo Intercooled',165,21000,400,22,190,15,'Hidráulica','800 mm triple grouser'],
        ['KOMATSU','PC200LC-8MO','EXCAVADORA','Excavación/Carguío','SAA6D107E-1 Turbo Intercooled',165,21000,400,22,190,15,'Hidráulica','800 mm triple grouser'],
        ['KOMATSU','PC210LL','PROCESADOR','Procesamiento','Komatsu SAA6D107E-3 Turbo',168,25000,416,22,240,15,'Hidráulica','800 mm triple grouser'],
        ['JOHN DEERE','859M','FELLER','Volteo','JD 6068 PowerTech PSS 6.8L T3',300,27200,416,19,280,24,'Hidrost. cadenas','600 mm triple grouser'],
        ['JOHN DEERE','2154G','PROCESADOR','Procesamiento','JD 6068 PowerTech PSS 6.8L FT4',225,25000,416,19,280,24,'Hidrost. cadenas','600 mm triple grouser'],
        ['JOHN DEERE','948L','SKIDDER','Arrastre','JD 6068 PowerTech PSS 6.8L FT4',230,19500,310,19,95,24,'Hidrost. ruedas','30.5L-32 forestales'],
        ['JOHN DEERE','640H','SKIDDER','Arrastre','JD 6068 PowerTech 6.8L T2',200,16500,270,19,85,20,'Hidrost. ruedas','30.5L-32 forestales'],
        ['JOHN DEERE','648L','SKIDDER','Arrastre','JD 6068 PowerTech PSS 6.8L FT4',230,18500,310,19,95,24,'Hidrost. ruedas','30.5L-32 forestales'],
        ['JOHN DEERE','848H','SKIDDER','Arrastre','JD 6068 PowerTech 6.8L T2',235,22000,310,19,95,20,'Hidrost. ruedas','30.5L-32 forestales'],
        ['BELL','LOGGER 220C','TRINEUMATICO','Transporte forestal','Mercedes-Benz OM906LA 6 cil',286,22000,300,28,null,22,'Powershift 6F/3R','Neumáticos forestales'],
        ['URUS','III PHIL','TORRE MADEREO','Madereo aéreo','N/A (montada sobre base)',null,null,null,null,null,null,null,null],
        ['TIGERCAT','625H 6X6','SKIDDER','Arrastre','FPT N67 T4F 6.7L',235,20500,308,18,110,22,'Hidrost. ruedas','30.5L-32 forestales'],
        ['TIGERCAT','625H','SKIDDER','Arrastre','FPT N67 T4F 6.7L',235,18000,308,18,110,22,'Hidrost. ruedas','30.5L-32 forestales'],
        ['TIGERCAT','632','SKIDDER','Arrastre','FPT N67 T4F 6.7L',252,22000,330,18,130,22,'Hidrost. ruedas','35.5L-32 forestales'],
        ['TIGERCAT','LS855E','SHOVEL LOGGER','Carga/Shovel','FPT N67 Stage V 6.7L',252,37000,450,18,300,25,'Hidrost. cadenas','700 mm triple grouser'],
        ['TIMBCO','T445D','FELLER','Volteo','Caterpillar C9 ACERT 8.8L',350,32000,500,26,300,30,'Hidrost. cadenas','700 mm triple grouser'],
        ['CATERPILLAR','525C','SKIDDER','Arrastre','Caterpillar C7 ACERT 7.2L',225,18000,284,17,90,20,'Powershift','30.5L-32 forestales'],
        ['ECOFORST','T-Winch 10.2','HUINCHE','Asistencia tracción','Accionamiento hidr. 100 kN max',null,2500,null,null,null,null,'Hidráulico',null],
        ['ECOFORST','T-Winch 30.2','HUINCHE','Asistencia tracción','Accionamiento hidr. 300 kN max',null,4500,null,null,null,null,'Hidráulico',null],
        ['NEUSON FOREST','243HVT','HARVESTER','Cosecha','Stage V 6 cilindros',250,27000,400,20,250,22,'Hidrost. cadenas','700 mm triple grouser'],
        ['DOOSAN','DX360LC-7M','TORRE MADEREO','Madereo aéreo','Doosan DL06K 6 cil Turbo',271,36200,600,24,null,28,'Hidráulica','600 mm triple grouser']
      ];
      for(const m of modelos){
        await pool.query('INSERT INTO modelos_equipo(marca,modelo,tipo_maquina,funcion_principal,motor_descripcion,potencia_hp,peso_kg,cap_tanque_combustible,cap_aceite_motor,cap_sist_hidraulico,cap_refrigerante,tipo_transmision,ancho_zapata) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) ON CONFLICT(marca,modelo) DO NOTHING',m);
      }
      console.log('  [OK] '+modelos.length+' modelos de equipo cargados');
    }
  }catch(e){console.log('[WARN] seed modelos:',e.message);}

  // ── Seed flota de equipos (28 máquinas Leonidas Poo) ──
  try{
    const fc=await pool.query("SELECT COUNT(*) FROM equipos WHERE tipo_cargo='maquinaria' AND modelo_id IS NOT NULL");
    if(parseInt(fc.rows[0].count)===0){
      // Find empresa Leonidas Poo
      const empQ=await pool.query("SELECT empresa_id FROM empresas WHERE LOWER(razon_social) LIKE '%leonidas%poo%' OR LOWER(razon_social) LIKE '%lpz%' LIMIT 1");
      const empId=empQ.rows.length?empQ.rows[0].empresa_id:null;
      const flota=[
        ['EG-05','Excavadora 05','EXCAVADORA','KOMATSU','PC210LC-10MO','KMTPC282TMC600671','RICARDO RIVEROS'],
        ['Feller-05','Feller 05','FELLER','JOHN DEERE','859M','1T0859MXHKC343920','RICARDO RIVEROS'],
        ['PRO-10','Procesador 10','PROCESADOR','JOHN DEERE','2154G','1FF2154GTKD212297','SEBASTIAN POO'],
        ['SK-12','Skidder 12','SKIDDER','JOHN DEERE','948L','1DW948LXCGC677331','RICARDO RIVEROS'],
        ['PRO-09','Procesador 09','PROCESADOR','JOHN DEERE','2154G','1FF2154GEKD212295','LUIS SAEZ'],
        ['EG-02','Excavadora 02','EXCAVADORA','KOMATSU','PC200LC','KMTPC244VGC400814','LUIS SAEZ'],
        ['Bell-8','Trineumático Bell 8','TRINEUMATICO','BELL','LOGGER 220C','BCH10147','LUIS SAEZ'],
        ['Torre-06','Torre Madereo 06','TORRE MADEREO','URUS','III PHIL','201','LUIS SAEZ'],
        ['SK-09','Skidder 09','SKIDDER','JOHN DEERE','640H','1DW640HXAA0630471','RICARDO RIVEROS'],
        ['EG-9','Excavadora 09','EXCAVADORA','KOMATSU','PC200LC-8MO','KMTPC244PEC400366','LUIS SAEZ'],
        ['EG-04','Ordenadora 04','ORDENADORA','KOMATSU','PC210LC-10MO','401502','SEBASTIAN POO'],
        ['EG-03','Excavadora 03','EXCAVADORA','KOMATSU','PC210LC-10MO','KMTPC244CJC401502','LUIS SAEZ'],
        ['Feller-04','Feller 04','FELLER','JOHN DEERE','859M','1T0859MXVKC343864','SEBASTIAN POO'],
        ['PRO-08','Procesador 08','PROCESADOR','JOHN DEERE','2154G','1FF2154GEKC212271','ALEJANDRO SEPULVEDA'],
        ['SK-11','Skidder 11','SKIDDER','JOHN DEERE','648L','1DW648LBKKC695616','SEBASTIAN POO'],
        ['SK-14A','Skidder 14A','SKIDDER','TIGERCAT','625H 6X6','6250938','SEBASTIAN POO'],
        ['Feller-03','Feller 03','FELLER','TIMBCO','T445D','FT4C-2155-031903','SEBASTIAN POO'],
        ['SK-10','Skidder 10','SKIDDER','JOHN DEERE','848H','1DW848HXKBC636904','SEBASTIAN POO'],
        ['SK-08','Skidder 08','SKIDDER','CATERPILLAR','525C','CAT0525CL52501408','SEBASTIAN POO'],
        ['PRO-11','Procesador 11','PROCESADOR','KOMATSU','PC210LL','KMTPC243ELWA52084','JUAN PAULO SILVA'],
        ['Shovel-01','Shovel Logger 01','SHOVEL LOGGER','TIGERCAT','LS855E','85504014','LUIS SAEZ'],
        ['SK-13','Skidder 13','SKIDDER','TIGERCAT','625H','6250922','LUIS SAEZ'],
        ['Twinch 01','T-Winch 01','HUINCHE','ECOFORST','T-Winch 10.2','T-W02-083','LUIS SAEZ'],
        ['Twinch 02','T-Winch 02','HUINCHE','ECOFORST','T-Winch 30.2','T-W30.2-090','LUIS SAEZ'],
        ['Harvester-01','Harvester 01','HARVESTER','NEUSON FOREST','243HVT','183140','LUIS SAEZ'],
        ['Torre-01','Torre Madereo 01','TORRE MADEREO','DOOSAN','DX360LC-7M','DWGCECFWCP1010831','LUIS SAEZ'],
        ['SK-14B','Skidder 14B','SKIDDER','TIGERCAT','632','6320163','GUSTAVO REYES'],
        ['PROC-12','Procesador 12','PROCESADOR','JOHN DEERE','2154G','1FF2154GENC212579','SEBASTIAN POO']
      ];
      let loaded=0;
      for(const[codigo,nombre,tipo,marca,modelo,chasis,contacto] of flota){
        const modQ=await pool.query('SELECT modelo_id FROM modelos_equipo WHERE marca=$1 AND modelo=$2 LIMIT 1',[marca,modelo]);
        const modId=modQ.rows.length?modQ.rows[0].modelo_id:null;
        const horometro=Math.round(5000+Math.random()*10000);
        await pool.query(`INSERT INTO equipos(codigo,nombre,tipo,marca,modelo,empresa_id,tipo_cargo,modelo_id,chasis,contacto_terreno,horometro_actual,horas_productivas_dia,activo)
          VALUES($1,$2,$3,$4,$5,$6,'maquinaria',$7,$8,$9,$10,12,true) ON CONFLICT(codigo) DO UPDATE SET nombre=$2,tipo=$3,marca=$4,modelo=$5,tipo_cargo='maquinaria',modelo_id=$7,chasis=$8,contacto_terreno=$9,horometro_actual=COALESCE(equipos.horometro_actual,$10),horas_productivas_dia=COALESCE(equipos.horas_productivas_dia,12)`,
          [codigo,nombre,tipo,marca,modelo,empId,modId,chasis,contacto,horometro]);
        loaded++;
      }
      console.log('  [OK] '+loaded+' equipos de flota cargados');
    }
  }catch(e){console.log('[WARN] seed flota:',e.message);}

  // ── Seed planes de mantención de ejemplo ──
  try{
    const pc=await pool.query('SELECT COUNT(*) FROM mant_planes');
    if(parseInt(pc.rows[0].count)===0){
      const empQ=await pool.query("SELECT empresa_id FROM empresas WHERE LOWER(razon_social) LIKE '%leonidas%poo%' LIMIT 1");
      const empId=empQ.rows.length?empQ.rows[0].empresa_id:null;
      const planes=[
        ['Cambio aceite motor','preventivo','motor',500,null,null,'Cambio de aceite motor y filtro según especificación OEM'],
        ['Cambio filtros combustible','preventivo','motor',500,null,null,'Filtro primario separador agua + filtro secundario'],
        ['Cambio filtro hidráulico','preventivo','hidraulico',1000,null,null,'Filtro retorno + filtro succión hidráulico'],
        ['Cambio aceite hidráulico','preventivo','hidraulico',2000,null,null,'Vaciado completo y recarga sistema hidráulico'],
        ['Engrase general','preventivo','lubricacion',50,null,null,'Engrase de todos los puntos según carta de lubricación'],
        ['Cambio refrigerante','preventivo','enfriamiento',2000,null,null,'Vaciado y recarga sistema de refrigeración'],
        ['Revisión frenos','preventivo','frenos',500,null,null,'Inspección pastillas, discos, líquido de frenos'],
        ['Inspección eléctrica','preventivo','electrico',1000,null,null,'Revisión cableado, alternador, baterías, luces'],
        ['Cambio filtro aire','preventivo','motor',250,null,null,'Filtro primario y secundario de admisión'],
        ['Revisión transmisión','preventivo','transmision',2000,null,null,'Nivel aceite, filtros, presiones de trabajo']
      ];
      for(const[nombre,tipo,sistema,hrs,km,dias,desc] of planes){
        await pool.query('INSERT INTO mant_planes(empresa_id,nombre,tipo_mantencion,sistema,intervalo_horas,intervalo_km,intervalo_dias,descripcion,activo) VALUES($1,$2,$3,$4,$5,$6,$7,$8,true)',
          [empId,nombre,tipo,sistema,hrs||null,km||null,dias||null,desc]);
      }
      // Crear programación para los primeros 10 equipos
      const eqs=await pool.query("SELECT equipo_id,horometro_actual FROM equipos WHERE tipo_cargo='maquinaria' AND activo=true ORDER BY equipo_id LIMIT 10");
      const pls=await pool.query('SELECT plan_id,intervalo_horas FROM mant_planes WHERE activo=true');
      for(const eq of eqs.rows){
        for(const pl of pls.rows.slice(0,4)){
          const proxHrs=(parseFloat(eq.horometro_actual)||0)+parseFloat(pl.intervalo_horas);
          const estados=['vigente','proxima','vencida'];
          const est=estados[Math.floor(Math.random()*3)];
          await pool.query('INSERT INTO mant_programacion(equipo_id,plan_id,empresa_id,proxima_horas,estado) VALUES($1,$2,$3,$4,$5) ON CONFLICT(equipo_id,plan_id) DO NOTHING',
            [eq.equipo_id,pl.plan_id,empId,proxHrs,est]);
        }
      }
      // Crear OTs de ejemplo
      const eqSample=await pool.query("SELECT equipo_id,nombre FROM equipos WHERE tipo_cargo='maquinaria' AND activo=true ORDER BY equipo_id LIMIT 6");
      const tipos=['preventivo','correctivo','preventivo','correctivo','preventivo','correctivo'];
      const estados2=['cerrada','cerrada','en_ejecucion','abierta','cerrada','abierta'];
      const costos=[350000,890000,0,0,420000,0];
      const hrsD=[4,24,0,0,6,0];
      const yr=new Date().getFullYear();
      let otN=1;
      for(let i=0;i<Math.min(6,eqSample.rows.length);i++){
        const eq=eqSample.rows[i];
        const num='OT-'+yr+'-'+String(otN++).padStart(4,'0');
        const dias=Math.floor(Math.random()*30)+1;
        const fa=new Date();fa.setDate(fa.getDate()-dias);
        await pool.query(`INSERT INTO mant_ot(numero_ot,empresa_id,equipo_id,tipo_mantencion,origen,fecha_apertura,estado,prioridad,costo_total,tiempo_detenido_hrs,usuario)
          VALUES($1,$2,$3,$4,'manual',$5,$6,$7,$8,$9,'sistema') ON CONFLICT(numero_ot) DO NOTHING`,
          [num,empId,eq.equipo_id,tipos[i],fa.toISOString().split('T')[0],estados2[i],i<2?'alta':'normal',costos[i],hrsD[i]]);
      }
      // Update seq to avoid conflicts
      await pool.query("SELECT setval('seq_oc_num',(SELECT COALESCE(MAX(oc_id),0)+1 FROM ordenes_compra))").catch(()=>{});
      console.log('  [OK] Planes de mantención, programación y OTs de ejemplo cargados');
    }
  }catch(e){console.log('[WARN] seed planes:',e.message);}

  // ── Seed datos de rendiciones de gasto ──
  try{
    const rc=await pool.query('SELECT COUNT(*) FROM rend_entregas');
    if(parseInt(rc.rows[0].count)===0){
      const empQ=await pool.query("SELECT empresa_id FROM empresas WHERE activo=true ORDER BY empresa_id LIMIT 1");
      const empId=empQ.rows.length?empQ.rows[0].empresa_id:null;
      // Crear personal de ejemplo si no hay suficientes
      const persQ=await pool.query("SELECT persona_id,nombre_completo FROM personal WHERE activo=true ORDER BY persona_id LIMIT 5");
      let personas=persQ.rows;
      if(personas.length<3){
        const nombres=[
          ['Ricardo Riveros','12.345.678-9','Jefe de faena','operaciones'],
          ['Sebastián Poo','13.456.789-0','Supervisor','administracion'],
          ['Luis Sáez','14.567.890-1','Mecánico líder','mantencion'],
          ['Alejandro Sepúlveda','15.678.901-2','Operador','operaciones'],
          ['Juan Paulo Silva','16.789.012-3','Mecánico','mantencion']
        ];
        for(const[nom,rut,cargo,esp] of nombres){
          await pool.query("INSERT INTO personal(empresa_id,rut,nombre_completo,cargo,especialidad,participa_mantencion,activo) VALUES($1,$2,$3,$4,$5,true,true)",[empId,rut,nom,cargo,esp]).catch(()=>{});
        }
        const p2=await pool.query("SELECT persona_id,nombre_completo FROM personal WHERE activo=true ORDER BY persona_id LIMIT 5");
        personas=p2.rows;
      }
      if(personas.length===0){console.log('  [SKIP] Rendiciones: no hay personal');throw new Error('skip');}
      const faenasQ=await pool.query("SELECT faena_id FROM faenas WHERE activo=true LIMIT 3");
      const faenaIds=faenasQ.rows.map(r=>r.faena_id);

      // Entregas de fondos
      const entregas=[
        [personas[0]?.persona_id,'2026-03-01',500000,'transferencia','TRF-001','Banco Estado','Fondos para insumos faena marzo'],
        [personas[0]?.persona_id,'2026-03-15',300000,'transferencia','TRF-012','Banco Estado','Reposición fondos'],
        [personas[1]?.persona_id,'2026-03-05',400000,'transferencia','TRF-003','Banco Chile','Fondos operación marzo'],
        [personas[1]?.persona_id,'2026-04-01',350000,'transferencia','TRF-025','Banco Chile','Fondos abril'],
        [personas[2]?.persona_id,'2026-03-10',250000,'transferencia','TRF-007','Banco Estado','Compras repuestos urgentes'],
        [personas[2]?.persona_id,'2026-04-05',200000,'transferencia','TRF-030','Banco Estado','Fondos mantención abril'],
        [personas[3]?.persona_id,'2026-03-20',150000,'efectivo',null,null,'Viáticos traslado máquina'],
        [personas[4]?.persona_id,'2026-04-01',180000,'transferencia','TRF-028','Banco Estado','Fondos repuestos menores']
      ];
      for(const[pid,fecha,monto,medio,nop,banco,obs] of entregas){
        if(!pid) continue;
        await pool.query('INSERT INTO rend_entregas(persona_id,empresa_id,fecha,monto,medio_pago,numero_operacion,banco,observaciones,usuario) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)',
          [pid,empId,fecha,monto,medio,nop,banco,obs,'sistema']);
      }

      // Gastos rendidos
      const gastos=[
        // Ricardo Riveros - gastos de faena
        [personas[0]?.persona_id,'2026-03-03','colacion','Almuerzo cuadrilla faena Mec 4 (8 personas)',64000,true,'boleta','B-4521',faenaIds[0]],
        [personas[0]?.persona_id,'2026-03-05','peaje','Peaje ruta Los Ángeles - Nacimiento ida y vuelta',7200,true,'voucher','V-889',null],
        [personas[0]?.persona_id,'2026-03-07','insumo','Cadena motosierra Oregon 72DPX',32500,true,'boleta','B-4588',faenaIds[0]],
        [personas[0]?.persona_id,'2026-03-10','colacion','Almuerzo equipo mantención en faena',48000,true,'boleta','B-4612',faenaIds[0]],
        [personas[0]?.persona_id,'2026-03-12','combustible','Bencina camioneta traslado insumos 60L',55800,true,'boleta','B-991',null],
        [personas[0]?.persona_id,'2026-03-15','otros','Estacionamiento terminal Los Ángeles 2 días',12000,false,'sin_respaldo',null,null],
        [personas[0]?.persona_id,'2026-03-18','insumo','Aceite hidráulico Shell Tellus 68 (20L)',38900,true,'factura','F-12045',faenaIds[0]],
        [personas[0]?.persona_id,'2026-03-22','colacion','Colación operadores turno noche',32000,true,'boleta','B-4690',faenaIds[1]],
        [personas[0]?.persona_id,'2026-03-25','peaje','Peajes semana 25-28 marzo',14400,true,'voucher','V-923',null],
        [personas[0]?.persona_id,'2026-03-28','herramienta','Llave de torque 3/4" Stanley',45000,true,'boleta','B-2210',null],
        // Sebastián Poo - gastos administrativos y operación
        [personas[1]?.persona_id,'2026-03-08','colacion','Reunión con cliente en terreno',18500,true,'boleta','B-1122',null],
        [personas[1]?.persona_id,'2026-03-12','combustible','Bencina camioneta supervisión 45L',41850,true,'boleta','B-993',null],
        [personas[1]?.persona_id,'2026-03-15','peaje','Peajes ruta Concepción ida/vuelta',9600,true,'voucher','V-901',null],
        [personas[1]?.persona_id,'2026-03-18','materiales','EPP: guantes, lentes, protector auditivo x5',67500,true,'factura','F-8834',null],
        [personas[1]?.persona_id,'2026-03-22','alojamiento','Hospedaje Concepción reunión proveedor',45000,true,'boleta','B-7712',null],
        [personas[1]?.persona_id,'2026-03-25','otros','Propina carguío materiales',5000,false,'sin_respaldo',null,faenaIds[0]],
        [personas[1]?.persona_id,'2026-04-02','colacion','Almuerzo equipo planificación abril',28000,true,'boleta','B-1205',null],
        [personas[1]?.persona_id,'2026-04-05','combustible','Diésel generador faena 100L',92000,true,'factura','F-9001',faenaIds[1]],
        // Luis Sáez - gastos mantención
        [personas[2]?.persona_id,'2026-03-12','insumo','Filtro hidráulico Komatsu 20Y-60-21470',28500,true,'factura','F-5523',null],
        [personas[2]?.persona_id,'2026-03-15','insumo','Sellos O-ring kit reparación cilindro',15200,true,'boleta','B-3301',null],
        [personas[2]?.persona_id,'2026-03-18','colacion','Almuerzo mecánicos faena Mec 3',24000,true,'boleta','B-3320',faenaIds[1]],
        [personas[2]?.persona_id,'2026-03-22','herramienta','Juego dados impacto 3/4" 8 piezas',35000,true,'boleta','B-2215',null],
        [personas[2]?.persona_id,'2026-03-28','insumo','Grasa EP2 balde 18kg',28500,true,'boleta','B-3401',null],
        [personas[2]?.persona_id,'2026-04-05','combustible','Bencina camioneta taller 50L',46500,true,'boleta','B-1001',null],
        [personas[2]?.persona_id,'2026-04-08','insumo','Manguera hidráulica 1/2" x 3m con terminales',42000,true,'factura','F-5590',null],
        // Alejandro Sepúlveda
        [personas[3]?.persona_id,'2026-03-22','colacion','Almuerzo traslado máquina',12000,true,'boleta','B-8810',null],
        [personas[3]?.persona_id,'2026-03-23','peaje','Peajes traslado máquina ruta 5',4800,true,'voucher','V-950',null],
        [personas[3]?.persona_id,'2026-03-25','combustible','Bencina camioneta acompañamiento cama baja',37200,true,'boleta','B-995',null],
        [personas[3]?.persona_id,'2026-03-28','otros','Estacionamiento hospital (trámite licencia)',3000,false,'sin_respaldo',null,null],
        // Juan Paulo Silva
        [personas[4]?.persona_id,'2026-04-02','insumo','Teflón, silicona, abrazaderas surtidas',8500,true,'boleta','B-4401',null],
        [personas[4]?.persona_id,'2026-04-03','colacion','Almuerzo reparación en terreno',8000,true,'boleta','B-4410',faenaIds[0]],
        [personas[4]?.persona_id,'2026-04-05','insumo','Fusibles y relés surtidos para excavadora',12500,true,'boleta','B-4425',null],
        [personas[4]?.persona_id,'2026-04-07','combustible','Bencina camioneta repuestos 40L',37200,true,'boleta','B-1005',null]
      ];
      let gLoaded=0;
      for(const[pid,fecha,tipo,desc,monto,resp,tipoResp,ndoc,fid] of gastos){
        if(!pid) continue;
        const est=Math.random()>0.3?'aprobado':'pendiente';
        await pool.query('INSERT INTO rend_gastos(persona_id,empresa_id,fecha_gasto,tipo_gasto,descripcion,monto,tiene_respaldo,tipo_respaldo,numero_documento,faena_id,estado,usuario) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)',
          [pid,empId,fecha,tipo,desc,monto,resp,tipoResp,ndoc,fid||null,est,'sistema']);
        gLoaded++;
      }
      console.log('  [OK] Rendiciones: '+entregas.length+' entregas, '+gLoaded+' gastos cargados');
    }
  }catch(e){console.log('[WARN] seed rendiciones:',e.message);}

  // ── Seed cuentas bancarias ──
  try{
    const bc=await pool.query('SELECT COUNT(*) FROM fin_cuentas_bancarias');
    if(parseInt(bc.rows[0].count)===0){
      const empLPZ=await pool.query("SELECT empresa_id FROM empresas WHERE LOWER(razon_social) LIKE '%leonidas%poo%' LIMIT 1");
      const empEMP=await pool.query("SELECT empresa_id FROM empresas WHERE LOWER(razon_social) LIKE '%emprecon%' LIMIT 1");
      const lpzId=empLPZ.rows.length?empLPZ.rows[0].empresa_id:null;
      const empId=empEMP.rows.length?empEMP.rows[0].empresa_id:null;
      if(lpzId){
        const cuentasLPZ=[
          ['Banco BICE','corriente','21-00123-01'],
          ['Banco Santander','corriente','0-071-00456-7'],
          ['Banco Itaú','corriente','0220078901'],
          ['Scotiabank','corriente','97-01234-05'],
          ['Banco de Chile','corriente','00-123-45678-09']
        ];
        for(const[banco,tipo,num] of cuentasLPZ){
          await pool.query('INSERT INTO fin_cuentas_bancarias(empresa_id,banco,tipo_cuenta,numero_cuenta) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING',[lpzId,banco,tipo,num]);
        }
      }
      if(empId){
        const cuentasEMP=[
          ['Banco Santander','corriente','0-071-00789-3'],
          ['Banco Itaú','corriente','0220098765'],
          ['Scotiabank','corriente','97-05678-02']
        ];
        for(const[banco,tipo,num] of cuentasEMP){
          await pool.query('INSERT INTO fin_cuentas_bancarias(empresa_id,banco,tipo_cuenta,numero_cuenta) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING',[empId,banco,tipo,num]);
        }
      }
      console.log('  [OK] Cuentas bancarias cargadas');
    }
  }catch(e){console.log('[WARN] seed cuentas bancarias:',e.message);}
}

async function insertarDatosIniciales(client) {
  await client.query(`INSERT INTO tipos_documento(codigo,nombre) VALUES
    ('33','Factura Electronica'),
    ('34','Factura No Afecta o Exenta Electronica'),
    ('39','Boleta Electronica'),
    ('41','Boleta Exenta Electronica'),
    ('43','Liquidacion Factura Electronica'),
    ('46','Factura de Compra Electronica'),
    ('52','Guia de Despacho Electronica'),
    ('56','Nota de Debito Electronica'),
    ('61','Nota de Credito Electronica'),
    ('110','Factura de Exportacion Electronica'),
    ('111','Nota de Debito de Exportacion Electronica'),
    ('112','Nota de Credito de Exportacion Electronica')
    ON CONFLICT(codigo) DO UPDATE SET nombre=EXCLUDED.nombre`);
  await client.query(`INSERT INTO motivos_movimiento(nombre,tipo) VALUES('Mantencion Correctiva','SALIDA'),('Mantencion Preventiva','SALIDA'),('Consumo Operacional','SALIDA'),('Consumo Taller','SALIDA'),('Perdida / Merma','AJUSTE'),('Diferencia Inventario Fisico','AJUSTE'),('Ajuste de Apertura','AJUSTE') ON CONFLICT DO NOTHING`);
  await client.query(`INSERT INTO bodegas(codigo,nombre,ubicacion,responsable) VALUES('BC','Bodega Central','Planta Principal','Juan Perez'),('BT','Bodega Taller','Taller Central','Pedro Gonzalez'),('BF3','Bodega Faena Mec 3','Faena Mecanica 3','Luis Torres'),('BL','Bodega Lubricantes','Planta Principal','Carlos Munoz') ON CONFLICT DO NOTHING`);
  await client.query(`INSERT INTO categorias(nombre) VALUES('Repuestos'),('Insumos'),('Lubricantes'),('Herramientas'),('Consumibles') ON CONFLICT DO NOTHING`);
  await client.query(`INSERT INTO subcategorias(categoria_id,nombre) SELECT id,sc FROM (VALUES(1,'Filtros'),(1,'Sellos y Retenes'),(1,'Rodamientos'),(1,'Mangueras'),(2,'Soldaduras'),(2,'Discos de Corte'),(2,'Abrasivos'),(3,'Aceite Hidraulico'),(3,'Aceite de Motor'),(3,'Grasas'),(3,'Refrigerantes'),(4,'Herramientas Manuales'),(5,'Elementos de Limpieza'))AS t(id,sc) JOIN categorias c ON c.categoria_id=t.id ON CONFLICT DO NOTHING`);
  await client.query(`INSERT INTO proveedores(rut,nombre,contacto,telefono) VALUES('76.543.210-5','Comercial Hidraulica Sur Ltda.','Roberto Araya','+56 9 1234 5678'),('76.111.222-3','Lubricantes y Filtros del Sur S.A.','Ana Morales','+56 9 8765 4321'),('77.333.444-1','Ferreteria Industrial Los Angeles','Miguel Castro','+56 43 234 5678'),('76.888.999-0','Distribuidora Tecnica Sur','Sandra Lopez','+56 9 5555 1234') ON CONFLICT DO NOTHING`);
  await client.query(`INSERT INTO faenas(codigo,nombre,descripcion) VALUES('FAE-MEC3','Faena Mec 3','Cosecha mecanizada sector 3'),('FAE-MEC4','Faena Mec 4','Cosecha mecanizada sector 4'),('FAE-MEC5','Faena Mec 5','Cosecha mecanizada sector 5'),('TALL','Taller Central','Taller central de mantencion') ON CONFLICT DO NOTHING`);
  await client.query(`INSERT INTO equipos(codigo,nombre,tipo,faena_id) SELECT cod,nom,tip,f.faena_id FROM(VALUES('HARV-01','Harvester 01','Cosechador','FAE-MEC3'),('HARV-02','Harvester 02','Cosechador','FAE-MEC4'),('SKID-11','Skidder 11','Arrastrador','FAE-MEC3'),('PROC-11','Procesadora 11','Procesador','FAE-MEC3'),('EXC-PC210','Excavadora PC210','Excavadora','TALL'),('CAM-LUB','Camion Lubricador','Camion','TALL'),('TALL-GEN','Taller Central','Taller','TALL'))AS t(cod,nom,tip,fcod) JOIN faenas f ON f.codigo=t.fcod ON CONFLICT DO NOTHING`);
  await client.query(`INSERT INTO productos(codigo,nombre,subcategoria_id,unidad_medida,stock_minimo,costo_referencia) SELECT cod,nom,sc.subcategoria_id,um,smin::numeric,cref::numeric FROM(VALUES('FLTR-HID-001','Filtro Hidraulico 90L','Filtros','UN',3,38500),('FLTR-MOT-002','Filtro Aceite Motor D6E','Filtros','UN',4,24900),('FLTR-AIR-003','Filtro Aire Primario','Filtros','UN',2,45000),('SELL-ORB-001','Kit Sellos Orbitrol','Sellos y Retenes','KIT',2,67800),('ROD-SKF-6205','Rodamiento SKF 6205','Rodamientos','UN',5,18500),('MANG-HID-3/4','Manguera Hidraulica 3/4','Mangueras','MT',10,8900),('ACE-HID-68-20','Aceite Hidraulico ISO 68 (20L)','Aceite Hidraulico','BID',5,42000),('ACE-MOT-15W40','Aceite Motor 15W-40 (20L)','Aceite de Motor','BID',8,38000),('GRAS-EP2-18KG','Grasa Litio EP-2 (18kg)','Grasas','BAL',3,28500),('REFR-DEX-5L','Refrigerante DexCool (5L)','Refrigerantes','GL',6,12500),('DISC-COR-4.5','Disco de Corte 4.5','Discos de Corte','UN',20,2200),('SOLD-E6011-KG','Electrodos E6011 1/8 (kg)','Soldaduras','KG',10,4800))AS t(cod,nom,scnom,um,smin,cref) JOIN subcategorias sc ON sc.nombre=t.scnom ON CONFLICT DO NOTHING`);
  await client.query(`WITH m1 AS(INSERT INTO movimiento_encabezado(tipo_movimiento,fecha,bodega_id,proveedor_id,tipo_doc_id,numero_documento,fecha_documento,responsable_recepcion,usuario) SELECT 'INGRESO','2025-01-10',b.bodega_id,p.proveedor_id,td.tipo_doc_id,'00045312','2025-01-10','Juan Perez','sistema' FROM bodegas b,proveedores p,tipos_documento td WHERE b.codigo='BC' AND p.rut='76.543.210-5' AND td.codigo='33' RETURNING movimiento_id) INSERT INTO movimiento_detalle(movimiento_id,producto_id,cantidad,costo_unitario) SELECT m.movimiento_id,p.producto_id,qty,cu FROM m1 m CROSS JOIN(VALUES('FLTR-HID-001',6,38500),('FLTR-MOT-002',8,24900),('SELL-ORB-001',3,67800),('ROD-SKF-6205',10,18500))AS d(cod,qty,cu) JOIN productos p ON p.codigo=d.cod`).catch(()=>{});
  await client.query(`WITH m2 AS(INSERT INTO movimiento_encabezado(tipo_movimiento,fecha,bodega_id,proveedor_id,tipo_doc_id,numero_documento,fecha_documento,responsable_recepcion,usuario) SELECT 'INGRESO','2025-01-12',b.bodega_id,p.proveedor_id,td.tipo_doc_id,'00012450','2025-01-12','Juan Perez','sistema' FROM bodegas b,proveedores p,tipos_documento td WHERE b.codigo='BC' AND p.rut='76.111.222-3' AND td.codigo='33' RETURNING movimiento_id) INSERT INTO movimiento_detalle(movimiento_id,producto_id,cantidad,costo_unitario) SELECT m.movimiento_id,p.producto_id,qty,cu FROM m2 m CROSS JOIN(VALUES('ACE-HID-68-20',10,42000),('ACE-MOT-15W40',12,38000),('GRAS-EP2-18KG',4,28500),('REFR-DEX-5L',8,12500))AS d(cod,qty,cu) JOIN productos p ON p.codigo=d.cod`).catch(()=>{});
  await client.query(`INSERT INTO stock_actual(producto_id,bodega_id,cantidad_disponible,costo_promedio_actual) SELECT md.producto_id,me.bodega_id,SUM(md.cantidad),SUM(md.cantidad*md.costo_unitario)/SUM(md.cantidad) FROM movimiento_detalle md JOIN movimiento_encabezado me ON md.movimiento_id=me.movimiento_id WHERE me.tipo_movimiento='INGRESO' AND me.estado='ACTIVO' GROUP BY md.producto_id,me.bodega_id ON CONFLICT(producto_id,bodega_id) DO UPDATE SET cantidad_disponible=EXCLUDED.cantidad_disponible,costo_promedio_actual=EXCLUDED.costo_promedio_actual`).catch(()=>{});
  await client.query(`WITH s1 AS(INSERT INTO movimiento_encabezado(tipo_movimiento,fecha,bodega_id,faena_id,equipo_id,motivo_id,observaciones,responsable_entrega,responsable_recepcion,usuario) SELECT 'SALIDA','2025-01-16',b.bodega_id,f.faena_id,e.equipo_id,m.motivo_id,'Cambio filtros','Juan Perez','Carlos Munoz','sistema' FROM bodegas b,faenas f,equipos e,motivos_movimiento m WHERE b.codigo='BC' AND f.codigo='FAE-MEC3' AND e.codigo='HARV-01' AND m.nombre='Mantencion Correctiva' RETURNING movimiento_id,bodega_id) INSERT INTO movimiento_detalle(movimiento_id,producto_id,cantidad,costo_unitario) SELECT s.movimiento_id,p.producto_id,qty,COALESCE(sa.costo_promedio_actual,p.costo_referencia) FROM s1 s CROSS JOIN(VALUES('FLTR-HID-001',2),('FLTR-MOT-002',2))AS d(cod,qty) JOIN productos p ON p.codigo=d.cod LEFT JOIN stock_actual sa ON sa.producto_id=p.producto_id AND sa.bodega_id=s.bodega_id`).catch(()=>{});
  await client.query(`UPDATE stock_actual sa SET cantidad_disponible=GREATEST(0,sa.cantidad_disponible-COALESCE((SELECT SUM(md.cantidad) FROM movimiento_detalle md JOIN movimiento_encabezado me ON md.movimiento_id=me.movimiento_id WHERE me.tipo_movimiento='SALIDA' AND me.estado='ACTIVO' AND md.producto_id=sa.producto_id AND me.bodega_id=sa.bodega_id),0)),ultima_actualizacion=NOW()`).catch(()=>{});
  console.log('  [OK] Datos iniciales insertados');
}

// ── AUTH ──
app.post('/api/auth/login', async(req,res)=>{
  try{
    const{email,password}=req.body;
    const r=await pool.query('SELECT u.*,ro.nombre AS rol_nombre,ro.modulos,ro.es_admin FROM usuarios u LEFT JOIN roles ro ON u.rol_id=ro.rol_id WHERE (u.email=$1 OR u.username=$1) AND u.activo=true',[email]);
    if(!r.rows.length) return res.status(401).json({error:'Credenciales invalidas'});
    const ok=await bcrypt.compare(password,r.rows[0].password_hash);
    if(!ok) return res.status(401).json({error:'Credenciales invalidas'});
    const u=r.rows[0];
    const modulos=u.modulos||[];
    const esAdmin=u.es_admin||u.rol==='ADMINISTRADOR';
    const token=jwt.sign({id:u.usuario_id,email:u.email,nombre:u.nombre,rol:u.rol_nombre||u.rol,es_admin:esAdmin},JWT_SECRET,{expiresIn:'8h'});
    res.json({token,usuario:{id:u.usuario_id,email:u.email,nombre:u.nombre,rol:u.rol_nombre||u.rol,es_admin:esAdmin,modulos:modulos,empresa_id:u.empresa_id||null,faena_id:u.faena_id||null}});
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

// ══ MODELOS DE EQUIPO ══
app.get('/api/modelos-equipo', auth, async(req,res)=>{
  try{const r=await pool.query('SELECT * FROM modelos_equipo WHERE activo=true ORDER BY marca,modelo');res.json(r.rows);}catch(e){res.status(500).json({error:e.message});}
});
app.post('/api/modelos-equipo', auth, async(req,res)=>{
  try{const{marca,modelo,tipo_maquina,funcion_principal,motor_descripcion,potencia_hp,peso_kg,cap_tanque_combustible,cap_aceite_motor,cap_sist_hidraulico,cap_refrigerante,tipo_transmision,ancho_zapata,observaciones}=req.body;
  const r=await pool.query('INSERT INTO modelos_equipo(marca,modelo,tipo_maquina,funcion_principal,motor_descripcion,potencia_hp,peso_kg,cap_tanque_combustible,cap_aceite_motor,cap_sist_hidraulico,cap_refrigerante,tipo_transmision,ancho_zapata,observaciones) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) ON CONFLICT(marca,modelo) DO UPDATE SET tipo_maquina=EXCLUDED.tipo_maquina,funcion_principal=EXCLUDED.funcion_principal,motor_descripcion=EXCLUDED.motor_descripcion,potencia_hp=EXCLUDED.potencia_hp,peso_kg=EXCLUDED.peso_kg,cap_tanque_combustible=EXCLUDED.cap_tanque_combustible,cap_aceite_motor=EXCLUDED.cap_aceite_motor,cap_sist_hidraulico=EXCLUDED.cap_sist_hidraulico,cap_refrigerante=EXCLUDED.cap_refrigerante,tipo_transmision=EXCLUDED.tipo_transmision,ancho_zapata=EXCLUDED.ancho_zapata,observaciones=EXCLUDED.observaciones RETURNING *',
    [marca,modelo,tipo_maquina||null,funcion_principal||null,motor_descripcion||null,potencia_hp||null,peso_kg||null,cap_tanque_combustible||null,cap_aceite_motor||null,cap_sist_hidraulico||null,cap_refrigerante||null,tipo_transmision||null,ancho_zapata||null,observaciones||null]);
  res.status(201).json(r.rows[0]);}catch(e){res.status(400).json({error:e.message});}
});
app.put('/api/modelos-equipo/:id', auth, async(req,res)=>{
  try{const{marca,modelo,tipo_maquina,funcion_principal,motor_descripcion,potencia_hp,peso_kg,cap_tanque_combustible,cap_aceite_motor,cap_sist_hidraulico,cap_refrigerante,tipo_transmision,ancho_zapata,observaciones,activo}=req.body;
  const r=await pool.query('UPDATE modelos_equipo SET marca=$1,modelo=$2,tipo_maquina=$3,funcion_principal=$4,motor_descripcion=$5,potencia_hp=$6,peso_kg=$7,cap_tanque_combustible=$8,cap_aceite_motor=$9,cap_sist_hidraulico=$10,cap_refrigerante=$11,tipo_transmision=$12,ancho_zapata=$13,observaciones=$14,activo=$15 WHERE modelo_id=$16 RETURNING *',
    [marca,modelo,tipo_maquina||null,funcion_principal||null,motor_descripcion||null,potencia_hp||null,peso_kg||null,cap_tanque_combustible||null,cap_aceite_motor||null,cap_sist_hidraulico||null,cap_refrigerante||null,tipo_transmision||null,ancho_zapata||null,observaciones||null,activo!==false,req.params.id]);
  res.json(r.rows[0]);}catch(e){res.status(400).json({error:e.message});}
});
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
    const{rut,razon_social,direccion,ciudad,giro,telefono,email,representante_nombre,representante_rut,comuna,region,firma_representante,timbre_empresa}=req.body;
    const r=await pool.query('INSERT INTO empresas(rut,razon_social,direccion,ciudad,giro,telefono,email,logo_base64,representante_nombre,representante_rut,comuna,region,firma_representante,timbre_empresa) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) RETURNING *',[rut,razon_social,direccion||null,ciudad||null,giro||null,telefono||null,email||null,req.body.logo_base64||null,representante_nombre||null,representante_rut||null,comuna||null,region||'VIII del Bio Bio',firma_representante||null,timbre_empresa||null]);
    res.status(201).json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
empR.put('/:id', auth, async(req,res)=>{
  try{
    const{rut,razon_social,direccion,ciudad,giro,telefono,email,representante_nombre,representante_rut,comuna,region,firma_representante,timbre_empresa}=req.body;
    const r=await pool.query('UPDATE empresas SET rut=$1,razon_social=$2,direccion=$3,ciudad=$4,giro=$5,telefono=$6,email=$7,logo_base64=$8,representante_nombre=$9,representante_rut=$10,comuna=$11,region=$12,firma_representante=$13,timbre_empresa=$14,modificado_en=NOW() WHERE empresa_id=$15 RETURNING *',[rut,razon_social,direccion||null,ciudad||null,giro||null,telefono||null,email||null,req.body.logo_base64||null,representante_nombre||null,representante_rut||null,comuna||null,region||'VIII del Bio Bio',firma_representante||null,timbre_empresa||null,req.params.id]);
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
    const r=await pool.query('SELECT e.*,f.nombre AS faena_nombre,emp.razon_social AS empresa_nombre,m.marca AS modelo_marca,m.modelo AS modelo_nombre,m.tipo_maquina AS modelo_tipo,m.motor_descripcion AS modelo_motor,m.potencia_hp AS modelo_hp FROM equipos e LEFT JOIN faenas f ON e.faena_id=f.faena_id LEFT JOIN empresas emp ON e.empresa_id=emp.empresa_id LEFT JOIN modelos_equipo m ON e.modelo_id=m.modelo_id ORDER BY emp.razon_social NULLS LAST,e.codigo');
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});
eqR.post('/', auth, async(req,res)=>{
  try{
    const{codigo,nombre,tipo,faena_id,patente_serie,marca,modelo,anio,placa_patente,num_chasis,tipo_cargo,modelo_id,contacto_terreno,chasis,horas_productivas_dia}=req.body;
    const empresa_id=await resolveEmpresaId(req.body.empresa_id);
    const r=await pool.query('INSERT INTO equipos(codigo,nombre,tipo,faena_id,patente_serie,marca,modelo,anio,placa_patente,num_chasis,empresa_id,tipo_cargo,modelo_id,contacto_terreno,horas_productivas_dia) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) RETURNING *',[codigo,nombre,tipo||null,faena_id||null,patente_serie||null,marca||null,modelo||null,anio||null,placa_patente||null,chasis||num_chasis||null,empresa_id,tipo_cargo||'maquinaria',modelo_id||null,contacto_terreno||null,parseFloat(horas_productivas_dia)||12]);
    res.status(201).json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
eqR.put('/:id', auth, async(req,res)=>{
  try{
    const{codigo,nombre,tipo,faena_id,patente_serie,marca,modelo,anio,placa_patente,num_chasis,tipo_cargo,modelo_id,contacto_terreno,chasis,horas_productivas_dia}=req.body;
    const empresa_id=await resolveEmpresaId(req.body.empresa_id);
    const r=await pool.query('UPDATE equipos SET codigo=$1,nombre=$2,tipo=$3,faena_id=$4,patente_serie=$5,marca=$6,modelo=$7,anio=$8,placa_patente=$9,num_chasis=$10,empresa_id=$11,tipo_cargo=$12,modelo_id=$13,contacto_terreno=$14,horas_productivas_dia=$15 WHERE equipo_id=$16 RETURNING *',[codigo,nombre,tipo||null,faena_id||null,patente_serie||null,marca||null,modelo||null,anio||null,placa_patente||null,chasis||num_chasis||null,empresa_id,tipo_cargo||'maquinaria',modelo_id||null,contacto_terreno||null,parseFloat(horas_productivas_dia)||12,req.params.id]);
    res.json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
eqR.patch('/:id/activo', auth, async(req,res)=>{try{res.json((await pool.query('UPDATE equipos SET activo=NOT activo WHERE equipo_id=$1 RETURNING *',[req.params.id])).rows[0]);}catch(e){res.status(400).json({error:e.message});}});
eqR.delete('/:id', auth, async(req,res)=>{
  try{await pool.query('DELETE FROM equipos WHERE equipo_id=$1',[req.params.id]);res.json({ok:true});}
  catch(e){if(e.code==='23503')return res.status(409).json({error:'No se puede eliminar: equipo en uso.'});res.status(400).json({error:e.message});}
});
app.use('/api/equipos', eqR);

// Importación masiva de equipos con validación por nombre
app.post('/api/import/equipos', auth, async(req,res)=>{
  try{
    const items=Array.isArray(req.body)?req.body:[];
    if(!items.length)return res.status(400).json({error:'Array vacío'});
    // Pre-cargar empresas y faenas
    const emps=(await pool.query('SELECT empresa_id,razon_social,rut FROM empresas')).rows;
    const faenas=(await pool.query('SELECT faena_id,codigo,nombre FROM faenas')).rows;
    const equiposExist=(await pool.query('SELECT equipo_id,codigo,nombre,empresa_id,faena_id FROM equipos')).rows;
    function norm(s){return (s||'').toString().toUpperCase().trim().replace(/\s+/g,' ').replace(/[.,]/g,'');}
    function findEmpresa(n){
      if(!n)return null;
      var nu=norm(n);
      for(var e of emps){
        var r=norm(e.razon_social);
        if(r===nu||r.indexOf(nu)>=0||nu.indexOf(r)>=0)return e.empresa_id;
      }
      return null;
    }
    function findFaena(n){
      if(!n)return null;
      var nu=norm(n);
      for(var f of faenas){
        if(norm(f.nombre)===nu||norm(f.codigo)===nu)return f.faena_id;
      }
      // Match parcial
      for(var f of faenas){
        if(norm(f.nombre).indexOf(nu)>=0||nu.indexOf(norm(f.nombre))>=0)return f.faena_id;
      }
      return null;
    }
    function findEquipo(nombre){
      if(!nombre)return null;
      var nu=norm(nombre);
      for(var eq of equiposExist){
        if(norm(eq.nombre)===nu||norm(eq.codigo)===nu)return eq;
      }
      return null;
    }
    function genCodigo(nombre){
      var base=norm(nombre).replace(/[^A-Z0-9]/g,'-').replace(/-+/g,'-').replace(/^-|-$/g,'').slice(0,25);
      return base||'EQ-'+Date.now();
    }
    const results=[];
    for(const item of items){
      const nombre=(item.nombre||'').trim();
      if(!nombre){results.push({nombre:'(vacío)',ok:false,error:'Sin nombre'});continue;}
      try{
        const empresa_id=findEmpresa(item.empresa);
        const faena_id=findFaena(item.faena);
        const existente=findEquipo(nombre);
        const tipo=item.tipo||null;
        const tipoCat=item.categoria||null; // Maquinaria/Vehículo/etc.
        if(existente){
          // Actualizar empresa y faena si cambiaron
          await pool.query('UPDATE equipos SET empresa_id=COALESCE($1,empresa_id),faena_id=COALESCE($2,faena_id),tipo=COALESCE($3,tipo),modificado_en=NOW() WHERE equipo_id=$4',[empresa_id,faena_id,tipo,existente.equipo_id]);
          results.push({nombre:nombre,codigo:existente.codigo,ok:true,accion:'actualizado',empresa:empresa_id?'ok':'sin mapeo',faena:faena_id?'ok':'sin mapeo'});
        } else {
          // Crear nuevo
          let codigo=item.codigo||genCodigo(nombre);
          // Verificar que el código no exista
          let suf=1;const base=codigo;
          while(equiposExist.find(function(e){return (e.codigo||'').toUpperCase()===codigo.toUpperCase();})){
            codigo=base+'-'+suf;suf++;
          }
          const r=await pool.query('INSERT INTO equipos(codigo,nombre,tipo,faena_id,empresa_id,tipo_cargo,activo) VALUES($1,$2,$3,$4,$5,$6,true) RETURNING equipo_id,codigo',
            [codigo,nombre,tipo,faena_id,empresa_id,tipoCat&&tipoCat.toLowerCase().indexOf('centro')>=0?'cargo':'maquinaria']);
          equiposExist.push({equipo_id:r.rows[0].equipo_id,codigo:r.rows[0].codigo,nombre:nombre,empresa_id:empresa_id,faena_id:faena_id});
          results.push({nombre:nombre,codigo:r.rows[0].codigo,ok:true,accion:'creado',empresa:empresa_id?'ok':'sin mapeo',faena:faena_id?'ok':'sin mapeo'});
        }
      }catch(e){results.push({nombre:nombre,ok:false,error:e.message});}
    }
    const creados=results.filter(function(r){return r.accion==='creado';}).length;
    const actualizados=results.filter(function(r){return r.accion==='actualizado';}).length;
    const errores=results.filter(function(r){return !r.ok;}).length;
    const sinEmpresa=results.filter(function(r){return r.empresa==='sin mapeo';}).length;
    const sinFaena=results.filter(function(r){return r.faena==='sin mapeo';}).length;
    res.json({results,resumen:{creados,actualizados,errores,sin_empresa:sinEmpresa,sin_faena:sinFaena,total:items.length}});
  }catch(e){res.status(400).json({error:e.message});}
});

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
    const r=await pool.query(`SELECT me.*,b.nombre AS bodega_nombre,f.nombre AS faena_nombre,e.nombre AS equipo_nombre,pr.nombre AS proveedor_nombre,td.nombre AS tipo_doc_nombre,mot.nombre AS motivo_nombre,(SELECT SUM(md.costo_total) FROM movimiento_detalle md WHERE md.movimiento_id=me.movimiento_id) AS total,(SELECT SUM(md.costo_total) FROM movimiento_detalle md WHERE md.movimiento_id=me.movimiento_id) AS total_ingreso,(SELECT COUNT(*) FROM movimiento_detalle md WHERE md.movimiento_id=me.movimiento_id) AS num_lineas,(SELECT string_agg(DISTINCT sc.nombre, ', ') FROM movimiento_detalle md JOIN productos p ON md.producto_id=p.producto_id LEFT JOIN subcategorias sc ON p.subcategoria_id=sc.subcategoria_id WHERE md.movimiento_id=me.movimiento_id) AS subcategorias,(SELECT string_agg(p.nombre||' ('||md.cantidad||')', ', ' ORDER BY md.detalle_id) FROM movimiento_detalle md JOIN productos p ON md.producto_id=p.producto_id WHERE md.movimiento_id=me.movimiento_id) AS detalle_productos FROM movimiento_encabezado me LEFT JOIN bodegas b ON me.bodega_id=b.bodega_id LEFT JOIN faenas f ON me.faena_id=f.faena_id LEFT JOIN equipos e ON me.equipo_id=e.equipo_id LEFT JOIN proveedores pr ON me.proveedor_id=pr.proveedor_id LEFT JOIN tipos_documento td ON me.tipo_doc_id=td.tipo_doc_id LEFT JOIN motivos_movimiento mot ON me.motivo_id=mot.motivo_id WHERE ${where.join(' AND ')} ORDER BY me.movimiento_id DESC`,vals);
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
      await client.query('INSERT INTO movimiento_detalle(movimiento_id,producto_id,cantidad,costo_unitario,ot_id) VALUES($1,$2,$3,$4,$5)',[movId,pid,qty,cu,l.ot_id||null]);
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
    const netoAfecto=lineas.filter(l=>!l.exenta).reduce((s,l)=>s+(parseFloat(l.cantidad)||0)*(parseFloat(l.precio_unitario)||0),0);
    const netoExento=lineas.filter(l=>l.exenta).reduce((s,l)=>s+(parseFloat(l.cantidad)||0)*(parseFloat(l.precio_unitario)||0),0);
    const neto=Math.round(netoAfecto+netoExento);
    const iva=Math.round(netoAfecto*0.19);
    const imp=Math.round(parseFloat(impuesto_adicional)||0);
    const total=neto+iva+imp;
    const ocR2=await client.query('INSERT INTO ordenes_compra(numero_oc,empresa_id,proveedor_id,fecha_emision,solicitante,retira,condicion_id,impuesto_adicional,neto,iva,total,observaciones,usuario) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) RETURNING oc_id',[numero_oc,empresa_id||null,proveedor_id,fecha_emision,solicitante||null,retira||null,condicion_id||null,imp,neto,iva,total,observaciones||null,req.user.email]);
    const ocId=ocR2.rows[0].oc_id;
    for(let i=0;i<lineas.length;i++){const l=lineas[i];await client.query('INSERT INTO ordenes_compra_detalle(oc_id,linea_num,descripcion,producto_id,subcategoria_id,faena_id,equipo_id,cantidad,precio_unitario,ingresa_bodega,bodega_destino_id,exenta) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)',[ocId,i+1,l.descripcion||null,l.producto_id||null,l.subcategoria_id||null,l.faena_id||null,l.equipo_id||null,parseFloat(l.cantidad)||0,parseFloat(l.precio_unitario)||0,l.ingresa_bodega||false,l.bodega_destino_id||null,l.exenta||false]);}
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
    const netoAfecto=lineas.filter(l=>!l.exenta).reduce((s,l)=>s+(parseFloat(l.cantidad)||0)*(parseFloat(l.precio_unitario)||0),0);
    const netoExento=lineas.filter(l=>l.exenta).reduce((s,l)=>s+(parseFloat(l.cantidad)||0)*(parseFloat(l.precio_unitario)||0),0);
    const neto=Math.round(netoAfecto+netoExento);const iva=Math.round(netoAfecto*0.19);const imp=Math.round(parseFloat(impuesto_adicional)||0);const total=neto+iva+imp;
    await client.query('UPDATE ordenes_compra SET empresa_id=$1,proveedor_id=$2,fecha_emision=$3,solicitante=$4,retira=$5,condicion_id=$6,impuesto_adicional=$7,neto=$8,iva=$9,total=$10,observaciones=$11,modificado_en=NOW() WHERE oc_id=$12',[empresa_id||null,proveedor_id,fecha_emision,solicitante||null,retira||null,condicion_id||null,imp,neto,iva,total,observaciones||null,req.params.id]);
    await client.query('DELETE FROM ordenes_compra_detalle WHERE oc_id=$1',[req.params.id]);
    for(let i=0;i<lineas.length;i++){const l=lineas[i];await client.query('INSERT INTO ordenes_compra_detalle(oc_id,linea_num,descripcion,producto_id,subcategoria_id,faena_id,equipo_id,cantidad,precio_unitario,ingresa_bodega,bodega_destino_id,exenta) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)',[req.params.id,i+1,l.descripcion||null,l.producto_id||null,l.subcategoria_id||null,l.faena_id||null,l.equipo_id||null,parseFloat(l.cantidad)||0,parseFloat(l.precio_unitario)||0,l.ingresa_bodega||false,l.bodega_destino_id||null,l.exenta||false]);}
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
  const client=await pool.connect();
  try{
    await client.query('BEGIN');
    const chk=await client.query('SELECT * FROM ordenes_compra WHERE oc_id=$1',[req.params.id]);
    if(!chk.rows.length) return res.status(404).json({error:'OC no encontrada'});
    const oc=chk.rows[0];
    if(oc.estado==='ANULADA') return res.status(400).json({error:'La OC ya esta anulada'});
    // Si tiene movimiento de inventario, no se puede anular desde aquí
    if(oc.movimiento_id) return res.status(400).json({error:'No se puede anular: ya se recibieron productos en bodega. Anule el movimiento primero.'});
    // Revertir movimientos de combustible asociados
    if(oc.recibido_en||oc.numero_oc){
      const combMovs=await client.query("SELECT * FROM comb_movimientos WHERE oc_referencia=$1 AND estado='ACTIVO'",[oc.numero_oc]);
      for(const mv of combMovs.rows){
        // Restar del stock
        await client.query('UPDATE comb_stock SET litros_disponibles=GREATEST(0,litros_disponibles-$1),ultima_actualizacion=NOW() WHERE estanque_id=$2 AND tipo_id=$3',[parseFloat(mv.litros),mv.estanque_destino_id,mv.tipo_id]);
        // Marcar movimiento como anulado
        await client.query("UPDATE comb_movimientos SET estado='ANULADO',anulado_en=NOW(),anulado_por=$1,motivo_anulacion='OC anulada' WHERE mov_id=$2",[req.user.email,mv.mov_id]);
      }
    }
    await client.query("UPDATE ordenes_compra SET estado='ANULADA',anulado_en=NOW(),anulado_por=$1,recibido_en=NULL WHERE oc_id=$2",[req.user.email,req.params.id]);
    await client.query('COMMIT');
    res.json({ok:true});
  }catch(e){await client.query('ROLLBACK');res.status(400).json({error:e.message});}
  finally{client.release();}
});
ocR.delete('/:id', auth, async(req,res)=>{
  const client=await pool.connect();
  try{
    await client.query('BEGIN');
    const chk=await client.query('SELECT * FROM ordenes_compra WHERE oc_id=$1',[req.params.id]);
    if(!chk.rows.length) return res.status(404).json({error:'OC no encontrada'});
    const oc=chk.rows[0];
    // Solo admin puede eliminar OCs cerradas/recibidas
    if((oc.estado==='CERRADA'||oc.movimiento_id||oc.recibido_en)&&req.user.rol!=='ADMINISTRADOR'&&req.user.rol!=='Administrador'){
      return res.status(403).json({error:'Solo administradores pueden eliminar OCs cerradas o recibidas.'});
    }
    // 1) Revertir movimientos de inventario (bodega) si los hay
    if(oc.movimiento_id){
      const dets=await client.query('SELECT * FROM movimiento_detalle WHERE movimiento_id=$1',[oc.movimiento_id]);
      const enc=await client.query('SELECT * FROM movimiento_encabezado WHERE movimiento_id=$1',[oc.movimiento_id]);
      const bodegaId=enc.rows.length?enc.rows[0].bodega_id:null;
      for(const d of dets.rows){
        const pid=d.producto_id,qty=parseFloat(d.cantidad)||0,cu=parseFloat(d.costo_unitario)||0;
        // Buscar la bodega destino (puede ser la de la línea o la del encabezado)
        const lineaDet=await client.query('SELECT bodega_destino_id FROM ordenes_compra_detalle WHERE oc_id=$1 AND producto_id=$2 LIMIT 1',[req.params.id,pid]);
        const bodDest=(lineaDet.rows[0]||{}).bodega_destino_id||bodegaId;
        if(bodDest){
          // Restar del stock (puede quedar negativo si ya se distribuyó)
          await client.query('UPDATE stock_actual SET cantidad_disponible=cantidad_disponible-$1,ultima_actualizacion=NOW() WHERE producto_id=$2 AND bodega_id=$3',[qty,pid,bodDest]);
        }
      }
      await client.query('DELETE FROM movimiento_detalle WHERE movimiento_id=$1',[oc.movimiento_id]);
      await client.query('DELETE FROM movimiento_encabezado WHERE movimiento_id=$1',[oc.movimiento_id]);
    }
    // 2) Revertir movimientos de combustible si hay
    if(oc.recibido_en||oc.numero_oc){
      const combMovs=await client.query("SELECT * FROM comb_movimientos WHERE oc_referencia=$1 AND estado='ACTIVO'",[oc.numero_oc]);
      for(const mv of combMovs.rows){
        // Restar litros del stock del estanque (puede quedar negativo)
        if(mv.estanque_destino_id&&mv.tipo_id){
          await client.query('UPDATE comb_stock SET litros_disponibles=litros_disponibles-$1,ultima_actualizacion=NOW() WHERE estanque_id=$2 AND tipo_id=$3',[parseFloat(mv.litros),mv.estanque_destino_id,mv.tipo_id]);
        }
        await client.query("UPDATE comb_movimientos SET estado='ANULADO',anulado_en=NOW(),anulado_por=$1,motivo_anulacion='OC eliminada' WHERE mov_id=$2",[req.user.email,mv.mov_id]);
      }
    }
    // 3) Desasociar de factura de guías si aplica
    if(oc.factura_guia_id){
      await client.query('UPDATE ordenes_compra SET factura_guia_id=NULL WHERE oc_id=$1',[req.params.id]);
    }
    // 4) Eliminar OC
    await client.query('DELETE FROM ordenes_compra_detalle WHERE oc_id=$1',[req.params.id]);
    await client.query('DELETE FROM ordenes_compra WHERE oc_id=$1',[req.params.id]);
    await client.query('COMMIT');
    res.json({ok:true});
  }catch(e){await client.query('ROLLBACK');res.status(400).json({error:e.message});}
  finally{client.release();}
});
ocR.post('/:id/recibir-bodega', auth, async(req,res)=>{
  const client=await pool.connect();
  try{
    await client.query('BEGIN');
    const ocQ=await client.query('SELECT * FROM ordenes_compra WHERE oc_id=$1',[req.params.id]);
    if(!ocQ.rows.length) throw new Error('OC no encontrada');
    const oc=ocQ.rows[0];
    if(oc.estado!=='CERRADA') throw new Error('Solo se pueden recibir ordenes CERRADAS');
    if(oc.recibido_en) throw new Error('Esta OC ya fue recibida el '+(oc.recibido_en+'').slice(0,10)+'. No se puede recibir nuevamente.');
    if(oc.movimiento_id) throw new Error('Esta OC ya fue recibida en bodega');
    const{bodega_id}=req.body;
    const prod_map=req.body.prod_map||{};
    const factor_map=req.body.factor_map||{};
    const comb_map=req.body.comb_map||{};
    const comb_splits=req.body.comb_splits||{}; // {detalle_id: [{estanque_id, litros}]}
    let lineas=await client.query('SELECT * FROM ordenes_compra_detalle WHERE oc_id=$1 AND ingresa_bodega=true',[req.params.id]);
    if(!lineas.rows.length) lineas=await client.query('SELECT * FROM ordenes_compra_detalle WHERE oc_id=$1 AND (ingresa_bodega IS NULL OR ingresa_bodega=true)',[req.params.id]);
    if(!lineas.rows.length) throw new Error('No hay lineas marcadas para ingresar.');
    // Detectar líneas combustible: usa comb_splits (nuevo) o comb_map (legacy)
    const lineasComb=lineas.rows.filter(function(l){
      return !!comb_splits[String(l.detalle_id)]||!!comb_map[String(l.detalle_id)];
    });
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
    // Determinar si es provisorio (guía de despacho = aún no facturado)
    var docTipo='';
    if(oc.tipo_doc_id){
      var tdR=await client.query('SELECT nombre FROM tipos_documento WHERE tipo_doc_id=$1',[oc.tipo_doc_id]);
      if(tdR.rows.length)docTipo=(tdR.rows[0].nombre||'').toLowerCase();
    }
    var esProvisorio=docTipo.indexOf('gu')>=0||docTipo.indexOf('despacho')>=0||docTipo.indexOf('provisori')>=0;
    for(const l of lineasComb){
      // Determinar destinos: comb_splits (multi) o comb_map (legacy single)
      var destinos=[];
      if(comb_splits[String(l.detalle_id)]){
        destinos=comb_splits[String(l.detalle_id)].filter(function(s){return s.estanque_id&&parseFloat(s.litros)>0;});
      }else if(comb_map[String(l.detalle_id)]){
        destinos=[{estanque_id:parseInt(comb_map[String(l.detalle_id)]),litros:parseFloat(l.cantidad)}];
      }
      const pu=parseFloat(l.precio_unitario)||0;
      for(const dest of destinos){
        const estanqueId=parseInt(dest.estanque_id);
        const lts=parseFloat(dest.litros);
        if(!estanqueId||!lts)continue;
        const estQ=await client.query('SELECT tipo_combustible_id,empresa_id FROM comb_estanques WHERE estanque_id=$1',[estanqueId]);
        if(!estQ.rows.length) throw new Error('Estanque no encontrado');
        const tipoId=estQ.rows[0].tipo_combustible_id,empresaId=oc.empresa_id||estQ.rows[0].empresa_id;
        if(!tipoId) throw new Error('El estanque no tiene tipo de combustible asignado. Configure el estanque primero.');
        const stk=await client.query('SELECT litros_disponibles,costo_promedio FROM comb_stock WHERE estanque_id=$1 AND tipo_id=$2',[estanqueId,tipoId]);
        if(stk.rows.length){
          const curQ=parseFloat(stk.rows[0].litros_disponibles),curCpp=parseFloat(stk.rows[0].costo_promedio);
          const newQ=curQ+lts,newCpp=newQ>0?(curQ*curCpp+lts*pu)/newQ:pu;
          await client.query('UPDATE comb_stock SET litros_disponibles=$1,costo_promedio=$2,ultima_actualizacion=NOW() WHERE estanque_id=$3 AND tipo_id=$4',[newQ,newCpp,estanqueId,tipoId]);
        }else{
          await client.query('INSERT INTO comb_stock(estanque_id,tipo_id,litros_disponibles,costo_promedio) VALUES($1,$2,$3,$4)',[estanqueId,tipoId,lts,pu]);
        }
        await client.query('INSERT INTO comb_movimientos(tipo_mov,empresa_id,fecha,tipo_id,estanque_destino_id,litros,precio_unitario,costo_total,proveedor_id,numero_documento,oc_referencia,usuario,es_provisorio) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)',
          ['INGRESO_STOCK',empresaId,oc.fecha_documento||new Date().toISOString().split('T')[0],tipoId,estanqueId,lts,pu,lts*pu,oc.proveedor_id,oc.numero_documento,oc.numero_oc,req.user.email,esProvisorio]);
      }
    }
    await client.query('UPDATE ordenes_compra SET movimiento_id=$1,recibido_en=NOW(),modificado_en=NOW() WHERE oc_id=$2',[movId,req.params.id]);
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
    if(oc.recibido_en) throw new Error('No se puede reabrir: la OC ya fue recibida el '+(oc.recibido_en+'').slice(0,10)+'. Debe revertir la recepción primero.');
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

// ══ IMPORTACIÓN MASIVA DE PROVEEDORES DESDE XML ══
app.post('/api/import/proveedores-xml', auth, async(req,res)=>{
  try{
    const{proveedores}=req.body;
    if(!Array.isArray(proveedores)||!proveedores.length) throw new Error('Sin proveedores para importar');
    const results=[];
    for(const p of proveedores){
      try{
        if(!p.rut||!p.nombre){results.push({rut:p.rut,nombre:p.nombre,ok:false,error:'RUT y nombre requeridos'});continue;}
        const exists=await pool.query('SELECT proveedor_id FROM proveedores WHERE rut=$1',[p.rut]);
        if(exists.rows.length){
          // Update if has more data
          if(p.giro||p.direccion){
            await pool.query('UPDATE proveedores SET giro=COALESCE(NULLIF($1,\'\'),giro),direccion=COALESCE(NULLIF($2,\'\'),direccion) WHERE rut=$3',[p.giro||null,p.direccion||null,p.rut]);
          }
          results.push({rut:p.rut,nombre:p.nombre,ok:true,accion:'existente'});
        }else{
          await pool.query('INSERT INTO proveedores(rut,nombre,giro,direccion) VALUES($1,$2,$3,$4)',[p.rut,p.nombre,p.giro||null,p.direccion||null]);
          results.push({rut:p.rut,nombre:p.nombre,ok:true,accion:'creado'});
        }
      }catch(e){results.push({rut:p.rut,nombre:p.nombre,ok:false,error:e.message});}
    }
    res.json({results});
  }catch(e){res.status(400).json({error:e.message});}
});

// ══ IMPORTACIÓN MASIVA DE OC DESDE XML (ZIP Facto) ══
app.post('/api/import/bulk-oc', auth, async(req,res)=>{
  const client=await pool.connect();
  try{
    await client.query('BEGIN');
    const{items}=req.body;
    if(!Array.isArray(items)||!items.length) throw new Error('Sin items para importar');
    const results=[];
    for(const item of items){
      // Match or create proveedor
      let prov_id=null;
      if(item.proveedor_rut){
        const rNorm=item.proveedor_rut.replace(/[\.\-]/g,'').toLowerCase();
        const prov=await client.query("SELECT proveedor_id FROM proveedores WHERE REPLACE(REPLACE(rut,'.',''),'-','')=$1 LIMIT 1",[rNorm]);
        if(prov.rows.length){
          prov_id=prov.rows[0].proveedor_id;
        }else{
          // Auto-create proveedor
          const np=await client.query('INSERT INTO proveedores(rut,nombre,giro,direccion,activo) VALUES($1,$2,$3,$4,true) ON CONFLICT(rut) DO UPDATE SET nombre=$2 RETURNING proveedor_id',
            [item.proveedor_rut,item.proveedor_nombre||'Proveedor importado',item.proveedor_giro||null,item.proveedor_direccion||null]);
          prov_id=np.rows[0].proveedor_id;
        }
      }
      if(!prov_id){results.push({folio:item.folio,error:'Sin proveedor'});continue;}
      // Match empresa
      let emp_id=null;
      if(item.cliente_rut){
        const eNorm=item.cliente_rut.replace(/[\.\-]/g,'').toLowerCase();
        const emp=await client.query("SELECT empresa_id FROM empresas WHERE REPLACE(REPLACE(rut,'.',''),'-','')=$1 LIMIT 1",[eNorm]);
        if(emp.rows.length) emp_id=emp.rows[0].empresa_id;
      }
      // Check duplicate by numero_documento
      if(item.folio){
        const dup=await client.query("SELECT oc_id,numero_oc FROM ordenes_compra WHERE numero_documento=$1 AND proveedor_id=$2 LIMIT 1",[String(item.folio),prov_id]);
        if(dup.rows.length){results.push({folio:item.folio,error:'Ya existe como '+dup.rows[0].numero_oc,oc_id:dup.rows[0].oc_id});continue;}
      }
      // Match tipo doc — first by DTE code, then by name
      let tdoc_id=null;
      if(item.tipo_dte){
        const td=await client.query("SELECT tipo_doc_id FROM tipos_documento WHERE codigo=$1 LIMIT 1",[item.tipo_dte]);
        if(td.rows.length) tdoc_id=td.rows[0].tipo_doc_id;
      }
      if(!tdoc_id&&item.tipo_doc){
        const td2=await client.query("SELECT tipo_doc_id FROM tipos_documento WHERE UPPER(nombre) LIKE $1 LIMIT 1",['%'+item.tipo_doc.toUpperCase()+'%']);
        if(td2.rows.length) tdoc_id=td2.rows[0].tipo_doc_id;
      }
      // Create OC
      const isNC=item.tipo_dte==='61'; // Nota de Crédito → montos negativos
      const sign=isNC?-1:1;
      const year=new Date().getFullYear();
      const seq=await client.query("SELECT nextval('seq_oc_num')");
      const numero_oc='OC-'+year+'-'+String(seq.rows[0].nextval).padStart(4,'0');
      const lineas=item.lineas||[];
      const neto=Math.round(sign*lineas.reduce(function(s,l){return s+(parseFloat(l.cantidad)||0)*(parseFloat(l.precio_unitario)||0);},0));
      const iva=Math.round(neto*0.19);
      const total=neto+iva;
      const ocRes=await client.query(
        "INSERT INTO ordenes_compra(numero_oc,empresa_id,proveedor_id,fecha_emision,tipo_doc_id,numero_documento,fecha_documento,neto,iva,total,estado,observaciones,usuario) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'PENDIENTE',$11,$12) RETURNING oc_id",
        [numero_oc,emp_id,prov_id,item.fecha_emision||new Date().toISOString().split('T')[0],tdoc_id,item.folio?String(item.folio):null,item.fecha_emision||null,neto,iva,total,'Importado desde XML Facto',req.user.email]
      );
      const ocId=ocRes.rows[0].oc_id;
      for(let i=0;i<lineas.length;i++){
        const l=lineas[i];
        await client.query('INSERT INTO ordenes_compra_detalle(oc_id,linea_num,descripcion,cantidad,precio_unitario) VALUES($1,$2,$3,$4,$5)',
          [ocId,i+1,l.descripcion||'Item '+(i+1),parseFloat(l.cantidad)||1,sign*(parseFloat(l.precio_unitario)||0)]);
      }
      results.push({folio:item.folio,ok:true,oc_id:ocId,numero_oc,proveedor:item.proveedor_nombre,total});
    }
    await client.query('COMMIT');
    res.json({ok:true,results});
  }catch(e){await client.query('ROLLBACK');res.status(400).json({error:e.message});}
  finally{client.release();}
});

// USUARIOS
app.get('/api/usuarios', auth, async(req,res)=>{try{res.json((await pool.query('SELECT u.usuario_id,u.email,u.username,u.nombre,u.rol,u.rol_id,u.empresa_id,u.faena_id,u.activo,u.creado_en,r.nombre AS rol_nombre,r.es_admin,e.razon_social AS empresa_nombre,f.nombre AS faena_nombre FROM usuarios u LEFT JOIN roles r ON u.rol_id=r.rol_id LEFT JOIN empresas e ON u.empresa_id=e.empresa_id LEFT JOIN faenas f ON u.faena_id=f.faena_id ORDER BY u.nombre')).rows);}catch(e){res.status(500).json({error:e.message});}});
app.post('/api/usuarios', auth, async(req,res)=>{
  try{const{email,username,nombre,password,rol_id,empresa_id,faena_id}=req.body;if(!email||!nombre||!password)return res.status(400).json({error:'Email, nombre y contraseña requeridos'});const hash=await bcrypt.hash(password,10);const rid=rol_id&&rol_id!==''?parseInt(rol_id):null;const rolNombre=rid?(await pool.query('SELECT nombre FROM roles WHERE rol_id=$1',[rid])).rows[0]?.nombre||'BODEGUERO':'BODEGUERO';const eid=empresa_id&&empresa_id!==''?parseInt(empresa_id):null;const fid=faena_id&&faena_id!==''?parseInt(faena_id):null;const r=await pool.query('INSERT INTO usuarios(email,username,nombre,password_hash,rol,rol_id,empresa_id,faena_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *',[email,username||null,nombre,hash,rolNombre,rid,eid,fid]);res.status(201).json(r.rows[0]);}catch(e){if(e.code==='23505'){var msg=e.detail&&e.detail.indexOf('username')>=0?'El nombre de usuario ya existe':'El email ya está registrado';return res.status(400).json({error:msg});}res.status(400).json({error:e.message});}
});
app.put('/api/usuarios/:id', auth, async(req,res)=>{
  try{const{email,username,nombre,rol_id,password,empresa_id,faena_id}=req.body;
  const rid=rol_id&&rol_id!==''?parseInt(rol_id):null;
  const eid=empresa_id&&empresa_id!==''?parseInt(empresa_id):null;
  const fid=faena_id&&faena_id!==''?parseInt(faena_id):null;
  const rolNombre=rid?(await pool.query('SELECT nombre FROM roles WHERE rol_id=$1',[rid])).rows[0]?.nombre||'BODEGUERO':'ADMINISTRADOR';
  if(password&&password.length>=4){const hash=await bcrypt.hash(password,10);await pool.query('UPDATE usuarios SET email=$1,username=$2,nombre=$3,rol=$4,rol_id=$5,password_hash=$6,empresa_id=$7,faena_id=$8 WHERE usuario_id=$9',[email,username||null,nombre,rolNombre,rid,hash,eid,fid,req.params.id]);}
  else{await pool.query('UPDATE usuarios SET email=$1,username=$2,nombre=$3,rol=$4,rol_id=$5,empresa_id=$6,faena_id=$7 WHERE usuario_id=$8',[email,username||null,nombre,rolNombre,rid,eid,fid,req.params.id]);}
  const r=await pool.query('SELECT u.usuario_id,u.email,u.username,u.nombre,u.rol,u.rol_id,u.empresa_id,u.faena_id,u.activo,r.nombre AS rol_nombre,e.razon_social AS empresa_nombre,f.nombre AS faena_nombre FROM usuarios u LEFT JOIN roles r ON u.rol_id=r.rol_id LEFT JOIN empresas e ON u.empresa_id=e.empresa_id LEFT JOIN faenas f ON u.faena_id=f.faena_id WHERE u.usuario_id=$1',[req.params.id]);
  res.json(r.rows[0]);}catch(e){if(e.code==='23505'){var msg=e.detail&&e.detail.indexOf('username')>=0?'El nombre de usuario ya existe':'El email ya está registrado';return res.status(400).json({error:msg});}res.status(400).json({error:e.message});}
});
app.patch('/api/usuarios/:id/activo', auth, async(req,res)=>{try{res.json((await pool.query('UPDATE usuarios SET activo=NOT activo WHERE usuario_id=$1 RETURNING *',[req.params.id])).rows[0]);}catch(e){res.status(400).json({error:e.message});}});
app.delete('/api/usuarios/:id', auth, async(req,res)=>{try{const chk=await pool.query('SELECT usuario_id FROM usuarios WHERE usuario_id=$1',[req.params.id]);if(!chk.rows.length)return res.status(404).json({error:'Usuario no encontrado'});await pool.query('DELETE FROM usuarios WHERE usuario_id=$1',[req.params.id]);res.json({ok:true});}catch(e){res.status(400).json({error:e.message});}});

// ── ROLES ──
app.get('/api/roles', auth, async(req,res)=>{try{res.json((await pool.query('SELECT * FROM roles ORDER BY rol_id')).rows);}catch(e){res.status(500).json({error:e.message});}});
app.post('/api/roles', auth, async(req,res)=>{
  try{const{nombre,descripcion,modulos,es_admin}=req.body;if(!nombre)return res.status(400).json({error:'Nombre requerido'});
  const r=await pool.query('INSERT INTO roles(nombre,descripcion,modulos,es_admin) VALUES($1,$2,$3,$4) RETURNING *',[nombre,descripcion||null,JSON.stringify(modulos||[]),es_admin||false]);res.status(201).json(r.rows[0]);}catch(e){res.status(400).json({error:e.message});}
});
app.put('/api/roles/:id', auth, async(req,res)=>{
  try{const{nombre,descripcion,modulos,es_admin}=req.body;
  const r=await pool.query('UPDATE roles SET nombre=$1,descripcion=$2,modulos=$3,es_admin=$4 WHERE rol_id=$5 RETURNING *',[nombre,descripcion||null,JSON.stringify(modulos||[]),es_admin||false,req.params.id]);res.json(r.rows[0]);}catch(e){res.status(400).json({error:e.message});}
});
app.delete('/api/roles/:id', auth, async(req,res)=>{try{await pool.query('DELETE FROM roles WHERE rol_id=$1',[req.params.id]);res.json({ok:true});}catch(e){if(e.code==='23503')return res.status(400).json({error:'Rol en uso, no se puede eliminar'});res.status(400).json({error:e.message});}});

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
    const empId=empresa_id?parseInt(empresa_id):null;
    console.log('[COMB-EST POST] empId:', empId, 'codigo:', codigo, 'nombre:', nombre);
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
    const empId=empresa_id?parseInt(empresa_id):null;
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
      COALESCE(emp.razon_social,'Todas las empresas') AS empresa_nombre, ct.nombre AS tipo_nombre,
      ROUND(cs.litros_disponibles*cs.costo_promedio,0) AS valor_total
      FROM comb_stock cs
      JOIN comb_estanques e ON cs.estanque_id=e.estanque_id
      LEFT JOIN empresas emp ON e.empresa_id=emp.empresa_id
      JOIN comb_tipos ct ON cs.tipo_id=ct.tipo_id
      WHERE cs.litros_disponibles>0
      ORDER BY COALESCE(emp.razon_social,'ZZZ'),e.nombre`);
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
        COALESCE(emp.razon_social,'Todas las empresas') AS empresa_nombre,ct.nombre AS tipo_nombre,
        cs.litros_disponibles,cs.costo_promedio,
        ROUND(cs.litros_disponibles*cs.costo_promedio,0) AS valor_total
      FROM comb_stock cs
      JOIN comb_estanques e ON cs.estanque_id=e.estanque_id
      LEFT JOIN empresas emp ON e.empresa_id=emp.empresa_id
      JOIN comb_tipos ct ON cs.tipo_id=ct.tipo_id
      WHERE cs.litros_disponibles>0
      ORDER BY COALESCE(emp.razon_social,'ZZZ'),e.nombre`);

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
  const tipoDoc=tipoDte==='33'||tipoDte==='34'?'FACTURA':tipoDte==='61'?'NOTA CREDITO':tipoDte==='56'?'NOTA DEBITO':tipoDte==='52'?'GUIA':tipoDte==='39'?'BOLETA':'FACTURA';

  // Encabezado
  const ndoc=tag('Folio');
  const fecha=tag('FchEmis');
  const condPago=tag('TermPagoGlosa')||tag('FmaPago');

  // Emisor (proveedor)
  const provRut=tag('RUTEmisor');
  const provNombre=tag('RznSoc');
  const provGiro=tag('GiroEmis');
  const provDir=tag('DirOrigen');
  const provComuna=tag('CmnaOrigen');

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

  return{numero_documento:ndoc,fecha_emision:fecha,tipo_doc:tipoDoc,tipo_dte:tipoDte,
    proveedor_rut:provRut,proveedor_nombre:provNombre,
    proveedor_giro:provGiro,proveedor_direccion:provDir?(provDir+(provComuna?' '+provComuna:'')):null,
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
// ══ MANTENEDOR DE SISTEMAS ══
app.get('/api/mant/sistemas', auth, async(req,res)=>{
  try{res.json((await pool.query('SELECT * FROM mant_sistemas ORDER BY orden,nombre')).rows);}catch(e){res.status(500).json({error:e.message});}
});
app.post('/api/mant/sistemas', auth, async(req,res)=>{
  try{const{codigo,nombre,descripcion,orden}=req.body;const r=await pool.query('INSERT INTO mant_sistemas(codigo,nombre,descripcion,orden) VALUES($1,$2,$3,$4) RETURNING *',[codigo,nombre,descripcion||null,orden||0]);res.status(201).json(r.rows[0]);}catch(e){res.status(400).json({error:e.message});}
});
app.put('/api/mant/sistemas/:id', auth, async(req,res)=>{
  try{const{codigo,nombre,descripcion,orden,activo}=req.body;const r=await pool.query('UPDATE mant_sistemas SET codigo=$1,nombre=$2,descripcion=$3,orden=$4,activo=$5 WHERE sistema_id=$6 RETURNING *',[codigo,nombre,descripcion||null,orden||0,activo!==false,req.params.id]);res.json(r.rows[0]);}catch(e){res.status(400).json({error:e.message});}
});

// ══ MANTENEDOR DE TAREAS ESTÁNDAR ══
app.get('/api/mant/tareas-std', auth, async(req,res)=>{
  try{const{sistema_id}=req.query;let q2='SELECT t.*,s.nombre AS sistema_nombre,s.codigo AS sistema_codigo FROM mant_tareas_std t JOIN mant_sistemas s ON t.sistema_id=s.sistema_id';if(sistema_id)q2+=' WHERE t.sistema_id='+parseInt(sistema_id);q2+=' ORDER BY s.orden,t.nombre';res.json((await pool.query(q2)).rows);}catch(e){res.status(500).json({error:e.message});}
});
app.post('/api/mant/tareas-std', auth, async(req,res)=>{
  try{const{sistema_id,nombre,descripcion,tipo_tarea,tipo_activo}=req.body;const r=await pool.query('INSERT INTO mant_tareas_std(sistema_id,nombre,descripcion,tipo_tarea,tipo_activo) VALUES($1,$2,$3,$4,$5) RETURNING *',[sistema_id,nombre,descripcion||null,tipo_tarea||'preventiva',tipo_activo||'todos']);res.status(201).json(r.rows[0]);}catch(e){res.status(400).json({error:e.message});}
});
app.put('/api/mant/tareas-std/:id', auth, async(req,res)=>{
  try{const{sistema_id,nombre,descripcion,tipo_tarea,tipo_activo,activo}=req.body;const r=await pool.query('UPDATE mant_tareas_std SET sistema_id=$1,nombre=$2,descripcion=$3,tipo_tarea=$4,tipo_activo=$5,activo=$6 WHERE tarea_std_id=$7 RETURNING *',[sistema_id,nombre,descripcion||null,tipo_tarea||'preventiva',tipo_activo||'todos',activo!==false,req.params.id]);res.json(r.rows[0]);}catch(e){res.status(400).json({error:e.message});}
});

// ══ RECALC OT COSTS (helper) ══
async function recalcOTCosts(ot_id){
  try{
    const miscQ=await pool.query('SELECT COALESCE(SUM(costo_total),0) AS misc FROM mant_ot_materiales WHERE ot_id=$1',[ot_id]);
    const moGlobalQ=await pool.query('SELECT COALESCE(SUM(costo_total),0) AS total FROM mant_ot_personal WHERE ot_id=$1',[ot_id]);
    const moTareaIntQ=await pool.query("SELECT COALESCE(SUM(tp.costo_total),0) AS total FROM mant_ot_tarea_personal tp JOIN mant_ot_tareas t ON tp.tarea_id=t.tarea_id WHERE t.ot_id=$1 AND tp.tiene_costo=true AND tp.tipo_personal='interno'",[ot_id]);
    const moTareaExtQ=await pool.query("SELECT COALESCE(SUM(tp.costo_total),0) AS total FROM mant_ot_tarea_personal tp JOIN mant_ot_tareas t ON tp.tarea_id=t.tarea_id WHERE t.ot_id=$1 AND tp.tiene_costo=true AND tp.tipo_personal='externo'",[ot_id]);
    const salidasQ=await pool.query(`SELECT COALESCE(SUM(md.cantidad*md.costo_unitario),0) AS total FROM movimiento_detalle md JOIN movimiento_encabezado me ON md.movimiento_id=me.movimiento_id WHERE md.ot_id=$1 AND me.tipo_movimiento='SALIDA' AND me.estado='ACTIVO'`,[ot_id]);
    const costoMisc=parseFloat(miscQ.rows[0].misc)||0;
    const moInt=Math.max(parseFloat(moGlobalQ.rows[0].total)||0, parseFloat(moTareaIntQ.rows[0].total)||0);
    const moExtTareas=parseFloat(moTareaExtQ.rows[0].total)||0;
    const costoSalidas=parseFloat(salidasQ.rows[0].total)||0;
    const otRow=await pool.query('SELECT costo_mano_obra_externa,costo_traslado,costo_combustible_traslado,costo_otros FROM mant_ot WHERE ot_id=$1',[ot_id]);
    const ot=otRow.rows[0]||{};
    const moExtManual=parseFloat(ot.costo_mano_obra_externa)||0;
    const moExt=moExtTareas+moExtManual;
    const tras=parseFloat(ot.costo_traslado)||parseFloat(ot.costo_combustible_traslado)||0;
    const otros=parseFloat(ot.costo_otros)||0;
    const total=moInt+moExt+tras+otros+costoSalidas+costoMisc;
    await pool.query('UPDATE mant_ot SET costo_mano_obra_interna=$1,costo_mano_obra_externa=$2,costo_total=$3,actualizado_en=NOW() WHERE ot_id=$4',[moInt,moExt,total,ot_id]);
  }catch(e){console.error('[recalcOTCosts]',e.message);}
}

// ══ PERSONAL POR TAREA OT ══
app.get('/api/mant/ot/tareas/:id/personal', auth, async(req,res)=>{
  try{const r=await pool.query(`SELECT tp.*,p.nombre_completo,p.cargo,p.valor_hora_hombre AS valor_hh_maestro FROM mant_ot_tarea_personal tp LEFT JOIN personal p ON tp.persona_id=p.persona_id WHERE tp.tarea_id=$1 ORDER BY tp.id`,[req.params.id]);res.json(r.rows);}catch(e){res.status(500).json({error:e.message});}
});
app.post('/api/mant/ot/tareas/:id/personal', auth, async(req,res)=>{
  try{
    const{tipo_personal,persona_id,nombre_externo,horas_trabajadas,valor_hora_aplicado,tiene_costo,observacion}=req.body;
    let vhh=parseFloat(valor_hora_aplicado)||0;
    if(tipo_personal==='interno'&&!vhh&&persona_id){const p=await pool.query('SELECT valor_hora_hombre FROM personal WHERE persona_id=$1',[persona_id]);if(p.rows.length)vhh=parseFloat(p.rows[0].valor_hora_hombre)||0;}
    const r=await pool.query(`INSERT INTO mant_ot_tarea_personal(tarea_id,tipo_personal,persona_id,nombre_externo,horas_trabajadas,valor_hora_aplicado,tiene_costo,observacion) VALUES($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`,
      [req.params.id,tipo_personal||'interno',persona_id||null,nombre_externo||null,parseFloat(horas_trabajadas)||0,vhh,tiene_costo!==false,observacion||null]);
    // Recalc OT costs
    const tareaRow=await pool.query('SELECT ot_id FROM mant_ot_tareas WHERE tarea_id=$1',[req.params.id]);
    if(tareaRow.rows.length) await recalcOTCosts(tareaRow.rows[0].ot_id);
    res.status(201).json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
app.delete('/api/mant/ot/tarea-personal/:id', auth, async(req,res)=>{
  try{
    const tp=await pool.query('SELECT t.ot_id FROM mant_ot_tarea_personal tp2 JOIN mant_ot_tareas t ON tp2.tarea_id=t.tarea_id WHERE tp2.id=$1',[req.params.id]);
    await pool.query('DELETE FROM mant_ot_tarea_personal WHERE id=$1',[req.params.id]);
    if(tp.rows.length) await recalcOTCosts(tp.rows[0].ot_id);
    res.json({ok:true});
  }catch(e){res.status(400).json({error:e.message});}
});

// ══ ENLACE SALIDA INVENTARIO A OT ══
app.patch('/api/mov-detalle/:id/ot', auth, async(req,res)=>{
  try{const{ot_id}=req.body;const r=await pool.query('UPDATE movimiento_detalle SET ot_id=$1 WHERE detalle_id=$2 RETURNING *',[ot_id||null,req.params.id]);res.json(r.rows[0]);}catch(e){res.status(400).json({error:e.message});}
});
app.get('/api/mant/ot/:id/salidas-inv', auth, async(req,res)=>{
  try{const r=await pool.query(`SELECT md.*,p.nombre AS producto_nombre,p.codigo AS producto_codigo,me.fecha,me.numero_documento,b.nombre AS bodega_nombre,me.movimiento_id AS mov_id FROM movimiento_detalle md JOIN movimiento_encabezado me ON md.movimiento_id=me.movimiento_id JOIN productos p ON md.producto_id=p.producto_id JOIN bodegas b ON me.bodega_id=b.bodega_id WHERE md.ot_id=$1 AND me.tipo_movimiento='SALIDA' AND me.estado='ACTIVO' ORDER BY me.fecha DESC`,[req.params.id]);res.json(r.rows);}catch(e){res.status(500).json({error:e.message});}
});

// Salidas disponibles para enlazar a una OT (no enlazadas aún, filtro por equipo)
app.get('/api/mant/salidas-disponibles-ot', auth, async(req,res)=>{
  try{
    const{equipo_id}=req.query;
    let where=["me.tipo_movimiento='SALIDA'","me.estado='ACTIVO'"],vals=[];
    if(equipo_id){vals.push(equipo_id);where.push(`me.equipo_id=$${vals.length}`);}
    // Solo movimientos que tienen al menos 1 línea sin ot_id
    const r=await pool.query(`SELECT DISTINCT me.movimiento_id,me.fecha,b.nombre AS bodega_nombre,e.nombre AS equipo_nombre,f.nombre AS faena_nombre,mot.nombre AS motivo_nombre,
      (SELECT COUNT(*) FROM movimiento_detalle md2 WHERE md2.movimiento_id=me.movimiento_id) AS n_lineas,
      (SELECT COUNT(*) FROM movimiento_detalle md3 WHERE md3.movimiento_id=me.movimiento_id AND md3.ot_id IS NULL) AS n_sin_ot,
      (SELECT SUM(md4.cantidad*md4.costo_unitario) FROM movimiento_detalle md4 WHERE md4.movimiento_id=me.movimiento_id AND md4.ot_id IS NULL) AS total_disp
      FROM movimiento_encabezado me
      JOIN bodegas b ON me.bodega_id=b.bodega_id
      LEFT JOIN equipos e ON me.equipo_id=e.equipo_id
      LEFT JOIN faenas f ON me.faena_id=f.faena_id
      LEFT JOIN motivos_movimiento mot ON me.motivo_id=mot.motivo_id
      WHERE ${where.join(' AND ')}
      AND EXISTS(SELECT 1 FROM movimiento_detalle md WHERE md.movimiento_id=me.movimiento_id AND md.ot_id IS NULL)
      ORDER BY me.fecha DESC`
    ,vals);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});

// Enlazar todas las líneas sin OT de un movimiento a una OT
app.patch('/api/mant/salida-link-ot', auth, async(req,res)=>{
  try{
    const{movimiento_id,ot_id}=req.body;
    if(!movimiento_id||!ot_id) return res.status(400).json({error:'movimiento_id y ot_id requeridos'});
    const chk=await pool.query(`SELECT md.ot_id,o.numero_ot FROM movimiento_detalle md LEFT JOIN mant_ot o ON md.ot_id=o.ot_id WHERE md.movimiento_id=$1 AND md.ot_id IS NOT NULL LIMIT 1`,[movimiento_id]);
    if(chk.rows.length>0) return res.status(400).json({error:'Esta salida ya está enlazada a '+(chk.rows[0].numero_ot||'OT #'+chk.rows[0].ot_id)+'. Debe desenlazarla primero.'});
    const r=await pool.query('UPDATE movimiento_detalle SET ot_id=$1 WHERE movimiento_id=$2 AND ot_id IS NULL RETURNING *',[ot_id,movimiento_id]);
    await recalcOTCosts(ot_id);
    res.json({ok:true,lineas_enlazadas:r.rowCount});
  }catch(e){res.status(400).json({error:e.message});}
});

// Desenlazar líneas de un movimiento de una OT
app.patch('/api/mant/salida-unlink-ot', auth, async(req,res)=>{
  try{
    const{movimiento_id,ot_id}=req.body;
    if(!movimiento_id||!ot_id) return res.status(400).json({error:'movimiento_id y ot_id requeridos'});
    await pool.query('UPDATE movimiento_detalle SET ot_id=NULL WHERE movimiento_id=$1 AND ot_id=$2',[movimiento_id,ot_id]);
    await recalcOTCosts(ot_id);
    res.json({ok:true});
  }catch(e){res.status(400).json({error:e.message});}
});

app.get('/api/mant/ot', auth, async(req,res)=>{
  try{
    const{empresa_id,equipo_id,estado,desde,hasta}=req.query;
    let where=['1=1'],vals=[];
    if(empresa_id){vals.push(empresa_id);where.push(`o.empresa_id=$${vals.length}`);}
    if(equipo_id){vals.push(equipo_id);where.push(`o.equipo_id=$${vals.length}`);}
    if(estado){vals.push(estado);where.push(`o.estado=$${vals.length}`);}
    if(desde){vals.push(desde);where.push(`o.fecha_apertura>=$${vals.length}`);}
    if(hasta){vals.push(hasta);where.push(`o.fecha_apertura<=$${vals.length}`);}
    const r=await pool.query(`SELECT o.*,eq.nombre AS equipo_nombre,eq.tipo_activo,eq.familia,eq.horas_productivas_dia,eq.horometro_actual,f.nombre AS faena_nombre,emp.razon_social AS empresa_nombre,COALESCE((SELECT SUM(d.cantidad*d.precio_unitario) FROM ordenes_compra_detalle d WHERE d.ot_id=o.ot_id),0) AS costo_oc,COALESCE((SELECT SUM(md.cantidad*md.costo_unitario) FROM movimiento_detalle md JOIN movimiento_encabezado me ON md.movimiento_id=me.movimiento_id WHERE md.ot_id=o.ot_id AND me.tipo_movimiento='SALIDA' AND me.estado='ACTIVO'),0) AS costo_salidas,COALESCE((SELECT SUM(tp.costo_total) FROM mant_ot_tarea_personal tp JOIN mant_ot_tareas t ON tp.tarea_id=t.tarea_id WHERE t.ot_id=o.ot_id AND tp.tiene_costo=true AND tp.tipo_personal='interno'),0) AS costo_mo_tareas,COALESCE((SELECT SUM(tp.costo_total) FROM mant_ot_tarea_personal tp JOIN mant_ot_tareas t ON tp.tarea_id=t.tarea_id WHERE t.ot_id=o.ot_id AND tp.tiene_costo=true AND tp.tipo_personal='externo'),0) AS costo_mo_ext_tareas FROM mant_ot o LEFT JOIN equipos eq ON o.equipo_id=eq.equipo_id LEFT JOIN faenas f ON o.faena_id=f.faena_id LEFT JOIN empresas emp ON o.empresa_id=emp.empresa_id WHERE ${where.join(' AND ')} ORDER BY o.creado_en DESC`,vals);
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
    const{estado,fecha_inicio,fecha_termino,horometro_servicio,kilometraje_servicio,diagnostico,causa,trabajo_realizado,observaciones,responsable,mecanico_asignado,taller_tipo,taller_nombre,tiempo_detenido_hrs,costo_mano_obra_interna,costo_mano_obra_externa,costo_servicios,costo_traslado,costo_otros,prioridad,sistema,vehiculo_traslado,distancia_km,costo_combustible_traslado}=req.body;

    // ── Auto-gestión de fechas y tiempo de detención ──
    const prevOT=await pool.query('SELECT estado,fecha_inicio,fecha_termino FROM mant_ot WHERE ot_id=$1',[req.params.id]);
    const prev=prevOT.rows[0]||{};
    // Auto fecha_inicio: si cambia a en_ejecucion y no tiene fecha_inicio
    let fInicio=fecha_inicio||null;
    if(estado==='en_ejecucion'&&!prev.fecha_inicio&&!fInicio){
      fInicio=new Date().toISOString();
    }else if(!fInicio&&prev.fecha_inicio){
      fInicio=prev.fecha_inicio;
    }
    // Auto fecha_termino: si cambia a cerrada y no tiene fecha_termino
    let fTermino=fecha_termino||null;
    if(estado==='cerrada'&&!fTermino){
      fTermino=new Date().toISOString();
    }
    // Auto-calcular tiempo_detenido_hrs desde fecha_inicio hasta fecha_termino
    // Usa horas_productivas_dia del equipo (default 12h) en vez de 24h
    let hrsDetenido=parseFloat(tiempo_detenido_hrs)||0;
    if(fInicio&&fTermino){
      var diffMs=new Date(fTermino).getTime()-new Date(fInicio).getTime();
      if(diffMs>0){
        var diasCalendario=diffMs/86400000;
        // Buscar horas productivas del equipo
        var eqHrs=await pool.query('SELECT horas_productivas_dia FROM equipos eq JOIN mant_ot o ON eq.equipo_id=o.equipo_id WHERE o.ot_id=$1',[req.params.id]);
        var hrsProductivas=parseFloat(eqHrs.rows[0]?.horas_productivas_dia)||12;
        hrsDetenido=Math.round(diasCalendario*hrsProductivas*100)/100;
      }
    }
    // Si se abre o pone en ejecución → marcar equipo como detenido
    if(estado==='en_ejecucion'||estado==='abierta'){
      const eqQ=await pool.query('SELECT equipo_id FROM mant_ot WHERE ot_id=$1',[req.params.id]);
      if(eqQ.rows.length) await pool.query("UPDATE equipos SET estado_operativo='detenido' WHERE equipo_id=$1",[eqQ.rows[0].equipo_id]);
    }

    // Recalculate total costs
    const matQ=await pool.query('SELECT COALESCE(SUM(costo_total),0) AS misc FROM mant_ot_materiales WHERE ot_id=$1',[req.params.id]);
    const moQ=await pool.query('SELECT COALESCE(SUM(costo_total),0) AS total FROM mant_ot_personal WHERE ot_id=$1',[req.params.id]);
    const moTareaQ=await pool.query('SELECT COALESCE(SUM(tp.costo_total),0) AS total FROM mant_ot_tarea_personal tp JOIN mant_ot_tareas t ON tp.tarea_id=t.tarea_id WHERE t.ot_id=$1 AND tp.tiene_costo=true',[req.params.id]);
    const salidasQ=await pool.query('SELECT COALESCE(SUM(md.cantidad*md.costo_unitario),0) AS total FROM movimiento_detalle md JOIN movimiento_encabezado me ON md.movimiento_id=me.movimiento_id WHERE md.ot_id=$1 AND me.tipo_movimiento=\'SALIDA\' AND me.estado=\'ACTIVO\'',[req.params.id]);
    const costoMisc=parseFloat(matQ.rows[0].misc)||0;
    const moGlobal=parseFloat(moQ.rows[0].total)||0;
    const moTareas=parseFloat(moTareaQ.rows[0].total)||0;
    const moInt=Math.max(moGlobal,moTareas)||parseFloat(costo_mano_obra_interna)||0;
    const costoSalidas=parseFloat(salidasQ.rows[0].total)||0;
    const moExt=parseFloat(costo_mano_obra_externa)||0;
    const tras=parseFloat(costo_traslado)||parseFloat(costo_combustible_traslado)||0;
    const otros=parseFloat(costo_otros)||0;
    const total=moInt+moExt+tras+otros+costoSalidas+costoMisc;
    const r=await pool.query(`UPDATE mant_ot SET estado=$1,fecha_inicio=$2,fecha_termino=$3,horometro_servicio=$4,kilometraje_servicio=$5,diagnostico=$6,causa=$7,trabajo_realizado=$8,observaciones=$9,responsable=$10,mecanico_asignado=$11,taller_tipo=$12,taller_nombre=$13,tiempo_detenido_hrs=$14,costo_repuestos=0,costo_lubricantes=0,costo_mano_obra_interna=$15,costo_mano_obra_externa=$16,costo_servicios=0,costo_traslado=$17,costo_otros=$18,costo_total=$19,prioridad=$20,sistema=$21,vehiculo_traslado=$22,distancia_km=$23,costo_combustible_traslado=$24,actualizado_en=NOW() WHERE ot_id=$25 RETURNING *`,
      [estado,fInicio,fTermino,horometro_servicio||null,kilometraje_servicio||null,diagnostico||null,causa||null,trabajo_realizado||null,observaciones||null,responsable||null,mecanico_asignado||null,taller_tipo||'interno',taller_nombre||null,hrsDetenido,moInt,moExt,tras,otros,total,prioridad||'normal',sistema||null,vehiculo_traslado||null,parseFloat(distancia_km)||0,parseFloat(costo_combustible_traslado)||0,req.params.id]);
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
    // Re-fetch con joins para devolver datos completos
    const full=await pool.query(`SELECT o.*,eq.nombre AS equipo_nombre,eq.tipo_activo,eq.familia,eq.horas_productivas_dia,eq.horometro_actual,f.nombre AS faena_nombre,emp.razon_social AS empresa_nombre,COALESCE((SELECT SUM(d.cantidad*d.precio_unitario) FROM ordenes_compra_detalle d WHERE d.ot_id=o.ot_id),0) AS costo_oc,COALESCE((SELECT SUM(md.cantidad*md.costo_unitario) FROM movimiento_detalle md JOIN movimiento_encabezado me ON md.movimiento_id=me.movimiento_id WHERE md.ot_id=o.ot_id AND me.tipo_movimiento='SALIDA' AND me.estado='ACTIVO'),0) AS costo_salidas,COALESCE((SELECT SUM(tp.costo_total) FROM mant_ot_tarea_personal tp JOIN mant_ot_tareas t ON tp.tarea_id=t.tarea_id WHERE t.ot_id=o.ot_id AND tp.tiene_costo=true AND tp.tipo_personal='interno'),0) AS costo_mo_tareas,COALESCE((SELECT SUM(tp.costo_total) FROM mant_ot_tarea_personal tp JOIN mant_ot_tareas t ON tp.tarea_id=t.tarea_id WHERE t.ot_id=o.ot_id AND tp.tiene_costo=true AND tp.tipo_personal='externo'),0) AS costo_mo_ext_tareas FROM mant_ot o LEFT JOIN equipos eq ON o.equipo_id=eq.equipo_id LEFT JOIN faenas f ON o.faena_id=f.faena_id LEFT JOIN empresas emp ON o.empresa_id=emp.empresa_id WHERE o.ot_id=$1`,[req.params.id]);
    res.json(full.rows[0]||ot);
  }catch(e){res.status(400).json({error:e.message});}
});

// GET single OT with joins
app.get('/api/mant/ot/:id/full', auth, async(req,res)=>{
  try{
    const r=await pool.query(`SELECT o.*,eq.nombre AS equipo_nombre,eq.tipo_activo,eq.familia,eq.horas_productivas_dia,eq.horometro_actual,f.nombre AS faena_nombre,emp.razon_social AS empresa_nombre,COALESCE((SELECT SUM(d.cantidad*d.precio_unitario) FROM ordenes_compra_detalle d WHERE d.ot_id=o.ot_id),0) AS costo_oc,COALESCE((SELECT SUM(md.cantidad*md.costo_unitario) FROM movimiento_detalle md JOIN movimiento_encabezado me ON md.movimiento_id=me.movimiento_id WHERE md.ot_id=o.ot_id AND me.tipo_movimiento='SALIDA' AND me.estado='ACTIVO'),0) AS costo_salidas,COALESCE((SELECT SUM(tp.costo_total) FROM mant_ot_tarea_personal tp JOIN mant_ot_tareas t ON tp.tarea_id=t.tarea_id WHERE t.ot_id=o.ot_id AND tp.tiene_costo=true AND tp.tipo_personal='interno'),0) AS costo_mo_tareas,COALESCE((SELECT SUM(tp.costo_total) FROM mant_ot_tarea_personal tp JOIN mant_ot_tareas t ON tp.tarea_id=t.tarea_id WHERE t.ot_id=o.ot_id AND tp.tiene_costo=true AND tp.tipo_personal='externo'),0) AS costo_mo_ext_tareas FROM mant_ot o LEFT JOIN equipos eq ON o.equipo_id=eq.equipo_id LEFT JOIN faenas f ON o.faena_id=f.faena_id LEFT JOIN empresas emp ON o.empresa_id=emp.empresa_id WHERE o.ot_id=$1`,[req.params.id]);
    res.json(r.rows[0]||null);
  }catch(e){res.status(500).json({error:e.message});}
});

// Eliminar OT completa
app.delete('/api/mant/ot/:id', auth, async(req,res)=>{
  try{
    const ot=await pool.query('SELECT * FROM mant_ot WHERE ot_id=$1',[req.params.id]);
    if(!ot.rows.length) return res.status(404).json({error:'OT no encontrada'});
    if(ot.rows[0].estado==='cerrada') return res.status(400).json({error:'No se puede eliminar una OT cerrada'});
    // Desenlazar OCs
    await pool.query('UPDATE ordenes_compra_detalle SET ot_id=NULL WHERE ot_id=$1',[req.params.id]);
    // Borrar dependencias (CASCADE debería cubrir la mayoría)
    await pool.query('DELETE FROM mant_ot WHERE ot_id=$1',[req.params.id]);
    res.json({ok:true});
  }catch(e){res.status(400).json({error:e.message});}
});

// OT Tareas
app.get('/api/mant/ot/:id/tareas', auth, async(req,res)=>{
  try{const r=await pool.query(`SELECT t.*,s.nombre AS sistema_nombre,s.codigo AS sistema_codigo,ts.nombre AS tarea_std_nombre FROM mant_ot_tareas t LEFT JOIN mant_sistemas s ON t.sistema_id=s.sistema_id LEFT JOIN mant_tareas_std ts ON t.tarea_std_id=ts.tarea_std_id WHERE t.ot_id=$1 ORDER BY t.orden,t.tarea_id`,[req.params.id]);res.json(r.rows);}catch(e){res.status(500).json({error:e.message});}
});
app.post('/api/mant/ot/:id/tareas', auth, async(req,res)=>{
  try{
    const{descripcion,sistema,tipo,desde_plan}=req.body;
    const cnt=await pool.query('SELECT COUNT(*)+1 AS n FROM mant_ot_tareas WHERE ot_id=$1',[req.params.id]);
    const r=await pool.query('INSERT INTO mant_ot_tareas(ot_id,orden,descripcion,sistema,tipo,desde_plan,sistema_id,tarea_std_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *',[req.params.id,cnt.rows[0].n,descripcion,sistema||null,tipo||'tarea',desde_plan||false,req.body.sistema_id||null,req.body.tarea_std_id||null]);
    res.status(201).json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
app.patch('/api/mant/ot/tareas/:id', auth, async(req,res)=>{
  try{
    const{estado,observacion,descripcion,sistema,sistema_id,tarea_std_id}=req.body;
    let sets=['estado=$1','observacion=$2'],vals=[estado,observacion||null];
    if(descripcion!==undefined){vals.push(descripcion);sets.push('descripcion=$'+vals.length);}
    if(sistema!==undefined){vals.push(sistema||null);sets.push('sistema=$'+vals.length);}
    if(sistema_id!==undefined){vals.push(sistema_id||null);sets.push('sistema_id=$'+vals.length);}
    if(tarea_std_id!==undefined){vals.push(tarea_std_id||null);sets.push('tarea_std_id=$'+vals.length);}
    vals.push(req.params.id);
    const r=await pool.query('UPDATE mant_ot_tareas SET '+sets.join(',')+' WHERE tarea_id=$'+vals.length+' RETURNING *',vals);
    // Re-fetch with joins
    const full=await pool.query('SELECT t.*,s.nombre AS sistema_nombre,s.codigo AS sistema_codigo,ts.nombre AS tarea_std_nombre FROM mant_ot_tareas t LEFT JOIN mant_sistemas s ON t.sistema_id=s.sistema_id LEFT JOIN mant_tareas_std ts ON t.tarea_std_id=ts.tarea_std_id WHERE t.tarea_id=$1',[req.params.id]);
    res.json(full.rows[0]||r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
app.delete('/api/mant/ot/tareas/:id', auth, async(req,res)=>{
  try{await pool.query('DELETE FROM mant_ot_tareas WHERE tarea_id=$1',[req.params.id]);res.json({ok:true});}catch(e){res.status(400).json({error:e.message});}
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
    let emp='',empEq='',vals=[],valsE=[];
    if(empresa_id){vals.push(empresa_id);emp=` AND o.empresa_id=$${vals.length}`;valsE=[empresa_id];empEq=' AND eq.empresa_id=$1';}
    const [otEstados,alertas,costoMes,costoTotal,equiposStatus,topEquipos,otRecientes,tipoMant]=await Promise.all([
      pool.query(`SELECT estado,COUNT(*) AS n FROM mant_ot o WHERE 1=1${emp} GROUP BY estado`,vals),
      pool.query(`SELECT COUNT(*) FILTER(WHERE estado='vencida') AS vencidas,COUNT(*) FILTER(WHERE estado='proxima') AS proximas,COUNT(*) FILTER(WHERE estado='vigente') AS vigentes FROM mant_programacion p ${empresa_id?'JOIN equipos eq ON p.equipo_id=eq.equipo_id WHERE eq.empresa_id=$1':'WHERE 1=1'}`,empresa_id?[empresa_id]:[]),
      pool.query(`SELECT COALESCE(SUM(costo_total),0) AS total FROM mant_ot o WHERE estado='cerrada' AND fecha_termino>=date_trunc('month',CURRENT_DATE)${emp}`,vals),
      pool.query(`SELECT COALESCE(SUM(costo_total),0) AS total,COALESCE(SUM(tiempo_detenido_hrs),0) AS hrs_detencion FROM mant_ot o WHERE estado='cerrada'${emp}`,vals),
      pool.query(`SELECT eq.equipo_id,eq.codigo,eq.nombre,eq.tipo,eq.estado_operativo,eq.horometro_actual,eq.kilometraje_actual,eq.tipo_cargo,m.marca AS modelo_marca,m.modelo AS modelo_nombre,
        (SELECT COUNT(*) FROM mant_ot o2 WHERE o2.equipo_id=eq.equipo_id AND o2.estado IN ('abierta','en_ejecucion')) AS ot_activas,
        (SELECT COUNT(*) FROM mant_programacion p2 WHERE p2.equipo_id=eq.equipo_id AND p2.estado='vencida') AS mant_vencidas,
        (SELECT COALESCE(SUM(o3.costo_total),0) FROM mant_ot o3 WHERE o3.equipo_id=eq.equipo_id AND o3.estado='cerrada') AS costo_acum,
        (SELECT COALESCE(SUM(o4.tiempo_detenido_hrs),0) FROM mant_ot o4 WHERE o4.equipo_id=eq.equipo_id AND o4.estado='cerrada') AS hrs_det_acum
        FROM equipos eq LEFT JOIN modelos_equipo m ON eq.modelo_id=m.modelo_id WHERE eq.activo=true AND eq.tipo_cargo='maquinaria'${empEq} ORDER BY eq.codigo`,valsE),
      pool.query(`SELECT eq.nombre AS equipo,COALESCE(SUM(o.costo_total),0) AS costo FROM mant_ot o JOIN equipos eq ON o.equipo_id=eq.equipo_id WHERE o.estado='cerrada'${emp} GROUP BY eq.equipo_id,eq.nombre ORDER BY costo DESC LIMIT 8`,vals),
      pool.query(`SELECT o.ot_id,o.numero_ot,o.estado,o.tipo_mantencion,o.fecha_apertura,o.costo_total,eq.nombre AS equipo_nombre,eq.codigo AS equipo_codigo FROM mant_ot o JOIN equipos eq ON o.equipo_id=eq.equipo_id WHERE 1=1${emp} ORDER BY o.creado_en DESC LIMIT 10`,vals),
      pool.query(`SELECT tipo_mantencion,COUNT(*) AS n,COALESCE(SUM(costo_total),0) AS costo FROM mant_ot o WHERE estado='cerrada'${emp} GROUP BY tipo_mantencion ORDER BY n DESC`,vals)
    ]);
    res.json({
      ot_por_estado:otEstados.rows,alertas:alertas.rows[0],
      costo_mes:costoMes.rows[0].total,costo_total:costoTotal.rows[0].total,
      hrs_detencion_total:costoTotal.rows[0].hrs_detencion,
      equipos:equiposStatus.rows,top_equipos:topEquipos.rows,
      ot_recientes:otRecientes.rows,tipo_mantencion:tipoMant.rows
    });
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
    const{empresa_id,activo,mantencion,faena_id}=req.query;
    let where=['1=1'],vals=[];
    if(empresa_id){vals.push(empresa_id);where.push(`p.empresa_id=$${vals.length}`);}
    if(activo!==undefined){vals.push(activo==='true');where.push(`p.activo=$${vals.length}`);}
    if(mantencion==='true'){where.push('p.participa_mantencion=true');}
    if(faena_id){vals.push(faena_id);where.push(`p.faena_id=$${vals.length}`);}
    const r=await pool.query(`SELECT p.*,e.razon_social AS empresa_nombre,f.nombre AS faena_nombre,f.codigo AS faena_codigo FROM personal p LEFT JOIN empresas e ON p.empresa_id=e.empresa_id LEFT JOIN faenas f ON p.faena_id=f.faena_id WHERE ${where.join(' AND ')} ORDER BY p.nombre_completo`,vals);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});
app.post('/api/personal', auth, async(req,res)=>{
  try{
    const b=req.body;
    if(!b.nombre_completo) return res.status(400).json({error:'Nombre requerido'});
    const r=await pool.query(`INSERT INTO personal(empresa_id,nombre_completo,rut,cargo,especialidad,telefono,correo,participa_mantencion,valor_hora_hombre,moneda,fecha_ingreso,cotizaciones_anteriores,observaciones,fecha_nacimiento,direccion,comuna,tipo_contrato,centro_costo,fecha_termino,categoria,faena_id,es_transporte,funcion_contrato,nacionalidad,estado_civil,afp,salud,region,sueldo_base,bono_responsabilidad,bono_produccion_fijo,bono_produccion_variable,bono_produccion_tarifa,bono_produccion_detalle,semana_corrida,asig_colacion,asig_movilizacion,asig_viatico,tiene_alimentacion,alimentacion_detalle,tiene_antiguedad_previa) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41) RETURNING *`,
      [b.empresa_id||null,b.nombre_completo,b.rut||null,b.cargo||null,b.especialidad||null,b.telefono||null,b.correo||null,b.participa_mantencion||false,b.valor_hora_hombre||null,b.moneda||'CLP',b.fecha_ingreso||null,parseInt(b.cotizaciones_anteriores)||0,b.observaciones||null,b.fecha_nacimiento||null,b.direccion||null,b.comuna||null,b.tipo_contrato||null,b.centro_costo||null,b.fecha_termino||null,b.categoria||'otros_faena',b.faena_id||null,b.es_transporte||false,b.funcion_contrato||null,b.nacionalidad||'Chileno(a)',b.estado_civil||null,b.afp||null,b.salud||'FONASA',b.region||'VIII del Bio Bio',parseFloat(b.sueldo_base)||0,parseFloat(b.bono_responsabilidad)||0,parseFloat(b.bono_produccion_fijo)||0,b.bono_produccion_variable||false,parseFloat(b.bono_produccion_tarifa)||0,b.bono_produccion_detalle||null,b.semana_corrida||false,parseFloat(b.asig_colacion)||0,parseFloat(b.asig_movilizacion)||0,parseFloat(b.asig_viatico)||0,b.tiene_alimentacion||false,b.alimentacion_detalle||null,b.tiene_antiguedad_previa!==false]);
    res.status(201).json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
app.put('/api/personal/:id', auth, async(req,res)=>{
  try{
    const b=req.body;
    const r=await pool.query(`UPDATE personal SET empresa_id=$1,nombre_completo=$2,rut=$3,cargo=$4,especialidad=$5,telefono=$6,correo=$7,participa_mantencion=$8,valor_hora_hombre=$9,moneda=$10,activo=$11,fecha_ingreso=$12,cotizaciones_anteriores=$13,observaciones=$14,fecha_nacimiento=$15,direccion=$16,comuna=$17,tipo_contrato=$18,centro_costo=$19,fecha_termino=$20,categoria=$21,faena_id=$22,es_transporte=$23,funcion_contrato=$24,nacionalidad=$25,estado_civil=$26,afp=$27,salud=$28,region=$29,sueldo_base=$30,bono_responsabilidad=$31,bono_produccion_fijo=$32,bono_produccion_variable=$33,bono_produccion_tarifa=$34,bono_produccion_detalle=$35,semana_corrida=$36,asig_colacion=$37,asig_movilizacion=$38,asig_viatico=$39,tiene_alimentacion=$40,alimentacion_detalle=$41,tiene_antiguedad_previa=$42 WHERE persona_id=$43 RETURNING *`,
      [b.empresa_id||null,b.nombre_completo,b.rut||null,b.cargo||null,b.especialidad||null,b.telefono||null,b.correo||null,b.participa_mantencion||false,b.valor_hora_hombre||null,b.moneda||'CLP',b.activo!==false,b.fecha_ingreso||null,parseInt(b.cotizaciones_anteriores)||0,b.observaciones||null,b.fecha_nacimiento||null,b.direccion||null,b.comuna||null,b.tipo_contrato||null,b.centro_costo||null,b.fecha_termino||null,b.categoria||'otros_faena',b.faena_id||null,b.es_transporte||false,b.funcion_contrato||null,b.nacionalidad||'Chileno(a)',b.estado_civil||null,b.afp||null,b.salud||'FONASA',b.region||'VIII del Bio Bio',parseFloat(b.sueldo_base)||0,parseFloat(b.bono_responsabilidad)||0,parseFloat(b.bono_produccion_fijo)||0,b.bono_produccion_variable||false,parseFloat(b.bono_produccion_tarifa)||0,b.bono_produccion_detalle||null,b.semana_corrida||false,parseFloat(b.asig_colacion)||0,parseFloat(b.asig_movilizacion)||0,parseFloat(b.asig_viatico)||0,b.tiene_alimentacion||false,b.alimentacion_detalle||null,b.tiene_antiguedad_previa!==false,req.params.id]);
    res.json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
app.patch('/api/personal/:id/activo', auth, async(req,res)=>{
  try{const r=await pool.query('UPDATE personal SET activo=NOT activo WHERE persona_id=$1 RETURNING *',[req.params.id]);res.json(r.rows[0]);}catch(e){res.status(400).json({error:e.message});}
});
app.delete('/api/personal/:id', auth, async(req,res)=>{
  try{
    const pid=req.params.id;
    // Check dependencies
    const deps=[];
    const otPers=await pool.query('SELECT COUNT(*) FROM mant_ot_personal WHERE persona_id=$1',[pid]);
    if(parseInt(otPers.rows[0].count)>0)deps.push('OT mantención ('+otPers.rows[0].count+')');
    const otTarea=await pool.query('SELECT COUNT(*) FROM mant_ot_tarea_personal WHERE persona_id=$1',[pid]);
    if(parseInt(otTarea.rows[0].count)>0)deps.push('tareas OT ('+otTarea.rows[0].count+')');
    const entregas=await pool.query('SELECT COUNT(*) FROM rend_entregas WHERE persona_id=$1',[pid]);
    if(parseInt(entregas.rows[0].count)>0)deps.push('rendiciones entrega ('+entregas.rows[0].count+')');
    const gastos=await pool.query('SELECT COUNT(*) FROM rend_gastos WHERE persona_id=$1',[pid]);
    if(parseInt(gastos.rows[0].count)>0)deps.push('rendiciones gasto ('+gastos.rows[0].count+')');
    const vac=await pool.query('SELECT COUNT(*) FROM vacaciones_registros WHERE persona_id=$1',[pid]);
    if(parseInt(vac.rows[0].count)>0)deps.push('vacaciones ('+vac.rows[0].count+')');
    if(deps.length>0)return res.status(400).json({error:'No se puede eliminar: tiene registros asociados en '+deps.join(', ')+'. Desactive al trabajador en su lugar.'});
    await pool.query('DELETE FROM personal WHERE persona_id=$1',[pid]);
    res.json({ok:true});
  }catch(e){
    if(e.code==='23503')return res.status(400).json({error:'No se puede eliminar: tiene registros asociados. Desactive al trabajador en su lugar.'});
    res.status(400).json({error:e.message});
  }
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

// GET líneas de una OC con info de OT asignada
app.get('/api/oc/:id/lineas', auth, async(req,res)=>{
  try{
    const r=await pool.query(`SELECT d.*,ot.numero_ot,ot.equipo_id,eq.nombre AS equipo_nombre,sc.nombre AS subcategoria_nombre,cat.nombre AS categoria_nombre FROM ordenes_compra_detalle d LEFT JOIN mant_ot ot ON d.ot_id=ot.ot_id LEFT JOIN equipos eq ON ot.equipo_id=eq.equipo_id LEFT JOIN subcategorias sc ON COALESCE(d.subcategoria_id,(SELECT p.subcategoria_id FROM productos p WHERE p.producto_id=d.producto_id))=sc.subcategoria_id LEFT JOIN categorias cat ON sc.categoria_id=cat.categoria_id WHERE d.oc_id=$1 ORDER BY d.linea_num,d.detalle_id`,[req.params.id]);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});
// OC líneas asociadas a una OT
app.get('/api/mant/ot/:id/compras', auth, async(req,res)=>{
  try{
    const r=await pool.query(`SELECT d.*,oc.numero_oc,oc.fecha_emision,oc.estado AS oc_estado,prov.nombre AS proveedor,p.nombre AS producto_nombre,p.codigo AS producto_codigo,sc.nombre AS subcategoria_nombre,cat.nombre AS categoria_nombre FROM ordenes_compra_detalle d JOIN ordenes_compra oc ON d.oc_id=oc.oc_id LEFT JOIN proveedores prov ON oc.proveedor_id=prov.proveedor_id LEFT JOIN productos p ON d.producto_id=p.producto_id LEFT JOIN subcategorias sc ON COALESCE(d.subcategoria_id,p.subcategoria_id)=sc.subcategoria_id LEFT JOIN categorias cat ON sc.categoria_id=cat.categoria_id WHERE d.ot_id=$1 ORDER BY oc.fecha_emision DESC`,[req.params.id]);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});


// Enlazar OC completa a una OT
app.patch('/api/oc/link-ot', auth, async(req,res)=>{
  try{
    const oc_id=parseInt(req.body.oc_id), ot_id=parseInt(req.body.ot_id);
    if(!oc_id||!ot_id) return res.status(400).json({error:'oc_id y ot_id requeridos'});
    console.log('[LINK-OT] oc_id=',oc_id,'ot_id=',ot_id);
    // Verificar si la OC ya está enlazada a alguna OT
    const chk=await pool.query(`SELECT d.ot_id,o.numero_ot FROM ordenes_compra_detalle d LEFT JOIN mant_ot o ON d.ot_id=o.ot_id WHERE d.oc_id=$1 AND d.ot_id IS NOT NULL LIMIT 1`,[oc_id]);
    if(chk.rows.length>0) return res.status(400).json({error:'Esta OC ya esta enlazada a '+(chk.rows[0].numero_ot||'OT #'+chk.rows[0].ot_id)+'. Debe desenlazarla primero.'});
    // Verificar columna ot_id existe
    const colChk=await pool.query(`SELECT column_name FROM information_schema.columns WHERE table_name='ordenes_compra_detalle' AND column_name='ot_id'`);
    if(colChk.rows.length===0){
      await pool.query('ALTER TABLE ordenes_compra_detalle ADD COLUMN IF NOT EXISTS ot_id INT');
      console.log('[LINK-OT] Columna ot_id creada on-demand');
    }
    const r=await pool.query(
      `UPDATE ordenes_compra_detalle SET ot_id=$1::int WHERE oc_id=$2::int RETURNING *`,
      [ot_id,oc_id]
    );
    console.log('[LINK-OT] Filas actualizadas:',r.rowCount,'ot_id en primera fila:',r.rows[0]?.ot_id);
    // Auto-insertar líneas OC como materiales de la OT
    for(const ln of r.rows){
      await pool.query(
        `INSERT INTO mant_ot_materiales(ot_id,tipo,prod_id,descripcion,cantidad,precio_unitario,costo_total,origen)
         VALUES($1,'repuesto',$2,$3,$4,$5,$6,'compra')
         ON CONFLICT DO NOTHING`,
        [ot_id, ln.producto_id||null, ln.descripcion||'Producto OC', ln.cantidad||0, ln.precio_unitario||0, (ln.cantidad||0)*(ln.precio_unitario||0)]
      );
    }
    // Verificación final
    const verify=await pool.query('SELECT ot_id FROM ordenes_compra_detalle WHERE oc_id=$1',[oc_id]);
    console.log('[LINK-OT] Verificacion - ot_id en BD:',verify.rows.map(function(r){return r.ot_id;}));
    res.json({ok:true,lineas_enlazadas:r.rowCount});
  }catch(e){console.error('[LINK-OT ERROR]',e.message);res.status(400).json({error:e.message});}
});

// Desenlazar OC de una OT
app.patch('/api/oc/unlink-ot', auth, async(req,res)=>{
  try{
    const{oc_id,ot_id}=req.body;
    if(!oc_id||!ot_id) return res.status(400).json({error:'oc_id y ot_id requeridos'});
    await pool.query(`UPDATE ordenes_compra_detalle SET ot_id=NULL WHERE oc_id=$1 AND ot_id=$2`,[oc_id,ot_id]);
    // Eliminar materiales insertados desde esa OC
    await pool.query(`DELETE FROM mant_ot_materiales WHERE ot_id=$1 AND origen='compra' AND descripcion IN (SELECT descripcion FROM ordenes_compra_detalle WHERE oc_id=$2)`,[ot_id,oc_id]);
    res.json({ok:true});
  }catch(e){res.status(400).json({error:e.message});}
});

// OCs disponibles para enlazar (no anuladas, sin enlace a otra OT)
app.get('/api/oc/disponibles-ot', auth, async(req,res)=>{
  try{
    const r=await pool.query(`SELECT DISTINCT oc.oc_id,oc.numero_oc,oc.fecha_emision,oc.estado,oc.total,pr.nombre AS proveedor FROM ordenes_compra oc LEFT JOIN proveedores pr ON oc.proveedor_id=pr.proveedor_id WHERE oc.estado NOT IN ('ANULADA') AND NOT EXISTS (SELECT 1 FROM ordenes_compra_detalle d WHERE d.oc_id=oc.oc_id AND d.ot_id IS NOT NULL) ORDER BY oc.fecha_emision DESC`);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});

// ══════════════════════════════════════════════════════
// MÓDULO RENDICIONES DE GASTO
// ══════════════════════════════════════════════════════

// ── Tablas ──
async function setupRendiciones(q){
  await q(`CREATE TABLE IF NOT EXISTS rend_entregas (
    entrega_id SERIAL PRIMARY KEY,
    persona_id INT NOT NULL REFERENCES personal(persona_id),
    empresa_id INT REFERENCES empresas(empresa_id),
    fecha DATE NOT NULL DEFAULT CURRENT_DATE,
    monto NUMERIC(14,2) NOT NULL,
    medio_pago VARCHAR(40) DEFAULT 'transferencia',
    numero_operacion VARCHAR(50),
    banco VARCHAR(60),
    observaciones TEXT,
    usuario VARCHAR(100),
    creado_en TIMESTAMP DEFAULT NOW()
  )`);
  await q(`CREATE TABLE IF NOT EXISTS rend_gastos (
    gasto_id SERIAL PRIMARY KEY,
    persona_id INT NOT NULL REFERENCES personal(persona_id),
    empresa_id INT REFERENCES empresas(empresa_id),
    fecha_gasto DATE NOT NULL DEFAULT CURRENT_DATE,
    tipo_gasto VARCHAR(40) NOT NULL DEFAULT 'otros',
    descripcion VARCHAR(300) NOT NULL,
    monto NUMERIC(14,2) NOT NULL,
    tiene_respaldo BOOLEAN DEFAULT false,
    tipo_respaldo VARCHAR(30),
    numero_documento VARCHAR(50),
    faena_id INT REFERENCES faenas(faena_id),
    equipo_id INT REFERENCES equipos(equipo_id),
    observaciones TEXT,
    estado VARCHAR(20) DEFAULT 'pendiente',
    aprobado_por VARCHAR(100),
    usuario VARCHAR(100),
    creado_en TIMESTAMP DEFAULT NOW()
  )`);
  try{await q('ALTER TABLE rend_gastos ADD COLUMN IF NOT EXISTS proveedor_id INT REFERENCES proveedores(proveedor_id)');}catch(e){}

  // ── Solicitudes ──
  await q(`CREATE TABLE IF NOT EXISTS solicitudes (
    solicitud_id SERIAL PRIMARY KEY,
    codigo SERIAL,
    empresa_id INT REFERENCES empresas(empresa_id),
    solicitante_id INT NOT NULL REFERENCES usuarios(usuario_id),
    dirigida_a_id INT NOT NULL REFERENCES usuarios(usuario_id),
    cantidad NUMERIC(10,2) DEFAULT 1,
    detalle VARCHAR(500) NOT NULL,
    subcategoria_id INT REFERENCES subcategorias(subcategoria_id),
    faena_id INT REFERENCES faenas(faena_id),
    equipo_id INT REFERENCES equipos(equipo_id),
    prioridad VARCHAR(20) DEFAULT 'normal',
    observacion TEXT,
    estado VARCHAR(20) DEFAULT 'pendiente',
    respuesta TEXT,
    respondido_en TIMESTAMP,
    completado_en TIMESTAMP,
    usuario_creador VARCHAR(100),
    creado_en TIMESTAMP DEFAULT NOW()
  )`);

  try{await q('ALTER TABLE personal ADD COLUMN IF NOT EXISTS fecha_ingreso DATE');}catch(e){}
  try{await q('ALTER TABLE personal ADD COLUMN IF NOT EXISTS cotizaciones_anteriores INT DEFAULT 0');}catch(e){}
  try{await q("ALTER TABLE personal ADD COLUMN IF NOT EXISTS categoria VARCHAR(30) DEFAULT 'otros_faena'");}catch(e){}
  try{await q('ALTER TABLE personal ADD COLUMN IF NOT EXISTS fecha_nacimiento DATE');}catch(e){}
  try{await q('ALTER TABLE personal ADD COLUMN IF NOT EXISTS direccion VARCHAR(200)');}catch(e){}
  try{await q('ALTER TABLE personal ADD COLUMN IF NOT EXISTS comuna VARCHAR(60)');}catch(e){}
  try{await q('ALTER TABLE personal ADD COLUMN IF NOT EXISTS tipo_contrato VARCHAR(30)');}catch(e){}
  try{await q('ALTER TABLE personal ADD COLUMN IF NOT EXISTS centro_costo VARCHAR(60)');}catch(e){}
  try{await q('ALTER TABLE personal ADD COLUMN IF NOT EXISTS fecha_termino DATE');}catch(e){}
  try{await q('ALTER TABLE personal ADD COLUMN IF NOT EXISTS faena_id INT REFERENCES faenas(faena_id)');}catch(e){}
  try{await q('ALTER TABLE personal ADD COLUMN IF NOT EXISTS es_transporte BOOLEAN DEFAULT false');}catch(e){}
  // Campos adicionales para generación de contrato
  try{await q('ALTER TABLE personal ADD COLUMN IF NOT EXISTS funcion_contrato TEXT');}catch(e){}
  try{await q('ALTER TABLE personal ADD COLUMN IF NOT EXISTS nacionalidad VARCHAR(50) DEFAULT \'Chileno(a)\'');}catch(e){}
  try{await q('ALTER TABLE personal ADD COLUMN IF NOT EXISTS estado_civil VARCHAR(30)');}catch(e){}
  try{await q('ALTER TABLE personal ADD COLUMN IF NOT EXISTS afp VARCHAR(30)');}catch(e){}
  try{await q('ALTER TABLE personal ADD COLUMN IF NOT EXISTS salud VARCHAR(30) DEFAULT \'FONASA\'');}catch(e){}
  try{await q('ALTER TABLE personal ADD COLUMN IF NOT EXISTS region VARCHAR(60) DEFAULT \'VIII del Bio Bio\'');}catch(e){}
  // Haberes del trabajador (estructura de remuneración)
  try{await q('ALTER TABLE personal ADD COLUMN IF NOT EXISTS sueldo_base NUMERIC(12,2) DEFAULT 0');}catch(e){}
  try{await q('ALTER TABLE personal ADD COLUMN IF NOT EXISTS bono_responsabilidad NUMERIC(12,2) DEFAULT 0');}catch(e){}
  try{await q('ALTER TABLE personal ADD COLUMN IF NOT EXISTS bono_produccion_fijo NUMERIC(12,2) DEFAULT 0');}catch(e){}
  try{await q('ALTER TABLE personal ADD COLUMN IF NOT EXISTS bono_produccion_variable BOOLEAN DEFAULT false');}catch(e){}
  try{await q('ALTER TABLE personal ADD COLUMN IF NOT EXISTS bono_produccion_tarifa NUMERIC(12,2) DEFAULT 0');}catch(e){}
  try{await q('ALTER TABLE personal ADD COLUMN IF NOT EXISTS bono_produccion_detalle TEXT');}catch(e){}
  try{await q('ALTER TABLE personal ADD COLUMN IF NOT EXISTS semana_corrida BOOLEAN DEFAULT false');}catch(e){}
  try{await q('ALTER TABLE personal ADD COLUMN IF NOT EXISTS asig_colacion NUMERIC(10,2) DEFAULT 0');}catch(e){}
  try{await q('ALTER TABLE personal ADD COLUMN IF NOT EXISTS asig_movilizacion NUMERIC(10,2) DEFAULT 0');}catch(e){}
  try{await q('ALTER TABLE personal ADD COLUMN IF NOT EXISTS asig_viatico NUMERIC(10,2) DEFAULT 0');}catch(e){}
  try{await q('ALTER TABLE personal ADD COLUMN IF NOT EXISTS tiene_alimentacion BOOLEAN DEFAULT false');}catch(e){}
  try{await q('ALTER TABLE personal ADD COLUMN IF NOT EXISTS alimentacion_detalle TEXT');}catch(e){}
  try{await q('ALTER TABLE personal ADD COLUMN IF NOT EXISTS tiene_antiguedad_previa BOOLEAN DEFAULT true');}catch(e){}

  // ── Vacaciones ──
  await q(`CREATE TABLE IF NOT EXISTS feriados_chile (
    feriado_id SERIAL PRIMARY KEY,
    fecha DATE NOT NULL UNIQUE,
    nombre VARCHAR(100) NOT NULL,
    tipo VARCHAR(30) DEFAULT 'irrenunciable'
  )`);
  await q(`CREATE TABLE IF NOT EXISTS vacaciones_registros (
    registro_id SERIAL PRIMARY KEY,
    persona_id INT NOT NULL REFERENCES personal(persona_id),
    periodo_desde DATE NOT NULL,
    periodo_hasta DATE NOT NULL,
    dias_correspondientes INT NOT NULL DEFAULT 15,
    dias_progresivos INT NOT NULL DEFAULT 0,
    vacaciones_desde DATE NOT NULL,
    vacaciones_hasta DATE NOT NULL,
    dias_habiles INT NOT NULL DEFAULT 0,
    dias_no_habiles INT NOT NULL DEFAULT 0,
    saldo_pendiente INT NOT NULL DEFAULT 0,
    observaciones TEXT,
    usuario VARCHAR(100),
    creado_en TIMESTAMP DEFAULT NOW()
  )`);

  // Seed feriados Chile 2024-2027
  try{
    const fc=await pool.query('SELECT COUNT(*) FROM feriados_chile');
    if(parseInt(fc.rows[0].count)===0){
      const feriados=[
        // 2024
        ['2024-01-01','Año Nuevo'],['2024-03-29','Viernes Santo'],['2024-03-30','Sábado Santo'],['2024-05-01','Día del Trabajo'],['2024-05-21','Día de las Glorias Navales'],['2024-06-20','Día Nacional de los Pueblos Indígenas'],['2024-06-29','San Pedro y San Pablo'],['2024-07-16','Día de la Virgen del Carmen'],['2024-08-15','Asunción de la Virgen'],['2024-09-18','Fiestas Patrias'],['2024-09-19','Día de las Glorias del Ejército'],['2024-09-20','Feriado adicional Fiestas Patrias'],['2024-10-12','Encuentro de Dos Mundos'],['2024-10-31','Día de las Iglesias Evangélicas'],['2024-11-01','Día de Todos los Santos'],['2024-12-08','Inmaculada Concepción'],['2024-12-25','Navidad'],
        // 2025
        ['2025-01-01','Año Nuevo'],['2025-04-18','Viernes Santo'],['2025-04-19','Sábado Santo'],['2025-05-01','Día del Trabajo'],['2025-05-21','Día de las Glorias Navales'],['2025-06-20','Día Nacional de los Pueblos Indígenas'],['2025-06-29','San Pedro y San Pablo'],['2025-07-16','Día de la Virgen del Carmen'],['2025-08-15','Asunción de la Virgen'],['2025-09-18','Fiestas Patrias'],['2025-09-19','Día de las Glorias del Ejército'],['2025-10-12','Encuentro de Dos Mundos'],['2025-10-31','Día de las Iglesias Evangélicas'],['2025-11-01','Día de Todos los Santos'],['2025-12-08','Inmaculada Concepción'],['2025-12-25','Navidad'],
        // 2026
        ['2026-01-01','Año Nuevo'],['2026-04-03','Viernes Santo'],['2026-04-04','Sábado Santo'],['2026-05-01','Día del Trabajo'],['2026-05-21','Día de las Glorias Navales'],['2026-06-20','Día Nacional de los Pueblos Indígenas'],['2026-06-29','San Pedro y San Pablo'],['2026-07-16','Día de la Virgen del Carmen'],['2026-08-15','Asunción de la Virgen'],['2026-09-18','Fiestas Patrias'],['2026-09-19','Día de las Glorias del Ejército'],['2026-10-12','Encuentro de Dos Mundos'],['2026-10-31','Día de las Iglesias Evangélicas'],['2026-11-01','Día de Todos los Santos'],['2026-12-08','Inmaculada Concepción'],['2026-12-25','Navidad'],
        // 2027
        ['2027-01-01','Año Nuevo'],['2027-03-26','Viernes Santo'],['2027-03-27','Sábado Santo'],['2027-05-01','Día del Trabajo'],['2027-05-21','Día de las Glorias Navales'],['2027-06-20','Día Nacional de los Pueblos Indígenas'],['2027-06-29','San Pedro y San Pablo'],['2027-07-16','Día de la Virgen del Carmen'],['2027-08-15','Asunción de la Virgen'],['2027-09-18','Fiestas Patrias'],['2027-09-19','Día de las Glorias del Ejército'],['2027-10-12','Encuentro de Dos Mundos'],['2027-10-31','Día de las Iglesias Evangélicas'],['2027-11-01','Día de Todos los Santos'],['2027-12-08','Inmaculada Concepción'],['2027-12-25','Navidad']
      ];
      for(const[f,n] of feriados){await pool.query('INSERT INTO feriados_chile(fecha,nombre) VALUES($1,$2) ON CONFLICT(fecha) DO NOTHING',[f,n]);}
      console.log('  [OK] Feriados Chile cargados ('+feriados.length+')');
    }
  }catch(e){console.log('[WARN] seed feriados:',e.message);}

  // ── Finanzas: Cuentas bancarias y cheques ──
  await q(`CREATE TABLE IF NOT EXISTS fin_cuentas_bancarias (
    cuenta_id SERIAL PRIMARY KEY,
    empresa_id INT NOT NULL REFERENCES empresas(empresa_id),
    banco VARCHAR(60) NOT NULL,
    tipo_cuenta VARCHAR(30) DEFAULT 'corriente',
    numero_cuenta VARCHAR(30) NOT NULL,
    moneda VARCHAR(10) DEFAULT 'CLP',
    activo BOOLEAN DEFAULT true,
    creado_en TIMESTAMP DEFAULT NOW(),
    UNIQUE(empresa_id,numero_cuenta)
  )`);
  await q(`CREATE TABLE IF NOT EXISTS fin_cheques (
    cheque_id SERIAL PRIMARY KEY,
    empresa_id INT NOT NULL REFERENCES empresas(empresa_id),
    cuenta_id INT NOT NULL REFERENCES fin_cuentas_bancarias(cuenta_id),
    numero_cheque VARCHAR(20) NOT NULL,
    fecha_emision DATE NOT NULL DEFAULT CURRENT_DATE,
    fecha_cobro DATE NOT NULL,
    monto NUMERIC(14,2) NOT NULL,
    tipo_beneficiario VARCHAR(20) DEFAULT 'proveedor',
    proveedor_id INT REFERENCES proveedores(proveedor_id),
    beneficiario_nombre VARCHAR(150),
    concepto VARCHAR(40) NOT NULL DEFAULT 'pago_factura',
    concepto_detalle VARCHAR(300),
    estado VARCHAR(20) DEFAULT 'emitido',
    fecha_pago DATE,
    observaciones TEXT,
    usuario VARCHAR(100),
    creado_en TIMESTAMP DEFAULT NOW()
  )`);

  // ── Terreno: Registros diarios y tiempos obvios ──
  await q(`CREATE TABLE IF NOT EXISTS terreno_tob_categorias (
    tob_cat_id SERIAL PRIMARY KEY,
    codigo VARCHAR(10),
    clasificacion VARCHAR(20),
    causa VARCHAR(200) NOT NULL UNIQUE,
    orden INT DEFAULT 0,
    activo BOOLEAN DEFAULT true
  )`);
  await q(`CREATE TABLE IF NOT EXISTS terreno_registros (
    registro_id SERIAL PRIMARY KEY,
    fecha DATE NOT NULL,
    faena_id INT NOT NULL REFERENCES faenas(faena_id),
    equipo_id INT NOT NULL REFERENCES equipos(equipo_id),
    horometro_inicial NUMERIC(10,2) NOT NULL,
    horometro_final NUMERIC(10,2) NOT NULL,
    horas_trabajadas NUMERIC(8,2) GENERATED ALWAYS AS (horometro_final - horometro_inicial) STORED,
    horas_perdidas NUMERIC(8,2) DEFAULT 0,
    litros_combustible NUMERIC(10,2) DEFAULT 0,
    estanque_id INT REFERENCES comb_estanques(estanque_id),
    observaciones TEXT,
    usuario VARCHAR(100),
    creado_en TIMESTAMP DEFAULT NOW(),
    UNIQUE(fecha,equipo_id)
  )`);
  await q(`CREATE TABLE IF NOT EXISTS terreno_tob_detalle (
    detalle_id SERIAL PRIMARY KEY,
    registro_id INT NOT NULL REFERENCES terreno_registros(registro_id) ON DELETE CASCADE,
    tob_cat_id INT NOT NULL REFERENCES terreno_tob_categorias(tob_cat_id),
    clasificacion VARCHAR(5) DEFAULT 'E',
    horas NUMERIC(6,2) NOT NULL,
    observacion VARCHAR(300)
  )`);
  try{await q("ALTER TABLE terreno_tob_detalle ADD COLUMN IF NOT EXISTS clasificacion VARCHAR(5) DEFAULT 'E'");}catch(e){}

  // Seed categorías TOB
  try{
    const c=await pool.query('SELECT COUNT(*) FROM terreno_tob_categorias');
    if(parseInt(c.rows[0].count)===0){
      const cats=[
        'MONITOREO Y/O PARALIZACIÓN FORMIN','DETENIDO POR PASO DE VEHÍCULOS','DETENIDO POR CARGUÍO DE CAMIONES',
        'CLIMA Y/O INCENDIO DETIENE OPERACIÓN','ATOCHAMIENTO EN CANCHA','ATRASO INICIO O SALIDA TEMPRANA DE FAENA',
        'CONTROL A FAENA','FALTA DE PERSONAL','CHARLA, CAPACITACIONES O REUNIONES','PUESTA EN MARCHA',
        'BAÑO OPERADOR','MANTENCIÓN PROGRAMADA','CARGA COMBUSTIBLE O ACEITE','FALLA EQUIPO',
        'TRASLADO DE FUNDO','DETENIDO POR FALTA DE VOLTEO','DETENIDO POR FALTA DE ORDENAMIENTO',
        'TRASLADO DENTRO FAENA','DETENIDO POR FALTA DE MADEREO','CAMBIO DE CADENA O ESPADA',
        'MEDICIÓN DE LARGO DE TROZOS','OTROS'
      ];
      for(let i=0;i<cats.length;i++){
        await pool.query("INSERT INTO terreno_tob_categorias(clasificacion,causa,orden) VALUES('E - F',$1,$2) ON CONFLICT(causa) DO NOTHING",[cats[i],i+1]);
      }
      console.log('  [OK] Categorías TOB cargadas ('+cats.length+')');
    }
  }catch(e){console.log('[WARN] seed tob:',e.message);}
}

// ── Entregas de fondos ──
app.get('/api/rend/entregas', auth, async(req,res)=>{
  try{
    const{persona_id,desde,hasta}=req.query;
    let w=['1=1'],v=[];
    if(persona_id){v.push(persona_id);w.push(`e.persona_id=$${v.length}`);}
    if(desde){v.push(desde);w.push(`e.fecha>=$${v.length}`);}
    if(hasta){v.push(hasta);w.push(`e.fecha<=$${v.length}`);}
    const r=await pool.query(`SELECT e.*,p.nombre_completo,emp.razon_social AS empresa_nombre FROM rend_entregas e JOIN personal p ON e.persona_id=p.persona_id LEFT JOIN empresas emp ON e.empresa_id=emp.empresa_id WHERE ${w.join(' AND ')} ORDER BY e.fecha DESC,e.entrega_id DESC`,v);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});
app.post('/api/rend/entregas', auth, async(req,res)=>{
  try{
    const{persona_id,empresa_id,fecha,monto,medio_pago,numero_operacion,banco,observaciones}=req.body;
    if(!persona_id||!monto) return res.status(400).json({error:'Persona y monto requeridos'});
    const r=await pool.query('INSERT INTO rend_entregas(persona_id,empresa_id,fecha,monto,medio_pago,numero_operacion,banco,observaciones,usuario) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING *',
      [persona_id,empresa_id||null,fecha||new Date().toISOString().split('T')[0],parseFloat(monto),medio_pago||'transferencia',numero_operacion||null,banco||null,observaciones||null,req.user.email]);
    res.status(201).json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
app.delete('/api/rend/entregas/:id', auth, async(req,res)=>{
  try{await pool.query('DELETE FROM rend_entregas WHERE entrega_id=$1',[req.params.id]);res.json({ok:true});}catch(e){res.status(400).json({error:e.message});}
});
app.put('/api/rend/entregas/:id', auth, async(req,res)=>{
  try{
    const{persona_id,empresa_id,fecha,monto,medio_pago,numero_operacion,banco,observaciones}=req.body;
    const r=await pool.query('UPDATE rend_entregas SET persona_id=$1,empresa_id=$2,fecha=$3,monto=$4,medio_pago=$5,numero_operacion=$6,banco=$7,observaciones=$8 WHERE entrega_id=$9 RETURNING *',
      [persona_id,empresa_id||null,fecha,parseFloat(monto),medio_pago||'transferencia',numero_operacion||null,banco||null,observaciones||null,req.params.id]);
    res.json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});

// ── Gastos rendidos ──
app.get('/api/rend/gastos', auth, async(req,res)=>{
  try{
    const{persona_id,desde,hasta,estado}=req.query;
    let w=['1=1'],v=[];
    if(persona_id){v.push(persona_id);w.push(`g.persona_id=$${v.length}`);}
    if(desde){v.push(desde);w.push(`g.fecha_gasto>=$${v.length}`);}
    if(hasta){v.push(hasta);w.push(`g.fecha_gasto<=$${v.length}`);}
    if(estado){v.push(estado);w.push(`g.estado=$${v.length}`);}
    const r=await pool.query(`SELECT g.*,p.nombre_completo,emp.razon_social AS empresa_nombre,f.nombre AS faena_nombre,eq.nombre AS equipo_nombre,pv.nombre AS proveedor_nombre FROM rend_gastos g JOIN personal p ON g.persona_id=p.persona_id LEFT JOIN empresas emp ON g.empresa_id=emp.empresa_id LEFT JOIN faenas f ON g.faena_id=f.faena_id LEFT JOIN equipos eq ON g.equipo_id=eq.equipo_id LEFT JOIN proveedores pv ON g.proveedor_id=pv.proveedor_id WHERE ${w.join(' AND ')} ORDER BY g.fecha_gasto DESC,g.gasto_id DESC`,v);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});
app.post('/api/rend/gastos', auth, async(req,res)=>{
  try{
    const{persona_id,empresa_id,fecha_gasto,tipo_gasto,descripcion,monto,tiene_respaldo,tipo_respaldo,numero_documento,faena_id,equipo_id,proveedor_id,observaciones}=req.body;
    if(!persona_id||!descripcion||!monto) return res.status(400).json({error:'Persona, descripción y monto requeridos'});
    const r=await pool.query('INSERT INTO rend_gastos(persona_id,empresa_id,fecha_gasto,tipo_gasto,descripcion,monto,tiene_respaldo,tipo_respaldo,numero_documento,faena_id,equipo_id,proveedor_id,observaciones,usuario) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) RETURNING *',
      [persona_id,empresa_id||null,fecha_gasto||new Date().toISOString().split('T')[0],tipo_gasto||'otros',descripcion,parseFloat(monto),tiene_respaldo||false,tipo_respaldo||null,numero_documento||null,faena_id||null,equipo_id||null,proveedor_id||null,observaciones||null,req.user.email]);
    res.status(201).json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
app.put('/api/rend/gastos/:id', auth, async(req,res)=>{
  try{
    const{fecha_gasto,tipo_gasto,descripcion,monto,tiene_respaldo,tipo_respaldo,numero_documento,faena_id,equipo_id,proveedor_id,observaciones,estado}=req.body;
    const r=await pool.query('UPDATE rend_gastos SET fecha_gasto=$1,tipo_gasto=$2,descripcion=$3,monto=$4,tiene_respaldo=$5,tipo_respaldo=$6,numero_documento=$7,faena_id=$8,equipo_id=$9,proveedor_id=$10,observaciones=$11,estado=COALESCE($12,estado) WHERE gasto_id=$13 RETURNING *',
      [fecha_gasto,tipo_gasto||'otros',descripcion,parseFloat(monto),tiene_respaldo||false,tipo_respaldo||null,numero_documento||null,faena_id||null,equipo_id||null,proveedor_id||null,observaciones||null,estado||null,req.params.id]);
    res.json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
app.post('/api/rend/gastos/bulk', auth, async(req,res)=>{
  const client=await pool.connect();
  try{
    await client.query('BEGIN');
    const{persona_id,empresa_id,lineas}=req.body;
    if(!persona_id||!Array.isArray(lineas)||!lineas.length) throw new Error('Persona y al menos un gasto requeridos');
    const results=[];
    for(const l of lineas){
      const r=await client.query('INSERT INTO rend_gastos(persona_id,empresa_id,fecha_gasto,tipo_gasto,descripcion,monto,tiene_respaldo,tipo_respaldo,numero_documento,faena_id,equipo_id,proveedor_id,observaciones,usuario) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) RETURNING *',
        [persona_id,empresa_id||null,l.fecha_gasto||new Date().toISOString().split('T')[0],l.tipo_gasto||'otros',l.descripcion,parseFloat(l.monto),l.tiene_respaldo||false,l.tipo_respaldo||null,l.numero_documento||null,l.faena_id||null,l.equipo_id||null,l.proveedor_id||null,l.observaciones||null,req.user.email]);
      results.push(r.rows[0]);
    }
    await client.query('COMMIT');
    res.status(201).json({ok:true,count:results.length});
  }catch(e){await client.query('ROLLBACK');res.status(400).json({error:e.message});}
  finally{client.release();}
});
app.patch('/api/rend/gastos/:id/aprobar', auth, async(req,res)=>{
  try{const r=await pool.query("UPDATE rend_gastos SET estado='aprobado',aprobado_por=$1 WHERE gasto_id=$2 RETURNING *",[req.user.email,req.params.id]);res.json(r.rows[0]);}catch(e){res.status(400).json({error:e.message});}
});
app.patch('/api/rend/gastos/:id/rechazar', auth, async(req,res)=>{
  try{const r=await pool.query("UPDATE rend_gastos SET estado='rechazado',aprobado_por=$1 WHERE gasto_id=$2 RETURNING *",[req.user.email,req.params.id]);res.json(r.rows[0]);}catch(e){res.status(400).json({error:e.message});}
});
app.delete('/api/rend/gastos/:id', auth, async(req,res)=>{
  try{await pool.query('DELETE FROM rend_gastos WHERE gasto_id=$1',[req.params.id]);res.json({ok:true});}catch(e){res.status(400).json({error:e.message});}
});

// ── Saldos por persona ──
app.get('/api/rend/saldos', auth, async(req,res)=>{
  try{
    const{empresa_id}=req.query;
    let empFilt=empresa_id?' AND p.empresa_id='+parseInt(empresa_id):'';
    const r=await pool.query(`SELECT p.persona_id,p.nombre_completo,p.rut,p.cargo,emp.razon_social AS empresa_nombre,
      COALESCE((SELECT SUM(e.monto) FROM rend_entregas e WHERE e.persona_id=p.persona_id),0) AS total_entregas,
      COALESCE((SELECT SUM(g.monto) FROM rend_gastos g WHERE g.persona_id=p.persona_id AND g.estado!='rechazado'),0) AS total_gastos,
      COALESCE((SELECT SUM(e.monto) FROM rend_entregas e WHERE e.persona_id=p.persona_id),0)-COALESCE((SELECT SUM(g.monto) FROM rend_gastos g WHERE g.persona_id=p.persona_id AND g.estado!='rechazado'),0) AS saldo,
      (SELECT COUNT(*) FROM rend_gastos g2 WHERE g2.persona_id=p.persona_id AND g2.estado='pendiente') AS gastos_pendientes,
      (SELECT MAX(e2.fecha) FROM rend_entregas e2 WHERE e2.persona_id=p.persona_id) AS ultima_entrega,
      (SELECT MAX(g3.fecha_gasto) FROM rend_gastos g3 WHERE g3.persona_id=p.persona_id) AS ultimo_gasto
      FROM personal p LEFT JOIN empresas emp ON p.empresa_id=emp.empresa_id
      WHERE p.activo=true${empFilt}
      AND (EXISTS(SELECT 1 FROM rend_entregas e WHERE e.persona_id=p.persona_id) OR EXISTS(SELECT 1 FROM rend_gastos g WHERE g.persona_id=p.persona_id))
      ORDER BY saldo DESC`);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});

// ══ FINANZAS — CUENTAS BANCARIAS Y CHEQUES ══
app.get('/api/fin/cuentas', auth, async(req,res)=>{
  try{res.json((await pool.query('SELECT c.*,e.razon_social AS empresa_nombre FROM fin_cuentas_bancarias c JOIN empresas e ON c.empresa_id=e.empresa_id ORDER BY e.razon_social,c.banco')).rows);}catch(e){res.status(500).json({error:e.message});}
});
app.put('/api/fin/cuentas/:id', auth, async(req,res)=>{
  try{const{banco,numero_cuenta,tipo_cuenta}=req.body;
  const r=await pool.query('UPDATE fin_cuentas_bancarias SET banco=$1,numero_cuenta=$2,tipo_cuenta=$3 WHERE cuenta_id=$4 RETURNING *',[banco,numero_cuenta,tipo_cuenta||'corriente',req.params.id]);
  res.json(r.rows[0]);}catch(e){res.status(400).json({error:e.message});}
});
app.post('/api/fin/cuentas', auth, async(req,res)=>{
  try{const{empresa_id,banco,numero_cuenta,tipo_cuenta}=req.body;
  if(!empresa_id||!banco||!numero_cuenta)return res.status(400).json({error:'Empresa, banco y número de cuenta son obligatorios'});
  const r=await pool.query('INSERT INTO fin_cuentas_bancarias(empresa_id,banco,tipo_cuenta,numero_cuenta) VALUES($1,$2,$3,$4) RETURNING *',[empresa_id,banco,tipo_cuenta||'corriente',numero_cuenta]);
  res.status(201).json(r.rows[0]);}catch(e){if(e.code==='23505')return res.status(400).json({error:'Esta cuenta ya existe para esa empresa'});res.status(400).json({error:e.message});}
});
app.delete('/api/fin/cuentas/:id', auth, async(req,res)=>{
  try{await pool.query('DELETE FROM fin_cuentas_bancarias WHERE cuenta_id=$1',[req.params.id]);res.json({ok:true});}catch(e){if(e.code==='23503')return res.status(400).json({error:'Cuenta en uso, no se puede eliminar'});res.status(400).json({error:e.message});}
});
app.get('/api/fin/cheques', auth, async(req,res)=>{
  try{
    const{empresa_id,cuenta_id,estado,desde,hasta}=req.query;
    let w=['1=1'],v=[];
    if(empresa_id){v.push(empresa_id);w.push(`ch.empresa_id=$${v.length}`);}
    if(cuenta_id){v.push(cuenta_id);w.push(`ch.cuenta_id=$${v.length}`);}
    if(estado){v.push(estado);w.push(`ch.estado=$${v.length}`);}
    if(desde){v.push(desde);w.push(`ch.fecha_cobro>=$${v.length}`);}
    if(hasta){v.push(hasta);w.push(`ch.fecha_cobro<=$${v.length}`);}
    const r=await pool.query(`SELECT ch.*,c.banco,c.numero_cuenta,e.razon_social AS empresa_nombre,p.nombre AS proveedor_nombre FROM fin_cheques ch JOIN fin_cuentas_bancarias c ON ch.cuenta_id=c.cuenta_id JOIN empresas e ON ch.empresa_id=e.empresa_id LEFT JOIN proveedores p ON ch.proveedor_id=p.proveedor_id WHERE ${w.join(' AND ')} ORDER BY ch.fecha_cobro ASC, ch.cheque_id DESC`,v);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});
app.post('/api/fin/cheques', auth, async(req,res)=>{
  try{
    const{empresa_id,cuenta_id,numero_cheque,fecha_emision,fecha_cobro,monto,tipo_beneficiario,proveedor_id,beneficiario_nombre,concepto,concepto_detalle,observaciones}=req.body;
    if(!cuenta_id||!numero_cheque||!fecha_cobro||!monto)return res.status(400).json({error:'Cuenta, N° cheque, fecha cobro y monto son obligatorios'});
    const cuenta=await pool.query('SELECT empresa_id FROM fin_cuentas_bancarias WHERE cuenta_id=$1',[cuenta_id]);
    const empId=empresa_id||cuenta.rows[0]?.empresa_id;
    const benNombre=tipo_beneficiario==='proveedor'&&proveedor_id?(await pool.query('SELECT nombre FROM proveedores WHERE proveedor_id=$1',[proveedor_id])).rows[0]?.nombre:beneficiario_nombre;
    const r=await pool.query('INSERT INTO fin_cheques(empresa_id,cuenta_id,numero_cheque,fecha_emision,fecha_cobro,monto,tipo_beneficiario,proveedor_id,beneficiario_nombre,concepto,concepto_detalle,observaciones,usuario) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) RETURNING *',
      [empId,cuenta_id,numero_cheque,fecha_emision||new Date().toISOString().slice(0,10),fecha_cobro,parseFloat(monto),tipo_beneficiario||'proveedor',tipo_beneficiario==='proveedor'?proveedor_id:null,benNombre||null,concepto||'pago_factura',concepto_detalle||null,observaciones||null,req.user.email]);
    res.status(201).json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
app.put('/api/fin/cheques/:id', auth, async(req,res)=>{
  try{
    const{cuenta_id,numero_cheque,fecha_emision,fecha_cobro,monto,tipo_beneficiario,proveedor_id,beneficiario_nombre,concepto,concepto_detalle,observaciones}=req.body;
    const benNombre=tipo_beneficiario==='proveedor'&&proveedor_id?(await pool.query('SELECT nombre FROM proveedores WHERE proveedor_id=$1',[proveedor_id])).rows[0]?.nombre:beneficiario_nombre;
    const r=await pool.query('UPDATE fin_cheques SET cuenta_id=$1,numero_cheque=$2,fecha_emision=$3,fecha_cobro=$4,monto=$5,tipo_beneficiario=$6,proveedor_id=$7,beneficiario_nombre=$8,concepto=$9,concepto_detalle=$10,observaciones=$11 WHERE cheque_id=$12 RETURNING *',
      [cuenta_id,numero_cheque,fecha_emision,fecha_cobro,parseFloat(monto),tipo_beneficiario,tipo_beneficiario==='proveedor'?proveedor_id:null,benNombre||null,concepto,concepto_detalle||null,observaciones||null,req.params.id]);
    res.json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
app.patch('/api/fin/cheques/:id/estado', auth, async(req,res)=>{
  try{const{estado}=req.body;
  const upd=estado==='pagado'?'estado=$1,fecha_pago=CURRENT_DATE':'estado=$1,fecha_pago=NULL';
  const r=await pool.query(`UPDATE fin_cheques SET ${upd} WHERE cheque_id=$2 RETURNING *`,[estado,req.params.id]);
  res.json(r.rows[0]);}catch(e){res.status(400).json({error:e.message});}
});
app.delete('/api/fin/cheques/:id', auth, async(req,res)=>{
  try{await pool.query('DELETE FROM fin_cheques WHERE cheque_id=$1',[req.params.id]);res.json({ok:true});}catch(e){res.status(400).json({error:e.message});}
});
app.get('/api/fin/dashboard', auth, async(req,res)=>{
  try{
    const{empresa_id}=req.query;
    let empFilter=empresa_id?` AND ch.empresa_id=${parseInt(empresa_id)}`:'';
    const porMes=await pool.query(`SELECT TO_CHAR(ch.fecha_cobro,'YYYY-MM') AS mes,ch.empresa_id,e.razon_social,SUM(ch.monto) AS total,COUNT(*) AS cantidad FROM fin_cheques ch JOIN empresas e ON ch.empresa_id=e.empresa_id WHERE ch.estado='emitido'${empFilter} GROUP BY mes,ch.empresa_id,e.razon_social ORDER BY mes`);
    const porBanco=await pool.query(`SELECT c.banco,ch.empresa_id,e.razon_social,SUM(ch.monto) AS total,COUNT(*) AS cantidad FROM fin_cheques ch JOIN fin_cuentas_bancarias c ON ch.cuenta_id=c.cuenta_id JOIN empresas e ON ch.empresa_id=e.empresa_id WHERE ch.estado='emitido'${empFilter} GROUP BY c.banco,ch.empresa_id,e.razon_social ORDER BY c.banco`);
    const totales=await pool.query(`SELECT SUM(CASE WHEN estado='emitido' THEN monto ELSE 0 END) AS pendiente,SUM(CASE WHEN estado='pagado' THEN monto ELSE 0 END) AS pagado,SUM(CASE WHEN estado='anulado' THEN monto ELSE 0 END) AS anulado,COUNT(*) FILTER(WHERE estado='emitido') AS cant_pendiente,COUNT(*) FILTER(WHERE estado='pagado') AS cant_pagado,COUNT(*) AS total FROM fin_cheques ch WHERE 1=1${empFilter}`);
    res.json({porMes:porMes.rows,porBanco:porBanco.rows,totales:totales.rows[0]||{}});
  }catch(e){res.status(500).json({error:e.message});}
});

// ══ IMPORTACIÓN MASIVA DE PERSONAL ══
app.post('/api/import/personal', auth, async(req,res)=>{
  try{
    const{trabajadores}=req.body;if(!Array.isArray(trabajadores))throw new Error('Sin datos');
    // Mapeo centro_costo (planilla) → código faena (sistema)
    const CENTRO_COSTO_MAP={
      // Leonidas Poo
      'ADMINISTRACIÓN':'ADM-LPOO','ADMINISTRACION':'ADM-LPOO',
      'FAENA MEC 3':'FAE-MEC3','FAENA MEC3':'FAE-MEC3',
      'FAENA MEC 4':'FAE-MEC4','FAENA MEC4':'FAE-MEC4',
      'TALLER':'TALL-LPOO',
      // Emprecon
      'ADMINISTRACIÓN EMPRECON':'ADM-EMP','ADMINISTRACION EMPRECON':'ADM-EMP',
      'FAENA CAMINOS':'FAE-CAM','FAENA ALCANTARILLAS':'FAE-ALC',
      'FAENA FAJAS':'FAE-FAJ','TALLER EMPRECON':'TALL-EMP'
    };
    // Pre-cargar faenas para lookup
    const faenasQ=await pool.query('SELECT faena_id,codigo,nombre FROM faenas');
    const faenasByCodigo={};const faenasByNombre={};
    for(const f of faenasQ.rows){
      faenasByCodigo[(f.codigo||'').toUpperCase()]=f.faena_id;
      faenasByNombre[(f.nombre||'').toUpperCase()]=f.faena_id;
    }
    // Resolver faena_id desde centro_costo
    function resolverFaena(cc){
      if(!cc)return null;
      var ccUp=cc.toUpperCase().trim();
      var codigo=CENTRO_COSTO_MAP[ccUp];
      if(codigo&&faenasByCodigo[codigo.toUpperCase()])return faenasByCodigo[codigo.toUpperCase()];
      // Fallback: buscar por nombre exacto
      if(faenasByNombre[ccUp])return faenasByNombre[ccUp];
      // Fallback: buscar por nombre parcial
      for(const f of faenasQ.rows){if((f.nombre||'').toUpperCase().indexOf(ccUp)>=0||(f.codigo||'').toUpperCase().indexOf(ccUp)>=0)return f.faena_id;}
      return null;
    }
    const results=[];
    for(const t of trabajadores){
      try{
        if(!t.nombre_completo)continue;
        var fid=resolverFaena(t.centro_costo);
        const exists=t.rut?await pool.query('SELECT persona_id FROM personal WHERE rut=$1',[t.rut]):null;
        if(exists&&exists.rows.length){
          // Overwrite: sobreescribir todos los campos con los nuevos valores
          await pool.query(`UPDATE personal SET nombre_completo=$1,cargo=$2,fecha_ingreso=$3,fecha_nacimiento=$4,direccion=$5,comuna=$6,tipo_contrato=$7,centro_costo=$8,categoria=$9,fecha_termino=$10,telefono=$11,correo=$12,empresa_id=COALESCE($13,empresa_id),faena_id=$14 WHERE rut=$15`,
            [t.nombre_completo,t.cargo||null,t.fecha_ingreso||null,t.fecha_nacimiento||null,t.direccion||null,t.comuna||null,t.tipo_contrato||null,t.centro_costo||null,t.categoria||'otros_faena',t.fecha_termino||null,t.telefono||null,t.correo||null,t.empresa_id||null,fid,t.rut]);
          results.push({rut:t.rut,nombre:t.nombre_completo,ok:true,accion:'actualizado',faena:fid?'asignada':'sin mapeo'});
        }else{
          await pool.query(`INSERT INTO personal(empresa_id,nombre_completo,rut,cargo,fecha_ingreso,fecha_nacimiento,direccion,comuna,tipo_contrato,centro_costo,categoria,fecha_termino,telefono,correo,faena_id,activo) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,true)`,
            [t.empresa_id||null,t.nombre_completo,t.rut||null,t.cargo||null,t.fecha_ingreso||null,t.fecha_nacimiento||null,t.direccion||null,t.comuna||null,t.tipo_contrato||null,t.centro_costo||null,t.categoria||'otros_faena',t.fecha_termino||null,t.telefono||null,t.correo||null,fid]);
          results.push({rut:t.rut,nombre:t.nombre_completo,ok:true,accion:'creado',faena:fid?'asignada':'sin mapeo'});
        }
      }catch(e){results.push({rut:t.rut,nombre:t.nombre_completo,ok:false,error:e.message});}
    }
    const conFaena=results.filter(function(r){return r.faena==='asignada';}).length;
    const sinFaena=results.filter(function(r){return r.faena==='sin mapeo';}).length;
    res.json({results,resumen:{con_faena:conFaena,sin_faena:sinFaena}});
  }catch(e){res.status(400).json({error:e.message});}
});

// ══ VACACIONES ══
app.get('/api/feriados', auth, async(req,res)=>{
  try{res.json((await pool.query('SELECT * FROM feriados_chile ORDER BY fecha')).rows);}catch(e){res.status(500).json({error:e.message});}
});
app.get('/api/vacaciones/calcular-dias', auth, async(req,res)=>{
  try{
    const{desde,hasta}=req.query;
    if(!desde||!hasta)return res.json({dias_habiles:0,dias_no_habiles:0,total_corridos:0});
    const feriados=(await pool.query('SELECT fecha FROM feriados_chile')).rows.map(r=>r.fecha.toISOString().slice(0,10));
    let d=new Date(desde);const h=new Date(hasta);let habiles=0,noHabiles=0;
    while(d<=h){
      const dow=d.getDay();const iso=d.toISOString().slice(0,10);
      if(dow===0||dow===6||feriados.includes(iso)){noHabiles++;}else{habiles++;}
      d.setDate(d.getDate()+1);
    }
    res.json({dias_habiles:habiles,dias_no_habiles:noHabiles,total_corridos:habiles+noHabiles});
  }catch(e){res.status(500).json({error:e.message});}
});
app.get('/api/vacaciones/fecha-termino', auth, async(req,res)=>{
  try{
    const{desde,dias_habiles}=req.query;
    if(!desde||!dias_habiles)return res.json({fecha_termino:null});
    const feriados=(await pool.query('SELECT fecha FROM feriados_chile')).rows.map(r=>r.fecha.toISOString().slice(0,10));
    let d=new Date(desde);let count=0;const target=parseInt(dias_habiles);
    while(count<target){
      const dow=d.getDay();const iso=d.toISOString().slice(0,10);
      if(dow!==0&&dow!==6&&!feriados.includes(iso)){count++;}
      if(count<target)d.setDate(d.getDate()+1);
    }
    res.json({fecha_termino:d.toISOString().slice(0,10)});
  }catch(e){res.status(500).json({error:e.message});}
});
app.get('/api/vacaciones', auth, async(req,res)=>{
  try{
    const{persona_id}=req.query;
    let w=['1=1'],v=[];
    if(persona_id){v.push(persona_id);w.push(`v.persona_id=$${v.length}`);}
    res.json((await pool.query(`SELECT v.*,p.nombre_completo,p.rut FROM vacaciones_registros v JOIN personal p ON v.persona_id=p.persona_id WHERE ${w.join(' AND ')} ORDER BY v.periodo_desde ASC, v.vacaciones_desde ASC`,v)).rows);
  }catch(e){res.status(500).json({error:e.message});}
});
app.post('/api/vacaciones', auth, async(req,res)=>{
  try{
    const{persona_id,periodo_desde,periodo_hasta,dias_correspondientes,dias_progresivos,vacaciones_desde,vacaciones_hasta,dias_habiles,dias_no_habiles,saldo_pendiente,observaciones}=req.body;
    if(!persona_id||!periodo_desde||!periodo_hasta||!vacaciones_desde||!vacaciones_hasta)return res.status(400).json({error:'Todos los campos de período y fechas son obligatorios'});
    const r=await pool.query('INSERT INTO vacaciones_registros(persona_id,periodo_desde,periodo_hasta,dias_correspondientes,dias_progresivos,vacaciones_desde,vacaciones_hasta,dias_habiles,dias_no_habiles,saldo_pendiente,observaciones,usuario) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING *',
      [persona_id,periodo_desde,periodo_hasta,parseInt(dias_correspondientes)||15,parseInt(dias_progresivos)||0,vacaciones_desde,vacaciones_hasta,parseInt(dias_habiles)||0,parseInt(dias_no_habiles)||0,parseInt(saldo_pendiente)||0,observaciones||null,req.user.email]);
    res.status(201).json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
app.put('/api/vacaciones/:id', auth, async(req,res)=>{
  try{
    const{periodo_desde,periodo_hasta,dias_correspondientes,dias_progresivos,vacaciones_desde,vacaciones_hasta,dias_habiles,dias_no_habiles,saldo_pendiente,observaciones}=req.body;
    const r=await pool.query('UPDATE vacaciones_registros SET periodo_desde=$1,periodo_hasta=$2,dias_correspondientes=$3,dias_progresivos=$4,vacaciones_desde=$5,vacaciones_hasta=$6,dias_habiles=$7,dias_no_habiles=$8,saldo_pendiente=$9,observaciones=$10 WHERE registro_id=$11 RETURNING *',
      [periodo_desde,periodo_hasta,parseInt(dias_correspondientes)||15,parseInt(dias_progresivos)||0,vacaciones_desde,vacaciones_hasta,parseInt(dias_habiles)||0,parseInt(dias_no_habiles)||0,parseInt(saldo_pendiente)||0,observaciones||null,req.params.id]);
    res.json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
app.delete('/api/vacaciones/:id', auth, async(req,res)=>{
  try{await pool.query('DELETE FROM vacaciones_registros WHERE registro_id=$1',[req.params.id]);res.json({ok:true});}catch(e){res.status(400).json({error:e.message});}
});

// ══ TERRENO — Registros diarios y tiempos obvios ══
app.get('/api/terreno/tob-categorias', auth, async(req,res)=>{
  try{res.json((await pool.query('SELECT * FROM terreno_tob_categorias WHERE activo=true ORDER BY orden,causa')).rows);}catch(e){res.status(500).json({error:e.message});}
});
app.get('/api/terreno/registros', auth, async(req,res)=>{
  try{
    const{equipo_id,faena_id,desde,hasta,mes}=req.query;
    let w=['1=1'],v=[];
    if(equipo_id){v.push(equipo_id);w.push(`r.equipo_id=$${v.length}`);}
    if(faena_id){v.push(faena_id);w.push(`r.faena_id=$${v.length}`);}
    if(desde){v.push(desde);w.push(`r.fecha>=$${v.length}`);}
    if(hasta){v.push(hasta);w.push(`r.fecha<=$${v.length}`);}
    if(mes){v.push(mes);w.push(`TO_CHAR(r.fecha,'YYYY-MM')=$${v.length}`);}
    const rs=await pool.query(`SELECT r.*,e.codigo AS equipo_codigo,e.nombre AS equipo_nombre,f.nombre AS faena_nombre,es.codigo AS estanque_codigo,es.nombre AS estanque_nombre FROM terreno_registros r JOIN equipos e ON r.equipo_id=e.equipo_id JOIN faenas f ON r.faena_id=f.faena_id LEFT JOIN comb_estanques es ON r.estanque_id=es.estanque_id WHERE ${w.join(' AND ')} ORDER BY r.fecha DESC, r.registro_id DESC`,v);
    res.json(rs.rows);
  }catch(e){res.status(500).json({error:e.message});}
});
app.get('/api/terreno/registros/:id/tob', auth, async(req,res)=>{
  try{res.json((await pool.query('SELECT d.*,c.causa,c.clasificacion FROM terreno_tob_detalle d JOIN terreno_tob_categorias c ON d.tob_cat_id=c.tob_cat_id WHERE d.registro_id=$1 ORDER BY d.detalle_id',[req.params.id])).rows);}catch(e){res.status(500).json({error:e.message});}
});
app.post('/api/terreno/registros', auth, async(req,res)=>{
  const client=await pool.connect();
  try{
    await client.query('BEGIN');
    const{fecha,faena_id,equipo_id,horometro_inicial,horometro_final,horas_perdidas,litros_combustible,estanque_id,observaciones,tob_detalle}=req.body;
    if(!fecha||!faena_id||!equipo_id||horometro_inicial==null||horometro_final==null)throw new Error('Fecha, faena, equipo y horómetros son obligatorios');
    if(parseFloat(horometro_final)<parseFloat(horometro_inicial))throw new Error('Horómetro final debe ser mayor o igual al inicial');
    // Validate TOB sum = horas_perdidas
    const hp=parseFloat(horas_perdidas||0);
    const detalle=Array.isArray(tob_detalle)?tob_detalle:[];
    const sumTob=detalle.reduce(function(s,d){return s+(parseFloat(d.horas)||0);},0);
    if(hp>0&&Math.abs(hp-sumTob)>0.01)throw new Error(`La suma del desglose de tiempos obvios (${sumTob}) no coincide con las horas perdidas (${hp})`);
    const r=await client.query('INSERT INTO terreno_registros(fecha,faena_id,equipo_id,horometro_inicial,horometro_final,horas_perdidas,litros_combustible,estanque_id,observaciones,usuario) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING *',
      [fecha,faena_id,equipo_id,parseFloat(horometro_inicial),parseFloat(horometro_final),hp,parseFloat(litros_combustible||0),estanque_id||null,observaciones||null,req.user.email]);
    const regId=r.rows[0].registro_id;
    for(const d of detalle){
      if(d.tob_cat_id&&parseFloat(d.horas)>0){
        await client.query('INSERT INTO terreno_tob_detalle(registro_id,tob_cat_id,clasificacion,horas,observacion) VALUES($1,$2,$3,$4,$5)',[regId,d.tob_cat_id,d.clasificacion||'E',parseFloat(d.horas),d.observacion||null]);
      }
    }
    // Actualizar horómetro del equipo si el final es mayor al actual
    await client.query('UPDATE equipos SET horometro_actual=GREATEST(COALESCE(horometro_actual,0),$1) WHERE equipo_id=$2',[parseFloat(horometro_final),equipo_id]);
    await client.query('COMMIT');
    res.status(201).json(r.rows[0]);
  }catch(e){await client.query('ROLLBACK');res.status(400).json({error:e.message});}
  finally{client.release();}
});
app.put('/api/terreno/registros/:id', auth, async(req,res)=>{
  const client=await pool.connect();
  try{
    await client.query('BEGIN');
    const{fecha,faena_id,equipo_id,horometro_inicial,horometro_final,horas_perdidas,litros_combustible,estanque_id,observaciones,tob_detalle}=req.body;
    if(parseFloat(horometro_final)<parseFloat(horometro_inicial))throw new Error('Horómetro final debe ser mayor o igual al inicial');
    const hp=parseFloat(horas_perdidas||0);
    const detalle=Array.isArray(tob_detalle)?tob_detalle:[];
    const sumTob=detalle.reduce(function(s,d){return s+(parseFloat(d.horas)||0);},0);
    if(hp>0&&Math.abs(hp-sumTob)>0.01)throw new Error(`La suma del desglose (${sumTob}) no coincide con las horas perdidas (${hp})`);
    await client.query('UPDATE terreno_registros SET fecha=$1,faena_id=$2,equipo_id=$3,horometro_inicial=$4,horometro_final=$5,horas_perdidas=$6,litros_combustible=$7,estanque_id=$8,observaciones=$9 WHERE registro_id=$10',
      [fecha,faena_id,equipo_id,parseFloat(horometro_inicial),parseFloat(horometro_final),hp,parseFloat(litros_combustible||0),estanque_id||null,observaciones||null,req.params.id]);
    await client.query('DELETE FROM terreno_tob_detalle WHERE registro_id=$1',[req.params.id]);
    for(const d of detalle){
      if(d.tob_cat_id&&parseFloat(d.horas)>0){
        await client.query('INSERT INTO terreno_tob_detalle(registro_id,tob_cat_id,clasificacion,horas,observacion) VALUES($1,$2,$3,$4,$5)',[req.params.id,d.tob_cat_id,d.clasificacion||'E',parseFloat(d.horas),d.observacion||null]);
      }
    }
    await client.query('COMMIT');
    res.json({ok:true});
  }catch(e){await client.query('ROLLBACK');res.status(400).json({error:e.message});}
  finally{client.release();}
});
app.delete('/api/terreno/registros/:id', auth, async(req,res)=>{
  try{await pool.query('DELETE FROM terreno_registros WHERE registro_id=$1',[req.params.id]);res.json({ok:true});}catch(e){res.status(400).json({error:e.message});}
});
app.get('/api/terreno/rendimiento-mensual', auth, async(req,res)=>{
  try{
    const{equipo_id,mes}=req.query;
    if(!equipo_id||!mes)return res.json({horas:0,litros:0,rendimiento:null});
    const r=await pool.query(`SELECT COALESCE(SUM(horas_trabajadas),0) AS horas, COALESCE(SUM(litros_combustible),0) AS litros FROM terreno_registros WHERE equipo_id=$1 AND TO_CHAR(fecha,'YYYY-MM')=$2`,[equipo_id,mes]);
    const h=parseFloat(r.rows[0].horas)||0;const l=parseFloat(r.rows[0].litros)||0;
    res.json({horas:h,litros:l,rendimiento:h>0?l/h:null});
  }catch(e){res.status(500).json({error:e.message});}
});

app.get('/api/terreno/ultimo-horometro/:equipo_id', auth, async(req,res)=>{
  try{const r=await pool.query('SELECT horometro_final FROM terreno_registros WHERE equipo_id=$1 ORDER BY fecha DESC, registro_id DESC LIMIT 1',[req.params.equipo_id]);
  res.json({horometro_final:r.rows.length?parseFloat(r.rows[0].horometro_final):null});}catch(e){res.status(500).json({error:e.message});}
});

// ══ SOLICITUDES ══
app.get('/api/solicitudes', auth, async(req,res)=>{
  try{
    const{estado,dirigida_a_id,solicitante_id}=req.query;
    let w=['1=1'],v=[];
    if(estado){v.push(estado);w.push(`s.estado=$${v.length}`);}
    if(dirigida_a_id){v.push(dirigida_a_id);w.push(`s.dirigida_a_id=$${v.length}`);}
    if(solicitante_id){v.push(solicitante_id);w.push(`s.solicitante_id=$${v.length}`);}
    const r=await pool.query(`SELECT s.*,sol.nombre AS solicitante_nombre,sol.email AS solicitante_email,dest.nombre AS dirigida_nombre,dest.email AS dirigida_email,emp.razon_social AS empresa_nombre,sc.nombre AS subcategoria_nombre,f.nombre AS faena_nombre,eq.nombre AS equipo_nombre,eq.codigo AS equipo_codigo FROM solicitudes s JOIN usuarios sol ON s.solicitante_id=sol.usuario_id JOIN usuarios dest ON s.dirigida_a_id=dest.usuario_id LEFT JOIN empresas emp ON s.empresa_id=emp.empresa_id LEFT JOIN subcategorias sc ON s.subcategoria_id=sc.subcategoria_id LEFT JOIN faenas f ON s.faena_id=f.faena_id LEFT JOIN equipos eq ON s.equipo_id=eq.equipo_id WHERE ${w.join(' AND ')} ORDER BY s.creado_en DESC`,v);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});
app.post('/api/solicitudes', auth, async(req,res)=>{
  try{
    const{empresa_id,dirigida_a_id,cantidad,detalle,subcategoria_id,faena_id,equipo_id,prioridad,observacion,lineas}=req.body;
    if(!dirigida_a_id) return res.status(400).json({error:'Destinatario es obligatorio'});
    // Multi-line support
    if(Array.isArray(lineas)&&lineas.length>0){
      const results=[];
      for(const l of lineas){
        if(!l.detalle)continue;
        const r=await pool.query('INSERT INTO solicitudes(empresa_id,solicitante_id,dirigida_a_id,cantidad,detalle,subcategoria_id,faena_id,equipo_id,prioridad,observacion,usuario_creador) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING *',
          [empresa_id||null,req.user.id,dirigida_a_id,parseFloat(l.cantidad)||1,l.detalle,l.subcategoria_id||null,l.faena_id||null,l.equipo_id||null,l.prioridad||prioridad||'normal',l.observacion||null,req.user.email]);
        results.push(r.rows[0]);
      }
      return res.status(201).json({ok:true,count:results.length});
    }
    // Single line
    if(!detalle) return res.status(400).json({error:'Detalle es obligatorio'});
    const r=await pool.query('INSERT INTO solicitudes(empresa_id,solicitante_id,dirigida_a_id,cantidad,detalle,subcategoria_id,faena_id,equipo_id,prioridad,observacion,usuario_creador) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING *',
      [empresa_id||null,req.user.id,dirigida_a_id,parseFloat(cantidad)||1,detalle,subcategoria_id||null,faena_id||null,equipo_id||null,prioridad||'normal',observacion||null,req.user.email]);
    res.status(201).json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
app.patch('/api/solicitudes/:id/en-curso', auth, async(req,res)=>{
  try{const{respuesta}=req.body;
  const r=await pool.query("UPDATE solicitudes SET estado='en_curso',respuesta=$1,respondido_en=NOW() WHERE solicitud_id=$2 RETURNING *",[respuesta||null,req.params.id]);
  res.json(r.rows[0]);}catch(e){res.status(400).json({error:e.message});}
});
app.patch('/api/solicitudes/:id/completar', auth, async(req,res)=>{
  try{const{respuesta}=req.body;
  const r=await pool.query("UPDATE solicitudes SET estado='completada',respuesta=COALESCE($1,respuesta),completado_en=NOW() WHERE solicitud_id=$2 RETURNING *",[respuesta||null,req.params.id]);
  res.json(r.rows[0]);}catch(e){res.status(400).json({error:e.message});}
});
app.patch('/api/solicitudes/:id/rechazar', auth, async(req,res)=>{
  try{const{respuesta}=req.body;
  const r=await pool.query("UPDATE solicitudes SET estado='rechazada',respuesta=$1,respondido_en=NOW() WHERE solicitud_id=$2 RETURNING *",[respuesta||null,req.params.id]);
  res.json(r.rows[0]);}catch(e){res.status(400).json({error:e.message});}
});
app.delete('/api/solicitudes/:id', auth, async(req,res)=>{
  try{await pool.query('DELETE FROM solicitudes WHERE solicitud_id=$1',[req.params.id]);res.json({ok:true});}catch(e){res.status(400).json({error:e.message});}
});

// ══════════════════════════════════════════════════════
// MÓDULO TRANSPORTE — TRASLADOS DE MAQUINARIA
// ══════════════════════════════════════════════════════
async function setupTransporte(q){
  // Campo para marcar equipos de transporte
  try{await q("ALTER TABLE equipos ADD COLUMN IF NOT EXISTS es_transporte BOOLEAN DEFAULT false");}catch(e){}
  try{await q("ALTER TABLE equipos ADD COLUMN IF NOT EXISTS patente_carro VARCHAR(20)");}catch(e){}

  // Tarifas de valorización
  await q(`CREATE TABLE IF NOT EXISTS trans_tarifas (
    tarifa_id SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    empresa_id INT REFERENCES empresas(empresa_id),
    valor_km_cargado NUMERIC(10,2) NOT NULL DEFAULT 0,
    valor_km_vacio NUMERIC(10,2) NOT NULL DEFAULT 0,
    costo_fijo_salida NUMERIC(10,2) DEFAULT 0,
    recargo_distancia_km NUMERIC(8,1) DEFAULT 0,
    pct_recargo NUMERIC(5,2) DEFAULT 0,
    vigente_desde DATE NOT NULL DEFAULT CURRENT_DATE,
    vigente_hasta DATE,
    activo BOOLEAN DEFAULT true,
    creado_en TIMESTAMP DEFAULT NOW()
  )`);

  // Registro de traslados
  await q(`CREATE TABLE IF NOT EXISTS trans_traslados (
    traslado_id SERIAL PRIMARY KEY,
    fecha DATE NOT NULL DEFAULT CURRENT_DATE,
    empresa_id INT NOT NULL REFERENCES empresas(empresa_id),
    camion_id INT NOT NULL REFERENCES equipos(equipo_id),
    chofer_id INT NOT NULL REFERENCES personal(persona_id),
    faena_id INT REFERENCES faenas(faena_id),
    equipo_id INT REFERENCES equipos(equipo_id),
    origen VARCHAR(200) NOT NULL,
    destino VARCHAR(200) NOT NULL,
    km_cargado NUMERIC(8,1) NOT NULL DEFAULT 0,
    km_vacio NUMERIC(8,1) NOT NULL DEFAULT 0,
    km_total NUMERIC(8,1) GENERATED ALWAYS AS (km_cargado + km_vacio) STORED,
    odometro_inicio INT,
    odometro_fin INT,
    tarifa_id INT REFERENCES trans_tarifas(tarifa_id),
    costo_km_cargado NUMERIC(12,2) DEFAULT 0,
    costo_km_vacio NUMERIC(12,2) DEFAULT 0,
    costo_fijo NUMERIC(12,2) DEFAULT 0,
    costo_recargo NUMERIC(12,2) DEFAULT 0,
    costo_total NUMERIC(12,2) DEFAULT 0,
    estado VARCHAR(20) DEFAULT 'finalizado',
    litros_combustible NUMERIC(10,2) DEFAULT 0,
    estanque_id INT REFERENCES comb_estanques(estanque_id),
    observaciones TEXT,
    usuario VARCHAR(100),
    creado_en TIMESTAMP DEFAULT NOW(),
    modificado_en TIMESTAMP DEFAULT NOW()
  )`);
  await q('CREATE INDEX IF NOT EXISTS idx_trans_fecha ON trans_traslados(fecha)');
  await q('CREATE INDEX IF NOT EXISTS idx_trans_empresa ON trans_traslados(empresa_id)');
  await q('CREATE INDEX IF NOT EXISTS idx_trans_camion ON trans_traslados(camion_id)');
  try{await q('ALTER TABLE trans_traslados ADD COLUMN IF NOT EXISTS litros_combustible NUMERIC(10,2) DEFAULT 0');}catch(e){}
  try{await q('ALTER TABLE trans_traslados ADD COLUMN IF NOT EXISTS estanque_id INT REFERENCES comb_estanques(estanque_id)');}catch(e){}
  try{await q('ALTER TABLE trans_traslados ADD COLUMN IF NOT EXISTS planificacion_id INT');}catch(e){}

  // Tabla planificación (referencia para el chofer)
  await q(`CREATE TABLE IF NOT EXISTS trans_planificacion (
    plan_id SERIAL PRIMARY KEY,
    fecha_plan DATE NOT NULL,
    hora_plan TIME,
    empresa_id INT REFERENCES empresas(empresa_id),
    camion_id INT REFERENCES equipos(equipo_id),
    chofer_id INT REFERENCES personal(persona_id),
    faena_id INT REFERENCES faenas(faena_id),
    equipo_id INT REFERENCES equipos(equipo_id),
    origen VARCHAR(200),
    destino VARCHAR(200),
    km_cargado_plan NUMERIC(8,1) DEFAULT 0,
    km_vacio_plan NUMERIC(8,1) DEFAULT 0,
    tarifa_id INT REFERENCES trans_tarifas(tarifa_id),
    prioridad VARCHAR(15) DEFAULT 'normal',
    estado VARCHAR(20) DEFAULT 'pendiente',
    traslado_id INT REFERENCES trans_traslados(traslado_id),
    observaciones TEXT,
    usuario VARCHAR(100),
    creado_en TIMESTAMP DEFAULT NOW(),
    modificado_en TIMESTAMP DEFAULT NOW()
  )`);
  await q('CREATE INDEX IF NOT EXISTS idx_plan_fecha ON trans_planificacion(fecha_plan)');
  await q('CREATE INDEX IF NOT EXISTS idx_plan_chofer ON trans_planificacion(chofer_id)');
  await q('CREATE INDEX IF NOT EXISTS idx_plan_estado ON trans_planificacion(estado)');

  // Seed camiones cama baja
  try{
    const cb=await q("SELECT COUNT(*) FROM equipos WHERE es_transporte=true");
    if(parseInt(cb.rows[0].count)===0){
      await q(`INSERT INTO equipos(codigo,nombre,tipo,patente_serie,patente_carro,es_transporte,tipo_cargo,activo) VALUES('CB-01','Camión Cama Baja 01','Camión Cama Baja','SRHC62','PWZR27',true,'transporte',true) ON CONFLICT(codigo) DO UPDATE SET es_transporte=true,patente_carro='PWZR27'`);
      await q(`INSERT INTO equipos(codigo,nombre,tipo,patente_serie,patente_carro,es_transporte,tipo_cargo,activo) VALUES('CB-02','Camión Cama Baja 02','Camión Cama Baja','KSCX54','JP5378',true,'transporte',true) ON CONFLICT(codigo) DO UPDATE SET es_transporte=true,patente_carro='JP5378'`);
      console.log('  [OK] Camiones cama baja creados');
    }
  }catch(e){console.log('[WARN] seed camiones:',e.message);}
}

// ── Tarifas transporte CRUD ──
app.get('/api/trans/tarifas', auth, async(req,res)=>{
  try{res.json((await pool.query('SELECT t.*,e.razon_social AS empresa_nombre FROM trans_tarifas t LEFT JOIN empresas e ON t.empresa_id=e.empresa_id ORDER BY t.activo DESC,t.vigente_desde DESC')).rows);}catch(e){res.status(500).json({error:e.message});}
});
app.post('/api/trans/tarifas', auth, async(req,res)=>{
  try{
    const{nombre,empresa_id,valor_km_cargado,valor_km_vacio,costo_fijo_salida,recargo_distancia_km,pct_recargo,vigente_desde,vigente_hasta}=req.body;
    if(!nombre)return res.status(400).json({error:'Nombre requerido'});
    const r=await pool.query('INSERT INTO trans_tarifas(nombre,empresa_id,valor_km_cargado,valor_km_vacio,costo_fijo_salida,recargo_distancia_km,pct_recargo,vigente_desde,vigente_hasta) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING *',
      [nombre,empresa_id||null,parseFloat(valor_km_cargado)||0,parseFloat(valor_km_vacio)||0,parseFloat(costo_fijo_salida)||0,parseFloat(recargo_distancia_km)||0,parseFloat(pct_recargo)||0,vigente_desde||new Date().toISOString().slice(0,10),vigente_hasta||null]);
    res.status(201).json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
app.put('/api/trans/tarifas/:id', auth, async(req,res)=>{
  try{
    const{nombre,empresa_id,valor_km_cargado,valor_km_vacio,costo_fijo_salida,recargo_distancia_km,pct_recargo,vigente_desde,vigente_hasta,activo}=req.body;
    const r=await pool.query('UPDATE trans_tarifas SET nombre=$1,empresa_id=$2,valor_km_cargado=$3,valor_km_vacio=$4,costo_fijo_salida=$5,recargo_distancia_km=$6,pct_recargo=$7,vigente_desde=$8,vigente_hasta=$9,activo=$10 WHERE tarifa_id=$11 RETURNING *',
      [nombre,empresa_id||null,parseFloat(valor_km_cargado)||0,parseFloat(valor_km_vacio)||0,parseFloat(costo_fijo_salida)||0,parseFloat(recargo_distancia_km)||0,parseFloat(pct_recargo)||0,vigente_desde,vigente_hasta||null,activo!==false,req.params.id]);
    res.json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
app.delete('/api/trans/tarifas/:id', auth, async(req,res)=>{
  try{await pool.query('DELETE FROM trans_tarifas WHERE tarifa_id=$1',[req.params.id]);res.json({ok:true});}catch(e){res.status(400).json({error:e.message});}
});

// ── Camiones transporte ──
app.get('/api/trans/camiones', auth, async(req,res)=>{
  try{res.json((await pool.query("SELECT equipo_id,codigo,nombre,patente_serie,patente_carro FROM equipos WHERE es_transporte=true AND activo=true ORDER BY codigo")).rows);}catch(e){res.status(500).json({error:e.message});}
});

// ── Choferes transporte (personal marcado como transporte) ──
app.get('/api/trans/choferes', auth, async(req,res)=>{
  try{res.json((await pool.query("SELECT persona_id,nombre_completo,rut,cargo,empresa_id FROM personal WHERE activo=true AND es_transporte=true ORDER BY nombre_completo")).rows);}catch(e){res.status(500).json({error:e.message});}
});

// ── Función para resolver tarifa y calcular costos ──
async function calcularCostoTraslado(empresa_id,fecha,km_cargado,km_vacio){
  // Buscar tarifa: 1) empresa+fecha, 2) sin empresa+fecha, 3) cualquiera activa
  let tarifa=null;
  const q1=await pool.query("SELECT * FROM trans_tarifas WHERE activo=true AND (empresa_id=$1 OR empresa_id IS NULL) AND vigente_desde<=$2 AND (vigente_hasta IS NULL OR vigente_hasta>=$2) ORDER BY empresa_id DESC NULLS LAST LIMIT 1",[empresa_id,fecha]);
  if(q1.rows.length)tarifa=q1.rows[0];
  else{const q2=await pool.query("SELECT * FROM trans_tarifas WHERE activo=true ORDER BY vigente_desde DESC LIMIT 1");if(q2.rows.length)tarifa=q2.rows[0];}
  if(!tarifa)return{tarifa_id:null,costo_km_cargado:0,costo_km_vacio:0,costo_fijo:0,costo_recargo:0,costo_total:0};
  const ckc=parseFloat(km_cargado)*parseFloat(tarifa.valor_km_cargado);
  const ckv=parseFloat(km_vacio)*parseFloat(tarifa.valor_km_vacio);
  const cfijo=parseFloat(tarifa.costo_fijo_salida)||0;
  let recargo=0;
  const kmTotal=parseFloat(km_cargado)+parseFloat(km_vacio);
  if(parseFloat(tarifa.recargo_distancia_km)>0&&kmTotal>parseFloat(tarifa.recargo_distancia_km)){
    recargo=(ckc+ckv)*(parseFloat(tarifa.pct_recargo)/100);
  }
  return{tarifa_id:tarifa.tarifa_id,costo_km_cargado:Math.round(ckc),costo_km_vacio:Math.round(ckv),costo_fijo:Math.round(cfijo),costo_recargo:Math.round(recargo),costo_total:Math.round(ckc+ckv+cfijo+recargo)};
}

// ── Traslados CRUD ──
app.get('/api/trans/traslados', auth, async(req,res)=>{
  try{
    const{empresa_id,camion_id,chofer_id,faena_id,equipo_id,estado,desde,hasta}=req.query;
    let w=['1=1'],v=[];
    if(empresa_id){v.push(empresa_id);w.push(`t.empresa_id=$${v.length}`);}
    if(camion_id){v.push(camion_id);w.push(`t.camion_id=$${v.length}`);}
    if(chofer_id){v.push(chofer_id);w.push(`t.chofer_id=$${v.length}`);}
    if(faena_id){v.push(faena_id);w.push(`t.faena_id=$${v.length}`);}
    if(equipo_id){v.push(equipo_id);w.push(`t.equipo_id=$${v.length}`);}
    if(estado){v.push(estado);w.push(`t.estado=$${v.length}`);}
    if(desde){v.push(desde);w.push(`t.fecha>=$${v.length}`);}
    if(hasta){v.push(hasta);w.push(`t.fecha<=$${v.length}`);}
    const r=await pool.query(`SELECT t.*,emp.razon_social AS empresa_nombre,cam.nombre AS camion_nombre,cam.patente_serie AS camion_patente,p.nombre_completo AS chofer_nombre,f.nombre AS faena_nombre,eq.nombre AS equipo_nombre,eq.codigo AS equipo_codigo,est.codigo AS estanque_codigo,est.nombre AS estanque_nombre FROM trans_traslados t JOIN empresas emp ON t.empresa_id=emp.empresa_id JOIN equipos cam ON t.camion_id=cam.equipo_id JOIN personal p ON t.chofer_id=p.persona_id LEFT JOIN faenas f ON t.faena_id=f.faena_id LEFT JOIN equipos eq ON t.equipo_id=eq.equipo_id LEFT JOIN comb_estanques est ON t.estanque_id=est.estanque_id WHERE ${w.join(' AND ')} ORDER BY t.fecha DESC,t.traslado_id DESC`,v);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});

app.post('/api/trans/traslados', auth, async(req,res)=>{
  try{
    const{fecha,empresa_id,camion_id,chofer_id,faena_id,equipo_id,origen,destino,km_cargado,km_vacio,odometro_inicio,odometro_fin,estado,observaciones,litros_combustible,estanque_id}=req.body;
    if(!empresa_id||!camion_id||!chofer_id||!origen||!destino)return res.status(400).json({error:'Empresa, camión, chofer, origen y destino son obligatorios'});
    const kc=parseFloat(km_cargado)||0;const kv=parseFloat(km_vacio)||0;
    if(kc<0||kv<0)return res.status(400).json({error:'Kilómetros no pueden ser negativos'});
    if(kc+kv===0)return res.status(400).json({error:'Debe ingresar al menos km cargado o km vacío'});
    if(odometro_inicio&&odometro_fin&&parseInt(odometro_fin)<parseInt(odometro_inicio))return res.status(400).json({error:'Odómetro final debe ser mayor al inicial'});
    const lts=parseFloat(litros_combustible)||0;
    if(lts<0)return res.status(400).json({error:'Litros no pueden ser negativos'});
    const costos=await calcularCostoTraslado(empresa_id,fecha||new Date().toISOString().slice(0,10),kc,kv);
    const r=await pool.query(`INSERT INTO trans_traslados(fecha,empresa_id,camion_id,chofer_id,faena_id,equipo_id,origen,destino,km_cargado,km_vacio,odometro_inicio,odometro_fin,tarifa_id,costo_km_cargado,costo_km_vacio,costo_fijo,costo_recargo,costo_total,estado,observaciones,litros_combustible,estanque_id,usuario) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23) RETURNING *`,
      [fecha||new Date().toISOString().slice(0,10),empresa_id,camion_id,chofer_id,faena_id||null,equipo_id||null,origen,destino,kc,kv,odometro_inicio?parseInt(odometro_inicio):null,odometro_fin?parseInt(odometro_fin):null,costos.tarifa_id,costos.costo_km_cargado,costos.costo_km_vacio,costos.costo_fijo,costos.costo_recargo,costos.costo_total,estado||'finalizado',observaciones||null,lts,estanque_id||null,req.user.email]);
    // Actualizar odómetro del camión
    if(odometro_fin)await pool.query('UPDATE equipos SET kilometraje_actual=GREATEST(COALESCE(kilometraje_actual,0),$1) WHERE equipo_id=$2',[parseInt(odometro_fin),camion_id]);
    res.status(201).json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});

app.put('/api/trans/traslados/:id', auth, async(req,res)=>{
  try{
    const{fecha,empresa_id,camion_id,chofer_id,faena_id,equipo_id,origen,destino,km_cargado,km_vacio,odometro_inicio,odometro_fin,estado,observaciones,litros_combustible,estanque_id}=req.body;
    const kc=parseFloat(km_cargado)||0;const kv=parseFloat(km_vacio)||0;
    if(kc<0||kv<0)return res.status(400).json({error:'Kilómetros no pueden ser negativos'});
    const lts=parseFloat(litros_combustible)||0;
    const costos=await calcularCostoTraslado(empresa_id,fecha,kc,kv);
    const r=await pool.query(`UPDATE trans_traslados SET fecha=$1,empresa_id=$2,camion_id=$3,chofer_id=$4,faena_id=$5,equipo_id=$6,origen=$7,destino=$8,km_cargado=$9,km_vacio=$10,odometro_inicio=$11,odometro_fin=$12,tarifa_id=$13,costo_km_cargado=$14,costo_km_vacio=$15,costo_fijo=$16,costo_recargo=$17,costo_total=$18,estado=$19,observaciones=$20,litros_combustible=$21,estanque_id=$22,modificado_en=NOW() WHERE traslado_id=$23 RETURNING *`,
      [fecha,empresa_id,camion_id,chofer_id,faena_id||null,equipo_id||null,origen,destino,kc,kv,odometro_inicio?parseInt(odometro_inicio):null,odometro_fin?parseInt(odometro_fin):null,costos.tarifa_id,costos.costo_km_cargado,costos.costo_km_vacio,costos.costo_fijo,costos.costo_recargo,costos.costo_total,estado||'finalizado',observaciones||null,lts,estanque_id||null,req.params.id]);
    res.json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});

app.delete('/api/trans/traslados/:id', auth, async(req,res)=>{
  try{await pool.query('DELETE FROM trans_traslados WHERE traslado_id=$1',[req.params.id]);res.json({ok:true});}catch(e){res.status(400).json({error:e.message});}
});

// ── Dashboard transporte ──
app.get('/api/trans/dashboard', auth, async(req,res)=>{
  try{
    const{empresa_id,desde,hasta}=req.query;
    let w=['t.estado!=\'anulado\''],v=[];
    if(empresa_id){v.push(empresa_id);w.push(`t.empresa_id=$${v.length}`);}
    if(desde){v.push(desde);w.push(`t.fecha>=$${v.length}`);}
    if(hasta){v.push(hasta);w.push(`t.fecha<=$${v.length}`);}
    const wh=w.join(' AND ');
    const totales=await pool.query(`SELECT COUNT(*) AS total,COALESCE(SUM(km_cargado),0) AS km_cargado,COALESCE(SUM(km_vacio),0) AS km_vacio,COALESCE(SUM(km_cargado+km_vacio),0) AS km_total,COALESCE(SUM(costo_total),0) AS costo_total,COALESCE(SUM(litros_combustible),0) AS litros_total FROM trans_traslados t WHERE ${wh}`,v);
    const porEmpresa=await pool.query(`SELECT emp.razon_social,COUNT(*) AS traslados,SUM(km_cargado+km_vacio) AS km,SUM(costo_total) AS costo FROM trans_traslados t JOIN empresas emp ON t.empresa_id=emp.empresa_id WHERE ${wh} GROUP BY emp.razon_social ORDER BY costo DESC`,v);
    const porCamion=await pool.query(`SELECT cam.nombre,cam.patente_serie,COUNT(*) AS traslados,SUM(km_cargado+km_vacio) AS km,SUM(costo_total) AS costo FROM trans_traslados t JOIN equipos cam ON t.camion_id=cam.equipo_id WHERE ${wh} GROUP BY cam.nombre,cam.patente_serie ORDER BY traslados DESC`,v);
    const porFaena=await pool.query(`SELECT COALESCE(f.nombre,'Sin faena') AS faena,COUNT(*) AS traslados,SUM(km_cargado+km_vacio) AS km,SUM(costo_total) AS costo FROM trans_traslados t LEFT JOIN faenas f ON t.faena_id=f.faena_id WHERE ${wh} GROUP BY f.nombre ORDER BY costo DESC`,v);
    const topEquipos=await pool.query(`SELECT eq.nombre,eq.codigo,COUNT(*) AS traslados,SUM(km_cargado+km_vacio) AS km FROM trans_traslados t JOIN equipos eq ON t.equipo_id=eq.equipo_id WHERE ${wh} AND t.equipo_id IS NOT NULL GROUP BY eq.nombre,eq.codigo ORDER BY traslados DESC LIMIT 10`,v);
    const topDestinos=await pool.query(`SELECT destino,COUNT(*) AS traslados,SUM(km_cargado+km_vacio) AS km FROM trans_traslados t WHERE ${wh} GROUP BY destino ORDER BY traslados DESC LIMIT 10`,v);
    const porMes=await pool.query(`SELECT TO_CHAR(fecha,'YYYY-MM') AS mes,COUNT(*) AS traslados,SUM(km_cargado) AS km_cargado,SUM(km_vacio) AS km_vacio,SUM(costo_total) AS costo FROM trans_traslados t WHERE ${wh} GROUP BY mes ORDER BY mes`,v);
    res.json({totales:totales.rows[0],porEmpresa:porEmpresa.rows,porCamion:porCamion.rows,porFaena:porFaena.rows,topEquipos:topEquipos.rows,topDestinos:topDestinos.rows,porMes:porMes.rows});
  }catch(e){res.status(500).json({error:e.message});}
});

// ══ PLANIFICACIÓN DE TRASLADOS ══
app.get('/api/trans/planificacion', auth, async(req,res)=>{
  try{
    const{desde,hasta,chofer_id,estado,solo_pendientes}=req.query;
    let w=['1=1'],v=[];
    if(desde){v.push(desde);w.push(`p.fecha_plan>=$${v.length}`);}
    if(hasta){v.push(hasta);w.push(`p.fecha_plan<=$${v.length}`);}
    if(chofer_id){v.push(chofer_id);w.push(`p.chofer_id=$${v.length}`);}
    if(estado){v.push(estado);w.push(`p.estado=$${v.length}`);}
    if(solo_pendientes==='1')w.push(`p.estado IN ('pendiente','en_curso')`);
    const r=await pool.query(`SELECT p.*,
      emp.razon_social AS empresa_nombre,
      cam.nombre AS camion_nombre,cam.patente_serie AS camion_patente,
      per.nombre_completo AS chofer_nombre,per.rut AS chofer_rut,
      f.nombre AS faena_nombre,
      eq.nombre AS equipo_nombre,eq.codigo AS equipo_codigo
      FROM trans_planificacion p
      LEFT JOIN empresas emp ON p.empresa_id=emp.empresa_id
      LEFT JOIN equipos cam ON p.camion_id=cam.equipo_id
      LEFT JOIN personal per ON p.chofer_id=per.persona_id
      LEFT JOIN faenas f ON p.faena_id=f.faena_id
      LEFT JOIN equipos eq ON p.equipo_id=eq.equipo_id
      WHERE ${w.join(' AND ')}
      ORDER BY p.fecha_plan,p.hora_plan NULLS LAST,p.plan_id`,v);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});

app.get('/api/trans/planificacion/:id', auth, async(req,res)=>{
  try{
    const r=await pool.query(`SELECT p.*,
      emp.razon_social AS empresa_nombre,
      cam.nombre AS camion_nombre,cam.patente_serie AS camion_patente,cam.patente_carro AS camion_patente_carro,
      per.nombre_completo AS chofer_nombre,per.rut AS chofer_rut,
      f.nombre AS faena_nombre,
      eq.nombre AS equipo_nombre,eq.codigo AS equipo_codigo
      FROM trans_planificacion p
      LEFT JOIN empresas emp ON p.empresa_id=emp.empresa_id
      LEFT JOIN equipos cam ON p.camion_id=cam.equipo_id
      LEFT JOIN personal per ON p.chofer_id=per.persona_id
      LEFT JOIN faenas f ON p.faena_id=f.faena_id
      LEFT JOIN equipos eq ON p.equipo_id=eq.equipo_id
      WHERE p.plan_id=$1`,[req.params.id]);
    if(!r.rows.length)return res.status(404).json({error:'Planificación no encontrada'});
    res.json(r.rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});

// Detectar solapamiento horario del chofer
app.get('/api/trans/planificacion/check-solape', auth, async(req,res)=>{
  try{
    const{fecha,chofer_id,hora,plan_id}=req.query;
    if(!fecha||!chofer_id)return res.json({solape:false});
    let w=['p.fecha_plan=$1','p.chofer_id=$2',`p.estado IN ('pendiente','en_curso')`],v=[fecha,chofer_id];
    if(plan_id){v.push(plan_id);w.push(`p.plan_id!=$${v.length}`);}
    const r=await pool.query(`SELECT p.plan_id,p.hora_plan,p.origen,p.destino FROM trans_planificacion p WHERE ${w.join(' AND ')}`,v);
    res.json({solape:r.rows.length>0,traslados:r.rows});
  }catch(e){res.status(500).json({error:e.message});}
});

app.post('/api/trans/planificacion', auth, async(req,res)=>{
  try{
    const b=req.body;
    if(!b.fecha_plan)return res.status(400).json({error:'Fecha es obligatoria'});
    const r=await pool.query(`INSERT INTO trans_planificacion(fecha_plan,hora_plan,empresa_id,camion_id,chofer_id,faena_id,equipo_id,origen,destino,km_cargado_plan,km_vacio_plan,tarifa_id,prioridad,estado,observaciones,usuario) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16) RETURNING *`,
      [b.fecha_plan,b.hora_plan||null,b.empresa_id||null,b.camion_id||null,b.chofer_id||null,b.faena_id||null,b.equipo_id||null,b.origen||null,b.destino||null,parseFloat(b.km_cargado_plan)||0,parseFloat(b.km_vacio_plan)||0,b.tarifa_id||null,b.prioridad||'normal',b.estado||'pendiente',b.observaciones||null,req.user.email]);
    res.status(201).json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});

app.put('/api/trans/planificacion/:id', auth, async(req,res)=>{
  try{
    const b=req.body;
    const r=await pool.query(`UPDATE trans_planificacion SET fecha_plan=$1,hora_plan=$2,empresa_id=$3,camion_id=$4,chofer_id=$5,faena_id=$6,equipo_id=$7,origen=$8,destino=$9,km_cargado_plan=$10,km_vacio_plan=$11,tarifa_id=$12,prioridad=$13,estado=$14,observaciones=$15,modificado_en=NOW() WHERE plan_id=$16 RETURNING *`,
      [b.fecha_plan,b.hora_plan||null,b.empresa_id||null,b.camion_id||null,b.chofer_id||null,b.faena_id||null,b.equipo_id||null,b.origen||null,b.destino||null,parseFloat(b.km_cargado_plan)||0,parseFloat(b.km_vacio_plan)||0,b.tarifa_id||null,b.prioridad||'normal',b.estado||'pendiente',b.observaciones||null,req.params.id]);
    res.json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});

app.delete('/api/trans/planificacion/:id', auth, async(req,res)=>{
  try{await pool.query('DELETE FROM trans_planificacion WHERE plan_id=$1',[req.params.id]);res.json({ok:true});}
  catch(e){res.status(400).json({error:e.message});}
});

// Marcar planificación como ejecutada y vincular al traslado real
app.post('/api/trans/planificacion/:id/ejecutar', auth, async(req,res)=>{
  try{
    const{traslado_id}=req.body;
    if(!traslado_id)return res.status(400).json({error:'traslado_id requerido'});
    await pool.query(`UPDATE trans_planificacion SET estado='ejecutado',traslado_id=$1,modificado_en=NOW() WHERE plan_id=$2`,[traslado_id,req.params.id]);
    await pool.query(`UPDATE trans_traslados SET planificacion_id=$1 WHERE traslado_id=$2`,[req.params.id,traslado_id]);
    res.json({ok:true});
  }catch(e){res.status(400).json({error:e.message});}
});

// ══════════════════════════════════════════════════════
// PROGRAMACIÓN SEMANAL DE MANTENCIÓN
// ══════════════════════════════════════════════════════
async function setupProgMant(q){
  await q(`CREATE TABLE IF NOT EXISTS mant_prog_semanal (
    prog_id SERIAL PRIMARY KEY,
    anio INT NOT NULL,
    mes INT NOT NULL,
    semana_idx INT NOT NULL DEFAULT 0,
    semana_inicio DATE,
    faena_id INT REFERENCES faenas(faena_id),
    faena_nombre VARCHAR(200),
    equipo_id INT REFERENCES equipos(equipo_id),
    cargo_nombre VARCHAR(200),
    detalle TEXT,
    dias JSONB NOT NULL DEFAULT '[[[""],[""]],[[""],[""]],[[""],[""]],[[""],[""]],[[""],[""]],[[""],[""]],[[""],[""]]]',
    estado VARCHAR(30) DEFAULT 'programado',
    usuario VARCHAR(100),
    creado_en TIMESTAMP DEFAULT NOW(),
    modificado_en TIMESTAMP DEFAULT NOW()
  )`);
  await q('CREATE INDEX IF NOT EXISTS idx_mps_anio_mes ON mant_prog_semanal(anio,mes)');
  await q('CREATE INDEX IF NOT EXISTS idx_mps_equipo ON mant_prog_semanal(equipo_id)');
  await q('CREATE INDEX IF NOT EXISTS idx_mps_estado ON mant_prog_semanal(estado)');
}

// GET — listar tareas de un mes (opcionalmente semana)
app.get('/api/mant/prog-semanal', auth, async(req,res)=>{
  try{
    const{anio,mes,semana_idx,faena_id,estado}=req.query;
    let w=['1=1'],v=[];
    if(anio){v.push(anio);w.push(`p.anio=$${v.length}`);}
    if(mes!==undefined&&mes!==''){v.push(mes);w.push(`p.mes=$${v.length}`);}
    if(semana_idx!==undefined&&semana_idx!==''){v.push(semana_idx);w.push(`p.semana_idx=$${v.length}`);}
    if(faena_id){v.push(faena_id);w.push(`p.faena_id=$${v.length}`);}
    if(estado){v.push(estado);w.push(`p.estado=$${v.length}`);}
    const r=await pool.query(`SELECT p.*,
      f.nombre AS faena_join_nombre,
      eq.nombre AS equipo_nombre,eq.codigo AS equipo_codigo
      FROM mant_prog_semanal p
      LEFT JOIN faenas f ON p.faena_id=f.faena_id
      LEFT JOIN equipos eq ON p.equipo_id=eq.equipo_id
      WHERE ${w.join(' AND ')}
      ORDER BY p.semana_idx,p.faena_nombre,p.cargo_nombre,p.prog_id`,v);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});

app.post('/api/mant/prog-semanal', auth, async(req,res)=>{
  try{
    const b=req.body;
    if(!b.anio||b.mes===undefined)return res.status(400).json({error:'Año y mes requeridos'});
    const r=await pool.query(`INSERT INTO mant_prog_semanal(anio,mes,semana_idx,semana_inicio,faena_id,faena_nombre,equipo_id,cargo_nombre,detalle,dias,estado,usuario) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING *`,
      [b.anio,b.mes,parseInt(b.semana_idx)||0,b.semana_inicio||null,b.faena_id||null,b.faena_nombre||null,b.equipo_id||null,b.cargo_nombre||null,b.detalle||null,JSON.stringify(b.dias||[]),b.estado||'programado',req.user.email]);
    res.status(201).json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});

app.put('/api/mant/prog-semanal/:id', auth, async(req,res)=>{
  try{
    const b=req.body;
    const r=await pool.query(`UPDATE mant_prog_semanal SET anio=$1,mes=$2,semana_idx=$3,semana_inicio=$4,faena_id=$5,faena_nombre=$6,equipo_id=$7,cargo_nombre=$8,detalle=$9,dias=$10,estado=$11,modificado_en=NOW() WHERE prog_id=$12 RETURNING *`,
      [b.anio,b.mes,parseInt(b.semana_idx)||0,b.semana_inicio||null,b.faena_id||null,b.faena_nombre||null,b.equipo_id||null,b.cargo_nombre||null,b.detalle||null,JSON.stringify(b.dias||[]),b.estado||'programado',req.params.id]);
    res.json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});

app.delete('/api/mant/prog-semanal/:id', auth, async(req,res)=>{
  try{await pool.query('DELETE FROM mant_prog_semanal WHERE prog_id=$1',[req.params.id]);res.json({ok:true});}
  catch(e){res.status(400).json({error:e.message});}
});

// Detección de conflictos: mecánicos asignados en un día/semana
app.get('/api/mant/prog-semanal/conflictos', auth, async(req,res)=>{
  try{
    const{anio,mes,semana_idx,excluir_id}=req.query;
    let w=['p.anio=$1','p.mes=$2','p.semana_idx=$3'],v=[anio,mes,semana_idx];
    if(excluir_id){v.push(excluir_id);w.push(`p.prog_id!=$${v.length}`);}
    const r=await pool.query(`SELECT p.prog_id,p.cargo_nombre,p.faena_nombre,p.dias FROM mant_prog_semanal p WHERE ${w.join(' AND ')}`,v);
    // Construir mapa de mecánico → día → tareas
    const mapa={};
    r.rows.forEach(function(t){
      var dias=t.dias||[];
      dias.forEach(function(day,di){
        if(!Array.isArray(day))return;
        day.forEach(function(turno,ti){
          if(!Array.isArray(turno))return;
          turno.forEach(function(mec){
            if(!mec||!mec.trim()||mec.trim().length<3)return;
            var k=mec.trim().toUpperCase();
            if(!mapa[k])mapa[k]=[];
            mapa[k].push({dia:di,turno:ti,cargo:t.cargo_nombre,faena:t.faena_nombre,prog_id:t.prog_id});
          });
        });
      });
    });
    res.json(mapa);
  }catch(e){res.status(500).json({error:e.message});}
});

// Mecánicos del personal de mantención
app.get('/api/mant/mecanicos', auth, async(req,res)=>{
  try{
    const r=await pool.query(`SELECT persona_id,nombre_completo,cargo FROM personal WHERE activo=true AND participa_mantencion=true ORDER BY nombre_completo`);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});

// Importación masiva de programación semanal
app.post('/api/mant/prog-semanal/import', auth, async(req,res)=>{
  try{
    const items=Array.isArray(req.body)?req.body:[];
    if(!items.length)return res.status(400).json({error:'Array vacío'});
    let creados=0,errores=0;
    for(const b of items){
      try{
        await pool.query(`INSERT INTO mant_prog_semanal(anio,mes,semana_idx,faena_nombre,cargo_nombre,detalle,dias,estado,usuario) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
          [b.anio||2026,b.mes!==undefined?b.mes:3,parseInt(b.semana_idx)||0,b.faena_nombre||null,b.cargo_nombre||null,b.detalle||null,JSON.stringify(b.dias||[]),b.estado||'programado',req.user.email]);
        creados++;
      }catch(e){errores++;}
    }
    res.json({ok:true,creados,errores,total:items.length});
  }catch(e){res.status(400).json({error:e.message});}
});

// ══════════════════════════════════════════════════════
// Importación masiva de usuarios
app.post('/api/usuarios/import', auth, async(req,res)=>{
  try{
    const items=req.body;
    if(!Array.isArray(items)||!items.length)return res.status(400).json({error:'Array vacío'});
    let creados=0,existentes=0,errores=[];
    // Cargar roles, empresas, faenas
    const rolesR=await pool.query('SELECT rol_id,nombre FROM roles');
    const empR=await pool.query('SELECT empresa_id,razon_social FROM empresas');
    const faeR=await pool.query('SELECT faena_id,nombre FROM faenas');
    for(const u of items){
      try{
        const email=String(u.email||'').trim().toLowerCase();
        if(!email){errores.push(u.nombre+': sin email');continue;}
        const exists=await pool.query('SELECT 1 FROM usuarios WHERE email=$1 OR (username IS NOT NULL AND username=$2)',[email,u.username||null]);
        if(exists.rows.length){existentes++;continue;}
        const hash=await require('bcryptjs').hash(String(u.password||'123456'),10);
        // Buscar rol_id
        const rolName=String(u.rol||'').trim();
        const rolRow=rolesR.rows.find(r=>r.nombre.toLowerCase()===rolName.toLowerCase());
        const rid=rolRow?rolRow.rol_id:null;
        const rolStr=rolRow?rolRow.nombre:rolName;
        // Buscar empresa_id
        let eid=null;
        if(u.empresa&&u.empresa!=='Todas las empresas'){
          const empRow=empR.rows.find(e=>e.razon_social===u.empresa);
          if(empRow)eid=empRow.empresa_id;
        }
        // Buscar faena_id
        let fid=null;
        if(u.faena&&u.faena!=='Todas las faenas'){
          const faeRow=faeR.rows.find(f=>f.nombre===u.faena);
          if(faeRow)fid=faeRow.faena_id;
        }
        await pool.query('INSERT INTO usuarios(email,username,nombre,password_hash,rol,rol_id,empresa_id,faena_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8)',
          [email,u.username||null,u.nombre,hash,rolStr,rid,eid,fid]);
        creados++;
      }catch(e2){errores.push((u.nombre||u.email)+': '+e2.message);}
    }
    res.json({ok:true,creados,existentes,errores,total:items.length});
  }catch(e){res.status(400).json({error:e.message});}
});

// Importación masiva de subcategorías
app.post('/api/subcategorias/import', auth, async(req,res)=>{
  try{
    const{categoria_id,items}=req.body;
    if(!categoria_id)return res.status(400).json({error:'categoria_id requerido'});
    if(!Array.isArray(items)||!items.length)return res.status(400).json({error:'items vacío'});
    let creadas=0,existentes=0;
    for(const item of items){
      const nombre=String(item.nombre||'').trim();
      if(!nombre)continue;
      const exists=await pool.query('SELECT 1 FROM subcategorias WHERE categoria_id=$1 AND nombre=$2',[categoria_id,nombre]);
      if(exists.rows.length){existentes++;continue;}
      await pool.query('INSERT INTO subcategorias(categoria_id,nombre,activo) VALUES($1,$2,true)',[categoria_id,nombre]);
      creadas++;
    }
    res.json({ok:true,creadas,existentes,total:items.length});
  }catch(e){res.status(400).json({error:e.message});}
});

// FACTURACIÓN DE GUÍAS DE DESPACHO
// ══════════════════════════════════════════════════════
async function setupFacturaGuias(q){
  await q(`CREATE TABLE IF NOT EXISTS oc_factura_guias (
    factura_id SERIAL PRIMARY KEY,
    proveedor_id INT NOT NULL REFERENCES proveedores(proveedor_id),
    empresa_id INT REFERENCES empresas(empresa_id),
    numero_factura VARCHAR(50),
    fecha_factura DATE,
    neto NUMERIC(14,2) DEFAULT 0,
    iva NUMERIC(14,2) DEFAULT 0,
    ie_total NUMERIC(14,2) DEFAULT 0,
    total NUMERIC(14,2) DEFAULT 0,
    observaciones TEXT,
    usuario VARCHAR(100),
    creado_en TIMESTAMP DEFAULT NOW()
  )`);
  await q('ALTER TABLE ordenes_compra ADD COLUMN IF NOT EXISTS factura_guia_id INT REFERENCES oc_factura_guias(factura_id)');
}

// Listar OCs con guía de despacho pendientes de facturar
app.get('/api/oc-guias/pendientes', auth, async(req,res)=>{
  try{
    const{proveedor_id,empresa_id}=req.query;
    let w=["oc.estado='CERRADA'","oc.factura_guia_id IS NULL"],v=[];
    w.push("(td.nombre ILIKE '%gu_a%' OR td.nombre ILIKE '%despacho%' OR td.nombre ILIKE '%provisori%')");
    if(proveedor_id){v.push(proveedor_id);w.push(`oc.proveedor_id=$${v.length}`);}
    if(empresa_id){v.push(empresa_id);w.push(`oc.empresa_id=$${v.length}`);}
    const r=await pool.query(`SELECT oc.*,e.razon_social AS empresa_nombre,pr.nombre AS proveedor_nombre,td.nombre AS tipo_doc_nombre
      FROM ordenes_compra oc
      LEFT JOIN empresas e ON oc.empresa_id=e.empresa_id
      LEFT JOIN proveedores pr ON oc.proveedor_id=pr.proveedor_id
      LEFT JOIN tipos_documento td ON oc.tipo_doc_id=td.tipo_doc_id
      WHERE ${w.join(' AND ')}
      ORDER BY oc.fecha_emision,oc.oc_id`,v);
    function detectCat(txt){
      var u=String(txt||'').toUpperCase();
      if(/DIESEL|PETR[OÓ]LEO|PETROLEO/.test(u))return'diesel';
      if(/GASOLINA|BENCINA|NAFTA/.test(u))return'gasolina';
      return null;
    }
    for(const row of r.rows){
      var comb_lineas=[];
      // 1) Movimientos de stock
      const movs=await pool.query(`SELECT cm.mov_id,cm.tipo_id,cm.litros,cm.precio_unitario,cm.estanque_destino_id,ct.nombre AS tipo_nombre
        FROM comb_movimientos cm LEFT JOIN comb_tipos ct ON cm.tipo_id=ct.tipo_id
        WHERE cm.oc_referencia=$1 AND cm.estado='ACTIVO'`,[row.numero_oc]);
      movs.rows.forEach(function(m){
        var cat=detectCat(m.tipo_nombre);
        if(cat)comb_lineas.push({source:'stock',mov_id:m.mov_id,tipo_id:m.tipo_id,tipo_nombre:m.tipo_nombre,categoria:cat,litros:parseFloat(m.litros)||0,precio_unitario:parseFloat(m.precio_unitario)||0,estanque_id:m.estanque_destino_id});
      });
      // 2) Si no hay stock, buscar en líneas OC via subcategoría
      if(movs.rows.length===0){
        const dets=await pool.query(`SELECT d.detalle_id,d.descripcion,d.cantidad,d.precio_unitario,
          sc.nombre AS subcat_nombre
          FROM ordenes_compra_detalle d
          LEFT JOIN subcategorias sc ON COALESCE(d.subcategoria_id,(SELECT p.subcategoria_id FROM productos p WHERE p.producto_id=d.producto_id))=sc.subcategoria_id
          WHERE d.oc_id=$1`,[row.oc_id]);
        dets.rows.forEach(function(d){
          var cat=detectCat(d.subcat_nombre)||detectCat(d.descripcion);
          if(cat){
            comb_lineas.push({source:'directo',detalle_id:d.detalle_id,tipo_id:null,
              tipo_nombre:d.subcat_nombre||d.descripcion,
              descripcion:d.descripcion,categoria:cat,
              litros:parseFloat(d.cantidad)||0,precio_unitario:parseFloat(d.precio_unitario)||0});
          }
        });
      }
      row.comb_lineas=comb_lineas;
      row.litros_comb=comb_lineas.reduce(function(s,l){return s+(parseFloat(l.litros)||0);},0);
    }
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});

// Listar proveedores que tienen guías pendientes
app.get('/api/oc-guias/proveedores-pendientes', auth, async(req,res)=>{
  try{
    const r=await pool.query(`SELECT DISTINCT pr.proveedor_id,pr.nombre,pr.rut,COUNT(oc.oc_id) AS guias_pendientes,SUM(oc.total) AS monto_total
      FROM ordenes_compra oc
      JOIN proveedores pr ON oc.proveedor_id=pr.proveedor_id
      LEFT JOIN tipos_documento td ON oc.tipo_doc_id=td.tipo_doc_id
      WHERE oc.estado='CERRADA' AND oc.factura_guia_id IS NULL
        AND (td.nombre ILIKE '%gu_a%' OR td.nombre ILIKE '%despacho%' OR td.nombre ILIKE '%provisori%')
      GROUP BY pr.proveedor_id,pr.nombre,pr.rut
      ORDER BY pr.nombre`);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});

// Crear factura asociando guías + recalcular combustible por categoría
app.post('/api/oc-guias/facturar', auth, async(req,res)=>{
  const client=await pool.connect();
  try{
    await client.query('BEGIN');
    const{proveedor_id,empresa_id,numero_factura,fecha_factura,neto,iva,ie_total,total,observaciones,oc_ids,combustible_ajustes}=req.body;
    if(!proveedor_id||!numero_factura)throw new Error('Proveedor y N° factura requeridos');
    if(!Array.isArray(oc_ids)||!oc_ids.length)throw new Error('Seleccione al menos una guía');
    const fr=await client.query(`INSERT INTO oc_factura_guias(proveedor_id,empresa_id,numero_factura,fecha_factura,neto,iva,ie_total,total,observaciones,usuario) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING factura_id`,
      [proveedor_id,empresa_id||null,numero_factura,fecha_factura||new Date().toISOString().slice(0,10),parseFloat(neto)||0,parseFloat(iva)||0,parseFloat(ie_total)||0,parseFloat(total)||0,observaciones||null,req.user.email]);
    const fid=fr.rows[0].factura_id;
    // Asociar OCs
    for(const ocId of oc_ids){
      await client.query('UPDATE ordenes_compra SET factura_guia_id=$1 WHERE oc_id=$2 AND factura_guia_id IS NULL',[fid,ocId]);
    }
    // Mapa de ajustes por categoría: {diesel: {neto_lt, ie_lt}, gasolina: {neto_lt, ie_lt}}
    const ajustes={diesel:null,gasolina:null};
    if(Array.isArray(combustible_ajustes)){
      combustible_ajustes.forEach(function(a){
        if(a.categoria&&a.neto_lt>0)ajustes[a.categoria]={neto_lt:parseFloat(a.neto_lt)||0,ie_lt:parseFloat(a.ie_lt)||0};
      });
    }
    // Función helper: detectar categoría por string
    function detectCat(s){
      var u=String(s||'').toUpperCase();
      if(u.indexOf('DIESEL')>=0||u.indexOf('PETR')>=0)return'diesel';
      if(u.indexOf('GASOLINA')>=0||u.indexOf('BENCINA')>=0||u.indexOf('NAFTA')>=0)return'gasolina';
      return null;
    }
    let recalculados=0;
    // Recolectar numero_oc de las OCs
    const ocs=[];
    for(const ocId of oc_ids){
      const ocR=await client.query('SELECT oc_id,numero_oc FROM ordenes_compra WHERE oc_id=$1',[ocId]);
      if(ocR.rows.length)ocs.push(ocR.rows[0]);
    }
    for(const oc of ocs){
      // 1) Procesar movimientos de stock (comb_movimientos)
      const movs=await client.query("SELECT cm.*,ct.nombre AS tipo_nombre FROM comb_movimientos cm LEFT JOIN comb_tipos ct ON cm.tipo_id=ct.tipo_id WHERE cm.oc_referencia=$1 AND cm.estado='ACTIVO'",[oc.numero_oc]);
      for(const mv of movs.rows){
        const cat=detectCat(mv.tipo_nombre);
        const aj=cat?ajustes[cat]:null;
        if(aj){
          const oldPU=parseFloat(mv.precio_unitario)||0;
          const lts=parseFloat(mv.litros)||0;
          if(lts>0){
            const newPU=aj.neto_lt;
            const delta=(newPU-oldPU)*lts;
            await client.query('UPDATE comb_movimientos SET precio_unitario=$1,costo_total=$2,es_provisorio=false,numero_documento=$3 WHERE mov_id=$4',
              [newPU,lts*newPU,numero_factura,mv.mov_id]);
            if(mv.estanque_destino_id&&mv.tipo_id){
              const stk=await client.query('SELECT litros_disponibles,costo_promedio FROM comb_stock WHERE estanque_id=$1 AND tipo_id=$2',[mv.estanque_destino_id,mv.tipo_id]);
              if(stk.rows.length){
                const curLts=parseFloat(stk.rows[0].litros_disponibles)||0;
                const curCpp=parseFloat(stk.rows[0].costo_promedio)||0;
                const newVal=curLts*curCpp+delta;
                const newCpp=curLts>0?Math.max(0,newVal/curLts):newPU;
                await client.query('UPDATE comb_stock SET costo_promedio=$1,ultima_actualizacion=NOW() WHERE estanque_id=$2 AND tipo_id=$3',[newCpp,mv.estanque_destino_id,mv.tipo_id]);
              }
            }
            recalculados++;
          }
        }
      }
      // Si hubo stock, también desmarcar provisorios remanentes
      if(movs.rows.length>0){
        await client.query("UPDATE comb_movimientos SET numero_documento=$1,es_provisorio=false WHERE oc_referencia=$2 AND es_provisorio=true AND cierre_id IS NULL",[numero_factura,oc.numero_oc]);
      }
      // 2) Procesar consumo directo: líneas de OC con keywords combustible (solo si no hay movimientos de stock)
      if(movs.rows.length===0){
        const detLines=await client.query(`SELECT * FROM ordenes_compra_detalle
          WHERE oc_id=$1 AND (UPPER(COALESCE(descripcion,'')) LIKE '%DIESEL%' OR UPPER(COALESCE(descripcion,'')) LIKE '%GASOLINA%'
            OR UPPER(COALESCE(descripcion,'')) LIKE '%PETR%' OR UPPER(COALESCE(descripcion,'')) LIKE '%BENCINA%' OR UPPER(COALESCE(descripcion,'')) LIKE '%NAFTA%')`,[oc.oc_id]);
        let ocChanged=false;
        for(const d of detLines.rows){
          const cat=detectCat(d.descripcion);
          const aj=cat?ajustes[cat]:null;
          if(aj&&aj.neto_lt>0){
            await client.query('UPDATE ordenes_compra_detalle SET precio_unitario=$1 WHERE detalle_id=$2',[aj.neto_lt,d.detalle_id]);
            ocChanged=true;recalculados++;
          }
        }
        // Si cambió algún precio, recalcular totales de la OC
        if(ocChanged){
          const recalc=await client.query(`SELECT COALESCE(SUM(cantidad*precio_unitario),0) AS neto FROM ordenes_compra_detalle WHERE oc_id=$1`,[oc.oc_id]);
          const newNeto=parseFloat(recalc.rows[0].neto)||0;
          const newIVA=Math.round(newNeto*0.19);
          const ocD=await client.query('SELECT impuesto_adicional FROM ordenes_compra WHERE oc_id=$1',[oc.oc_id]);
          const imp=parseFloat((ocD.rows[0]||{}).impuesto_adicional)||0;
          const newTotal=newNeto+newIVA+imp;
          await client.query('UPDATE ordenes_compra SET neto=$1,iva=$2,total=$3,modificado_en=NOW() WHERE oc_id=$4',[newNeto,newIVA,newTotal,oc.oc_id]);
        }
      }
    }
    await client.query('COMMIT');
    res.status(201).json({ok:true,factura_id:fid,recalculados});
  }catch(e){await client.query('ROLLBACK');res.status(400).json({error:e.message});}
  finally{client.release();}
});

// Listar facturas de guías
app.get('/api/oc-guias/facturas', auth, async(req,res)=>{
  try{
    const{proveedor_id}=req.query;
    let w=['1=1'],v=[];
    if(proveedor_id){v.push(proveedor_id);w.push(`f.proveedor_id=$${v.length}`);}
    const r=await pool.query(`SELECT f.*,pr.nombre AS proveedor_nombre,e.razon_social AS empresa_nombre,
      (SELECT COUNT(*) FROM ordenes_compra oc WHERE oc.factura_guia_id=f.factura_id) AS num_guias,
      (SELECT STRING_AGG(oc.numero_oc,', ') FROM ordenes_compra oc WHERE oc.factura_guia_id=f.factura_id) AS guias_str
      FROM oc_factura_guias f
      LEFT JOIN proveedores pr ON f.proveedor_id=pr.proveedor_id
      LEFT JOIN empresas e ON f.empresa_id=e.empresa_id
      WHERE ${w.join(' AND ')}
      ORDER BY f.creado_en DESC`,v);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});

// Eliminar factura de guías (desasocia las OCs)
app.delete('/api/oc-guias/facturas/:id', auth, async(req,res)=>{
  const client=await pool.connect();
  try{
    await client.query('BEGIN');
    await client.query('UPDATE ordenes_compra SET factura_guia_id=NULL WHERE factura_guia_id=$1',[req.params.id]);
    await client.query('DELETE FROM oc_factura_guias WHERE factura_id=$1',[req.params.id]);
    await client.query('COMMIT');
    res.json({ok:true});
  }catch(e){await client.query('ROLLBACK');res.status(400).json({error:e.message});}
  finally{client.release();}
});

// ══ PWA — MANIFEST, SERVICE WORKER, ICON ══
app.get('/manifest.json', (req,res)=>{
  res.json({
    name:'Empresas Poo',short_name:'EP Gestión',description:'Sistema de Gestión Forestal',
    start_url:'/',display:'standalone',orientation:'portrait',
    background_color:'#1E3A2D',theme_color:'#1E3A2D',
    icons:[{src:'/icon-192.png',sizes:'192x192',type:'image/png'},{src:'/icon-512.png',sizes:'512x512',type:'image/png'}]
  });
});
app.get('/sw.js', (req,res)=>{
  res.setHeader('Content-Type','application/javascript');
  res.send(`
    const CACHE='ep-v1';
    self.addEventListener('install',e=>{self.skipWaiting();});
    self.addEventListener('activate',e=>{e.waitUntil(caches.keys().then(ks=>Promise.all(ks.filter(k=>k!==CACHE).map(k=>caches.delete(k)))));});
    self.addEventListener('fetch',e=>{
      if(e.request.method!=='GET')return;
      if(e.request.url.includes('/api/'))return;
      e.respondWith(fetch(e.request).then(r=>{if(r.ok){const c=r.clone();caches.open(CACHE).then(ca=>ca.put(e.request,c));}return r;}).catch(()=>caches.match(e.request)));
    });
  `);
});
// Generar íconos PWA dinámicamente como SVG→PNG (fallback SVG)
app.get('/icon-:size.png', (req,res)=>{
  const s=parseInt(req.params.size)||192;
  res.setHeader('Content-Type','image/svg+xml');
  res.send(`<svg xmlns="http://www.w3.org/2000/svg" width="${s}" height="${s}" viewBox="0 0 100 100"><rect width="100" height="100" rx="18" fill="#1E3A2D"/><text x="50" y="68" font-size="52" font-weight="700" text-anchor="middle" fill="#D4C5A9" font-family="Arial">LP</text></svg>`);
});

// ══════════════════════════════════════════════════════
// MÓDULO CONTRATOS — GENERACIÓN AUTOMÁTICA
// ══════════════════════════════════════════════════════
async function setupContratos(q){
  // Campos adicionales en empresas para contrato
  try{await q("ALTER TABLE empresas ADD COLUMN IF NOT EXISTS representante_nombre VARCHAR(150)");}catch(e){}
  try{await q("ALTER TABLE empresas ADD COLUMN IF NOT EXISTS representante_rut VARCHAR(15)");}catch(e){}
  try{await q("ALTER TABLE empresas ADD COLUMN IF NOT EXISTS region VARCHAR(60) DEFAULT 'VIII del Bio Bio'");}catch(e){}
  try{await q("ALTER TABLE empresas ADD COLUMN IF NOT EXISTS comuna VARCHAR(100)");}catch(e){}
  try{await q("ALTER TABLE empresas ADD COLUMN IF NOT EXISTS firma_representante TEXT");}catch(e){}
  try{await q("ALTER TABLE empresas ADD COLUMN IF NOT EXISTS timbre_empresa TEXT");}catch(e){}

  // Funciones estándar por cargo (catálogo editable)
  await q(`CREATE TABLE IF NOT EXISTS contrato_funciones (
    funcion_id SERIAL PRIMARY KEY,
    cargo VARCHAR(150) NOT NULL UNIQUE,
    descripcion_funcion TEXT NOT NULL,
    activo BOOLEAN DEFAULT true,
    creado_en TIMESTAMP DEFAULT NOW()
  )`);

  // Registro de contratos generados
  await q(`CREATE TABLE IF NOT EXISTS contratos (
    contrato_id SERIAL PRIMARY KEY,
    persona_id INT NOT NULL REFERENCES personal(persona_id),
    empresa_id INT NOT NULL REFERENCES empresas(empresa_id),
    tipo_contrato VARCHAR(30) NOT NULL,
    es_actualizacion BOOLEAN DEFAULT false,
    fecha_contrato DATE NOT NULL,
    fecha_inicio DATE NOT NULL,
    fecha_termino DATE,
    lugar_firma VARCHAR(100),
    funcion_texto TEXT NOT NULL,
    jornada_tipo VARCHAR(20) DEFAULT 'normal',
    jornada_horas INT DEFAULT 44,
    jornada_texto TEXT,
    lugar_prestacion TEXT,
    sueldo_base NUMERIC(12,2) NOT NULL DEFAULT 0,
    bono_responsabilidad NUMERIC(12,2) DEFAULT 0,
    bono_produccion_fijo NUMERIC(12,2) DEFAULT 0,
    bono_produccion_variable BOOLEAN DEFAULT false,
    bono_produccion_tarifa NUMERIC(12,2) DEFAULT 0,
    bono_produccion_detalle TEXT,
    semana_corrida BOOLEAN DEFAULT false,
    asig_colacion NUMERIC(10,2) DEFAULT 0,
    asig_movilizacion NUMERIC(10,2) DEFAULT 0,
    asig_viatico NUMERIC(10,2) DEFAULT 0,
    tiene_alimentacion BOOLEAN DEFAULT false,
    alimentacion_detalle TEXT,
    otros_beneficios TEXT,
    observaciones TEXT,
    usuario VARCHAR(100),
    creado_en TIMESTAMP DEFAULT NOW()
  )`);
  try{await q("ALTER TABLE contratos ADD COLUMN IF NOT EXISTS jornada_tipo VARCHAR(20) DEFAULT 'normal'");}catch(e){}

  // Seed representante legal por defecto en empresas del grupo Poo
  try{
    await q("UPDATE empresas SET representante_nombre='LEONIDAS FERNANDO POO ZENTENO',representante_rut='8.413.067-2' WHERE representante_nombre IS NULL OR representante_nombre=''");
    await q("UPDATE empresas SET comuna='Nacimiento' WHERE comuna IS NULL OR comuna=''");
    await q("UPDATE empresas SET region='VIII del Bio Bio' WHERE region IS NULL OR region=''");
  }catch(e){}

  // Seed funciones estándar por cargo
  try{
    const c=await q('SELECT COUNT(*) FROM contrato_funciones');
    if(parseInt(c.rows[0].count)===0){
      const funcs=[
        ['MECANICO','Mantener, reparar y diagnosticar el correcto funcionamiento de la maquinaria forestal y equipos de la empresa, realizando mantenciones preventivas y correctivas, cambios de repuestos, ajustes mecánicos, hidráulicos y eléctricos según corresponda, registrando las intervenciones efectuadas y velando por el cumplimiento de los estándares de seguridad y calidad establecidos por la empresa.'],
        ['AYUDANTE MECANICO','Apoyar al mecánico en las labores de mantención y reparación de maquinaria y equipos, preparando herramientas, repuestos e insumos, colaborando en el desarme, armado y limpieza de piezas, y realizando tareas complementarias de apoyo operativo bajo supervisión.'],
        ['OPERADOR DE MAQUINARIA','Operar maquinaria forestal asignada (torre, skidder, harvester, forwarder, cargador u otros equipos) en las distintas faenas de la empresa, ejecutando las labores productivas conforme a las instrucciones operacionales recibidas, realizando el chequeo diario del equipo, reportando fallas o anomalías, y cumpliendo con los estándares de seguridad, calidad y producción establecidos.'],
        ['OPERADOR DE EXCAVADORA','Operar excavadora hidráulica en labores de movimiento de tierra, construcción de caminos, carguío y tareas forestales según requerimientos de faena, realizando el chequeo diario del equipo, reportando fallas o anomalías y velando por el cumplimiento de los estándares operacionales y de seguridad.'],
        ['OPERADOR DE RETROEXCAVADORA','Operar retroexcavadora en labores de excavación, movimiento de tierra, zanjas, nivelaciones y apoyo a faenas, efectuando mantención básica del equipo, reportando fallas y cumpliendo las instrucciones operacionales y normas de seguridad.'],
        ['OPERADOR DE PROCESADOR','Operar procesador forestal en labores de descorte, trozado y clasificación de madera, cumpliendo con las especificaciones de largo, diámetro y calidad solicitadas, velando por la productividad y los estándares de seguridad establecidos.'],
        ['OPERADOR DE TRINEUMATICO','Operar cargador trineumático en labores de carguío, apilado, madereo y traslado de madera dentro de faena, realizando chequeo diario del equipo y cumpliendo las instrucciones operacionales y de seguridad.'],
        ['CHOFER CONDUCTOR DE CAMION','Conducir camión asignado por la empresa en labores de transporte de materiales, insumos o maquinaria entre puntos operacionales, velando por el correcto uso del vehículo, cumpliendo la normativa vigente de tránsito, realizando el chequeo diario y reportando fallas o novedades.'],
        ['CHOFER CONDUCTOR DE CAMION TOLVA','Conducir y operar camión tolva en labores de movimiento de tierra, efectuando el traslado, carga y descarga de material según las necesidades de la faena, velando por el correcto uso del equipo y el cumplimiento de las instrucciones de la empresa.'],
        ['CHOFER CAMA BAJA','Conducir camión cama baja en labores de traslado de maquinaria entre faenas y puntos operacionales, verificando el correcto amarre y aseguramiento de la carga, cumpliendo la normativa de tránsito aplicable a carga sobredimensionada, y reportando cualquier incidente o falla.'],
        ['MOTOSIERRISTA','Efectuar labores de corte, volteo, desrame y trozado de madera con motosierra según instrucciones operacionales, manteniendo el equipo en buenas condiciones, respetando las medidas de seguridad y cumpliendo los estándares de producción y calidad establecidos.'],
        ['JEFE DE FAENA','Dirigir, coordinar y supervisar las labores operacionales ejecutadas en la faena asignada, gestionando recursos humanos y técnicos, velando por el cumplimiento de los objetivos de producción, los estándares de seguridad y calidad, y reportando al nivel superior el avance y novedades de la operación.'],
        ['SUPERVISOR','Supervisar las actividades operacionales del personal a cargo, asegurando el cumplimiento de los procedimientos, estándares de calidad, seguridad y productividad, detectando y reportando desviaciones, gestionando la asignación de tareas y apoyando la coordinación con la jefatura.'],
        ['COORDINADOR DE OPERACIONES','Coordinar las operaciones productivas de la empresa, planificar la asignación de recursos humanos y maquinaria, interactuar con supervisores y jefes de faena, controlar indicadores operacionales, y gestionar la logística necesaria para el correcto desarrollo de las faenas.'],
        ['PREVENCIONISTA DE RIESGOS','Gestionar el sistema de prevención de riesgos laborales de la empresa, elaborar procedimientos y planes, ejecutar capacitaciones, investigar incidentes y accidentes, realizar inspecciones de terreno, asesorar a las jefaturas y velar por el cumplimiento de la normativa legal vigente.'],
        ['ADMINISTRATIVA','Ejecutar labores administrativas de apoyo a la gestión de la empresa, incluyendo registro y archivo de documentación, atención telefónica y presencial, ingreso de datos a sistemas, manejo de correspondencia y tareas similares asignadas por la jefatura.'],
        ['ASISTENTE ADMINISTRATIVO','Apoyar en tareas administrativas de la empresa como ingreso de documentación, archivo, atención de consultas internas, manejo de planillas y colaborar en la gestión operativa diaria según las instrucciones recibidas de la jefatura.'],
        ['CONTADORA','Llevar la contabilidad de la empresa, registrar operaciones económicas, elaborar estados financieros, gestionar obligaciones tributarias, coordinar con proveedores y asesorar a la administración en materias contables y financieras.'],
        ['ENCARGADO DE COMPRAS','Gestionar el proceso de compras de la empresa, cotizar productos y servicios, emitir órdenes de compra, coordinar con proveedores, controlar plazos de entrega y asegurar el abastecimiento oportuno de insumos para las operaciones.'],
        ['JEFE DE CUADRILLA','Liderar el trabajo en terreno de una cuadrilla de trabajadores, asignar tareas, verificar avances, controlar el uso de herramientas y elementos de protección personal, reportar producción y novedades a la jefatura superior.'],
        ['SERENO','Vigilar las dependencias, maquinarias e instalaciones de la empresa durante los turnos asignados, controlando el ingreso y salida de personas y vehículos, reportando novedades y velando por la seguridad de los bienes.'],
        ['TRABAJADOR MANUAL','Ejecutar labores operativas y de apoyo en terreno según instrucciones del supervisor o jefatura, incluyendo tareas de limpieza, despeje, ordenamiento, traslado de materiales y apoyo a las diversas faenas.'],
        ['AYUDANTE DE OPERADOR MAQUINARIA','Apoyar al operador de maquinaria en las labores productivas, realizando tareas complementarias, preparando el área de trabajo, colaborando en el chequeo del equipo y en tareas auxiliares que permitan el correcto desarrollo de la operación.'],
        ['GERENTE GENERAL','Dirigir, administrar y representar a la empresa, planificar e implementar estrategias de gestión, coordinar las distintas áreas operativas y administrativas, tomar decisiones ejecutivas y velar por el cumplimiento de los objetivos organizacionales.']
      ];
      for(const [cargo,desc] of funcs){
        await q('INSERT INTO contrato_funciones(cargo,descripcion_funcion) VALUES($1,$2) ON CONFLICT(cargo) DO NOTHING',[cargo,desc]);
      }
      console.log('  [OK] Funciones de contrato cargadas ('+funcs.length+')');
    }
  }catch(e){console.log('[WARN] seed contratos:',e.message);}
}

// ── Funciones por cargo ──
app.get('/api/contratos/funciones', auth, async(req,res)=>{
  try{res.json((await pool.query('SELECT * FROM contrato_funciones WHERE activo=true ORDER BY cargo')).rows);}catch(e){res.status(500).json({error:e.message});}
});
app.post('/api/contratos/funciones', auth, async(req,res)=>{
  try{
    const{cargo,descripcion_funcion}=req.body;
    if(!cargo||!descripcion_funcion)return res.status(400).json({error:'Cargo y descripción requeridos'});
    const r=await pool.query('INSERT INTO contrato_funciones(cargo,descripcion_funcion) VALUES($1,$2) ON CONFLICT(cargo) DO UPDATE SET descripcion_funcion=$2 RETURNING *',[cargo.toUpperCase(),descripcion_funcion]);
    res.status(201).json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
app.put('/api/contratos/funciones/:id', auth, async(req,res)=>{
  try{
    const{cargo,descripcion_funcion,activo}=req.body;
    const r=await pool.query('UPDATE contrato_funciones SET cargo=$1,descripcion_funcion=$2,activo=$3 WHERE funcion_id=$4 RETURNING *',[cargo.toUpperCase(),descripcion_funcion,activo!==false,req.params.id]);
    res.json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
app.delete('/api/contratos/funciones/:id', auth, async(req,res)=>{
  try{await pool.query('DELETE FROM contrato_funciones WHERE funcion_id=$1',[req.params.id]);res.json({ok:true});}catch(e){res.status(400).json({error:e.message});}
});
// Función por cargo específico (para pre-cargar al seleccionar trabajador)
app.get('/api/contratos/funcion-cargo/:cargo', auth, async(req,res)=>{
  try{const r=await pool.query('SELECT * FROM contrato_funciones WHERE UPPER(cargo)=UPPER($1) AND activo=true LIMIT 1',[req.params.cargo]);res.json(r.rows[0]||null);}catch(e){res.status(500).json({error:e.message});}
});

// ── Contratos ──
app.get('/api/contratos', auth, async(req,res)=>{
  try{
    const{persona_id}=req.query;
    let w=['1=1'],v=[];
    if(persona_id){v.push(persona_id);w.push(`c.persona_id=$${v.length}`);}
    const r=await pool.query(`SELECT c.*,p.nombre_completo,p.rut AS persona_rut,p.cargo,e.razon_social AS empresa_nombre FROM contratos c JOIN personal p ON c.persona_id=p.persona_id JOIN empresas e ON c.empresa_id=e.empresa_id WHERE ${w.join(' AND ')} ORDER BY c.fecha_contrato DESC,c.contrato_id DESC`,v);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});
app.get('/api/contratos/:id', auth, async(req,res)=>{
  try{
    const r=await pool.query(`SELECT
      c.contrato_id,c.persona_id,c.empresa_id,c.tipo_contrato,c.es_actualizacion,
      c.fecha_contrato,c.fecha_inicio,c.fecha_termino,c.lugar_firma,c.funcion_texto,
      c.jornada_tipo,c.jornada_horas,c.jornada_texto,c.lugar_prestacion,
      c.sueldo_base,c.bono_responsabilidad,c.bono_produccion_fijo,c.bono_produccion_variable,
      c.bono_produccion_tarifa,c.bono_produccion_detalle,c.semana_corrida,
      c.asig_colacion,c.asig_movilizacion,c.asig_viatico,c.tiene_alimentacion,c.alimentacion_detalle,
      c.otros_beneficios,c.observaciones,c.usuario,c.creado_en,
      p.nombre_completo,p.rut,p.cargo,p.fecha_nacimiento,p.direccion,p.comuna,
      p.nacionalidad,p.estado_civil,p.region AS persona_region,
      e.razon_social AS empresa_nombre,e.rut AS empresa_rut,
      e.direccion AS empresa_direccion,e.comuna AS empresa_comuna,e.region AS empresa_region,
      e.representante_nombre,e.representante_rut,e.logo_base64,e.firma_representante,e.timbre_empresa
      FROM contratos c
      JOIN personal p ON c.persona_id=p.persona_id
      JOIN empresas e ON c.empresa_id=e.empresa_id
      WHERE c.contrato_id=$1`,[req.params.id]);
    if(!r.rows.length)return res.status(404).json({error:'Contrato no encontrado'});
    res.json(r.rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});
app.post('/api/contratos', auth, async(req,res)=>{
  try{
    const b=req.body;
    if(!b.persona_id||!b.empresa_id||!b.tipo_contrato||!b.fecha_contrato||!b.fecha_inicio||!b.funcion_texto){
      return res.status(400).json({error:'Persona, empresa, tipo, fechas y función son obligatorios'});
    }
    if(b.tipo_contrato==='plazo_fijo'&&!b.fecha_termino){
      return res.status(400).json({error:'Contrato a plazo fijo requiere fecha de término'});
    }
    const r=await pool.query(`INSERT INTO contratos(persona_id,empresa_id,tipo_contrato,es_actualizacion,fecha_contrato,fecha_inicio,fecha_termino,lugar_firma,funcion_texto,jornada_tipo,jornada_horas,jornada_texto,lugar_prestacion,sueldo_base,bono_responsabilidad,bono_produccion_fijo,bono_produccion_variable,bono_produccion_tarifa,bono_produccion_detalle,semana_corrida,asig_colacion,asig_movilizacion,asig_viatico,tiene_alimentacion,alimentacion_detalle,otros_beneficios,observaciones,usuario) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28) RETURNING *`,
      [b.persona_id,b.empresa_id,b.tipo_contrato,b.es_actualizacion||false,b.fecha_contrato,b.fecha_inicio,b.fecha_termino||null,b.lugar_firma||'Nacimiento',b.funcion_texto,b.jornada_tipo||'normal',parseInt(b.jornada_horas)||44,b.jornada_texto||null,b.lugar_prestacion||null,parseFloat(b.sueldo_base)||0,parseFloat(b.bono_responsabilidad)||0,parseFloat(b.bono_produccion_fijo)||0,b.bono_produccion_variable||false,parseFloat(b.bono_produccion_tarifa)||0,b.bono_produccion_detalle||null,b.semana_corrida||false,parseFloat(b.asig_colacion)||0,parseFloat(b.asig_movilizacion)||0,parseFloat(b.asig_viatico)||0,b.tiene_alimentacion||false,b.alimentacion_detalle||null,b.otros_beneficios||null,b.observaciones||null,req.user.email]);
    // Si es contrato nuevo, actualizar fecha_ingreso en personal si no tiene
    if(!b.es_actualizacion){
      await pool.query('UPDATE personal SET fecha_ingreso=COALESCE(fecha_ingreso,$1),tipo_contrato=$2,fecha_termino=$3 WHERE persona_id=$4',
        [b.fecha_inicio,({plazo_fijo:'A Plazo',indefinido:'Indefinido',obra_servicio:'Por Obra'})[b.tipo_contrato]||'Indefinido',b.fecha_termino||null,b.persona_id]);
    }
    // Actualizar haberes del trabajador con los del contrato (siempre refleja el contrato más reciente)
    await pool.query(`UPDATE personal SET sueldo_base=$1,bono_responsabilidad=$2,bono_produccion_fijo=$3,bono_produccion_variable=$4,bono_produccion_tarifa=$5,bono_produccion_detalle=$6,semana_corrida=$7,asig_colacion=$8,asig_movilizacion=$9,asig_viatico=$10,tiene_alimentacion=$11,alimentacion_detalle=$12,funcion_contrato=COALESCE(funcion_contrato,$13) WHERE persona_id=$14`,
      [parseFloat(b.sueldo_base)||0,parseFloat(b.bono_responsabilidad)||0,parseFloat(b.bono_produccion_fijo)||0,b.bono_produccion_variable||false,parseFloat(b.bono_produccion_tarifa)||0,b.bono_produccion_detalle||null,b.semana_corrida||false,parseFloat(b.asig_colacion)||0,parseFloat(b.asig_movilizacion)||0,parseFloat(b.asig_viatico)||0,b.tiene_alimentacion||false,b.alimentacion_detalle||null,b.funcion_texto,b.persona_id]);
    res.status(201).json(r.rows[0]);
  }catch(e){res.status(400).json({error:e.message});}
});
app.delete('/api/contratos/:id', auth, async(req,res)=>{
  try{await pool.query('DELETE FROM contratos WHERE contrato_id=$1',[req.params.id]);res.json({ok:true});}catch(e){res.status(400).json({error:e.message});}
});

// ══════════════════════════════════════════════════════
// ANEXOS DE CONTRATO
// ══════════════════════════════════════════════════════
async function setupAnexos(q){
  await q(`CREATE TABLE IF NOT EXISTS contrato_anexos (
    anexo_id SERIAL PRIMARY KEY,
    persona_id INT NOT NULL REFERENCES personal(persona_id),
    empresa_id INT NOT NULL REFERENCES empresas(empresa_id),
    contrato_id INT REFERENCES contratos(contrato_id),
    fecha_anexo DATE NOT NULL,
    fecha_contrato_original DATE,
    funcion_original TEXT,
    lugar_firma VARCHAR(120) DEFAULT 'Nacimiento',
    clausulas JSONB NOT NULL DEFAULT '[]',
    observaciones TEXT,
    usuario VARCHAR(120),
    creado_en TIMESTAMP DEFAULT NOW()
  )`);
  // Migración: si la columna ya existía como VARCHAR(255), ampliar
  try{await q('ALTER TABLE contrato_anexos ALTER COLUMN funcion_original TYPE TEXT');}catch(e){}
}
setupAnexos(pool.query.bind(pool)).catch(function(e){console.log('[WARN] anexos:',e.message);});

// Listar anexos
app.get('/api/anexos', auth, async(req,res)=>{
  try{
    const{persona_id,empresa_id}=req.query;
    let w=['1=1'],v=[];
    if(persona_id){v.push(persona_id);w.push(`a.persona_id=$${v.length}`);}
    if(empresa_id){v.push(empresa_id);w.push(`a.empresa_id=$${v.length}`);}
    const r=await pool.query(`SELECT a.*,p.nombre_completo,p.rut,p.cargo,p.faena_id,
      f.nombre AS faena_nombre,
      e.razon_social AS empresa_nombre,e.rut AS empresa_rut,e.direccion AS empresa_direccion,
      e.comuna AS empresa_comuna,e.region AS empresa_region,
      e.representante_nombre,e.representante_rut,e.logo_base64,e.firma_representante,e.timbre_empresa
      FROM contrato_anexos a
      JOIN personal p ON a.persona_id=p.persona_id
      LEFT JOIN faenas f ON p.faena_id=f.faena_id
      JOIN empresas e ON a.empresa_id=e.empresa_id
      WHERE ${w.join(' AND ')}
      ORDER BY a.fecha_anexo DESC,a.anexo_id DESC`,v);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});
// Obtener anexo individual
app.get('/api/anexos/:id', auth, async(req,res)=>{
  try{
    const r=await pool.query(`SELECT a.*,p.nombre_completo,p.rut,p.cargo,
      e.razon_social AS empresa_nombre,e.rut AS empresa_rut,e.direccion AS empresa_direccion,
      e.comuna AS empresa_comuna,e.region AS empresa_region,
      e.representante_nombre,e.representante_rut,e.logo_base64,e.firma_representante,e.timbre_empresa
      FROM contrato_anexos a
      JOIN personal p ON a.persona_id=p.persona_id
      JOIN empresas e ON a.empresa_id=e.empresa_id
      WHERE a.anexo_id=$1`,[req.params.id]);
    if(!r.rows.length)return res.status(404).json({error:'Anexo no encontrado'});
    res.json(r.rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});
// Crear anexo individual o masivo
app.post('/api/anexos', auth, async(req,res)=>{
  const client=await pool.connect();
  try{
    await client.query('BEGIN');
    const b=req.body;
    if(!b.empresa_id||!b.fecha_anexo||!Array.isArray(b.clausulas)||!b.clausulas.length){
      throw new Error('Empresa, fecha y al menos una cláusula son obligatorios');
    }
    // Lista de personas: puede ser una sola (persona_id) o varias (persona_ids)
    let personas=[];
    if(Array.isArray(b.persona_ids)&&b.persona_ids.length){
      personas=b.persona_ids;
    }else if(b.persona_id){
      personas=[b.persona_id];
    }else{
      throw new Error('Debe indicar persona_id o persona_ids');
    }
    const creados=[];
    for(const pid of personas){
      // Buscar último contrato del trabajador (solo para vincular)
      const ultC=await client.query('SELECT contrato_id FROM contratos WHERE persona_id=$1 AND empresa_id=$2 ORDER BY fecha_contrato DESC,contrato_id DESC LIMIT 1',[pid,b.empresa_id]);
      const cId=ultC.rows.length?ultC.rows[0].contrato_id:null;
      // Usar la fecha_ingreso del trabajador como fecha del contrato original
      const persR=await client.query('SELECT cargo,fecha_ingreso FROM personal WHERE persona_id=$1',[pid]);
      const fOrig=persR.rows.length?persR.rows[0].fecha_ingreso:null;
      const funOrig=persR.rows.length?persR.rows[0].cargo:(b.funcion_original||null);
      const r=await client.query(`INSERT INTO contrato_anexos(persona_id,empresa_id,contrato_id,fecha_anexo,fecha_contrato_original,funcion_original,lugar_firma,clausulas,observaciones,usuario)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10) RETURNING anexo_id`,
        [pid,b.empresa_id,cId,b.fecha_anexo,fOrig,funOrig,b.lugar_firma||'Nacimiento',JSON.stringify(b.clausulas),b.observaciones||null,req.user.email]);
      creados.push(r.rows[0].anexo_id);
    }
    await client.query('COMMIT');
    res.status(201).json({ok:true,creados,total:creados.length});
  }catch(e){await client.query('ROLLBACK');res.status(400).json({error:e.message});}
  finally{client.release();}
});
// Eliminar anexo
app.delete('/api/anexos/:id', auth, async(req,res)=>{
  try{await pool.query('DELETE FROM contrato_anexos WHERE anexo_id=$1',[req.params.id]);res.json({ok:true});}
  catch(e){res.status(400).json({error:e.message});}
});

// ══════════════════════════════════════════════════════
// FINIQUITOS
// ══════════════════════════════════════════════════════
async function setupFiniquitos(q){
  await q(`CREATE TABLE IF NOT EXISTS finiquitos (
    finiquito_id SERIAL PRIMARY KEY,
    persona_id INT NOT NULL REFERENCES personal(persona_id),
    empresa_id INT NOT NULL REFERENCES empresas(empresa_id),
    causal VARCHAR(200),
    fecha_inicio DATE,
    fecha_termino DATE,
    fecha_aviso DATE,
    es_zona_extrema BOOLEAN DEFAULT false,
    tipo_sueldo VARCHAR(20) DEFAULT 'Fijo',
    valor_sueldo_minimo NUMERIC(14,2),
    valor_uf NUMERIC(14,4),
    sueldo_base NUMERIC(14,2) DEFAULT 0,
    gratificacion_mensual BOOLEAN DEFAULT true,
    asignacion_colacion NUMERIC(14,2) DEFAULT 0,
    asignacion_movilizacion NUMERIC(14,2) DEFAULT 0,
    haber_var_mes1 NUMERIC(14,2) DEFAULT 0,
    haber_var_mes2 NUMERIC(14,2) DEFAULT 0,
    haber_var_mes3 NUMERIC(14,2) DEFAULT 0,
    promedio_variable NUMERIC(14,2) DEFAULT 0,
    dias_feriado_tomados NUMERIC(8,2) DEFAULT 0,
    dias_inhabiles NUMERIC(8,2) DEFAULT 0,
    remuneracion_pendiente NUMERIC(14,2) DEFAULT 0,
    descuentos NUMERIC(14,2) DEFAULT 0,
    -- Resultados calculados
    anios_servicio NUMERIC(8,2),
    dias_feriado_legal NUMERIC(8,2),
    dias_feriado_pendiente NUMERIC(8,2),
    total_haberes NUMERIC(14,2),
    indem_aviso_previo NUMERIC(14,2) DEFAULT 0,
    indem_anios_servicio NUMERIC(14,2) DEFAULT 0,
    indem_vacaciones NUMERIC(14,2) DEFAULT 0,
    indem_tiempo_servido NUMERIC(14,2) DEFAULT 0,
    total_finiquito NUMERIC(14,2),
    observaciones TEXT,
    usuario VARCHAR(120),
    creado_en TIMESTAMP DEFAULT NOW()
  )`);
  try{await q('ALTER TABLE finiquitos ADD COLUMN IF NOT EXISTS descuentos NUMERIC(14,2) DEFAULT 0');}catch(e){}
  // Cartas de término de contrato
  await q(`CREATE TABLE IF NOT EXISTS cartas_termino (
    carta_id SERIAL PRIMARY KEY,
    persona_id INT NOT NULL REFERENCES personal(persona_id),
    empresa_id INT NOT NULL REFERENCES empresas(empresa_id),
    finiquito_id INT REFERENCES finiquitos(finiquito_id),
    fecha_carta DATE NOT NULL,
    fecha_contrato DATE,
    fecha_termino DATE NOT NULL,
    causal VARCHAR(200),
    causal_hecho TEXT,
    cargo VARCHAR(150),
    estado VARCHAR(20) DEFAULT 'PENDIENTE',
    indem_anios_servicio NUMERIC(14,2) DEFAULT 0,
    indem_vacaciones NUMERIC(14,2) DEFAULT 0,
    indem_tiempo_servido NUMERIC(14,2) DEFAULT 0,
    indem_aviso_previo NUMERIC(14,2) DEFAULT 0,
    observaciones TEXT,
    usuario VARCHAR(120),
    creado_en TIMESTAMP DEFAULT NOW()
  )`);
  try{await q("ALTER TABLE cartas_termino ADD COLUMN IF NOT EXISTS estado VARCHAR(20) DEFAULT 'PENDIENTE'");}catch(e){}
}
setupFiniquitos(pool.query.bind(pool)).catch(function(e){console.log('[WARN] finiquitos:',e.message);});

app.get('/api/finiquitos', auth, async(req,res)=>{
  try{
    const{persona_id}=req.query;
    let w=['1=1'],v=[];
    if(persona_id){v.push(persona_id);w.push(`f.persona_id=$${v.length}`);}
    const r=await pool.query(`SELECT f.*,p.nombre_completo,p.rut,p.cargo,p.fecha_ingreso,
      e.razon_social AS empresa_nombre,e.rut AS empresa_rut,e.direccion AS empresa_direccion,
      e.comuna AS empresa_comuna,e.region AS empresa_region,
      e.representante_nombre,e.representante_rut,e.logo_base64,e.firma_representante,e.timbre_empresa
      FROM finiquitos f
      JOIN personal p ON f.persona_id=p.persona_id
      JOIN empresas e ON f.empresa_id=e.empresa_id
      WHERE ${w.join(' AND ')}
      ORDER BY f.creado_en DESC`,v);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});
app.get('/api/finiquitos/:id', auth, async(req,res)=>{
  try{
    const r=await pool.query(`SELECT f.*,p.nombre_completo,p.rut,p.cargo,p.fecha_ingreso,p.direccion AS persona_direccion,p.comuna AS persona_comuna,p.region AS persona_region,
      e.razon_social AS empresa_nombre,e.rut AS empresa_rut,e.direccion AS empresa_direccion,
      e.comuna AS empresa_comuna,e.region AS empresa_region,
      e.representante_nombre,e.representante_rut,e.logo_base64,e.firma_representante,e.timbre_empresa
      FROM finiquitos f
      JOIN personal p ON f.persona_id=p.persona_id
      JOIN empresas e ON f.empresa_id=e.empresa_id
      WHERE f.finiquito_id=$1`,[req.params.id]);
    if(!r.rows.length)return res.status(404).json({error:'Finiquito no encontrado'});
    res.json(r.rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});
app.post('/api/finiquitos', auth, async(req,res)=>{
  const client=await pool.connect();
  try{
    await client.query('BEGIN');
    const b=req.body;
    if(!b.persona_id||!b.empresa_id||!b.fecha_inicio||!b.fecha_termino)throw new Error('Datos obligatorios faltantes');
    const r=await client.query(`INSERT INTO finiquitos(persona_id,empresa_id,causal,fecha_inicio,fecha_termino,fecha_aviso,es_zona_extrema,tipo_sueldo,valor_sueldo_minimo,valor_uf,sueldo_base,gratificacion_mensual,asignacion_colacion,asignacion_movilizacion,haber_var_mes1,haber_var_mes2,haber_var_mes3,promedio_variable,dias_feriado_tomados,dias_inhabiles,remuneracion_pendiente,descuentos,anios_servicio,dias_feriado_legal,dias_feriado_pendiente,total_haberes,indem_aviso_previo,indem_anios_servicio,indem_vacaciones,indem_tiempo_servido,total_finiquito,observaciones,usuario)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33) RETURNING finiquito_id`,
      [b.persona_id,b.empresa_id,b.causal||null,b.fecha_inicio,b.fecha_termino,b.fecha_aviso||null,b.es_zona_extrema||false,b.tipo_sueldo||'Fijo',b.valor_sueldo_minimo||0,b.valor_uf||0,b.sueldo_base||0,b.gratificacion_mensual!==false,b.asignacion_colacion||0,b.asignacion_movilizacion||0,b.haber_var_mes1||0,b.haber_var_mes2||0,b.haber_var_mes3||0,b.promedio_variable||0,b.dias_feriado_tomados||0,b.dias_inhabiles||0,b.remuneracion_pendiente||0,b.descuentos||0,b.anios_servicio||0,b.dias_feriado_legal||0,b.dias_feriado_pendiente||0,b.total_haberes||0,b.indem_aviso_previo||0,b.indem_anios_servicio||0,b.indem_vacaciones||0,b.indem_tiempo_servido||0,b.total_finiquito||0,b.observaciones||null,req.user.email]);
    const finiquito_id=r.rows[0].finiquito_id;
    // Traspasar valores a la(s) carta(s) PENDIENTE del mismo trabajador y empresa
    const cartaUpd=await client.query(`UPDATE cartas_termino SET 
        finiquito_id=$1,
        indem_anios_servicio=$2,
        indem_vacaciones=$3,
        indem_tiempo_servido=$4,
        indem_aviso_previo=$5,
        estado='COMPLETADA'
      WHERE persona_id=$6 AND empresa_id=$7 AND (estado IS NULL OR estado='PENDIENTE')
      RETURNING carta_id`,
      [finiquito_id,b.indem_anios_servicio||0,b.indem_vacaciones||0,b.indem_tiempo_servido||0,b.indem_aviso_previo||0,b.persona_id,b.empresa_id]);
    await client.query('COMMIT');
    res.status(201).json({ok:true,finiquito_id:finiquito_id,cartas_actualizadas:cartaUpd.rows.length});
  }catch(e){await client.query('ROLLBACK');res.status(400).json({error:e.message});}
  finally{client.release();}
});
app.delete('/api/finiquitos/:id', auth, async(req,res)=>{
  try{await pool.query('DELETE FROM finiquitos WHERE finiquito_id=$1',[req.params.id]);res.json({ok:true});}
  catch(e){res.status(400).json({error:e.message});}
});

// Cartas de término de contrato
app.get('/api/cartas-termino', auth, async(req,res)=>{
  try{
    const{persona_id}=req.query;
    let w=['1=1'],v=[];
    if(persona_id){v.push(persona_id);w.push(`c.persona_id=$${v.length}`);}
    const r=await pool.query(`SELECT c.*,p.nombre_completo,p.rut,p.cargo,p.direccion AS persona_direccion,p.comuna AS persona_comuna,
      e.razon_social AS empresa_nombre,e.rut AS empresa_rut,
      e.representante_nombre,e.representante_rut,e.logo_base64,e.firma_representante,e.timbre_empresa
      FROM cartas_termino c
      JOIN personal p ON c.persona_id=p.persona_id
      JOIN empresas e ON c.empresa_id=e.empresa_id
      WHERE ${w.join(' AND ')}
      ORDER BY c.fecha_carta DESC,c.carta_id DESC`,v);
    res.json(r.rows);
  }catch(e){res.status(500).json({error:e.message});}
});
app.get('/api/cartas-termino/:id', auth, async(req,res)=>{
  try{
    const r=await pool.query(`SELECT c.*,p.nombre_completo,p.rut,p.cargo,p.direccion AS persona_direccion,p.comuna AS persona_comuna,
      e.razon_social AS empresa_nombre,e.rut AS empresa_rut,
      e.representante_nombre,e.representante_rut,e.logo_base64,e.firma_representante,e.timbre_empresa
      FROM cartas_termino c
      JOIN personal p ON c.persona_id=p.persona_id
      JOIN empresas e ON c.empresa_id=e.empresa_id
      WHERE c.carta_id=$1`,[req.params.id]);
    if(!r.rows.length)return res.status(404).json({error:'Carta no encontrada'});
    res.json(r.rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});
app.post('/api/cartas-termino', auth, async(req,res)=>{
  try{
    const b=req.body;
    if(!b.persona_id||!b.empresa_id||!b.fecha_carta||!b.fecha_termino)throw new Error('Datos obligatorios faltantes');
    const r=await pool.query(`INSERT INTO cartas_termino(persona_id,empresa_id,finiquito_id,fecha_carta,fecha_contrato,fecha_termino,causal,causal_hecho,cargo,indem_anios_servicio,indem_vacaciones,indem_tiempo_servido,indem_aviso_previo,observaciones,usuario)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) RETURNING carta_id`,
      [b.persona_id,b.empresa_id,b.finiquito_id||null,b.fecha_carta,b.fecha_contrato||null,b.fecha_termino,b.causal||null,b.causal_hecho||null,b.cargo||null,b.indem_anios_servicio||0,b.indem_vacaciones||0,b.indem_tiempo_servido||0,b.indem_aviso_previo||0,b.observaciones||null,req.user.email]);
    res.status(201).json({ok:true,carta_id:r.rows[0].carta_id});
  }catch(e){res.status(400).json({error:e.message});}
});
app.delete('/api/cartas-termino/:id', auth, async(req,res)=>{
  try{await pool.query('DELETE FROM cartas_termino WHERE carta_id=$1',[req.params.id]);res.json({ok:true});}
  catch(e){res.status(400).json({error:e.message});}
});

// ══════════════════════════════════════════════════════
// ADMIN — LIMPIEZA DE DATOS TRANSACCIONALES
// ══════════════════════════════════════════════════════
app.post('/api/admin/wipe-transacciones', auth, async(req,res)=>{
  try{
    // Solo admin puede ejecutar
    const u=await pool.query('SELECT u.*,r.es_admin FROM usuarios u LEFT JOIN roles r ON u.rol_id=r.rol_id WHERE u.usuario_id=$1',[req.user.id]);
    if(!u.rows.length||!u.rows[0].es_admin)return res.status(403).json({error:'Solo administradores pueden ejecutar esta acción'});

    const{confirmacion}=req.body;
    if(confirmacion!=='BORRAR TODAS LAS TRANSACCIONES')return res.status(400).json({error:'Debe enviar confirmación exacta'});

    const tablas=[
      'contratos','vacaciones_registros','trans_traslados','solicitudes',
      'rend_gastos','rend_entregas','fin_cheques',
      'terreno_tob_detalle','terreno_registros',
      'mant_ot_tarea_personal','mant_ot_tareas','mant_ot_sistemas','mant_ot_materiales','mant_ot_personal','mant_ot',
      'mant_avisos','mant_programacion','mant_lecturas','mant_prog_semanal',
      'comb_cierre_guias','comb_cierres','comb_movimientos',
      'movimiento_detalle','movimiento_encabezado',
      'ordenes_compra_detalle','ordenes_compra','auditoria'
    ];

    // Pre-filtrar: solo tablas que realmente existen en la BD
    const existentes=(await pool.query(`SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename=ANY($1::text[])`,[tablas])).rows.map(function(r){return r.tablename;});

    const client=await pool.connect();
    const borrados={};
    try{
      for(const t of tablas){
        if(existentes.indexOf(t)<0){borrados[t]='no existe';continue;}
        try{
          const c=await client.query(`SELECT COUNT(*) FROM ${t}`);
          const count=parseInt(c.rows[0].count);
          await client.query(`TRUNCATE TABLE ${t} RESTART IDENTITY CASCADE`);
          borrados[t]=count;
        }catch(e){
          borrados[t]='error: '+e.message;
        }
      }
      // Reset stocks — cada uno en su propio try para no abortar
      try{
        const ok=await pool.query(`SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='stock_actual'`);
        if(ok.rows.length){await pool.query('UPDATE stock_actual SET stock=0,costo_promedio=0,ultima_actualizacion=NOW()');borrados.stock_actual='reseteado';}
      }catch(e){borrados.stock_actual='error: '+e.message;}
      try{
        const ok=await pool.query(`SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='comb_stock'`);
        if(ok.rows.length){await pool.query('UPDATE comb_stock SET litros_disponibles=0,costo_promedio=0,ultima_actualizacion=NOW()');borrados.comb_stock='reseteado';}
      }catch(e){borrados.comb_stock='error: '+e.message;}
      // Reset horómetros si se pide
      if(req.body.reset_horometros){
        try{await pool.query('UPDATE equipos SET horometro_actual=0,kilometraje_actual=0');borrados.equipos_contadores='reseteados';}
        catch(e){borrados.equipos_contadores='error: '+e.message;}
      }
      res.json({ok:true,borrados:borrados,usuario:req.user.email,fecha:new Date().toISOString()});
    }catch(e){
      res.status(500).json({error:e.message,borrados:borrados});
    }finally{
      client.release();
    }
  }catch(e){res.status(500).json({error:e.message});}
});

// SPA fallback — must be AFTER all API routes
app.get('*', (req,res)=>res.sendFile(path.join(__dirname,'frontend','index.html')));

app.listen(PORT,'0.0.0.0', async()=>{
  console.log('\n============================================================');
  console.log('  Empresas Poo — Sistema de Gestión — Puerto', PORT);
  console.log('============================================================');
  let tries=0;
  while(tries<12){try{await pool.query('SELECT 1');console.log('  [OK] BD conectada');break;}catch{tries++;console.log(`  [ESPERA] BD... ${tries}/12`);await new Promise(r=>setTimeout(r,3000));}}
  await autoSetup();
  console.log('  [OK] Sistema listo — admin@lpz.cl / admin123');
  console.log('============================================================\n');
});
