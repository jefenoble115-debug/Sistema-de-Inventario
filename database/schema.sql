-- 1. Tabla de Productos
CREATE TABLE productos (
    id_producto SERIAL PRIMARY KEY,
    codigo_barras VARCHAR(50) UNIQUE NOT NULL,
    nombre VARCHAR(100) NOT NULL,
    descripcion TEXT,
    stock_sistema INT DEFAULT 0 CHECK (stock_sistema >= 0),
	activo BOOLEAN DEFAULT TRUE NOT NULL
);

-- 2. Tabla de Movimientos (Entradas y Salidas)
CREATE TABLE movimientos (
    id_movimiento SERIAL PRIMARY KEY,
    id_producto INT REFERENCES productos(id_producto) ON DELETE CASCADE,
    tipo_movimiento VARCHAR(10) CHECK (tipo_movimiento IN ('ENTRADA', 'SALIDA')),
    cantidad INT NOT NULL CHECK (cantidad > 0),
    fecha_movimiento TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    observaciones TEXT
);

-- 3. Tabla de Inventarios Físicos (Auditoría)
CREATE TABLE inventarios_auditoria (
    id_inventario SERIAL PRIMARY KEY,
    fecha_auditoria TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    responsable VARCHAR(100),
	activo BOOLEAN DEFAULT TRUE NOT NULL
);

-- 4. Detalle del Inventario (Lo que el cliente cuenta físicamente)
CREATE TABLE detalle_inventario (
    id_detalle SERIAL PRIMARY KEY,
    id_inventario INT REFERENCES inventarios_auditoria(id_inventario) ON DELETE CASCADE,
    id_producto INT REFERENCES productos(id_producto),
    cantidad_fisica INT NOT NULL CHECK (cantidad_fisica >= 0)
);
--5. Tabla de Logs Auditoria (Historial de cambio)
CREATE TABLE logs_auditoria (
    id_log SERIAL PRIMARY KEY,
    nombre_tabla VARCHAR(50) NOT NULL,
    operacion VARCHAR(10) NOT NULL,
    id_registro INT NOT NULL,
    valor_anterior JSONB,
    valor_nuevo JSONB,
    usuario VARCHAR(100) DEFAULT CURRENT_USER,
    fecha_accion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

--6. Tabla de Usuarios (Compatible con Argon2id)
CREATE TABLE usuarios (
    id_usuario SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    -- Argon2id genera un hash largo (aprox. 100 char) que incluye salt y parámetros.
    -- Usamos VARCHAR(255) para asegurar espacio suficiente ante cualquier configuración del algoritmo.
    password_hash VARCHAR(255) NOT NULL,
    rol VARCHAR(30) DEFAULT 'OPERADOR' CHECK (rol IN ('ADMINISTRADOR', 'OPERADOR', 'AUDITOR')),
    activo BOOLEAN DEFAULT TRUE NOT NULL,
    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ultimo_acceso TIMESTAMP
);
