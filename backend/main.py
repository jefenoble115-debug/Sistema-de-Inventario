import os
import json
import contextlib
import threading
import time
import psycopg2
from psycopg2 import pool
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import stomp
from passlib.context import CryptContext
from dotenv import load_dotenv

# Cargar variables del archivo .env
load_dotenv()

# ==========================================
# 1. CONFIGURACIÓN DE SERVICIOS
# ==========================================

DB_PARAMS = {
    "host": os.getenv("DB_HOST", "localhost"),
    "database": os.getenv("DB_NAME", "postgres"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD"),
    "port": int(os.getenv("DB_PORT", "5432"))
}

ACTIVEMQ_HOST = os.getenv("ACTIVEMQ_HOST", "127.0.0.1")
ACTIVEMQ_PORT = int(os.getenv("ACTIVEMQ_PORT", "61613"))
QUEUE_NAME = os.getenv("ACTIVEMQ_QUEUE", "/queue/inventario_movimientos")
ACTIVEMQ_USER = os.getenv("ACTIVEMQ_USER", "admin")
ACTIVEMQ_PASSWORD = os.getenv("ACTIVEMQ_PASSWORD")

mq_conn = None
db_pool = None

pwd_context = CryptContext(schemes=["argon2"], deprecated="auto")


# ==========================================
# 2. MODELOS PYDANTIC
# ==========================================

class LoginRequest(BaseModel):
    usuario: str
    password: str


class RegistroRequest(BaseModel):
    username: str
    email: str
    password: str
    rol: str = "OPERADOR"


class ProductoRequest(BaseModel):
    codigo_barras: str
    nombre: str
    descripcion: str = ""


class MovimientoRequest(BaseModel):
    id_producto: int
    tipo_movimiento: str
    cantidad: int
    observaciones: str = "Movimiento desde API"


class InventarioRequest(BaseModel):
    id_producto: int
    cantidad_fisica: int
    responsable: str


# ==========================================
# 3. PROCESADOR DE MENSAJES ACTIVEMQ
# ==========================================

class InventarioListener(stomp.ConnectionListener):
    def __init__(self, conn):
        self.conn = conn

    def on_message(self, frame):
        print("\n📩 ¡Mensaje recibido desde ActiveMQ!")

        conn = None
        cursor = None

        try:
            datos = json.loads(frame.body)

            id_producto = datos.get("id_producto")
            tipo_movimiento = datos.get("tipo_movimiento")
            cantidad = datos.get("cantidad")
            observaciones = datos.get("observaciones", "Insertado vía ActiveMQ")

            if id_producto is None or tipo_movimiento is None or cantidad is None:
                print("⚠️ Error: Faltan campos obligatorios en el JSON.")
                return

            if tipo_movimiento not in ["ENTRADA", "SALIDA"]:
                print("⚠️ Error: Tipo de movimiento inválido.")
                return

            if int(cantidad) <= 0:
                print("⚠️ Error: La cantidad debe ser mayor a cero.")
                return

            conn = db_pool.getconn()
            cursor = conn.cursor()

            query = """
                INSERT INTO movimientos 
                (id_producto, tipo_movimiento, cantidad, observaciones)
                VALUES (%s, %s, %s, %s);
            """

            cursor.execute(query, (
                id_producto,
                tipo_movimiento,
                cantidad,
                observaciones
            ))

            conn.commit()
            print("💾 ✅ Movimiento guardado en la BD. ¡Trigger ejecutado!")

        except Exception as e:
            if conn:
                conn.rollback()
            print(f"❌ Error al procesar mensaje de ActiveMQ: {e}")

        finally:
            if cursor:
                cursor.close()
            if conn:
                db_pool.putconn(conn)

    def on_disconnected(self):
        print("🔌 ⚠️ Se perdió la conexión con ActiveMQ. Reintentando...")

        while True:
            try:
                time.sleep(5)

                if not self.conn.is_connected():
                    self.conn.connect(
                        login=ACTIVEMQ_USER,
                        passcode=ACTIVEMQ_PASSWORD,
                        wait=True
                    )

                    self.conn.subscribe(
                        destination=QUEUE_NAME,
                        id=1,
                        ack="auto"
                    )

                    print("🚀 Re-suscrito con éxito a ActiveMQ.")
                    break

            except Exception as e:
                print(f"❌ Fallo al reconectar: {e}. Reintentando en 5 segundos...")


def iniciar_escucha_activemq():
    global mq_conn

    while True:
        try:
            mq_conn = stomp.Connection([(ACTIVEMQ_HOST, ACTIVEMQ_PORT)])
            mq_conn.set_listener("InventarioListener", InventarioListener(mq_conn))

            mq_conn.connect(
                login=ACTIVEMQ_USER,
                passcode=ACTIVEMQ_PASSWORD,
                wait=True
            )

            mq_conn.subscribe(
                destination=QUEUE_NAME,
                id=1,
                ack="auto"
            )

            print(f"🚀 Escuchando ActiveMQ en la cola '{QUEUE_NAME}'...")
            break

        except Exception as e:
            print(f"❌ No se pudo conectar a ActiveMQ: {e}. Reintentando en 5 segundos...")
            time.sleep(5)


# ==========================================
# 4. LIFECYCLE DE FASTAPI
# ==========================================

@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool

    try:
        db_pool = psycopg2.pool.ThreadedConnectionPool(1, 10, **DB_PARAMS)
        print("✅ Pool de conexiones a PostgreSQL inicializado.")

    except Exception as e:
        print(f"❌ Error crítico: No se pudo inicializar el Pool de la BD: {e}")
        raise e

    hilo_mq = threading.Thread(target=iniciar_escucha_activemq, daemon=True)
    hilo_mq.start()

    yield

    try:
        if mq_conn and mq_conn.is_connected():
            mq_conn.disconnect()
            print("🔌 Desconectado de ActiveMQ limpiamente.")

    except Exception as e:
        print(f"Error al desconectar ActiveMQ: {e}")

    if db_pool:
        db_pool.closeall()
        print("💾 Pool de conexiones a PostgreSQL cerrado.")


# ==========================================
# 5. CREACIÓN DE APP
# ==========================================

app = FastAPI(
    title="API de Control de Inventarios",
    description="Endpoints para consultar stock, movimientos, productos e inventarios",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ==========================================
# 6. ENDPOINT INICIAL
# ==========================================

@app.get("/")
def inicio():
    return {"mensaje": "Servidor API de Inventarios Operativo"}


# ==========================================
# 7. PRODUCTOS
# ==========================================

@app.get("/productos")
def obtener_productos():
    conn = None
    cursor = None

    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT 
                id_producto, 
                codigo_barras, 
                nombre, 
                descripcion,
                stock_sistema 
            FROM productos 
            Where activo=True
            ORDER BY id_producto;
        """)

        columnas = [col[0] for col in cursor.description]
        resultado = [dict(zip(columnas, fila)) for fila in cursor.fetchall()]

        return resultado

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en la BD: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            db_pool.putconn(conn)


@app.post("/productos")
def agregar_producto(datos: ProductoRequest):
    conn = None
    cursor = None

    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()

        query = """
            INSERT INTO productos 
            (codigo_barras, nombre, descripcion, stock_sistema, activo)
            VALUES (%s, %s, %s, 0, TRUE)
            RETURNING id_producto;
        """

        cursor.execute(query, (
            datos.codigo_barras,
            datos.nombre,
            datos.descripcion
        ))

        id_producto = cursor.fetchone()[0]

        conn.commit()

        return {
            "exito": True,
            "mensaje": "Producto agregado correctamente.",
            "id_producto": id_producto
        }

    except psycopg2.IntegrityError:
        if conn:
            conn.rollback()

        raise HTTPException(
            status_code=400,
            detail="Ya existe un producto con ese código de barras."
        )

    except Exception as e:
        if conn:
            conn.rollback()

        raise HTTPException(
            status_code=500,
            detail=f"Error al agregar producto: {e}"
        )

    finally:
        if cursor:
            cursor.close()
        if conn:
            db_pool.putconn(conn)


# ==========================================
# 8. MOVIMIENTOS
# ==========================================

@app.get("/movimientos")
def obtener_movimientos():
    conn = None
    cursor = None

    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT 
                id_movimiento, 
                id_producto, 
                tipo_movimiento, 
                cantidad, 
                fecha_movimiento, 
                observaciones 
            FROM movimientos 
            ORDER BY fecha_movimiento DESC;
        """)

        columnas = [col[0] for col in cursor.description]
        resultado = [dict(zip(columnas, fila)) for fila in cursor.fetchall()]

        return resultado

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en la BD: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            db_pool.putconn(conn)


@app.post("/movimientos")
def registrar_movimiento_api(datos: MovimientoRequest):
    global mq_conn

    try:
        if datos.tipo_movimiento not in ["ENTRADA", "SALIDA"]:
            raise HTTPException(
                status_code=400,
                detail="El tipo de movimiento debe ser ENTRADA o SALIDA."
            )

        if datos.cantidad <= 0:
            raise HTTPException(
                status_code=400,
                detail="La cantidad debe ser mayor a cero."
            )

        if not mq_conn or not mq_conn.is_connected():
            raise HTTPException(
                status_code=503,
                detail="El servicio de mensajería ActiveMQ no está disponible."
            )

        mensaje_json = json.dumps({
            "id_producto": datos.id_producto,
            "tipo_movimiento": datos.tipo_movimiento,
            "cantidad": datos.cantidad,
            "observaciones": datos.observaciones
        })

        mq_conn.send(
            body=mensaje_json,
            destination=QUEUE_NAME
        )

        return {
            "exito": True,
            "mensaje": "Movimiento enviado con éxito a ActiveMQ."
        }

    except HTTPException:
        raise

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error al procesar movimiento: {e}"
        )


# ==========================================
# 9. INVENTARIO FÍSICO
# ==========================================

@app.get("/inventarios")
def obtener_inventarios():
    conn = None
    cursor = None

    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT 
                ia.id_inventario,
                ia.fecha_auditoria,
                ia.responsable,
                di.id_producto,
                p.nombre AS nombre_producto,
                di.cantidad_fisica
            FROM inventarios_auditoria ia
            INNER JOIN detalle_inventario di
                ON ia.id_inventario = di.id_inventario
            INNER JOIN productos p
                ON di.id_producto = p.id_producto
            ORDER BY ia.fecha_auditoria DESC;
        """)

        columnas = [col[0] for col in cursor.description]
        resultado = [dict(zip(columnas, fila)) for fila in cursor.fetchall()]

        return resultado

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error al obtener inventarios: {e}"
        )

    finally:
        if cursor:
            cursor.close()
        if conn:
            db_pool.putconn(conn)

@app.post("/inventario")
def registrar_inventario(datos: InventarioRequest):
    conn = None
    cursor = None

    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()

        if datos.cantidad_fisica < 0:
            raise HTTPException(
                status_code=400,
                detail="La cantidad física no puede ser negativa."
            )

        # Consultar producto y stock actual del sistema
        cursor.execute(
            """
            SELECT id_producto, nombre, stock_sistema, activo
            FROM productos 
            WHERE id_producto = %s;
            """,
            (datos.id_producto,)
        )

        producto = cursor.fetchone()

        if not producto:
            raise HTTPException(
                status_code=404,
                detail="El producto no existe."
            )

        id_producto, nombre_producto, stock_sistema, activo = producto

        if not activo:
            raise HTTPException(
                status_code=400,
                detail="No se puede inventariar un producto deshabilitado."
            )

        # Crear encabezado del inventario
        cursor.execute(
            """
            INSERT INTO inventarios_auditoria (responsable)
            VALUES (%s)
            RETURNING id_inventario;
            """,
            (datos.responsable,)
        )

        id_inventario = cursor.fetchone()[0]

        # Guardar detalle del inventario físico
        cursor.execute(
            """
            INSERT INTO detalle_inventario 
            (id_inventario, id_producto, cantidad_fisica)
            VALUES (%s, %s, %s);
            """,
            (
                id_inventario,
                datos.id_producto,
                datos.cantidad_fisica
            )
        )

        conn.commit()

        # Comparar cantidad física contra stock del sistema
        diferencia = datos.cantidad_fisica - stock_sistema

        if diferencia == 0:
            mensaje = (
                f"Conteo correcto. El producto '{nombre_producto}' coincide con el stock registrado. "
                f"Stock del sistema: {stock_sistema}, cantidad física: {datos.cantidad_fisica}."
            )
            estado_conteo = "CORRECTO"
        else:
            mensaje = (
                f"Diferencia detectada en el producto '{nombre_producto}'. "
                f"Stock del sistema: {stock_sistema}, cantidad física: {datos.cantidad_fisica}, "
                f"diferencia: {diferencia}."
            )
            estado_conteo = "DIFERENTE"

        return {
            "exito": True,
            "mensaje": mensaje,
            "estado_conteo": estado_conteo,
            "id_inventario": id_inventario,
            "id_producto": datos.id_producto,
            "producto": nombre_producto,
            "stock_sistema": stock_sistema,
            "cantidad_fisica": datos.cantidad_fisica,
            "diferencia": diferencia
        }

    except HTTPException:
        if conn:
            conn.rollback()
        raise

    except Exception as e:
        if conn:
            conn.rollback()

        raise HTTPException(
            status_code=500,
            detail=f"Error al registrar inventario: {e}"
        )

    finally:
        if cursor:
            cursor.close()
        if conn:
            db_pool.putconn(conn)


# ==========================================
# 10. LOGIN
# ==========================================

@app.post("/login")
def login(datos: LoginRequest):
    conn = None
    cursor = None

    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT password_hash, activo, rol
            FROM usuarios 
            WHERE username = %s;
            """,
            (datos.usuario,)
        )

        usuario_db = cursor.fetchone()

        if not usuario_db:
            raise HTTPException(
                status_code=401,
                detail="Usuario o contraseña incorrectos"
            )

        password_hash, activo, rol = usuario_db

        if not activo:
            raise HTTPException(
                status_code=403,
                detail="El usuario se encuentra deshabilitado"
            )

        if pwd_context.verify(datos.password, password_hash):
            cursor.execute(
                """
                UPDATE usuarios 
                SET ultimo_acceso = CURRENT_TIMESTAMP 
                WHERE username = %s;
                """,
                (datos.usuario,)
            )

            conn.commit()

            return {
                "exito": True,
                "mensaje": "Autenticación exitosa",
                "usuario": datos.usuario,
                "rol": rol
            }

        else:
            raise HTTPException(
                status_code=401,
                detail="Usuario o contraseña incorrectos"
            )

    except HTTPException:
        raise

    except Exception:
        raise HTTPException(
            status_code=500,
            detail="Error interno en autenticación"
        )

    finally:
        if cursor:
            cursor.close()
        if conn:
            db_pool.putconn(conn)


# ==========================================
# 11. REGISTRO DE USUARIOS
# ==========================================

@app.get("/usuarios")
def obtener_usuarios():
    conn = None
    cursor = None

    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT 
                id_usuario,
                username,
                email,
                rol,
                activo,
                fecha_creacion,
                ultimo_acceso
            FROM usuarios
            ORDER BY id_usuario;
        """)

        columnas = [col[0] for col in cursor.description]
        resultado = [dict(zip(columnas, fila)) for fila in cursor.fetchall()]

        return resultado

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error al obtener usuarios: {e}"
        )

    finally:
        if cursor:
            cursor.close()
        if conn:
            db_pool.putconn(conn)

@app.delete("/usuarios/{id_usuario}")
def eliminar_usuario(id_usuario: int):
    conn = None
    cursor = None

    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()

        # Verificar si el usuario existe
        cursor.execute(
            """
            SELECT id_usuario, username, activo
            FROM usuarios
            WHERE id_usuario = %s;
            """,
            (id_usuario,)
        )

        usuario = cursor.fetchone()

        if not usuario:
            raise HTTPException(
                status_code=404,
                detail="El usuario no existe."
            )

        id_usuario_db, username, activo = usuario

        if not activo:
            raise HTTPException(
                status_code=400,
                detail="El usuario ya se encuentra deshabilitado."
            )

        # Desactivar usuario, no borrarlo físicamente
        cursor.execute(
            """
            UPDATE usuarios
            SET activo = FALSE
            WHERE id_usuario = %s;
            """,
            (id_usuario,)
        )

        conn.commit()

        return {
            "exito": True,
            "mensaje": f"Usuario '{username}' eliminado correctamente."
        }

    except HTTPException:
        if conn:
            conn.rollback()
        raise

    except Exception as e:
        if conn:
            conn.rollback()

        raise HTTPException(
            status_code=500,
            detail=f"Error al eliminar usuario: {e}"
        )

    finally:
        if cursor:
            cursor.close()
        if conn:
            db_pool.putconn(conn)

@app.post("/registro")
def registrar_usuario(datos: RegistroRequest):
    conn = None
    cursor = None

    try:
        if datos.rol not in ["ADMINISTRADOR", "OPERADOR", "AUDITOR"]:
            raise HTTPException(
                status_code=400,
                detail="Rol inválido. Usa ADMINISTRADOR, OPERADOR o AUDITOR."
            )

        password_hasheada = pwd_context.hash(datos.password)

        conn = db_pool.getconn()
        cursor = conn.cursor()

        query = """
            INSERT INTO usuarios 
            (username, email, password_hash, rol)
            VALUES (%s, %s, %s, %s);
        """

        cursor.execute(query, (
            datos.username,
            datos.email,
            password_hasheada,
            datos.rol
        ))

        conn.commit()

        return {
            "exito": True,
            "mensaje": f"Usuario '{datos.username}' creado exitosamente."
        }

    except HTTPException:
        raise

    except psycopg2.IntegrityError:
        if conn:
            conn.rollback()

        raise HTTPException(
            status_code=400,
            detail="El nombre de usuario o el email ya están registrados."
        )

    except Exception:
        if conn:
            conn.rollback()

        raise HTTPException(
            status_code=500,
            detail="Error al registrar usuario"
        )


    finally:
        if cursor:
            cursor.close()
        if conn:
            db_pool.putconn(conn)
