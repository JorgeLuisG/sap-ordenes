import os
import socket
# === FORZAR IPv4 ANTES DE CUALQUIER OTRA COSA ===
if os.getenv("DISABLE_IPV6") == "1":
    _orig_getaddrinfo = socket.getaddrinfo

    def _ipv4_only_getaddrinfo(*args, **kwargs):
        kwargs["family"] = socket.AF_INET
        return _orig_getaddrinfo(*args, **kwargs)

    socket.getaddrinfo = _ipv4_only_getaddrinfo
# ===============================================

from fastapi import FastAPI, UploadFile, File, HTTPException
import pandas as pd
import io
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError



# Leer DATABASE_URL desde variables de entorno
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("La variable de entorno DATABASE_URL no está definida")

# Crear engine de SQLAlchemy
engine = create_engine(
    DATABASE_URL,
    pool_size=5,
    max_overflow=0,
    pool_pre_ping=True
)

app = FastAPI(title="Importador SAP")

@app.on_event("startup")
def startup():
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS ordenes_sap (
            id SERIAL PRIMARY KEY,
            orden VARCHAR(20),
            aviso VARCHAR(20),
            fecha_inicio DATE,
            texto_breve TEXT,
            autor VARCHAR(50),
            status_usuario TEXT,
            costo REAL
        )
        """))

@app.post("/importar-excel")
async def importar_excel(file: UploadFile = File(...)):
    # Validar tipo de archivo
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Solo se permiten archivos Excel (.xlsx, .xls)")

    try:
        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents))

        # Renombrar columnas esperadas
        df = df.rename(columns={
            "Orden": "orden",
            "Aviso": "aviso",
            "Inic.extr.": "fecha_inicio",
            "Texto breve": "texto_breve",
            "Autor": "autor",
            "StatUsu": "status_usuario",
            "SumCosReal": "costo"
        })

        # Seleccionar solo las columnas necesarias
        required_columns = ["orden", "aviso", "fecha_inicio", "texto_breve", "autor", "status_usuario", "costo"]
        if not all(col in df.columns for col in required_columns):
            missing = set(required_columns) - set(df.columns)
            raise HTTPException(status_code=400, detail=f"Faltan columnas en el archivo: {missing}")

        df = df[required_columns]

        # Convertir fecha (manejar errores)
        df["fecha_inicio"] = pd.to_datetime(df["fecha_inicio"], errors="coerce").dt.date

        # Limpiar y convertir costo
        df["costo"] = df["costo"].astype(str)
        df["costo"] = df["costo"].str.replace(".", "", regex=False)
        df["costo"] = df["costo"].str.replace(",", ".", regex=False)
        df["costo"] = pd.to_numeric(df["costo"], errors="coerce").fillna(0.0)

        # Insertar en base de datos
        with engine.begin() as conn:
            df.to_sql("ordenes_sap", conn, if_exists="append", index=False, method="multi")

        return {"registros_insertados": len(df)}

    except Exception as e:
        # En producción, registra el error real (aquí solo imprimimos)
        print(f"Error al procesar archivo: {e}")
        raise HTTPException(status_code=500, detail="Error al procesar el archivo Excel")