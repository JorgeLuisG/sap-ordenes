from fastapi import FastAPI, UploadFile, File
import pandas as pd
import io
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

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
    contents = await file.read()
    df = pd.read_excel(io.BytesIO(contents))

    # Limpieza SAP
    df = df.rename(columns={
        "Orden": "orden",
        "Aviso": "aviso",
        "Inic.extr.": "fecha_inicio",
        "Texto breve": "texto_breve",
        "Autor": "autor",
        "StatUsu": "status_usuario",
        "SumCosReal": "costo"
    })

    df = df[[
        "orden",
        "aviso",
        "fecha_inicio",
        "texto_breve",
        "autor",
        "status_usuario",
        "costo"
    ]]

    df["costo"] = (
        df["costo"]
        .astype(str)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
        .astype(float)
    )

    df.to_sql("ordenes_sap", engine, if_exists="append", index=False)

    return {
        "registros_insertados": len(df)
    }
