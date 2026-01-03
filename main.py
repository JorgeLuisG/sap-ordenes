from fastapi import FastAPI, UploadFile, File, HTTPException
from sqlalchemy import create_engine, text
import pandas as pd

app = FastAPI(title="Importador SAP Órdenes")

# ==============================
# CONFIGURACIÓN BASE DE DATOS
# ==============================
DB_URL = "mysql+pymysql://usuario:password@localhost:3306/sap_ordenes"
engine = create_engine(DB_URL)

# ==============================
# COLUMNAS OBLIGATORIAS
# ==============================
COLUMNAS_REQUERIDAS = [
    "Orden",
    "Aviso",
    "Inic.extr.",
    "Texto breve",
    "Ubicación técnica",
    "PtoTrbRes",
    "Autor",
    "StatUsu",
    "Status del sistema",
    "SumCosReal",
    "TotalGen.(real)"
]

# ==============================
# ENDPOINT DE CARGA
# ==============================
@app.post("/api/ordenes/importar")
async def importar_ordenes(file: UploadFile = File(...)):

    if not file.filename.endswith((".xlsx", ".xlsm", ".xls")):
        raise HTTPException(status_code=400, detail="Formato de archivo no válido")

    try:
        df = limpiar_excel_sap(file.file)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error leyendo Excel: {e}")

    # Validar encabezados
    faltantes = [c for c in COLUMNAS_REQUERIDAS if c not in df.columns]
    if faltantes:
        raise HTTPException(
            status_code=400,
            detail=f"Faltan columnas requeridas: {faltantes}"
        )

    # Seleccionar solo columnas necesarias
    df = df[COLUMNAS_REQUERIDAS]

    # Normalización
    df["Inic.extr."] = pd.to_datetime(df["Inic.extr."], errors="coerce")
    df["SumCosReal"] = pd.to_numeric(df["SumCosReal"], errors="coerce").fillna(0)
    df["TotalGen.(real)"] = pd.to_numeric(df["TotalGen.(real)"], errors="coerce").fillna(0)

    insertadas = 0
    actualizadas = 0
    errores = []

    with engine.begin() as conn:
        for index, row in df.iterrows():
            try:
                sql = text("""
                    INSERT INTO ordenes_sap (
                        orden,
                        aviso,
                        inicio_extraordinario,
                        texto_breve,
                        ubicacion_tecnica,
                        punto_trabajo_responsable,
                        autor,
                        estado_usuario,
                        estado_sistema,
                        costo_real,
                        total_general
                    )
                    VALUES (
                        :orden,
                        :aviso,
                        :inicio_extraordinario,
                        :texto_breve,
                        :ubicacion_tecnica,
                        :punto_trabajo_responsable,
                        :autor,
                        :estado_usuario,
                        :estado_sistema,
                        :costo_real,
                        :total_general
                    )
                    ON DUPLICATE KEY UPDATE
                        aviso = VALUES(aviso),
                        inicio_extraordinario = VALUES(inicio_extraordinario),
                        texto_breve = VALUES(texto_breve),
                        ubicacion_tecnica = VALUES(ubicacion_tecnica),
                        punto_trabajo_responsable = VALUES(punto_trabajo_responsable),
                        autor = VALUES(autor),
                        estado_usuario = VALUES(estado_usuario),
                        estado_sistema = VALUES(estado_sistema),
                        costo_real = VALUES(costo_real),
                        total_general = VALUES(total_general)
                """)

                result = conn.execute(sql, {
                    "orden": str(row["Orden"]),
                    "aviso": str(row["Aviso"]),
                    "inicio_extraordinario": row["Inic.extr."],
                    "texto_breve": row["Texto breve"],
                    "ubicacion_tecnica": row["Ubicación técnica"],
                    "punto_trabajo_responsable": row["PtoTrbRes"],
                    "autor": row["Autor"],
                    "estado_usuario": row["StatUsu"],
                    "estado_sistema": row["Status del sistema"],
                    "costo_real": row["SumCosReal"],
                    "total_general": row["TotalGen.(real)"]
                })

                if result.rowcount == 1:
                    insertadas += 1
                else:
                    actualizadas += 1

            except Exception as e:
                errores.append({
                    "fila": int(index) + 2,
                    "orden": row["Orden"],
                    "error": str(e)
                })

    return {
        "status": "ok",
        "procesadas": len(df),
        "insertadas": insertadas,
        "actualizadas": actualizadas,
        "errores": errores
    }
def limpiar_excel_sap(file):
    # Leer sin encabezado
    df = pd.read_excel(file, header=None)

    # Buscar fila donde está el encabezado real
    header_row = None
    for i, row in df.iterrows():
        if "Orden" in row.values:
            header_row = i
            break

    if header_row is None:
        raise ValueError("No se encontró el encabezado SAP")

    # Releer con encabezado correcto
    df = pd.read_excel(file, header=header_row)

    # Eliminar columnas completamente vacías
    df = df.dropna(axis=1, how="all")

    # Eliminar filas sin Orden
    df = df[df["Orden"].notna()]

    # Limpiar espacios SAP
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # Normalizar fecha
    df["Inic.extr."] = pd.to_datetime(
        df["Inic.extr."],
        dayfirst=True,
        errors="coerce"
    )

    # Normalizar importes (SAP europeo)
    for col in ["SumCosReal", "TotalGen.(real)"]:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
        )
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Asegurar columnas faltantes
    for col in COLUMNAS_REQUERIDAS:
        if col not in df.columns:
            df[col] = None

    # Reordenar columnas
    df = df[COLUMNAS_REQUERIDAS]

    return df
