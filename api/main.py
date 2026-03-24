#####librerias#####
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import HTMLResponse
import os
import pandas as pd
from sqlalchemy import create_engine, text
from typing import List

app = FastAPI()

engine = create_engine("sqlite:///database.db")

##### SCHEMAS #####
def get_schema(table_name):
    schemas = {
        "departments": ["id", "department"],
        "jobs": ["id", "job"],
        "hired_employees": ["id", "name", "datetime", "department_id", "job_id"]
    }
    return schemas.get(table_name)

##### CREAR TABLAS #####
def create_tables():
    with engine.connect() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS departments (
            id INTEGER PRIMARY KEY,
            department TEXT
        )
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY,
            job TEXT
        )
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS hired_employees (
            id INTEGER PRIMARY KEY,
            name TEXT,
            datetime TEXT,
            department_id INTEGER,
            job_id INTEGER
        )
        """))

create_tables()

####testear la api
@app.get("/")
def root():
    return {"prueba": "ejecutando API ..."}

####cargar csv####
@app.post("/upload") 
async def upload_file(file: UploadFile = File(...)):

    os.makedirs("data", exist_ok=True)

    filename = file.filename
    table_name = os.path.splitext(filename)[0]

    file_location = f"data/{table_name}.csv"

    with open(file_location, "wb") as f:
        f.write(await file.read())

    columns = get_schema(table_name)

    if not columns:
        return {"error": f"Tabla {table_name} no soportada"}

    df = pd.read_csv(file_location, header=None)

    # Validación del esquema
    if df.shape[1] != len(columns):
        return {"error": "El CSV no coincide con el schema esperado"}

    df.columns = columns

    try:
        df.to_sql(table_name, engine, if_exists="append", index=False)

        return {
            "message": f"Archivo cargado en tabla {table_name}",
            "Cantidad de registros": len(df)
        }

    except Exception as e:
        return {"error": str(e)}

@app.post("/batch/{table}") 
def batch_insert(table: str, data: List[dict]):

    if len(data) == 0:
        return {"Error": "Envie al menos 1 registro"}

    if len(data) > 1000:
        return {"error": "Máximo 1000 registros por request"}

    df = pd.DataFrame(data)

    try:
        df.to_sql(table, engine, if_exists="append", index=False)

        return {
            "message": f"{len(data)} registros insertados en {table}"
        }

    except Exception as e:
        return {"error": str(e)}
    
 ###analytics   
@app.get("/metricas/quarter")
def obtener_empleados():
    query = """
    SELECT 
        d.department,
        j.job,
        SUM(CASE WHEN strftime('%m', e.datetime) IN ('01','02','03') THEN 1 ELSE 0 END) AS Q1,
        SUM(CASE WHEN strftime('%m', e.datetime) IN ('04','05','06') THEN 1 ELSE 0 END) AS Q2,
        SUM(CASE WHEN strftime('%m', e.datetime) IN ('07','08','09') THEN 1 ELSE 0 END) AS Q3,
        SUM(CASE WHEN strftime('%m', e.datetime) IN ('10','11','12') THEN 1 ELSE 0 END) AS Q4
    FROM hired_employees e
    JOIN departments d ON e.department_id = d.id
    JOIN jobs j ON e.job_id = j.id
    WHERE strftime('%Y', e.datetime) = '2021'
    GROUP BY d.department, j.job
    ORDER BY d.department, j.job
    """

    df = pd.read_sql(query, engine)
    return df.to_dict(orient="records")

@app.get("/metrics/departments-above-average")
def departments_above_average():

    query = """
    WITH dptos_2021 AS (
        SELECT 
            d.id,
            d.department,
            COUNT(e.id) AS hired
        FROM hired_employees e
        JOIN departments d ON e.department_id = d.id
        WHERE strftime('%Y', e.datetime) = '2021'
        GROUP BY d.id, d.department
    )
    SELECT 
        id,
        department,
        hired
    FROM dptos_2021
    WHERE hired > (SELECT AVG(hired) FROM dptos_2021)
    ORDER BY hired DESC;
    """

    df = pd.read_sql(query, engine)

    return df.to_dict(index=False)