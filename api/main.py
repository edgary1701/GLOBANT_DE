#####librerias#####
from fastapi import FastAPI, UploadFile, File
import os
import pandas as pd
from sqlalchemy import create_engine

app = FastAPI()

engine = create_engine("sqlite:///database.db")

####testear la api
@app.get("/")
def root():
    return {"test": "ejecutando API ..."}

####cargar csv####
@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    
    os.makedirs("data", exist_ok=True)

    filename = file.filename
    table_name = os.path.splitext(filename)[0]

    file_location = f"data/{table_name}.csv"
    
    with open(file_location, "wb") as f:
        f.write(await file.read())

    #####lectura de datos
    df = pd.read_csv(file_location)

    ####ingesta de datos
    df.to_sql(table_name, engine, if_exists="append", index=False)

    return {
        "message": f"Archivo cargado en tabla {table_name}",
        "Cantidad de registros:": len(df)
    }