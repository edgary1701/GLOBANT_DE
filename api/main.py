from fastapi import UploadFile, File

app = FastAPI()

@app.get("/")
def root():
    return {"test": "ejecutando FastAPI...."}

@app.post("/upload/{table}")
async def upload_file(table: str, file: UploadFile = File(...)):
    
    file_location = f"data/{table}_{file.filename}"
    
    with open(file_location, "wb") as f:
        f.write(await file.read())

    return {
        "": f"Archivo cargado for {table}",
        "Ruta": file_location
    }