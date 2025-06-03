from fastapi import FastAPI, File, UploadFile, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import os
from pathlib import Path

app = FastAPI()

# Создаем директорию для загруженных файлов, если её нет
UPLOAD_FOLDER = 'uploads'
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

# Настраиваем шаблоны
templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "message": None})

@app.post("/upload")
async def upload_file(request: Request, file: UploadFile = File(...)):
    try:
        # Проверяем расширение файла
        if not file.filename.endswith(('.txt', '.log')):
            return templates.TemplateResponse(
                "index.html", 
                {"request": request, "message": "Ошибка: поддерживаются только файлы .txt и .log"}
            )
        
        # Сохраняем файл
        file_path = os.path.join(UPLOAD_FOLDER, file.filename)
        with open(file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        return templates.TemplateResponse(
            "index.html", 
            {"request": request, "message": "Файл успешно загружен"}
        )
    
    except Exception as e:
        return templates.TemplateResponse(
            "index.html", 
            {"request": request, "message": f"Ошибка при загрузке файла: {str(e)}"}
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
