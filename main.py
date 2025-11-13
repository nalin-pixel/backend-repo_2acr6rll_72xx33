import os
from io import BytesIO
from typing import List, Optional

from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse

from pypdf import PdfWriter, PdfReader
from PIL import Image
import zipfile

app = FastAPI(title="PDF Toolkit API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def read_root():
    return {"message": "PDF Toolkit Backend Running"}


@app.get("/api/hello")
def hello():
    return {"message": "Hello from the backend API!"}


@app.get("/test")
def test_database():
    """Test endpoint to check if database is available and accessible"""
    response = {
        "backend": "✅ Running",
        "database": "❌ Not Available",
        "database_url": None,
        "database_name": None,
        "connection_status": "Not Connected",
        "collections": []
    }

    try:
        from database import db

        if db is not None:
            response["database"] = "✅ Available"
            response["database_url"] = "✅ Configured"
            response["database_name"] = db.name if hasattr(db, 'name') else "✅ Connected"
            response["connection_status"] = "Connected"

            try:
                collections = db.list_collection_names()
                response["collections"] = collections[:10]
                response["database"] = "✅ Connected & Working"
            except Exception as e:
                response["database"] = f"⚠️  Connected but Error: {str(e)[:50]}"
        else:
            response["database"] = "⚠️  Available but not initialized"

    except ImportError:
        response["database"] = "❌ Database module not found (run enable-database first)"
    except Exception as e:
        response["database"] = f"❌ Error: {str(e)[:50]}"

    import os as _os
    response["database_url"] = "✅ Set" if _os.getenv("DATABASE_URL") else "❌ Not Set"
    response["database_name"] = "✅ Set" if _os.getenv("DATABASE_NAME") else "❌ Not Set"

    return response


# ---------- Helpers ----------

def _bytesio_response(buf: BytesIO, filename: str, media_type: str = "application/pdf"):
    buf.seek(0)
    headers = {"Content-Disposition": f"attachment; filename={filename}"}
    return StreamingResponse(buf, media_type=media_type, headers=headers)


def _parse_pages(pages: Optional[str], total_pages: int) -> List[int]:
    # pages string like "1-3,5,8-9" -> 0-based indices
    if not pages:
        return list(range(total_pages))
    selected = set()
    for part in pages.split(','):
        part = part.strip()
        if not part:
            continue
        if '-' in part:
            a, b = part.split('-', 1)
            start = max(1, int(a))
            end = min(total_pages, int(b))
            selected.update(range(start - 1, end))
        else:
            idx = int(part)
            if 1 <= idx <= total_pages:
                selected.add(idx - 1)
    ordered = sorted(selected)
    if not ordered:
        raise HTTPException(status_code=400, detail="No valid pages selected")
    return ordered


# ---------- PDF Operations ----------

@app.post("/api/pdf/merge")
async def merge_pdfs(files: List[UploadFile] = File(...)):
    if not files or len(files) < 2:
        raise HTTPException(status_code=400, detail="Upload at least two PDF files to merge")

    writer = PdfWriter()
    try:
        for f in files:
            if not f.filename.lower().endswith('.pdf'):
                raise HTTPException(status_code=400, detail=f"{f.filename} is not a PDF")
            reader = PdfReader(BytesIO(await f.read()))
            for page in reader.pages:
                writer.add_page(page)
        out = BytesIO()
        writer.write(out)
        writer.close()
        return _bytesio_response(out, "merged.pdf")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/pdf/split")
async def split_pdf(file: UploadFile = File(...), pages: Optional[str] = Form(None)):
    if not file.filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="File must be a PDF")
    data = await file.read()
    reader = PdfReader(BytesIO(data))
    indices = _parse_pages(pages, len(reader.pages))

    writer = PdfWriter()
    for i in indices:
        writer.add_page(reader.pages[i])
    out = BytesIO()
    writer.write(out)
    writer.close()
    return _bytesio_response(out, "split.pdf")


@app.post("/api/pdf/rotate")
async def rotate_pdf(
    file: UploadFile = File(...),
    angle: int = Form(90),
    pages: Optional[str] = Form(None),
):
    if angle not in (90, 180, 270):
        raise HTTPException(status_code=400, detail="Angle must be 90, 180, or 270")
    if not file.filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="File must be a PDF")

    reader = PdfReader(BytesIO(await file.read()))
    total = len(reader.pages)
    indices = set(_parse_pages(pages, total)) if pages else set(range(total))

    writer = PdfWriter()
    for i, page in enumerate(reader.pages):
        if i in indices:
            page.rotate(angle)
        writer.add_page(page)
    out = BytesIO()
    writer.write(out)
    writer.close()
    return _bytesio_response(out, "rotated.pdf")


@app.post("/api/pdf/compress")
async def compress_pdf(file: UploadFile = File(...)):
    # Note: True visual compression requires rasterization; here we rebuild the PDF which can reduce size for some files.
    if not file.filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="File must be a PDF")
    reader = PdfReader(BytesIO(await file.read()))
    writer = PdfWriter()
    # Enable object streams can help reduce size
    for page in reader.pages:
        writer.add_page(page)
    out = BytesIO()
    writer.write(out)
    writer.close()
    return _bytesio_response(out, "compressed.pdf")


@app.post("/api/pdf/images-to-pdf")
async def images_to_pdf(images: List[UploadFile] = File(...), page_size: Optional[str] = Form("auto")):
    if not images:
        raise HTTPException(status_code=400, detail="Upload at least one image")
    pil_images = []
    try:
        for img in images:
            content = await img.read()
            im = Image.open(BytesIO(content)).convert("RGB")
            pil_images.append(im)
    except Exception:
        for im in pil_images:
            try:
                im.close()
            except Exception:
                pass
        raise HTTPException(status_code=400, detail="One or more files are not valid images")

    pdf_bytes = BytesIO()
    first, rest = pil_images[0], pil_images[1:]
    first.save(pdf_bytes, format="PDF", save_all=True, append_images=rest)
    for im in pil_images:
        try:
            im.close()
        except Exception:
            pass
    return _bytesio_response(pdf_bytes, "images.pdf")


@app.post("/api/pdf/extract-images")
async def extract_images(file: UploadFile = File(...)):
    if not file.filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="File must be a PDF")

    reader = PdfReader(BytesIO(await file.read()))
    zip_buf = BytesIO()
    with zipfile.ZipFile(zip_buf, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
        img_count = 0
        for page_idx, page in enumerate(reader.pages, start=1):
            try:
                for image_file_object in page.images:
                    img_count += 1
                    ext = image_file_object.image.format.lower() if getattr(image_file_object, 'image', None) else 'bin'
                    name = f"page{page_idx}_img{img_count}.{ext}"
                    zf.writestr(name, image_file_object.data)
            except Exception:
                # If extraction fails for a page, skip it
                continue
    if zip_buf.getbuffer().nbytes == 0:
        return JSONResponse({"message": "No extractable images found"})
    return _bytesio_response(zip_buf, "images.zip", media_type="application/zip")


# Run with Uvicorn when executed directly
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
