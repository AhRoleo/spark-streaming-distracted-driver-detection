import io
import os
import numpy as np
import onnxruntime as ort
from PIL import Image
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import FileResponse, JSONResponse

app = FastAPI(title="ML Model Service", description="Microservice de prédiction distracted driver")

# Chemin du modèle ONNX (résolu depuis la racine du projet)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODEL_PATH   = os.path.join(PROJECT_ROOT, "models", "best_distracted_driver_cnn.onnx")

IMG_WIDTH  = 64
IMG_HEIGHT = 64

CLASSES = {
    0: "c0: Conduite normale",
    1: "c1: SMS au volant (Droit)",
    2: "c2: Téléphone (Droit)",
    3: "c3: SMS au volant (Gauche)",
    4: "c4: Téléphone (Gauche)",
    5: "c5: Réglage Radio",
    6: "c6: En train de Boire",
    7: "c7: Se retourner derrière",
    8: "c8: Maquillage",
    9: "c9: Parler au passager",
}

# Chargement unique de la session ONNX Runtime au démarrage
_session: ort.InferenceSession = None

def get_session() -> ort.InferenceSession:
    global _session
    if _session is None:
        if not os.path.exists(MODEL_PATH):
            raise RuntimeError(f"Modèle introuvable : {MODEL_PATH}")
        _session = ort.InferenceSession(MODEL_PATH)
    return _session


@app.get("/health")
def health():
    model_ok = os.path.exists(MODEL_PATH)
    return {"status": "ok", "model_loaded": model_ok, "model_path": MODEL_PATH}


@app.get("/model")
def download_model():
    """Sert le fichier ONNX — le consumer Scala le télécharge ici au démarrage (READ arrow)."""
    if not os.path.exists(MODEL_PATH):
        raise HTTPException(status_code=404, detail="Modèle ONNX introuvable")
    return FileResponse(
        MODEL_PATH,
        media_type="application/octet-stream",
        filename="best_distracted_driver_cnn.onnx"
    )


@app.get("/model/info")
def model_info():
    """Métadonnées du modèle : classes, dimensions d'entrée."""
    session = get_session()
    input_info = session.get_inputs()[0]
    return {
        "input_name":  input_info.name,
        "input_shape": input_info.shape,
        "num_classes": len(CLASSES),
        "classes":     CLASSES,
        "img_width":   IMG_WIDTH,
        "img_height":  IMG_HEIGHT,
    }


@app.post("/predict")
async def predict(file: UploadFile = File(...)):
    """
    Reçoit une image (jpg/png), retourne la classe prédite.
    Utilisé optionnellement par des clients externes.
    """
    contents = await file.read()
    image = Image.open(io.BytesIO(contents)).convert("RGB")
    image = image.resize((IMG_WIDTH, IMG_HEIGHT))

    pixels = np.array(image, dtype=np.float32) / 255.0          # (64, 64, 3)
    tensor = pixels.reshape(1, IMG_HEIGHT, IMG_WIDTH, 3)         # (1, 64, 64, 3) NHWC

    session    = get_session()
    input_name = session.get_inputs()[0].name
    outputs    = session.run(None, {input_name: tensor})

    probs          = outputs[0][0]                               # (10,)
    predicted_idx  = int(np.argmax(probs))
    confidence     = float(probs[predicted_idx])

    return JSONResponse({
        "prediction":  predicted_idx,
        "class_name":  CLASSES[predicted_idx],
        "confidence":  round(confidence, 4),
        "all_probs":   {str(i): round(float(p), 4) for i, p in enumerate(probs)},
    })
