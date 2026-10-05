import json
import os
import struct
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import cv2
import numpy as np
import onnxruntime as ort
import redis
import zmq

YOLO_INPUT_SIZE = 640
YOLO_CONF_THRESHOLD = 0.35
YOLO_NMS_IOU = 0.45
YOLO_CLASSES = ("knife", "firearm", "rod", "struggle", "assault")
YOLO_CLASS_WEIGHTS = {
    "knife": 0.85,
    "firearm": 1.0,
    "rod": 0.7,
    "struggle": 0.9,
    "assault": 1.0,
}

AUDIO_SAMPLE_RATE = 16000
AUDIO_WINDOW_SAMPLES = AUDIO_SAMPLE_RATE * 2
WAVLM_CLASSES = ("background", "gunshot", "glass_break", "scream", "pain_cry", "blunt_impact")
AUDIO_LABEL_THRESHOLD = 0.30

IPC_ENDPOINT = os.environ.get("ALIOTH_IPC_ENDPOINT", "ipc:///tmp/alioth_frames")
REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")
SCORE_CHANNEL = "alioth:svi"
YOLO_MODEL_PATH = os.environ.get("E1_YOLO_PATH", "models/e1_m3_yolov8n_int8.onnx")
WAVLM_MODEL_PATH = os.environ.get("E1_WAVLM_PATH", "models/e1_m2_wavlm_base_int8.onnx")
PUBLISH_INTERVAL_S = 0.25
POLL_TIMEOUT_MS = 100


@dataclass
class Detection:
    label: str
    confidence: float
    box: Tuple[int, int, int, int]


@dataclass
class Models:
    yolo: ort.InferenceSession
    yolo_input: str
    wavlm: ort.InferenceSession
    wavlm_input: str


def select_providers() -> List[str]:
    available = ort.get_available_providers()
    preferred = [
        "TensorrtExecutionProvider",
        "CUDAExecutionProvider",
        "CPUExecutionProvider",
    ]
    return [name for name in preferred if name in available]


def load_session(path: str) -> ort.InferenceSession:
    options = ort.SessionOptions()
    options.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
    options.intra_op_num_threads = 2
    return ort.InferenceSession(path, sess_options=options, providers=select_providers())


def load_models() -> Models:
    yolo = load_session(YOLO_MODEL_PATH)
    wavlm = load_session(WAVLM_MODEL_PATH)
    return Models(
        yolo=yolo,
        yolo_input=yolo.get_inputs()[0].name,
        wavlm=wavlm,
        wavlm_input=wavlm.get_inputs()[0].name,
    )


def letterbox(image: np.ndarray, size: int) -> Tuple[np.ndarray, float, Tuple[int, int]]:
    height, width = image.shape[:2]
    ratio = min(size / height, size / width)
    new_w, new_h = int(round(width * ratio)), int(round(height * ratio))
    resized = cv2.resize(image, (new_w, new_h), interpolation=cv2.INTER_LINEAR)
    pad_w, pad_h = (size - new_w) / 2, (size - new_h) / 2
    top, bottom = int(round(pad_h - 0.1)), int(round(pad_h + 0.1))
    left, right = int(round(pad_w - 0.1)), int(round(pad_w + 0.1))
    padded = cv2.copyMakeBorder(
        resized, top, bottom, left, right, cv2.BORDER_CONSTANT, value=(114, 114, 114)
    )
    return padded, ratio, (left, top)


def preprocess_frame(image: np.ndarray) -> Tuple[np.ndarray, float, Tuple[int, int]]:
    padded, ratio, pad = letterbox(image, YOLO_INPUT_SIZE)
    rgb = cv2.cvtColor(padded, cv2.COLOR_BGR2RGB)
    tensor = rgb.astype(np.float32) / 255.0
    tensor = np.transpose(tensor, (2, 0, 1))[np.newaxis, ...]
    return np.ascontiguousarray(tensor), ratio, pad


def to_image_boxes(boxes: np.ndarray, ratio: float, pad: Tuple[int, int]) -> np.ndarray:
    x = (boxes[:, 0] - boxes[:, 2] / 2 - pad[0]) / ratio
    y = (boxes[:, 1] - boxes[:, 3] / 2 - pad[1]) / ratio
    w = boxes[:, 2] / ratio
    h = boxes[:, 3] / ratio
    return np.stack([x, y, w, h], axis=1)


def decode_detections(raw: np.ndarray, ratio: float, pad: Tuple[int, int]) -> List[Detection]:
    predictions = np.squeeze(raw, axis=0).T
    scores = predictions[:, 4:]
    confidences = scores.max(axis=1)
    keep = confidences >= YOLO_CONF_THRESHOLD
    if not keep.any():
        return []
    class_ids = scores.argmax(axis=1)[keep]
    confidences = confidences[keep]
    boxes = to_image_boxes(predictions[keep, :4], ratio, pad)
    indices = cv2.dnn.NMSBoxes(
        boxes.tolist(), confidences.tolist(), YOLO_CONF_THRESHOLD, YOLO_NMS_IOU
    )
    return [
        Detection(
            YOLO_CLASSES[int(class_ids[i])],
            float(confidences[i]),
            tuple(int(v) for v in boxes[i]),
        )
        for i in np.array(indices).flatten()
    ]


def detect_hazards(models: Models, image: np.ndarray) -> List[Detection]:
    tensor, ratio, pad = preprocess_frame(image)
    raw = models.yolo.run(None, {models.yolo_input: tensor})[0]
    return decode_detections(raw, ratio, pad)


def visual_threat_score(detections: List[Detection]) -> float:
    if not detections:
        return 0.0
    return max(d.confidence * YOLO_CLASS_WEIGHTS.get(d.label, 0.5) for d in detections)


def decode_pcm16(payload: bytes) -> np.ndarray:
    return np.frombuffer(payload, dtype=np.int16).astype(np.float32) / 32768.0


def fit_window(wave: np.ndarray) -> np.ndarray:
    if wave.shape[0] >= AUDIO_WINDOW_SAMPLES:
        return wave[-AUDIO_WINDOW_SAMPLES:]
    return np.pad(wave, (AUDIO_WINDOW_SAMPLES - wave.shape[0], 0))


def normalize_wave(wave: np.ndarray) -> np.ndarray:
    return (wave - wave.mean()) / np.sqrt(wave.var() + 1e-7)


def softmax(logits: np.ndarray) -> np.ndarray:
    shifted = logits - logits.max()
    exp = np.exp(shifted)
    return exp / exp.sum()


def classify_impulse_audio(models: Models, payload: bytes) -> np.ndarray:
    wave = normalize_wave(fit_window(decode_pcm16(payload)))
    tensor = wave[np.newaxis, :].astype(np.float32)
    logits = models.wavlm.run(None, {models.wavlm_input: tensor})[0]
    return softmax(np.squeeze(logits, axis=0))


def audio_threat_score(probabilities: np.ndarray) -> float:
    return float(1.0 - probabilities[0])


def audio_labels(probabilities: np.ndarray) -> List[str]:
    return [
        WAVLM_CLASSES[i]
        for i in range(1, len(WAVLM_CLASSES))
        if probabilities[i] >= AUDIO_LABEL_THRESHOLD
    ]


def decode_image(payload: bytes) -> Optional[np.ndarray]:
    return cv2.imdecode(np.frombuffer(payload, dtype=np.uint8), cv2.IMREAD_COLOR)


def score_video(models: Models, payload: bytes) -> Optional[Tuple[float, List[str]]]:
    image = decode_image(payload)
    if image is None:
        return None
    detections = detect_hazards(models, image)
    return visual_threat_score(detections), sorted({d.label for d in detections})


def score_audio(models: Models, payload: bytes) -> Tuple[float, List[str]]:
    probabilities = classify_impulse_audio(models, payload)
    return audio_threat_score(probabilities), audio_labels(probabilities)


def score_frame(models: Models, topic: str, payload: bytes) -> Optional[Tuple[float, List[str]]]:
    if topic == "video":
        return score_video(models, payload)
    if topic == "audio":
        return score_audio(models, payload)
    return None


def parse_meta(meta: bytes) -> Tuple[int, int]:
    session_id, pts_us = struct.unpack("<QQ", meta)
    return session_id, pts_us


def build_event(session_id: int, topic: str, score: float, labels: List[str]) -> str:
    return json.dumps(
        {
            "session_id": str(session_id),
            "engine": "E1",
            "modality": topic,
            "svi": int(round(score * 100)),
            "labels": labels,
            "ts": time.time(),
        }
    )


def publish_event(client: redis.Redis, event: str) -> None:
    client.publish(SCORE_CHANNEL, event)


def should_publish(last_sent: Dict[Tuple[int, str], float], session_id: int, topic: str) -> bool:
    now = time.monotonic()
    key = (session_id, topic)
    if now - last_sent.get(key, 0.0) < PUBLISH_INTERVAL_S:
        return False
    last_sent[key] = now
    return True


def open_subscriber() -> Tuple[zmq.Context, zmq.Socket]:
    context = zmq.Context.instance()
    socket = context.socket(zmq.SUB)
    socket.setsockopt(zmq.RCVHWM, 256)
    socket.connect(IPC_ENDPOINT)
    for topic in (b"audio", b"video"):
        socket.setsockopt(zmq.SUBSCRIBE, topic)
    return context, socket


def handle_message(
    models: Models,
    client: redis.Redis,
    last_sent: Dict[Tuple[int, str], float],
    parts: List[bytes],
) -> None:
    if len(parts) != 3:
        return
    topic = parts[0].decode()
    session_id, _ = parse_meta(parts[1])
    result = score_frame(models, topic, parts[2])
    if result is None or not should_publish(last_sent, session_id, topic):
        return
    score, labels = result
    publish_event(client, build_event(session_id, topic, score, labels))


def run_worker() -> None:
    models = load_models()
    client = redis.Redis.from_url(REDIS_URL)
    _, socket = open_subscriber()
    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    last_sent: Dict[Tuple[int, str], float] = {}
    while True:
        if dict(poller.poll(POLL_TIMEOUT_MS)).get(socket) == zmq.POLLIN:
            handle_message(models, client, last_sent, socket.recv_multipart())


if __name__ == "__main__":
    run_worker()
