**# ALIOTH: Edge-Native Multimodal Triage Layer for NHAA 14566**



[![System Architecture](https://img.shields.io/badge/Architecture-Distributed%20Edge%20Cluster-blue.svg)](#distributed-edge-cluster-topology)
[![Runtime](https://img.shields.io/badge/Optimization-ONNX%20INT8-green.svg)](#tri-engine-micro-module-matrix)
[![IPC Protocol](https://img.shields.io/badge/IPC-C%2B%2B%20%7C%20ZeroMQ%20%7C%20Zero--Copy-orange.svg)](#core-system-architecture--ipc-protocol)
[![Compliance](https://img.shields.io/badge/Compliance-BNS%20Chain--of--Custody-purple.svg)](#compliance--legal-framework)
[![License](https://img.shields.io/badge/License-Proprietary%20%2F%20SIH%202026-red.svg)](#)

**ALIOTH** is an air-gapped, zero-cloud-dependency multimodal AI copilot and priority triage system purpose-built for the **National Health Authority Helpline (14566)**. Operating entirely on local area network (LAN) edge hardware, ALIOTH ingests real-time WebRTC streams (voice, text, and video), executes low-latency multi-engine threat/distress scoring, and dynamically routes cases to human operators without cloud OpEx or data leakage risks.

---

## Technical Highlights

* **Zero-Copy C++ Modality Router:** Bypasses Python Global Interpreter Lock (GIL) bottlenecks by splitting WebRTC media tracks at the memory-pointer level into concurrent human P2P and AI analysis loops.
* **Quantized Micro-Module Ensemble:** Executes 9 specialized micro-classifiers concurrently under 3.2 GB VRAM on an NVIDIA AGX Orin board using ONNX Runtime (INT8).
* **Distributed LAN Edge Cluster:** Scales across on-premise hardware nodes connected over LAN, delivering high availability, automatic load balancing, and air-gapped security.
* **Anti-Starvation Queue Logic:** Enforces an N:1:1:1 token-bucket allocation and absolute priority ceilings to eliminate low/moderate queue starvation mathematically.
* **BNS-Compliant Chain-of-Custody:** Implements end-to-end mTLS and AES-256 encryption with tamper-evident audit logs aligned with Bharatiya Nyaya Sanhita (BNS) standards.

---

## Distributed Edge Cluster Topology

ALIOTH shifts emergency response processing away from vulnerable cloud SaaS APIs to a resilient, local edge node network.

```text
                  [ 14566 IVRS / WebRTC / Secure Chat Intake ]
                                       │
                                       ▼
                  [ C++ Modality Router (SFU + ZeroMQ) ]
                   │                                  │
      (Track A: Zero-Copy)                   (Track B: Zero-Copy)
                   │                                  │
                   ▼                                  ▼
      [ Operator Dashboard UI ]          [ Distributed Edge Cluster ]
      (Human-in-the-Loop Control)         (LAN-Networked AGX Orins)
                   │                                  │
                   │      ┌───────────────────────────┴───────────────────────────┐
                   │      │ Engine 1: Threat to Life (IndicBERT / WavLM / YOLOv8) │
                   │      │ Engine 2: Distress (WavLM DSP / FER / ST-GCN)         │
                   │      │ Engine 3: Domain Decision (Sarvam-1 RAG / AST)       │
                   │      └───────────────────────────┬───────────────────────────┘
                   │                                  │
                   └───────────────────┬──────────────┘
                                       ▼
                     [ Redis Priority Queue (O(log N)) ]
                                       │
                                       ▼
                  [ Deterministic Dispatch: ERSS-112 CAD ]



```


---



## Tri-Engine Micro-Module Matrix



ALIOTH decouples threat evaluation into 9 lightweight, single-purpose classifiers running in parallel.



**Engine 1**: Threat to Life (E1)

E1_M1 (Text Threat Classifier):IndicBERT v2 fine-tuned on custom dictionaries of Indian violent crime terminology, emergency slang, and threat syntax.

E1_M2 (Impulse Audio Detector):WavLM (Base) fine-tuned on impulse noise datasets (gunshots, glass shattering, screams, physical pain, blunt force impacts).

E1_M3 (Visual Hazard Detector):YOLOv8-Nano trained on weapon detection (knives, firearms, rods), struggle poses, and physical assault bounding boxes.



**Engine 2**: Emotional Distress (E2)

E2_M1 (Psychological Syntax Analyzer):IndicBERT v2 fine-tuned on distress lexicons, suicide helpline transcripts, and panic-induced syntax fragmentation.

E2_M2 (Acoustic DSP Extractor):WavLM coupled with DSP algorithms to extract fundamental frequency ($F\_0$) anomalies, pitch jitter, shimmer, hyperventilation signatures, and sobbing acoustic profiles.

E2_M3 (Affect \& Behavioral Tracking):MobileNetV3-FER (fine-tuned on AffectNet) combined with ST-GCN on MediaPipe keypoint graphs (detecting rocking, trembling, face-hiding) and DSP Fourier chest keypoint frequency analysis (Hyperventilation BPM \& Motor Freeze Index).



**Engine 3**: Domain & Legal Decision (E3)

E3_M1 (Legal \& SOP RAG):Fine-tuned Sarvam-1 (2B) integrated with FAISS offline vector storage, querying Bharatiya Nyaya Sanhita (BNS) codes and disaster response SOPs.

E3_M2 (Environmental Acoustics): Audio Spectrogram Transformer (AST) fine-tuned on Indian ambient acoustics (sirens, crowd riots, structural collapse).

E3_M3 (Visual Hazard \& Triage Synthesis): MobileNetV3 combined with a deterministic Python Triage Matrix, synthesizing outputs from E1 and E2 to dictate Police, Fire, Medical, or Counseling routing.



---



## Core System Architecture & IPC Protocol

C++ Modality Router & Zero-Copy Ingestion
To maintain sub-200ms latency without dropping WebRTC packets, the core media pipeline is written in native C++ using FFmpeg bindings and ZeroMQ:

```cpp

// High-Level Zero-Copy Media Buffer Splitting Architecture
void route_webrtc_stream(rtc_frame_t* frame) {
    uint8_t* shared_buffer = frame->data_pointer;
    
    // Track A: Zero-copy direct P2P socket pass-through to Human Operator UI
    dispatch_to_operator_p2p(shared_buffer, frame->size);
    
    // Track B: Zero-copy shared pointer pass-through to ZeroMQ IPC for AI Engine loop
    dispatch_to_zeromq_ipc(shared_buffer, frame->size);
}

```

---



## Advanced Algorithmic Innovations



**1. Anti-Starvation Queue Engine:** 

Standard priority queues cause low-severity calls to sit perpetually during high-volume surges. ALIOTH uses an N:1:1:1 token routing model with absolute priority ceilings: 



Priority Aging: Moderate/Low-tier cases receive incremental score boosts over time.



Priority Ceiling: Math-locked boundaries prevent non-critical cases from crossing into true life-threat bands, preserving emergency bandwidth while guaranteeing worst-case wait time bounds.



**2. Silent-Safe Mode**

For callers in active danger who cannot speak:



Acoustic Signatures: Detects ambient struggle, forced silence, or non-verbal distress cues.



Keypad Override: Interactively accepts DTMF/keypad inputs or physical mic double-taps to override the triage score directly to CRITICAL.



**3. Geo-Linguistic Proximity Routing**



Spatial Matching: Uses Redis GEOSEARCH to find operator regional proximity.



Dialect Fallback: If an exact dialect match is unavailable, the router automatically calculates dialect proximity trees (e.g., Maithili-Awadhi) and routes to the geographically closest regional center to maximize dialect overlap probability.





**4. Empath AI Auto-Hold with WebSocket Kill-Switch**



While callers wait for operator assignment, a localized fine-tuned Sarvam-1 (2B) model provides supportive, trauma-informed conversational holding.



Hardware Intercept: The millisecond a human operator connects, a native WebSocket kill-switch instantly mutes and drops the holding model session, preventing conversational overlap.





---



## Tech Stack \& Dependencies


```text

+--------------------+-----------------------------------+-----------------------------------------------------------------------+

| Component          | Technology                        | Purpose                                                               |

+--------------------+-----------------------------------+-----------------------------------------------------------------------+

| Ingestion \& IPC    | C++17, WebRTC SFU, FFmpeg, ZeroMQ | Native media stream splitting and high-throughput IPC                 |

| Model Optimization | ONNX Runtime (INT8), TensorRT     | Low-precision, concurrent micro-module execution                      |

| Edge Hardware      | NVIDIA AGX Orin Cluster (LAN)     | Air-gapped, on-premise edge compute nodes                             |

| State \& In-Memory  | Redis (ZADD, GEOSEARCH)           | O(log N) priority scoring \& spatial operator mapping                  |

| Persistent Data    | PostgreSQL                        | ACID-compliant, BNS-aligned forensic case logs                        |

| API \& Realtime     | Node.js, Socket.io                | Zero-lag WebSocket JSON state distribution                            |

| Operator Interface | React.js, Tailwind CSS            | Live operator dashboard showing real-time SVI score and triage advice |

+--------------------+-----------------------------------+-----------------------------------------------------------------------+

```

---



## Repository Structure

```text
.
├── cxx_modality_router/     # Native C++ SFU & ZeroMQ zero-copy media splitter
│   ├── src/
│   └── include/
├── ai_engines/              # Quantized micro-module classifiers (ONNX INT8)
│   ├── threat_engine_e1/    # IndicBERT v2, WavLM, YOLOv8-Nano
│   ├── distress_engine_e2/  # Acoustic DSP, FER, ST-GCN keypoint graphs
│   └── domain_engine_e3/    # Sarvam-1 RAG, AST, Triage Matrix
├── priority_queue/          # Redis ZADD aging algorithms & token routers
├── database/                # PostgreSQL schema & BNS forensic audit triggers
├── dashboard_ui/            # React + Socket.io operator dashboard
└── docs/                    # Architecture diagrams & SIH 2026 specs


```
---



## Deployment & Edge Cluster Setup



Prerequisites

NVIDIA Jetpack 5.1+ on AGX Orin nodes

CUDA 11.8 / TensorRT 8.5

Redis 7.0+ with Spatial Indexing

Node.js v18+ & C++17 Toolchain




\---



## Node Initialization (Single-Line Launch)


```bash
# Build C++ Modality Router
cd cxx_modality_router && mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release .. && make -j$(nproc)

# Launch Edge Cluster Worker Node
./alioth_router --config=../config/edge_node_lan.json

```



---



## Compliance \& Legal Framework



ALIOTH is engineered to adhere strictly to the Bharatiya Nyaya Sanhita (BNS) digital evidence chain-of-custody requirements. All telemetry, transcribed context, and operator override actions are signed cryptographically with SHA-256 hashes and appended to immutable PostgreSQL ledgers.



**Developed for Smart India Hackathon (SIH) 2026 | Team Sententia**





