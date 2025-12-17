"""
Presidio Deep Clean API
========================
Isolated, containerized PII detection service.
Only accessed when user explicitly clicks "Deep Clean" button.
No persistence, no file access, no telemetry.
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import logging

# Presidio imports
from presidio_analyzer import AnalyzerEngine
from presidio_analyzer.nlp_engine import NlpEngineProvider

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Financial Anonymizer - Deep Clean API",
    description="Isolated Presidio service for AI-powered PII detection",
    version="1.0.0"
)

# CORS - localhost only for security
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:4321",
        "http://localhost:3000",
        "http://127.0.0.1:4321",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=False,
    allow_methods=["POST", "GET"],
    allow_headers=["Content-Type"],
)

# ============================================================================
# PRESIDIO ENGINE INITIALIZATION (loaded once at container startup)
# ============================================================================

logger.info("Initializing Presidio engines...")

# NLP Configuration - using spaCy with English model
nlp_config = {
    "nlp_engine_name": "spacy",
    "models": [{"lang_code": "en", "model_name": "en_core_web_md"}]
}

try:
    provider = NlpEngineProvider(nlp_configuration=nlp_config)
    nlp_engine = provider.create_engine()
    analyzer = AnalyzerEngine(nlp_engine=nlp_engine)
    logger.info("Presidio engines loaded successfully")
except Exception as e:
    logger.error(f"Failed to initialize Presidio: {e}")
    raise

# ============================================================================
# DATA MODELS
# ============================================================================

class Transaction(BaseModel):
    date: str
    merchant: str
    description: str
    category: str
    amount: float
    type: str


class ScanRequest(BaseModel):
    transactions: List[Transaction]
    # Optional: specify which entity types to detect
    entities: Optional[List[str]] = None


class Candidate(BaseModel):
    text: str
    type: str
    confidence: float
    count: int
    locations: List[Dict[str, Any]] # e.g., [{"row": 0, "field": "merchant", "start": 0, "end": 5}]


class ScanResponse(BaseModel):
    candidates: List[Candidate]
    total_candidates: int


# ============================================================================
# ENDPOINTS
# ============================================================================

@app.get("/health")
async def health_check():
    """Health check endpoint - no data access"""
    return {
        "status": "healthy",
        "service": "presidio-deep-clean",
        "isolation": "docker-container",
        "persistence": "none"
    }


@app.post("/scan", response_model=ScanResponse)
async def scan_data(request: ScanRequest):
    """
    Run Presidio NER-based PII detection on transactions.
    Returns grouping of CANDIDATES instead of applying redaction.
    
    Security:
    - Only processes data sent in this specific request
    - No data is persisted or logged
    - Results are returned and memory is freed
    """
    
    # Default entities to detect (financial-focused)
    # ORGANIZATION is key for merchant classification (Tim Hortons, Netflix, etc.)
    entities_to_detect = request.entities or [
        "PERSON",
        "ORGANIZATION",  # Detects business names for smart categorization
        "PHONE_NUMBER", 
        "EMAIL_ADDRESS",
        "CREDIT_CARD",
        "US_SSN",
        "US_BANK_NUMBER",
        "IBAN_CODE",
        "US_PASSPORT",
        "US_DRIVER_LICENSE",
        "IP_ADDRESS",
        "LOCATION",
        "URL",
    ]
    
    # Store candidates: Key = "text|type" -> Candidate
    candidate_map: Dict[str, Candidate] = {}

    def add_finding(text: str, entity_type: str, score: float, row_idx: int, field: str, start: int, end: int):
        key = f"{text}|{entity_type}"
        if key not in candidate_map:
            candidate_map[key] = Candidate(
                text=text,
                type=entity_type,
                confidence=score,
                count=0,
                locations=[]
            )
        
        cand = candidate_map[key]
        cand.count += 1
        # Keep track of where it was found (useful for context, debugging)
        # Limit location storage to avoid massive payloads for common terms
        if len(cand.locations) < 50:
            cand.locations.append({
                "row": row_idx,
                "field": field,
                "start": start,
                "end": end
            })
        
        # Update confidence if we found a higher score
        if score > cand.confidence:
            cand.confidence = score

    
    for i, tx in enumerate(request.transactions):
        # Analyze description field
        try:
            desc_results = analyzer.analyze(
                text=tx.description,
                language="en",
                entities=entities_to_detect,
                score_threshold=0.5
            )
            for res in desc_results:
                text_slice = tx.description[res.start:res.end]
                add_finding(text_slice, res.entity_type, res.score, i, "description", res.start, res.end)
        except Exception as e:
            logger.warning(f"Error analyzing description at row {i}: {e}")
        
        # Analyze merchant field
        try:
            merchant_results = analyzer.analyze(
                text=tx.merchant,
                language="en",
                entities=entities_to_detect,
                score_threshold=0.5
            )
            for res in merchant_results:
                text_slice = tx.merchant[res.start:res.end]
                add_finding(text_slice, res.entity_type, res.score, i, "merchant", res.start, res.end)
        except Exception as e:
            logger.warning(f"Error analyzing merchant at row {i}: {e}")

    candidates_list = list(candidate_map.values())
    # Sort by confidence (desc) then count (desc)
    candidates_list.sort(key=lambda x: (x.confidence, x.count), reverse=True)
    
    return ScanResponse(
        candidates=candidates_list,
        total_candidates=len(candidates_list)
    )


# ============================================================================
# STARTUP/SHUTDOWN
# ============================================================================

@app.on_event("startup")
async def startup_event():
    logger.info("Deep Clean API started - isolated container mode")
    logger.info("No persistence, no file access, localhost only")


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Deep Clean API shutting down - all data cleared from memory")
