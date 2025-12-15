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
from typing import List, Optional, Dict
import logging

# Presidio imports
from presidio_analyzer import AnalyzerEngine
from presidio_analyzer.nlp_engine import NlpEngineProvider
from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities import OperatorConfig

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
    anonymizer = AnonymizerEngine()
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


class DeepCleanRequest(BaseModel):
    transactions: List[Transaction]
    # Optional: specify which entity types to detect
    entities: Optional[List[str]] = None


class DeepCleanResponse(BaseModel):
    transactions: List[Transaction]
    findings: Dict[str, int]  # Entity type -> count found
    total_found: int


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


@app.post("/deep-clean", response_model=DeepCleanResponse)
async def deep_clean(request: DeepCleanRequest):
    """
    Run Presidio NER-based PII detection on transactions.
    
    Security:
    - Only processes data sent in this specific request
    - No data is persisted or logged
    - Results are returned and memory is freed
    """
    
    # Default entities to detect (financial-focused)
    entities_to_detect = request.entities or [
        "PERSON",
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
    
    # Anonymization operators - how to replace each entity type
    operators = {
        "PERSON": OperatorConfig("replace", {"new_value": "[PERSON]"}),
        "PHONE_NUMBER": OperatorConfig("replace", {"new_value": "[PHONE]"}),
        "EMAIL_ADDRESS": OperatorConfig("replace", {"new_value": "[EMAIL]"}),
        "CREDIT_CARD": OperatorConfig("replace", {"new_value": "****"}),
        "US_SSN": OperatorConfig("replace", {"new_value": "[SSN]"}),
        "US_BANK_NUMBER": OperatorConfig("replace", {"new_value": "[ACCOUNT]"}),
        "IBAN_CODE": OperatorConfig("replace", {"new_value": "[IBAN]"}),
        "US_PASSPORT": OperatorConfig("replace", {"new_value": "[PASSPORT]"}),
        "US_DRIVER_LICENSE": OperatorConfig("replace", {"new_value": "[LICENSE]"}),
        "IP_ADDRESS": OperatorConfig("replace", {"new_value": "[IP]"}),
        "LOCATION": OperatorConfig("replace", {"new_value": "[LOCATION]"}),
        "URL": OperatorConfig("replace", {"new_value": "[URL]"}),
        "DEFAULT": OperatorConfig("replace", {"new_value": "[REDACTED]"}),
    }
    
    findings: Dict[str, int] = {}
    cleaned_transactions: List[Transaction] = []
    
    for tx in request.transactions:
        new_desc = tx.description
        new_merchant = tx.merchant
        
        # Analyze description field
        try:
            desc_results = analyzer.analyze(
                text=tx.description,
                language="en",
                entities=entities_to_detect,
                score_threshold=0.5  # Only high-confidence matches
            )
            
            if desc_results:
                anon_result = anonymizer.anonymize(
                    text=tx.description,
                    analyzer_results=desc_results,
                    operators=operators
                )
                new_desc = anon_result.text
                
                for result in desc_results:
                    findings[result.entity_type] = findings.get(result.entity_type, 0) + 1
        except Exception as e:
            logger.warning(f"Error analyzing description: {e}")
        
        # Analyze merchant field
        try:
            merchant_results = analyzer.analyze(
                text=tx.merchant,
                language="en",
                entities=entities_to_detect,
                score_threshold=0.5
            )
            
            if merchant_results:
                anon_result = anonymizer.anonymize(
                    text=tx.merchant,
                    analyzer_results=merchant_results,
                    operators=operators
                )
                new_merchant = anon_result.text
                
                for result in merchant_results:
                    findings[result.entity_type] = findings.get(result.entity_type, 0) + 1
        except Exception as e:
            logger.warning(f"Error analyzing merchant: {e}")
        
        cleaned_transactions.append(Transaction(
            date=tx.date,
            merchant=new_merchant,
            description=new_desc,
            category=tx.category,
            amount=tx.amount,
            type=tx.type
        ))
    
    total_found = sum(findings.values())
    
    return DeepCleanResponse(
        transactions=cleaned_transactions,
        findings=findings,
        total_found=total_found
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

