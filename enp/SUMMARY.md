# ENP (Enrichment Pipeline) - Summary

## Overview
The ENP module is responsible for enriching and classifying content from multiple sources (Gmail, YouTube, Chrome) into system groups using intelligent classification and embedding-based similarity matching.

## Core Components

### 1. **enrichment_pipeline.py**
Main orchestration module that:
- Connects to PostgreSQL database
- Fetches raw items from ingestion sources
- Processes items through the enrichment pipeline
- Cleans text, generates embeddings, and classifies into system groups
- Stores enriched data back to the database

### 2. **system_group_classifier.py**
Advanced classification engine with 4-stage cascade:
- **Stage 1**: Structural Signals (Gmail labels, YouTube categories, domain patterns)
- **Stage 2**: Domain Lookup (200+ domain-to-category mappings)
- **Stage 3**: Nearest Centroid (embedding similarity with configurable thresholds)
- **Stage 4**: LLM Fallback (stub for future LLM integration)

Returns confidence scores for classification reliability.

### 3. **text_cleaner.py**
Text preprocessing utilities:
- `clean_gmail_text()` - Removes HTML, tracking pixels, email signatures, quoted replies
- `clean_chrome_text()` - Extracts main article content (Readability-like approach)
- `clean_youtube_text()` - Combines title, description, and transcript snippet

### 4. **embedding_generator.py**
Generates vector embeddings from cleaned text for similarity-based classification.

### 5. **requirements_enrichment.txt**
Python dependencies for the enrichment pipeline module.

## System Groups
Content is classified into 5 categories:
- **Work** - Professional content
- **Learning** - Educational materials
- **Entertainment** - Media and leisure
- **Health** - Wellness and fitness
- **Miscellaneous** - Other content

## Data Flow
```
Raw Items (DB) 
  ↓
Text Cleaning 
  ↓
Embedding Generation 
  ↓
System Group Classification (4-Stage)
  ↓
Enriched Items (DB)
```

## Production Ready
✅ All core modules complete and functional
✅ Multi-stage classification with fallback handling
✅ Database integration
✅ Handles Gmail, YouTube, and Chrome content types
