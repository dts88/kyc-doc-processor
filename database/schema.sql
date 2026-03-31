-- KYC Document Processor Database Schema

PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

-- Counterparty (trading partner) information
CREATE TABLE IF NOT EXISTS counterparties (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    slug TEXT NOT NULL UNIQUE,
    aliases TEXT DEFAULT '[]',  -- JSON array of known name variants
    status TEXT NOT NULL DEFAULT 'in_progress'
        CHECK (status IN ('in_progress', 'pending_review', 'completed', 'delivered')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_counterparties_slug ON counterparties(slug);
CREATE INDEX IF NOT EXISTS idx_counterparties_status ON counterparties(status);

-- KYC document type definitions (pre-populated)
CREATE TABLE IF NOT EXISTS kyc_doc_types (
    id INTEGER PRIMARY KEY,
    code TEXT NOT NULL UNIQUE,
    name_en TEXT NOT NULL,
    name_zh TEXT NOT NULL,
    required INTEGER NOT NULL DEFAULT 1,  -- 1=required, 0=optional
    sort_order INTEGER NOT NULL,
    description TEXT
);

-- Submitted raw files
CREATE TABLE IF NOT EXISTS submitted_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    original_filename TEXT NOT NULL,
    file_path TEXT NOT NULL,
    file_size INTEGER NOT NULL,
    file_hash TEXT NOT NULL,  -- SHA-256
    mime_type TEXT,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'processing', 'classified', 'extraction_done',
                          'needs_review', 'error', 'duplicate', 'packaged')),
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_submitted_files_hash ON submitted_files(file_hash);
CREATE INDEX IF NOT EXISTS idx_submitted_files_status ON submitted_files(status);

-- Classification results from Claude
-- A single file can have multiple classifications (one per applicable doc_type)
CREATE TABLE IF NOT EXISTS document_classifications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id INTEGER NOT NULL REFERENCES submitted_files(id),
    doc_type_id INTEGER REFERENCES kyc_doc_types(id),
    counterparty_id INTEGER REFERENCES counterparties(id),
    detected_company_name TEXT,
    confidence REAL NOT NULL,
    is_primary INTEGER NOT NULL DEFAULT 1,  -- 1=primary type, 0=additional type
    model_used TEXT NOT NULL,
    input_tokens INTEGER DEFAULT 0,
    output_tokens INTEGER DEFAULT 0,
    raw_response TEXT,  -- full Claude response JSON
    classified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(file_id, doc_type_id)
);

CREATE INDEX IF NOT EXISTS idx_classifications_file ON document_classifications(file_id);
CREATE INDEX IF NOT EXISTS idx_classifications_counterparty ON document_classifications(counterparty_id);

-- Extraction results from Claude
-- A single file can have multiple extractions (one per applicable doc_type)
CREATE TABLE IF NOT EXISTS extraction_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id INTEGER NOT NULL REFERENCES submitted_files(id),
    classification_id INTEGER NOT NULL REFERENCES document_classifications(id),
    extracted_data TEXT NOT NULL,  -- JSON
    validation_passed INTEGER DEFAULT 0,
    validation_errors TEXT,  -- JSON array
    model_used TEXT NOT NULL,
    input_tokens INTEGER DEFAULT 0,
    output_tokens INTEGER DEFAULT 0,
    raw_response TEXT,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(file_id, classification_id)
);

CREATE INDEX IF NOT EXISTS idx_extraction_file ON extraction_results(file_id);

-- Per-counterparty document checklist
CREATE TABLE IF NOT EXISTS counterparty_checklist (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    counterparty_id INTEGER NOT NULL REFERENCES counterparties(id),
    doc_type_id INTEGER NOT NULL REFERENCES kyc_doc_types(id),
    file_id INTEGER REFERENCES submitted_files(id),
    status TEXT NOT NULL DEFAULT 'missing'
        CHECK (status IN ('missing', 'received', 'verified', 'rejected')),
    notes TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(counterparty_id, doc_type_id)
);

CREATE INDEX IF NOT EXISTS idx_checklist_counterparty ON counterparty_checklist(counterparty_id);

-- Completed packages ready for delivery
CREATE TABLE IF NOT EXISTS completed_packages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    counterparty_id INTEGER NOT NULL REFERENCES counterparties(id),
    package_path TEXT NOT NULL,
    file_count INTEGER NOT NULL,
    total_size INTEGER NOT NULL,
    delivered INTEGER DEFAULT 0,
    delivered_to TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    delivered_at TIMESTAMP
);

-- Processing audit log
CREATE TABLE IF NOT EXISTS processing_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id INTEGER REFERENCES submitted_files(id),
    counterparty_id INTEGER REFERENCES counterparties(id),
    stage TEXT NOT NULL,  -- intake, conversion, classification, extraction, packaging
    action TEXT NOT NULL,
    details TEXT,  -- JSON
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_log_file ON processing_log(file_id);
CREATE INDEX IF NOT EXISTS idx_log_stage ON processing_log(stage);

-- Weekly report records
CREATE TABLE IF NOT EXISTS weekly_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_date DATE NOT NULL,
    report_path TEXT NOT NULL,
    summary TEXT,  -- JSON summary stats
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Application settings (key-value store for SMTP etc.)
CREATE TABLE IF NOT EXISTS app_settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- VP email whitelist (VPs who can confirm onboarding applications)
CREATE TABLE IF NOT EXISTS vp_emails (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT NOT NULL UNIQUE,
    vp_name TEXT NOT NULL,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- VP signatures recorded from verified onboarding forms
CREATE TABLE IF NOT EXISTS vp_signatures (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    vp_name TEXT NOT NULL,
    source_file_id INTEGER REFERENCES submitted_files(id),
    source_description TEXT,
    verified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active INTEGER NOT NULL DEFAULT 1
);

-- Classification corrections for few-shot learning (independent of production data)
CREATE TABLE IF NOT EXISTS classification_corrections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    original_filename TEXT NOT NULL,
    content_excerpt TEXT,            -- first ~1000 chars of document content for content-based learning
    machine_doc_type TEXT,           -- what the machine classified it as (code or NULL for unknown)
    correct_doc_type TEXT NOT NULL,  -- what the human corrected it to (code)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- KYC/Risk team member emails
CREATE TABLE IF NOT EXISTS kyc_team (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    role TEXT DEFAULT 'kyc_reviewer',
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
