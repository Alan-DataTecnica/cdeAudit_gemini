-- DDL for the main CDE catalog table. Must be created first.
CREATE TABLE IF NOT EXISTS public.cde_catalog (
    "ID" TEXT PRIMARY KEY,
    title TEXT,
    variable_name TEXT,
    preferred_question_text TEXT,
    collections TEXT[],
    unit_of_measure TEXT,
    value_format TEXT,
    permissible_values TEXT[],
    min_value NUMERIC,
    max_value NUMERIC,
    version TEXT
);

-- DDL for the synonyms table, linked to the main catalog.
CREATE TABLE IF NOT EXISTS public.cde_synonyms (
    synonym_id SERIAL PRIMARY KEY,
    cde_id TEXT NOT NULL REFERENCES public.cde_catalog("ID") ON DELETE CASCADE,
    synonym_text TEXT NOT NULL,
    synonym_type TEXT NOT NULL,
    UNIQUE (cde_id, synonym_text, synonym_type)
);
CREATE INDEX IF NOT EXISTS idx_cde_synonyms_cde_id ON public.cde_synonyms (cde_id);
CREATE INDEX IF NOT EXISTS idx_cde_synonyms_text ON public.cde_synonyms (synonym_text);

-- DDL for the variants table, linked to the main catalog.
CREATE TABLE IF NOT EXISTS public.cde_variants (
    id SERIAL PRIMARY KEY,
    variant_id TEXT NOT NULL UNIQUE,
    canonical_cde_id TEXT NOT NULL REFERENCES public.cde_catalog("ID") ON DELETE CASCADE,
    variant_description TEXT,
    data_type TEXT,
    permissible_values TEXT[],
    unit_of_measure TEXT,
    value_format TEXT,
    min_value NUMERIC,
    max_value NUMERIC
);
CREATE INDEX IF NOT EXISTS idx_cde_variants_canonical_id ON public.cde_variants (canonical_cde_id);

-- DDL for the harmonization rules table.
CREATE TABLE IF NOT EXISTS public.harmonization_rules (
    rule_id SERIAL PRIMARY KEY,
    source_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    transformation_logic TEXT NOT NULL,
    context_notes TEXT
);
CREATE INDEX IF NOT EXISTS idx_harmonization_rules_source_id ON public.harmonization_rules (source_id);

-- DDL for the references table (for URLs), linked to the main catalog.
CREATE TABLE IF NOT EXISTS public.cde_references (
    reference_id SERIAL PRIMARY KEY,
    cde_id TEXT NOT NULL REFERENCES public.cde_catalog("ID") ON DELETE CASCADE,
    source_url TEXT NOT NULL,
    source_description TEXT
);
CREATE INDEX IF NOT EXISTS idx_cde_references_cde_id ON public.cde_references (cde_id);

-- DDL for the external codes table (ICD, SNOMED, etc.), linked to the main catalog.
CREATE TABLE IF NOT EXISTS public.cde_external_codes (
    id SERIAL PRIMARY KEY,
    cde_id TEXT NOT NULL REFERENCES public.cde_catalog("ID") ON DELETE CASCADE,
    code_system TEXT NOT NULL,
    code_value TEXT NOT NULL,
    code_description TEXT,
    UNIQUE (cde_id, code_system, code_value)
);
CREATE INDEX IF NOT EXISTS idx_cde_external_codes_cde_id ON public.cde_external_codes (cde_id);

-- DDL for the live review decisions table.
CREATE TABLE IF NOT EXISTS public.review_decisions (
    cde_id TEXT NOT NULL,
    field TEXT NOT NULL,
    status TEXT NOT NULL,
    suggestion TEXT,
    reviewer_id TEXT,
    last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (cde_id, field)
);

-- DDL for the permanent audit log table.
CREATE TABLE IF NOT EXISTS public.audit_log (
    log_id BIGSERIAL PRIMARY KEY,
    log_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    reviewer_id TEXT,
    cde_id TEXT NOT NULL,
    field TEXT NOT NULL,
    action TEXT NOT NULL,
    previous_value TEXT,
    new_value TEXT,
    details JSONB,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS public.cde_notes (
    note_id SERIAL PRIMARY KEY,
    cde_id TEXT NOT NULL REFERENCES public.cde_catalog("ID") ON DELETE CASCADE,
    reviewer_id TEXT,
    note_text TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- An index to quickly retrieve all notes for a CDE
CREATE INDEX IF NOT EXISTS idx_cde_notes_cde_id ON public.cde_notes (cde_id);