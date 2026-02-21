-- Migration: Add updated_at and updated_by columns to companies table
-- Date: 2025-02-21
-- Description: Track when and which git commit updated each company record

ALTER TABLE companies 
ADD COLUMN updated_at TIMESTAMP NULL;

ALTER TABLE companies 
ADD COLUMN updated_by VARCHAR(50) NULL;

-- Add comments for documentation
COMMENT ON COLUMN companies.updated_at IS 'Timestamp of last update to this record';
COMMENT ON COLUMN companies.updated_by IS 'Git commit hash that made the update';
