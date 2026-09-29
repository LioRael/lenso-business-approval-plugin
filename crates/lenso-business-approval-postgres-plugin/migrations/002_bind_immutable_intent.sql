ALTER TABLE business_approval_requests ADD COLUMN intent_digest TEXT CHECK (intent_digest ~ '^[a-f0-9]{64}$');
