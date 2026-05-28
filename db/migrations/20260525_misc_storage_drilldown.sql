ALTER TABLE misc_storage_items
    ADD COLUMN IF NOT EXISTS parent_item_id UUID REFERENCES misc_storage_items(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_misc_storage_items_parent_item_id
    ON misc_storage_items(parent_item_id);
