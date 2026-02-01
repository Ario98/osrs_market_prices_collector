# OSRS Market Database Migrations

This directory contains database migration scripts for schema updates.

## Migration Naming Convention

Format: `migration_YYYYMMDD_NNN_description.sql`

Examples:
- `migration_20240115_001_add_indexes.sql`
- `migration_20240120_002_new_calculated_fields.sql`

## Applying Migrations

### Automatic Migrations

Migrations in this directory are automatically applied when the TimescaleDB container starts.
Files are executed in alphabetical order.

### Manual Migrations

For production systems or specific timing:

```bash
# Connect to database and run migration
docker-compose exec -T timescaledb psql -U osrs_trader -d osrs_market < database/migrations/migration_YYYYMMDD_NNN_description.sql
```

## Creating New Migrations

1. Create migration file with proper naming
2. Include rollback instructions in comments
3. Test on backup before applying to production
4. Document any breaking changes

## Migration Template

```sql
-- Migration: YYYYMMDD_NNN_description
-- Description: Brief description of changes
-- Author: Your name
-- Date: YYYY-MM-DD

-- Apply migration
-- [Your SQL here]

-- Verification query
-- SELECT ...

-- Rollback instructions (if applicable):
-- [How to undo this migration]
```

## Best Practices

1. **Always backup before migrations**
2. **Test migrations on copy of production data**
3. **Run during low-activity periods**
4. **Include verification queries**
5. **Document rollback procedures**
6. **Keep migrations idempotent when possible**

## Example Migrations

### Add New Index
```sql
-- migration_20240115_001_add_item_name_index.sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_items_name_lower 
ON items (LOWER(name));
```

### Add New Column
```sql
-- migration_20240120_002_add_price_change_column.sql
ALTER TABLE prices 
ADD COLUMN IF NOT EXISTS price_change_24h NUMERIC;

-- Create index for new column
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_prices_change 
ON prices (price_change_24h) 
WHERE price_change_24h IS NOT NULL;
```
