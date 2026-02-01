# OSRS Market Data Collector

Production-ready data collection system for Old School RuneScape Grand Exchange market data.

## Quick Start

### Requirements
- Docker & Docker Compose
- 4GB RAM (8GB recommended)
- ~60GB free disk space

### Setup

```bash
# 1. Configure environment
cp .env.example .env
# Edit .env and set secure passwords

# 2. Start services
docker-compose up -d

# 3. Access Grafana
echo "http://localhost:3000 (admin/admin)"
```

### Services
- **TimescaleDB**: Time-series database on port 5432
- **Grafana**: Dashboards on port 3000
- **Realtime Collector**: Fetches 5-min price data
- **Backfill Collector**: Historical data (6 months)
- **Metadata Collector**: Item information

## Configuration

Key environment variables in `.env`:

```bash
POSTGRES_PASSWORD=your_secure_password
API_USER_AGENT=YourAppName/1.0
HISTORICAL_BACKFILL_MONTHS=6
LOG_LEVEL=INFO
```

## Operations

```bash
# View logs
docker-compose logs -f realtime-collector

# Check status
docker-compose ps

# Restart collectors
docker-compose restart realtime-collector backfill-collector

# Stop everything
docker-compose down -v
```

## Backup & Restore

```bash
# Automated backup
./scripts/backup.sh

# Restore
docker-compose exec -T timescaledb pg_restore -U osrs_trader -d osrs_market < backup.dump
```

## Database Schema

**prices** table (hypertable):
- `timestamp`, `item_id` (primary key)
- `avg_high_price`, `avg_low_price`
- `high_price_volume`, `low_price_volume`
- `spread`, `total_volume` (generated)

See `database/init.sql` for full schema.

## Troubleshooting

**No data being collected:**
```bash
docker-compose logs -f realtime-collector
```

**Database connection issues:**
```bash
docker-compose exec timescaledb pg_isready -U osrs_trader
```

**Check disk space:**
```bash
docker-compose exec timescaledb psql -U osrs_trader -c "SELECT pg_size_pretty(pg_database_size('osrs_market'));"
```

## Development

```bash
# Run tests
cd tests && python test_api_client.py

# Code formatting
ruff check . --fix
```

## License

MIT
