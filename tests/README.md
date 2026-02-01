# OSRS Market Data Collector - Test Suite

This directory contains unit tests for the collector utilities.

## Running Tests

### Install Test Dependencies

```bash
pip install pytest pytest-cov
```

### Run All Tests

```bash
# From project root
pytest tests/ -v

# With coverage report
pytest tests/ -v --cov=collectors/utils --cov-report=html
```

### Run Specific Test File

```bash
pytest tests/test_timezone.py -v
pytest tests/test_state_management.py -v
pytest tests/test_api_client.py -v
```

### Run Tests in Docker

```bash
# Run tests in collector container
docker-compose exec realtime-collector python -m pytest tests/ -v
```

## Test Coverage

Current test coverage includes:

- **test_timezone.py**: Timezone conversion, UTC enforcement, strict mode
- **test_state_management.py**: State persistence, restart logic, progress tracking
- **test_api_client.py**: API requests, rate limiting, data parsing

## Adding New Tests

1. Create new test file: `test_<module>.py`
2. Import the module being tested
3. Create test class inheriting from `unittest.TestCase`
4. Write test methods starting with `test_`
5. Run tests to verify

Example:

```python
import unittest
from utils.your_module import YourClass

class TestYourClass(unittest.TestCase):
    def test_something(self):
        result = YourClass.do_something()
        self.assertEqual(result, expected_value)
```
