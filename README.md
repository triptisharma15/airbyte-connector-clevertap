# CleverTap Profiles API Source Connector

This is an Airbyte source connector for the CleverTap Profiles API. It implements a 2-step cursor-based pagination to fetch user profile data from CleverTap.

## Features

- ✅ Full refresh sync mode
- ✅ 2-step cursor pagination (POST for cursor, GET with cursor for records)
- ✅ Multi-region support (in1, us1, sg1, sk1, eu1)
- ✅ Configurable event name and date range
- ✅ Flexible schema with additionalProperties for custom profile fields
- ✅ Proper error handling and logging

## Configuration

The connector requires the following configuration:

```json
{
  "account_id": "YOUR_ACCOUNT_ID",
  "passcode": "YOUR_PASSCODE",
  "region": "in1",
  "event_name": "App Launched",
  "start_date": 20220101,
  "end_date": 20260212
}
```

### Configuration Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `account_id` | string | Yes | Your CleverTap Account ID |
| `passcode` | string | Yes | Your CleverTap Passcode (secret) |
| `region` | string | No | API region: in1, us1, sg1, sk1, eu1 (default: no region prefix) |
| `event_name` | string | Yes | Event name to filter profiles (e.g., "App Launched") |
| `start_date` | integer | Yes | Start date in YYYYMMDD format (e.g., 20220101) |
| `end_date` | integer | Yes | End date in YYYYMMDD format (e.g., 20260212) |

## Installation

### Local Development

1. Clone this repository:
   ```bash
   cd source-clevertap
   ```

2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   pip install -e .
   ```

4. Create your configuration file:
   ```bash
   mkdir -p secrets
   cp secrets/config.json.example secrets/config.json
   # Edit secrets/config.json with your credentials
   ```

## Usage

### Get Connector Spec
```bash
python main.py spec
```

### Check Connection
```bash
python main.py check --config secrets/config.json
```

### Discover Schema
```bash
python main.py discover --config secrets/config.json
```

### Read Data (Sync)
```bash
python main.py read --config secrets/config.json --catalog integration_tests/configured_catalog.json
```

## API Details

The connector uses CleverTap's Profiles API with the following flow:

### Step 1: Get Cursor (POST)
```
POST https://in1.api.clevertap.com/1/profiles.json
Body: {
  "event_name": "App Launched",
  "from": 20220101,
  "to": 20260212
}
Response: {
  "status": "success",
  "cursor": "CURSOR_STRING"
}
```

### Step 2: Fetch Records (GET with cursor)
```
GET https://in1.api.clevertap.com/1/profiles.json?cursor=CURSOR_VALUE
Response: {
  "status": "success",
  "records": [...],
  "cursor": "NEXT_CURSOR_OR_ABSENT"
}
```

The connector loops through Step 2 until no cursor is returned.

## Region Support

The connector supports multiple CleverTap regions:

- `in1` → https://in1.api.clevertap.com
- `us1` → https://us1.api.clevertap.com
- `sg1` → https://sg1.api.clevertap.com
- `sk1` → https://sk1.api.clevertap.com
- `eu1` → https://eu1.api.clevertap.com
- (empty) → https://api.clevertap.com

## Building for Airbyte Cloud

To build and publish this connector to Airbyte:

1. Build the Docker image:
   ```bash
   docker build . -t airbyte/source-clevertap:dev
   ```

2. Test the Docker image:
   ```bash
   docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-clevertap:dev spec
   docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-clevertap:dev check --config /secrets/config.json
   docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-clevertap:dev discover --config /secrets/config.json
   ```

3. Add to Airbyte:
   - Follow the [Airbyte documentation](https://docs.airbyte.com/connector-development/connector-builder-ui/tutorial) to add this custom connector to your Airbyte instance

## Troubleshooting

### Connection Check Fails
- Verify your `account_id` and `passcode` are correct
- Ensure you're using the correct `region` for your account
- Check that your date range is valid (start_date <= end_date)

### No Records Returned
- Verify that the `event_name` exists in your CleverTap account
- Check that there are users who triggered the event in the specified date range
- Try a broader date range or different event name

### API Errors
- Check CleverTap API status and rate limits
- Review the error message in the logs for specific API errors
- Ensure your credentials have the necessary permissions

## License

MIT License

## Support

For issues and questions:
- CleverTap API Documentation: https://developer.clevertap.com/docs/profiles-api
- Airbyte CDK Documentation: https://docs.airbyte.com/connector-development/cdk-python/

