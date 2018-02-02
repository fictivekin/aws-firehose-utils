
## Firehose Failure Reader

This allows you to retrieve and even resubmit any records that were previously sent to an AWS Kinesis Firehose Stream but failed to be delivered to their end destination.

It also corrects a recurring error that we have with response.exception.message content being inconsistent between string/dict/list. ES rejects them because the first one submitted was a string.

To run:
- source your creds and create the virtual environment
- `python ./manage resubmit_to_es bifrost-<stage> <year> <month> <date>` (month and date must be zero padded when <10)
- Sit back and have a beer while it runs.
