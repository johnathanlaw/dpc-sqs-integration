# Minimum Permissions

The Lambda will need the following permissions:

- To interact with SQS:
  - `sqs:GetQueueAttributes`,
  - `sqs:ReceiveMessage`,
  - `sqs:DeleteMessage`
- To interact with Secrets Manager:
  - `secretsmanager:GetSecretValue`
