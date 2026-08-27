import { SNSClient, PublishCommand } from '@aws-sdk/client-sns';
import { withRetry } from './retry.js';

const TOPIC_ARN = process.env.SNS_TOPIC_ARN;

let snsClient;

function getClient() {
  if (!snsClient) {
    snsClient = new SNSClient({
      region: process.env.AWS_REGION ?? 'ap-south-1',
    });
  }
  return snsClient;
}

function buildMessage(alert) {
  return JSON.stringify(
    {
      alert: true,
      reason: alert.reason,
      source_id: alert.source_id,
      event_type: alert.event_type,
      value: alert.value,
      timestamp: alert.timestamp,
    },
    null,
    2,
  );
}

function buildSubject(alert) {
  return `[PulseGrid Alert] ${alert.reason} on ${alert.source_id}`;
}

export function isSnsConfigured() {
  return Boolean(TOPIC_ARN && !TOPIC_ARN.startsWith('arn:aws:sns') === false && TOPIC_ARN.length > 20 && !TOPIC_ARN.includes('xxxx'));
}

export async function publishAlert(alert) {
  const topicArn = process.env.SNS_TOPIC_ARN;
  if (!topicArn) {
    console.log('SNS: SNS_TOPIC_ARN not set, skipping delivery');
    return { skipped: true, reason: 'no_topic_arn' };
  }
  if (topicArn.includes('xxxxxxxxxxxx')) {
    console.log('SNS: SNS_TOPIC_ARN is a placeholder, skipping delivery');
    return { skipped: true, reason: 'placeholder_topic_arn' };
  }

  const client = getClient();
  const subject = buildSubject(alert);
  const message = buildMessage(alert);

  const result = await withRetry(
    async () => {
      const cmd = new PublishCommand({
        TopicArn: topicArn,
        Subject: subject,
        Message: message,
      });
      return await client.send(cmd);
    },
    {
      operationName: 'SNS Publish',
      onRetry: ({ attempt, maxAttempts, delay, err }) => {
        console.warn(
          `SNS retry (${attempt}/${maxAttempts - 1}) after ${delay}ms: ${err.name}: ${err.message}`,
        );
      },
    },
  );

  return { sent: true, messageId: result.MessageId };
}
