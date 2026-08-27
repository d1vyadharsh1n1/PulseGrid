import { z } from 'zod';

export const eventPayloadSchema = z.object({
  source_id: z.string().min(1, 'source_id is required'),
  event_type: z.string().min(1, 'event_type is required'),
  value: z.number({ message: 'value must be a number' }),
  timestamp: z
    .string()
    .min(1, 'timestamp is required')
    .refine((value) => !Number.isNaN(Date.parse(value)), {
      message: 'timestamp must be a valid ISO 8601 datetime string',
    }),
});
