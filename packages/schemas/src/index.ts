// Re-export all TypeScript types
export * from './types/events';

// Re-export Avro schema JSON objects so the producer's registry module
// can import them without duplicating the files.
import worldcupEventSchemaJson from './schemas/worldcup-event.json';
import nbaEventSchemaJson from './schemas/nba-event.json';

export const worldcupEventSchema = worldcupEventSchemaJson;
export const nbaEventSchema = nbaEventSchemaJson;
