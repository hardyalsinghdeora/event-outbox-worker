import type { Database } from '../db/database.js';
import { type EventEnvelope, ORDER_PLACED, orderPlacedSchema } from '../events.js';
import type { EventHandler } from './consumer.js';

export const INVENTORY_PROJECTION = 'inventory-projection';

/** Reserves stock per SKU for every OrderPlaced. The validation here is what turns a bad payload into a dead letter. */
export const inventoryProjection: EventHandler = {
  eventType: ORDER_PLACED,
  apply(db: Database, event: EventEnvelope): void {
    const order = orderPlacedSchema.parse(event.data);
    for (const line of order.lines) {
      db.run(
        `INSERT INTO inventory_projection (sku, reserved_qty) VALUES (?, ?)
         ON CONFLICT (sku) DO UPDATE SET reserved_qty = reserved_qty + excluded.reserved_qty`,
        [line.sku, line.qty],
      );
    }
  },
};

export interface InventoryRow {
  sku: string;
  reserved_qty: number;
}

export function readInventoryProjection(db: Database): InventoryRow[] {
  return db.all<InventoryRow>('SELECT sku, reserved_qty FROM inventory_projection ORDER BY sku');
}
