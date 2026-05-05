import { useQueryClient } from '@tanstack/react-query';
import type { BatchTaskQueuedResponse } from '../api/schema';
import { useDatabase } from '../context/DatabaseContext';
import { useMessageApi } from '../context/MessageContext';

export function useMaintenanceBatchSuccess() {
  const queryClient = useQueryClient();
  const messageApi = useMessageApi();
  const { database } = useDatabase();

  return (data: BatchTaskQueuedResponse, requestedCount: number, actionLabel: string) => {
    queryClient.invalidateQueries({ queryKey: ['tasks', database] });
    queryClient.invalidateQueries({ queryKey: ['taskCounts', database] });

    if (data.failed_tables.length === 0) {
      messageApi.success(
        `Enqueued ${data.enqueued_count} ${actionLabel} task${data.enqueued_count === 1 ? '' : 's'} for ${requestedCount} table${requestedCount === 1 ? '' : 's'}`
      );
      return;
    }

    const failedPreview = data.failed_tables.slice(0, 3).map((failure) => failure.table).join(', ');
    messageApi.warning(
      `Enqueued ${data.enqueued_count} ${actionLabel} task${data.enqueued_count === 1 ? '' : 's'} with ${data.failed_tables.length} failure${data.failed_tables.length === 1 ? '' : 's'} (${failedPreview}${data.failed_tables.length > 3 ? ', ...' : ''})`
    );
  };
}
