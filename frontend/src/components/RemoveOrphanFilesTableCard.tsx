import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert } from 'antd';
import { removeOrphanFiles } from '../api/schema';
import { useMessageApi } from '../context/MessageContext';
import { RetentionActionCard } from './RetentionActionCard';

interface RemoveOrphanFilesTableCardProps {
  tableName: string;
}

export function RemoveOrphanFilesTableCard({ tableName }: RemoveOrphanFilesTableCardProps) {
  const queryClient = useQueryClient();
  const messageApi = useMessageApi();

  const mutation = useMutation({
    mutationFn: (values: { retention_days: number }) => removeOrphanFiles(tableName, values.retention_days),
    onSuccess: (data) => {
      messageApi.success(`Remove orphan files task enqueued (Task ID: ${data.task_id})`);
      queryClient.invalidateQueries({ queryKey: ['tasks', tableName] });
    },
    onError: (error: Error) => {
      messageApi.error(`Failed to enqueue remove orphan files task: ${error.message}`);
    },
  });

  const afterForm = mutation.isError ? (
    <Alert
      type="error"
      showIcon
      message="Operation Failed"
      description={mutation.error.message}
    />
  ) : undefined;

  return (
    <RetentionActionCard
      title="Remove Orphan Files"
      description="Removes files that are no longer referenced by any snapshot. This helps reclaim storage space. This operation can be time-consuming for large tables."
      disabled={mutation.isPending}
      isSubmitting={mutation.isPending}
      retentionDaysExtra="Files older than this that are not referenced by any snapshot will be removed."
      sliderWidth={500}
      confirmTitle="Remove orphan files"
      confirmDescription="Are you sure you want to remove orphan files?"
      confirmOkText="Yes, remove"
      submitLabel="Remove Orphan Files"
      afterForm={afterForm}
      onSubmit={(values) => mutation.mutate(values)}
    />
  );
}
