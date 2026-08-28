import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Space, Typography } from 'antd';
import { removeOrphanFiles, type TableMaintenanceSettings } from '../api/schema';
import { useMessageApi } from '../context/MessageContext';
import { MaintenanceTaskToggle } from './MaintenanceTaskToggle';
import { RetentionActionCard } from './RetentionActionCard';

interface RemoveOrphanFilesTableCardProps {
  database: string;
  tableName: string;
  maintenanceDisabled?: boolean;
  maintenanceSettings?: TableMaintenanceSettings;
}

export function RemoveOrphanFilesTableCard({ database, tableName, maintenanceDisabled = false, maintenanceSettings }: RemoveOrphanFilesTableCardProps) {
  const queryClient = useQueryClient();
  const messageApi = useMessageApi();

  const mutation = useMutation({
    mutationFn: (values: { retention_days: number }) => removeOrphanFiles(database, tableName, values.retention_days),
    onSuccess: (data) => {
      messageApi.success(`Remove orphan files task enqueued (Task ID: ${data.task_id})`);
      queryClient.invalidateQueries({ queryKey: ['tasks', database, tableName] });
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

  const beforeForm = maintenanceDisabled ? (
    <Alert type="warning" showIcon message="Task disabled" description="Remove orphan files is disabled for this table." />
  ) : undefined;

  return (
    <RetentionActionCard
      title={<Space><Typography.Text>Remove Orphan Files</Typography.Text>{maintenanceSettings && <MaintenanceTaskToggle database={database} tableName={tableName} label="Remove orphan files" disabledField="remove_orphan_files_disabled" settings={maintenanceSettings} />}</Space>}
      description="Removes files that are no longer referenced by any snapshot. This helps reclaim storage space. This operation can be time-consuming for large tables."
      beforeForm={beforeForm}
      disabled={maintenanceDisabled || mutation.isPending}
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
