import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Spin, Space, Typography } from 'antd';
import { expireSnapshots, type TableMaintenanceSettings } from '../api/schema';
import { useMessageApi } from '../context/MessageContext';
import { MaintenanceTaskToggle } from './MaintenanceTaskToggle';
import { RetentionActionCard } from './RetentionActionCard';

interface ExpireSnapshotsTableCardProps {
  database: string;
  tableName: string;
  snapshotCount?: number;
  snapshotCountLoading?: boolean;
  maintenanceDisabled?: boolean;
  maintenanceSettings?: TableMaintenanceSettings;
}

export function ExpireSnapshotsTableCard({
  database,
  tableName,
  snapshotCount,
  snapshotCountLoading,
  maintenanceDisabled = false,
  maintenanceSettings,
}: ExpireSnapshotsTableCardProps) {
  const queryClient = useQueryClient();
  const messageApi = useMessageApi();

  const mutation = useMutation({
    mutationFn: (values: { retention_days: number }) =>
      expireSnapshots(database, tableName, values.retention_days),
    onSuccess: (data) => {
      messageApi.success(`Expire snapshots task enqueued (Task ID: ${data.task_id})`);
      queryClient.invalidateQueries({ queryKey: ['tasks', database, tableName] });
    },
    onError: (error: Error) => {
      messageApi.error(`Failed to enqueue expire snapshots task: ${error.message}`);
    },
  });

  const isReady = !snapshotCountLoading;
  const hasSnapshots = isReady && (snapshotCount ?? 0) > 0;

  const isDisabled = maintenanceDisabled || !isReady || !hasSnapshots || mutation.isPending;

  const beforeForm = maintenanceDisabled ? (
    <Alert type="warning" showIcon message="Task disabled" description="Expire snapshots is disabled for this table." />
  ) : !isReady ? (
    <div style={{ textAlign: 'center', padding: '16px 0' }}>
      <Spin size="default" />
      <div style={{ marginTop: 8 }}>Loading snapshot count...</div>
    </div>
  ) : isReady && !hasSnapshots ? (
    <Alert
      type="warning"
      showIcon
      message="No snapshots available"
      description="This table currently has no snapshots, so there is nothing to expire."
    />
  ) : undefined;

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
      title={<Space><Typography.Text>Expire Snapshots</Typography.Text>{maintenanceSettings && <MaintenanceTaskToggle database={database} tableName={tableName} label="Expire snapshots" disabledField="expire_snapshots_disabled" settings={maintenanceSettings} />}</Space>}
      description="Removes snapshots older than the specified retention period. This action frees up storage space and cleans up metadata, but it is irreversible."
      beforeForm={beforeForm}
      afterForm={afterForm}
      disabled={isDisabled}
      isSubmitting={mutation.isPending}
      sliderWidth={1024}
      confirmTitle="Expire snapshots"
      confirmDescription="Are you sure you want to expire old snapshots? This cannot be undone."
      confirmOkText="Yes, expire"
      submitLabel="Expire Snapshots"
      onSubmit={(values) => mutation.mutate(values)}
    />
  );
}
