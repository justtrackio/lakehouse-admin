import { useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Popconfirm, Space, Switch, Typography } from 'antd';
import { updateTableMaintenanceSettings, type TableMaintenanceSettings } from '../api/schema';
import { useMessageApi } from '../context/MessageContext';

const { Text } = Typography;

type DisabledField = 'optimize_disabled' | 'expire_snapshots_disabled' | 'remove_orphan_files_disabled';

interface MaintenanceTaskToggleProps {
  database: string;
  tableName: string;
  label: string;
  disabledField: DisabledField;
  settings: TableMaintenanceSettings;
}

export function MaintenanceTaskToggle({ database, tableName, label, disabledField, settings }: MaintenanceTaskToggleProps) {
  const [confirmOpen, setConfirmOpen] = useState(false);
  const queryClient = useQueryClient();
  const messageApi = useMessageApi();
  const enabled = !settings[disabledField];

  const mutation = useMutation({
    mutationFn: () => updateTableMaintenanceSettings(database, tableName, {
      ...settings,
      [disabledField]: enabled,
    }),
    onSuccess: (result) => {
      queryClient.invalidateQueries({ queryKey: ['table', database, tableName] });
      queryClient.invalidateQueries({ queryKey: ['tables', database] });
      queryClient.invalidateQueries({ queryKey: ['tasks'] });
      queryClient.invalidateQueries({ queryKey: ['taskCounts'] });
      messageApi.success(
        result.cancelled_task_count > 0
          ? `${label} disabled and ${result.cancelled_task_count} queued task${result.cancelled_task_count === 1 ? '' : 's'} cancelled`
          : `${label} ${enabled ? 'disabled' : 'enabled'}`,
      );
    },
    onError: (error: Error) => {
      messageApi.error(`Failed to update ${label.toLowerCase()}: ${error.message}`);
    },
  });

  return (
    <Popconfirm
      title={`${enabled ? 'Disable' : 'Enable'} ${label}?`}
      description={enabled ? 'Queued tasks of this type will be cancelled.' : 'This task type can be queued again.'}
      open={confirmOpen}
      onOpenChange={setConfirmOpen}
      onConfirm={() => mutation.mutate()}
      okText={enabled ? 'Disable' : 'Enable'}
      okButtonProps={enabled ? { danger: true } : undefined}
      cancelText="Cancel"
    >
      <Space size="small">
        <Text type="secondary">Enabled</Text>
        <Switch size="small" checked={enabled} loading={mutation.isPending} />
      </Space>
    </Popconfirm>
  );
}
