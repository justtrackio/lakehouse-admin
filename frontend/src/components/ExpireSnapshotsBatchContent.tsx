import { useState } from 'react';
import type { Key } from 'react';
import { useMutation } from '@tanstack/react-query';
import { batchExpireSnapshots } from '../api/schema';
import { useDatabase } from '../context/DatabaseContext';
import { useMessageApi } from '../context/MessageContext';
import { MaintenanceTaskContent } from './MaintenanceTaskContent';
import { RetentionActionCard } from './RetentionActionCard';
import { useMaintenanceBatchSuccess } from './useMaintenanceBatchSuccess';

const config = {
  description: 'Expire old snapshots in batch across multiple tables.',
  selectAllLabel: 'Select All',
  selectionLabel: 'selected',
};

export function ExpireSnapshotsBatchContent() {
  const [selectedRowKeys, setSelectedRowKeys] = useState<Key[]>([]);
  const { database } = useDatabase();
  const handleBatchSuccess = useMaintenanceBatchSuccess();
  const messageApi = useMessageApi();

  const mutation = useMutation({
    mutationFn: (values: { tables: string[]; retention_days: number }) =>
      batchExpireSnapshots(database, values.tables, values.retention_days),
    onSuccess: (data, values) => {
      handleBatchSuccess(data, values.tables.length, 'expire snapshots');
      setSelectedRowKeys([]);
    },
    onError: (error: Error) => {
      messageApi.error(`Failed to enqueue expire snapshots tasks: ${error.message}`);
    },
  });

  return (
    <MaintenanceTaskContent
      database={database}
      config={config}
      selectedRowKeys={selectedRowKeys}
      onSelectedRowKeysChange={setSelectedRowKeys}
      isSubmitting={mutation.isPending}
      formContent={(selectedTableNames) => (
        <RetentionActionCard
          title="Batch Expire Snapshots"
          cardSize="small"
          disabled={selectedTableNames.length === 0 || mutation.isPending}
          isSubmitting={mutation.isPending}
          sliderWidth={1024}
          confirmTitle="Expire snapshots"
          confirmDescription={`Enqueue expire snapshots for ${selectedTableNames.length} selected table${selectedTableNames.length === 1 ? '' : 's'}?`}
          confirmOkText="Yes, enqueue"
          submitLabel="Expire Snapshots for Selected Tables"
          onSubmit={(values) =>
            mutation.mutate({
              tables: selectedTableNames,
              retention_days: values.retention_days,
            })
          }
        />
      )}
    />
  );
}
