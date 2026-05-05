import { useState } from 'react';
import type { Key } from 'react';
import { useMutation } from '@tanstack/react-query';
import { batchRemoveOrphanFiles } from '../api/schema';
import { useDatabase } from '../context/DatabaseContext';
import { useMessageApi } from '../context/MessageContext';
import { MaintenanceTaskContent } from './MaintenanceTaskContent';
import { RetentionActionCard } from './RetentionActionCard';
import { useMaintenanceBatchSuccess } from './useMaintenanceBatchSuccess';

const config = {
  description: 'Queue orphan file cleanup for multiple tables at once.',
  selectAllLabel: 'Select All',
  selectionLabel: 'selected',
};

export function RemoveOrphanFilesBatchContent() {
  const [selectedRowKeys, setSelectedRowKeys] = useState<Key[]>([]);
  const { database } = useDatabase();
  const handleBatchSuccess = useMaintenanceBatchSuccess();
  const messageApi = useMessageApi();

  const mutation = useMutation({
    mutationFn: (values: { tables: string[]; retention_days: number }) =>
      batchRemoveOrphanFiles(database, values.tables, values.retention_days),
    onSuccess: (data, values) => {
      handleBatchSuccess(data, values.tables.length, 'remove orphan files');
      setSelectedRowKeys([]);
    },
    onError: (error: Error) => {
      messageApi.error(`Failed to enqueue remove orphan files tasks: ${error.message}`);
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
          title="Batch Remove Orphan Files"
          cardSize="small"
          disabled={selectedTableNames.length === 0 || mutation.isPending}
          isSubmitting={mutation.isPending}
          sliderWidth={320}
          confirmTitle="Queue remove orphan files tasks"
          confirmDescription={`Enqueue remove orphan files for ${selectedTableNames.length} selected table${selectedTableNames.length === 1 ? '' : 's'}?`}
          confirmOkText="Yes, enqueue"
          submitLabel="Remove Orphan Files for Selected Tables"
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
