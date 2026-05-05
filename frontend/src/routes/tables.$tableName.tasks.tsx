import { createFileRoute } from '@tanstack/react-router';
import { useQuery } from '@tanstack/react-query';
import { Space } from 'antd';
import { ExpireSnapshotsTableCard } from '../components/ExpireSnapshotsTableCard';
import { RemoveOrphanFilesTableCard } from '../components/RemoveOrphanFilesTableCard';
import { OptimizeCard } from '../components/OptimizeCard';
import { MaintenanceTasksTable } from '../components/MaintenanceTasksTable';
import { fetchTableDetails } from '../api/schema';
import { normalizeDatabaseSearch } from '../utils/database';

export const Route = createFileRoute('/tables/$tableName/tasks')({
  validateSearch: normalizeDatabaseSearch,
  component: TasksPage,
});

function TasksPage() {
  const { tableName } = Route.useParams();
  const { database } = Route.useSearch();

  const { data: tableDetails, isLoading } = useQuery({
    queryKey: ['table', database, tableName],
    queryFn: () => fetchTableDetails(database, tableName),
  });

  return (
    <Space direction="vertical" style={{ width: '100%' }} size="large">
      <MaintenanceTasksTable database={database} tableName={tableName} />
      <ExpireSnapshotsTableCard
        database={database}
        tableName={tableName}
        snapshotCount={tableDetails?.snapshot_count}
        snapshotCountLoading={isLoading}
      />
      <RemoveOrphanFilesTableCard database={database} tableName={tableName} />
      <OptimizeCard database={database} tableName={tableName} />
    </Space>
  );
}
