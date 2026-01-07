import { createFileRoute } from '@tanstack/react-router';
import { useQuery } from '@tanstack/react-query';
import { Space } from 'antd';
import { ExpireSnapshotsCard } from '../components/ExpireSnapshotsCard';
import { RemoveOrphanFilesCard } from '../components/RemoveOrphanFilesCard';
import { OptimizeCard } from '../components/OptimizeCard';
import { fetchTableDetails } from '../api/schema';

export const Route = createFileRoute('/tables/$tableName/maintenance')({
  component: MaintenancePage,
});

function MaintenancePage() {
  const { tableName } = Route.useParams();

  const { data: tableDetails, isLoading } = useQuery({
    queryKey: ['table', tableName],
    queryFn: () => fetchTableDetails(tableName),
  });

  return (
    <Space direction="vertical" style={{ width: '100%' }} size="large">
      <ExpireSnapshotsCard
        tableName={tableName}
        snapshotCount={tableDetails?.snapshot_count}
        snapshotCountLoading={isLoading}
      />
      <RemoveOrphanFilesCard tableName={tableName} />
      <OptimizeCard tableName={tableName} />
    </Space>
  );
}
