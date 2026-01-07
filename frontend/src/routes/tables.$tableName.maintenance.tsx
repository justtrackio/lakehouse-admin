import { createFileRoute } from '@tanstack/react-router';
import { Space } from 'antd';
import { ExpireSnapshotsCard } from '../components/ExpireSnapshotsCard';
import { RemoveOrphanFilesCard } from '../components/RemoveOrphanFilesCard';

export const Route = createFileRoute('/tables/$tableName/maintenance')({
  component: MaintenancePage,
});

function MaintenancePage() {
  const { tableName } = Route.useParams();

  return (
    <Space direction="vertical" style={{ width: '100%' }} size="large">
      <ExpireSnapshotsCard tableName={tableName} />
      <RemoveOrphanFilesCard tableName={tableName} />
    </Space>
  );
}
