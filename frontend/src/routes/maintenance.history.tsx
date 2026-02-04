import { createFileRoute } from '@tanstack/react-router';
import { MaintenanceHistoryTable } from '../components/MaintenanceHistoryTable';

export const Route = createFileRoute('/maintenance/history')({
  component: MaintenanceHistoryPage,
});

function MaintenanceHistoryPage() {
  return (
    <div style={{ padding: 24 }}>
      <MaintenanceHistoryTable />
    </div>
  );
}
