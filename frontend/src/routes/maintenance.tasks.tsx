import { createFileRoute } from '@tanstack/react-router';
import { MaintenanceTasksTable } from '../components/MaintenanceTasksTable';

export const Route = createFileRoute('/maintenance/tasks')({
  component: MaintenanceTasksPage,
});

function MaintenanceTasksPage() {
  return (
    <div style={{ padding: 24 }}>
      <MaintenanceTasksTable />
    </div>
  );
}
