import { createFileRoute } from '@tanstack/react-router';
import { MaintenanceTasksTable } from '../components/MaintenanceTasksTable';

export const Route = createFileRoute('/tasks')({
  component: TasksPage,
});

function TasksPage() {
  return (
    <div style={{ padding: 24 }}>
      <MaintenanceTasksTable />
    </div>
  );
}
