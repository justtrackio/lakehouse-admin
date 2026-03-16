import { createFileRoute } from '@tanstack/react-router';
import { RemoveOrphanFilesBatchContent } from '../components/RemoveOrphanFilesBatchContent';

export const Route = createFileRoute('/maintenance/remove-orphan-files')({
  component: RemoveOrphanFilesBatchContent,
});
