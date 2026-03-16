import { createFileRoute } from '@tanstack/react-router';
import { ExpireSnapshotsBatchContent } from '../components/ExpireSnapshotsBatchContent';

export const Route = createFileRoute('/maintenance/expire-snapshots')({
  component: ExpireSnapshotsBatchContent,
});
