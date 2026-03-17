import { createFileRoute } from '@tanstack/react-router';
import { OptimizeBatchContent } from '../components/OptimizeBatchContent';

export const Route = createFileRoute('/maintenance/optimize')({
  component: OptimizeBatchContent,
});
