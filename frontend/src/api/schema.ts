import { apiClient } from './client';

export interface PartitionHiddenInfo {
  column: string;
  type: string;
}

export interface Partition {
  name: string;
  is_hidden: boolean;
  hidden: PartitionHiddenInfo;
}

export interface ListTableItem {
  name: string;
  partitions: Partition[];
  snapshot_count: number;
  partition_count: number;
  file_count: number;
  record_count: number;
  total_data_file_size_in_bytes: number;
  needs_optimize: boolean;
}

export interface ListTablesResponse {
  tables: ListTableItem[];
}

export async function fetchTables(): Promise<ListTableItem[]> {
  const response = await apiClient.get<ListTablesResponse>('/api/browse/tables');
  return response.tables;
}

export interface TableDetails {
  name: string;
  partitions: Partition[];
  snapshot_count: number;
  partition_count: number;
  file_count: number;
  record_count: number;
  total_data_file_size_in_bytes: number;
  updated_at: string;
}

export async function fetchTableDetails(tableName: string): Promise<TableDetails> {
  return apiClient.get<TableDetails>(`/api/browse/${tableName}`);
}

export interface TableSchemaColumn {
  name: string;
  type: string;
}

export interface TableSchema {
  name: string;
  columns: TableSchemaColumn[];
  partitions: Partition[];
  updated_at: string;
}

export async function fetchTableSchema(tableName: string): Promise<TableSchema> {
  return apiClient.get<TableSchema>(`/api/iceberg/${tableName}`);
}

export interface ListPartitionItem {
  name: string;
  file_count: number;
  record_count: number;
  total_data_file_size_in_bytes: number;
  needs_optimize: boolean;
  needs_optimize_count: number;
}

export interface ListPartitionsResponse {
  partitions: ListPartitionItem[];
}

export async function fetchPartitionValues(
  tableName: string,
  partitionFilters: Record<string, string>,
): Promise<ListPartitionItem[]> {
  const response = await apiClient.post<ListPartitionsResponse>(
    `/api/browse/${tableName}/partitions`,
    { partitions: partitionFilters }
  );
  return response.partitions;
}

export interface SnapshotItem {
  committed_at: string;
  snapshot_id: string;
  parent_id: string;
  operation: string;
  manifest_list: string;
  summary: Record<string, unknown>;
}

export async function fetchSnapshots(tableName: string): Promise<SnapshotItem[]> {
  return apiClient.get<SnapshotItem[]>(`/api/metadata/snapshots?table=${tableName}`);
}

export interface TaskQueuedResponse {
  task_id: number;
  status: string;
}

export interface BatchTaskFailure {
  table: string;
  error: string;
}

export interface BatchTaskQueuedResponse {
  task_ids: number[];
  enqueued_count: number;
  failed_tables: BatchTaskFailure[];
}

export async function expireSnapshots(
  tableName: string,
  retentionDays: number,
): Promise<TaskQueuedResponse> {
  return apiClient.post<TaskQueuedResponse>(
    `/api/tasks/by-table/${tableName}/expire-snapshots`,
    {
      retention_days: retentionDays,
    }
  );
}

export async function removeOrphanFiles(
  tableName: string,
  retentionDays: number,
): Promise<TaskQueuedResponse> {
  return apiClient.post<TaskQueuedResponse>(
    `/api/tasks/by-table/${tableName}/remove-orphan-files`,
    {
      retention_days: retentionDays,
    }
  );
}

export async function batchExpireSnapshots(
  tables: string[],
  retentionDays: number,
): Promise<BatchTaskQueuedResponse> {
  return apiClient.post<BatchTaskQueuedResponse>(
    '/api/maintenance/expire-snapshots',
    {
      tables,
      retention_days: retentionDays,
    }
  );
}

export async function batchRemoveOrphanFiles(
  tables: string[],
  retentionDays: number,
): Promise<BatchTaskQueuedResponse> {
  return apiClient.post<BatchTaskQueuedResponse>(
    '/api/maintenance/remove-orphan-files',
    {
      tables,
      retention_days: retentionDays,
    }
  );
}

export interface OptimizeTaskQueuedResponse {
  task_ids: number[];
  status: string;
}

export type OptimizeChunkBy = 'daily' | 'weekly' | 'monthly';

export interface BatchOptimizeTableRequest {
  table: string;
  chunk_by: OptimizeChunkBy;
}

export async function optimizeTable(
  tableName: string,
  fileSizeThresholdMb: number,
  from?: string,
  to?: string,
  chunkBy: OptimizeChunkBy = 'daily',
): Promise<OptimizeTaskQueuedResponse> {
  return apiClient.post<OptimizeTaskQueuedResponse>(
    `/api/tasks/by-table/${tableName}/optimize`,
    {
      file_size_threshold_mb: fileSizeThresholdMb,
      from: from,
      to: to,
      chunk_by: chunkBy,
    }
  );
}

export async function batchOptimize(
  tables: BatchOptimizeTableRequest[],
  fileSizeThresholdMb: number,
  from: string,
  to: string,
): Promise<BatchTaskQueuedResponse> {
  return apiClient.post<BatchTaskQueuedResponse>(
    '/api/maintenance/optimize',
    {
      tables,
      file_size_threshold_mb: fileSizeThresholdMb,
      from,
      to,
    }
  );
}

export interface RefreshFullResponse {
  status: string;
}

export async function refreshFull(): Promise<RefreshFullResponse> {
  return apiClient.get<RefreshFullResponse>('/api/refresh/full');
}

export interface RefreshTableResponse {
  name: string;
}

export async function refreshTable(tableName: string): Promise<RefreshTableResponse> {
  return apiClient.get<RefreshTableResponse>(`/api/refresh/table?table=${tableName}`);
}

export interface Task {
  id: number;
  table: string;
  kind: string;
  engine: string;
  status: string;
  retried: boolean;
  can_retry: boolean;
  started_at: string;
  picked_up_at: string | null;
  finished_at: string | null;
  error_message: string | null;
  input: Record<string, unknown>;
  result: Record<string, unknown>;
}

export interface PaginatedTasks {
  items: Task[];
  total: number;
}

export async function fetchTasks(
  tableName?: string,
  limit: number = 20,
  offset: number = 0,
  kinds?: string[],
  statuses?: string[],
): Promise<PaginatedTasks> {
  const params = new URLSearchParams();
  if (tableName) {
    params.append('table', tableName);
  }
  if (kinds) {
    kinds.forEach((k) => params.append('kind', k));
  }
  if (statuses) {
    statuses.forEach((s) => params.append('status', s));
  }
  params.append('limit', limit.toString());
  params.append('offset', offset.toString());

  return apiClient.get<PaginatedTasks>(`/api/tasks?${params.toString()}`);
}

export interface TaskCountsResponse {
  running: number;
  queued: number;
}

export async function fetchTaskCounts(): Promise<TaskCountsResponse> {
  return apiClient.get<TaskCountsResponse>('/api/tasks/counts');
}

export interface TaskConcurrencyResponse {
  value: number;
}

export async function fetchTaskConcurrency(): Promise<TaskConcurrencyResponse> {
  return apiClient.get<TaskConcurrencyResponse>('/api/settings/task-concurrency');
}

export async function setTaskConcurrency(value: number): Promise<TaskConcurrencyResponse> {
  return apiClient.put<TaskConcurrencyResponse>('/api/settings/task-concurrency', { value });
}

export interface FlushTasksResponse {
  deleted: number;
}

export async function flushTasks(): Promise<FlushTasksResponse> {
  return apiClient.delete<FlushTasksResponse>('/api/tasks');
}

export interface RetryAllTasksResponse {
  retried_count: number;
}

export async function retryAllTasks(): Promise<RetryAllTasksResponse> {
  return apiClient.post<RetryAllTasksResponse>('/api/tasks/retry-all', {});
}

export async function retryTask(taskId: number): Promise<TaskQueuedResponse> {
  return apiClient.post<TaskQueuedResponse>(`/api/tasks/retry/${taskId}`, {});
}
