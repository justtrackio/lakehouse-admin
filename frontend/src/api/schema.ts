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
}

export async function fetchTableDetails(tableName: string): Promise<TableDetails> {
  return apiClient.get<TableDetails>(`/api/browse/${tableName}`);
}

export interface ListPartitionItem {
  name: string;
  file_count: number;
  record_count: number;
  total_data_file_size_in_bytes: number;
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

export interface ExpireSnapshotsResponse {
  table: string;
  retention_days: number;
  retain_last: number;
  clean_expired_metadata: boolean;
  status: string;
}

export async function expireSnapshots(
  tableName: string,
  retentionDays: number,
  retainLast: number,
): Promise<ExpireSnapshotsResponse> {
  return apiClient.post<ExpireSnapshotsResponse>(
    `/api/maintenance/${tableName}/expire-snapshots`,
    {
      retention_days: retentionDays,
      retain_last: retainLast,
    }
  );
}

export interface RemoveOrphanFilesResponse {
  table: string;
  retention_days: number;
  metrics: Record<string, unknown>;
  status: string;
}

export async function removeOrphanFiles(
  tableName: string,
  retentionDays: number,
): Promise<RemoveOrphanFilesResponse> {
  return apiClient.post<RemoveOrphanFilesResponse>(
    `/api/maintenance/${tableName}/remove-orphan-files`,
    {
      retention_days: retentionDays,
    }
  );
}

export interface OptimizeResponse {
  table: string;
  file_size_threshold_mb: number;
  where: string;
  status: string;
}

export async function optimizeTable(
  tableName: string,
  fileSizeThresholdMb: number,
  from?: string,
  to?: string,
  batchSize?: string,
): Promise<OptimizeResponse> {
  return apiClient.post<OptimizeResponse>(
    `/api/maintenance/${tableName}/optimize`,
    {
      file_size_threshold_mb: fileSizeThresholdMb,
      from: from,
      to: to,
      batch_size: batchSize,
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
