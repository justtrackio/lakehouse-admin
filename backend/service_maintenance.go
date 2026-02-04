package main

import (
	"context"
	"fmt"
	"time"

	"github.com/gosoline-project/sqlc"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/db"
	"github.com/justtrackio/gosoline/pkg/log"
)

type ExpireSnapshotsResult struct {
	Table                string `json:"table"`
	RetentionDays        int    `json:"retention_days"`
	RetainLast           int    `json:"retain_last"`
	CleanExpiredMetadata bool   `json:"clean_expired_metadata"`
	Status               string `json:"status"`
}

type RemoveOrphanFilesResult struct {
	Table         string         `json:"table"`
	RetentionDays int            `json:"retention_days"`
	Metrics       map[string]any `json:"metrics"`
	Status        string         `json:"status"`
}

type OptimizeResult struct {
	Table               string `json:"table"`
	FileSizeThresholdMb int    `json:"file_size_threshold_mb"`
	Where               string `json:"where"`
	Status              string `json:"status"`
}

func NewServiceMaintenance(ctx context.Context, config cfg.Config, logger log.Logger) (*ServiceMaintenance, error) {
	var err error
	var trino *TrinoClient
	var metadata *ServiceMetadata
	var sqlClient sqlc.Client

	if trino, err = ProvideTrinoClient(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create trino client: %w", err)
	}

	if metadata, err = NewServiceMetadata(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create metadata service: %w", err)
	}

	if sqlClient, err = sqlc.ProvideClient(ctx, config, logger, "default"); err != nil {
		return nil, fmt.Errorf("could not create sqlg client: %w", err)
	}

	return &ServiceMaintenance{
		logger:    logger.WithChannel("maintenance"),
		trino:     trino,
		metadata:  metadata,
		sqlClient: sqlClient,
	}, nil
}

type ServiceMaintenance struct {
	logger    log.Logger
	trino     *TrinoClient
	metadata  *ServiceMetadata
	sqlClient sqlc.Client
}

func (s *ServiceMaintenance) startHistory(ctx context.Context, table string, kind string, input map[string]any) (int64, error) {
	entry := &MaintenanceHistory{
		Table:     table,
		Kind:      kind,
		StartedAt: time.Now(),
		Status:    "running",
		Input:     db.NewJSON(input, db.NonNullable{}),
		Result:    db.NewJSON(map[string]any{}, db.NonNullable{}),
	}

	ins := s.sqlClient.Q().Into("maintenance_history").Records(entry)
	res, err := ins.Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("could not insert maintenance history: %w", err)
	}

	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("could not get last insert id: %w", err)
	}

	return id, nil
}

func (s *ServiceMaintenance) finishHistory(ctx context.Context, id int64, result map[string]any, err error) {
	status := "success"
	var errMsg *string

	if err != nil {
		status = "error"
		msg := err.Error()
		errMsg = &msg
	}

	now := time.Now()
	upd := s.sqlClient.Q().Update("maintenance_history").
		Set("finished_at", &now).
		Set("status", status).
		Set("error_message", errMsg).
		Set("result", db.NewJSON(result, db.NonNullable{})).
		Where(sqlc.Eq{"id": id})

	if _, err := upd.Exec(ctx); err != nil {
		s.logger.Error(ctx, "could not update maintenance history: %s", err)
	}
}

func (s *ServiceMaintenance) ExpireSnapshots(ctx context.Context, table string, retentionDays int, retainLast int) (*ExpireSnapshotsResult, error) {
	var err error
	var result *ExpireSnapshotsResult
	var historyId int64

	input := map[string]any{
		"retention_days": retentionDays,
		"retain_last":    retainLast,
	}

	if historyId, err = s.startHistory(ctx, table, "expire_snapshots", input); err != nil {
		s.logger.Error(ctx, "could not start history: %s", err)
	}

	defer func() {
		res := map[string]any{}
		if result != nil {
			res = map[string]any{
				"table":                  result.Table,
				"retention_days":         result.RetentionDays,
				"retain_last":            result.RetainLast,
				"clean_expired_metadata": result.CleanExpiredMetadata,
				"status":                 result.Status,
			}
		}
		if historyId != 0 {
			s.finishHistory(ctx, historyId, res, err)
		}
	}()

	if retentionDays < 1 {
		err = fmt.Errorf("retention days must be at least 1")
		return nil, err
	}

	if retainLast < 1 {
		err = fmt.Errorf("retain last must be at least 1")
		return nil, err
	}

	retentionThreshold := fmt.Sprintf("%dd", retentionDays)
	qualifiedTable := qualifiedTableName("lakehouse", "main", table)
	query := fmt.Sprintf("ALTER TABLE %s EXECUTE expire_snapshots(retention_threshold => %s, retain_last => %d, clean_expired_metadata => true)", qualifiedTable, quoteLiteral(retentionThreshold), retainLast)

	if err = s.trino.Exec(ctx, query); err != nil {
		err = fmt.Errorf("could not expire snapshots for table %s: %w", table, err)
		return nil, err
	}

	result = &ExpireSnapshotsResult{
		Table:                table,
		RetentionDays:        retentionDays,
		RetainLast:           retainLast,
		CleanExpiredMetadata: true,
		Status:               "ok",
	}

	return result, nil
}

func (s *ServiceMaintenance) RemoveOrphanFiles(ctx context.Context, table string, retentionDays int) (*RemoveOrphanFilesResult, error) {
	var err error
	var result *RemoveOrphanFilesResult
	var historyId int64

	input := map[string]any{
		"retention_days": retentionDays,
	}

	if historyId, err = s.startHistory(ctx, table, "remove_orphan_files", input); err != nil {
		s.logger.Error(ctx, "could not start history: %s", err)
	}

	defer func() {
		res := map[string]any{}
		if result != nil {
			res = map[string]any{
				"table":          result.Table,
				"retention_days": result.RetentionDays,
				"metrics":        result.Metrics,
				"status":         result.Status,
			}
		}
		if historyId != 0 {
			s.finishHistory(ctx, historyId, res, err)
		}
	}()

	if retentionDays < 1 {
		err = fmt.Errorf("retention days must be at least 1")
		return nil, err
	}

	var rows []map[string]any

	retentionThreshold := fmt.Sprintf("%dd", retentionDays)
	qualifiedTable := qualifiedTableName("lakehouse", "main", table)
	query := fmt.Sprintf("ALTER TABLE %s EXECUTE remove_orphan_files(retention_threshold => %s)", qualifiedTable, quoteLiteral(retentionThreshold))

	if rows, err = s.trino.QueryRows(ctx, query); err != nil {
		err = fmt.Errorf("could not remove orphan files for table %s: %w", table, err)
		return nil, err
	}

	metrics := make(map[string]any)
	for _, row := range rows {
		name, okName := row["metric_name"].(string)
		val, okVal := row["metric_value"]

		if okName && okVal {
			metrics[name] = val
		}
	}

	result = &RemoveOrphanFilesResult{
		Table:         table,
		RetentionDays: retentionDays,
		Metrics:       metrics,
		Status:        "ok",
	}

	return result, nil
}

func (s *ServiceMaintenance) Optimize(ctx context.Context, table string, fileSizeThresholdMb int, from DateTime, to DateTime, batchSize string) (*OptimizeResult, error) {
	var err error
	var result *OptimizeResult
	var historyId int64

	input := map[string]any{
		"file_size_threshold_mb": fileSizeThresholdMb,
		"from":                   from,
		"to":                     to,
		"batch_size":             batchSize,
	}

	if historyId, err = s.startHistory(ctx, table, "optimize", input); err != nil {
		s.logger.Error(ctx, "could not start history: %s", err)
	}

	defer func() {
		res := map[string]any{}
		if result != nil {
			res = map[string]any{
				"table":                  result.Table,
				"file_size_threshold_mb": result.FileSizeThresholdMb,
				"where":                  result.Where,
				"status":                 result.Status,
			}
		}
		if historyId != 0 {
			s.finishHistory(ctx, historyId, res, err)
		}
	}()

	if fileSizeThresholdMb < 1 {
		err = fmt.Errorf("file size threshold must be at least 1")
		return nil, err
	}

	var desc *TableDescription
	var partitionColumn string
	var startDate, endDate time.Time

	startDate = from.Time
	endDate = to.Time

	if startDate.After(endDate) {
		err = fmt.Errorf("from date must be before or equal to to date")
		return nil, err
	}

	if desc, err = s.metadata.GetTable(ctx, table); err != nil {
		err = fmt.Errorf("could not get table metadata: %w", err)
		return nil, err
	}

	for _, p := range desc.Partitions.Get() {
		if p.IsHidden && p.Hidden.Type == "day" {
			partitionColumn = p.Hidden.Column
		}
	}

	if partitionColumn == "" {
		err = fmt.Errorf("no suitable day-partition column found for optimization")
		return nil, err
	}

	threshold := fmt.Sprintf("%dMB", fileSizeThresholdMb)
	qualifiedTable := qualifiedTableName("lakehouse", "main", table)

	// We split the optimization into chunks to avoid hitting Trino limits
	current := startDate
	finalEnd := endDate

	for !current.After(finalEnd) {
		var nextStart time.Time

		switch batchSize {
		case "daily":
			nextStart = current.AddDate(0, 0, 1)
		case "weekly":
			nextStart = current.AddDate(0, 0, 7)
		case "yearly":
			nextStart = current.AddDate(1, 0, 0)
		case "monthly":
			fallthrough
		default:
			nextStart = current.AddDate(0, 1, 0)
		}

		// batchEnd is one day before nextStart
		batchEnd := nextStart.AddDate(0, 0, -1)

		// Clamp batchEnd to finalEnd
		if batchEnd.After(finalEnd) {
			batchEnd = finalEnd
		}

		batchWhere := fmt.Sprintf("date(%s) >= date '%s' AND date(%s) <= date '%s'", partitionColumn, current.Format(time.DateOnly), partitionColumn, batchEnd.Format(time.DateOnly))
		query := fmt.Sprintf("ALTER TABLE %s EXECUTE optimize(file_size_threshold => %s) WHERE %s", qualifiedTable, quoteLiteral(threshold), batchWhere)

		s.logger.Info(ctx, "optimizing table %s batch %s to %s", table, current.Format(time.DateOnly), batchEnd.Format(time.DateOnly))

		if err = s.trino.Exec(ctx, query); err != nil {
			err = fmt.Errorf("could not optimize table %s (batch %s): %w", table, batchWhere, err)
			return nil, err
		}

		// Move to the next batch start
		current = nextStart
	}

	// The returned "Where" field still represents the user's original request range
	fullWhere := fmt.Sprintf("date(%s) >= date '%s' AND date(%s) <= date '%s'", partitionColumn, startDate.Format(time.DateOnly), partitionColumn, endDate.Format(time.DateOnly))

	result = &OptimizeResult{
		Table:               table,
		FileSizeThresholdMb: fileSizeThresholdMb,
		Where:               fullWhere,
		Status:              "ok",
	}

	return result, nil
}

func (s *ServiceMaintenance) ListHistory(ctx context.Context, table string, limit int, offset int) (*PaginatedMaintenanceHistory, error) {
	var result []MaintenanceHistory
	var count struct {
		Total int64 `db:"total"`
	}
	var err error

	// 1. Get total count
	cnt := s.sqlClient.Q().From("maintenance_history").Column(sqlc.Col("*").Count().As("total"))
	if table != "" {
		cnt = cnt.Where(sqlc.Eq{"table": table})
	}

	if err = cnt.Get(ctx, &count); err != nil {
		return nil, fmt.Errorf("could not get maintenance history count: %w", err)
	}

	// 2. Get paginated items
	sel := s.sqlClient.Q().From("maintenance_history").OrderBy(sqlc.Col("started_at").Desc())

	if table != "" {
		sel = sel.Where(sqlc.Eq{"table": table})
	}

	sel = sel.Limit(limit).Offset(offset)

	if err = sel.Select(ctx, &result); err != nil {
		return nil, fmt.Errorf("could not list maintenance history: %w", err)
	}

	// Convert to DTO
	dtos := make([]sMaintenanceHistory, len(result))
	for i, r := range result {
		dtos[i] = sMaintenanceHistory{
			Id:           r.Id,
			Table:        r.Table,
			Kind:         r.Kind,
			StartedAt:    r.StartedAt,
			FinishedAt:   r.FinishedAt,
			Status:       r.Status,
			ErrorMessage: r.ErrorMessage,
			Input:        r.Input.Get(),
			Result:       r.Result.Get(),
		}
	}

	return &PaginatedMaintenanceHistory{
		Items: dtos,
		Total: count.Total,
	}, nil
}
