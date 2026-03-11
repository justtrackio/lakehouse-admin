package internal

func expireSnapshotsResultMap(res *ExpireSnapshotsResult) map[string]any {
	return map[string]any{
		"table":                  res.Table,
		"retention_days":         res.RetentionDays,
		"retain_last":            res.RetainLast,
		"clean_expired_metadata": res.CleanExpiredMetadata,
		"status":                 res.Status,
	}
}

func removeOrphanFilesResultMap(res *RemoveOrphanFilesResult) map[string]any {
	return map[string]any{
		"table":          res.Table,
		"retention_days": res.RetentionDays,
		"metrics":        res.Metrics,
		"status":         res.Status,
	}
}

func optimizeResultMap(res *OptimizeResult) map[string]any {
	return map[string]any{
		"table":                  res.Table,
		"file_size_threshold_mb": res.FileSizeThresholdMb,
		"where":                  res.Where,
		"tracking_id":            res.ApplicationName,
		"application_name":       res.ApplicationName,
		"status":                 res.Status,
	}
}
