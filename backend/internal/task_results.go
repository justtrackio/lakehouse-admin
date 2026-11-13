package internal

func expireSnapshotsResultMap(res *ExpireSnapshotsResult) map[string]any {
	return map[string]any{
		fieldDatabase:            res.Database,
		fieldTable:               res.Table,
		fieldRetentionDays:       res.RetentionDays,
		"clean_expired_metadata": res.CleanExpiredMetadata,
		fieldStatus:              res.Status,
	}
}

func removeOrphanFilesResultMap(res *RemoveOrphanFilesResult) map[string]any {
	return map[string]any{
		fieldDatabase:      res.Database,
		fieldTable:         res.Table,
		fieldRetentionDays: res.RetentionDays,
		"metrics":          res.Metrics,
		fieldStatus:        res.Status,
	}
}

func optimizeResultMap(res *OptimizeResult) map[string]any {
	return map[string]any{
		fieldDatabase:         res.Database,
		fieldTable:            res.Table,
		"target_file_size_mb": res.TargetFileSizeMb,
		"where":               res.Where,
		fieldTrackingID:       res.ApplicationName,
		fieldApplicationName:  res.ApplicationName,
		fieldStatus:           res.Status,
	}
}
