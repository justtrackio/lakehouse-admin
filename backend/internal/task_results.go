package internal

func optimizeResultMap(res *OptimizeResult) map[string]any {
	return map[string]any{
		"database":            res.Database,
		"table":               res.Table,
		"target_file_size_mb": res.TargetFileSizeMb,
		"where":               res.Where,
		"tracking_id":         res.ApplicationName,
		"application_name":    res.ApplicationName,
		"status":              res.Status,
	}
}
