package build

import (
	_ "embed"
)

//go:embed spark/maintenance.yaml
var SparkApplicationTemplates []byte
