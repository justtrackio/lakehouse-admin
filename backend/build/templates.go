package build

import (
	_ "embed"
)

//go:embed spark/rewrite-data-files.yaml
var SparkApplicationTemplates []byte
