package internal

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsS3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/justtrackio/gosoline/pkg/cfg"
	gosoS3 "github.com/justtrackio/gosoline/pkg/cloud/aws/s3"
	"github.com/justtrackio/gosoline/pkg/funk"
	"github.com/justtrackio/gosoline/pkg/log"
)

func NewServiceFileIntegrity(ctx context.Context, config cfg.Config, logger log.Logger) (*ServiceFileIntegrity, error) {
	var err error
	var icebergClient *IcebergClient
	var s3Client *awsS3.Client

	if icebergClient, err = ProvideIcebergClient(ctx, config, logger); err != nil {
		return nil, fmt.Errorf("could not create iceberg client: %w", err)
	}

	if s3Client, err = gosoS3.ProvideClient(ctx, config, logger, "default"); err != nil {
		return nil, fmt.Errorf("could not create s3 client: %w", err)
	}

	return &ServiceFileIntegrity{
		logger:        logger.WithChannel("file_integrity"),
		icebergClient: icebergClient,
		s3Client:      s3Client,
	}, nil
}

type ServiceFileIntegrity struct {
	logger        log.Logger
	icebergClient *IcebergClient
	s3Client      *awsS3.Client
}

func (s *ServiceFileIntegrity) ListMissingFiles(ctx context.Context, database string, tableName string, snapshotID int64) ([]string, error) {
	var err error
	var filePaths []string
	var group *s3ListGroup
	var existingKeys funk.Set[string]

	if filePaths, err = s.icebergClient.ListSnapshotDataFilePaths(ctx, database, tableName, snapshotID); err != nil {
		return nil, fmt.Errorf("could not list data files for snapshot %d in table %s: %w", snapshotID, tableName, err)
	}

	if len(filePaths) == 0 {
		return []string{}, nil
	}

	if group, err = s.buildS3ListGroup(filePaths); err != nil {
		return nil, fmt.Errorf("could not group data files for s3 lookup: %w", err)
	}

	if existingKeys, err = s.listKeysByPrefix(ctx, group.bucket, group.prefix); err != nil {
		return nil, err
	}

	missing := make([]string, 0)
	for key, uri := range group.expectedByKey {
		if existingKeys.Contains(key) {
			continue
		}

		missing = append(missing, uri)
	}

	sort.Strings(missing)

	s.logger.Info(ctx, "checked %d data files for snapshot %d in table %s and found %d missing", len(filePaths), snapshotID, tableName, len(missing))

	return missing, nil
}

type s3ObjectLocation struct {
	bucket string
	key    string
	uri    string
}

type s3ListGroup struct {
	bucket        string
	prefix        string
	expectedByKey map[string]string
}

func (s *ServiceFileIntegrity) buildS3ListGroup(filePaths []string) (*s3ListGroup, error) {
	firstLocation, err := parseS3ObjectLocation(filePaths[0])
	if err != nil {
		return nil, err
	}

	prefix, err := dataPrefixFromKey(firstLocation.key)
	if err != nil {
		return nil, err
	}

	expectedByKey := make(map[string]string, len(filePaths))
	for _, filePath := range filePaths {
		location, err := parseS3ObjectLocation(filePath)
		if err != nil {
			return nil, err
		}

		if location.bucket != firstLocation.bucket {
			return nil, fmt.Errorf("found data files in multiple buckets: %q and %q", firstLocation.bucket, location.bucket)
		}

		if !strings.HasPrefix(location.key, prefix) {
			return nil, fmt.Errorf("data file %q does not use expected data prefix %q", filePath, prefix)
		}

		expectedByKey[location.key] = location.uri
	}

	return &s3ListGroup{
		bucket:        firstLocation.bucket,
		prefix:        prefix,
		expectedByKey: expectedByKey,
	}, nil
}

func (s *ServiceFileIntegrity) listKeysByPrefix(ctx context.Context, bucket, prefix string) (funk.Set[string], error) {
	paginator := awsS3.NewListObjectsV2Paginator(s.s3Client, &awsS3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})

	keys := funk.Set[string]{}
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("could not list s3 objects for s3://%s/%s: %w", bucket, prefix, err)
		}

		for _, object := range page.Contents {
			if object.Key == nil {
				continue
			}

			keys.Add(*object.Key)
		}
	}

	return keys, nil
}

func parseS3ObjectLocation(filePath string) (s3ObjectLocation, error) {
	parsedURL, err := url.Parse(filePath)
	if err != nil {
		return s3ObjectLocation{}, fmt.Errorf("could not parse s3 object location %q: %w", filePath, err)
	}

	switch parsedURL.Scheme {
	case "s3", "s3a", "s3n":
	default:
		return s3ObjectLocation{}, fmt.Errorf("unsupported object storage scheme %q for %q", parsedURL.Scheme, filePath)
	}

	bucket := parsedURL.Host
	if bucket == "" {
		return s3ObjectLocation{}, fmt.Errorf("missing bucket in s3 object location %q", filePath)
	}

	key := strings.TrimPrefix(parsedURL.Path, "/")
	if key == "" {
		return s3ObjectLocation{}, fmt.Errorf("missing key in s3 object location %q", filePath)
	}

	return s3ObjectLocation{
		bucket: bucket,
		key:    key,
		uri:    filePath,
	}, nil
}

func dataPrefixFromKey(key string) (string, error) {
	separator := "/data/"
	idx := strings.Index(key, separator)
	if idx == -1 {
		return "", fmt.Errorf("s3 object key %q does not contain %q", key, separator)
	}

	return key[:idx+len(separator)], nil
}
