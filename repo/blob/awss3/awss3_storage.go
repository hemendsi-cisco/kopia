// Package awss3 implements Storage based on an S3 bucket using AWS SDK v2.
package awss3

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/retrying"
)

const (
	awsS3StorageType = "awss3"
	latestVersionID  = ""
)

type awsS3Storage struct {
	Options
	blob.DefaultProviderImplementation

	cli *s3.Client

	storageConfig *StorageConfig
}

func (s *awsS3Storage) GetBlob(ctx context.Context, b blob.ID, offset, length int64, output blob.OutputBuffer) error {
	return s.getBlobWithVersion(ctx, b, latestVersionID, offset, length, output)
}

// getBlobWithVersion returns full or partial contents of a blob with given ID and version.
func (s *awsS3Storage) getBlobWithVersion(ctx context.Context, b blob.ID, version string, offset, length int64, output blob.OutputBuffer) error {
	output.Reset()

	attempt := func() error {
		input := &s3.GetObjectInput{
			Bucket: aws.String(s.BucketName),
			Key:    aws.String(s.getObjectNameString(b)),
		}
		if version != "" {
			input.VersionId = aws.String(version)
		}

		if length > 0 {
			input.Range = aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
		}

		if length == 0 {
			// zero-length ranges require special handling, set non-zero range and
			// we won't be trying to read the response anyway.
			input.Range = aws.String("bytes=0-1")
		}

		o, err := s.cli.GetObject(ctx, input)
		if err != nil {
			return errors.Wrap(err, "GetObject")
		}

		defer o.Body.Close() //nolint:errcheck

		if length == 0 {
			return nil
		}

		return iocopy.JustCopy(output, o.Body)
	}

	if err := attempt(); err != nil {
		return translateError(err)
	}

	//nolint:wrapcheck
	return blob.EnsureLengthExactly(output.Length(), length)
}

func isInvalidCredentials(err error) bool {
	if err == nil {
		return false
	}

	if strings.Contains(err.Error(), blob.InvalidCredentialsErrStr) {
		return true
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "ExpiredToken", "InvalidAccessKeyId":
			return true
		}
	}

	return false
}

func translateError(err error) error {
	if isInvalidCredentials(err) {
		return blob.ErrInvalidCredentials
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NoSuchKey", "NotFound":
			return blob.ErrBlobNotFound
		case "InvalidRange":
			return blob.ErrInvalidRange
		}
	}

	var respErr *smithyhttp.ResponseError
	if errors.As(err, &respErr) {
		switch respErr.HTTPStatusCode() {
		case http.StatusNotFound:
			return blob.ErrBlobNotFound
		case http.StatusRequestedRangeNotSatisfiable:
			return blob.ErrInvalidRange
		}
	}

	return err
}

func (s *awsS3Storage) GetMetadata(ctx context.Context, b blob.ID) (blob.Metadata, error) {
	vm, err := s.getVersionMetadata(ctx, b, "")

	return vm.Metadata, err
}

func (s *awsS3Storage) getVersionMetadata(ctx context.Context, b blob.ID, version string) (versionMetadata, error) {
	input := &s3.HeadObjectInput{
		Bucket: aws.String(s.BucketName),
		Key:    aws.String(s.getObjectNameString(b)),
	}
	if version != "" {
		input.VersionId = aws.String(version)
	}

	oi, err := s.cli.HeadObject(ctx, input)
	if err != nil {
		return versionMetadata{}, errors.Wrap(translateError(err), "HeadObject")
	}

	return versionMetadata{
		Metadata: blob.Metadata{
			BlobID:    b,
			Length:    aws.ToInt64(oi.ContentLength),
			Timestamp: aws.ToTime(oi.LastModified),
		},
		Version: aws.ToString(oi.VersionId),
	}, nil
}

func (s *awsS3Storage) PutBlob(ctx context.Context, b blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	switch {
	case opts.DoNotRecreate:
		return errors.Wrap(blob.ErrUnsupportedPutBlobOption, "do-not-recreate")
	case !opts.SetModTime.IsZero():
		return blob.ErrSetTimeUnsupported
	}

	_, err := s.putBlob(ctx, b, data, opts)

	if opts.GetModTime != nil {
		bm, err2 := s.GetMetadata(ctx, b)
		if err2 != nil {
			return err2
		}

		*opts.GetModTime = bm.Timestamp
	}

	return err
}

func (s *awsS3Storage) putBlob(ctx context.Context, b blob.ID, data blob.Bytes, opts blob.PutOptions) (versionMetadata, error) {
	var (
		storageClass    = s.storageConfig.getStorageClassForBlobID(b)
		retentionMode   types.ObjectLockMode
		retainUntilDate time.Time
	)

	if opts.RetentionPeriod != 0 {
		switch opts.RetentionMode {
		case blob.Governance:
			retentionMode = types.ObjectLockModeGovernance
		case blob.Compliance:
			retentionMode = types.ObjectLockModeCompliance
		default:
			return versionMetadata{}, errors.Errorf("invalid retention mode: %q", opts.RetentionMode)
		}

		retainUntilDate = clock.Now().Add(opts.RetentionPeriod).UTC()
	}

	reader := data.Reader()
	defer reader.Close() //nolint:errcheck

	input := &s3.PutObjectInput{
		Bucket:        aws.String(s.BucketName),
		Key:           aws.String(s.getObjectNameString(b)),
		Body:          reader,
		ContentLength: aws.Int64(int64(data.Length())),
		ContentType:   aws.String("application/x-kopia"),
		StorageClass:  types.StorageClass(storageClass),
	}

	if opts.RetentionPeriod != 0 {
		input.ObjectLockMode = retentionMode
		input.ObjectLockRetainUntilDate = aws.Time(retainUntilDate)
		// Use SHA-256 checksum for object locking without relying on MD5.
		input.ChecksumAlgorithm = types.ChecksumAlgorithmSha256
	}

	uploadInfo, err := s.cli.PutObject(ctx, input)
	if isInvalidCredentials(err) {
		return versionMetadata{}, blob.ErrInvalidCredentials
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "InvalidRequest" &&
		strings.Contains(strings.ToLower(apiErr.ErrorMessage()), "content-md5") {
		return versionMetadata{}, err //nolint:wrapcheck
	}

	if err != nil {
		return versionMetadata{}, err //nolint:wrapcheck
	}

	return versionMetadata{
		Metadata: blob.Metadata{
			BlobID:    b,
			Length:    int64(data.Length()),
			Timestamp: clock.Now(),
		},
		Version: aws.ToString(uploadInfo.VersionId),
	}, nil
}

func (s *awsS3Storage) DeleteBlob(ctx context.Context, b blob.ID) error {
	_, err := s.cli.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.BucketName),
		Key:    aws.String(s.getObjectNameString(b)),
	})
	if errors.Is(translateError(err), blob.ErrBlobNotFound) {
		return nil
	}

	return translateError(err)
}

func (s *awsS3Storage) ExtendBlobRetention(ctx context.Context, b blob.ID, opts blob.ExtendOptions) error {
	var retentionMode types.ObjectLockRetentionMode
	switch opts.RetentionMode {
	case blob.Governance:
		retentionMode = types.ObjectLockRetentionModeGovernance
	case blob.Compliance:
		retentionMode = types.ObjectLockRetentionModeCompliance
	default:
		return errors.Errorf("invalid retention mode: %q", opts.RetentionMode)
	}

	retainUntilDate := clock.Now().Add(opts.RetentionPeriod).UTC()

	_, err := s.cli.PutObjectRetention(ctx, &s3.PutObjectRetentionInput{
		Bucket: aws.String(s.BucketName),
		Key:    aws.String(s.getObjectNameString(b)),
		Retention: &types.ObjectLockRetention{
			RetainUntilDate: aws.Time(retainUntilDate),
			Mode:            retentionMode,
		},
	})
	if err != nil {
		return errors.Wrap(err, "unable to extend retention period")
	}

	return nil
}

func (s *awsS3Storage) getObjectNameString(b blob.ID) string {
	return s.Prefix + string(b)
}

func (s *awsS3Storage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	p := s3.NewListObjectsV2Paginator(s.cli, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.BucketName),
		Prefix: aws.String(s.getObjectNameString(prefix)),
	})

	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			if isInvalidCredentials(err) {
				return blob.ErrInvalidCredentials
			}

			return err
		}

		for _, o := range page.Contents {
			bm := blob.Metadata{
				BlobID:    blob.ID(strings.TrimPrefix(aws.ToString(o.Key), s.Prefix)),
				Length:    aws.ToInt64(o.Size),
				Timestamp: aws.ToTime(o.LastModified),
			}

			if bm.BlobID == ConfigName {
				continue
			}

			if err := callback(bm); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *awsS3Storage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   awsS3StorageType,
		Config: &s.Options,
	}
}

func (s *awsS3Storage) String() string {
	return fmt.Sprintf("awss3://%v/%v", s.BucketName, s.Prefix)
}

func (s *awsS3Storage) DisplayName() string {
	return fmt.Sprintf("AWS S3: %v %v", s.Endpoint, s.BucketName)
}

func getCustomTransport(opt *Options) (*http.Transport, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone() //nolint:forcetypeassert

	return transport, nil
}

// New creates new S3-backed storage with specified options:
//
// - the 'BucketName' field is required and all other parameters are optional.
func New(ctx context.Context, opt *Options, isCreate bool) (blob.Storage, error) {
	_ = isCreate

	st, err := newStorage(ctx, opt)
	if err != nil {
		return nil, err
	}

	s, err := maybePointInTimeStore(ctx, st, opt.PointInTime)
	if err != nil {
		return nil, err
	}

	return retrying.NewWrapper(s), nil
}

func newStorage(ctx context.Context, opt *Options) (*awsS3Storage, error) {
	if opt.BucketName == "" {
		return nil, errors.New("bucket name must be specified")
	}

	transport, err := getCustomTransport(opt)
	if err != nil {
		return nil, err
	}

	region := opt.Region
	if region == "" {
		region = "us-east-1"
	}

	loadOptions := []func(*config.LoadOptions) error{
		config.WithRegion(region),
		config.WithHTTPClient(&http.Client{Transport: transport}),
	}

	if opt.AccessKeyID != "" || opt.SecretAccessKey != "" || opt.SessionToken != "" {
		loadOptions = append(loadOptions, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(opt.AccessKeyID, opt.SecretAccessKey, opt.SessionToken),
		))
	}

	cfg, err := config.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return nil, errors.Wrap(err, "unable to load AWS configuration")
	}

	if opt.RoleARN != "" {
		roleRegion := opt.RoleRegion
		if roleRegion == "" {
			roleRegion = region
		}

		stsCfg := cfg
		stsCfg.Region = roleRegion
		if opt.RoleEndpoint != "" {
			roleEndpointURL, err := buildEndpointURL(opt.RoleEndpoint)
			if err != nil {
				return nil, err
			}
			stsCfg.BaseEndpoint = aws.String(roleEndpointURL)
		}

		stsClient := sts.NewFromConfig(stsCfg)
		assumeRoleProvider := stscreds.NewAssumeRoleProvider(stsClient, opt.RoleARN, func(o *stscreds.AssumeRoleOptions) {
			if opt.SessionName != "" {
				o.RoleSessionName = opt.SessionName
			}
			if opt.RoleDuration.Duration != 0 {
				o.Duration = opt.RoleDuration.Duration
			}
		})

		cfg.Credentials = aws.NewCredentialsCache(assumeRoleProvider)
	}

	endpointURL, err := buildEndpointURL(opt.Endpoint)
	if err != nil {
		return nil, err
	}

	cli := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if opt.Endpoint != "" {
			o.BaseEndpoint = aws.String(endpointURL)
		}
		o.UsePathStyle = shouldUsePathStyle(opt.Endpoint)
		o.UseAccelerate = opt.UseAccelerate
	})

	s := awsS3Storage{
		Options:       *opt,
		cli:           cli,
		storageConfig: &StorageConfig{},
	}

	var scOutput gather.WriteBuffer

	if getBlobErr := s.GetBlob(ctx, ConfigName, 0, -1, &scOutput); getBlobErr == nil {
		if scErr := s.storageConfig.Load(scOutput.Bytes().Reader()); scErr != nil {
			return nil, errors.Wrapf(scErr, "error parsing storage config for bucket %q", opt.BucketName)
		}
	} else if !errors.Is(getBlobErr, blob.ErrBlobNotFound) {
		return nil, errors.Wrapf(getBlobErr, "error retrieving storage config from bucket %q", opt.BucketName)
	}

	return &s, nil
}

func buildEndpointURL(endpoint string) (string, error) {
	if endpoint == "" {
		return "", errors.New("endpoint must be specified")
	}

	if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
		return endpoint, nil
	}

	return "https://" + endpoint, nil
}

func shouldUsePathStyle(endpoint string) bool {
	if endpoint == "" {
		return false
	}

	if strings.Contains(endpoint, "amazonaws.com") {
		return false
	}

	return true
}

func init() {
	blob.AddSupportedStorage(awsS3StorageType, Options{}, New)
}
