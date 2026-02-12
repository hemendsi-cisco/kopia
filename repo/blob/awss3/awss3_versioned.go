package awss3

import (
	"context"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
)

// versionMetadata has metadata for a single BLOB version.
type versionMetadata struct {
	blob.Metadata

	// Versioning related information
	IsLatest       bool
	IsDeleteMarker bool
	Version        string
}

// versionMetadataCallback is called when processing the metadata for each blob version.
type versionMetadataCallback func(versionMetadata) error

// IsVersioned returns whether versioning is enabled in the store.
// It returns true even if versioning is enabled but currently suspended for the
// bucket. Notice that when object locking is enabled in a bucket, object
// versioning is enabled and cannot be suspended.
func (s *awsS3Storage) IsVersioned(ctx context.Context) (bool, error) {
	vi, err := s.cli.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{
		Bucket: aws.String(s.BucketName),
	})
	if err != nil {
		return false, errors.Wrapf(err, "could not get versioning info for %s", s.BucketName)
	}

	return vi.Status == types.BucketVersioningStatusEnabled || vi.Status == types.BucketVersioningStatusSuspended, nil
}

// getBlobVersions lists all the versions for the blob with the given ID.
func (s *awsS3Storage) getBlobVersions(ctx context.Context, prefix blob.ID, callback versionMetadataCallback) error {
	var foundBlobs bool

	if err := s.list(ctx, prefix, true, func(vm versionMetadata) error {
		foundBlobs = true

		return callback(vm)
	}); err != nil {
		return err
	}

	if !foundBlobs {
		return blob.ErrBlobNotFound
	}

	return nil
}

// listBlobVersions lists all versions for all the blobs with the given blob ID prefix.
func (s *awsS3Storage) listBlobVersions(ctx context.Context, prefix blob.ID, callback versionMetadataCallback) error {
	return s.list(ctx, prefix, false, callback)
}

func (s *awsS3Storage) list(ctx context.Context, prefix blob.ID, onlyMatching bool, callback versionMetadataCallback) error {
	objectPrefix := s.getObjectNameString(prefix)
	p := s3.NewListObjectVersionsPaginator(s.cli, &s3.ListObjectVersionsInput{
		Bucket: aws.String(s.BucketName),
		Prefix: aws.String(objectPrefix),
	})

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for p.HasMorePages() {
		output, err := p.NextPage(ctx)
		if err != nil {
			return errors.Wrapf(err, "could not list objects with prefix %q", objectPrefix)
		}

		entries := entriesFromVersionOutput(s.Prefix, output)
		sortVersionEntries(entries)

		for _, vm := range entries {
			if onlyMatching && vm.BlobID != prefix {
				return nil
			}

			if err := callback(vm); err != nil {
				return errors.Wrapf(err, "callback failed for %q", vm.BlobID)
			}
		}
	}

	return nil
}

func entriesFromVersionOutput(prefix string, output *s3.ListObjectVersionsOutput) []versionMetadata {
	entries := make([]versionMetadata, 0, len(output.Versions)+len(output.DeleteMarkers))

	for _, v := range output.Versions {
		entries = append(entries, versionEntryFromObjectVersion(prefix, v))
	}

	for _, v := range output.DeleteMarkers {
		entries = append(entries, versionEntryFromDeleteMarker(prefix, v))
	}

	return entries
}

func sortVersionEntries(entries []versionMetadata) {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].BlobID != entries[j].BlobID {
			return entries[i].BlobID < entries[j].BlobID
		}

		if entries[i].Timestamp.Equal(entries[j].Timestamp) {
			return entries[i].Version < entries[j].Version
		}

		return entries[i].Timestamp.After(entries[j].Timestamp)
	})
}

func toBlobID(blobName, prefix string) blob.ID {
	return blob.ID(strings.TrimPrefix(blobName, prefix))
}

func versionEntryFromObjectVersion(prefix string, ov types.ObjectVersion) versionMetadata {
	bm := blob.Metadata{
		BlobID:    toBlobID(aws.ToString(ov.Key), prefix),
		Length:    aws.ToInt64(ov.Size),
		Timestamp: aws.ToTime(ov.LastModified),
	}

	return versionMetadata{
		Metadata:       bm,
		IsLatest:       aws.ToBool(ov.IsLatest),
		IsDeleteMarker: false,
		Version:        aws.ToString(ov.VersionId),
	}
}

func versionEntryFromDeleteMarker(prefix string, dm types.DeleteMarkerEntry) versionMetadata {
	bm := blob.Metadata{
		BlobID:    toBlobID(aws.ToString(dm.Key), prefix),
		Length:    0,
		Timestamp: aws.ToTime(dm.LastModified),
	}

	return versionMetadata{
		Metadata:       bm,
		IsLatest:       aws.ToBool(dm.IsLatest),
		IsDeleteMarker: true,
		Version:        aws.ToString(dm.VersionId),
	}
}
