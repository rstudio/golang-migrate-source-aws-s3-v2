// The contents of this package are based on
// https://github.com/golang-migrate/migrate/tree/331a15d92a86e002ce5043e788cc2ca287ab0cc2/source/aws_s3
// with the intention of hopefully proposing that it be added to
// golang-migrate/migrate.
package awss3v2

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/golang-migrate/migrate/v4/source"
)

var (
	_ctx context.Context = context.TODO()
)

func init() {
	source.Register("s3", &s3Driver{})
}

// SetContext allows setting the context used internally since the
// golang-migrate/migrate/v4/source Driver interface does not
// require a context.Context argument while the inner aws-sdk-go-v2
// functions do. No effort is made to lock writes to this internal
// context, so thread safety is very much punted.
func SetContext(ctx context.Context) {
	_ctx = ctx
}

type S3er interface {
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

type s3Driver struct {
	s3client   S3er
	cfg        *Config
	migrations *source.Migrations
}

type Config struct {
	Bucket string
	Prefix string
}

func (s *s3Driver) Open(folder string) (source.Driver, error) {
	cfg, err := parseURI(folder)
	if err != nil {
		return nil, err
	}

	awsCfg, err := config.LoadDefaultConfig(_ctx)
	if err != nil {
		return nil, err
	}

	return WithInstance(s3.NewFromConfig(awsCfg), cfg)
}

func WithInstance(s3client S3er, cfg *Config) (source.Driver, error) {
	driver := &s3Driver{
		cfg:        cfg,
		s3client:   s3client,
		migrations: source.NewMigrations(),
	}

	if err := driver.loadMigrations(); err != nil {
		return nil, err
	}

	return driver, nil
}

func parseURI(uri string) (*Config, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	prefix := strings.Trim(u.Path, "/")
	if prefix != "" {
		prefix += "/"
	}

	return &Config{
		Bucket: u.Host,
		Prefix: prefix,
	}, nil
}

func (s *s3Driver) loadMigrations() error {
	output, err := s.s3client.ListObjectsV2(_ctx, &s3.ListObjectsV2Input{
		Bucket:    aws.String(s.cfg.Bucket),
		Prefix:    aws.String(s.cfg.Prefix),
		Delimiter: aws.String("/"),
	})
	if err != nil {
		return err
	}

	for _, object := range output.Contents {
		_, fileName := path.Split(aws.ToString(object.Key))

		m, err := source.DefaultParse(fileName)
		if err != nil {
			continue
		}

		if !s.migrations.Append(m) {
			return fmt.Errorf("unable to parse file %v", aws.ToString(object.Key))
		}
	}

	return nil
}

func (s *s3Driver) Close() error {
	return nil
}

func (s *s3Driver) First() (uint, error) {
	v, ok := s.migrations.First()
	if !ok {
		return 0, os.ErrNotExist
	}

	return v, nil
}

func (s *s3Driver) Prev(version uint) (uint, error) {
	v, ok := s.migrations.Prev(version)
	if !ok {
		return 0, os.ErrNotExist
	}

	return v, nil
}

func (s *s3Driver) Next(version uint) (uint, error) {
	v, ok := s.migrations.Next(version)
	if !ok {
		return 0, os.ErrNotExist
	}

	return v, nil
}

func (s *s3Driver) ReadUp(version uint) (io.ReadCloser, string, error) {
	if m, ok := s.migrations.Up(version); ok {
		return s.open(m)
	}

	return nil, "", os.ErrNotExist
}

func (s *s3Driver) ReadDown(version uint) (io.ReadCloser, string, error) {
	if m, ok := s.migrations.Down(version); ok {
		return s.open(m)
	}

	return nil, "", os.ErrNotExist
}

func (s *s3Driver) open(m *source.Migration) (io.ReadCloser, string, error) {
	key := path.Join(s.cfg.Prefix, m.Raw)
	object, err := s.s3client.GetObject(_ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		return nil, "", err
	}

	return object.Body, m.Identifier, nil
}
