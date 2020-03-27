package chunk

import (
	"bytes"
	"context"
	"math/rand"
	"path"

	"github.com/gogo/protobuf/proto"
	"github.com/pachyderm/pachyderm/src/server/pkg/obj"
	"github.com/pachyderm/pachyderm/src/server/pkg/storage/gc"
)

const (
	// KB is Kilobyte.
	KB = 1024
	// MB is Megabyte.
	MB = 1024 * KB
)

type deleter struct {
	objC obj.Client
}

func (d *deleter) Delete(ctx context.Context, chunks []string) error {
	for _, chunk := range chunks {
		if err := d.objC.Delete(ctx, path.Join(prefix, chunk)); err != nil {
			return err
		}
	}
	return nil
}

// (bryce) this needs to be refactored a bit with the obj package.
func NewLocalStorage(objC obj.Client, opts ...StorageOption) (*Storage, error) {
	gcC, err := gc.NewLocalServer(&deleter{objC: objC})
	if err != nil {
		return nil, err
	}
	return NewStorage(objC, gcC, opts...), nil
}

// WithLocalStorage constructs a local storage instance for testing during the lifetime of
// the callback.
func WithLocalStorage(f func(obj.Client, *Storage) error, opts ...StorageOption) error {
	return obj.WithLocalClient(func(objC obj.Client) error {
		return gc.WithLocalServer(&deleter{objC: objC}, func(gcC gc.Client) error {
			return f(objC, NewStorage(objC, gcC))
		})
	})
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

// RandSeq generates a random sequence of data (n is number of bytes)
func RandSeq(n int) []byte {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return []byte(string(b))
}

// Reference creates a data reference for the full chunk referenced by a data reference.
func Reference(dataRef *DataRef) *DataRef {
	chunkRef := &DataRef{}
	chunkRef.ChunkInfo = dataRef.ChunkInfo
	chunkRef.SizeBytes = dataRef.ChunkInfo.SizeBytes
	return chunkRef
}

func joinAnnotations(as []*Annotation, a *Annotation) []*Annotation {
	if as != nil {
		lastA := as[len(as)-1]
		// If the annotation being added is the same as the
		// last, then they are merged.
		if lastA.Data == a.Data {
			if lastA.tags != nil && a.tags != nil {
				lastA.buf.Write(a.buf.Bytes())
				if lastA.tags != nil {
					lastA.tags = joinTags(lastA.tags, a.tags)
				}
				return as
			} else if lastA.drs != nil && a.drs != nil {
				return as
			}
		}
	}
	return append(as, a)
}

func joinTags(ts1, ts2 []*Tag) []*Tag {
	if ts1 != nil {
		lastT := ts1[len(ts1)-1]
		if lastT.Id == ts2[0].Id {
			lastT.SizeBytes += ts2[0].SizeBytes
			ts2 = ts2[1:]
		}
	}
	return append(ts1, ts2...)
}

func splitAnnotation(a *Annotation, size int) (*Annotation, *Annotation) {
	a1 := copyAnnotation(a)
	a2 := copyAnnotation(a)
	if a.buf != nil {
		a1.buf = bytes.NewBuffer(a.buf.Bytes()[:size])
		a2.buf = bytes.NewBuffer(a.buf.Bytes()[size:])
	}
	if a.tags != nil {
		a1.tags, a2.tags = splitTags(a.tags, size)
	}
	return a1, a2
}

func copyAnnotation(a *Annotation) *Annotation {
	copyA := &Annotation{Data: a.Data}
	if a.NextDataRef != nil {
		copyA.NextDataRef = &DataRef{}
	}
	if a.buf != nil {
		copyA.buf = &bytes.Buffer{}
	}
	return copyA
}

func splitTags(ts []*Tag, size int) ([]*Tag, []*Tag) {
	var ts1, ts2 []*Tag
	for _, t := range ts {
		ts2 = append(ts2, proto.Clone(t).(*Tag))
	}
	for {
		if int(ts2[0].SizeBytes) >= size {
			t := proto.Clone(ts2[0]).(*Tag)
			t.SizeBytes = int64(size)
			ts1 = append(ts1, t)
			ts2[0].SizeBytes -= int64(size)
			break
		}
		size -= int(ts2[0].SizeBytes)
		ts1 = append(ts1, ts2[0])
		ts2 = ts2[1:]
	}
	return ts1, ts2
}
