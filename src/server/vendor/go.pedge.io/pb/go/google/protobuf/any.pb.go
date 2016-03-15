// Code generated by protoc-gen-go.
// source: google/protobuf/any.proto
// DO NOT EDIT!

package google_protobuf

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
const _ = proto.ProtoPackageIsVersion1

// `Any` contains an arbitrary serialized message along with a URL
// that describes the type of the serialized message.
//
//
// JSON
// ====
// The JSON representation of an `Any` value uses the regular
// representation of the deserialized, embedded message, with an
// additional field `@type` which contains the type URL. Example:
//
//     package google.profile;
//     message Person {
//       string first_name = 1;
//       string last_name = 2;
//     }
//
//     {
//       "@type": "type.googleapis.com/google.profile.Person",
//       "firstName": <string>,
//       "lastName": <string>
//     }
//
// If the embedded message type is well-known and has a custom JSON
// representation, that representation will be embedded adding a field
// `value` which holds the custom JSON in addition to the `@type`
// field. Example (for message [google.protobuf.Duration][]):
//
//     {
//       "@type": "type.googleapis.com/google.protobuf.Duration",
//       "value": "1.212s"
//     }
//
type Any struct {
	// A URL/resource name whose content describes the type of the
	// serialized message.
	//
	// For URLs which use the schema `http`, `https`, or no schema, the
	// following restrictions and interpretations apply:
	//
	// * If no schema is provided, `https` is assumed.
	// * The last segment of the URL's path must represent the fully
	//   qualified name of the type (as in `path/google.protobuf.Duration`).
	// * An HTTP GET on the URL must yield a [google.protobuf.Type][]
	//   value in binary format, or produce an error.
	// * Applications are allowed to cache lookup results based on the
	//   URL, or have them precompiled into a binary to avoid any
	//   lookup. Therefore, binary compatibility needs to be preserved
	//   on changes to types. (Use versioned type names to manage
	//   breaking changes.)
	//
	// Schemas other than `http`, `https` (or the empty schema) might be
	// used with implementation specific semantics.
	//
	TypeUrl string `protobuf:"bytes,1,opt,name=type_url" json:"type_url,omitempty"`
	// Must be valid serialized data of the above specified type.
	Value []byte `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
}

func (m *Any) Reset()                    { *m = Any{} }
func (m *Any) String() string            { return proto.CompactTextString(m) }
func (*Any) ProtoMessage()               {}
func (*Any) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func init() {
	proto.RegisterType((*Any)(nil), "google.protobuf.Any")
}

var fileDescriptor0 = []byte{
	// 150 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0xe2, 0x92, 0x4c, 0xcf, 0xcf, 0x4f,
	0xcf, 0x49, 0xd5, 0x2f, 0x28, 0xca, 0x2f, 0xc9, 0x4f, 0x2a, 0x4d, 0xd3, 0x4f, 0xcc, 0xab, 0xd4,
	0x03, 0x73, 0x84, 0xf8, 0x21, 0x52, 0x7a, 0x30, 0x29, 0x25, 0x35, 0x2e, 0x66, 0xc7, 0xbc, 0x4a,
	0x21, 0x01, 0x2e, 0x8e, 0x92, 0xca, 0x82, 0xd4, 0xf8, 0xd2, 0xa2, 0x1c, 0x09, 0x46, 0x05, 0x46,
	0x0d, 0x4e, 0x21, 0x5e, 0x2e, 0xd6, 0xb2, 0xc4, 0x9c, 0xd2, 0x54, 0x09, 0x26, 0x20, 0x97, 0xc7,
	0xc9, 0x9b, 0x4b, 0x38, 0x39, 0x3f, 0x57, 0x0f, 0x4d, 0xbb, 0x13, 0x07, 0x50, 0x73, 0x00, 0x88,
	0x13, 0xc0, 0xb8, 0x80, 0x91, 0x71, 0x11, 0x13, 0xb3, 0x7b, 0x80, 0xd3, 0x2a, 0x26, 0x39, 0x77,
	0x88, 0xb2, 0x00, 0xa8, 0x32, 0xbd, 0xf0, 0xd4, 0x9c, 0x1c, 0xef, 0xbc, 0xfc, 0xf2, 0xbc, 0x10,
	0xa0, 0x25, 0xc5, 0x49, 0x6c, 0x60, 0xfd, 0xc6, 0x80, 0x00, 0x00, 0x00, 0xff, 0xff, 0xa0, 0x4c,
	0x2d, 0x0d, 0xa9, 0x00, 0x00, 0x00,
}
