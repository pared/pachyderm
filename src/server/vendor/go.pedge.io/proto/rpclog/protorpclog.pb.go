// Code generated by protoc-gen-go.
// source: rpclog/protorpclog.proto
// DO NOT EDIT!

package protorpclog

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"
import google_protobuf "go.pedge.io/pb/go/google/protobuf"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type Call struct {
	Service  string                    `protobuf:"bytes,1,opt,name=service" json:"service,omitempty"`
	Method   string                    `protobuf:"bytes,2,opt,name=method" json:"method,omitempty"`
	Request  string                    `protobuf:"bytes,3,opt,name=request" json:"request,omitempty"`
	Response string                    `protobuf:"bytes,4,opt,name=response" json:"response,omitempty"`
	Error    string                    `protobuf:"bytes,5,opt,name=error" json:"error,omitempty"`
	Duration *google_protobuf.Duration `protobuf:"bytes,6,opt,name=duration" json:"duration,omitempty"`
}

func (m *Call) Reset()                    { *m = Call{} }
func (m *Call) String() string            { return proto.CompactTextString(m) }
func (*Call) ProtoMessage()               {}
func (*Call) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *Call) GetDuration() *google_protobuf.Duration {
	if m != nil {
		return m.Duration
	}
	return nil
}

func init() {
	proto.RegisterType((*Call)(nil), "protorpclog.Call")
}

var fileDescriptor0 = []byte{
	// 192 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0x4c, 0x8e, 0x4d, 0x8e, 0x82, 0x40,
	0x10, 0x85, 0xc3, 0x0c, 0x30, 0x4c, 0xb3, 0xeb, 0x4c, 0x26, 0x35, 0x2c, 0x26, 0xc6, 0x95, 0xab,
	0x26, 0xd1, 0x78, 0x02, 0x3d, 0x01, 0x37, 0xe0, 0xa7, 0x44, 0x92, 0x96, 0xc2, 0xea, 0xc6, 0xcb,
	0x79, 0x39, 0xb5, 0x1b, 0x88, 0xbb, 0xfa, 0xea, 0x7b, 0x2f, 0x79, 0x02, 0x78, 0xa8, 0x35, 0xb5,
	0xf9, 0xc0, 0x64, 0xc9, 0xdf, 0xca, 0xdd, 0x32, 0x7d, 0x7b, 0x65, 0xff, 0x2d, 0x51, 0xab, 0xd1,
	0xc7, 0xaa, 0xf1, 0x94, 0x37, 0x23, 0x97, 0xb6, 0xa3, 0xde, 0x87, 0xd7, 0xf7, 0x40, 0x84, 0x87,
	0x52, 0x6b, 0x09, 0xe2, 0xcb, 0x20, 0xdf, 0xba, 0x1a, 0x21, 0x58, 0x05, 0x9b, 0xef, 0x62, 0x46,
	0xf9, 0x2b, 0xe2, 0x0b, 0xda, 0x33, 0x35, 0xf0, 0xe1, 0xc4, 0x44, 0xaf, 0x06, 0xe3, 0x75, 0x44,
	0x63, 0xe1, 0xd3, 0x37, 0x26, 0x94, 0x99, 0x48, 0x18, 0xcd, 0x40, 0xbd, 0x41, 0x08, 0x9d, 0x5a,
	0x58, 0xfe, 0x88, 0x08, 0x99, 0x89, 0x21, 0x72, 0xc2, 0x83, 0xdc, 0x8b, 0x64, 0x1e, 0x06, 0xf1,
	0x53, 0xa4, 0xdb, 0x3f, 0xe5, 0x97, 0xab, 0x79, 0xb9, 0x3a, 0x4e, 0x81, 0x62, 0x89, 0x56, 0xb1,
	0x93, 0xbb, 0x47, 0x00, 0x00, 0x00, 0xff, 0xff, 0xc7, 0x6b, 0x60, 0x07, 0x0d, 0x01, 0x00, 0x00,
}
