// Code generated by protoc-gen-go. DO NOT EDIT.
// source: user.proto

package pb

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type User struct {
	Id                   string   `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Name                 string   `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	Email                string   `protobuf:"bytes,3,opt,name=email,proto3" json:"email,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *User) Reset()         { *m = User{} }
func (m *User) String() string { return proto.CompactTextString(m) }
func (*User) ProtoMessage()    {}
func (*User) Descriptor() ([]byte, []int) {
	return fileDescriptor_116e343673f7ffaf, []int{0}
}

func (m *User) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_User.Unmarshal(m, b)
}
func (m *User) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_User.Marshal(b, m, deterministic)
}
func (m *User) XXX_Merge(src proto.Message) {
	xxx_messageInfo_User.Merge(m, src)
}
func (m *User) XXX_Size() int {
	return xxx_messageInfo_User.Size(m)
}
func (m *User) XXX_DiscardUnknown() {
	xxx_messageInfo_User.DiscardUnknown(m)
}

var xxx_messageInfo_User proto.InternalMessageInfo

func (m *User) GetId() string {
	if m != nil {
		return m.Id
	}
	return ""
}

func (m *User) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *User) GetEmail() string {
	if m != nil {
		return m.Email
	}
	return ""
}

type UserResultStream struct {
	Status               string   `protobuf:"bytes,1,opt,name=status,proto3" json:"status,omitempty"`
	User                 *User    `protobuf:"bytes,2,opt,name=user,proto3" json:"user,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *UserResultStream) Reset()         { *m = UserResultStream{} }
func (m *UserResultStream) String() string { return proto.CompactTextString(m) }
func (*UserResultStream) ProtoMessage()    {}
func (*UserResultStream) Descriptor() ([]byte, []int) {
	return fileDescriptor_116e343673f7ffaf, []int{1}
}

func (m *UserResultStream) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_UserResultStream.Unmarshal(m, b)
}
func (m *UserResultStream) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_UserResultStream.Marshal(b, m, deterministic)
}
func (m *UserResultStream) XXX_Merge(src proto.Message) {
	xxx_messageInfo_UserResultStream.Merge(m, src)
}
func (m *UserResultStream) XXX_Size() int {
	return xxx_messageInfo_UserResultStream.Size(m)
}
func (m *UserResultStream) XXX_DiscardUnknown() {
	xxx_messageInfo_UserResultStream.DiscardUnknown(m)
}

var xxx_messageInfo_UserResultStream proto.InternalMessageInfo

func (m *UserResultStream) GetStatus() string {
	if m != nil {
		return m.Status
	}
	return ""
}

func (m *UserResultStream) GetUser() *User {
	if m != nil {
		return m.User
	}
	return nil
}

type Users struct {
	User                 []*User  `protobuf:"bytes,1,rep,name=user,proto3" json:"user,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Users) Reset()         { *m = Users{} }
func (m *Users) String() string { return proto.CompactTextString(m) }
func (*Users) ProtoMessage()    {}
func (*Users) Descriptor() ([]byte, []int) {
	return fileDescriptor_116e343673f7ffaf, []int{2}
}

func (m *Users) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Users.Unmarshal(m, b)
}
func (m *Users) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Users.Marshal(b, m, deterministic)
}
func (m *Users) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Users.Merge(m, src)
}
func (m *Users) XXX_Size() int {
	return xxx_messageInfo_Users.Size(m)
}
func (m *Users) XXX_DiscardUnknown() {
	xxx_messageInfo_Users.DiscardUnknown(m)
}

var xxx_messageInfo_Users proto.InternalMessageInfo

func (m *Users) GetUser() []*User {
	if m != nil {
		return m.User
	}
	return nil
}

func init() {
	proto.RegisterType((*User)(nil), "pb.User")
	proto.RegisterType((*UserResultStream)(nil), "pb.UserResultStream")
	proto.RegisterType((*Users)(nil), "pb.Users")
}

func init() { proto.RegisterFile("user.proto", fileDescriptor_116e343673f7ffaf) }

var fileDescriptor_116e343673f7ffaf = []byte{
	// 248 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x84, 0x91, 0x4f, 0x4b, 0xc3, 0x40,
	0x10, 0xc5, 0xd9, 0xfc, 0xa9, 0xed, 0x14, 0x8a, 0x0e, 0x45, 0x82, 0x28, 0xd4, 0x80, 0x90, 0x53,
	0x2c, 0xf1, 0xe0, 0x55, 0x7b, 0xf2, 0xdc, 0xa2, 0x07, 0x6f, 0x59, 0x33, 0x60, 0xa0, 0x31, 0x61,
	0x67, 0xe3, 0xa7, 0xf3, 0xc3, 0xc9, 0x4e, 0xd6, 0x18, 0xf0, 0xe0, 0xed, 0xbd, 0x97, 0xdf, 0x7b,
	0xd9, 0x65, 0x01, 0x7a, 0x26, 0x93, 0x77, 0xa6, 0xb5, 0x2d, 0x06, 0x9d, 0x4e, 0x1f, 0x20, 0x7a,
	0x66, 0x32, 0xb8, 0x82, 0xa0, 0xae, 0x12, 0xb5, 0x51, 0xd9, 0x62, 0x1f, 0xd4, 0x15, 0x22, 0x44,
	0x1f, 0x65, 0x43, 0x49, 0x20, 0x89, 0x68, 0x5c, 0x43, 0x4c, 0x4d, 0x59, 0x1f, 0x93, 0x50, 0xc2,
	0xc1, 0xa4, 0x4f, 0x70, 0xea, 0x16, 0xf6, 0xc4, 0xfd, 0xd1, 0x1e, 0xac, 0xa1, 0xb2, 0xc1, 0x73,
	0x98, 0xb1, 0x2d, 0x6d, 0xcf, 0x7e, 0xd1, 0x3b, 0xbc, 0x84, 0xc8, 0xfd, 0x5f, 0x56, 0x97, 0xc5,
	0x3c, 0xef, 0x74, 0x2e, 0x5d, 0x49, 0xd3, 0x1b, 0x88, 0x9d, 0xfb, 0xc5, 0xd4, 0x26, 0xfc, 0x8b,
	0x15, 0x5f, 0x0a, 0x96, 0xce, 0x1e, 0xc8, 0x7c, 0xd6, 0x6f, 0x84, 0x57, 0x70, 0xf2, 0x58, 0x55,
	0x72, 0x8b, 0x11, 0xbd, 0x18, 0x15, 0x16, 0xb0, 0xf2, 0x9f, 0x5f, 0xc8, 0xe8, 0x96, 0x69, 0x42,
	0xad, 0xc7, 0xe9, 0xc9, 0xe9, 0xb7, 0x0a, 0xaf, 0x61, 0xee, 0x3b, 0x3c, 0xa1, 0x17, 0x3f, 0x8a,
	0x33, 0x85, 0xf7, 0x70, 0xe6, 0x91, 0xa1, 0xb5, 0x6b, 0xed, 0xfb, 0x7f, 0xcb, 0x99, 0xda, 0xaa,
	0x5d, 0xfc, 0x1a, 0xde, 0x76, 0x5a, 0xcf, 0xe4, 0x0d, 0xee, 0xbe, 0x03, 0x00, 0x00, 0xff, 0xff,
	0xf5, 0x59, 0x77, 0xc9, 0x91, 0x01, 0x00, 0x00,
}