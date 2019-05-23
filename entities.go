package main

import (
	"github.com/valyala/fasthttp"
	"reflect"
	"strconv"
	"unsafe"
)

type User struct {
	Id          uint32
	Email       string
	FirstName   string
	LastName    string
	Gender      string
	BirthDate   int
	VisitsIndex []*Visit
}

type Location struct {
	Place       string
	Country     string
	City        string
	Id          uint32
	Distance    uint32
	VisitsIndex []*Visit
}

type Visit struct {
	Id        uint32
	Location  *Location
	User      *User
	VisitedAt int
	Mark      int8
}

func (u *User) Serialize(entityBuffer []byte) []byte {
	entityBuffer = append(entityBuffer, `{"first_name":"`...)
	entityBuffer = append(entityBuffer, u.FirstName...)
	entityBuffer = append(entityBuffer, `","last_name":"`...)
	entityBuffer = append(entityBuffer, u.LastName...)
	entityBuffer = append(entityBuffer, `","gender":"`...)
	entityBuffer = append(entityBuffer, u.Gender...)
	entityBuffer = append(entityBuffer, `","email":"`...)
	entityBuffer = append(entityBuffer, u.Email...)
	entityBuffer = append(entityBuffer, `","birth_date":`...)
	entityBuffer = strconv.AppendInt(entityBuffer, int64(u.BirthDate), 10)
	entityBuffer = append(entityBuffer, `,"id":`...)
	entityBuffer = fasthttp.AppendUint(entityBuffer, int(u.Id))
	entityBuffer = append(entityBuffer, '}')

	return entityBuffer
}

func (l *Location) Serialize(entityBuffer []byte) []byte {
	entityBuffer = append(entityBuffer, `{"distance":`...)
	entityBuffer = fasthttp.AppendUint(entityBuffer, int(l.Distance))
	entityBuffer = append(entityBuffer, `,"city":"`...)
	entityBuffer = append(entityBuffer, l.City...)
	entityBuffer = append(entityBuffer, `","country":"`...)
	entityBuffer = append(entityBuffer, l.Country...)
	entityBuffer = append(entityBuffer, `","place":"`...)
	entityBuffer = append(entityBuffer, l.Place...)
	entityBuffer = append(entityBuffer, `","id":`...)
	entityBuffer = fasthttp.AppendUint(entityBuffer, int(l.Id))
	entityBuffer = append(entityBuffer, '}')

	return entityBuffer
}

func (v *Visit) Serialize(entityBuffer []byte) []byte {
	entityBuffer = append(entityBuffer, `{"mark":`...)
	entityBuffer = fasthttp.AppendUint(entityBuffer, int(v.Mark))
	entityBuffer = append(entityBuffer, `,"visited_at":`...)
	entityBuffer = fasthttp.AppendUint(entityBuffer, int(v.VisitedAt))
	entityBuffer = append(entityBuffer, `,"user":`...)
	entityBuffer = fasthttp.AppendUint(entityBuffer, int(v.User.Id))
	entityBuffer = append(entityBuffer, `,"id":`...)
	entityBuffer = fasthttp.AppendUint(entityBuffer, int(v.Id))
	entityBuffer = append(entityBuffer, `,"location":`...)
	entityBuffer = fasthttp.AppendUint(entityBuffer, int(v.Location.Id))
	entityBuffer = append(entityBuffer, '}')

	return entityBuffer
}

func (v *Visit) SerializeVisited(entityBuffer []byte) []byte {
	entityBuffer = append(entityBuffer, `{"mark":`...)
	entityBuffer = fasthttp.AppendUint(entityBuffer, int(v.Mark))
	entityBuffer = append(entityBuffer, `,"visited_at":`...)
	entityBuffer = fasthttp.AppendUint(entityBuffer, int(v.VisitedAt))
	entityBuffer = append(entityBuffer, `,"place":"`...)
	entityBuffer = append(entityBuffer, v.Location.Place...)
	entityBuffer = append(entityBuffer, `"}`...)

	return entityBuffer
}

func stringToBytes(str string) []byte {
	strh := (*reflect.StringHeader)(unsafe.Pointer(&str))
	var sh reflect.SliceHeader
	sh.Data = strh.Data
	sh.Len = strh.Len
	sh.Cap = strh.Len
	return *(*[]byte)(unsafe.Pointer(&sh))
}

func bytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
