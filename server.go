package main

import (
	"bytes"
	"github.com/tidwall/evio"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

const GetUserRoute = "/users/<id>"
const GetLocationRoute = "/locations/<id>"
const GetVisitRoute = "/visits/<id>"

const GetVisitedPlacesRoute = "/users/<id>/visits"
const GetAvgMarkRoute = "/locations/<id>/avg"

const UpdateUserRoute = GetUserRoute
const UpdateLocationRoute = GetLocationRoute
const UpdateVisitRoute = GetVisitRoute

const CreateUserRoute = "/users/new"
const CreateLocationRoute = "/locations/new"
const CreateVisitRoute = "/visits/new"

type Server struct {
	RequestPool sync.Pool
	DataBase    *DataBase
}

func NewServer(database *DataBase) *Server {
	server := new(Server)

	server.RequestPool = sync.Pool{
		New: func() interface{} { return &Request{Query: map[string]string{}} },
	}
	server.DataBase = database

	return server
}

type RequestContext struct {
	InputStream evio.InputStream
	Out         [4096]byte
}

type Request struct {
	Method   string
	Path     string
	Query    map[string]string
	Body     []byte
	EntityId int
}

func (s *Server) Run(port int) {
	var events evio.Events

	events.NumLoops = 4
	events.LoadBalance = evio.RoundRobin

	events.Serving = func(server evio.Server) (action evio.Action) {
		log.Println("Server is listening")
		return
	}

	events.Opened = func(c evio.Conn) (out []byte, opts evio.Options, action evio.Action) {
		c.SetContext(new(RequestContext))
		opts.ReuseInputBuffer = true
		opts.TCPKeepAlive = 30 * time.Second
		return
	}

	events.Data = func(c evio.Conn, in []byte) (out []byte, action evio.Action) {
		ctx := c.Context().(*RequestContext)
		data := ctx.InputStream.Begin(in)
		out = ctx.Out[:0]

		request, statusCode := s.acquireRequest(data)

		if statusCode == 404 {
			out = notFoundResponse
		} else if request.Path == GetUserRoute && request.Method == "GET" {
			out = s.DataBase.GetUser(request.EntityId, out)

		} else if request.Path == GetLocationRoute && request.Method == "GET" {
			out = s.DataBase.GetLocation(request.EntityId, out)

		} else if request.Path == GetVisitRoute && request.Method == "GET" {
			out = s.DataBase.GetVisit(request.EntityId, out)

		} else if request.Path == GetVisitedPlacesRoute && request.Method == "GET" {
			out = s.DataBase.GetVisitedPlaces(request.EntityId, out, request)

		} else if request.Path == GetAvgMarkRoute && request.Method == "GET" {
			out = s.DataBase.GetAvgMark(request.EntityId, out, request)

		} else {
			out = notFoundResponse
		}

		//s.releaseRequest(request)
		ctx.InputStream.End(data[:0])
		return
	}

	if err := evio.Serve(events, "tcp4://:"+strconv.Itoa(port)); err != nil {
		log.Fatal(err)
	}
}

func (s *Server) acquireRequest(body []byte) (*Request, int) {
	//request := s.RequestPool.Get().(*Request)
	request := new(Request)
	request.Query = make(map[string]string)

	startIndex := 0
	startEntityIdIndex := 0
	endEntityIdIndex := 0

	index := bytes.IndexByte(body, ' ')
	request.Method = bytesToString(body[startIndex:index])
	startIndex = index + 1

	index = bytes.IndexByte(body[startIndex:], '?')

	if index == -1 {
		index = bytes.IndexByte(body[startIndex:], ' ')
	}

	if index != -1 {
		request.Path = bytesToString(body[startIndex : index+startIndex])
		startIndex = index + startIndex + 1
	}

	for index, char := range request.Path {
		if startEntityIdIndex == 0 {
			if char >= 48 && char <= 57 {
				startEntityIdIndex = index
			} else {
				continue
			}
		}

		if char >= 48 && char <= 57 {
			endEntityIdIndex = index
			continue
		}

		break
	}

	request.EntityId, _ = strconv.Atoi(request.Path[startEntityIdIndex : endEntityIdIndex+1])

	//todo performance degradation
	request.Path = strings.Replace(request.Path, request.Path[startEntityIdIndex:endEntityIdIndex+1], "<id>", -1)

	if request.Path != GetUserRoute &&
		request.Path != GetLocationRoute &&
		request.Path != GetVisitRoute &&
		request.Path != GetVisitedPlacesRoute &&
		request.Path != GetAvgMarkRoute &&
		request.Path != CreateUserRoute &&
		request.Path != CreateLocationRoute &&
		request.Path != CreateVisitRoute {
		return request, 404
	}

	if body[startIndex-1] == '?' {
		for {
			index = bytes.IndexByte(body[startIndex:], '&')

			if index == -1 {
				index = bytes.IndexByte(body[startIndex:], ' ')
			}

			splitIndex := bytes.IndexByte(body[startIndex:index+startIndex], '=')

			request.Query[bytesToString(body[startIndex:startIndex+splitIndex])] = bytesToString(body[startIndex+splitIndex+1 : startIndex+index])
			startIndex = index + startIndex + 1

			if body[startIndex] == 'H' {
				break
			}
		}
	}

	if request.Method == "POST" {
		index = bytes.IndexByte(body, '{')

		if index == -1 {
			return request, 400
		}

		request.Body = body[index:]
	}

	return request, 200
}

func (s *Server) releaseRequest(request *Request) {
	s.RequestPool.Put(request)
}
