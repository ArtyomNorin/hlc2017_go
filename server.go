package main

import (
	"bytes"
	"github.com/tidwall/evio"
	"github.com/valyala/fasthttp"
	"log"
	"strconv"
	"sync"
	"time"
)

const GetUserMethod = 1
const GetLocationMethod = 2
const GetVisitMethod = 3

const GetVisitedPlacesMethod = 4
const GetAvgMarkMethod = 5

const UpdateUserMethod = 6
const UpdateLocationMethod = 7
const UpdateVisitMethod = 8

const CreateUserMethod = 9
const CreateLocationMethod = 10
const CreateVisitMethod = 11

var GetUserRoute = []byte("/users/<id>")
var GetLocationRoute = []byte("/locations/<id>")
var GetVisitRoute = []byte("/visits/<id>")

var GetVisitedPlacesRoute = []byte("/users/<id>/visits")
var GetAvgMarkRoute = []byte("/locations/<id>/avg")

var CreateUserRoute = []byte("/users/new")
var CreateLocationRoute = []byte("/locations/new")
var CreateVisitRoute = []byte("/visits/new")

var GetRequest = []byte("GET")
var PostRequest = []byte("POST")

var IdReplacer = []byte("<id>")
/*var UsersRoutePart = []byte("users")
var VisitsRoutePart = []byte("visits")
var LocationsRoutePart = []byte("locations")
var AvgRoutePart = []byte("avg")
var NewRoutePart = []byte("new")

var queryParamsDelimiter = []byte("&")
var queryKeyValueDelimiter = []byte("=")*/

type Server struct {
	RequestPool         sync.Pool
	DataBase            *DataBase
	UsersCache          map[string][]byte
	UsersCacheMutex     *sync.Mutex
	LocationsCache      map[string][]byte
	LocationsCacheMutex *sync.Mutex
}

func NewServer(database *DataBase) *Server {
	server := new(Server)

	server.RequestPool = sync.Pool{
		New: func() interface{} { return &Request{Query: make(map[string]string, 5)} },
	}

	server.DataBase = database

	server.LocationsCacheMutex = new(sync.Mutex)
	server.UsersCacheMutex = new(sync.Mutex)

	if database.IsTrain {
		server.UsersCache = make(map[string][]byte, 1782)
		server.LocationsCache = make(map[string][]byte, 1846)
	} else {
		server.UsersCache = make(map[string][]byte, 30044)
		server.LocationsCache = make(map[string][]byte, 29775)
	}

	return server
}

type RequestContext struct {
	InputStream evio.InputStream
	Out         [4096]byte
}

/*type HttpRequest struct {
	Route    uint8
	Query    map[string]string
	Body     []byte
	EntityId int
}*/

type Request struct {
	Method   []byte
	Path     []byte
	CacheKey string
	Query    map[string]string
	Body     []byte
	EntityId int
}

func (s *Server) SaveUserToCache(request *Request, response []byte) {
	_, isFound := s.GetUserFromCache(request)

	if !isFound {
		s.UsersCacheMutex.Lock()
		s.UsersCache[request.CacheKey] = response
		s.UsersCacheMutex.Unlock()
	}
}

func (s *Server) GetUserFromCache(request *Request) (response []byte, isFound bool) {
	response, isFound = s.UsersCache[request.CacheKey]
	return
}

func (s *Server) SaveLocationToCache(request *Request, response []byte) {
	_, isFound := s.GetLocationFromCache(request)

	if !isFound {
		s.LocationsCacheMutex.Lock()
		s.LocationsCache[request.CacheKey] = response
		s.LocationsCacheMutex.Unlock()
	}
}

func (s *Server) GetLocationFromCache(request *Request) (response []byte, isFound bool) {
	response, isFound = s.LocationsCache[request.CacheKey]
	return
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
		} else if bytes.Equal(request.Path, GetUserRoute) && bytes.Equal(request.Method, GetRequest) {
			out = s.DataBase.GetUser(request.EntityId, out)
			/*response, isFound := s.GetUserFromCache(request)

			if isFound {
				out = response
			} else {
				out = s.DataBase.GetUser(request.EntityId, out)
				s.SaveUserToCache(request, out)
			}*/

		} else if bytes.Equal(request.Path, GetLocationRoute) && bytes.Equal(request.Method, GetRequest) {
			out = s.DataBase.GetLocation(request.EntityId, out)
			/*response, isFound := s.GetLocationFromCache(request)

			if isFound {
				out = response
			} else {
				out = s.DataBase.GetLocation(request.EntityId, out)
				s.SaveLocationToCache(request, out)
			}*/

		} else if bytes.Equal(request.Path, GetVisitRoute) && bytes.Equal(request.Method, GetRequest) {
			out = s.DataBase.GetVisit(request.EntityId, out)
			/*response, isFound := s.GetUserFromCache(request)

			if isFound {
				out = response
			} else {
				out = s.DataBase.GetVisit(request.EntityId, out)
				s.SaveUserToCache(request, out)
			}*/

		} else if bytes.Equal(request.Path, GetVisitedPlacesRoute) && bytes.Equal(request.Method, GetRequest) {
			out = s.DataBase.GetVisitedPlaces(request.EntityId, out, request)

		} else if bytes.Equal(request.Path, GetAvgMarkRoute) && bytes.Equal(request.Method, GetRequest) {
			out = s.DataBase.GetAvgMark(request.EntityId, out, request)

		} else {
			out = notFoundResponse
		}

		s.releaseRequest(request)
		ctx.InputStream.End(data[:0])
		return
	}

	if err := evio.Serve(events, "tcp4://:"+strconv.Itoa(port)); err != nil {
		log.Fatal(err)
	}
}

/*func (s *Server) parseRequest(body []byte) (*HttpRequest, int) {
	httpRequest := new(HttpRequest)
	httpRequest.Query = make(map[string]string, 5)

	isPost := false
	leftIndex := 4
	rightIndex := 0
	var query []byte

	if bytes.Equal(body[:4], PostRequest) {
		isPost = true
		leftIndex = 5
	}

	rightIndex = bytes.IndexByte(body[leftIndex:], '?')

	if rightIndex == -1 {
		rightIndex = bytes.IndexByte(body[leftIndex:], ' ')
	} else {
		query = body[rightIndex+leftIndex+1 : bytes.IndexByte(body[leftIndex:], ' ')+leftIndex]
	}

	path := body[leftIndex : rightIndex+leftIndex]

	switch {
	case bytes.Contains(path, UsersRoutePart) && bytes.Contains(path, VisitsRoutePart) && !isPost:
		httpRequest.Route = GetVisitedPlacesMethod
		entityId, err := fasthttp.ParseUint(path[7 : len(path)-7])

		if err != nil {
			return httpRequest, 404
		}

		httpRequest.EntityId = entityId
		break
	case bytes.Contains(path, LocationsRoutePart) && bytes.Contains(path, AvgRoutePart) && !isPost:
		httpRequest.Route = GetAvgMarkMethod
		entityId, err := fasthttp.ParseUint(path[11 : len(path)-4])

		if err != nil {
			return httpRequest, 404
		}

		httpRequest.EntityId = entityId
		break
	case bytes.Contains(path, UsersRoutePart) && bytes.Contains(path, NewRoutePart) && isPost:
		httpRequest.Route = CreateUserMethod
		break
	case bytes.Contains(path, LocationsRoutePart) && bytes.Contains(path, NewRoutePart) && isPost:
		httpRequest.Route = CreateLocationMethod
		break
	case bytes.Contains(path, VisitsRoutePart) && bytes.Contains(path, NewRoutePart) && isPost:
		httpRequest.Route = CreateVisitMethod
		break
	case bytes.Contains(path, UsersRoutePart) && isPost:
		httpRequest.Route = UpdateUserMethod
		entityId, err := fasthttp.ParseUint(path[7:])

		if err != nil {
			return httpRequest, 404
		}

		httpRequest.EntityId = entityId
		break
	case bytes.Contains(path, LocationsRoutePart) && isPost:
		httpRequest.Route = UpdateLocationMethod
		entityId, err := fasthttp.ParseUint(path[11:])

		if err != nil {
			return httpRequest, 404
		}

		httpRequest.EntityId = entityId
		break
	case bytes.Contains(path, VisitsRoutePart) && isPost:
		httpRequest.Route = UpdateVisitMethod
		entityId, err := fasthttp.ParseUint(path[8:])

		if err != nil {
			return httpRequest, 404
		}

		httpRequest.EntityId = entityId
		break
	case bytes.Contains(path, UsersRoutePart) && !isPost:
		httpRequest.Route = GetUserMethod
		entityId, err := fasthttp.ParseUint(path[7:])

		if err != nil {
			return httpRequest, 404
		}

		httpRequest.EntityId = entityId
		break
	case bytes.Contains(path, LocationsRoutePart) && !isPost:
		httpRequest.Route = GetLocationMethod
		entityId, err := fasthttp.ParseUint(path[11:])

		if err != nil {
			return httpRequest, 404
		}

		httpRequest.EntityId = entityId
		break
	case bytes.Contains(path, VisitsRoutePart) && !isPost:
		httpRequest.Route = GetVisitMethod
		entityId, err := fasthttp.ParseUint(path[8:])

		if err != nil {
			return httpRequest, 404
		}

		httpRequest.EntityId = entityId
		break
	default:
		return httpRequest, 404
	}

	if isPost {
		leftIndex = bytes.IndexByte(body, '{')

		if leftIndex == -1 {
			return httpRequest, 400
		}

		httpRequest.Body = body[leftIndex:]
	}

	if query != nil {
		leftIndex = 0

		for {
			rightIndex = bytes.IndexByte(query[leftIndex:], '=')

			queryKey := query[leftIndex : rightIndex+leftIndex]
			leftIndex += len(queryKey) + 1

			rightIndex = bytes.IndexByte(query[leftIndex:], '&')

			if rightIndex == -1 {
				queryValue := query[leftIndex:]

				httpRequest.Query[string(queryKey)] = string(queryValue)
				break
			} else {
				queryValue := query[leftIndex : rightIndex+leftIndex]

				httpRequest.Query[string(queryKey)] = string(queryValue)
				leftIndex += len(queryValue) + 1
			}
		}
	}

	return httpRequest, 200
}*/

func (s *Server) acquireRequest(body []byte) (*Request, int) {
	request := s.RequestPool.Get().(*Request)

	startIndex := 0
	startEntityIdIndex := 0
	endEntityIdIndex := 0

	index := bytes.IndexByte(body, ' ')
	request.Method = body[startIndex:index]
	startIndex = index + 1

	index = bytes.IndexByte(body[startIndex:], '?')

	if index == -1 {
		index = bytes.IndexByte(body[startIndex:], ' ')
	}

	if index != -1 {
		request.Path = body[startIndex : index+startIndex]
		request.CacheKey = string(body[startIndex : index+startIndex])
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

	request.EntityId, _ = fasthttp.ParseUint(request.Path[startEntityIdIndex : endEntityIdIndex+1])

	//todo performance degradation
	request.Path = bytes.Replace(request.Path, request.Path[startEntityIdIndex:endEntityIdIndex+1], IdReplacer, -1)

	if !bytes.Equal(request.Path, GetUserRoute) &&
		!bytes.Equal(request.Path, GetLocationRoute) &&
		!bytes.Equal(request.Path, GetVisitRoute) &&
		!bytes.Equal(request.Path, GetVisitedPlacesRoute) &&
		!bytes.Equal(request.Path, GetAvgMarkRoute) &&
		!bytes.Equal(request.Path, CreateUserRoute) &&
		!bytes.Equal(request.Path, CreateLocationRoute) &&
		!bytes.Equal(request.Path, CreateVisitRoute) {
		return request, 404
	}

	if body[startIndex-1] == '?' {
		for {
			index = bytes.IndexByte(body[startIndex:], '&')

			if index == -1 {
				index = bytes.IndexByte(body[startIndex:], ' ')
			}

			splitIndex := bytes.IndexByte(body[startIndex:index+startIndex], '=')

			request.Query[string(body[startIndex:startIndex+splitIndex])] = string(body[startIndex+splitIndex+1 : startIndex+index])
			startIndex = index + startIndex + 1

			if body[startIndex] == 'H' {
				break
			}
		}
	}

	if bytes.Equal(request.Method, PostRequest) {
		index = bytes.IndexByte(body, '{')

		if index == -1 {
			return request, 400
		}

		request.Body = body[index:]
	}

	return request, 200
}

func (s *Server) releaseRequest(request *Request) {
	request.Body = nil
	request.Path = nil
	request.Method = nil
	request.EntityId = 0
	request.CacheKey = ""
	for k := range request.Query {
		delete(request.Query, k)
	}
	s.RequestPool.Put(request)
}
